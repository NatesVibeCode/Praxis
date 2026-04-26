"""On-demand pill triage: aggregate per-packet audits, resolve only conflicts.

Each fork-out author emits a ``pill_audit_local`` (its triage of pills relevant
to its own packet). Most prompts have no cross-packet disagreement; in that
case this module fires zero LLM calls. When two or more packets disagree on
the same pill ref (e.g. one says ``confirmed``, another says ``misattributed``),
we fire one parallel triage call per conflicted ref to resolve.

This avoids the synthesis-call bottleneck (eager global audit) while still
producing a coherent plan-wide pill verdict when packets disagree.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_suggestion import SuggestedAtoms
from runtime.plan_section_author import AuthoredPacket

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PillConflict:
    ref: str
    verdicts: dict[str, list[str]]  # verdict_label -> [packet_label, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"ref": self.ref, "verdicts": dict(self.verdicts)}


@dataclass(frozen=True)
class TriageResolution:
    ref: str
    verdict: str  # confirmed | misattributed | inconclusive
    reason: str
    raw_response: str
    provider_slug: str
    model_slug: str
    usage: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ref": self.ref, "verdict": self.verdict, "reason": self.reason,
            "raw_response": self.raw_response,
            "provider_slug": self.provider_slug, "model_slug": self.model_slug,
            "usage": dict(self.usage),
        }


@dataclass(frozen=True)
class PillTriageResult:
    conflicts: list[PillConflict]
    resolutions: list[TriageResolution]
    confirmed: list[str]   # consensus refs (no conflict, all confirmed)
    rejected: list[str]    # consensus refs (no conflict, all misattributed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "conflicts": [c.to_dict() for c in self.conflicts],
            "resolutions": [r.to_dict() for r in self.resolutions],
            "confirmed": list(self.confirmed),
            "rejected": list(self.rejected),
        }


def _aggregate_audits(packets: list[AuthoredPacket]) -> tuple[
    dict[str, dict[str, list[str]]], list[str], list[str]
]:
    """Return (per_ref_verdicts, consensus_confirmed, consensus_rejected).

    per_ref_verdicts[ref] = {verdict_label: [packet_label, ...]}
    """
    per_ref: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for packet in packets:
        for entry in packet.pill_audit_local or []:
            ref = str(entry.get("ref") or "").strip()
            verdict = str(entry.get("verdict") or "").strip().lower()
            if not ref or not verdict:
                continue
            per_ref[ref][verdict].append(packet.label)

    confirmed: list[str] = []
    rejected: list[str] = []
    for ref, verdicts in per_ref.items():
        verdict_set = set(verdicts.keys())
        if verdict_set == {"confirmed"}:
            confirmed.append(ref)
        elif verdict_set == {"misattributed"}:
            rejected.append(ref)
    return per_ref, sorted(confirmed), sorted(rejected)


def _find_conflicts(per_ref: dict[str, dict[str, list[str]]]) -> list[PillConflict]:
    """A pill ref is in conflict when more than one verdict label applies."""
    conflicts: list[PillConflict] = []
    for ref, verdicts in per_ref.items():
        if len(verdicts) >= 2:
            conflicts.append(PillConflict(
                ref=ref,
                verdicts={k: list(v) for k, v in verdicts.items()},
            ))
    conflicts.sort(key=lambda c: c.ref)
    return conflicts


def _build_triage_prompt(
    *, conflict: PillConflict, atoms: SuggestedAtoms,
    packets: list[AuthoredPacket], sandbox: Any,
) -> str:
    """Triage prompt = same shared_prefix as fork-out (cache hit) + author reasoning."""
    from runtime.plan_section_author import build_shared_prefix

    # Pull each disagreeing author's actual reasoning from their audit_local.
    # That's what they said about THIS specific pill ref — far more useful
    # than re-deriving from packet description alone.
    label_to_packet = {p.label: p for p in packets}
    disagreement_view: list[dict[str, Any]] = []
    for verdict_label, packet_labels in conflict.verdicts.items():
        for packet_label in packet_labels:
            packet = label_to_packet.get(packet_label)
            if not packet:
                continue
            author_reason = ""
            for entry in packet.pill_audit_local or []:
                if str(entry.get("ref") or "") == conflict.ref:
                    author_reason = str(entry.get("reason") or "")
                    break
            disagreement_view.append({
                "packet_label": packet_label,
                "packet_stage": packet.stage,
                "packet_description": packet.description,
                "verdict": verdict_label,
                "author_reason": author_reason,
            })

    return "\n\n".join([
        build_shared_prefix(atoms, sandbox),
        f"TRIAGE TASK: Multiple per-packet authors audited the same pill and "
        f"disagreed. Their verdicts and reasoning are below. Decide the "
        f"workflow-wide verdict.",
        f"PILL: {conflict.ref}",
        "DISAGREEING AUTHORS:",
        json.dumps(disagreement_view, indent=2, sort_keys=True),
        "OUTPUT: {\"verdict\": \"confirmed\" | \"misattributed\" | "
        "\"inconclusive\", \"reason\": \"<one sentence>\"}. No fences.",
    ])


def _call_triage_llm(prompt: str) -> tuple[str, str, str, dict[str, int]]:
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )
    from runtime.plan_section_author import (
        _resolve_routes_for_task_type,
        _section_author_timeout_seconds,
    )

    timeout = int(_section_author_timeout_seconds())
    last_error: Exception | None = None
    # Triage uses fork-out routes (Pro primary today; same warm cache).
    for provider_slug, model_slug in _resolve_routes_for_task_type("plan_fork_author"):
        try:
            endpoint = resolve_api_endpoint(provider_slug, model_slug)
            if not endpoint:
                raise RuntimeError(f"no endpoint for {provider_slug}/{model_slug}")
            protocol_family = resolve_api_protocol_family(provider_slug)
            if not protocol_family:
                raise RuntimeError(f"no protocol_family for {provider_slug}")
            env = dict(os.environ)
            api_key: str | None = None
            for env_var in resolve_api_key_env_vars(provider_slug):
                candidate = resolve_secret(env_var, env=env)
                if candidate and candidate.strip():
                    api_key = candidate.strip()
                    break
            if not api_key:
                raise RuntimeError(f"no API key for {provider_slug}")
            request = LLMRequest(
                endpoint_uri=str(endpoint), api_key=api_key,
                provider_slug=provider_slug, model_slug=model_slug,
                messages=({"role": "user", "content": prompt},),
                protocol_family=str(protocol_family),
                timeout_seconds=timeout, retry_attempts=0,
            )
            response = call_llm(request)
            return response.content, provider_slug, model_slug, dict(response.usage or {})
        except Exception as exc:
            last_error = exc
            logger.warning("Triage route failed for %s/%s: %s", provider_slug, model_slug, exc)
    if last_error is not None:
        raise last_error
    raise RuntimeError("no llm_task routes for plan_fork_author")


def _resolve_one_conflict(
    conflict: PillConflict,
    *, atoms: SuggestedAtoms, packets: list[AuthoredPacket], sandbox: Any,
) -> TriageResolution:
    prompt = _build_triage_prompt(
        conflict=conflict, atoms=atoms, packets=packets, sandbox=sandbox,
    )
    try:
        raw, provider_slug, model_slug, usage = _call_triage_llm(prompt)
    except Exception as exc:
        return TriageResolution(
            ref=conflict.ref, verdict="inconclusive",
            reason=f"triage call failed: {type(exc).__name__}: {exc}",
            raw_response="", provider_slug="", model_slug="",
        )
    text = raw.strip()
    if text.startswith("```"):
        import re
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return TriageResolution(
            ref=conflict.ref, verdict="inconclusive",
            reason="triage returned non-JSON",
            raw_response=raw, provider_slug=provider_slug, model_slug=model_slug,
            usage=usage,
        )
    verdict = str(parsed.get("verdict") or "").strip().lower()
    if verdict not in {"confirmed", "misattributed", "inconclusive"}:
        verdict = "inconclusive"
    reason = str(parsed.get("reason") or "")
    return TriageResolution(
        ref=conflict.ref, verdict=verdict, reason=reason,
        raw_response=raw, provider_slug=provider_slug, model_slug=model_slug,
        usage=usage,
    )


def triage_plan_pills(
    *,
    atoms: SuggestedAtoms,
    packets: list[AuthoredPacket],
    conn: Any,
    concurrency: int = 8,
) -> PillTriageResult:
    """Aggregate per-packet audits; fire parallel triage forks only on conflicts.

    Triage prompts share the same prefix as fork-out so the provider's prompt
    cache hits, and carry each disagreeing author's actual reasoning so the
    triage LLM doesn't re-derive what they meant.
    """
    from runtime.plan_section_author import build_section_sandbox

    per_ref, confirmed, rejected = _aggregate_audits(packets)
    conflicts = _find_conflicts(per_ref)

    if not conflicts:
        return PillTriageResult(
            conflicts=[], resolutions=[],
            confirmed=confirmed, rejected=rejected,
        )

    sandbox = build_section_sandbox(conn)
    n_workers = max(1, min(concurrency, len(conflicts)))
    resolutions: list[TriageResolution] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(
                _resolve_one_conflict, conflict,
                atoms=atoms, packets=packets, sandbox=sandbox,
            ): conflict.ref
            for conflict in conflicts
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                resolutions.append(future.result())
            except Exception as exc:
                ref = futures[future]
                resolutions.append(TriageResolution(
                    ref=ref, verdict="inconclusive",
                    reason=f"triage future failed: {type(exc).__name__}: {exc}",
                    raw_response="", provider_slug="", model_slug="",
                ))

    return PillTriageResult(
        conflicts=conflicts, resolutions=resolutions,
        confirmed=confirmed, rejected=rejected,
    )
