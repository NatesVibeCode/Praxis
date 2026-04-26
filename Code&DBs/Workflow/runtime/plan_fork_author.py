"""Layer 4 (Fork-out author): N parallel author calls forking a shared synthesis.

Each call sees:
  - the synthesis statement from runtime.plan_synthesis (shared prefix → cache hit)
  - the lean per-packet skeleton (its label, stage, depends_on, floors, gates)
  - the plan_field schema (what to fill)

No waves. No stage clusters. All N packets author in parallel. The synthesis
prefix is identical across calls so the provider-side cache (DeepSeek auto,
Together auto, OpenAI auto for >1024 tok) gives input-cost discount for free.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPacket, SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms
from runtime.plan_section_author import (
    AuthoredPacket,
    AuthorError,
    SectionSandbox,
    _coerce_packet_response,
    _resolve_routes_for_task_type,
    _section_author_timeout_seconds,
    _strip_json_fences,
)
from runtime.plan_synthesis import PlanSynthesis

logger = logging.getLogger(__name__)


_DEFAULT_FORK_CONCURRENCY = 20


@dataclass(frozen=True)
class AuthoredPlan:
    packets: list[AuthoredPacket]
    errors: list[AuthorError]
    notes: list[str] = field(default_factory=list)
    synthesis: PlanSynthesis | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "packets": [p.to_dict() for p in self.packets],
            "errors": [e.to_dict() for e in self.errors],
            "notes": list(self.notes),
            "synthesis": self.synthesis.to_dict() if self.synthesis else None,
        }


def _build_fork_prompt(
    *, synthesis: PlanSynthesis, target: SkeletalPacket,
    sandbox: SectionSandbox, atoms: SuggestedAtoms,
) -> str:
    """Fork-out prompt = shared_prefix + per-packet suffix.

    The shared_prefix is byte-identical to the synthesis call's prefix; the
    provider's prefix cache hits and only the suffix delta pays full cost.
    Synthesis output is intentionally NOT included here — it would change the
    prefix bytes and break cache. Synthesis exists only to prime the cache.
    """
    from runtime.plan_section_author import build_shared_prefix

    target_view = {
        "label": target.label, "stage": target.stage, "description": target.description,
        "consumes_floor": list(target.consumes_floor),
        "produces_floor": list(target.produces_floor),
        "capabilities_floor": list(target.capabilities_floor),
        "depends_on": list(target.depends_on),
        "scaffolded_gate_ids": [gate.gate_id for gate in target.gates_scaffold],
    }

    return "\n\n".join(
        [
            build_shared_prefix(atoms, sandbox),
            "TASK: Author one PlanPacket for the skeleton below. Keep depends_on "
            "as given. gates is a list of {gate_id, params} objects.\n"
            "Also emit one side-channel alongside the packet:\n"
            "  - pill_audit_local: triage of every DATA PILL above that was "
            "RELEVANT to this packet. {ref, verdict (confirmed|misattributed), "
            "reason}. Skip pills you didn't consider. Keep this list short.",
            "SKELETON:",
            json.dumps(target_view, indent=2, sort_keys=True),
            "OUTPUT: a JSON object with the PlanPacket fields PLUS a top-level "
            "\"pill_audit_local\": [...] array. Empty array is fine. No fences, "
            "no prose outside JSON.",
        ]
    )


def _call_fork_llm(
    prompt: str,
    *,
    pinned_route: tuple[str, str] | None = None,
    hydrate_env: Any | None = None,
) -> tuple[str, str, str, dict[str, int]]:
    """If ``pinned_route`` is given, try it only; else iterate the route list."""
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    if hydrate_env is not None:
        hydrate_env()

    routes = [pinned_route] if pinned_route else _resolve_routes_for_task_type("plan_fork_author")
    timeout = int(_section_author_timeout_seconds())
    last_error: Exception | None = None
    for provider_slug, model_slug in routes:
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
            logger.warning(
                "Fork-author route failed for %s/%s: %s", provider_slug, model_slug, exc,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError("no llm_task routes for plan_section_author")


def _author_one_packet(
    *,
    target: SkeletalPacket,
    synthesis: PlanSynthesis,
    sandbox: SectionSandbox,
    atoms: SuggestedAtoms,
    pinned_route: tuple[str, str] | None,
    hydrate_env: Any | None,
) -> AuthoredPacket | AuthorError:
    prompt = _build_fork_prompt(
        synthesis=synthesis, target=target, sandbox=sandbox, atoms=atoms,
    )
    try:
        raw, provider_slug, model_slug, usage = _call_fork_llm(
            prompt, pinned_route=pinned_route, hydrate_env=hydrate_env,
        )
    except Exception as exc:
        return AuthorError(
            label=target.label, error=str(exc),
            reason_code="fork_author.llm_call_failed", raw_llm_response=None,
        )

    try:
        parsed = json.loads(_strip_json_fences(raw))
    except json.JSONDecodeError as exc:
        return AuthorError(
            label=target.label,
            error=f"fork author returned non-JSON: {exc}",
            reason_code="fork_author.parse_failed", raw_llm_response=raw,
        )
    if not isinstance(parsed, dict):
        return AuthorError(
            label=target.label,
            error=f"fork author returned {type(parsed).__name__}, expected object",
            reason_code="fork_author.shape_invalid", raw_llm_response=raw,
        )

    return _coerce_packet_response(
        target=target, parsed=parsed, raw=raw,
        provider_slug=provider_slug, model_slug=model_slug,
        usage=usage,
    )


def fork_author_packets(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    synthesis: PlanSynthesis,
    conn: Any,
    concurrency: int = _DEFAULT_FORK_CONCURRENCY,
    hydrate_env: Any | None = None,
) -> AuthoredPlan:
    """Fan out N parallel author calls, all sharing the synthesis prefix.

    Provider stickiness: the route that won the synthesis call is pinned for
    every fork-out, so the provider's cache holds the synthesis prefix warm
    across all N calls.
    """
    from runtime.plan_section_author import build_section_sandbox

    sandbox = build_section_sandbox(conn)
    # Don't pin the synthesis route onto fork-outs: synthesis uses Pro
    # (heavy reasoning), fork-out uses Flash (cheaper, sufficient for bounded
    # per-packet work). Each fork-out resolves its own route from
    # plan_fork_author task_type rows.
    pinned_route: tuple[str, str] | None = None

    notes: list[str] = []
    if not sandbox.plan_field_schema:
        notes.append("no plan_field rows registered (apply migration 247)")
    if not skeleton.packets:
        notes.append("skeleton has no packets — nothing to author")
        return AuthoredPlan(packets=[], errors=[], notes=notes, synthesis=synthesis)

    successes: list[AuthoredPacket] = []
    failures: list[AuthorError] = []

    n_workers = max(1, min(concurrency, len(skeleton.packets)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(
                _author_one_packet,
                target=target, synthesis=synthesis,
                sandbox=sandbox, atoms=atoms,
                pinned_route=pinned_route, hydrate_env=hydrate_env,
            ): target.label
            for target in skeleton.packets
        }
        for future in concurrent.futures.as_completed(futures):
            label = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                failures.append(AuthorError(
                    label=label, error=f"{type(exc).__name__}: {exc}",
                    reason_code="fork_author.unhandled", raw_llm_response=None,
                ))
                continue
            if isinstance(result, AuthorError):
                failures.append(result)
            else:
                successes.append(result)

    label_order = {p.label: i for i, p in enumerate(skeleton.packets)}
    successes.sort(key=lambda p: label_order.get(p.label, 999_999))

    return AuthoredPlan(
        packets=successes, errors=failures, notes=notes, synthesis=synthesis,
    )
