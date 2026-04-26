"""Layer 3 (Synthesize prose): single LLM call, sandbox + atoms + intent
→ a few-sentence synthesis statement that primes every fork-out author.

Sole purpose: distill what the plan IS so 20 parallel author calls each
get a tiny shared prefix (the synthesis) instead of re-deriving the whole
plan structure on each call. Output is pure prose, ~3-5 sentences.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms
from runtime.plan_section_author import (
    SectionSandbox,
    _resolve_section_author_routes,
    _section_author_timeout_seconds,
    _strip_json_fences,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PacketSeed:
    """One LLM-emitted packet target produced by the synthesis call.

    The synthesis call decomposes the prose into N seeds so fork-out fans
    out across N parallel author calls regardless of how many step-verbs
    the prose used. The deterministic synthesizer then wraps each seed
    with stage-derived floors, scaffolded gates, and depends_on edges
    sourced from the data dictionary.
    """

    label: str
    stage: str
    description: str
    depends_on: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label, "stage": self.stage,
            "description": self.description, "depends_on": list(self.depends_on),
        }


@dataclass(frozen=True)
class PlanSynthesis:
    """Synthesis output is just decomposition. Pill triage / proposals move to fork-out."""

    packet_seeds: list[PacketSeed]
    raw_response: str
    provider_slug: str
    model_slug: str
    usage: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "packet_seeds": [s.to_dict() for s in self.packet_seeds],
            "raw_response": self.raw_response,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "usage": dict(self.usage),
            "notes": list(self.notes),
        }


def _build_synthesis_prompt(
    *, atoms: SuggestedAtoms, skeleton: SkeletalPlan, sandbox: SectionSandbox,
) -> str:
    """Synthesis prompt = shared_prefix + synth-task suffix.

    The shared_prefix is byte-identical to the prefix that every fork-out call
    will ship, so this synthesis call primes the provider's prefix cache for
    the N parallel author calls that follow.
    """
    from runtime.plan_section_author import build_shared_prefix

    return "\n\n".join(
        [
            build_shared_prefix(atoms, sandbox),
            "TASK: Decompose the work into up to 20 concrete packet seeds. "
            "Split by WORK VOLUME, not by step-verbs. Stages limited to "
            "research / review / build (NO test, NO fix). Pill triage and pill "
            "proposals are NOT your job — fork-out authors handle those "
            "per-packet. Just emit seeds.",
            "OUTPUT: {\"packet_seeds\": [{\"label\": \"<slug>\", \"stage\": "
            "\"<research|review|build>\", \"description\": \"<one sentence>\", "
            "\"depends_on\": [\"<upstream_label>\", ...]}, ...]}",
        ]
    )


def _call_synthesis_llm(
    prompt: str, *, hydrate_env: Any | None = None,
) -> tuple[str, str, str, dict[str, int]]:
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    if hydrate_env is not None:
        hydrate_env()

    from runtime.plan_section_author import _resolve_routes_for_task_type
    timeout = int(_section_author_timeout_seconds())
    last_error: Exception | None = None
    for provider_slug, model_slug in _resolve_routes_for_task_type("plan_synthesis"):
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
                "Synthesis route failed for %s/%s: %s", provider_slug, model_slug, exc,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError("no llm_task routes for plan_section_author")


def synthesize_plan_statement(
    *, atoms: SuggestedAtoms, skeleton: SkeletalPlan, conn: Any,
    hydrate_env: Any | None = None,
) -> PlanSynthesis:
    from runtime.plan_section_author import build_section_sandbox

    sandbox = build_section_sandbox(conn)
    prompt = _build_synthesis_prompt(atoms=atoms, skeleton=skeleton, sandbox=sandbox)

    raw, provider_slug, model_slug, usage = _call_synthesis_llm(prompt, hydrate_env=hydrate_env)

    notes: list[str] = []
    seeds: list[PacketSeed] = []
    try:
        parsed = json.loads(_strip_json_fences(raw))
        if isinstance(parsed, dict):
            seed_entries = parsed.get("packet_seeds")
            if isinstance(seed_entries, list):
                for entry in seed_entries[:20]:
                    if not isinstance(entry, dict):
                        continue
                    label = str(entry.get("label") or "").strip()
                    stage = str(entry.get("stage") or "").strip().lower()
                    description = str(entry.get("description") or "").strip()
                    depends_on = entry.get("depends_on") or []
                    if not (label and stage and description):
                        continue
                    # Per the synthesis prompt, only research/review/build are
                    # allowed before approval. Drop any test/fix that slip in.
                    if stage not in {"research", "review", "build"}:
                        notes.append(f"seed {label!r} dropped: stage {stage!r} not allowed pre-approval")
                        continue
                    seeds.append(PacketSeed(
                        label=label, stage=stage, description=description,
                        depends_on=[str(d) for d in depends_on if isinstance(d, str)],
                    ))
        else:
            notes.append("synthesis returned non-object")
    except json.JSONDecodeError:
        notes.append("synthesis returned non-JSON")
    if not seeds:
        notes.append("synthesis emitted no usable packet seeds; falling back to clause-based skeleton")

    return PlanSynthesis(
        packet_seeds=seeds, raw_response=raw,
        provider_slug=provider_slug, model_slug=model_slug,
        usage=usage, notes=notes,
    )
