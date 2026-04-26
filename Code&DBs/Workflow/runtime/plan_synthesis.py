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
    # Synthesis observability — same fields as AuthoredPacket so the
    # experiment report can render per-call rows uniformly across the
    # synthesis call and every fork-out call.
    wall_ms: int | None = None
    latency_ms: int | None = None
    finish_reason: str | None = None
    content_len: int | None = None
    reasoning_len: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "packet_seeds": [s.to_dict() for s in self.packet_seeds],
            "raw_response": self.raw_response,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "usage": dict(self.usage),
            "notes": list(self.notes),
            "wall_ms": self.wall_ms,
            "latency_ms": self.latency_ms,
            "finish_reason": self.finish_reason,
            "content_len": self.content_len,
            "reasoning_len": self.reasoning_len,
        }


def _render_recognized_block(recognition: Any) -> str | None:
    """Render the recognize_intent output as a prompt section.

    Returns ``None`` when there's nothing useful to anchor on. Otherwise emits
    a compact "RECOGNIZED" block that names the spans Praxis already
    extracted and the suggested-step labels they map to. The synthesis call
    then anchors decomposition on this list rather than re-deriving it (and
    rather than the previous hardcoded "up to 20 seeds" attractor that
    triggered degenerate reasoning loops on quantized models).
    """
    if recognition is None:
        return None
    spans = list(getattr(recognition, "spans", []) or [])
    suggested_steps = list(getattr(recognition, "suggested_steps", []) or [])
    if not spans and not suggested_steps:
        return None

    lines: list[str] = ["RECOGNIZED:"]
    if spans:
        lines.append("  spans (verb phrases the operator wrote):")
        for span in spans[:10]:
            text = str(getattr(span, "text", "") or "").strip()
            kind = str(getattr(span, "normalized", "") or getattr(span, "kind", "") or "").strip()
            if text:
                lines.append(f"    - {text!r} (kind={kind})" if kind else f"    - {text!r}")
    if suggested_steps:
        lines.append("  suggested_steps (what Praxis matched these to):")
        for step in suggested_steps[:10]:
            label = str(getattr(step, "label", "") or "").strip()
            reason = str(getattr(step, "reason", "") or "").strip()
            if label:
                lines.append(f"    - {label}: {reason}" if reason else f"    - {label}")
    return "\n".join(lines)


def _build_synthesis_prompt(
    *, atoms: SuggestedAtoms, skeleton: SkeletalPlan, sandbox: SectionSandbox,
    recognition: Any | None = None,
) -> str:
    """Synthesis prompt = shared_prefix + RECOGNIZED + synth-task suffix.

    The shared_prefix is byte-identical to the prefix that every fork-out call
    will ship, so this synthesis call primes the provider's prefix cache for
    the N parallel author calls that follow.

    The RECOGNIZED block (when ``recognition`` is provided) tells the model
    exactly which spans Praxis already extracted from the intent, so it
    anchors decomposition on those instead of inferring them from scratch.
    Decomposition target = ``len(spans)``, not a hardcoded 20.
    """
    from runtime.plan_section_author import build_shared_prefix

    span_count = 0
    if recognition is not None:
        span_count = len(list(getattr(recognition, "spans", []) or []))

    if span_count > 0:
        target_clause = (
            f"Emit one packet seed per recognized span (~{span_count} here). "
            f"Use the suggested_step labels above as packet labels when they fit."
        )
    else:
        # Recognition produced no spans; fall back to the work-volume framing
        # but with a much tighter cap that doesn't push the model toward
        # filler decomposition.
        target_clause = (
            "Decompose by WORK VOLUME into 1-5 concrete packet seeds — "
            "no filler, no padding."
        )

    sections: list[str] = [build_shared_prefix(atoms, sandbox)]
    recognized_block = _render_recognized_block(recognition)
    if recognized_block:
        sections.append(recognized_block)
    sections.append(
        "TASK: " + target_clause + " "
        "Stages limited to research / review / build (NO test, NO fix). "
        "Pill triage and pill proposals are NOT your job — fork-out authors "
        "handle those per-packet. Just emit seeds."
    )
    # The shared_prefix above lists all 16 PlanPacket fields because fork-out
    # authors fill them. Synthesis only emits the 4-field seed shape; tell
    # the model explicitly so it doesn't try to fill the other 12 here. (The
    # synthesizer + fork-out authors take care of the rest downstream.)
    sections.append(
        "SEED FIELDS (synthesis emits these 4 ONLY — synthesizer scaffolds "
        "the other 12 PlanPacket fields downstream):\n"
        "  - label: stable identifier (e.g. \"validate_input\")\n"
        "  - stage: one of research | review | build\n"
        "  - description: one sentence of what this packet does\n"
        "  - depends_on: array of upstream packet labels (empty for first packet)"
    )
    # Render a fully-formed example (not an angle-bracket placeholder
    # template). Quantized reasoning models can fail to ground on
    # metavariable shapes — they need a concrete reference to copy. The
    # example seed count matches the recognized span count when available,
    # so the model isn't pushed to invent more or fewer seeds than the
    # TASK clause asks for.
    example_count = max(1, span_count) if span_count > 0 else 2
    example_seeds = [
        {
            "label": "validate_input",
            "stage": "review",
            "description": "Confirm the input text is non-empty and parseable.",
            "depends_on": [],
        },
        {
            "label": "summarize_input",
            "stage": "build",
            "description": "Produce a one-sentence summary from the validated input.",
            "depends_on": ["validate_input"],
        },
        {
            "label": "record_receipt",
            "stage": "build",
            "description": "Persist a receipt of the run for later replay.",
            "depends_on": ["summarize_input"],
        },
    ][:example_count]
    sections.append(
        "EXAMPLE OUTPUT (illustrative shape; emit your own seeds for the "
        "actual intent):\n"
        + json.dumps({"packet_seeds": example_seeds}, indent=2)
    )
    sections.append(
        "OUTPUT: a JSON object {\"packet_seeds\": [...]} with no fences and "
        "no prose outside JSON."
    )
    return "\n\n".join(sections)


def _call_synthesis_llm(
    prompt: str, *, hydrate_env: Any | None = None,
    llm_overrides: dict[str, Any] | None = None,
) -> tuple[str, str, str, dict[str, int], dict[str, Any]]:
    """Call the synthesis LLM. When ``llm_overrides`` is set, skip route
    resolution and use the override (model_slug, provider_slug, temperature,
    max_tokens). Otherwise fall back to ``task_type_routing`` resolution
    with the per-task default cap of 4096.

    Returns ``(content, provider_slug, model_slug, usage, call_metrics)``.
    ``call_metrics`` carries non-token observability:
    ``{latency_ms, finish_reason, content_len, reasoning_len}``.

    Override keys honoured: provider_slug, model_slug, temperature,
    max_tokens. All optional inside the override; missing keys keep their
    task-level defaults (provider/model from routing if neither is set;
    temp = LLMRequest default; max_tokens = 4096).
    """
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    if hydrate_env is not None:
        hydrate_env()

    overrides = dict(llm_overrides or {})
    override_temperature = overrides.get("temperature")
    override_max_tokens = overrides.get("max_tokens")
    override_provider = overrides.get("provider_slug")
    override_model = overrides.get("model_slug")

    from runtime.compiler_llm import resolve_matrix_gated_route_configs
    timeout = int(_section_author_timeout_seconds())

    # Pull the full row config (provider, model, temperature, max_tokens)
    # from task_type_routing. Migration 276 lifted the LLM knobs into
    # rows so experiments can inherit them. Override resolution priority:
    #   - both provider+model pinned → use exactly that pair (skip routing)
    #   - only model_slug → resolve provider from provider_model_candidates
    #     (lets experiments name a model not in this task_type's routing)
    #   - only provider_slug → narrow to that provider's rows
    #   - neither → full routing list
    if override_provider and override_model:
        route_configs: list[dict[str, Any]] = [{
            "provider_slug": override_provider,
            "model_slug": override_model,
            "temperature": None,
            "max_tokens": None,
        }]
    elif override_model and not override_provider:
        # Look up the model in provider_model_candidates so the override
        # works even if the model isn't currently registered as a route
        # for this task_type.
        from runtime.compiler_llm import (
            resolve_matrix_gated_route_configs as _resolve_configs,
            _resolve_provider_for_model,
        )
        all_configs = _resolve_configs("plan_synthesis")
        narrowed = [r for r in all_configs if r["model_slug"] == override_model]
        if narrowed:
            route_configs = narrowed
        else:
            inferred_provider = _resolve_provider_for_model(override_model)
            if inferred_provider:
                route_configs = [{
                    "provider_slug": inferred_provider,
                    "model_slug": override_model,
                    "temperature": None,
                    "max_tokens": None,
                }]
            else:
                raise RuntimeError(
                    f"plan_synthesis override pinned model_slug={override_model!r} "
                    f"but no provider candidate was found in provider_model_candidates"
                )
    else:
        all_configs = resolve_matrix_gated_route_configs("plan_synthesis")
        if override_provider:
            route_configs = [r for r in all_configs if r["provider_slug"] == override_provider] or all_configs
        else:
            route_configs = all_configs

    last_error: Exception | None = None
    for route_config in route_configs:
        provider_slug = route_config["provider_slug"]
        model_slug = route_config["model_slug"]
        row_temperature = route_config.get("temperature")
        row_max_tokens = route_config.get("max_tokens")
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

            # Resolution priority for max_tokens / temperature:
            #   1. explicit override on this call
            #   2. row value from task_type_routing (migration 276)
            #   3. hardcoded default (4096 / LLMRequest temp default)
            resolved_max_tokens = (
                int(override_max_tokens) if override_max_tokens is not None
                else (int(row_max_tokens) if row_max_tokens is not None else 4096)
            )
            resolved_temperature = (
                float(override_temperature) if override_temperature is not None
                else (float(row_temperature) if row_temperature is not None else None)
            )
            request_kwargs: dict[str, Any] = dict(
                endpoint_uri=str(endpoint), api_key=api_key,
                provider_slug=provider_slug, model_slug=model_slug,
                messages=({"role": "user", "content": prompt},),
                protocol_family=str(protocol_family),
                timeout_seconds=timeout, retry_attempts=0,
                max_tokens=resolved_max_tokens,
            )
            if resolved_temperature is not None:
                request_kwargs["temperature"] = resolved_temperature
            request = LLMRequest(**request_kwargs)
            response = call_llm(request)
            choices = (response.raw_response or {}).get("choices") or []
            first_message = (choices[0] or {}).get("message") if choices else {}
            finish_reason = (choices[0] or {}).get("finish_reason") if choices else None
            reasoning_text = (first_message or {}).get("reasoning") or ""
            call_metrics = {
                "latency_ms": int(response.latency_ms or 0),
                "finish_reason": str(finish_reason) if finish_reason else None,
                "content_len": len(response.content or ""),
                "reasoning_len": len(reasoning_text),
            }
            return (
                response.content,
                provider_slug,
                model_slug,
                dict(response.usage or {}),
                call_metrics,
            )
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
    llm_overrides: dict[str, Any] | None = None,
) -> PlanSynthesis:
    from runtime.plan_section_author import build_section_sandbox

    sandbox = build_section_sandbox(conn)

    # Pull recognize_intent's structured output (spans, suggested_steps, gaps)
    # so the synthesis prompt anchors on what Praxis already extracted from
    # the prose. Without this, the prompt forces the model to re-derive the
    # decomposition from scratch and the hardcoded "up to 20 seeds" clause
    # creates a degenerate basin on quantized models.
    recognition: Any | None = None
    intent_text = (atoms.intent or "").strip()
    if intent_text:
        try:
            from runtime.intent_recognition import recognize_intent

            recognition = recognize_intent(intent_text, conn=conn)
        except Exception as exc:
            # Recognition is advisory, not a hard prereq. Log and continue
            # with the no-recognition fallback path inside _build_synthesis_prompt.
            logger.warning("recognize_intent failed during synthesis: %s", exc)
            recognition = None

    prompt = _build_synthesis_prompt(
        atoms=atoms, skeleton=skeleton, sandbox=sandbox,
        recognition=recognition,
    )

    import time
    started_ns = time.monotonic_ns()
    call_metrics: dict[str, Any] = {}
    try:
        raw, provider_slug, model_slug, usage, call_metrics = _call_synthesis_llm(
            prompt, hydrate_env=hydrate_env, llm_overrides=llm_overrides,
        )
    except Exception:
        # Re-raise; the caller's exception path handles it. We still want
        # the wall_ms recorded if the call manages to succeed below.
        raise
    wall_ms = (time.monotonic_ns() - started_ns) // 1_000_000

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
        wall_ms=wall_ms,
        latency_ms=call_metrics.get("latency_ms"),
        finish_reason=call_metrics.get("finish_reason"),
        content_len=call_metrics.get("content_len"),
        reasoning_len=call_metrics.get("reasoning_len"),
    )
