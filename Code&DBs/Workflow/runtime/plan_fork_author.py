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
    # Total wall-clock for the fan-out: start of first packet submission
    # → completion of the last packet (success or failure). Useful for
    # experiment reports — distinguishes "synthesis was slow" from "the
    # fan-out itself spread out across the rate limit".
    fork_author_wall_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "packets": [p.to_dict() for p in self.packets],
            "errors": [e.to_dict() for e in self.errors],
            "notes": list(self.notes),
            "synthesis": self.synthesis.to_dict() if self.synthesis else None,
            "fork_author_wall_ms": self.fork_author_wall_ms,
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
            (
                "TASK: Author the ONE PlanPacket described by SKELETON below. The "
                "skeleton already pins your label, stage, depends_on, and required-"
                "gate floor; do not change those. Your job is to fill in:\n"
                "  • `prompt` — the agent's runtime instruction. Use {{placeholders}} "
                "    bound through `parameters`; describe what to produce, not how.\n"
                "  • `parameters` — the input dict. Bind every {{placeholder}} you "
                "    reference in `prompt` here. Pill refs go in as strings; upstream "
                "    outputs as `{{<upstream_label>.output}}` (per AUTHORING CONVENTIONS).\n"
                "  • `consumes`/`produces` — narrow from the stage floor to what "
                "    this packet actually consumes/produces (per AUTHORING CONVENTIONS).\n"
                "  • `write` — packet-scoped paths under `artifacts/<stage>/<label>/`.\n"
                "  • `gates` — one entry per scaffolded `gate_id`; `params={}` unless "
                "    you know the gate's param contract.\n"
                "  • `task_type` — usually the stage name; only specialize if you're "
                "    sure a registered task_type fits.\n"
                "  • `agent` — `auto/<stage>` unless you have a concrete reason.\n"
                "Plus the side-channel:\n"
                "  • `pill_audit_local` — verdict for each pill you ACTUALLY "
                "    referenced in `prompt` or `parameters`. Skip pills you didn't "
                "    use. Reject (`misattributed`) when a pill matched by prose-"
                "    similarity but isn't semantically right for THIS packet."
            ),
            "SKELETON:",
            json.dumps(target_view, indent=2, sort_keys=True),
            (
                "OUTPUT: a single JSON object with the PlanPacket fields PLUS a "
                "top-level `pill_audit_local: []` array. No code fences, no prose "
                "outside JSON. If you can't author cleanly, return a JSON object "
                "with `\"error\": \"<brief reason>\"` instead of inventing fields."
            ),
        ]
    )


def _call_fork_llm(
    prompt: str,
    *,
    pinned_route: tuple[str, str] | None = None,
    hydrate_env: Any | None = None,
    llm_overrides: dict[str, Any] | None = None,
) -> tuple[str, str, str, dict[str, int], dict[str, Any]]:
    """If ``pinned_route`` is given, try it only; else iterate the route list.

    Returns ``(content, provider_slug, model_slug, usage, call_metrics)``.
    ``call_metrics`` carries non-token-budget observability:
    ``{latency_ms, finish_reason, content_len, reasoning_len}``. The
    experiment report surfaces these so an operator can see "leg N hit
    finish_reason=length at 4096 tokens with content_len=0" without
    digging into raw responses.

    ``llm_overrides`` honours provider_slug, model_slug, temperature,
    max_tokens (all optional). When provider+model are both pinned via the
    override, route resolution is skipped entirely. Mirrors
    ``plan_synthesis._call_synthesis_llm``.
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

    # Pull the full row config (provider, model, temperature, max_tokens)
    # from task_type_routing. Migration 276 lifted the LLM knobs into
    # rows so experiments can inherit them.
    if override_provider and override_model:
        route_configs: list[dict[str, Any]] = [{
            "provider_slug": override_provider,
            "model_slug": override_model,
            "temperature": None,
            "max_tokens": None,
        }]
    elif pinned_route is not None:
        route_configs = [{
            "provider_slug": pinned_route[0],
            "model_slug": pinned_route[1],
            "temperature": None,
            "max_tokens": None,
        }]
    elif override_model and not override_provider:
        # Look up provider from provider_model_candidates when the
        # override pins model_slug only. See plan_synthesis for the
        # rationale and matching pattern.
        from runtime.compiler_llm import _resolve_provider_for_model
        all_configs = resolve_matrix_gated_route_configs("plan_fork_author")
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
                    f"plan_fork_author override pinned model_slug={override_model!r} "
                    f"but no provider candidate was found in provider_model_candidates"
                )
    else:
        all_configs = resolve_matrix_gated_route_configs("plan_fork_author")
        if override_provider:
            route_configs = [r for r in all_configs if r["provider_slug"] == override_provider] or all_configs
        else:
            route_configs = all_configs

    timeout = int(_section_author_timeout_seconds())
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

            # Resolution priority: explicit override > row value > hardcoded fallback.
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
            # Pull non-token observability from the raw OpenAI/Anthropic
            # response shape so callers don't have to re-parse it.
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
    llm_overrides: dict[str, Any] | None = None,
) -> AuthoredPacket | AuthorError:
    """Author one packet. Times the full LLM-call+parse round-trip and
    threads the call metrics (finish_reason, latency_ms, content_len,
    reasoning_len) onto both the success path (AuthoredPacket) and every
    failure path (AuthorError) so experiment reports never lose them."""
    import time

    prompt = _build_fork_prompt(
        synthesis=synthesis, target=target, sandbox=sandbox, atoms=atoms,
    )
    started_ns = time.monotonic_ns()
    call_metrics: dict[str, Any] = {}
    provider_slug: str | None = None
    model_slug: str | None = None
    usage: dict[str, int] = {}
    try:
        raw, provider_slug, model_slug, usage, call_metrics = _call_fork_llm(
            prompt, pinned_route=pinned_route, hydrate_env=hydrate_env,
            llm_overrides=llm_overrides,
        )
    except Exception as exc:
        wall_ms = (time.monotonic_ns() - started_ns) // 1_000_000
        return AuthorError(
            label=target.label, error=str(exc),
            reason_code="fork_author.llm_call_failed", raw_llm_response=None,
            wall_ms=wall_ms,
            latency_ms=call_metrics.get("latency_ms"),
            finish_reason=call_metrics.get("finish_reason"),
            content_len=call_metrics.get("content_len"),
            reasoning_len=call_metrics.get("reasoning_len"),
            provider_slug=provider_slug,
            model_slug=model_slug,
            usage=usage or None,
        )

    wall_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    try:
        parsed = json.loads(_strip_json_fences(raw))
    except json.JSONDecodeError as exc:
        return AuthorError(
            label=target.label,
            error=f"fork author returned non-JSON: {exc}",
            reason_code="fork_author.parse_failed", raw_llm_response=raw,
            wall_ms=wall_ms,
            latency_ms=call_metrics.get("latency_ms"),
            finish_reason=call_metrics.get("finish_reason"),
            content_len=call_metrics.get("content_len"),
            reasoning_len=call_metrics.get("reasoning_len"),
            provider_slug=provider_slug,
            model_slug=model_slug,
            usage=usage or None,
        )
    if not isinstance(parsed, dict):
        return AuthorError(
            label=target.label,
            error=f"fork author returned {type(parsed).__name__}, expected object",
            reason_code="fork_author.shape_invalid", raw_llm_response=raw,
            wall_ms=wall_ms,
            latency_ms=call_metrics.get("latency_ms"),
            finish_reason=call_metrics.get("finish_reason"),
            content_len=call_metrics.get("content_len"),
            reasoning_len=call_metrics.get("reasoning_len"),
            provider_slug=provider_slug,
            model_slug=model_slug,
            usage=usage or None,
        )

    return _coerce_packet_response(
        target=target, parsed=parsed, raw=raw,
        provider_slug=provider_slug, model_slug=model_slug,
        usage=usage,
        wall_ms=wall_ms,
        latency_ms=call_metrics.get("latency_ms"),
        finish_reason=call_metrics.get("finish_reason"),
        content_len=call_metrics.get("content_len"),
        reasoning_len=call_metrics.get("reasoning_len"),
    )


def fork_author_packets(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    synthesis: PlanSynthesis,
    conn: Any,
    concurrency: int = _DEFAULT_FORK_CONCURRENCY,
    hydrate_env: Any | None = None,
    llm_overrides: dict[str, Any] | None = None,
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

    import time

    successes: list[AuthoredPacket] = []
    failures: list[AuthorError] = []

    fork_started_ns = time.monotonic_ns()
    n_workers = max(1, min(concurrency, len(skeleton.packets)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(
                _author_one_packet,
                target=target, synthesis=synthesis,
                sandbox=sandbox, atoms=atoms,
                pinned_route=pinned_route, hydrate_env=hydrate_env,
                llm_overrides=llm_overrides,
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
    fork_wall_ms = (time.monotonic_ns() - fork_started_ns) // 1_000_000

    label_order = {p.label: i for i, p in enumerate(skeleton.packets)}
    successes.sort(key=lambda p: label_order.get(p.label, 999_999))

    return AuthoredPlan(
        packets=successes, errors=failures, notes=notes, synthesis=synthesis,
        fork_author_wall_ms=fork_wall_ms,
    )
