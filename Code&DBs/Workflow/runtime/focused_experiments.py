"""Focused single-skill experiment runners — decomposition-only and pill-match-only.

The full ``compose_experiment`` runner exercises the entire compose pipeline
(synthesis + N parallel fork-author calls + validation) per leg. That's
expensive when we want to compare models on a SPECIFIC skill — e.g. "which
model decomposes structure best" or "which model audits pills most
precisely". This module builds focused experiments that fire ONE LLM call
per leg, dedicated to one skill, with a quality scorer matching that skill.

Both runners reuse compose_experiment's parallel ThreadPoolExecutor pattern
(forking for time + cost win — same intent shape, multiple models in
parallel), and produce ComposeExperimentReport-shaped output so the
existing ranking + summary tooling keeps working.
"""
from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from runtime.compose_experiment import (
    ComposeExperimentReport,
    ComposeExperimentRun,
    _normalize_config,
    _project_cost,
    _rank,
    _resolve_config_to_llm_overrides,
    register_quality_scorer,
)

logger = logging.getLogger(__name__)


# =====================================================================
# Decomposition-only experiment
# =====================================================================

def _run_synthesis_only_leg(
    intent: str,
    config_index: int,
    config: dict[str, Any],
    *,
    atoms: Any,
    skeleton: Any,
    sandbox: Any,
    recognition: Any,
) -> ComposeExperimentRun:
    """Fire ONE synthesis LLM call per leg. atoms/skeleton/sandbox/recognition
    are pre-computed (serial, conn-using) and shared across all legs so the
    worker thread doesn't touch the DB and avoids the asyncpg-bridge race."""
    started = time.monotonic()
    try:
        resolved = _resolve_config_to_llm_overrides(config)
    except Exception as exc:
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=False,
            wall_seconds=time.monotonic() - started, result=None,
            error=f"{type(exc).__name__}: {exc}",
            resolved_overrides=None,
        )

    try:
        from runtime.plan_synthesis import (
            _build_synthesis_prompt, _call_synthesis_llm, PlanSynthesis, PacketSeed,
        )
        import json as _json

        # Build prompt + fire LLM directly. No DB calls in this worker.
        prompt = _build_synthesis_prompt(
            atoms=atoms, skeleton=skeleton, sandbox=sandbox, recognition=recognition,
        )
        t0 = time.monotonic_ns()
        raw, provider_slug, model_slug, usage, call_metrics = _call_synthesis_llm(
            prompt, llm_overrides=resolved,
        )
        wall_ms = (time.monotonic_ns() - t0) // 1_000_000

        # Parse seeds (mirror what synthesize_plan_statement does)
        notes: list[str] = []
        seeds: list[PacketSeed] = []
        try:
            text = raw.strip()
            if text.startswith("```"):
                text = text.split("```", 2)[1]
                if text.startswith("json"): text = text[4:]
                text = text.rsplit("```", 1)[0]
            parsed = _json.loads(text.strip())
            if isinstance(parsed, dict):
                for entry in (parsed.get("packet_seeds") or [])[:20]:
                    if not isinstance(entry, dict): continue
                    label = str(entry.get("label") or "").strip()
                    stage = str(entry.get("stage") or "").strip().lower()
                    desc = str(entry.get("description") or "").strip()
                    deps = entry.get("depends_on") or []
                    if not (label and stage and desc): continue
                    if stage not in {"research", "review", "build"}: continue
                    seeds.append(PacketSeed(
                        label=label, stage=stage, description=desc,
                        depends_on=[str(d) for d in deps if isinstance(d, str)],
                    ))
        except Exception:
            notes.append("synthesis returned non-JSON")

        synthesis = PlanSynthesis(
            packet_seeds=seeds, raw_response=raw,
            provider_slug=provider_slug, model_slug=model_slug,
            usage=usage, notes=notes,
            wall_ms=wall_ms,
            latency_ms=call_metrics.get("latency_ms"),
            finish_reason=call_metrics.get("finish_reason"),
            content_len=call_metrics.get("content_len"),
            reasoning_len=call_metrics.get("reasoning_len"),
        )

        wall = time.monotonic() - started
        # Stuff the synthesis into a slim "result" object so downstream
        # quality-scoring + summary code can read .synthesis / .atoms / etc.
        # without the full ComposeViaLLMResult.
        result = _SynthesisOnlyResult(
            ok=bool(synthesis.packet_seeds),
            intent=intent,
            atoms=atoms,
            synthesis=synthesis,
        )
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=True,
            wall_seconds=wall, result=result, error=None,
            resolved_overrides=resolved,
        )
    except Exception as exc:
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=False,
            wall_seconds=time.monotonic() - started, result=None,
            error=f"{type(exc).__name__}: {exc}",
            resolved_overrides=resolved,
        )


@dataclass(frozen=True)
class _SynthesisOnlyResult:
    """Slim shim: enough shape for the synthesis-only quality scorer + summary."""
    ok: bool
    intent: str
    atoms: Any
    synthesis: Any
    # Empty placeholders so summary_row reading these fields doesn't crash:
    authored: Any = field(default=None)
    validation: Any = field(default=None)
    plan_packets: list[Any] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    reason_code: str | None = None
    error: str | None = None
    skeleton: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok, "intent": self.intent,
            "synthesis": self.synthesis.to_dict() if self.synthesis else None,
            "atoms": self.atoms.to_dict() if hasattr(self.atoms, "to_dict") else None,
        }

    def usage_summary(self) -> dict[str, Any]:
        if self.synthesis is None: return {}
        u = dict(self.synthesis.usage)
        u["calls"] = 1
        if u.get("prompt_tokens"):
            u["cache_hit_ratio"] = round(
                (u.get("cached_tokens") or 0) / u["prompt_tokens"], 3)
        return u


def _decomposition_quality_scorer(result: Any, plan_packets: list[dict[str, Any]]) -> dict[str, Any]:
    """Quality scorer for ``plan_synthesis_only``.

    The output we have is just synthesis.packet_seeds (label, stage,
    description, depends_on per seed). Score:
    - seed_count vs target step_types
    - distinct stages used
    - depends_on chain depth (1+ means SOMETHING depends on something)
    - recognized step_types present in seed labels (semantic match)
    """
    syn = getattr(result, "synthesis", None)
    seeds = list(getattr(syn, "packet_seeds", []) or []) if syn else []

    distinct_stages = sorted({str(s.stage or "").strip() for s in seeds if s.stage})
    label_to_deps = {str(s.label): list(s.depends_on or []) for s in seeds}

    depth_cache: dict[str, int] = {}
    def _depth(label: str, seen: frozenset[str]) -> int:
        if label in depth_cache: return depth_cache[label]
        if label in seen: return 0
        deps = label_to_deps.get(label) or []
        if not deps:
            depth_cache[label] = 1; return 1
        d = 1 + max((_depth(dep, seen | {label}) for dep in deps), default=0)
        depth_cache[label] = d; return d
    chain = max((_depth(L, frozenset()) for L in label_to_deps), default=0)

    # Step-types from atoms (Praxis intent recognition)
    atoms = getattr(result, "atoms", None)
    step_types = list(getattr(atoms, "step_types", []) or []) if atoms else []
    step_types_count = len(step_types)

    return {
        "seed_count": len(seeds),
        "step_types_count": step_types_count,
        "distinct_stages_used": distinct_stages,
        "distinct_stages_count": len(distinct_stages),
        "depends_on_chain_max_depth": chain,
        "seeds_with_dependencies": sum(1 for s in seeds if s.depends_on),
        "seed_labels": [s.label for s in seeds],
        "seed_stages": [s.stage for s in seeds],
    }


# Auto-register the synthesis-only quality scorer.
register_quality_scorer("plan_synthesis_only", _decomposition_quality_scorer)


def run_decomposition_experiment(
    intent: str,
    configs: list[dict[str, Any]],
    *,
    subsystems: Any,
    max_workers: int = 8,
) -> ComposeExperimentReport:
    """Fire N parallel synthesis-only calls. Each leg = 1 LLM call.

    atoms / skeleton / sandbox / recognition are pre-computed serially
    once per intent (uses DB conn) and SHARED across all parallel legs.
    Worker threads do not touch the DB — they just build prompts and
    fire LLM calls — so the asyncpg sync-bridge doesn't race.
    """
    if not configs:
        raise ValueError("decomposition_experiment: configs must be non-empty")
    normalized = [_normalize_config(c, index=i) for i, c in enumerate(configs)]
    workers = max(1, min(max_workers, len(normalized)))
    started = time.monotonic()

    # Pre-compute the per-intent context ONCE — shared across all legs.
    from runtime.intent_suggestion import suggest_plan_atoms
    from runtime.intent_dependency import synthesize_skeleton
    from runtime.plan_section_author import build_section_sandbox
    conn = subsystems.get_pg_conn()
    atoms = suggest_plan_atoms(intent, conn=conn)
    skeleton = synthesize_skeleton(atoms, conn=conn)
    sandbox = build_section_sandbox(conn)
    recognition = None
    if (atoms.intent or "").strip():
        try:
            from runtime.intent_recognition import recognize_intent
            recognition = recognize_intent(atoms.intent, conn=conn)
        except Exception:
            recognition = None

    runs: list[ComposeExperimentRun] = [None] * len(normalized)  # type: ignore[list-item]

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _run_synthesis_only_leg, intent, i, cfg,
                atoms=atoms, skeleton=skeleton, sandbox=sandbox, recognition=recognition,
            ): i
            for i, cfg in enumerate(normalized)
        }
        for fut in concurrent.futures.as_completed(futures):
            i = futures[fut]
            try:
                runs[i] = fut.result()
            except Exception as exc:
                runs[i] = ComposeExperimentRun(
                    config_index=i, config=normalized[i], ok=False,
                    wall_seconds=time.monotonic() - started, result=None,
                    error=f"unhandled: {type(exc).__name__}: {exc}",
                )
    return ComposeExperimentReport(
        intent=intent, runs=runs, ranked_indices=_rank(runs),
        total_wall_seconds=time.monotonic() - started, notes=[],
        work_task_type="plan_synthesis_only",
    )


# =====================================================================
# Pill-match-only experiment
# =====================================================================

def _run_pill_match_only_leg(
    intent: str,
    config_index: int,
    config: dict[str, Any],
    *,
    candidate_pills: list[Any],
) -> ComposeExperimentRun:
    """Fire ONE pill-scope-filter LLM call per leg. candidate_pills are
    pre-computed (serial) and shared across all legs."""
    started = time.monotonic()
    try:
        resolved = _resolve_config_to_llm_overrides(config)
    except Exception as exc:
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=False,
            wall_seconds=time.monotonic() - started, result=None,
            error=f"{type(exc).__name__}: {exc}", resolved_overrides=None,
        )

    try:

        # Build a focused prompt: just pill triage, no decomposition.
        import json as _json
        pills_view = [
            {"ref": p.ref, "field_kind": p.field_kind, "label": p.label, "summary": p.summary}
            for p in candidate_pills
        ]
        prompt = (
            f"INTENT:\n{intent}\n\n"
            f"CANDIDATE PILLS (surfaced by token-overlap; many likely irrelevant):\n"
            f"{_json.dumps(pills_view, indent=2)}\n\n"
            f"TASK: For each candidate pill above, decide whether it is RELEVANT to the intent. "
            f"Be skeptical — many will be unrelated.\n\n"
            f"OUTPUT: a JSON object {{\"verdicts\": [{{\"ref\": \"<pill_ref>\", "
            f"\"verdict\": \"confirmed|misattributed\", \"reason\": \"<one sentence>\"}}, ...]}}. "
            f"Include EVERY candidate pill exactly once. No fences, no prose outside JSON."
        )

        from adapters.keychain import resolve_secret
        from adapters.llm_client import LLMRequest, call_llm
        from registry.provider_execution_registry import (
            resolve_api_endpoint, resolve_api_key_env_vars, resolve_api_protocol_family,
        )

        provider = (resolved or {}).get("provider_slug") or "openrouter"
        model = (resolved or {}).get("model_slug")
        if not model:
            raise RuntimeError("pill_match_only: model_slug required in config")
        endpoint = resolve_api_endpoint(provider, model)
        if not endpoint:
            raise RuntimeError(f"no endpoint for {provider}/{model}")
        proto = resolve_api_protocol_family(provider) or "openai_chat_completions"
        env = dict(__import__('os').environ)
        api_key = None
        for var in resolve_api_key_env_vars(provider):
            cand = resolve_secret(var, env=env)
            if cand and cand.strip(): api_key = cand.strip(); break
        if not api_key:
            raise RuntimeError(f"no API key for {provider}")

        max_t = (resolved or {}).get("max_tokens") or 4096
        temp = (resolved or {}).get("temperature")
        kwargs = dict(
            endpoint_uri=str(endpoint), api_key=api_key,
            provider_slug=provider, model_slug=model,
            messages=({"role": "user", "content": prompt},),
            protocol_family=str(proto),
            timeout_seconds=300, retry_attempts=0,
            max_tokens=int(max_t),
        )
        if temp is not None: kwargs["temperature"] = float(temp)

        # Honour OpenAI-style reasoning effort if specified. OpenRouter's
        # unified schema is `reasoning: {"effort": "low|medium|high"}`,
        # which it translates to provider-specific params. We pass it
        # through verbatim via extra_body so any provider-specific
        # equivalent (Anthropic thinking, etc.) can use the same hook.
        reasoning_effort = (resolved or {}).get("reasoning_effort")
        extra_body_override = (resolved or {}).get("extra_body")
        merged_extra: dict[str, Any] = {}
        if reasoning_effort is not None:
            merged_extra["reasoning"] = {"effort": str(reasoning_effort)}
        if isinstance(extra_body_override, dict):
            merged_extra.update(extra_body_override)
        if merged_extra:
            kwargs["extra_body"] = merged_extra

        req = LLMRequest(**kwargs)
        t0 = time.monotonic_ns()
        resp = call_llm(req)
        wall_call = (time.monotonic_ns() - t0) // 1_000_000

        # Parse verdicts
        raw = (resp.content or "").strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"): raw = raw[4:]
            raw = raw.rsplit("```", 1)[0]
        try:
            parsed = _json.loads(raw.strip())
        except Exception as exc:
            return ComposeExperimentRun(
                config_index=config_index, config=config, ok=True,
                wall_seconds=time.monotonic() - started, result=None,
                error=f"parse_failed: {type(exc).__name__}: {str(exc)[:100]}",
                resolved_overrides=resolved,
            )

        verdicts = parsed.get("verdicts") or [] if isinstance(parsed, dict) else []
        result = _PillMatchResult(
            ok=True, intent=intent, candidate_count=len(candidate_pills),
            verdicts=verdicts, usage=dict(resp.usage or {}),
            provider_slug=provider, model_slug=model,
            wall_ms=wall_call, latency_ms=int(resp.latency_ms or 0),
            finish_reason=str((resp.raw_response.get("choices") or [{}])[0].get("finish_reason") or ""),
            content_len=len(resp.content or ""),
        )
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=True,
            wall_seconds=time.monotonic() - started, result=result, error=None,
            resolved_overrides=resolved,
        )
    except Exception as exc:
        return ComposeExperimentRun(
            config_index=config_index, config=config, ok=False,
            wall_seconds=time.monotonic() - started, result=None,
            error=f"{type(exc).__name__}: {exc}",
            resolved_overrides=resolved,
        )


@dataclass(frozen=True)
class _PillMatchResult:
    ok: bool
    intent: str
    candidate_count: int
    verdicts: list[dict[str, Any]]
    usage: dict[str, int]
    provider_slug: str
    model_slug: str
    wall_ms: int
    latency_ms: int
    finish_reason: str
    content_len: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok, "intent": self.intent,
            "candidate_count": self.candidate_count,
            "verdicts": list(self.verdicts), "usage": dict(self.usage),
            "provider_slug": self.provider_slug, "model_slug": self.model_slug,
            "wall_ms": self.wall_ms, "latency_ms": self.latency_ms,
            "finish_reason": self.finish_reason, "content_len": self.content_len,
        }

    def usage_summary(self) -> dict[str, Any]:
        u = dict(self.usage); u["calls"] = 1
        if u.get("prompt_tokens"):
            u["cache_hit_ratio"] = round((u.get("cached_tokens") or 0) / u["prompt_tokens"], 3)
        return u


def _pill_match_quality_scorer(result: Any, plan_packets: list[dict[str, Any]]) -> dict[str, Any]:
    """Quality scorer for ``plan_pill_match_only``.

    Score on the verdicts list — confirmed/misattributed counts, plus a
    coverage signal (did the model verdict every candidate pill).
    """
    verdicts = list(getattr(result, "verdicts", []) or [])
    candidate_count = getattr(result, "candidate_count", 0) or 0
    confirmed = sum(1 for v in verdicts if isinstance(v, dict) and str(v.get("verdict") or "").strip().lower() == "confirmed")
    misattributed = sum(1 for v in verdicts if isinstance(v, dict) and str(v.get("verdict") or "").strip().lower() == "misattributed")
    other = max(0, len(verdicts) - confirmed - misattributed)
    coverage = (len(verdicts) / candidate_count) if candidate_count else None
    rate = (confirmed / (confirmed + misattributed)) if (confirmed + misattributed) else None

    return {
        "candidate_count": candidate_count,
        "verdicts_count": len(verdicts),
        "coverage": round(coverage, 3) if coverage is not None else None,
        "confirmed_count": confirmed,
        "misattributed_count": misattributed,
        "other_count": other,
        "confirmation_rate": round(rate, 3) if rate is not None else None,
        "rejection_rate": round((misattributed / len(verdicts)), 3) if verdicts else None,
    }


register_quality_scorer("plan_pill_match_only", _pill_match_quality_scorer)


def run_pill_match_experiment(
    intent: str,
    configs: list[dict[str, Any]],
    *,
    subsystems: Any,
    max_workers: int = 8,
) -> ComposeExperimentReport:
    """Fire N parallel pill-match-only calls. candidate_pills are
    pre-computed serially once per intent and shared across all legs."""
    if not configs:
        raise ValueError("pill_match_experiment: configs must be non-empty")
    normalized = [_normalize_config(c, index=i) for i, c in enumerate(configs)]
    workers = max(1, min(max_workers, len(normalized)))
    started = time.monotonic()

    # Pre-compute candidate pills ONCE — shared across all legs.
    from runtime.intent_suggestion import suggest_plan_atoms
    conn = subsystems.get_pg_conn()
    atoms = suggest_plan_atoms(intent, conn=conn)
    candidate_pills = list(atoms.suggested_pills or [])

    runs: list[ComposeExperimentRun] = [None] * len(normalized)  # type: ignore[list-item]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _run_pill_match_only_leg, intent, i, cfg,
                candidate_pills=candidate_pills,
            ): i
            for i, cfg in enumerate(normalized)
        }
        for fut in concurrent.futures.as_completed(futures):
            i = futures[fut]
            try:
                runs[i] = fut.result()
            except Exception as exc:
                runs[i] = ComposeExperimentRun(
                    config_index=i, config=normalized[i], ok=False,
                    wall_seconds=time.monotonic() - started, result=None,
                    error=f"unhandled: {type(exc).__name__}: {exc}",
                )
    return ComposeExperimentReport(
        intent=intent, runs=runs, ranked_indices=_rank(runs),
        total_wall_seconds=time.monotonic() - started, notes=[],
        work_task_type="plan_pill_match_only",
    )


__all__ = [
    "run_decomposition_experiment",
    "run_pill_match_experiment",
]
