"""Compose-Experiment Matrix — N parallel `compose_plan_via_llm` runs with knob variation.

Operator-facing primitive that turns the ad-hoc "fire concurrent compose calls
in a thread pool" pattern into a first-class platform tool. Reuses the
``ThreadPoolExecutor`` fan-out pattern from ``runtime/plan_fork_author.py``
(which proves out parallel LLM call coordination inside one compose run); this
module stacks the same idea one level up — running N FULL compose calls in
parallel with different ``llm_overrides`` so the operator can compare model /
temperature / max_tokens knob settings side-by-side.

Architecture per ``decision.architecture_policy.product_architecture.llm-first-infrastructure-trust-compiler-engine``:
the trust compiler must surface the right tools at the moment of action.
Choosing between LLM knob configurations IS that moment of action; the
platform exposes this primitive natively rather than forcing operators to
hand-roll thread pools in scratch scripts.
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from runtime.compose_plan_via_llm import ComposeViaLLMResult, compose_plan_via_llm

logger = logging.getLogger(__name__)


# Default cap on the experiment fan-out. 8 children × 5-way internal fork-out
# = ~40 outstanding HTTP requests at peak — already aggressive against
# Together's per-account rate limit. Operators can raise via the
# ``max_workers`` argument when their tier supports it.
_DEFAULT_MAX_WORKERS = 8

# Cap on configs per experiment to backstop foot-guns. Easy to override; just
# a soft warning below this and a hard refusal above ``_HARD_CAP_CONFIGS``.
_SOFT_CAP_CONFIGS = 32
_HARD_CAP_CONFIGS = 128


@dataclass(frozen=True)
class ComposeExperimentRun:
    """One config run — its knob set, the compose result, wall-time, error.

    ``ok`` / ``error`` are exposed at the run level for quick filtering;
    deeper data lives on ``result`` (when present) — its
    ``compose_provenance`` block carries usage / validation / synthesis
    state from the underlying compose call.

    ``child_receipt_id`` is the receipt UUID for this child's
    ``compose-plan-via-llm`` gateway dispatch; populated when the
    gateway path was used. ``fallback_reason`` is set when the runner
    fell back to a direct function call (skipping receipt + event); it
    flags CQRS coverage gaps for an operator to investigate.
    """

    config_index: int
    config: dict[str, Any]
    ok: bool
    wall_seconds: float
    result: ComposeViaLLMResult | None
    error: str | None = None
    child_receipt_id: str | None = None
    fallback_reason: str | None = None
    # Final flat-dict knobs the LLM call actually saw, after layering
    # base_task_type's row + overrides. Reported on the summary so the
    # operator can see "what was actually tested" not just "what was
    # requested." None when resolution failed before dispatch.
    resolved_overrides: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        compose_dict = self.result.to_dict() if self.result is not None else None
        return {
            "config_index": self.config_index,
            "config": dict(self.config),
            "resolved_overrides": (
                dict(self.resolved_overrides) if self.resolved_overrides else None
            ),
            "ok": self.ok,
            "wall_seconds": round(self.wall_seconds, 3),
            "error": self.error,
            "compose": compose_dict,
            "child_receipt_id": self.child_receipt_id,
            "fallback_reason": self.fallback_reason,
        }

    def summary_row(self, *, work_task_type: str = "compose_plan_via_llm") -> dict[str, Any]:
        """Comprehensive per-leg trace record. Designed to be lossless —
        every measurable dimension surfaces here so analyses don't have
        to re-parse the underlying ComposeViaLLMResult.

        ``work_task_type`` selects which quality scorer runs against the
        result. Default is ``compose_plan_via_llm``; future experiment
        runners can register other scorers via ``register_quality_scorer``.

        Aligns with the operator's system-of-record thesis: every
        experiment leg becomes a labeled training-style row of
        (config, resolved knobs, tokens, latency, finish_reason,
        structural quality signals, validation findings, success/failure).
        """
        row: dict[str, Any] = {
            "config_index": self.config_index,
            "config": dict(self.config),
            "resolved_overrides": (
                dict(self.resolved_overrides) if self.resolved_overrides else None
            ),
            "ok": self.ok,
            "wall_seconds": round(self.wall_seconds, 3),
            "error": self.error,
            "child_receipt_id": self.child_receipt_id,
            "fallback_reason": self.fallback_reason,
        }
        if self.result is None:
            return row

        result = self.result
        synthesis = result.synthesis

        # Per-call rollup (sums across synthesis + every fork-out)
        usage = result.usage_summary()
        row["totals"] = {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "cached_tokens": usage.get("cached_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "calls": usage.get("calls"),
            "cache_hit_ratio": usage.get("cache_hit_ratio"),
        }

        # Synthesis (one LLM call) — full observability set
        row["synthesis"] = (
            {
                "provider_slug": synthesis.provider_slug,
                "model_slug": synthesis.model_slug,
                "prompt_tokens": synthesis.usage.get("prompt_tokens"),
                "completion_tokens": synthesis.usage.get("completion_tokens"),
                "cached_tokens": synthesis.usage.get("cached_tokens"),
                "total_tokens": synthesis.usage.get("total_tokens"),
                "wall_ms": synthesis.wall_ms,
                "latency_ms": synthesis.latency_ms,
                "finish_reason": synthesis.finish_reason,
                "content_len": synthesis.content_len,
                "reasoning_len": synthesis.reasoning_len,
                "seed_count": len(synthesis.packet_seeds or []),
            }
            if synthesis is not None
            else None
        )

        # Fork-out fan: per-packet success rows + per-packet failure rows
        authored = result.authored
        row["fork_author"] = {
            "n_attempted": len(authored.packets) + len(authored.errors),
            "n_succeeded": len(authored.packets),
            "n_failed": len(authored.errors),
            "wall_ms": authored.fork_author_wall_ms,
        }
        row["per_packet"] = [
            {
                "label": p.label,
                "stage": p.stage,
                "provider_slug": p.provider_slug,
                "model_slug": p.model_slug,
                "prompt_tokens": p.usage.get("prompt_tokens"),
                "completion_tokens": p.usage.get("completion_tokens"),
                "cached_tokens": p.usage.get("cached_tokens"),
                "total_tokens": p.usage.get("total_tokens"),
                "wall_ms": p.wall_ms,
                "latency_ms": p.latency_ms,
                "finish_reason": p.finish_reason,
                "content_len": p.content_len,
                "reasoning_len": p.reasoning_len,
            }
            for p in authored.packets
        ]
        row["per_packet_failures"] = [
            {
                "label": e.label,
                "reason_code": e.reason_code,
                "error": e.error,
                "provider_slug": e.provider_slug,
                "model_slug": e.model_slug,
                "wall_ms": e.wall_ms,
                "latency_ms": e.latency_ms,
                "finish_reason": e.finish_reason,
                "content_len": e.content_len,
                "reasoning_len": e.reasoning_len,
                "usage": dict(e.usage) if e.usage else None,
                # Truncate raw_llm_response in the summary; full text lives
                # on AuthorError.to_dict() in the deeper compose payload.
                "raw_llm_response_preview": (
                    (e.raw_llm_response or "")[:160] if e.raw_llm_response else None
                ),
            }
            for e in authored.errors
        ]

        # Validation — full per-finding detail (not just a count)
        validation = result.validation
        findings = list(getattr(validation, "findings", []) or [])
        findings_by_severity: dict[str, int] = {}
        for f in findings:
            sev = str(getattr(f, "severity", "info") or "info")
            findings_by_severity[sev] = findings_by_severity.get(sev, 0) + 1
        row["validation"] = {
            "passed": getattr(validation, "passed", None),
            "every_required_filled": getattr(validation, "every_required_filled", None),
            "no_forbidden_placeholders": getattr(validation, "no_forbidden_placeholders", None),
            "no_workspace_root": getattr(validation, "no_workspace_root", None),
            "no_dropped_floors": getattr(validation, "no_dropped_floors", None),
            "every_required_gate_scaffolded": getattr(
                validation, "every_required_gate_scaffolded", None
            ),
            "findings_count": len(findings),
            "findings_by_severity": findings_by_severity,
            "findings": [
                f.to_dict() if hasattr(f, "to_dict") else dict(f) for f in findings
            ],
        }

        # Quality signals — dispatched per work_task_type. compose runs
        # get the compose-specific scorer (pills, match rate, validation
        # accuracy); future work types register their own.
        plan_packets = list(result.plan_packets or [])
        row["compose_ok"] = result.ok
        row["reason_code"] = result.reason_code
        row["packet_count"] = len(plan_packets)
        scorer = _QUALITY_SCORERS.get(work_task_type, _universal_quality_signals)
        try:
            quality = scorer(result, plan_packets)
        except Exception as exc:
            # Quality scoring is best-effort; never fail the leg over it.
            quality = {
                "scorer_error": f"{type(exc).__name__}: {exc}",
                **_universal_quality_signals(result, plan_packets),
            }
        row["quality"] = quality
        row["work_task_type"] = work_task_type

        # Cost projection (None when no price card is registered for the
        # provider/model). Computed off the rollup totals only — per-call
        # breakdown is in synthesis.* and per_packet[*].
        row["cost_usd"] = _project_cost(result)

        return row


@dataclass(frozen=True)
class ComposeExperimentReport:
    """Rolled-up output of a matrix run."""

    intent: str
    runs: list[ComposeExperimentRun]
    ranked_indices: list[int]
    total_wall_seconds: float
    notes: list[str] = field(default_factory=list)
    # Names which work type's quality scorer was used for this matrix.
    # All legs in one matrix share a work_task_type — comparing legs
    # only makes sense when they're scored on the same dimensions.
    work_task_type: str = "compose_plan_via_llm"

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "work_task_type": self.work_task_type,
            "runs": [run.to_dict() for run in self.runs],
            "ranked_indices": list(self.ranked_indices),
            "total_wall_seconds": round(self.total_wall_seconds, 3),
            "notes": list(self.notes),
            "summary_table": [
                run.summary_row(work_task_type=self.work_task_type)
                for run in self.runs
            ],
            "ranked_summary": [
                self.runs[idx].summary_row(work_task_type=self.work_task_type)
                for idx in self.ranked_indices
            ],
        }

    def winner(self) -> ComposeExperimentRun | None:
        """Top-ranked run — first ``ok=True`` config by wall-time, else None."""
        for idx in self.ranked_indices:
            run = self.runs[idx]
            if run.ok and run.result is not None and run.result.ok:
                return run
        return None


_KNOWN_OVERRIDE_KEYS = {"provider_slug", "model_slug", "temperature", "max_tokens"}


def _coerce_override_dict(raw: Any, *, index: int, ctx: str) -> dict[str, Any]:
    """Coerce + validate an override dict (or top-level legacy config dict)."""
    if not isinstance(raw, dict):
        raise ValueError(
            f"compose_experiment: config[{index}] {ctx} must be a dict, got {type(raw).__name__}"
        )
    out: dict[str, Any] = dict(raw)
    if "temperature" in out:
        try:
            out["temperature"] = float(out["temperature"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"compose_experiment: config[{index}] {ctx}.temperature not a number: {raw['temperature']!r}"
            ) from exc
    if "max_tokens" in out:
        try:
            out["max_tokens"] = int(out["max_tokens"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"compose_experiment: config[{index}] {ctx}.max_tokens not an integer: {raw['max_tokens']!r}"
            ) from exc
        if out["max_tokens"] <= 0:
            raise ValueError(
                f"compose_experiment: config[{index}] {ctx}.max_tokens must be positive"
            )
    for slug_field in ("provider_slug", "model_slug"):
        if slug_field in out and not isinstance(out[slug_field], str):
            raise ValueError(
                f"compose_experiment: config[{index}] {ctx}.{slug_field} must be a string"
            )
    return out


def _normalize_config(raw: Any, *, index: int) -> dict[str, Any]:
    """Normalize one experiment config into a unified shape.

    Two input shapes are accepted:

    1. **Base + overrides (preferred)**::

           {"base_task_type": "plan_synthesis", "overrides": {"temperature": 0.7}}

       The runner looks up the rank-1 row for ``plan_synthesis`` in
       ``task_type_routing`` (provider, model, temperature, max_tokens
       columns added by migration 276) and layers ``overrides`` on top.

    2. **Flat dict (legacy / escape hatch)**::

           {"model_slug": "x/y", "temperature": 0.7, "max_tokens": 4096}

       Treated as a free-form ``llm_overrides`` payload — no base
       task_type, no inheritance.

    Output shape is always normalized to::

        {
          "base_task_type": str | None,
          "overrides": {provider_slug?, model_slug?, temperature?, max_tokens?},
        }

    The runner resolves base + overrides into the final
    ``llm_overrides`` payload at dispatch time so failures during base
    resolution surface as per-run errors, not validation failures.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"compose_experiment: config[{index}] must be a dict, got {type(raw).__name__}"
        )

    base_task_type = raw.get("base_task_type")
    if base_task_type is not None and not isinstance(base_task_type, str):
        raise ValueError(
            f"compose_experiment: config[{index}].base_task_type must be a string"
        )
    base_task_type = (base_task_type or "").strip() or None

    if "overrides" in raw or base_task_type is not None:
        # Base+overrides shape. Validate the overrides dict (default empty).
        overrides_raw = raw.get("overrides") or {}
        overrides = _coerce_override_dict(overrides_raw, index=index, ctx="overrides")
        # Reject any keys outside the known override set so silent typos
        # become loud errors. Unknown keys would silently disappear at
        # the call site and the operator would never know.
        unknown = set(overrides) - _KNOWN_OVERRIDE_KEYS
        if unknown:
            raise ValueError(
                f"compose_experiment: config[{index}].overrides has unknown keys {sorted(unknown)}"
            )
    else:
        # Flat-dict legacy shape — treat the whole dict as overrides with
        # no base. Forward-compat keys still preserved through coerce.
        overrides = _coerce_override_dict(raw, index=index, ctx="config")

    return {"base_task_type": base_task_type, "overrides": overrides}


def _resolve_config_to_llm_overrides(config: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve a normalized config (base_task_type + overrides) into the
    flat ``llm_overrides`` dict that ``compose_plan_via_llm`` consumes.

    Layering rules: base task_type's row config is the floor; overrides
    win on conflict. Keys whose final value is ``None`` are dropped so
    the call site falls back to its own default.
    """
    from runtime.compiler_llm import resolve_task_type_config

    base_task_type = config.get("base_task_type")
    overrides = dict(config.get("overrides") or {})

    base_config: dict[str, Any] = {}
    if base_task_type:
        resolved = resolve_task_type_config(base_task_type)
        if resolved is None:
            raise ValueError(
                f"compose_experiment: base_task_type {base_task_type!r} resolved to no rows"
            )
        # resolve_task_type_config returns NULLs as None; keep those None
        # values out of the layered dict so the call-site fallbacks fire
        # when the row didn't pin them either.
        base_config = {k: v for k, v in resolved.items() if v is not None}

    layered: dict[str, Any] = {**base_config, **overrides}
    layered = {k: v for k, v in layered.items() if v is not None}
    return layered or None


# =====================================================================
# Quality scorers — plugged per work task type.
#
# Quality signals are work-type-specific: a compose run has pills + match
# rate + validation, a future synthesize-only experiment would score on
# seed coverage, a future fork-author-only experiment would score on
# per-packet write-scope correctness. The runner dispatches to the
# registered scorer by ``work_task_type``; an experiment without a
# registered scorer falls back to ``_universal_quality_signals`` (just
# the dimensions that work regardless of work type).
# =====================================================================


_QualityScorer = Any  # callable[[result, plan_packets], dict[str, Any]]
_QUALITY_SCORERS: dict[str, _QualityScorer] = {}


def register_quality_scorer(work_task_type: str, scorer: _QualityScorer) -> None:
    """Register a quality scorer for a given work task type. Future
    experiment runners (synthesis_experiment, fork_author_experiment,
    etc.) call this at import time so the compose_experiment summary
    schema stays consistent across work types."""
    if not isinstance(work_task_type, str) or not work_task_type.strip():
        raise ValueError("register_quality_scorer: work_task_type must be a non-empty string")
    if not callable(scorer):
        raise ValueError("register_quality_scorer: scorer must be callable")
    _QUALITY_SCORERS[work_task_type.strip()] = scorer


def _universal_quality_signals(
    result: Any, plan_packets: list[dict[str, Any]]
) -> dict[str, Any]:
    """Universal signals — work whether the result is from compose,
    synthesis, fork-author, or any future work type. Acts as the floor
    so reports always have *something* even when the work type has no
    registered scorer.
    """
    distinct_stages: set[str] = set()
    chain_depth = 0
    label_to_depends: dict[str, list[str]] = {}
    packets_with_gates = 0
    for packet in plan_packets:
        if not isinstance(packet, dict):
            continue
        stage = str(packet.get("stage") or "").strip()
        if stage:
            distinct_stages.add(stage)
        label = str(packet.get("label") or "").strip()
        deps = packet.get("depends_on") or []
        if isinstance(deps, list) and label:
            label_to_depends[label] = [str(d) for d in deps if isinstance(d, str)]
        gates = packet.get("gates") or []
        if isinstance(gates, list) and gates:
            packets_with_gates += 1

    depth_cache: dict[str, int] = {}

    def _depth(label: str, seen: frozenset[str]) -> int:
        if label in depth_cache:
            return depth_cache[label]
        if label in seen:
            return 0
        deps = label_to_depends.get(label) or []
        if not deps:
            depth_cache[label] = 1
            return 1
        next_seen = seen | {label}
        d = 1 + max((_depth(dep, next_seen) for dep in deps), default=0)
        depth_cache[label] = d
        return d

    for label in label_to_depends:
        chain_depth = max(chain_depth, _depth(label, frozenset()))

    validation = getattr(result, "validation", None)
    return {
        "packet_count": len(plan_packets),
        "distinct_stages_used": sorted(distinct_stages),
        "distinct_stages_count": len(distinct_stages),
        "depends_on_chain_max_depth": chain_depth,
        "packets_with_gates": packets_with_gates,
        "validation_passed": getattr(validation, "passed", None),
    }


def _compose_plan_via_llm_quality_scorer(
    result: Any, plan_packets: list[dict[str, Any]]
) -> dict[str, Any]:
    """Quality scorer for ``compose_plan_via_llm`` work — pills, match
    rate, validation accuracy, plus the universal floor."""
    out = _universal_quality_signals(result, plan_packets)

    synthesis_seed_count = (
        len(result.synthesis.packet_seeds) if result.synthesis is not None else 0
    )

    # Pill-domain quality signals — concrete to compose, not a judge model.
    # Praxis pills are bound (recognized → data dictionary), suggested
    # (model-found candidates), proposed (per-packet new pill suggestions
    # from the model), and audit_local (per-packet triage of "was this
    # pill actually relevant to this packet" with confirmed/misattributed
    # verdicts). Counting them tells us:
    #   - did the model find pills the data dictionary backs (bound)
    #   - did it propose new pills it thinks should exist (proposed)
    #   - was its self-triage of what was relevant correct (audit verdicts)
    bound_pill_count = (
        len(getattr(getattr(result.atoms, "pills", None), "bound", []) or [])
        if result.atoms is not None else 0
    )
    # When the result came back via gateway round-trip, the atoms
    # dataclass holds empty lists but counts are stashed on
    # _reconstituted_counts. Otherwise just len() the live lists.
    _counts = getattr(result.atoms, "_reconstituted_counts", None) or {}
    suggested_pill_count = _counts.get("suggested_pills_count")
    if suggested_pill_count is None:
        suggested_pill_count = len(getattr(result.atoms, "suggested_pills", []) or [])
    proposed_pill_count = 0
    audit_confirmed = 0
    audit_misattributed = 0
    audit_other = 0
    for packet in result.authored.packets:
        proposed_pill_count += len(getattr(packet, "proposed_pills", []) or [])
        for entry in getattr(packet, "pill_audit_local", []) or []:
            verdict = ""
            if isinstance(entry, dict):
                verdict = str(entry.get("verdict") or "").strip().lower()
            if verdict == "confirmed":
                audit_confirmed += 1
            elif verdict == "misattributed":
                audit_misattributed += 1
            elif verdict:
                audit_other += 1
    audit_total = audit_confirmed + audit_misattributed + audit_other
    pill_confirmation_rate = (
        round(audit_confirmed / audit_total, 3) if audit_total else None
    )

    # Match rate — what fraction of recognized step_types/intent atoms
    # landed in the final plan? Praxis intent_recognition extracts
    # step_types from prose; synthesis is supposed to decompose those
    # into seeds; fork-out + validation produce the final packets. A leg
    # that produces 5 packets from 8 step_types has match_rate 0.625.
    step_types_count = _counts.get("step_types_count")
    if step_types_count is None:
        step_types_count = len(getattr(result.atoms, "step_types", []) or [])
    synthesis_seed_match_rate = (
        round(synthesis_seed_count / step_types_count, 3)
        if step_types_count else None
    )
    final_packet_match_rate = (
        round(len(plan_packets) / step_types_count, 3)
        if step_types_count else None
    )

    # Accuracy — validation pass + finding severity breakdown
    findings = list(getattr(result.validation, "findings", []) or [])
    findings_by_severity: dict[str, int] = {}
    for f in findings:
        sev = str(getattr(f, "severity", "info") or "info")
        findings_by_severity[sev] = findings_by_severity.get(sev, 0) + 1
    validation_error_count = findings_by_severity.get("error", 0)
    validation_warning_count = findings_by_severity.get("warning", 0)

    out.update({
        "synthesis_seed_count": synthesis_seed_count,
        "step_types_count": step_types_count,
        # Pill quality
        "bound_pill_count": bound_pill_count,
        "suggested_pill_count": suggested_pill_count,
        "proposed_pill_count": proposed_pill_count,
        "audit_confirmed_count": audit_confirmed,
        "audit_misattributed_count": audit_misattributed,
        "audit_other_count": audit_other,
        "pill_confirmation_rate": pill_confirmation_rate,
        # Match rates — how well the leg covered what intent recognition extracted
        "synthesis_seed_match_rate": synthesis_seed_match_rate,
        "final_packet_match_rate": final_packet_match_rate,
        # Accuracy detail
        "validation_error_count": validation_error_count,
        "validation_warning_count": validation_warning_count,
        # Validation flags pulled up so structural quality is self-contained
        "every_required_filled": getattr(result.validation, "every_required_filled", None),
        "no_forbidden_placeholders": getattr(result.validation, "no_forbidden_placeholders", None),
        "no_workspace_root": getattr(result.validation, "no_workspace_root", None),
        "no_dropped_floors": getattr(result.validation, "no_dropped_floors", None),
        "every_required_gate_scaffolded": getattr(
            result.validation, "every_required_gate_scaffolded", None
        ),
    })
    return out


# Auto-register the compose scorer on import.
register_quality_scorer("compose_plan_via_llm", _compose_plan_via_llm_quality_scorer)


def _price_card_cache() -> dict[tuple[str, str], dict[str, float]]:
    """Lazy cache of (provider_slug, model_slug) → {input_per_m, output_per_m}
    pulled from ``provider_model_candidates.default_parameters.pricing``."""
    cached: dict[tuple[str, str], dict[str, float]] | None = getattr(
        _price_card_cache, "_cache", None
    )
    if cached is not None:
        return cached
    cached = {}
    try:
        from storage.postgres.connection import (
            SyncPostgresConnection,
            get_workflow_pool,
        )

        pool = get_workflow_pool()
        pg = SyncPostgresConnection(pool)
        rows = pg.fetch(
            """
            SELECT provider_slug, model_slug, default_parameters
              FROM provider_model_candidates
             WHERE status = 'active'
            """
        )
        for r in rows or []:
            provider = str(r["provider_slug"] if "provider_slug" in r else r.get("provider_slug") or "").strip()
            model = str(r["model_slug"] if "model_slug" in r else r.get("model_slug") or "").strip()
            if not provider or not model:
                continue
            params = r["default_parameters"] if "default_parameters" in r else r.get("default_parameters")
            if isinstance(params, str):
                try:
                    import json
                    params = json.loads(params)
                except Exception:
                    params = {}
            pricing = (params or {}).get("pricing") if isinstance(params, dict) else None
            if not isinstance(pricing, dict):
                continue
            try:
                input_per_m = float(pricing.get("input_per_m_tokens"))
                output_per_m = float(pricing.get("output_per_m_tokens"))
            except (TypeError, ValueError):
                continue
            cached[(provider, model)] = {
                "input_per_m": input_per_m,
                "output_per_m": output_per_m,
            }
    except Exception as exc:
        # Price card is best-effort — failure here just means cost_usd
        # comes back None on the summary, not that the experiment fails.
        logger.debug("price card cache load failed: %s", exc)
    setattr(_price_card_cache, "_cache", cached)
    return cached


def _project_cost(result: Any) -> dict[str, Any] | None:
    """Project USD cost for the leg from per-call usage × price card.

    Returns ``None`` when no price card is registered for any of the
    providers used in this leg. Otherwise returns
    ``{synthesis_usd, fork_author_usd, total_usd, missing_price_cards}``.
    """
    cards = _price_card_cache()
    if not cards:
        return None

    def _usd(provider: str, model: str, prompt: int, completion: int) -> float | None:
        card = cards.get((provider, model))
        if not card:
            return None
        return (
            (prompt / 1_000_000) * card["input_per_m"]
            + (completion / 1_000_000) * card["output_per_m"]
        )

    missing: list[str] = []
    synthesis_usd: float | None = None
    if result.synthesis is not None:
        prov = str(result.synthesis.provider_slug or "")
        mod = str(result.synthesis.model_slug or "")
        synthesis_usd = _usd(
            prov, mod,
            int(result.synthesis.usage.get("prompt_tokens") or 0),
            int(result.synthesis.usage.get("completion_tokens") or 0),
        )
        if synthesis_usd is None and prov and mod:
            missing.append(f"{prov}/{mod}")

    fork_usd_total = 0.0
    fork_seen = False
    for packet in result.authored.packets:
        prov = str(packet.provider_slug or "")
        mod = str(packet.model_slug or "")
        cost = _usd(
            prov, mod,
            int(packet.usage.get("prompt_tokens") or 0),
            int(packet.usage.get("completion_tokens") or 0),
        )
        if cost is None:
            if prov and mod:
                missing.append(f"{prov}/{mod}")
            continue
        fork_usd_total += cost
        fork_seen = True

    if synthesis_usd is None and not fork_seen:
        return None

    total = (synthesis_usd or 0.0) + fork_usd_total
    return {
        "synthesis_usd": round(synthesis_usd, 6) if synthesis_usd is not None else None,
        "fork_author_usd": round(fork_usd_total, 6) if fork_seen else None,
        "total_usd": round(total, 6),
        "missing_price_cards": sorted(set(missing)) or None,
    }


def _rank(runs: list[ComposeExperimentRun]) -> list[int]:
    """Sort: successful compose runs first by wall-time asc; failures sink
    to the bottom by wall-time asc among themselves."""
    indexed = list(enumerate(runs))

    def sort_key(pair: tuple[int, ComposeExperimentRun]) -> tuple[int, float]:
        idx, run = pair
        compose_ok = bool(run.ok and run.result is not None and run.result.ok)
        return (0 if compose_ok else 1, run.wall_seconds)

    indexed.sort(key=sort_key)
    return [idx for idx, _ in indexed]


def _run_one(
    intent: str,
    config_index: int,
    config: dict[str, Any],
    *,
    subsystems: Any,
    plan_name: str | None,
    concurrency: int,
    hydrate_env: Any | None,
) -> ComposeExperimentRun:
    """One worker. Dispatches via the operation gateway so each child
    compose call produces its own ``plan.composed`` receipt + event for
    CQRS replay parity with single-compose calls. Gateway handles
    receipt insertion + event emission.

    When the gateway path fails (binding-resolution issue, missing
    operation, etc.), falls back to a direct ``compose_plan_via_llm``
    call so the matrix run still produces useful output. The fall-back
    path is logged on the run's ``notes`` so an operator can spot
    receipt-less children quickly.
    """
    started = time.monotonic()
    child_receipt_id: str | None = None
    fallback_reason: str | None = None
    # Resolve base_task_type + overrides into a flat llm_overrides dict
    # BEFORE dispatch so a failed resolution surfaces as a clean per-run
    # error rather than a runtime exception inside compose.
    try:
        resolved_overrides = _resolve_config_to_llm_overrides(config)
    except Exception as exc:
        return ComposeExperimentRun(
            config_index=config_index,
            config=config,
            ok=False,
            wall_seconds=time.monotonic() - started,
            result=None,
            error=f"{type(exc).__name__}: {exc}",
            resolved_overrides=None,
        )
    try:
        from runtime.operation_catalog_gateway import (
            execute_operation_from_subsystems,
        )
        try:
            gateway_result = execute_operation_from_subsystems(
                subsystems,
                operation_name="compose_plan_via_llm",
                payload={
                    "intent": intent,
                    "plan_name": plan_name,
                    "concurrency": int(concurrency),
                    "llm_overrides": resolved_overrides,
                    "caller_ref": f"compose_experiment.child[{config_index}]",
                },
            )
        except Exception as gateway_exc:
            # Binding resolution / dispatch failure — fall back to a
            # direct call so the experiment still produces output.
            logger.warning(
                "child[%s] gateway dispatch failed (%s); falling back to direct call",
                config_index, gateway_exc,
            )
            fallback_reason = f"gateway: {type(gateway_exc).__name__}: {gateway_exc}"
            gateway_result = None

        if gateway_result is not None:
            # The gateway returns the handler payload + an
            # operation_receipt block when the binding is well-formed.
            # When the handler raised, the gateway returns an error
            # envelope ``{ok: False, error, error_code}`` with NO
            # operation_receipt key. Treat that as a leg failure and
            # surface the error string visibly so the operator doesn't
            # see a silent compose_ok=False with no explanation.
            if (
                isinstance(gateway_result, dict)
                and gateway_result.get("ok") is False
                and "operation_receipt" not in gateway_result
            ):
                gw_err = gateway_result.get("error") or "gateway returned ok=False"
                gw_code = gateway_result.get("error_code") or "gateway.error"
                wall = time.monotonic() - started
                return ComposeExperimentRun(
                    config_index=config_index,
                    config=config,
                    ok=False,
                    wall_seconds=wall,
                    result=None,
                    error=f"{gw_code}: {gw_err}",
                    child_receipt_id=None,
                    fallback_reason=fallback_reason,
                    resolved_overrides=resolved_overrides,
                )

            receipt_block = (
                gateway_result.get("operation_receipt")
                if isinstance(gateway_result, dict)
                else None
            )
            if isinstance(receipt_block, dict):
                child_receipt_id = receipt_block.get("receipt_id")
            # Reconstruct the underlying ComposeViaLLMResult from the
            # serialized payload so the experiment runner's downstream
            # ranking / summary logic keeps working.
            result = _reconstitute_compose_result(gateway_result)
        else:
            # Fall-back path: direct function call (no child receipt).
            conn = subsystems.get_pg_conn()
            result = compose_plan_via_llm(
                intent,
                conn=conn,
                plan_name=plan_name,
                concurrency=concurrency,
                hydrate_env=hydrate_env,
                llm_overrides=resolved_overrides,
            )

        wall = time.monotonic() - started
        return ComposeExperimentRun(
            config_index=config_index,
            config=config,
            ok=True,
            wall_seconds=wall,
            result=result,
            error=None,
            child_receipt_id=child_receipt_id,
            fallback_reason=fallback_reason,
            resolved_overrides=resolved_overrides,
        )
    except Exception as exc:
        wall = time.monotonic() - started
        return ComposeExperimentRun(
            config_index=config_index,
            config=config,
            ok=False,
            wall_seconds=wall,
            result=None,
            error=f"{type(exc).__name__}: {exc}",
            child_receipt_id=child_receipt_id,
            fallback_reason=fallback_reason,
            resolved_overrides=resolved_overrides,
        )


def _reconstitute_compose_result(gateway_result: Any) -> ComposeViaLLMResult | None:
    """The gateway returns a dict (handler payload + operation_receipt).
    Rebuild a minimal ComposeViaLLMResult so the experiment ranking +
    summary logic doesn't have to special-case dict vs dataclass.

    Returns None if the payload shape is unrecognized — callers treat
    that as a failed child.
    """
    if not isinstance(gateway_result, dict):
        return None
    # The handler returns ``ComposeViaLLMResult.to_dict()`` flattened. We
    # need the original dataclass for ranking, so re-import + rebuild
    # the minimal subset the runner reads (ok, plan_packets, synthesis,
    # validation, usage_summary). Easiest path: hydrate from dict.
    from runtime.intent_dependency import SkeletalPlan
    from runtime.intent_binding import BoundIntent
    from runtime.intent_suggestion import SuggestedAtoms
    from runtime.plan_fork_author import AuthoredPlan
    from runtime.plan_section_validator import ValidationFinding, ValidationReport
    from runtime.plan_synthesis import PacketSeed, PlanSynthesis

    try:
        atoms_dict = gateway_result.get("atoms") or {}
        skel_dict = gateway_result.get("skeleton") or {}
        synth_dict = gateway_result.get("synthesis")
        validation_dict = gateway_result.get("validation") or {}
        authored_dict = gateway_result.get("authored") or {}

        # Counts of suggested_pills and step_types are hoisted onto the
        # reconstituted atoms via private attributes so the quality scorer
        # can still access them. The dataclass's lists themselves stay
        # empty (their elements need full SuggestedPill / StepTypeSuggestion
        # construction to satisfy atoms.to_dict() downstream, and we don't
        # ship the full dataclass through the gateway).
        atoms = SuggestedAtoms(
            intent=str(atoms_dict.get("intent") or gateway_result.get("intent") or ""),
            pills=BoundIntent(intent=str(atoms_dict.get("intent") or "")),
            suggested_pills=[],
            step_types=[],
            parameters=[],
        )
        # Counts the compose quality scorer needs but can't recompute
        # from empty lists. Stash them on a private dict so we don't
        # have to bend the SuggestedAtoms dataclass shape.
        object.__setattr__(
            atoms, "_reconstituted_counts",
            {
                "suggested_pills_count": len(atoms_dict.get("suggested_pills") or []),
                "step_types_count": len(atoms_dict.get("step_types") or []),
            },
        )
        skeleton = SkeletalPlan(
            parameters=[], packets=[], notes=list(skel_dict.get("notes") or []),
            stage_contracts={}, gate_contracts={},
        )
        synthesis: PlanSynthesis | None = None
        if isinstance(synth_dict, dict):
            seeds_raw = synth_dict.get("packet_seeds") or []
            seeds = [
                PacketSeed(
                    label=str(s.get("label") or ""),
                    stage=str(s.get("stage") or "build"),
                    description=str(s.get("description") or ""),
                    depends_on=list(s.get("depends_on") or []),
                )
                for s in seeds_raw
                if isinstance(s, dict)
            ]
            synthesis = PlanSynthesis(
                packet_seeds=seeds,
                raw_response=str(synth_dict.get("raw_response") or ""),
                provider_slug=str(synth_dict.get("provider_slug") or ""),
                model_slug=str(synth_dict.get("model_slug") or ""),
                usage=dict(synth_dict.get("usage") or {}),
                notes=list(synth_dict.get("notes") or []),
                # Lossless-reconstitute new instrumentation so the summary
                # row's synthesis section never reports None for these.
                wall_ms=synth_dict.get("wall_ms"),
                latency_ms=synth_dict.get("latency_ms"),
                finish_reason=synth_dict.get("finish_reason"),
                content_len=synth_dict.get("content_len"),
                reasoning_len=synth_dict.get("reasoning_len"),
            )
        validation = ValidationReport(
            findings=[
                ValidationFinding(
                    label=str(f.get("label") or ""),
                    field=str(f.get("field") or ""),
                    severity=str(f.get("severity") or "info"),
                    code=str(f.get("code") or ""),
                    detail=str(f.get("detail") or ""),
                )
                for f in (validation_dict.get("findings") or [])
                if isinstance(f, dict)
            ],
            every_required_filled=bool(validation_dict.get("every_required_filled")),
            no_forbidden_placeholders=bool(validation_dict.get("no_forbidden_placeholders")),
            no_workspace_root=bool(validation_dict.get("no_workspace_root")),
            no_dropped_floors=bool(validation_dict.get("no_dropped_floors")),
            every_required_gate_scaffolded=bool(
                validation_dict.get("every_required_gate_scaffolded")
            ),
        )

        # Lossless-reconstitute AuthoredPlan including packets, errors,
        # per-packet observability, and fork_author_wall_ms.
        from runtime.plan_section_author import (
            AuthoredPacket as _RehydPacket,
            AuthorError as _RehydError,
        )

        def _rebuild_packet(p: dict[str, Any]) -> _RehydPacket:
            return _RehydPacket(
                label=str(p.get("label") or ""),
                stage=str(p.get("stage") or ""),
                description=str(p.get("description") or ""),
                prompt=str(p.get("prompt") or ""),
                write=list(p.get("write") or []),
                agent=str(p.get("agent") or ""),
                task_type=str(p.get("task_type") or ""),
                capabilities=list(p.get("capabilities") or []),
                consumes=list(p.get("consumes") or []),
                produces=list(p.get("produces") or []),
                depends_on=list(p.get("depends_on") or []),
                gates=[dict(g) for g in (p.get("gates") or []) if isinstance(g, dict)],
                parameters=dict(p.get("parameters") or {}),
                workdir=p.get("workdir"),
                on_failure=str(p.get("on_failure") or "abort"),
                on_success=str(p.get("on_success") or "continue"),
                timeout=int(p.get("timeout") or 300),
                budget=dict(p["budget"]) if isinstance(p.get("budget"), dict) else None,
                raw_llm_response=str(p.get("raw_llm_response") or ""),
                provider_slug=str(p.get("provider_slug") or ""),
                model_slug=str(p.get("model_slug") or ""),
                usage=dict(p.get("usage") or {}),
                proposed_pills=list(p.get("proposed_pills") or []),
                pill_audit_local=list(p.get("pill_audit_local") or []),
                wall_ms=p.get("wall_ms"),
                latency_ms=p.get("latency_ms"),
                finish_reason=p.get("finish_reason"),
                content_len=p.get("content_len"),
                reasoning_len=p.get("reasoning_len"),
            )

        def _rebuild_error(e: dict[str, Any]) -> _RehydError:
            return _RehydError(
                label=str(e.get("label") or ""),
                error=str(e.get("error") or ""),
                reason_code=str(e.get("reason_code") or ""),
                raw_llm_response=e.get("raw_llm_response"),
                wall_ms=e.get("wall_ms"),
                latency_ms=e.get("latency_ms"),
                finish_reason=e.get("finish_reason"),
                content_len=e.get("content_len"),
                reasoning_len=e.get("reasoning_len"),
                provider_slug=e.get("provider_slug"),
                model_slug=e.get("model_slug"),
                usage=dict(e["usage"]) if isinstance(e.get("usage"), dict) else None,
            )

        authored = AuthoredPlan(
            packets=[
                _rebuild_packet(p)
                for p in (authored_dict.get("packets") or [])
                if isinstance(p, dict)
            ],
            errors=[
                _rebuild_error(e)
                for e in (authored_dict.get("errors") or [])
                if isinstance(e, dict)
            ],
            notes=list(authored_dict.get("notes") or []),
            synthesis=synthesis,
            fork_author_wall_ms=authored_dict.get("fork_author_wall_ms"),
        )

        plan_packets = list(gateway_result.get("plan_packets") or [])
        return ComposeViaLLMResult(
            ok=bool(gateway_result.get("ok")),
            intent=str(gateway_result.get("intent") or ""),
            atoms=atoms,
            skeleton=skeleton,
            synthesis=synthesis,
            authored=authored,
            validation=validation,
            plan_packets=plan_packets,
            notes=list(gateway_result.get("notes") or []),
            reason_code=gateway_result.get("reason_code"),
            error=gateway_result.get("error"),
        )
    except Exception as exc:
        logger.warning("could not reconstitute ComposeViaLLMResult: %s", exc)
        return None


def run_compose_experiment(
    intent: str,
    configs: list[dict[str, Any]],
    *,
    subsystems: Any,
    plan_name: str | None = None,
    concurrency: int = 5,
    max_workers: int = _DEFAULT_MAX_WORKERS,
    hydrate_env: Any | None = None,
    work_task_type: str = "compose_plan_via_llm",
) -> ComposeExperimentReport:
    """Fire N parallel compose calls with per-call knob overrides.

    Each child compose call dispatches through
    ``operation_catalog_gateway.execute_operation_from_subsystems`` so it
    produces its own ``compose-plan-via-llm`` receipt + ``plan.composed``
    event for CQRS replay parity. The matrix run wraps that in its own
    parent receipt + ``compose.experiment.completed`` event.

    Args:
        intent: prose intent, same shape ``compose_plan_via_llm`` accepts.
        configs: list of override dicts (each: provider_slug, model_slug,
            temperature, max_tokens — all optional).
        subsystems: lazy-load subsystems container (mints DB conns,
            event log handles, etc.). Each child worker dispatches via
            the gateway with this object; subsystems mints fresh
            per-thread connections under the hood.
        plan_name: optional caller-provided plan label, forwarded to each
            compose call.
        concurrency: PER-CHILD fork-out concurrency (passed through to
            ``compose_plan_via_llm``). Lower than the default 20 because
            each parent worker already runs one compose; compounding
            stacks against the rate limit.
        max_workers: PARENT fan-out concurrency — how many compose calls
            run side-by-side. Default 8.
        hydrate_env: forwarded to ``compose_plan_via_llm``.

    Returns:
        :class:`ComposeExperimentReport` with per-config results + a
        ranked summary table. Each run carries its child receipt id when
        the gateway path was used.

    Raises:
        ValueError: configs is empty, malformed, or exceeds the hard cap.
    """
    if not isinstance(configs, list) or not configs:
        raise ValueError("compose_experiment: configs must be a non-empty list")
    if len(configs) > _HARD_CAP_CONFIGS:
        raise ValueError(
            f"compose_experiment: refusing matrix of {len(configs)} configs (hard cap {_HARD_CAP_CONFIGS})"
        )
    notes: list[str] = []
    if len(configs) > _SOFT_CAP_CONFIGS:
        notes.append(
            f"warn: matrix size {len(configs)} exceeds soft cap {_SOFT_CAP_CONFIGS}; "
            f"watch for provider rate-limit cascades"
        )

    normalized = [_normalize_config(c, index=i) for i, c in enumerate(configs)]

    workers = max(1, min(max_workers, len(normalized)))
    if workers != max_workers and len(normalized) < max_workers:
        # Common, expected case — fewer configs than worker cap. No note.
        pass
    elif workers != max_workers:
        notes.append(f"max_workers clamped to {workers}")

    started = time.monotonic()
    runs: list[ComposeExperimentRun] = [None] * len(normalized)  # type: ignore[list-item]

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _run_one,
                intent,
                i,
                cfg,
                subsystems=subsystems,
                plan_name=plan_name,
                concurrency=concurrency,
                hydrate_env=hydrate_env,
            ): i
            for i, cfg in enumerate(normalized)
        }
        for future in concurrent.futures.as_completed(futures):
            i = futures[future]
            try:
                runs[i] = future.result()
            except Exception as exc:  # pragma: no cover - defensive guard
                runs[i] = ComposeExperimentRun(
                    config_index=i,
                    config=normalized[i],
                    ok=False,
                    wall_seconds=time.monotonic() - started,
                    result=None,
                    error=f"unhandled: {type(exc).__name__}: {exc}",
                )

    # Surface CQRS coverage gap if any child fell back to a direct call
    # (no receipt). Loud signal so the operator notices a binding
    # regression rather than silently losing replay parity.
    fallback_count = sum(1 for r in runs if r.fallback_reason)
    if fallback_count:
        notes.append(
            f"cqrs.warning: {fallback_count} of {len(runs)} child compose calls "
            f"fell back to direct dispatch (no plan.composed receipt + event). "
            f"Inspect run.fallback_reason for details."
        )

    total_wall = time.monotonic() - started
    ranked = _rank(runs)

    return ComposeExperimentReport(
        intent=intent,
        runs=runs,
        ranked_indices=ranked,
        total_wall_seconds=total_wall,
        notes=notes,
        work_task_type=work_task_type,
    )


__all__ = [
    "ComposeExperimentReport",
    "ComposeExperimentRun",
    "run_compose_experiment",
]
