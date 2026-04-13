"""Capability-based model routing.

Instead of using tier (frontier/mid/economy) as a proxy, jobs declare what
capabilities they need and the router picks the cheapest model that has
demonstrated the highest fitness for those capabilities in historical receipts.

Capability taxonomy:
  mechanical_edit  — find/replace, formatting, simple transforms
  code_generation  — write new code from requirements
  code_review      — review existing code for issues
  architecture     — system design, multi-component planning
  analysis         — data analysis, scoring, evaluation
  creative         — writing, outreach drafts, communication
  research         — information gathering, summarization
  debug            — diagnose failures, trace issues

Fitness score formula:
  success_rate * 100 - (avg_cost_usd * 10) + (1000 / max(avg_latency_ms, 1))

This rewards success, penalises cost, and rewards speed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .auto_router import RouteDecision, resolve_route, _utc_now
from .cost_tracker import _extract_cost, _safe_float, _safe_int
from . import receipt_store

if TYPE_CHECKING:
    from .route_outcomes import RouteOutcomeStore

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known capability names
# ---------------------------------------------------------------------------

class TaskCapability:
    """Namespace for the known capability slug constants."""

    mechanical_edit = "mechanical_edit"
    code_generation = "code_generation"
    code_review = "code_review"
    architecture = "architecture"
    analysis = "analysis"
    creative = "creative"
    research = "research"
    debug = "debug"

    _ALL: tuple[str, ...] = (
        "mechanical_edit",
        "code_generation",
        "code_review",
        "architecture",
        "analysis",
        "creative",
        "research",
        "debug",
    )

    @classmethod
    def all(cls) -> tuple[str, ...]:
        return cls._ALL

    @classmethod
    def is_known(cls, cap: str) -> bool:
        return cap in cls._ALL


# ---------------------------------------------------------------------------
# Keyword → capability mapping (used for inference from labels/prompts)
# ---------------------------------------------------------------------------

_KEYWORD_MAP: list[tuple[tuple[str, ...], str]] = [
    # debug before code_generation — "fix" overlaps both, debug wins when
    # "debug", "diagnose", "trace" appear first
    (("debug", "diagnose", "trace"), TaskCapability.debug),
    (("edit", "fix", "rename", "format", "refactor"), TaskCapability.mechanical_edit),
    (("build", "create", "implement", "generate"), TaskCapability.code_generation),
    # "write" is shared between code_generation and creative; context wins
    (("review", "audit", "check", "lint", "inspect"), TaskCapability.code_review),
    (("design", "architect", "plan", "schema", "structure"), TaskCapability.architecture),
    (("score", "evaluate", "analyze", "analyse", "rank", "assess"), TaskCapability.analysis),
    (("draft", "email", "outreach", "compose", "copywrite"), TaskCapability.creative),
    (("discover", "search", "find", "research", "summarize", "summarise", "gather"), TaskCapability.research),
]

# "write" is ambiguous — resolve by additional context clues
_WRITE_CODE_CONTEXT = ("function", "class", "module", "test", "script", "code", "implement")
_WRITE_CREATIVE_CONTEXT = ("email", "message", "outreach", "blog", "post", "content")


def infer_capabilities(prompt: str, *, label: str | None = None) -> list[str]:
    """Infer task capabilities from prompt text and optional label.

    Uses keyword matching. Returns a list of capability slugs with no
    duplicates, preserving match order (most-specific first).

    Falls back to ["code_generation"] when no keywords match and the prompt
    is non-empty, which is a reasonable default for most LLM dispatch tasks.
    """
    combined = " ".join(filter(None, [label or "", prompt])).lower()

    found: list[str] = []
    seen: set[str] = set()

    for keywords, capability in _KEYWORD_MAP:
        if capability in seen:
            continue
        for kw in keywords:
            if kw in combined:
                found.append(capability)
                seen.add(capability)
                break

    # Handle "write" ambiguity
    if "write" in combined and TaskCapability.code_generation not in seen and TaskCapability.creative not in seen:
        is_code = any(kw in combined for kw in _WRITE_CODE_CONTEXT)
        is_creative = any(kw in combined for kw in _WRITE_CREATIVE_CONTEXT)
        if is_code and not is_creative:
            found.append(TaskCapability.code_generation)
            seen.add(TaskCapability.code_generation)
        elif is_creative and not is_code:
            found.append(TaskCapability.creative)
            seen.add(TaskCapability.creative)
        else:
            # Ambiguous — prefer code_generation for developer-facing platform
            found.append(TaskCapability.code_generation)
            seen.add(TaskCapability.code_generation)

    if not found and prompt.strip():
        found.append(TaskCapability.code_generation)

    return found


# ---------------------------------------------------------------------------
# ModelFitness
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ModelFitness:
    """Aggregate fitness for one (provider, model, capability) triple."""

    provider_slug: str
    model_slug: str
    capability: str
    success_rate: float
    sample_count: int
    avg_latency_ms: int
    avg_cost_usd: float
    fitness_score: float  # computed: success_rate*100 - avg_cost*10 + 1000/max(latency,1)

    @classmethod
    def compute(
        cls,
        provider_slug: str,
        model_slug: str,
        capability: str,
        *,
        success_rate: float,
        sample_count: int,
        avg_latency_ms: int,
        avg_cost_usd: float,
    ) -> "ModelFitness":
        fitness_score = (
            success_rate * 100.0
            - (avg_cost_usd * 10.0)
            + (1000.0 / max(avg_latency_ms, 1))
        )
        return cls(
            provider_slug=provider_slug,
            model_slug=model_slug,
            capability=capability,
            success_rate=success_rate,
            sample_count=sample_count,
            avg_latency_ms=avg_latency_ms,
            avg_cost_usd=avg_cost_usd,
            fitness_score=round(fitness_score, 4),
        )


# ---------------------------------------------------------------------------
# compute_model_fitness
# ---------------------------------------------------------------------------

def _load_quality_index() -> dict[tuple[str, str, str], list[float]]:
    """Load quality signals from the capability tracker.

    Returns a mapping of (provider_slug, model_slug, capability) → list of
    quality scores.  Each score is in [0.0, 1.0].
    """
    from .capability_feedback import get_capability_tracker

    tracker = get_capability_tracker()
    outcomes = tracker.all_outcomes()

    index: dict[tuple[str, str, str], list[float]] = {}
    for outcome in outcomes:
        for cap in outcome.inferred_capabilities:
            quality = outcome.output_quality_signals.get(cap)
            if quality is None:
                continue
            key = (outcome.provider_slug, outcome.model_slug, cap)
            index.setdefault(key, []).append(quality)
    return index


def compute_model_fitness(
    receipts_dir: str | None = None,
) -> dict[tuple[str, str, str], ModelFitness]:
    """Load all workflow receipts and compute per-capability fitness scores.

    Groups by (provider_slug, model_slug, capability) where capability is
    inferred from the receipt's ``label`` and/or ``capabilities`` fields.

    Returns a mapping of (provider_slug, model_slug, capability) → ModelFitness.
    When a receipt has explicit ``capabilities`` stored, those are used directly.
    Otherwise, capabilities are inferred from the ``label`` field via keyword
    matching.

    Quality weighting
    -----------------
    When capability quality signals are available (from CapabilityTracker),
    the effective success contribution of each dispatch is weighted by the
    quality score for that capability.  A dispatch that succeeded but had
    quality=0.3 contributes 0.3 to the success numerator instead of 1.0,
    while quality=0.9 contributes 0.9.  Dispatches with no quality signal
    use the raw succeeded flag (0.0 or 1.0) as a fallback, preserving
    full backward compatibility.
    """
    records = receipt_store.list_receipts(limit=100_000)

    # Pre-load quality signals for weighting
    quality_index = _load_quality_index()

    # Accumulator: key → list of (quality_weighted_success, latency_ms, cost_usd)
    # quality_weighted_success is a float in [0.0, 1.0]
    accum: dict[tuple[str, str, str], list[tuple[float, int, float]]] = {}

    for rec in records:
        receipt = rec.to_dict()

        provider = receipt.get("provider_slug") or "unknown"
        model = receipt.get("model_slug") or "unknown"
        status = receipt.get("status", "")
        succeeded = status == "succeeded"

        latency_ms = _safe_int(receipt.get("latency_ms"))

        # Extract cost
        outputs = receipt.get("outputs") or {}
        cost_usd, _, _ = _extract_cost(outputs)
        if cost_usd == 0.0:
            cost_usd = _safe_float(receipt.get("total_cost_usd"))

        # Determine capabilities for this receipt
        caps: list[str] = []

        # 1. Explicit capabilities stored in the receipt (if the platform wrote them)
        stored_caps = receipt.get("capabilities")
        if isinstance(stored_caps, list) and stored_caps:
            caps = [c for c in stored_caps if TaskCapability.is_known(c)]

        # 2. Infer from label / prompt if not explicitly stored
        if not caps:
            label = receipt.get("label") or ""
            # completion text can also help but is expensive to scan; use label only
            caps = infer_capabilities("", label=label) if label else []

        # 3. If still nothing, skip this receipt for capability fitness
        #    (it contributes to the leaderboard but not capability routing)
        if not caps:
            continue

        run_id = receipt.get("run_id", "")
        for cap in caps:
            key = (provider, model, cap)
            # Quality weighting: use quality score if available, else raw success flag
            quality_scores = quality_index.get(key, [])
            if quality_scores:
                # Use average quality as the weight — better than point-in-time lookup
                avg_quality = sum(quality_scores) / len(quality_scores)
                effective_success = avg_quality if succeeded else 0.0
            else:
                effective_success = 1.0 if succeeded else 0.0
            accum.setdefault(key, []).append((effective_success, latency_ms, cost_usd))

    # Aggregate
    result: dict[tuple[str, str, str], ModelFitness] = {}
    for (provider, model, cap), records in accum.items():
        n = len(records)
        # success_rate now reflects quality-weighted success
        success_rate = sum(qs for qs, _, _ in records) / n if n else 0.0
        avg_latency = int(sum(lat for _, lat, _ in records) / n) if n else 0
        avg_cost = sum(c for _, _, c in records) / n if n else 0.0

        result[(provider, model, cap)] = ModelFitness.compute(
            provider_slug=provider,
            model_slug=model,
            capability=cap,
            success_rate=success_rate,
            sample_count=n,
            avg_latency_ms=avg_latency,
            avg_cost_usd=avg_cost,
        )

    # BUG-2603B020: apply capability outcome quality multiplier
    from .capability_feedback import get_capability_tracker

    tracker = get_capability_tracker()
    tracker_matrix = tracker.model_capability_matrix()

    quality_matrix: dict[tuple[str, str], dict[str, float]] = {}
    for model_key, capability_data in tracker_matrix.items():
        if not isinstance(model_key, str) or "/" not in model_key:
            continue
        if not isinstance(capability_data, dict):
            continue
        provider, model = model_key.split("/", 1)
        model_quality = quality_matrix.setdefault((provider, model), {})
        for capability, metrics in capability_data.items():
            if isinstance(metrics, dict):
                quality_signal = _safe_float(metrics.get("avg_quality"))
            else:
                quality_signal = _safe_float(metrics)
            model_quality[capability] = max(0.0, min(1.0, quality_signal))

    adjusted_result: dict[tuple[str, str, str], ModelFitness] = {}
    for key, fitness in result.items():
        quality_signal = quality_matrix.get(
            (fitness.provider_slug, fitness.model_slug),
            {},
        ).get(fitness.capability, 0.5)
        blended_weight = 0.7 + (0.3 * max(0.0, min(1.0, quality_signal)))
        adjusted_result[key] = ModelFitness(
            provider_slug=fitness.provider_slug,
            model_slug=fitness.model_slug,
            capability=fitness.capability,
            success_rate=fitness.success_rate,
            sample_count=fitness.sample_count,
            avg_latency_ms=fitness.avg_latency_ms,
            avg_cost_usd=fitness.avg_cost_usd,
            fitness_score=round(fitness.fitness_score * blended_weight, 4),
        )
    return adjusted_result


# ---------------------------------------------------------------------------
# Tier cost ordering (for tie-breaking — cheaper tiers preferred)
# ---------------------------------------------------------------------------

_TIER_COST_RANK: dict[str, int] = {
    "economy": 0,
    "mid": 1,
    "frontier": 2,
}


def _tier_for(provider_slug: str, model_slug: str) -> str:
    """Return the tier for a known candidate, or 'frontier' as default."""
    from .auto_router import _CANDIDATES
    for c in _CANDIDATES:
        if c.provider_slug == provider_slug and c.model_slug == model_slug:
            return c.tier
    return "frontier"


# ---------------------------------------------------------------------------
# resolve_by_capability
# ---------------------------------------------------------------------------

def resolve_by_capability(
    capabilities: list[str],
    *,
    route_outcomes: "RouteOutcomeStore | None" = None,
    min_samples: int = 3,
) -> RouteDecision:
    """Find the model with the highest fitness across all requested capabilities.

    Algorithm:
      1. Load fitness scores.
      2. For each (provider, model) pair that has data for ALL requested
         capabilities AND has >= min_samples for each: compute the minimum
         fitness score across capabilities (the bottleneck capability drives
         the decision).
      3. If no model has enough samples, fall back to tier-based routing
         using "mid" as the default tier.
      4. Among candidates with scores within 10% of the best score, prefer
         the cheapest tier.

    Returns a RouteDecision compatible with the existing dispatch pipeline.
    """
    fitness_map = compute_model_fitness()

    # Group by (provider, model) — check coverage across all requested caps
    coverage: dict[tuple[str, str], dict[str, ModelFitness]] = {}
    for cap in capabilities:
        for (provider, model, c), mf in fitness_map.items():
            if c != cap:
                continue
            if mf.sample_count < min_samples:
                continue
            coverage.setdefault((provider, model), {})[cap] = mf

    # Find models that have data for ALL capabilities
    all_caps_set = set(capabilities)
    qualified: list[tuple[tuple[str, str], float, str]] = []  # (key, min_fitness, reason)

    for (provider, model), cap_data in coverage.items():
        if set(cap_data.keys()) < all_caps_set:
            # Missing coverage for at least one capability — skip
            continue
        min_fitness = min(mf.fitness_score for mf in cap_data.values())
        tier = _tier_for(provider, model)
        qualified.append(((provider, model), min_fitness, tier))

    if not qualified:
        # No model has enough samples — fall back to tier routing
        _log.debug(
            "capability_router: no model has >= %d samples for %s, "
            "falling back to tier=mid",
            min_samples,
            capabilities,
        )
        return resolve_route(
            "mid",
            route_outcomes=route_outcomes,
        )

    # Sort: best fitness DESC, then cheaper tier preferred
    qualified.sort(
        key=lambda x: (-x[1], _TIER_COST_RANK.get(x[2], 99))
    )

    best_fitness = qualified[0][1]
    # Within 10% of best score → prefer cheapest tier
    threshold = best_fitness * 0.90 if best_fitness > 0 else best_fitness - abs(best_fitness) * 0.10

    within_threshold = [
        item for item in qualified if item[1] >= threshold
    ]

    # Re-sort within_threshold by tier cost ASC (cheapest first), then fitness DESC
    within_threshold.sort(
        key=lambda x: (_TIER_COST_RANK.get(x[2], 99), -x[1])
    )

    picked_key, picked_fitness, picked_tier = within_threshold[0]
    provider_slug, model_slug = picked_key

    cap_str = ", ".join(capabilities)
    reason = (
        f"capability routing: fitness={picked_fitness:.2f} for [{cap_str}] "
        f"(tier={picked_tier}, {len(qualified)} qualified models)"
    )

    return RouteDecision(
        provider_slug=provider_slug,
        model_slug=model_slug,
        tier=picked_tier,
        reason=reason,
        candidates_considered=len(qualified),
        candidates_healthy=len(qualified),
        decided_at=_utc_now(),
    )


# ---------------------------------------------------------------------------
# Fitness table formatting (for the CLI)
# ---------------------------------------------------------------------------

def format_fitness_table(
    fitness_map: dict[tuple[str, str, str], ModelFitness],
    *,
    capability_filter: str | None = None,
) -> str:
    """Render fitness scores as a fixed-width table, grouped by capability."""

    if not fitness_map:
        return "No capability fitness data found (no labeled receipts yet)."

    # Filter by capability if requested
    items = list(fitness_map.values())
    if capability_filter:
        items = [m for m in items if m.capability == capability_filter]
        if not items:
            return f"No fitness data for capability: {capability_filter}"

    # Group by capability
    by_cap: dict[str, list[ModelFitness]] = {}
    for mf in items:
        by_cap.setdefault(mf.capability, []).append(mf)

    lines: list[str] = []
    col_w = 28
    header = (
        f"{'provider/model':<{col_w}} {'capability':<18} {'samples':>7} "
        f"{'pass%':>6} {'avg_ms':>7} {'avg_cost':>10} {'fitness':>9}"
    )
    sep = "-" * len(header)

    for cap in sorted(by_cap.keys()):
        lines.append(f"\n[{cap}]")
        lines.append(sep)
        lines.append(header)
        lines.append(sep)

        entries = sorted(by_cap[cap], key=lambda m: -m.fitness_score)
        for m in entries:
            label = f"{m.provider_slug}/{m.model_slug}"
            lines.append(
                f"{label:<{col_w}} {m.capability:<18} {m.sample_count:>7} "
                f"{m.success_rate * 100:>5.0f}% {m.avg_latency_ms:>7} "
                f"${m.avg_cost_usd:>9.4f} {m.fitness_score:>9.2f}"
            )

    return "\n".join(lines).lstrip("\n")
