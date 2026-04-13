"""Adaptive parameter tuning backed by the Postgres config registry.

Adaptive parameter names are preserved for CLI/runtime callers, but the
authoritative values now live in ``platform_config`` through
``registry.config_registry``. That removes the old JSON artifact state and
keeps runtime tuning on the same control plane as the rest of config.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from registry.config_registry import get_config
from runtime.failure_classifier import FailureCategory

_log = logging.getLogger(__name__)

# Maximum proportional change per adaptation cycle (15%).
_MAX_DELTA_FRACTION = 0.15

# Minimum receipts required before adaptation kicks in.
_MIN_RECEIPTS_FOR_ADAPTATION = 10


# ---------------------------------------------------------------------------
# Parameter specification
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _ParamSpec:
    """Static metadata for one tunable parameter."""

    name: str
    default: float
    min_val: float
    max_val: float
    description: str


_PARAM_SPECS: dict[str, _ParamSpec] = {}
_CONFIG_KEYS: dict[str, str] = {
    "context_budget_ratio": "context.budget_ratio",
    "circuit_breaker_failure_threshold": "breaker.failure_threshold",
    "circuit_breaker_recovery_s": "breaker.recovery_timeout_s",
    "max_consecutive_failures": "health.max_consecutive_failures",
    "context_preview_chars": "context.preview_chars",
}


def _register(
    name: str,
    default: float,
    min_val: float,
    max_val: float,
    description: str,
) -> None:
    _PARAM_SPECS[name] = _ParamSpec(
        name=name,
        default=default,
        min_val=min_val,
        max_val=max_val,
        description=description,
    )


_register(
    "context_budget_ratio", 0.60, 0.30, 0.85,
    "Fraction of context window reserved for pipeline context.",
)
_register(
    "circuit_breaker_failure_threshold", 5, 2, 15,
    "Consecutive failures before opening the circuit breaker.",
)
_register(
    "circuit_breaker_recovery_s", 300, 30, 1800,
    "Seconds to wait in OPEN state before probing recovery.",
)
_register(
    "max_consecutive_failures", 3, 1, 10,
    "Consecutive route failures before marking unhealthy.",
)
_register(
    "context_preview_chars", 2000, 500, 5000,
    "Max chars kept in upstream context previews.",
)


# ---------------------------------------------------------------------------
# History entry
# ---------------------------------------------------------------------------

def _coerce_param_value(name: str, value: float) -> float | int:
    spec = _PARAM_SPECS[name]
    if isinstance(spec.default, int) and not isinstance(spec.default, bool):
        return int(round(value))
    return float(value)


def _config_category_for(name: str) -> str:
    if name in {"context_budget_ratio", "context_preview_chars"}:
        return "context"
    return "routing"


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class AdaptiveParameterStore:
    """Thin facade over Postgres-backed platform config."""

    def __init__(self) -> None:
        self._config = get_config()

    # -- public API -----------------------------------------------------------

    def get_param(self, name: str) -> float | int:
        """Return the authoritative current value for *name*."""
        spec = _PARAM_SPECS.get(name)
        if spec is None:
            raise KeyError(f"unknown adaptive parameter: {name}")
        return _coerce_param_value(
            name,
            float(self._config.get_float(_CONFIG_KEYS[name])),
        )

    def all_params(self) -> dict[str, float]:
        """Return a snapshot of all current parameter values."""
        return {name: self.get_param(name) for name in _PARAM_SPECS}

    def all_params_detail(self) -> dict[str, Any]:
        """Return all params with metadata (bounds, history length)."""
        result: dict[str, Any] = {}
        for name, spec in _PARAM_SPECS.items():
            result[name] = {
                "value": self.get_param(name),
                "default": spec.default,
                "min": spec.min_val,
                "max": spec.max_val,
                "description": spec.description,
                "history_count": 0,
            }
        return result

    def set_param(self, name: str, value: float, *, reason: str = "manual") -> float:
        """Set a parameter, clamping to bounds.  Returns the clamped value."""
        spec = _PARAM_SPECS.get(name)
        if spec is None:
            raise KeyError(f"unknown adaptive parameter: {name}")
        clamped = max(spec.min_val, min(spec.max_val, float(value)))
        stored = _coerce_param_value(name, clamped)
        description = spec.description if reason == "manual" else f"{spec.description} [{reason}]"
        self._config.set(
            _CONFIG_KEYS[name],
            stored,
            category=_config_category_for(name),
            description=description,
        )
        return clamped

    def reset(self) -> None:
        """Reset all parameters to their initial defaults."""
        for name, spec in _PARAM_SPECS.items():
            self._config.set(
                _CONFIG_KEYS[name],
                _coerce_param_value(name, float(spec.default)),
                category=_config_category_for(name),
                description=spec.description,
            )

    # -- adaptation ------------------------------------------------------------

    def adapt_from_receipts(self, receipts_dir: str | None = None) -> dict[str, Any]:
        """Run one adaptation cycle from recent workflow receipts.

        Loads the last 200 receipts, computes provider-level statistics,
        and nudges each parameter toward a value implied by the data.
        Changes are dampened to at most 15% per cycle.

        Returns a summary dict describing what changed.
        """
        from .receipt_store import list_receipt_payloads

        del receipts_dir
        receipts = list_receipt_payloads(limit=200)
        if len(receipts) < _MIN_RECEIPTS_FOR_ADAPTATION:
            return {
                "adapted": False,
                "reason": f"too few receipts ({len(receipts)} < {_MIN_RECEIPTS_FOR_ADAPTATION})",
            }

        stats = _compute_receipt_stats(receipts)
        changes: dict[str, dict[str, Any]] = {}

        # 1. context_budget_ratio
        changes["context_budget_ratio"] = self._adapt_context_budget(stats)

        # 2. circuit_breaker_failure_threshold
        changes["circuit_breaker_failure_threshold"] = (
            self._adapt_cb_threshold(stats)
        )

        # 3. circuit_breaker_recovery_s
        changes["circuit_breaker_recovery_s"] = self._adapt_cb_recovery(stats)

        # 4. max_consecutive_failures
        changes["max_consecutive_failures"] = self._adapt_max_failures(stats)

        # 5. context_preview_chars
        changes["context_preview_chars"] = self._adapt_preview_chars(stats)

        return {"adapted": True, "changes": changes, "receipt_count": len(receipts)}

    # -- private adaptation helpers -------------------------------------------

    def _damped_adjust(
        self,
        name: str,
        target: float,
        reason: str,
    ) -> dict[str, Any]:
        """Move current value toward *target* by at most _MAX_DELTA_FRACTION.

        Returns a change record dict.
        """
        spec = _PARAM_SPECS[name]
        current = self.get_param(name)

        delta = target - current
        max_abs = abs(current) * _MAX_DELTA_FRACTION
        if max_abs < 1e-9:
            max_abs = abs(spec.default) * _MAX_DELTA_FRACTION

        if abs(delta) > max_abs:
            delta = max_abs if delta > 0 else -max_abs

        new_val = current + delta
        new_val = max(spec.min_val, min(spec.max_val, new_val))

        if abs(new_val - current) < 1e-9:
            return {"name": name, "changed": False, "value": current}

        self.set_param(name, new_val, reason=reason)
        return {
            "name": name,
            "changed": True,
            "old": round(current, 6),
            "new": round(new_val, 6),
            "target": round(target, 6),
            "reason": reason,
        }

    def _adapt_context_budget(self, stats: _ReceiptStats) -> dict[str, Any]:
        """If context-too-long failures are frequent, shrink the ratio.
        If quality is high with low token usage, allow growth.
        """
        current = self.get_param("context_budget_ratio")

        context_fail_rate = stats.context_error_rate
        if context_fail_rate > 0.10:
            # Significant context overflow -- shrink
            target = current * (1.0 - context_fail_rate)
        elif context_fail_rate < 0.02 and stats.overall_success_rate > 0.85:
            # Things are going well, nudge up slightly
            target = current * 1.05
        else:
            target = current  # no change

        return self._damped_adjust(
            "context_budget_ratio",
            target,
            f"context_error_rate={context_fail_rate:.3f}, success_rate={stats.overall_success_rate:.3f}",
        )

    def _adapt_cb_threshold(self, stats: _ReceiptStats) -> dict[str, Any]:
        """Stable providers get higher thresholds (more patience).
        Flaky providers get lower thresholds (trip faster).
        Uses the weighted-average provider success rate.
        """
        current = self.get_param("circuit_breaker_failure_threshold")
        sr = stats.overall_success_rate

        if sr > 0.95:
            # Very stable -- can afford more patience
            target = current * 1.10
        elif sr > 0.80:
            # Moderately stable -- hold steady
            target = current
        elif sr > 0.60:
            # Getting flaky -- reduce threshold
            target = current * 0.90
        else:
            # Unreliable -- trip fast
            target = current * 0.80

        return self._damped_adjust(
            "circuit_breaker_failure_threshold",
            target,
            f"overall_success_rate={sr:.3f}",
        )

    def _adapt_cb_recovery(self, stats: _ReceiptStats) -> dict[str, Any]:
        """Adjust recovery timeout toward the observed median recovery time.
        If providers recover quickly, reduce. If slowly, increase.
        """
        current = self.get_param("circuit_breaker_recovery_s")

        if stats.median_recovery_gap_s is not None and stats.median_recovery_gap_s > 0:
            # Target 1.5x the observed median gap to give a buffer
            target = stats.median_recovery_gap_s * 1.5
        else:
            target = current

        return self._damped_adjust(
            "circuit_breaker_recovery_s",
            target,
            f"median_recovery_gap_s={stats.median_recovery_gap_s}",
        )

    def _adapt_max_failures(self, stats: _ReceiptStats) -> dict[str, Any]:
        """More failures in the data => lower threshold so we skip faster.
        Fewer failures => raise threshold for more tolerance.
        """
        current = self.get_param("max_consecutive_failures")
        sr = stats.overall_success_rate

        if sr > 0.92:
            target = current * 1.08
        elif sr < 0.70:
            target = current * 0.85
        else:
            target = current

        return self._damped_adjust(
            "max_consecutive_failures",
            target,
            f"overall_success_rate={sr:.3f}",
        )

    def _adapt_preview_chars(self, stats: _ReceiptStats) -> dict[str, Any]:
        """If downstream parse failures are elevated, reduce preview size.
        If things are clean, allow growth.
        """
        current = self.get_param("context_preview_chars")

        parse_fail_rate = stats.downstream_parse_error_rate
        if parse_fail_rate > 0.08:
            target = current * 0.90
        elif parse_fail_rate < 0.02 and stats.overall_success_rate > 0.85:
            target = current * 1.05
        else:
            target = current

        return self._damped_adjust(
            "context_preview_chars",
            target,
            f"parse_error_rate={parse_fail_rate:.3f}",
        )

# ---------------------------------------------------------------------------
# Receipt statistics
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _ReceiptStats:
    """Aggregated statistics from a batch of receipts."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    overall_success_rate: float = 0.0

    # Specific failure pattern rates
    context_error_rate: float = 0.0
    downstream_parse_error_rate: float = 0.0

    # Recovery timing
    median_recovery_gap_s: float | None = None

    # Per-provider success rates
    provider_success_rates: dict[str, float] = field(default_factory=dict)


def _compute_receipt_stats(receipts: list[dict[str, Any]]) -> _ReceiptStats:
    """Derive aggregate stats from loaded receipt dicts."""
    stats = _ReceiptStats()
    stats.total = len(receipts)
    if stats.total == 0:
        return stats

    context_errors = 0
    parse_errors = 0
    per_provider: dict[str, list[bool]] = {}
    failure_recovery_gaps: list[float] = []

    # Sort by timestamp for recovery gap analysis
    sorted_receipts = sorted(
        receipts,
        key=lambda r: r.get("finished_at", ""),
    )

    # Track per-provider failure/recovery sequences
    provider_last_failure: dict[str, str] = {}

    for r in sorted_receipts:
        status = r.get("status", "failed")
        provider = r.get("provider_slug", "unknown")
        failure_code = r.get("failure_code", "") or ""
        finished_at = r.get("finished_at", "")

        is_success = status == "succeeded"
        if is_success:
            stats.succeeded += 1
        else:
            stats.failed += 1

        per_provider.setdefault(provider, []).append(is_success)

        # Context-too-long detection
        if not is_success and _is_context_or_parse(failure_code, r, FailureCategory.CONTEXT_OVERFLOW):
            context_errors += 1

        # Downstream parse error detection
        if not is_success and _is_context_or_parse(failure_code, r, FailureCategory.PARSE_ERROR):
            parse_errors += 1

        # Recovery gap: time between a failure and the next success for
        # the same provider
        if not is_success:
            provider_last_failure[provider] = finished_at
        elif provider in provider_last_failure and finished_at:
            gap = _iso_gap_seconds(provider_last_failure[provider], finished_at)
            if gap is not None and gap > 0:
                failure_recovery_gaps.append(gap)
            provider_last_failure.pop(provider, None)

    stats.overall_success_rate = stats.succeeded / stats.total if stats.total else 0.0
    stats.context_error_rate = context_errors / stats.total if stats.total else 0.0
    stats.downstream_parse_error_rate = parse_errors / stats.total if stats.total else 0.0

    for prov, outcomes in per_provider.items():
        s = sum(1 for o in outcomes if o)
        stats.provider_success_rates[prov] = s / len(outcomes) if outcomes else 0.0

    if failure_recovery_gaps:
        failure_recovery_gaps.sort()
        mid = len(failure_recovery_gaps) // 2
        stats.median_recovery_gap_s = failure_recovery_gaps[mid]

    return stats


def _is_context_or_parse(
    failure_code: str,
    receipt: dict[str, Any],
    expected_category: "FailureCategory",
) -> bool:
    """Check if a failure matches *expected_category* via the central classifier."""
    from runtime.failure_classifier import classify_failure, FailureCategory  # noqa: F811

    outputs = {
        "stderr": receipt.get("stderr", ""),
        "reason_code": receipt.get("reason_code", ""),
    }
    classification = classify_failure(failure_code, outputs=outputs)
    return classification.category == expected_category


def _iso_gap_seconds(iso_a: str, iso_b: str) -> float | None:
    """Return seconds between two ISO timestamps, or None on parse failure."""
    try:
        a = datetime.fromisoformat(iso_a)
        b = datetime.fromisoformat(iso_b)
        return (b - a).total_seconds()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_STORE: AdaptiveParameterStore | None = None
_STORE_LOCK = threading.Lock()


def get_adaptive_params() -> AdaptiveParameterStore:
    """Return (or create) the module-level singleton."""
    global _STORE
    if _STORE is None:
        with _STORE_LOCK:
            if _STORE is None:
                _STORE = AdaptiveParameterStore()
    return _STORE


__all__ = [
    "AdaptiveParameterStore",
    "get_adaptive_params",
]
