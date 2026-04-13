"""Trend detection for workflow provider metrics.

Detects when providers are degrading, failure rates are accelerating, and costs
are trending up using basic statistical baselines (rolling medians and threshold
comparisons, no ML).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from .receipt_store import list_receipt_payloads
from .cost_tracker import _extract_cost, _safe_float, _safe_int


class TrendDirection(str, Enum):
    """Direction of a detected trend."""

    IMPROVING = "improving"
    STABLE = "stable"
    DEGRADING = "degrading"
    ACCELERATING = "accelerating"


@dataclass(frozen=True)
class Trend:
    """A detected trend in provider or platform metrics."""

    metric_name: str
    provider_slug: str | None
    direction: TrendDirection
    baseline_value: float
    current_value: float
    change_pct: float
    sample_count: int
    severity: str  # "info", "warning", "critical"


def _rolling_median(values: list[float]) -> float:
    """Return the median of a list of floats."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n % 2 == 1:
        return sorted_vals[n // 2]
    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0


def _receipt_timestamp(receipt: dict[str, Any]) -> datetime | None:
    """Extract UTC datetime from receipt finished_at field."""
    finished_at = receipt.get("finished_at")
    if not finished_at:
        return None
    try:
        return datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TrendDetector:
    """Detects trends in workflow provider metrics."""

    def __init__(self):
        self.min_samples = 5

    def detect_from_receipts(
        self,
        receipts_dir: str | None = None,
    ) -> list[Trend]:
        """Detect all trends from DB-backed receipt authority.

        Returns a list of Trend objects, ordered by severity and metric name.
        """
        del receipts_dir
        receipts = list_receipt_payloads(limit=10_000)

        if len(receipts) < self.min_samples:
            return []

        # Organize receipts by (provider, model) and timestamp
        now = _utc_now()
        cutoff_7d = now - timedelta(days=7)
        cutoff_6d = now - timedelta(days=6)
        cutoff_24h = now - timedelta(hours=24)

        # Group receipts by provider
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in receipts:
            ts = _receipt_timestamp(r)
            if ts is None or ts < cutoff_7d:
                continue
            provider = r.get("provider_slug", "unknown")
            grouped.setdefault(provider, []).append(r)

        trends: list[Trend] = []

        # Detect per-provider trends
        for provider, provider_receipts in grouped.items():
            if len(provider_receipts) < self.min_samples:
                continue

            # 1. Pass rate trend (last 5 days vs current day)
            pass_rate_trend = self._detect_pass_rate_trend(provider, provider_receipts, now)
            if pass_rate_trend:
                trends.append(pass_rate_trend)

            # 2. Failure acceleration (last 24h vs prior 6-day avg)
            failure_accel = self._detect_failure_acceleration(
                provider, provider_receipts, now
            )
            if failure_accel:
                trends.append(failure_accel)

            # 3. Cost trend (last 7 days vs prior 7 days)
            cost_trend = self._detect_cost_trend(provider, provider_receipts, now)
            if cost_trend:
                trends.append(cost_trend)

            # 4. Latency trend (recent p50 vs historical p50)
            latency_trend = self._detect_latency_trend(provider, provider_receipts, now)
            if latency_trend:
                trends.append(latency_trend)

        # Sort by severity (critical → warning → info) and metric name
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        trends.sort(
            key=lambda t: (severity_order.get(t.severity, 99), t.metric_name)
        )

        return trends

    def _detect_pass_rate_trend(
        self,
        provider: str,
        receipts: list[dict[str, Any]],
        now: datetime,
    ) -> Trend | None:
        """Detect pass rate degradation/improvement.

        Baseline: median pass rate of last 5 days
        Signal: current day (last 24h)
        >1.5× baseline failures = degrading, <0.5× = improving
        """
        cutoff_24h = now - timedelta(hours=24)
        cutoff_5d = now - timedelta(days=5)

        current_day = [r for r in receipts if _receipt_timestamp(r) is not None and _receipt_timestamp(r) >= cutoff_24h]
        baseline_5d = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None
            and cutoff_5d <= _receipt_timestamp(r) < cutoff_24h
        ]

        if len(current_day) < self.min_samples or len(baseline_5d) < self.min_samples:
            return None

        # Calculate failure rates
        baseline_fails = sum(
            1 for r in baseline_5d if r.get("status") != "succeeded"
        )
        baseline_fail_rate = baseline_fails / len(baseline_5d)

        current_fails = sum(1 for r in current_day if r.get("status") != "succeeded")
        current_fail_rate = current_fails / len(current_day)

        if baseline_fail_rate == 0.0:
            if current_fail_rate > 0.0:
                direction = TrendDirection.DEGRADING
            else:
                return None
        else:
            ratio = current_fail_rate / baseline_fail_rate
            if ratio > 1.5:
                direction = TrendDirection.DEGRADING
            elif ratio < 0.5:
                direction = TrendDirection.IMPROVING
            else:
                return None

        # Severity: degrading = warning, improving = info
        severity = "warning" if direction == TrendDirection.DEGRADING else "info"

        change_pct = (
            (current_fail_rate - baseline_fail_rate) / baseline_fail_rate * 100
            if baseline_fail_rate > 0
            else (100 if current_fail_rate > 0 else 0)
        )

        return Trend(
            metric_name="pass_rate",
            provider_slug=provider,
            direction=direction,
            baseline_value=baseline_fail_rate,
            current_value=current_fail_rate,
            change_pct=change_pct,
            sample_count=len(current_day),
            severity=severity,
        )

    def _detect_failure_acceleration(
        self,
        provider: str,
        receipts: list[dict[str, Any]],
        now: datetime,
    ) -> Trend | None:
        """Detect failure acceleration.

        Compare last 24h failure count to prior 6-day average.
        >2× = accelerating
        """
        cutoff_24h = now - timedelta(hours=24)
        cutoff_7d = now - timedelta(days=7)

        current_day = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None and _receipt_timestamp(r) >= cutoff_24h
        ]
        prior_6d = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None
            and cutoff_7d <= _receipt_timestamp(r) < cutoff_24h
        ]

        if len(current_day) < self.min_samples or len(prior_6d) < self.min_samples:
            return None

        current_failures = sum(1 for r in current_day if r.get("status") != "succeeded")
        prior_failures = sum(1 for r in prior_6d if r.get("status") != "succeeded")

        if prior_failures == 0:
            if current_failures == 0:
                return None
            # accelerating from 0 is critical
            direction = TrendDirection.ACCELERATING
            change_pct = 100.0
        else:
            avg_prior_daily = prior_failures / 6.0
            if current_failures > avg_prior_daily * 2.0:
                direction = TrendDirection.ACCELERATING
                change_pct = (current_failures - avg_prior_daily) / avg_prior_daily * 100
            else:
                return None

        return Trend(
            metric_name="failure_acceleration",
            provider_slug=provider,
            direction=direction,
            baseline_value=prior_failures / 6.0 if prior_failures > 0 else 0.0,
            current_value=float(current_failures),
            change_pct=change_pct,
            sample_count=len(current_day),
            severity="critical",
        )

    def _detect_cost_trend(
        self,
        provider: str,
        receipts: list[dict[str, Any]],
        now: datetime,
    ) -> Trend | None:
        """Detect cost increase trend.

        Compare last 7 days avg cost per dispatch to prior 7 days.
        >1.3× = increasing
        """
        cutoff_7d = now - timedelta(days=7)
        cutoff_14d = now - timedelta(days=14)

        recent_7d = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None and _receipt_timestamp(r) >= cutoff_7d
        ]
        prior_7d = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None
            and cutoff_14d <= _receipt_timestamp(r) < cutoff_7d
        ]

        if len(recent_7d) < self.min_samples or len(prior_7d) < self.min_samples:
            return None

        # Extract costs
        recent_costs = []
        for r in recent_7d:
            outputs = r.get("outputs") or {}
            cost_usd, _, _ = _extract_cost(outputs)
            if cost_usd == 0.0:
                cost_usd = _safe_float(r.get("total_cost_usd"))
            if cost_usd > 0.0:
                recent_costs.append(cost_usd)

        prior_costs = []
        for r in prior_7d:
            outputs = r.get("outputs") or {}
            cost_usd, _, _ = _extract_cost(outputs)
            if cost_usd == 0.0:
                cost_usd = _safe_float(r.get("total_cost_usd"))
            if cost_usd > 0.0:
                prior_costs.append(cost_usd)

        if len(recent_costs) < self.min_samples or len(prior_costs) < self.min_samples:
            return None

        recent_avg = sum(recent_costs) / len(recent_costs)
        prior_avg = sum(prior_costs) / len(prior_costs)

        if prior_avg == 0.0:
            if recent_avg == 0.0:
                return None
            direction = TrendDirection.DEGRADING
            change_pct = 100.0
        else:
            ratio = recent_avg / prior_avg
            if ratio > 1.3:
                direction = TrendDirection.DEGRADING
                change_pct = (ratio - 1.0) * 100
            elif ratio < 0.7:
                direction = TrendDirection.IMPROVING
                change_pct = (1.0 - ratio) * -100
            else:
                return None

        severity = "warning" if direction == TrendDirection.DEGRADING else "info"

        return Trend(
            metric_name="cost_trend",
            provider_slug=provider,
            direction=direction,
            baseline_value=prior_avg,
            current_value=recent_avg,
            change_pct=change_pct,
            sample_count=len(recent_7d),
            severity=severity,
        )

    def _detect_latency_trend(
        self,
        provider: str,
        receipts: list[dict[str, Any]],
        now: datetime,
    ) -> Trend | None:
        """Detect latency degradation.

        Compare recent p50 latency to historical p50 (all time).
        >1.5× = degrading
        """
        cutoff_24h = now - timedelta(hours=24)

        recent = [
            r
            for r in receipts
            if _receipt_timestamp(r) is not None and _receipt_timestamp(r) >= cutoff_24h
        ]
        all_receipts = receipts

        if len(recent) < self.min_samples or len(all_receipts) < self.min_samples:
            return None

        # Extract latencies
        recent_latencies = [
            _safe_int(r.get("latency_ms"))
            for r in recent
            if _safe_int(r.get("latency_ms")) > 0
        ]
        all_latencies = [
            _safe_int(r.get("latency_ms"))
            for r in all_receipts
            if _safe_int(r.get("latency_ms")) > 0
        ]

        if len(recent_latencies) < self.min_samples or len(all_latencies) < self.min_samples:
            return None

        # Calculate p50
        recent_p50 = _rolling_median(recent_latencies)
        historical_p50 = _rolling_median(all_latencies)

        if historical_p50 == 0.0:
            return None

        ratio = recent_p50 / historical_p50
        if ratio > 1.5:
            direction = TrendDirection.DEGRADING
            change_pct = (ratio - 1.0) * 100
        elif ratio < 0.67:
            direction = TrendDirection.IMPROVING
            change_pct = (1.0 - ratio) * -100
        else:
            return None

        severity = "warning" if direction == TrendDirection.DEGRADING else "info"

        return Trend(
            metric_name="latency_p50",
            provider_slug=provider,
            direction=direction,
            baseline_value=historical_p50,
            current_value=recent_p50,
            change_pct=change_pct,
            sample_count=len(recent_latencies),
            severity=severity,
        )


def format_trends(trends: list[Trend]) -> str:
    """Format detected trends as human-readable summary."""
    if not trends:
        return "No trends detected."

    # Group by severity
    by_severity = {}
    for trend in trends:
        by_severity.setdefault(trend.severity, []).append(trend)

    lines = []

    # Critical trends first
    for severity in ["critical", "warning", "info"]:
        if severity not in by_severity:
            continue

        severity_label = severity.upper()
        lines.append(f"\n{severity_label}:")
        lines.append("-" * 60)

        for trend in by_severity[severity]:
            provider_label = f"{trend.provider_slug}" if trend.provider_slug else "platform"
            direction_emoji = {
                TrendDirection.IMPROVING: "↑",
                TrendDirection.DEGRADING: "↓",
                TrendDirection.ACCELERATING: "⚡",
                TrendDirection.STABLE: "→",
            }.get(trend.direction, "?")

            # Format change percentage
            change_str = f"{abs(trend.change_pct):+.1f}%"

            # Format values based on metric type
            if trend.metric_name == "cost_trend":
                baseline_str = f"${trend.baseline_value:.4f}/dispatch"
                current_str = f"${trend.current_value:.4f}/dispatch"
            elif trend.metric_name == "latency_p50":
                baseline_str = f"{trend.baseline_value:.0f}ms"
                current_str = f"{trend.current_value:.0f}ms"
            elif "rate" in trend.metric_name:
                baseline_str = f"{trend.baseline_value * 100:.1f}%"
                current_str = f"{trend.current_value * 100:.1f}%"
            else:
                baseline_str = f"{trend.baseline_value:.2f}"
                current_str = f"{trend.current_value:.2f}"

            lines.append(
                f"  {direction_emoji} {trend.metric_name:<25} "
                f"({provider_label:<12}) "
                f"baseline: {baseline_str:<20} → {current_str:<20} "
                f"[{change_str} n={trend.sample_count}]"
            )

    # Summary line
    critical_count = len(by_severity.get("critical", []))
    warning_count = len(by_severity.get("warning", []))
    info_count = len(by_severity.get("info", []))

    lines.append(f"\nSummary: {critical_count} critical, {warning_count} warnings, {info_count} info")

    return "\n".join(lines)
