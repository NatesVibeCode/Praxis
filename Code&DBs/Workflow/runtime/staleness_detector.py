"""Staleness Detection — classifies items by freshness and urgency."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


class FreshnessBucket(enum.Enum):
    FRESH = "fresh"
    RECENT = "recent"
    AGING = "aging"
    STALE_WARNING = "stale_warning"
    CONFIRMED_STALE = "confirmed_stale"


@dataclass(frozen=True)
class StalenessRule:
    entity_type: str
    warning_days: int
    stale_days: int
    urgency_multiplier: float


@dataclass(frozen=True)
class StaleItem:
    item_id: str
    item_type: str
    last_activity: datetime
    bucket: FreshnessBucket
    days_inactive: int
    urgency_score: float


_DEFAULT_RULES: list[StalenessRule] = [
    StalenessRule(entity_type="phases", warning_days=7, stale_days=14, urgency_multiplier=2.0),
    StalenessRule(entity_type="agents", warning_days=14, stale_days=30, urgency_multiplier=1.5),
    StalenessRule(entity_type="work_items", warning_days=21, stale_days=45, urgency_multiplier=1.0),
]


class StalenessDetector:
    """Classify items by freshness bucket and urgency score."""

    def __init__(self, rules: Optional[list[StalenessRule]] = None) -> None:
        rules_list = rules if rules is not None else _DEFAULT_RULES
        self._rules: dict[str, StalenessRule] = {r.entity_type: r for r in rules_list}

    # ------------------------------------------------------------------
    def classify(
        self,
        item_id: str,
        item_type: str,
        last_activity: datetime,
        now: Optional[datetime] = None,
    ) -> StaleItem:
        now = now or datetime.now(timezone.utc)
        delta = now - last_activity
        days_inactive = max(int(delta.total_seconds() / 86400), 0)

        rule = self._rules.get(item_type)
        if rule is None:
            # Fall back: use work_items defaults
            rule = StalenessRule(entity_type=item_type, warning_days=21, stale_days=45, urgency_multiplier=1.0)

        bucket = self._assign_bucket(days_inactive, rule)
        urgency = days_inactive * rule.urgency_multiplier

        return StaleItem(
            item_id=item_id,
            item_type=item_type,
            last_activity=last_activity,
            bucket=bucket,
            days_inactive=days_inactive,
            urgency_score=urgency,
        )

    # ------------------------------------------------------------------
    def scan(self, items: list[dict]) -> list[StaleItem]:
        """Classify a batch; return only AGING+ items sorted by urgency descending."""
        results: list[StaleItem] = []
        dominated = {FreshnessBucket.AGING, FreshnessBucket.STALE_WARNING, FreshnessBucket.CONFIRMED_STALE}
        for item in items:
            si = self.classify(
                item_id=item["item_id"],
                item_type=item["item_type"],
                last_activity=item["last_activity"],
                now=item.get("now"),
            )
            if si.bucket in dominated:
                results.append(si)
        results.sort(key=lambda s: s.urgency_score, reverse=True)
        return results

    # ------------------------------------------------------------------
    def alert_summary(self, items: list[StaleItem]) -> str:
        if not items:
            return "No stale items detected."
        lines = [f"Staleness alert: {len(items)} item(s) need attention"]
        for si in items:
            lines.append(
                f"  - [{si.bucket.value}] {si.item_type}/{si.item_id}: "
                f"{si.days_inactive}d inactive, urgency={si.urgency_score:.1f}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    @staticmethod
    def _assign_bucket(days_inactive: int, rule: StalenessRule) -> FreshnessBucket:
        warning_half = rule.warning_days // 2
        if days_inactive <= warning_half:
            return FreshnessBucket.FRESH
        if days_inactive <= rule.warning_days:
            return FreshnessBucket.RECENT
        # Between warning and stale thresholds
        midpoint = rule.warning_days + (rule.stale_days - rule.warning_days) // 2
        if days_inactive <= midpoint:
            return FreshnessBucket.AGING
        if days_inactive <= rule.stale_days:
            return FreshnessBucket.STALE_WARNING
        return FreshnessBucket.CONFIRMED_STALE
