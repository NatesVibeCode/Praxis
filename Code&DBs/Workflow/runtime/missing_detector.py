"""Detect missing, stale, or orphaned content in the entity graph."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class MissingContentType(Enum):
    STALE_TOPIC = "stale_topic"
    MISSING_TRANSCRIPT = "missing_transcript"
    WEEKLY_GAP = "weekly_gap"
    ORPHANED_ACTION = "orphaned_action"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

_VALID_SEVERITIES = ("low", "medium", "high")


@dataclass(frozen=True)
class MissingFinding:
    finding_type: MissingContentType
    description: str
    entity_id: str | None
    severity: str  # 'low' | 'medium' | 'high'
    suggested_action: str

    def __post_init__(self) -> None:
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(f"severity must be one of {_VALID_SEVERITIES}, got {self.severity!r}")


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------

class MissingContentDetector:
    """Scans entity lists for staleness, gaps, and orphans."""

    def __init__(self, stale_days: int = 30, weekly_cadence_check: bool = True) -> None:
        self.stale_days = stale_days
        self.weekly_cadence_check = weekly_cadence_check

    # -- individual detectors -----------------------------------------------

    def detect_stale_topics(self, entities: list[dict]) -> list[MissingFinding]:
        findings: list[MissingFinding] = []
        now = _now()
        cutoff = now - timedelta(days=self.stale_days)

        for ent in entities:
            if ent.get("type") != "topic":
                continue
            updated = _parse_dt(ent.get("updated_at") or ent.get("created_at"))
            if updated is None:
                continue
            if updated < cutoff:
                days_stale = (now - updated).days
                sev = "high" if days_stale > self.stale_days * 2 else "medium"
                findings.append(MissingFinding(
                    finding_type=MissingContentType.STALE_TOPIC,
                    description=f"Topic '{ent.get('name', ent.get('id', '?'))}' not updated in {days_stale} days",
                    entity_id=ent.get("id"),
                    severity=sev,
                    suggested_action="Review and update or archive this topic",
                ))
        return findings

    def detect_weekly_gaps(
        self,
        entities: list[dict],
        expected_weekly_types: list[str] | tuple[str, ...] = ("document",),
    ) -> list[MissingFinding]:
        if not self.weekly_cadence_check:
            return []

        findings: list[MissingFinding] = []
        now = _now()

        # Group latest timestamp per type
        latest_by_type: dict[str, datetime] = {}
        for ent in entities:
            etype = ent.get("type", "")
            if etype not in expected_weekly_types:
                continue
            dt = _parse_dt(ent.get("updated_at") or ent.get("created_at"))
            if dt is None:
                continue
            if etype not in latest_by_type or dt > latest_by_type[etype]:
                latest_by_type[etype] = dt

        for etype in expected_weekly_types:
            latest = latest_by_type.get(etype)
            if latest is None:
                findings.append(MissingFinding(
                    finding_type=MissingContentType.WEEKLY_GAP,
                    description=f"No '{etype}' entities found at all",
                    entity_id=None,
                    severity="high",
                    suggested_action=f"Create a '{etype}' entity to fill the gap",
                ))
                continue
            gap_days = (now - latest).days
            if gap_days > 7:
                sev = "high" if gap_days > 14 else "medium"
                findings.append(MissingFinding(
                    finding_type=MissingContentType.WEEKLY_GAP,
                    description=f"'{etype}' has a {gap_days}-day gap (expected weekly)",
                    entity_id=None,
                    severity=sev,
                    suggested_action=f"Add a new '{etype}' to maintain weekly cadence",
                ))
        return findings

    def detect_orphaned_actions(
        self,
        entities: list[dict],
        edges: list[dict],
    ) -> list[MissingFinding]:
        findings: list[MissingFinding] = []

        # Collect ids that appear in any edge
        connected_ids: set[str] = set()
        for edge in edges:
            src = edge.get("source") or edge.get("from")
            tgt = edge.get("target") or edge.get("to")
            if src:
                connected_ids.add(str(src))
            if tgt:
                connected_ids.add(str(tgt))

        for ent in entities:
            if ent.get("type") != "action":
                continue
            eid = str(ent.get("id", ""))
            if eid and eid not in connected_ids:
                findings.append(MissingFinding(
                    finding_type=MissingContentType.ORPHANED_ACTION,
                    description=f"Action '{ent.get('name', eid)}' has no edges to any workstream",
                    entity_id=eid,
                    severity="medium",
                    suggested_action="Link this action to a workstream or mark complete",
                ))
        return findings

    # -- combined scan ------------------------------------------------------

    def scan_all(
        self,
        entities: list[dict],
        edges: list[dict],
    ) -> list[MissingFinding]:
        findings: list[MissingFinding] = []
        findings.extend(self.detect_stale_topics(entities))
        findings.extend(self.detect_weekly_gaps(entities))
        findings.extend(self.detect_orphaned_actions(entities, edges))

        severity_order = {"high": 0, "medium": 1, "low": 2}
        findings.sort(key=lambda f: severity_order.get(f.severity, 9))
        return findings


# ---------------------------------------------------------------------------
# Prioritizer
# ---------------------------------------------------------------------------

_SEVERITY_SCORE = {"high": 3, "medium": 2, "low": 1}


class FindingPrioritizer:
    """Rank and cap findings by severity tier score."""

    def prioritize(
        self,
        findings: list[MissingFinding],
        max_surfaced: int = 5,
    ) -> list[MissingFinding]:
        scored = sorted(findings, key=lambda f: _SEVERITY_SCORE.get(f.severity, 0), reverse=True)
        return scored[:max_surfaced]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    # fromisoformat handles microseconds and +00:00 offsets (Python 3.7+)
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None
