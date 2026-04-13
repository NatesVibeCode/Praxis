"""Tests for friction_ledger and staleness_detector modules."""

import importlib.util
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

# Direct file imports — bypass runtime/__init__.py which pulls in unrelated deps
_RUNTIME_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "runtime")


def _load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_RUNTIME_DIR, f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod  # register so dataclasses can resolve annotations
    spec.loader.exec_module(mod)
    return mod


_fl_mod = _load_module("friction_ledger")
_sd_mod = _load_module("staleness_detector")

FrictionEvent = _fl_mod.FrictionEvent
FrictionLedger = _fl_mod.FrictionLedger
FrictionStats = _fl_mod.FrictionStats
FrictionType = _fl_mod.FrictionType

FreshnessBucket = _sd_mod.FreshnessBucket
StaleItem = _sd_mod.StaleItem
StalenessDetector = _sd_mod.StalenessDetector
StalenessRule = _sd_mod.StalenessRule


# ═══════════════════════════════════════════════════════════════════════
# Friction Ledger tests
# ═══════════════════════════════════════════════════════════════════════


class _InMemoryConn:
    """Minimal in-memory mock of SyncPostgresConnection for friction_events."""

    def __init__(self):
        self._rows: list[dict] = []

    def execute(self, query: str, *args) -> list:
        import re
        q = query.strip()
        if q.upper().startswith("INSERT"):
            # Parse column names from INSERT INTO friction_events (col1, ...) VALUES ($1, ...)
            col_match = re.search(r'\(([^)]+)\)\s*VALUES', q)
            if col_match:
                cols = [c.strip() for c in col_match.group(1).split(",")]
                row = {col: args[i] for i, col in enumerate(cols)}
                self._rows.append(row)
            return []
        if q.upper().startswith("SELECT"):
            # Extract columns
            col_part = re.search(r'SELECT\s+(.+?)\s+FROM', q, re.IGNORECASE).group(1)
            cols = [c.strip() for c in col_part.split(",")]
            # Filter rows by WHERE clauses
            filtered = list(self._rows)
            if "WHERE" in q.upper():
                where_part = re.split(r'\bWHERE\b', q, flags=re.IGNORECASE)[1]
                where_part = re.split(r'\bORDER\b|\bLIMIT\b', where_part, flags=re.IGNORECASE)[0]
                conditions = re.split(r'\bAND\b', where_part, flags=re.IGNORECASE)
                for cond in conditions:
                    cond = cond.strip()
                    # Handle col = $N
                    eq_match = re.match(r'(\w+)\s*=\s*\$(\d+)', cond)
                    if eq_match:
                        col, pidx = eq_match.group(1), int(eq_match.group(2)) - 1
                        val = args[pidx]
                        filtered = [r for r in filtered if r.get(col) == val]
                        continue
                    # Handle col >= $N
                    gte_match = re.match(r'(\w+)\s*>=\s*\$(\d+)', cond)
                    if gte_match:
                        col, pidx = gte_match.group(1), int(gte_match.group(2)) - 1
                        val = args[pidx]
                        filtered = [r for r in filtered if r.get(col) >= val]
                        continue
            # Apply LIMIT
            limit_match = re.search(r'LIMIT\s+\$(\d+)', q, re.IGNORECASE)
            if limit_match:
                pidx = int(limit_match.group(1)) - 1
                filtered = filtered[:args[pidx]]
            # Project columns
            return [{c: r.get(c) for c in cols} for r in filtered]
        return []


@pytest.fixture
def ledger():
    return FrictionLedger(_InMemoryConn())


class TestFrictionType:
    def test_enum_values(self):
        assert FrictionType.GUARDRAIL_BOUNCE.value == "guardrail_bounce"
        assert FrictionType.WARN_ONLY.value == "warn_only"
        assert FrictionType.HARD_FAILURE.value == "hard_failure"


class TestFrictionEvent:
    def test_frozen(self):
        ev = FrictionEvent("id1", FrictionType.WARN_ONLY, "governance", "job-1", "msg", datetime.now(timezone.utc))
        with pytest.raises(AttributeError):
            ev.message = "changed"


class TestFrictionLedger:
    def test_record_returns_event(self, ledger):
        ev = ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "job-1", "blocked by scope")
        assert isinstance(ev, FrictionEvent)
        assert ev.friction_type == FrictionType.GUARDRAIL_BOUNCE
        assert ev.source == "governance"
        assert ev.job_label == "job-1"

    def test_list_events_all(self, ledger):
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "m1")
        ledger.record(FrictionType.HARD_FAILURE, "loop_detector", "j2", "m2")
        events = ledger.list_events()
        assert len(events) == 2

    def test_list_events_filter_type(self, ledger):
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "m1")
        ledger.record(FrictionType.HARD_FAILURE, "loop_detector", "j2", "m2")
        events = ledger.list_events(friction_type=FrictionType.HARD_FAILURE)
        assert len(events) == 1
        assert events[0].friction_type == FrictionType.HARD_FAILURE

    def test_list_events_filter_source(self, ledger):
        ledger.record(FrictionType.WARN_ONLY, "posture", "j1", "m1")
        ledger.record(FrictionType.WARN_ONLY, "governance", "j2", "m2")
        events = ledger.list_events(source="posture")
        assert len(events) == 1
        assert events[0].source == "posture"

    def test_list_events_since(self, ledger):
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "old")
        since = datetime.now(timezone.utc) + timedelta(seconds=1)
        events = ledger.list_events(since=since)
        assert len(events) == 0

    def test_list_events_limit(self, ledger):
        for i in range(10):
            ledger.record(FrictionType.WARN_ONLY, "governance", f"j{i}", f"m{i}")
        events = ledger.list_events(limit=3)
        assert len(events) == 3

    def test_stats(self, ledger):
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "m1")
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "posture", "j2", "m2")
        ledger.record(FrictionType.HARD_FAILURE, "loop_detector", "j3", "m3")
        s = ledger.stats()
        assert isinstance(s, FrictionStats)
        assert s.total == 3
        assert s.by_type["guardrail_bounce"] == 2
        assert s.by_source["governance"] == 1
        assert abs(s.bounce_rate - 2 / 3) < 0.01

    def test_stats_empty(self, ledger):
        s = ledger.stats()
        assert s.total == 0
        assert s.bounce_rate == 0.0

    def test_bounce_rate(self, ledger):
        ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "m1")
        ledger.record(FrictionType.HARD_FAILURE, "loop_detector", "j2", "m2")
        rate = ledger.bounce_rate(since_hours=1)
        assert abs(rate - 0.5) < 0.01

    def test_is_guardrail(self, ledger):
        ev_bounce = ledger.record(FrictionType.GUARDRAIL_BOUNCE, "governance", "j1", "m1")
        ev_warn = ledger.record(FrictionType.WARN_ONLY, "posture", "j2", "m2")
        ev_fail = ledger.record(FrictionType.HARD_FAILURE, "loop_detector", "j3", "m3")
        assert FrictionLedger.is_guardrail(ev_bounce) is True
        assert FrictionLedger.is_guardrail(ev_warn) is True
        assert FrictionLedger.is_guardrail(ev_fail) is False


# ═══════════════════════════════════════════════════════════════════════
# Staleness Detector tests
# ═══════════════════════════════════════════════════════════════════════


@pytest.fixture
def detector():
    return StalenessDetector()


class TestFreshnessBucket:
    def test_bucket_ordering(self):
        vals = [b.value for b in FreshnessBucket]
        assert "fresh" in vals
        assert "confirmed_stale" in vals


class TestStalenessDetector:
    def test_classify_fresh(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        last = now - timedelta(days=1)
        si = detector.classify("p1", "phases", last, now=now)
        assert si.bucket == FreshnessBucket.FRESH
        assert si.days_inactive == 1

    def test_classify_confirmed_stale(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        last = now - timedelta(days=60)
        si = detector.classify("p1", "phases", last, now=now)
        assert si.bucket == FreshnessBucket.CONFIRMED_STALE

    def test_classify_urgency_multiplier(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        last = now - timedelta(days=10)
        si = detector.classify("p1", "phases", last, now=now)
        # phases urgency_multiplier=2.0
        assert si.urgency_score == 10 * 2.0

    def test_classify_unknown_type_uses_fallback(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        last = now - timedelta(days=50)
        si = detector.classify("x1", "unknown_thing", last, now=now)
        assert si.bucket == FreshnessBucket.CONFIRMED_STALE
        assert si.urgency_score == 50 * 1.0

    def test_custom_rules(self):
        rules = [StalenessRule("tasks", warning_days=3, stale_days=7, urgency_multiplier=5.0)]
        det = StalenessDetector(rules=rules)
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        si = det.classify("t1", "tasks", now - timedelta(days=10), now=now)
        assert si.bucket == FreshnessBucket.CONFIRMED_STALE
        assert si.urgency_score == 50.0

    def test_scan_filters_and_sorts(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        items = [
            {"item_id": "a", "item_type": "phases", "last_activity": now - timedelta(days=1), "now": now},
            {"item_id": "b", "item_type": "phases", "last_activity": now - timedelta(days=20), "now": now},
            {"item_id": "c", "item_type": "phases", "last_activity": now - timedelta(days=10), "now": now},
        ]
        result = detector.scan(items)
        # a (1d) = FRESH, filtered out; b and c should remain
        assert all(r.bucket in {FreshnessBucket.AGING, FreshnessBucket.STALE_WARNING, FreshnessBucket.CONFIRMED_STALE} for r in result)
        # Sorted by urgency desc
        assert result[0].urgency_score >= result[-1].urgency_score

    def test_scan_empty(self, detector):
        assert detector.scan([]) == []

    def test_alert_summary_with_items(self, detector):
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        items = [
            {"item_id": "p1", "item_type": "phases", "last_activity": now - timedelta(days=20), "now": now},
        ]
        stale = detector.scan(items)
        summary = detector.alert_summary(stale)
        assert "1 item(s)" in summary
        assert "phases/p1" in summary

    def test_alert_summary_empty(self, detector):
        assert detector.alert_summary([]) == "No stale items detected."
