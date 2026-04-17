"""Focused regressions for read-only bug tracker surfaces."""

import importlib.util
import sys
from pathlib import Path

import pytest

_mod_path = Path(__file__).resolve().parents[2] / "runtime" / "bug_tracker.py"
_spec = importlib.util.spec_from_file_location("bug_tracker_read_only", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["bug_tracker_read_only"] = _mod
_spec.loader.exec_module(_mod)

BugCategory = _mod.BugCategory
BugSeverity = _mod.BugSeverity
BugTracker = _mod.BugTracker


@pytest.fixture
def tracker():
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    yield BugTracker(conn=conn)
    conn.close()


def test_failure_packet_skips_replay_backfill_when_read_only(
    tracker: BugTracker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bug, _ = tracker.file_bug(
        title="Read-only replay packet",
        severity=BugSeverity.P2,
        category=BugCategory.RUNTIME,
        description="Should not backfill provenance from a read path.",
        filed_by="alice",
    )

    monkeypatch.setattr(
        tracker,
        "backfill_replay_provenance",
        lambda _bug_id: (_ for _ in ()).throw(
            AssertionError("read-only failure_packet must not backfill provenance")
        ),
    )

    packet = tracker.failure_packet(bug.bug_id, allow_backfill=False)

    assert packet is not None
    assert packet["provenance_backfill"]["reason_code"] == "bug.replay_backfill.skipped_read_only"
