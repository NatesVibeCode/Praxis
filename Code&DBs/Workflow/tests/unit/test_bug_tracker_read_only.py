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

from surfaces.api.handlers import _bug_surface_contract as bug_contract


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


def test_replay_bug_skips_replay_backfill_by_default(
    tracker: BugTracker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bug, _ = tracker.file_bug(
        title="Read-only replay action",
        severity=BugSeverity.P2,
        category=BugCategory.RUNTIME,
        description="Replay reads should not backfill provenance implicitly.",
        filed_by="alice",
    )

    monkeypatch.setattr(
        tracker,
        "backfill_replay_provenance",
        lambda _bug_id: (_ for _ in ()).throw(
            AssertionError("replay_bug read path must not backfill provenance")
        ),
    )

    replay = tracker.replay_bug(bug.bug_id)

    assert replay is not None
    assert replay["bug_id"] == bug.bug_id


def test_bug_surface_packet_history_replay_are_read_only() -> None:
    calls: list[tuple[str, bool]] = []

    class _Tracker:
        def failure_packet(self, bug_id: str, *, receipt_limit: int = 5, allow_backfill: bool = True):
            calls.append((f"packet:{bug_id}:{receipt_limit}", allow_backfill))
            return {
                "signature": {"bug_id": bug_id},
                "agent_actions": {},
                "replay_context": {"ready": False},
            }

        def replay_bug(self, bug_id: str, *, receipt_limit: int = 5, allow_backfill: bool = True):
            calls.append((f"replay:{bug_id}:{receipt_limit}", allow_backfill))
            return {"bug_id": bug_id, "ready": False}

    serialize = lambda value, **_kwargs: value
    tracker = _Tracker()

    bug_contract.packet_payload(
        bt=tracker,
        body={"bug_id": "BUG-READONLY", "receipt_limit": 2},
        serialize=serialize,
    )
    bug_contract.history_payload(
        bt=tracker,
        body={"bug_id": "BUG-READONLY", "receipt_limit": 3},
        serialize=serialize,
    )
    bug_contract.replay_payload(
        bt=tracker,
        body={"bug_id": "BUG-READONLY", "receipt_limit": 4},
        serialize=serialize,
    )

    assert calls == [
        ("packet:BUG-READONLY:2", False),
        ("packet:BUG-READONLY:3", False),
        ("replay:BUG-READONLY:4", False),
    ]
