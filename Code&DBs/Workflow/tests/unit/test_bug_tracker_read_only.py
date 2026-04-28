"""Focused regressions for read-only bug tracker surfaces."""

import importlib.util
import sys
from pathlib import Path

import pytest
from storage.postgres.validators import PostgresConfigurationError

_mod_path = Path(__file__).resolve().parents[2] / "runtime" / "bug_tracker.py"
_spec = importlib.util.spec_from_file_location("bug_tracker_read_only", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["bug_tracker_read_only"] = _mod
_spec.loader.exec_module(_mod)

BugCategory = _mod.BugCategory
BugSeverity = _mod.BugSeverity
BugStatus = _mod.BugStatus
BugTracker = _mod.BugTracker

from surfaces.api.handlers import _bug_surface_contract as bug_contract
from surfaces.mcp.tools import bugs as mcp_bugs
from runtime import bug_evidence


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


def test_bug_history_summary_exposes_authoritative_degraded_state() -> None:
    packet = {
        "signature": {"authority": "bug_record_fallback", "fingerprint_scope": "bug_only"},
        "observability_state": "degraded",
        "observability_gaps": ("bug.evidence_links.missing", "receipt.missing"),
        "errors": ({"scope": "receipts", "reason_code": "receipts.query_failed"},),
        "trace": {"run_ids": (), "receipt_ids": ()},
        "latest_receipt": None,
        "fallback_receipts": (),
        "replay_context": {"source": "missing", "ready": False},
        "provenance_backfill": {
            "bug_id": "BUG-HISTORY",
            "reason_code": "bug.replay_backfill.skipped_read_only",
        },
        "agent_actions": {
            "replay": {"reason_code": "bug.replay_missing_run_context"},
        },
    }

    history = bug_evidence.history_summary(bug_id="BUG-HISTORY", packet=packet)

    assert history["observability_state"] == "degraded"
    assert history["observability_gaps"] == (
        "bug.evidence_links.missing",
        "receipt.missing",
    )
    assert history["errors"] == (
        {"scope": "receipts", "reason_code": "receipts.query_failed"},
    )
    assert history["trace"] == {"run_ids": (), "receipt_ids": ()}
    assert history["provenance_backfill"]["reason_code"] == (
        "bug.replay_backfill.skipped_read_only"
    )


def test_duplicate_check_uses_fast_title_like_list_path() -> None:
    class _Bug:
        bug_id = "BUG-DUPE"
        title = "Routing timeout"

    class _Tracker:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list_bugs(self, **kwargs):
            self.calls.append(kwargs)
            return [_Bug()]

        def search(self, *_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("duplicate_check must not use enriched search")

        def replay_hint(self, *_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("duplicate_check must not load replay state")

    tracker = _Tracker()

    payload = bug_contract.duplicate_check_payload(
        bt=tracker,
        bt_mod=_mod,
        body={"title_like": "Routing", "limit": 3},
        serialize_bug=lambda bug: {"bug_id": bug.bug_id, "title": bug.title},
    )

    assert payload["enrichment"] == {
        "clusters": False,
        "replay_state": False,
        "semantic_search": False,
        "reason_code": "bug.duplicate_check.fast_title_like",
    }
    assert payload["bugs"] == [{"bug_id": "BUG-DUPE", "title": "Routing timeout"}]
    assert tracker.calls == [
        {
            "status": None,
            "severity": None,
            "category": None,
            "title_like": "Routing",
            "tags": None,
            "exclude_tags": None,
            "open_only": True,
            "limit": 3,
        }
    ]


def test_duplicate_check_accepts_description_context() -> None:
    class _Bug:
        bug_id = "BUG-DUPE"
        title = "Runtime validation failed"

    class _Tracker:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list_bugs(self, **kwargs):
            self.calls.append(kwargs)
            return [_Bug()]

    tracker = _Tracker()

    payload = bug_contract.duplicate_check_payload(
        bt=tracker,
        bt_mod=_mod,
        body={"description": "runtime validation failed after DB authority lookup"},
        serialize_bug=lambda bug: {"bug_id": bug.bug_id, "title": bug.title},
    )

    assert payload["bugs"] == [{"bug_id": "BUG-DUPE", "title": "Runtime validation failed"}]
    assert payload["query"]["title_like"] == "runtime validation failed after DB authority lookup"
    assert payload["query"]["description"] == "runtime validation failed after DB authority lookup"
    assert tracker.calls[0]["title_like"] == "runtime validation failed after DB authority lookup"


def test_bug_tracker_search_exact_bug_id_uses_direct_lookup() -> None:
    class _Conn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def fetchrow(self, query: str, *params: object):
            self.calls.append((query, params))
            return {
                "bug_id": "BUG-1234ABCD",
                "bug_key": "bug_1234abcd",
                "title": "Exact id target",
                "severity": "P2",
                "status": "OPEN",
                "priority": "P2",
                "category": "RUNTIME",
                "description": "Exact bug id search should not fall through to FTS.",
                "summary": "Exact bug id search should not fall through to FTS.",
                "filed_at": "2026-04-23T00:00:00+00:00",
                "created_at": "2026-04-23T00:00:00+00:00",
                "updated_at": "2026-04-23T00:00:00+00:00",
                "resolved_at": None,
                "filed_by": "test",
                "tags": "",
            }

        def execute(self, *_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("exact bug id search must not use ranked search")

    conn = _Conn()
    tracker = BugTracker(conn=conn)

    results = tracker.search("bug-1234abcd", status=BugStatus.OPEN)

    assert [bug.bug_id for bug in results] == ["BUG-1234ABCD"]
    assert "bug_id = $1" in conn.calls[0][0]
    assert conn.calls[0][1][:2] == ("BUG-1234ABCD", "OPEN")


def test_mcp_bug_tool_returns_structured_error_when_bug_authority_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnavailableSubs:
        def get_bug_tracker(self):
            raise PostgresConfigurationError(
                "postgres.authority_unavailable",
                "WORKFLOW_DATABASE_URL authority unavailable",
                details={"operation": "get_bug_tracker"},
            )

        def get_bug_tracker_mod(self):
            raise AssertionError("bug tracker module should not be loaded after authority failure")

    monkeypatch.setattr(mcp_bugs, "_subs", _UnavailableSubs())

    payload = mcp_bugs.tool_praxis_bugs({"action": "stats"})

    assert payload == {
        "error": "WORKFLOW_DATABASE_URL authority unavailable",
        "error_code": "postgres.authority_unavailable",
        "details": {"operation": "get_bug_tracker"},
    }


def test_mcp_bug_tool_marks_database_authority_drift_as_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Tracker:
        def __init__(self) -> None:
            self._conn = type(
                "_Conn",
                (),
                {"_database_url": "postgresql://stale.example:5432/praxis"},
            )()

        def stats(self):
            return {"total": 1, "open_count": 1}

    class _Subs:
        def _postgres_env(self):
            return {
                "WORKFLOW_DATABASE_URL": "postgresql://live.example:5432/praxis",
                "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "repo_env:/repo/.env",
            }

        def get_bug_tracker(self):
            return _Tracker()

        def get_bug_tracker_mod(self):
            return _mod

    monkeypatch.setattr(mcp_bugs, "_subs", _Subs())

    payload = mcp_bugs.tool_praxis_bugs({"action": "stats"})

    assert payload["observability_state"] == "degraded"
    assert payload["warnings"] == [
        "Resolved workflow DB authority does not match the live surface connection "
        "fingerprint; treat results as degraded until the surface is rebound to "
        "the canonical authority."
    ]
    assert payload["database_authority"]["status"] == "degraded"
    assert payload["database_authority"]["authority_source"] == "repo_env:/repo/.env"
    assert payload["database_authority"]["fingerprint"].startswith("workflow_pool:")
    assert payload["database_authority"]["observed_fingerprint"].startswith("workflow_pool:")
    assert (
        payload["database_authority"]["fingerprint"]
        != payload["database_authority"]["observed_fingerprint"]
    )
