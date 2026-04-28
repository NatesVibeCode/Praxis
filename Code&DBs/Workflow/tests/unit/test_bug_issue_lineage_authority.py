from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.bug_tracker import Bug, BugCategory, BugSeverity, BugStatus, BugTracker, afile_bug
import runtime.engineering_observability as observability_mod
from surfaces.api.handlers import _bug_surface_contract as bug_contract
from surfaces.api.handlers._shared import _bug_to_dict


def _sample_bug(*, source_issue_id: str | None = "issue.dispatch-gap") -> Bug:
    now = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    return Bug(
        bug_id="BUG-LINEAGE",
        bug_key="bug_lineage",
        title="Issue lineage regression",
        severity=BugSeverity.P1,
        status=BugStatus.OPEN,
        priority="P1",
        category=BugCategory.ARCHITECTURE,
        description="Bug lineage should stay attached to the canonical bug surface.",
        summary="Bug lineage should stay attached to the canonical bug surface.",
        filed_at=now,
        updated_at=now,
        resolved_at=None,
        created_at=now,
        filed_by="tester",
        assigned_to=None,
        tags=("cluster:lineage",),
        source_kind="manual",
        discovered_in_run_id=None,
        discovered_in_receipt_id=None,
        owner_ref=None,
        source_issue_id=source_issue_id,
        decision_ref="",
        resolution_summary=None,
        resume_context={},
    )


class _RecordingConn:
    def __init__(self) -> None:
        self.insert_query: str | None = None
        self.insert_params: tuple[object, ...] | None = None

    def execute(self, query: str, *params: object):
        self.insert_query = query
        self.insert_params = params
        return []

    def fetchrow(self, query: str, *params: object):
        del query, params
        if self.insert_params is None:
            return None
        return {
            "bug_id": self.insert_params[0],
            "bug_key": self.insert_params[1],
            "title": self.insert_params[2],
            "severity": self.insert_params[3],
            "status": self.insert_params[4],
            "priority": self.insert_params[5],
            "category": self.insert_params[6],
            "description": self.insert_params[7],
            "summary": self.insert_params[8],
            "source_kind": self.insert_params[9],
            "discovered_in_run_id": self.insert_params[10],
            "discovered_in_receipt_id": self.insert_params[11],
            "owner_ref": self.insert_params[12],
            "source_issue_id": self.insert_params[13],
            "decision_ref": self.insert_params[14],
            "opened_at": self.insert_params[15],
            "resolved_at": None,
            "created_at": self.insert_params[16],
            "updated_at": self.insert_params[17],
            "filed_by": self.insert_params[18],
            "tags": self.insert_params[19],
            "resume_context": self.insert_params[20],
        }


class _AsyncRecordingBugConn:
    def __init__(self) -> None:
        self.insert_query: str | None = None
        self.insert_params: tuple[object, ...] | None = None

    def transaction(self):
        # Production code wraps the bug-tracker insert in `async with conn.transaction():`
        # for atomicity. Provide a no-op async context manager so the fake honors
        # that contract.
        outer = self

        class _Tx:
            async def __aenter__(self_inner):
                return outer

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Tx()

    async def execute(self, query: str, *params: object):
        self.insert_query = query
        self.insert_params = params
        return "INSERT 0 1"

    async def fetchrow(self, query: str, *params: object):
        if query.startswith("SELECT 1 FROM"):
            return {"exists": 1}
        if "SELECT * FROM bugs" not in query or self.insert_params is None:
            return None
        return {
            "bug_id": self.insert_params[0],
            "bug_key": self.insert_params[1],
            "title": self.insert_params[2],
            "severity": self.insert_params[3],
            "status": self.insert_params[4],
            "priority": self.insert_params[5],
            "category": self.insert_params[6],
            "description": self.insert_params[7],
            "summary": self.insert_params[8],
            "source_kind": self.insert_params[9],
            "discovered_in_run_id": self.insert_params[10],
            "discovered_in_receipt_id": self.insert_params[11],
            "owner_ref": self.insert_params[12],
            "source_issue_id": self.insert_params[13],
            "decision_ref": self.insert_params[14],
            "opened_at": self.insert_params[15],
            "resolved_at": None,
            "created_at": self.insert_params[16],
            "updated_at": self.insert_params[17],
            "filed_by": self.insert_params[18],
            "tags": self.insert_params[19],
            "resume_context": self.insert_params[20],
        }


class _FakeBugTrackerMod:
    class BugSeverity:
        P2 = BugSeverity.P2

    class BugCategory:
        OTHER = BugCategory.OTHER


def test_bug_tracker_file_bug_persists_source_issue_id() -> None:
    conn = _RecordingConn()
    tracker = BugTracker(conn=conn)

    bug, similar = tracker.file_bug(
        title="Persist lineage",
        severity=BugSeverity.P2,
        category=BugCategory.RUNTIME,
        description="Persist the source issue id through the runtime tracker.",
        filed_by="tester",
        source_issue_id="issue.dispatch-gap",
    )

    assert similar == []
    assert conn.insert_query is not None
    assert "source_issue_id" in conn.insert_query
    assert bug.source_issue_id == "issue.dispatch-gap"
    assert _bug_to_dict(bug)["source_issue_id"] == "issue.dispatch-gap"


def test_bug_tracker_file_bug_derives_non_blank_decision_ref() -> None:
    conn = _RecordingConn()
    tracker = BugTracker(conn=conn)

    bug, _similar = tracker.file_bug(
        title="Decision fallback",
        severity=BugSeverity.P2,
        category=BugCategory.RUNTIME,
        description="Blank decision refs should not persist as hidden missing authority.",
        filed_by="tester",
        source_kind="mcp workflow server",
    )

    assert bug.decision_ref == "decision.bug_tracker.filing.mcp-workflow-server.implicit"


def test_async_file_bug_uses_canonical_bug_tracker_insert_contract() -> None:
    conn = _AsyncRecordingBugConn()

    row, similar = asyncio.run(
        afile_bug(
            conn,
            title="Async lineage",
            severity="high",
            category=BugCategory.RUNTIME,
            description="Async operator surfaces should use the same bug contract.",
            filed_by="operator_write",
            source_kind="issue_promotion",
            source_issue_id="issue.dispatch-gap",
        )
    )

    assert similar == []
    assert conn.insert_query is not None
    assert "source_issue_id" in conn.insert_query
    assert row["bug_id"].startswith("BUG-")
    assert row["bug_key"] == row["bug_id"].lower().replace("-", "_")
    assert row["status"] == "OPEN"
    assert row["severity"] == "P1"
    assert row["priority"] == "P1"
    assert row["category"] == "RUNTIME"
    assert row["filed_by"] == "operator_write"
    assert row["source_issue_id"] == "issue.dispatch-gap"
    assert row["decision_ref"] == "decision.bug_tracker.filing.issue_promotion.implicit"


def test_issue_promotion_routes_through_bug_tracker_authority() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "surfaces"
        / "api"
        / "operator_write.py"
    ).read_text()
    start = source.index("    async def _ensure_issue_promoted_to_bug(")
    end = source.index("    async def _ensure_bug_promoted_to_roadmap(")
    promotion_body = source[start:end]

    assert "afile_bug(" in promotion_body
    assert "INSERT INTO bugs" not in promotion_body
    assert "_auto_promoted_issue_bug_id" not in source


def test_file_bug_payload_forwards_source_issue_id_to_tracker() -> None:
    captured: dict[str, object] = {}
    bug = _sample_bug()

    class _FakeBugTracker:
        def file_bug(self, **kwargs):
            captured.update(kwargs)
            return bug, []

    payload = bug_contract.file_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": bug.title,
            "description": bug.description,
            "source_issue_id": "issue.dispatch-gap",
            "discovered_in_receipt_id": "receipt-123",
        },
        serialize_bug=_bug_to_dict,
        filed_by_default="workflow_api",
        source_kind_default="workflow_api",
    )

    assert captured["source_issue_id"] == "issue.dispatch-gap"
    assert captured["decision_ref"] == ""
    assert payload["bug"]["source_issue_id"] == "issue.dispatch-gap"


def test_file_bug_payload_dry_run_skips_insert() -> None:
    calls: list[str] = []

    class _FakeBugTracker:
        def file_bug(self, **_kwargs):
            calls.append("file_bug")
            raise AssertionError("file_bug should not be called when dry_run is true")

        def search(self, _title, **_kwargs):
            return []

    payload = bug_contract.file_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": "Preview only",
            "dry_run": True,
            "severity": "P2",
            "category": "OTHER",
            "description": "no insert",
            "discovered_in_receipt_id": "receipt-123",
        },
        serialize_bug=_bug_to_dict,
        filed_by_default="workflow_api",
        source_kind_default="workflow_api",
        include_similar_bugs=True,
    )

    assert calls == []
    assert payload["dry_run"] is True
    assert payload["filed"] is False
    assert payload["preview"]["severity"] == "P2"
    assert payload["preview"]["title"] == "Preview only"


def test_file_bug_payload_blocks_strong_duplicate_before_insert() -> None:
    calls: list[str] = []

    class _FakeBugTracker:
        def find_similar_bugs(self, **_kwargs):
            return [
                {
                    "bug_id": "BUG-EXISTING",
                    "title": "Existing duplicate",
                    "status": "OPEN",
                    "severity": "P1",
                    "similarity": 0.91,
                }
            ]

        def file_bug(self, **_kwargs):
            calls.append("file_bug")
            raise AssertionError("duplicate gate should block before insert")

    payload = bug_contract.file_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": "Existing duplicate",
            "severity": "P2",
            "category": "OTHER",
            "description": "same failure",
            "discovered_in_receipt_id": "receipt-123",
        },
        serialize_bug=_bug_to_dict,
        filed_by_default="workflow_api",
        source_kind_default="workflow_api",
        include_similar_bugs=True,
    )

    assert calls == []
    assert payload["ok"] is False
    assert payload["filed"] is False
    assert payload["reason_code"] == "bug.file.duplicate_candidate"
    assert payload["similar_bugs"][0]["bug_id"] == "BUG-EXISTING"


def test_file_bug_payload_allows_intentional_duplicate_override() -> None:
    calls: list[str] = []
    bug = _sample_bug()

    class _FakeBugTracker:
        def find_similar_bugs(self, **_kwargs):
            return [
                {
                    "bug_id": "BUG-EXISTING",
                    "title": "Existing duplicate",
                    "status": "OPEN",
                    "severity": "P1",
                    "similarity": 0.91,
                }
            ]

        def file_bug(self, **kwargs):
            calls.append("file_bug")
            return bug, []

    payload = bug_contract.file_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": "Existing duplicate but different root cause",
            "severity": "P2",
            "category": "OTHER",
            "description": "distinct failure",
            "discovered_in_receipt_id": "receipt-123",
            "allow_duplicate": True,
        },
        serialize_bug=_bug_to_dict,
        filed_by_default="workflow_api",
        source_kind_default="workflow_api",
        include_similar_bugs=True,
    )

    assert calls == ["file_bug"]
    assert payload["ok"] is True
    assert payload["filed"] is True
    assert payload["similar_bugs"][0]["bug_id"] == "BUG-EXISTING"


def test_file_bug_payload_preview_flag_aliases_dry_run() -> None:
    class _FakeBugTracker:
        def file_bug(self, **_kwargs):
            raise AssertionError("no insert")

        def search(self, *_a, **_k):
            return []

    payload = bug_contract.file_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": "Alias probe",
            "preview": True,
            "category": "OTHER",
            "discovered_in_receipt_id": "receipt-123",
        },
        serialize_bug=_bug_to_dict,
        filed_by_default="workflow_api",
        source_kind_default="workflow_api",
    )
    assert payload["dry_run"] is True
    assert payload["filed"] is False


def test_list_and_search_payloads_apply_source_issue_filter() -> None:
    captured: dict[str, dict[str, object]] = {}
    bug = _sample_bug()

    class _FakeBugTracker:
        def count_bugs(self, **kwargs):
            captured["count_bugs"] = kwargs
            return 1

        def list_bugs(self, **kwargs):
            captured["list_bugs"] = kwargs
            return [bug]

        def search(self, *_args, **kwargs):
            captured["search"] = kwargs
            return [bug]

    tracker = _FakeBugTracker()
    listed = bug_contract.list_bugs_payload(
        bt=tracker,
        bt_mod=_FakeBugTrackerMod(),
        body={"source_issue_id": "issue.dispatch-gap"},
        serialize_bug=_bug_to_dict,
        default_limit=10,
        include_replay_details=False,
    )
    found = bug_contract.search_bugs_payload(
        bt=tracker,
        bt_mod=_FakeBugTrackerMod(),
        body={
            "title": "lineage",
            "source_issue_id": "issue.dispatch-gap",
        },
        serialize_bug=_bug_to_dict,
        default_limit=10,
    )

    assert captured["count_bugs"]["source_issue_id"] == "issue.dispatch-gap"
    assert captured["list_bugs"]["source_issue_id"] == "issue.dispatch-gap"
    assert captured["search"]["source_issue_id"] == "issue.dispatch-gap"
    assert listed["bugs"][0]["source_issue_id"] == "issue.dispatch-gap"
    assert found["bugs"][0]["source_issue_id"] == "issue.dispatch-gap"


def test_resolve_payload_marks_fix_pending_verification_without_terminal_resolution() -> None:
    captured: dict[str, object] = {}
    bug = _sample_bug()

    class _FakeBugTracker:
        def get(self, bug_id: str):
            assert bug_id == bug.bug_id
            return bug

        def update_status(self, bug_id: str, status: BugStatus, *, resolution_summary=None):
            captured["bug_id"] = bug_id
            captured["status"] = status
            captured["resolution_summary"] = resolution_summary
            return bug

    payload = bug_contract.resolve_bug_payload(
        bt=_FakeBugTracker(),
        bt_mod=SimpleNamespace(BugStatus=BugStatus, BugTracker=BugTracker),
        body={
            "bug_id": bug.bug_id,
            "status": "FIX_PENDING_VERIFICATION",
            "notes": "Fix is in tree; verifier evidence still needs to run.",
        },
        serialize_bug=_bug_to_dict,
        resolved_statuses={BugStatus.FIXED, BugStatus.WONT_FIX, BugStatus.DEFERRED},
    )

    assert payload["ok"] is True
    assert payload["resolved"] is False
    assert payload["marked"] is True
    assert captured == {
        "bug_id": bug.bug_id,
        "status": BugStatus.FIX_PENDING_VERIFICATION,
        "resolution_summary": "Fix is in tree; verifier evidence still needs to run.",
    }


def test_bug_scoreboard_surfaces_source_issue_id(tmp_path: Path) -> None:
    bug = _sample_bug(source_issue_id="issue.dispatch-gap")

    class _FakeBugTracker:
        def list_bugs(self, *args, **kwargs):
            del args, kwargs
            return [bug]

        def failure_packet(self, bug_id: str, *, receipt_limit: int = 1, allow_backfill: bool = True):
            del bug_id, receipt_limit, allow_backfill
            return {
                "latest_receipt": {
                    "write_paths": ("runtime/engine.py",),
                    "verified_paths": ("runtime/engine.py",),
                },
                "lifecycle": {
                    "recurrence_count": 3,
                    "impacted_run_count": 2,
                    "has_regression_after_fix": False,
                },
                "replay_context": {"ready": True},
                "fix_verification": {"fix_verified": False},
                "observability_state": "complete",
                "observability_gaps": (),
            }

        def stats(self):
            return SimpleNamespace(
                total=1,
                by_status={"OPEN": 1},
                by_severity={"P1": 1},
                by_category={"ARCHITECTURE": 1},
                open_count=1,
                mttr_hours=None,
                packet_ready_count=1,
                replay_ready_count=1,
                replay_blocked_count=0,
                fix_verified_count=0,
                underlinked_count=0,
                observability_state="complete",
                errors=(),
            )

    payload = observability_mod.build_bug_scoreboard(
        bug_tracker=_FakeBugTracker(),
        limit=5,
        repo_root=tmp_path,
    )

    assert payload["top_recurring"][0]["source_issue_id"] == "issue.dispatch-gap"
