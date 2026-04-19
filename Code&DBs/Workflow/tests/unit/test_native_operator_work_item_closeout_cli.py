from __future__ import annotations

import asyncio
import json
from io import StringIO
from datetime import datetime, timezone

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main
from surfaces.api import operator_write


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def _env() -> dict[str, str]:
    return {"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test"}


def test_native_operator_work_item_closeout_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "action": payload["action"],
            "proof_threshold": {
                "bug_requires_evidence_role": "validates_fix",
                "roadmap_requires_source_bug_fix_proof": True,
            },
            "operation_receipt": {
                "operation_name": operation_name,
                "operation_kind": "command",
            },
            "evaluated": {
                "bug_ids": list(payload["bug_ids"]),
                "roadmap_item_ids": list(payload["roadmap_item_ids"]),
            },
            "candidates": {"bugs": [], "roadmap_items": []},
            "skipped": {"bugs": [], "roadmap_items": []},
            "committed": True,
            "applied": {"bugs": [], "roadmap_items": []},
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operation_catalog_gateway,
        "execute_operation_from_env",
        _execute_operation_from_env,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "work-item-closeout",
                "--bug-id",
                "bug.closeout.1",
                "--roadmap-item-id",
                "roadmap_item.closeout.1",
                "--commit",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["operation_name"] == "operator.work_item_closeout"
    assert captured["payload"]["action"] == "commit"
    assert captured["payload"]["bug_ids"] == ("bug.closeout.1",)
    assert captured["payload"]["roadmap_item_ids"] == ("roadmap_item.closeout.1",)
    assert payload["committed"] is True
    assert payload["operation_receipt"]["operation_name"] == "operator.work_item_closeout"
    assert payload["proof_threshold"]["bug_requires_evidence_role"] == "validates_fix"


def test_native_operator_work_item_closeout_preview_commits_also_use_shared_gate(monkeypatch) -> None:
    captured: list[str] = []

    def _execute_operation_from_env(*, env, operation_name: str, payload):
        del env, operation_name
        captured.append(payload["action"])
        return {
            "action": payload["action"],
            "proof_threshold": {
                "bug_requires_evidence_role": "validates_fix",
                "roadmap_requires_source_bug_fix_proof": True,
            },
            "operation_receipt": {
                "operation_name": "operator.work_item_closeout",
                "operation_kind": "command",
            },
            "evaluated": {
                "bug_ids": list(payload["bug_ids"]),
                "roadmap_item_ids": list(payload["roadmap_item_ids"]),
            },
            "candidates": {"bugs": [], "roadmap_items": []},
            "skipped": {"bugs": [], "roadmap_items": []},
            "committed": payload["action"] == "commit",
            "applied": {"bugs": [], "roadmap_items": []},
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operation_catalog_gateway,
        "execute_operation_from_env",
        _execute_operation_from_env,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "work-item-closeout",
                "--bug-id",
                "bug.closeout.2",
                "--roadmap-item-id",
                "roadmap_item.closeout.2",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )
    assert "\"committed\": false" in stdout.getvalue()

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "work-item-closeout",
                "--bug-id",
                "bug.closeout.2",
                "--roadmap-item-id",
                "roadmap_item.closeout.2",
                "--commit",
            ],
            env=_env(),
            stdout=stdout,
        )
        == 0
    )
    assert "\"committed\": true" in stdout.getvalue()
    assert captured == ["preview", "commit"]


class _NoSqlConnectionProxy:
    def transaction(self):
        return _NoSqlTransaction()

    async def close(self) -> None:
        return None

    async def execute(self, *_: object, **__: object) -> str:
        raise AssertionError("direct SQL execution is not expected in closeout delegation test")

    async def fetch(self, *_: object, **__: object) -> list[dict[str, object]]:
        raise AssertionError("direct SQL fetch is not expected in closeout delegation test")

    async def fetchrow(self, *_: object, **__: object):
        raise AssertionError("direct SQL fetchrow is not expected in closeout delegation test")


class _NoSqlTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


async def _fetch_no_relation_rows_for_closeout(
    self,
    conn,
    roadmap_item_ids: tuple[str, ...],
    bug_ids: tuple[str, ...],
) -> tuple[dict[str, object], ...]:
    del self, conn, roadmap_item_ids, bug_ids
    return ()


class _CloseoutRepositorySpy:
    def __init__(self, *, resolved_at: datetime) -> None:
        self.resolved_at = resolved_at
        self.bug_calls: list[tuple[tuple[str, ...], dict[str, str], datetime]] = []
        self.issue_calls: list[tuple[tuple[str, ...], datetime]] = []
        self.roadmap_calls: list[tuple[tuple[str, ...], str, datetime]] = []

    async def mark_bugs_fixed(
        self,
        *,
        bug_ids: tuple[str, ...],
        resolution_summaries_by_bug_id: dict[str, str],
        resolved_at: datetime,
    ) -> tuple[dict[str, object], ...]:
        self.bug_calls.append((bug_ids, resolution_summaries_by_bug_id, resolved_at))
        return tuple(
            {
                "bug_id": bug_id,
                "status": "FIXED",
                "resolved_at": resolved_at,
                "resolution_summary": resolution_summaries_by_bug_id[bug_id],
            }
            for bug_id in bug_ids
        )

    async def mark_issues_resolved_by_bug_ids(
        self,
        *,
        bug_ids: tuple[str, ...],
        resolved_at: datetime,
    ) -> tuple[dict[str, object], ...]:
        self.issue_calls.append((bug_ids, resolved_at))
        return ()

    async def mark_roadmap_items_completed(
        self,
        *,
        roadmap_item_ids: tuple[str, ...],
        completed_status: str,
        completed_at: datetime,
    ) -> tuple[dict[str, object], ...]:
        self.roadmap_calls.append((roadmap_item_ids, completed_status, completed_at))
        return tuple(
            {
                "roadmap_item_id": roadmap_item_id,
                "status": completed_status,
                "lifecycle": "completed",
                "completed_at": completed_at,
                "source_bug_id": "bug.closeout.1",
            }
            for roadmap_item_id in roadmap_item_ids
        )


def test_work_item_closeout_commit_delegates_to_closeout_repository(monkeypatch) -> None:
    resolved_at = datetime(2026, 4, 9, 17, 0, tzinfo=timezone.utc)
    spy = _CloseoutRepositorySpy(resolved_at=resolved_at)

    async def _fetch_bug_rows_for_closeout(self, conn, bug_ids):
        del conn
        return (
            {
                "bug_id": "bug.closeout.1",
                "resolved_at": None,
                "status": "OPEN",
            },
        )

    async def _fetch_roadmap_rows_for_closeout(
        self,
        conn,
        roadmap_item_ids: tuple[str, ...],
        source_bug_ids: tuple[str, ...],
    ) -> tuple[dict[str, object], ...]:
        del conn, source_bug_ids
        if not roadmap_item_ids:
            return ()
        return (
            {
                "roadmap_item_id": roadmap_item_ids[0],
                "title": "Closeout roadmap",
                "status": "active",
                "lifecycle": "claimed",
                "source_bug_id": "bug.closeout.1",
                "completed_at": None,
                "updated_at": resolved_at,
            },
        )

    async def _fetch_bug_evidence_for_closeout(
        self,
        conn,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[dict[str, str], ...]]:
        del conn
        assert bug_ids == ("bug.closeout.1",)
        return {
            "bug.closeout.1": (
                {
                    "evidence_kind": "verification_run",
                    "evidence_ref": "verification_run.closeout.1",
                    "evidence_role": "validates_fix",
                    "verification_status": "passed",
                },
            )
        }

    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_rows_for_closeout",
        _fetch_bug_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_rows_for_closeout",
        _fetch_roadmap_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_evidence_for_closeout",
        _fetch_bug_evidence_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_bug_relation_rows_for_closeout",
        _fetch_no_relation_rows_for_closeout,
    )
    monkeypatch.setattr(operator_write, "_now", lambda: resolved_at)

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=lambda env=None: asyncio.sleep(0, result=_NoSqlConnectionProxy()),
        work_item_closeout_repository_factory=lambda conn: spy,
    )

    payload = asyncio.run(
        frontdoor.reconcile_work_item_closeout_async(
            action="commit",
            bug_ids=("bug.closeout.1",),
            roadmap_item_ids=("roadmap_item.closeout.1",),
        )
    )

    assert payload["committed"] is True
    assert len(spy.bug_calls) == 1
    assert spy.bug_calls[0][0] == ("bug.closeout.1",)
    assert spy.bug_calls[0][2] == resolved_at
    assert len(spy.roadmap_calls) == 1
    assert spy.roadmap_calls[0][0] == ("roadmap_item.closeout.1",)
    assert spy.roadmap_calls[0][1] == "completed"
    assert spy.roadmap_calls[0][2] == resolved_at
    assert payload["applied"]["bugs"] == [
        {
            "bug_id": "bug.closeout.1",
            "status": "FIXED",
            "resolved_at": resolved_at.isoformat(),
            "resolution_summary": operator_write._closeout_resolution_summary(
                bug_id="bug.closeout.1", evidence_count=1
            ),
        }
    ]
    assert payload["applied"]["roadmap_items"] == [
        {
            "roadmap_item_id": "roadmap_item.closeout.1",
            "status": "completed",
            "lifecycle": "completed",
            "completed_at": resolved_at.isoformat(),
            "source_bug_id": "bug.closeout.1",
            "source_bug_link_source": "roadmap_items.source_bug_id",
            "source_bug_relation_id": None,
        }
    ]


def test_work_item_closeout_accepts_operator_relation_as_roadmap_bug_link(monkeypatch) -> None:
    resolved_at = datetime(2026, 4, 9, 17, 0, tzinfo=timezone.utc)

    async def _fetch_bug_rows_for_closeout(self, conn, bug_ids):
        del self, conn
        assert bug_ids == ("bug.closeout.related",)
        return (
            {
                "bug_id": "bug.closeout.related",
                "resolved_at": resolved_at,
                "status": "FIXED",
            },
        )

    async def _fetch_roadmap_rows_for_closeout(
        self,
        conn,
        roadmap_item_ids: tuple[str, ...],
        source_bug_ids: tuple[str, ...],
    ) -> tuple[dict[str, object], ...]:
        del self, conn, source_bug_ids
        if not roadmap_item_ids:
            return ()
        return (
            {
                "roadmap_item_id": roadmap_item_ids[0],
                "title": "Closeout roadmap",
                "status": "active",
                "lifecycle": "claimed",
                "source_bug_id": None,
                "completed_at": None,
                "updated_at": resolved_at,
            },
        )

    async def _fetch_relation_rows_for_closeout(
        self,
        conn,
        roadmap_item_ids: tuple[str, ...],
        bug_ids: tuple[str, ...],
    ) -> tuple[dict[str, object], ...]:
        del self, conn
        assert roadmap_item_ids == ("roadmap_item.closeout.related",)
        assert bug_ids == ("bug.closeout.related",)
        return (
            {
                "operator_object_relation_id": (
                    "operator_object_relation:implemented-by-fix:roadmap_item:"
                    "roadmap_item.closeout.related:bug:bug.closeout.related"
                ),
                "relation_kind": "implemented_by_fix",
                "relation_status": "active",
                "roadmap_item_id": "roadmap_item.closeout.related",
                "bug_id": "bug.closeout.related",
            },
        )

    async def _fetch_bug_evidence_for_closeout(
        self,
        conn,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[dict[str, str], ...]]:
        del self, conn
        assert bug_ids == ("bug.closeout.related",)
        return {
            "bug.closeout.related": (
                {
                    "evidence_kind": "verification_run",
                    "evidence_ref": "verification_run.closeout.related",
                    "evidence_role": "validates_fix",
                    "verification_status": "passed",
                },
            )
        }

    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_rows_for_closeout",
        _fetch_bug_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_rows_for_closeout",
        _fetch_roadmap_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_bug_relation_rows_for_closeout",
        _fetch_relation_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_evidence_for_closeout",
        _fetch_bug_evidence_for_closeout,
    )

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=lambda env=None: asyncio.sleep(0, result=_NoSqlConnectionProxy()),
    )

    payload = asyncio.run(
        frontdoor.reconcile_work_item_closeout_async(
            action="preview",
            bug_ids=("bug.closeout.related",),
            roadmap_item_ids=("roadmap_item.closeout.related",),
        )
    )

    assert payload["proof_threshold"]["roadmap_bug_link_authorities"] == [
        "roadmap_items.source_bug_id",
        "operator_object_relations.active_roadmap_item_to_bug",
    ]
    assert payload["candidates"]["roadmap_items"] == [
        {
            "roadmap_item_id": "roadmap_item.closeout.related",
            "source_bug_id": "bug.closeout.related",
            "source_bug_link_source": "operator_object_relations",
            "source_bug_relation_id": (
                "operator_object_relation:implemented-by-fix:roadmap_item:"
                "roadmap_item.closeout.related:bug:bug.closeout.related"
            ),
            "current_status": "active",
            "current_lifecycle": "claimed",
            "next_status": "completed",
            "next_lifecycle": "completed",
            "reason_codes": ["relation_bug_has_explicit_passed_fix_proof"],
            "evidence_refs": [
                {
                    "kind": "verification_run",
                    "ref": "verification_run.closeout.related",
                    "role": "validates_fix",
                    "verification_status": "passed",
                }
            ],
        }
    ]
    assert payload["skipped"]["roadmap_items"] == []


def test_work_item_closeout_requires_passed_fix_verification(monkeypatch) -> None:
    async def _fetch_bug_rows_for_closeout(self, conn, bug_ids):
        del conn, bug_ids
        return (
            {
                "bug_id": "bug.closeout.failed",
                "resolved_at": None,
                "status": "OPEN",
            },
        )

    async def _fetch_roadmap_rows_for_closeout(
        self,
        conn,
        roadmap_item_ids: tuple[str, ...],
        source_bug_ids: tuple[str, ...],
    ) -> tuple[dict[str, object], ...]:
        del self, conn, roadmap_item_ids, source_bug_ids
        return ()

    async def _fetch_bug_evidence_for_closeout(
        self,
        conn,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[dict[str, str], ...]]:
        del self, conn
        assert bug_ids == ("bug.closeout.failed",)
        return {
            "bug.closeout.failed": (
                {
                    "evidence_kind": "verification_run",
                    "evidence_ref": "verification_run.closeout.failed",
                    "evidence_role": "validates_fix",
                    "verification_status": "failed",
                },
            )
        }

    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_rows_for_closeout",
        _fetch_bug_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_rows_for_closeout",
        _fetch_roadmap_rows_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_bug_evidence_for_closeout",
        _fetch_bug_evidence_for_closeout,
    )
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_fetch_roadmap_bug_relation_rows_for_closeout",
        _fetch_no_relation_rows_for_closeout,
    )

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=lambda env=None: asyncio.sleep(0, result=_NoSqlConnectionProxy()),
    )

    payload = asyncio.run(
        frontdoor.reconcile_work_item_closeout_async(
            action="preview",
            bug_ids=("bug.closeout.failed",),
            roadmap_item_ids=(),
        )
    )

    assert payload["proof_threshold"]["bug_requires_passed_verification"] is True
    assert payload["candidates"]["bugs"] == []
    assert payload["skipped"]["bugs"] == [
        {
            "bug_id": "bug.closeout.failed",
            "current_status": "OPEN",
            "reason_codes": ["missing_passed_validates_fix_verification"],
        }
    ]
