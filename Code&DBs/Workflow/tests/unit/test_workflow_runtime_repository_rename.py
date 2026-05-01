from __future__ import annotations

import json

import pytest

from storage.postgres import workflow_runtime_repository as repo


class _Conn:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = row
        self.workflow_versions = [{"id": "wv-1", "definition": row["definition"]}]
        self.execution_manifests = [
            {"execution_manifest_ref": "wem-1", "materialized_spec_json": row["materialized_spec"]}
        ]
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT * FROM public.workflows WHERE id = $1"):
            if args[0] == self.row["id"]:
                return dict(self.row)
            return None
        if normalized.startswith("INSERT INTO public.workflows"):
            return {
                **self.row,
                "id": args[1],
                "name": args[2],
                "definition": json.loads(args[3]),
                "materialized_spec": json.loads(args[4]) if args[4] is not None else None,
            }
        return None

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT id, definition FROM workflow_versions WHERE workflow_id = $1"):
            if args[0] == self.row["id"]:
                return list(self.workflow_versions)
            return []
        if normalized.startswith(
            "SELECT execution_manifest_ref, materialized_spec_json FROM workflow_build_execution_manifests WHERE workflow_id = $1"
        ):
            if args[0] == self.row["id"]:
                return list(self.execution_manifests)
            return []
        return []


def test_rename_workflow_record_rekeys_dependents_and_rewrites_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Conn(
        {
            "id": "agent_handoff_search_db_probe",
            "name": "Agent Handoff Search DB Probe",
            "description": "probe workflow",
            "definition": {
                "workflow_id": "agent_handoff_search_db_probe",
                "definition_revision": "def_agent_handoff_search_db_probe",
                "notes": "Keep agent_handoff_search_db_probe in prose.",
                "nested": {
                    "target_workflow_id": "agent_handoff_search_db_probe",
                },
            },
            "materialized_spec": {
                "workflow_id": "agent_handoff_search_db_probe",
                "definition_revision": "def_agent_handoff_search_db_probe",
                "notes": "agent_handoff_search_db_probe stays in review prose",
            },
            "tags": ["probe"],
            "created_at": "2026-04-16T00:00:00+00:00",
            "updated_at": "2026-04-16T00:00:00+00:00",
            "version": 7,
            "is_template": False,
            "invocation_count": 3,
            "last_invoked_at": "2026-04-16T00:00:00+00:00",
        }
    )
    monkeypatch.setattr(repo, "load_workflow_record", lambda _conn, *, workflow_id: conn.row if workflow_id == conn.row["id"] else None)
    monkeypatch.setattr(repo, "workflow_exists", lambda _conn, *, workflow_id: False)

    renamed = repo.rename_workflow_record(
        conn,
        workflow_id="agent_handoff_search_db_probe",
        new_workflow_id="runtime_regression_probe",
        name="Runtime Regression Probe",
    )

    assert renamed["id"] == "runtime_regression_probe"
    assert renamed["name"] == "Runtime Regression Probe"
    assert renamed["definition"]["workflow_id"] == "runtime_regression_probe"
    assert renamed["definition"]["definition_revision"] == "def_agent_handoff_search_db_probe"
    assert renamed["definition"]["notes"] == "Keep agent_handoff_search_db_probe in prose."
    assert renamed["definition"]["nested"]["target_workflow_id"] == "runtime_regression_probe"
    assert renamed["materialized_spec"]["workflow_id"] == "runtime_regression_probe"
    assert renamed["materialized_spec"]["definition_revision"] == "def_agent_handoff_search_db_probe"
    assert renamed["materialized_spec"]["notes"] == "agent_handoff_search_db_probe stays in review prose"

    dependent_updates = [
        query
        for query, _ in conn.calls
        if query.strip().startswith("UPDATE ")
    ]
    assert dependent_updates == [
        """UPDATE workflow_versions
               SET workflow_id = $2,
                   definition = $3::jsonb
               WHERE id = $1""",
        """UPDATE workflow_build_execution_manifests
               SET workflow_id = $2,
                   materialized_spec_json = $3::jsonb
               WHERE execution_manifest_ref = $1""",
        "UPDATE workflow_triggers SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE uploaded_files SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_review_decisions SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_intents SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_candidate_manifests SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_candidate_slots SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_candidates SET workflow_id = $2 WHERE workflow_id = $1",
        "UPDATE workflow_build_review_sessions SET workflow_id = $2 WHERE workflow_id = $1",
    ]
    assert any(
        query == "DELETE FROM public.workflows WHERE id = $1" and args == ("agent_handoff_search_db_probe",)
        for query, args in conn.calls
    )


def test_rename_workflow_record_rejects_existing_target(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _Conn(
        {
            "id": "agent_handoff_search_db_probe",
            "name": "Agent Handoff Search DB Probe",
            "description": "probe workflow",
            "definition": {"workflow_id": "agent_handoff_search_db_probe"},
            "materialized_spec": {"workflow_id": "agent_handoff_search_db_probe"},
            "tags": [],
            "created_at": "2026-04-16T00:00:00+00:00",
            "updated_at": "2026-04-16T00:00:00+00:00",
            "version": 7,
            "is_template": False,
            "invocation_count": 3,
            "last_invoked_at": "2026-04-16T00:00:00+00:00",
        }
    )
    monkeypatch.setattr(repo, "load_workflow_record", lambda _conn, *, workflow_id: conn.row if workflow_id == conn.row["id"] else None)
    monkeypatch.setattr(repo, "workflow_exists", lambda _conn, *, workflow_id: workflow_id == "runtime_regression_probe")

    with pytest.raises(repo.PostgresWriteError):
        repo.rename_workflow_record(
            conn,
            workflow_id="agent_handoff_search_db_probe",
            new_workflow_id="runtime_regression_probe",
        )


def test_list_workflow_records_filters_never_run_and_bounds_limit() -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []

    class _ListConn:
        def execute(self, query: str, *args):
            calls.append((query, args))
            return [
                {
                    "id": "wf_draft",
                    "name": "Draft Flow",
                    "definition": {"type": "pipeline"},
                    "materialized_spec": None,
                    "invocation_count": 0,
                    "last_invoked_at": None,
                }
            ]

    rows = repo.list_workflow_records(_ListConn(), never_run=True, limit=999)

    assert rows[0]["id"] == "wf_draft"
    query, args = calls[0]
    assert "COALESCE(invocation_count, 0) = 0" in query
    assert "last_invoked_at IS NULL" in query
    assert args == (500,)
