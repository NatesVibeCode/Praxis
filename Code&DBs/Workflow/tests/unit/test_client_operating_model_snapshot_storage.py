from __future__ import annotations

import json
from types import SimpleNamespace

from runtime.operations.commands.client_operating_model import (
    StoreOperatorViewSnapshotCommand,
    handle_store_operator_view_snapshot,
)
from runtime.operations.queries.client_operating_model import (
    QueryClientOperatingModelSnapshotRead,
    handle_client_operating_model_snapshot_read,
)
from storage.postgres.client_operating_model_repository import (
    list_operator_view_snapshots,
    persist_operator_view_snapshot,
)


def _operator_view() -> dict:
    return {
        "kind": "client_operating_model.operator_surface.system_census.v1",
        "schema_version": 1,
        "view_id": "system_census.demo",
        "state": "empty",
        "freshness": {"status": "unknown", "generated_at": "2026-04-30T12:00:00Z"},
        "permission_scope": {"scope_ref": "tenant.acme", "visibility": "full"},
        "evidence_refs": ["fixture.empty_census"],
        "correlation_ids": ["corr.demo"],
        "payload": {"counts": {"systems": 0}},
    }


def _workflow_context_composite_view() -> dict:
    return {
        "kind": "client_operating_model.operator_surface.workflow_context_composite.v1",
        "schema_version": 1,
        "view_id": "workflow_context_composite.demo",
        "state": "partial",
        "freshness": {"status": "unknown", "generated_at": "2026-04-30T12:00:00Z"},
        "permission_scope": {"scope_ref": "workflow.demo", "visibility": "full"},
        "evidence_refs": ["workflow_context.demo"],
        "correlation_ids": ["corr.demo"],
        "payload": {
            "truth_state_classes": {
                "none": 0,
                "inferred": 0,
                "synthetic": 1,
                "documented": 0,
                "anonymized_operational": 0,
                "schema_bound": 0,
                "observed": 0,
                "verified": 0,
                "promoted": 0,
                "stale": 0,
                "contradicted": 0,
                "blocked": 0,
            },
            "deployability": {"state": "simulation_ready"},
        },
    }


class _SnapshotConn:
    def __init__(self) -> None:
        self.fetchrow_args = None
        self.fetch_args = None

    def fetchrow(self, query: str, *args):
        assert "client_operating_model_operator_view_snapshots" in query
        self.fetchrow_args = args
        return {
            "snapshot_digest": args[0],
            "snapshot_ref": args[1],
            "view_name": args[2],
            "view_id": args[3],
            "scope_ref": args[4],
            "state": args[5],
            "freshness_json": args[6],
            "permission_scope_json": args[7],
            "evidence_refs_json": args[8],
            "correlation_ids_json": args[9],
            "observed_by_ref": args[11],
            "source_ref": args[12],
        }

    def fetch(self, query: str, *args):
        assert "client_operating_model_operator_view_snapshots" in query
        self.fetch_args = args
        return [
            {
                "snapshot_digest": "digest.1",
                "snapshot_ref": "snapshot.1",
                "view_name": "system_census",
                "view_id": "system_census.demo",
                "scope_ref": "tenant.acme",
                "state": "empty",
                "freshness_json": json.dumps({"status": "unknown"}),
                "permission_scope_json": json.dumps({"scope_ref": "tenant.acme"}),
                "evidence_refs_json": json.dumps(["fixture.empty_census"]),
                "correlation_ids_json": json.dumps(["corr.demo"]),
                "operator_view_json": json.dumps(_operator_view()),
                "observed_by_ref": "operator:nate",
                "source_ref": "phase_13.test",
            }
        ]


def test_persist_operator_view_snapshot_writes_stable_projection_row() -> None:
    conn = _SnapshotConn()

    result = persist_operator_view_snapshot(
        conn,
        operator_view=_operator_view(),
        observed_by_ref="operator:nate",
        source_ref="phase_13.test",
    )

    assert result["snapshot_digest"]
    assert result["snapshot_ref"].startswith("client_operating_model_operator_view_snapshot:")
    assert result["view_name"] == "system_census"
    assert result["view_id"] == "system_census.demo"
    assert result["scope_ref"] == "tenant.acme"
    assert result["evidence_refs_json"] == ["fixture.empty_census"]


def test_store_snapshot_command_returns_event_payload() -> None:
    conn = _SnapshotConn()
    subsystems = SimpleNamespace(get_pg_conn=lambda: conn)

    result = handle_store_operator_view_snapshot(
        StoreOperatorViewSnapshotCommand(
            operator_view=_operator_view(),
            observed_by_ref="operator:nate",
            source_ref="phase_13.test",
        ),
        subsystems,
    )

    assert result["ok"] is True
    assert result["operation"] == "client_operating_model_operator_view_snapshot_store"
    assert result["event_payload"]["view_name"] == "system_census"
    assert result["event_payload"]["scope_ref"] == "tenant.acme"


def test_snapshot_read_query_returns_exact_snapshot_and_filters() -> None:
    conn = _SnapshotConn()
    subsystems = SimpleNamespace(get_pg_conn=lambda: conn)

    result = handle_client_operating_model_snapshot_read(
        QueryClientOperatingModelSnapshotRead(
            snapshot_ref="snapshot.1",
            view="system_census",
            scope_ref="tenant.acme",
            limit=5,
        ),
        subsystems,
    )

    assert result["ok"] is True
    assert result["operation"] == "client_operating_model_operator_view_snapshot_read"
    assert result["count"] == 1
    assert result["snapshot"]["operator_view_json"]["view_id"] == "system_census.demo"
    assert conn.fetch_args[:5] == ("snapshot.1", None, "system_census", "tenant.acme", 5)


def test_list_operator_view_snapshots_supports_latest_view_readback() -> None:
    conn = _SnapshotConn()

    rows = list_operator_view_snapshots(
        conn,
        view="system_census",
        scope_ref="tenant.acme",
        limit=1,
    )

    assert rows[0]["snapshot_ref"] == "snapshot.1"
    assert conn.fetch_args[:5] == (None, None, "system_census", "tenant.acme", 1)


def test_workflow_context_composite_snapshot_can_be_stored_and_read() -> None:
    conn = _SnapshotConn()

    stored = persist_operator_view_snapshot(
        conn,
        operator_view=_workflow_context_composite_view(),
        observed_by_ref="operator:nate",
        source_ref="lunchbox.12",
    )
    rows = list_operator_view_snapshots(
        conn,
        view="workflow_context_composite",
        scope_ref="workflow.demo",
        limit=1,
    )

    assert stored["view_name"] == "workflow_context_composite"
    assert stored["scope_ref"] == "workflow.demo"
    assert rows[0]["snapshot_ref"] == "snapshot.1"
    assert conn.fetch_args[:5] == (None, None, "workflow_context_composite", "workflow.demo", 1)
