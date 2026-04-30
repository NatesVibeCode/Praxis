from __future__ import annotations

from runtime.operations.queries.object_truth import QueryReadiness, handle_readiness
from storage.postgres.object_truth_repository import inspect_readiness


_TABLES = [
    "object_truth_object_versions",
    "object_truth_field_observations",
    "object_truth_schema_snapshots",
    "object_truth_comparison_runs",
]

_OPERATIONS = {
    "object_truth_observe_record": ("query", "read_only"),
    "object_truth_compare_versions": ("query", "read_only"),
    "object_truth_store_observed_record": ("command", "idempotent"),
    "object_truth_store_schema_snapshot": ("command", "idempotent"),
    "object_truth_record_comparison_run": ("command", "idempotent"),
}

_EVENTS = [
    "object_truth.object_version_stored",
    "object_truth.schema_snapshot_stored",
    "object_truth.comparison_run_recorded",
]

_COLUMNS = {
    "object_truth_object_versions": [
        "payload_digest",
        "source_metadata_json",
        "object_version_json",
    ],
    "object_truth_field_observations": [
        "sensitive",
        "normalized_value_digest",
        "redacted_value_preview_json",
    ],
}


class _ReadinessConn:
    def __init__(self, *, missing_tables: set[str] | None = None) -> None:
        self.missing_tables = missing_tables or set()

    def fetch(self, query: str, *args):
        if "to_regclass" in query:
            return [
                {"table_name": table, "present": table not in self.missing_tables}
                for table in args[0]
            ]
        if "FROM operation_catalog_registry" in query:
            return [
                {
                    "operation_name": name,
                    "operation_kind": kind,
                    "idempotency_policy": idempotency,
                    "posture": "observe" if kind == "query" else "operate",
                    "enabled": True,
                    "authority_domain_ref": "authority.object_truth",
                }
                for name, (kind, idempotency) in _OPERATIONS.items()
            ]
        if "FROM authority_object_registry" in query:
            return [
                {
                    "object_name": table,
                    "object_kind": "table",
                    "lifecycle_status": "active",
                    "data_dictionary_object_kind": table,
                }
                for table in _TABLES
                if table not in self.missing_tables
            ]
        if "FROM data_dictionary_objects" in query:
            return [
                {"object_kind": table, "category": "table"}
                for table in _TABLES
                if table not in self.missing_tables
            ]
        if "FROM authority_event_contracts" in query:
            return [
                {"event_type": event, "enabled": True, "receipt_required": True}
                for event in _EVENTS
            ]
        if "FROM information_schema.columns" in query:
            return [
                {"table_name": table, "column_name": column}
                for table, columns in _COLUMNS.items()
                for column in columns
                if table not in self.missing_tables
            ]
        raise AssertionError(f"unexpected fetch query: {query}")

    def fetchrow(self, query: str, *args):
        if "FROM authority_domains" in query:
            return {
                "authority_domain_ref": "authority.object_truth",
                "enabled": True,
                "decision_ref": "decision.object_truth",
            }
        if "SELECT count(*) FROM object_truth_object_versions" in query:
            return {
                "object_versions": 2,
                "field_observations": 8,
                "schema_snapshots": 1,
                "comparison_runs": 1,
            }
        raise AssertionError(f"unexpected fetchrow query: {query}")


class _Subsystems:
    def __init__(self, conn) -> None:
        self.conn = conn

    def get_pg_conn(self):
        return self.conn


def test_inspect_readiness_reports_ready_when_authority_is_complete() -> None:
    result = inspect_readiness(
        _ReadinessConn(),
        client_payload_mode="redacted_hashes",
        planned_fanout=3,
    )

    assert result["state"] == "ready"
    assert result["can_advance"] is True
    assert result["no_go_conditions"] == []
    assert {gate["status"] for gate in result["gates"]} == {"passed"}
    assert result["counts"] == {
        "object_versions": 2,
        "field_observations": 8,
        "schema_snapshots": 1,
        "comparison_runs": 1,
    }


def test_inspect_readiness_fails_closed_for_missing_tables() -> None:
    result = inspect_readiness(
        _ReadinessConn(missing_tables={"object_truth_comparison_runs"}),
        include_counts=True,
    )

    assert result["state"] == "blocked"
    assert result["can_advance"] is False
    assert result["counts"] is None
    assert any(item["gate_ref"] == "object_truth.tables" for item in result["no_go_conditions"])


def test_readiness_blocks_raw_payloads_without_policy() -> None:
    result = inspect_readiness(
        _ReadinessConn(),
        client_payload_mode="raw_client_payloads",
    )

    assert result["state"] == "blocked"
    assert result["privacy_posture"]["status"] == "blocked"
    assert any(
        item["gate_ref"] == "object_truth.client_payload_policy"
        for item in result["no_go_conditions"]
    )


def test_readiness_query_handler_returns_gate_result() -> None:
    result = handle_readiness(
        QueryReadiness(planned_fanout=2, include_counts=False),
        _Subsystems(_ReadinessConn()),
    )

    assert result["ok"] is True
    assert result["operation"] == "object_truth_readiness"
    assert result["state"] == "ready"
    assert result["counts"] is None
