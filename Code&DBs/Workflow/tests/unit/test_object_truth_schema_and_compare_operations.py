from __future__ import annotations

from runtime.operations.commands import object_truth as command_module
from runtime.operations.commands.object_truth import (
    RecordComparisonRunCommand,
    StoreSchemaSnapshotCommand,
    handle_record_comparison_run,
    handle_store_schema_snapshot,
)
from runtime.operations.queries import object_truth as query_module
from runtime.operations.queries.object_truth import QueryCompareVersions, handle_compare_versions


class _Subsystems:
    def __init__(self) -> None:
        self.conn = object()

    def get_pg_conn(self) -> object:
        return self.conn


def test_store_schema_snapshot_command_persists_normalized_schema(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _persist(conn, *, schema_snapshot, observed_by_ref, source_ref):
        captured["conn"] = conn
        captured["schema_snapshot"] = schema_snapshot
        captured["observed_by_ref"] = observed_by_ref
        captured["source_ref"] = source_ref
        return {
            "schema_snapshot_ref": f"object_truth_schema_snapshot:{schema_snapshot['schema_digest']}",
            "field_count": len(schema_snapshot["fields"]),
        }

    monkeypatch.setattr(command_module, "persist_schema_snapshot", _persist)

    result = handle_store_schema_snapshot(
        StoreSchemaSnapshotCommand(
            system_ref=" salesforce ",
            object_ref="account",
            raw_schema={
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "id", "type": "string", "required": True},
                ]
            },
            observed_by_ref="operator:nate",
            source_ref="schema:account",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "object_truth_store_schema_snapshot"
    assert result["field_count"] == 2
    assert result["schema_snapshot"]["fields"][0]["field_path"] == "id"
    assert result["event_payload"]["observed_by_ref"] == "operator:nate"
    assert captured["source_ref"] == "schema:account"


def test_compare_versions_query_loads_persisted_versions(monkeypatch) -> None:
    left = {
        "object_version_digest": "left",
        "identity": {"identity_digest": "same"},
        "source_metadata": {"updated_at": "2026-04-28T10:00:00Z"},
        "field_observations": [
            {
                "field_path": "id",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "same-id",
                "redacted_value_preview": "001",
            },
            {
                "field_path": "name",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "left-name",
                "redacted_value_preview": "Acme",
            },
        ],
    }
    right = {
        "object_version_digest": "right",
        "identity": {"identity_digest": "same"},
        "source_metadata": {"updated_at": "2026-04-28T11:00:00Z"},
        "field_observations": [
            {
                "field_path": "id",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "same-id",
                "redacted_value_preview": "001",
            },
            {
                "field_path": "name",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "right-name",
                "redacted_value_preview": "ACME Corp",
            },
        ],
    }

    def _load(conn, *, object_version_digest):
        return {"left": left, "right": right}.get(object_version_digest)

    monkeypatch.setattr(query_module, "load_object_version", _load)

    result = handle_compare_versions(
        QueryCompareVersions(
            left_object_version_digest="left",
            right_object_version_digest="right",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["stats"] == {
        "matching_fields": 1,
        "different_fields": 1,
        "missing_left_fields": 0,
        "missing_right_fields": 0,
    }
    assert result["comparison"]["freshness"]["state"] == "right_newer"


def test_compare_versions_query_reports_missing_versions(monkeypatch) -> None:
    monkeypatch.setattr(query_module, "load_object_version", lambda conn, *, object_version_digest: None)

    result = handle_compare_versions(
        QueryCompareVersions(
            left_object_version_digest="missing-left",
            right_object_version_digest="missing-right",
        ),
        _Subsystems(),
    )

    assert result["ok"] is False
    assert result["error_code"] == "object_truth.object_version_not_found"
    assert result["missing"] == ["left_object_version_digest", "right_object_version_digest"]


def test_record_comparison_run_command_persists_comparison(monkeypatch) -> None:
    left = {
        "object_version_digest": "left",
        "identity": {"identity_digest": "same"},
        "source_metadata": {"updated_at": "2026-04-28T10:00:00Z"},
        "field_observations": [
            {
                "field_path": "id",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "same-id",
                "redacted_value_preview": "001",
            }
        ],
    }
    right = {
        "object_version_digest": "right",
        "identity": {"identity_digest": "same"},
        "source_metadata": {"updated_at": "2026-04-28T11:00:00Z"},
        "field_observations": [
            {
                "field_path": "id",
                "field_kind": "text",
                "presence": "present",
                "cardinality_kind": "one",
                "normalized_value_digest": "same-id",
                "redacted_value_preview": "001",
            }
        ],
    }
    captured: dict[str, object] = {}

    def _load(conn, *, object_version_digest):
        return {"left": left, "right": right}.get(object_version_digest)

    def _persist(conn, *, comparison, observed_by_ref, source_ref):
        captured["comparison"] = comparison
        captured["observed_by_ref"] = observed_by_ref
        captured["source_ref"] = source_ref
        return {
            "comparison_run_digest": comparison["comparison_digest"],
            "comparison_run_ref": f"object_truth_comparison_run:{comparison['comparison_digest']}",
        }

    monkeypatch.setattr(command_module, "load_object_version", _load)
    monkeypatch.setattr(command_module, "persist_comparison_run", _persist)

    result = handle_record_comparison_run(
        RecordComparisonRunCommand(
            left_object_version_digest="left",
            right_object_version_digest="right",
            observed_by_ref="operator:nate",
            source_ref="comparison:demo",
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "object_truth_record_comparison_run"
    assert result["comparison"]["summary"]["matching_fields"] == 1
    assert result["event_payload"]["observed_by_ref"] == "operator:nate"
    assert captured["source_ref"] == "comparison:demo"
