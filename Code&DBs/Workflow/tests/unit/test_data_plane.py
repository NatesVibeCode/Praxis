from __future__ import annotations

import json
from pathlib import Path

import pytest

from contracts.data_contracts import DataContractError, normalize_data_job
from runtime.data_plane import build_data_workflow_spec, execute_data_job
from surfaces.mcp.tools import data as data_tools


def test_normalize_data_job_requires_input_source() -> None:
    with pytest.raises(DataContractError, match="input source"):
        normalize_data_job({"operation": "profile"})


def test_execute_data_job_normalizes_inline_records() -> None:
    receipt = execute_data_job(
        {
            "operation": "normalize",
            "records": [
                {"email": "  Alice@Example.COM  ", "name": " Alice  "},
                {"email": "Bob@Example.COM", "name": "Bob"},
            ],
            "rules": {
                "email": ["trim", "lower"],
                "name": ["trim", "collapse_whitespace"],
            },
        }
    )

    assert receipt["ok"] is True
    assert receipt["operation"] == "normalize"
    assert receipt["stats"]["changed_rows"] == 2
    assert receipt["records"][0]["email"] == "alice@example.com"
    assert receipt["records"][0]["name"] == "Alice"


def test_execute_data_job_reconciles_inline_sources() -> None:
    receipt = execute_data_job(
        {
            "operation": "reconcile",
            "records": [
                {"id": "1", "email": "alice@example.com"},
                {"id": "2", "email": "bob@example.com"},
            ],
            "secondary_records": [
                {"id": "1", "email": "alice@old.example.com"},
                {"id": "3", "email": "carol@example.com"},
            ],
            "keys": ["id"],
        }
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["create_count"] == 1
    assert receipt["stats"]["update_count"] == 1
    assert receipt["stats"]["delete_count"] == 1
    assert receipt["plan"]["update"][0]["key"] == {"id": "1"}


def test_execute_data_job_filters_and_sorts_inline_records() -> None:
    receipt = execute_data_job(
        {
            "operation": "filter",
            "records": [
                {"id": "1", "status": "inactive", "score": 1},
                {"id": "2", "status": "active", "score": 3},
                {"id": "3", "status": "active", "score": 2},
            ],
            "predicates": [
                {"field": "status", "op": "equals", "value": "active"},
                {"field": "score", "op": "gte", "value": 2},
            ],
        }
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["output_rows"] == 2
    assert [row["id"] for row in receipt["records"]] == ["2", "3"]

    sorted_receipt = execute_data_job(
        {
            "operation": "sort",
            "records": receipt["records"],
            "sort": [{"field": "score", "direction": "desc"}],
        }
    )

    assert [row["id"] for row in sorted_receipt["records"]] == ["2", "3"]


def test_execute_data_job_joins_and_aggregates_inline_records() -> None:
    join_receipt = execute_data_job(
        {
            "operation": "join",
            "records": [
                {"user_id": "u1", "name": "Alice"},
                {"user_id": "u2", "name": "Bob"},
            ],
            "secondary_records": [
                {"user_id": "u1", "amount": 20},
                {"user_id": "u1", "amount": 5},
                {"user_id": "u3", "amount": 99},
            ],
            "keys": ["user_id"],
            "join_kind": "left",
            "right_prefix": "order_",
        }
    )

    assert join_receipt["ok"] is True
    assert join_receipt["stats"]["match_count"] == 2
    assert join_receipt["stats"]["left_only_count"] == 1
    assert join_receipt["records"][0]["order_amount"] == 20

    aggregate_receipt = execute_data_job(
        {
            "operation": "aggregate",
            "records": [
                {"status": "open", "amount": 10},
                {"status": "open", "amount": 5},
                {"status": "closed", "amount": 7},
            ],
            "group_by": ["status"],
            "aggregations": [
                {"op": "count", "as": "row_count"},
                {"op": "sum", "field": "amount", "as": "amount_total"},
            ],
        }
    )

    assert aggregate_receipt["ok"] is True
    rows_by_status = {row["status"]: row for row in aggregate_receipt["records"]}
    assert rows_by_status["open"]["row_count"] == 2
    assert rows_by_status["open"]["amount_total"] == 15
    assert rows_by_status["closed"]["amount_total"] == 7


def test_execute_data_job_redacts_and_exports_inline_records() -> None:
    redact_receipt = execute_data_job(
        {
            "operation": "redact",
            "records": [
                {"id": "1", "email": "alice@example.com", "ssn": "111-22-3333"},
            ],
            "redactions": {
                "email": "mask_email",
                "ssn": "remove",
            },
        }
    )

    assert redact_receipt["ok"] is True
    assert redact_receipt["stats"]["changed_rows"] == 1
    assert redact_receipt["records"][0]["email"] == "a***@example.com"
    assert "ssn" not in redact_receipt["records"][0]

    export_receipt = execute_data_job(
        {
            "operation": "export",
            "records": redact_receipt["records"],
            "fields": ["id", "email"],
            "field_map": {"email": "user_email"},
        }
    )

    assert export_receipt["ok"] is True
    assert export_receipt["records"] == [{"id": "1", "user_email": "a***@example.com"}]
    assert export_receipt["stats"]["target_fields"] == ["id", "user_email"]


def test_execute_data_job_merges_inline_sources() -> None:
    receipt = execute_data_job(
        {
            "operation": "merge",
            "records": [
                {"id": "1", "name": "Alice"},
                {"id": "1", "name": "Alice Duplicate"},
            ],
            "secondary_records": [
                {"id": "1", "status": "active"},
                {"id": "2", "status": "inactive"},
            ],
            "keys": ["id"],
            "merge_mode": "full",
            "precedence": "right",
        }
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["match_count"] == 1
    assert receipt["stats"]["right_only_count"] == 1
    assert receipt["stats"]["conflict_count"] == 1
    rows_by_id = {row["id"]: row for row in receipt["records"]}
    assert rows_by_id["1"]["name"] == "Alice"
    assert rows_by_id["1"]["status"] == "active"
    assert rows_by_id["2"]["status"] == "inactive"
    assert receipt["conflicts"][0]["side"] == "left"


def test_execute_data_job_splits_records_and_writes_partition_outputs(tmp_path: Path) -> None:
    receipt = execute_data_job(
        {
            "operation": "split",
            "records": [
                {"id": "1", "status": "active"},
                {"id": "2", "status": "inactive"},
                {"id": "3", "status": "active"},
            ],
            "split_by_field": "status",
            "output_path": "artifacts/data/users_by_status",
            "output_format": "jsonl",
        },
        workspace_root=tmp_path,
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["partition_count"] == 2
    assert receipt["partition_counts"] == {"active": 2, "inactive": 1}
    assert receipt["output"]["kind"] == "partition_directory"
    written_files = {Path(item["path"]).name: item for item in receipt["output"]["files"]}
    assert set(written_files) == {"active.jsonl", "inactive.jsonl"}
    active_rows = [
        json.loads(line)
        for line in Path(written_files["active.jsonl"]["path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [row["id"] for row in active_rows] == ["1", "3"]


def test_execute_data_job_syncs_target_state() -> None:
    receipt = execute_data_job(
        {
            "operation": "sync",
            "records": [
                {"id": "1", "email": "alice@example.com"},
                {"id": "2", "email": "bob@example.com"},
            ],
            "secondary_records": [
                {"id": "1", "email": "alice@old.example.com"},
                {"id": "3", "email": "carol@example.com"},
            ],
            "keys": ["id"],
            "sync_mode": "mirror",
        }
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["applied_create_count"] == 1
    assert receipt["stats"]["applied_update_count"] == 1
    assert receipt["stats"]["applied_delete_count"] == 1
    assert [row["id"] for row in receipt["records"]] == ["1", "2"]


def test_build_data_workflow_spec_uses_catalog_integration() -> None:
    spec = build_data_workflow_spec(
        {
            "operation": "dedupe",
            "records": [{"email": "a@example.com"}],
            "keys": ["email"],
        }
    )

    assert spec["jobs"][0]["integration_id"] == "praxis_data"
    assert spec["jobs"][0]["integration_action"] == "dedupe"
    assert spec["jobs"][0]["agent"] == "integration/praxis_data/dedupe"


def test_tool_praxis_data_workflow_spec_writes_queue_file(tmp_path: Path) -> None:
    spec_path = tmp_path / "artifacts" / "workflow" / "data.queue.json"
    original_root = data_tools.REPO_ROOT
    data_tools.REPO_ROOT = tmp_path
    try:
        payload = data_tools.tool_praxis_data(
            {
                "action": "workflow_spec",
                "workflow_spec_path": str(spec_path.relative_to(tmp_path)),
                "job": {
                    "operation": "normalize",
                    "records": [{"email": " A@Example.com "}],
                    "rules": {"email": ["trim", "lower"]},
                },
            }
        )
    finally:
        data_tools.REPO_ROOT = original_root

    assert payload["ok"] is True
    assert payload["workflow_spec_path"] == str(spec_path)
    written = json.loads(spec_path.read_text(encoding="utf-8"))
    assert written["jobs"][0]["integration_id"] == "praxis_data"
