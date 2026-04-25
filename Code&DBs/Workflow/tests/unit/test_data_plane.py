from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from contracts.data_contracts import DataContractError, normalize_data_job
import runtime.data_plane as data_plane
from runtime.data_plane import build_data_workflow_spec, execute_data_job
from surfaces.mcp.tools import data as data_tools


def test_normalize_data_job_requires_input_source() -> None:
    with pytest.raises(DataContractError, match="input source"):
        normalize_data_job({"operation": "profile"})


def test_supported_data_operations_cover_the_dispatch_surface() -> None:
    assert data_plane.SUPPORTED_DATA_OPERATIONS == {
        "parse",
        "profile",
        "filter",
        "sort",
        "normalize",
        "repair",
        "repair_loop",
        "backfill",
        "redact",
        "checkpoint",
        "replay",
        "approve",
        "apply",
        "validate",
        "transform",
        "join",
        "merge",
        "aggregate",
        "split",
        "export",
        "dead_letter",
        "dedupe",
        "reconcile",
        "sync",
    }


def test_normalize_data_job_accepts_plan_and_approval_manifest_ids() -> None:
    job = normalize_data_job(
        {
            "operation": "apply",
            "plan_manifest_id": "plan_manifest_123",
            "approval_manifest_id": "approval_manifest_123",
            "secondary_records": [{"id": "1"}],
            "keys": ["id"],
        }
    )

    assert job["plan"]["manifest_id"] == "plan_manifest_123"
    assert job["approval"]["manifest_id"] == "approval_manifest_123"


def test_normalize_data_job_accepts_checkpoint_manifest_id() -> None:
    job = normalize_data_job(
        {
            "operation": "replay",
            "records": [{"id": "1", "updated_at": "2025-01-01T00:00:00Z"}],
            "checkpoint_manifest_id": "checkpoint_manifest_123",
        }
    )

    assert job["checkpoint"]["manifest_id"] == "checkpoint_manifest_123"


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


def test_execute_data_job_persists_plan_manifest_when_pg_available() -> None:
    with patch.object(
        data_plane,
        "create_data_plan_manifest",
        return_value={
            "id": "plan_manifest_123",
            "version": 1,
            "status": "draft",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_plan",
                "status": "draft",
            },
        },
    ) as create_mock:
        receipt = execute_data_job(
            {
                "operation": "reconcile",
                "records": [{"id": "1", "email": "alice@example.com"}],
                "secondary_records": [{"id": "1", "email": "alice@old.example.com"}],
                "keys": ["id"],
            },
            pg_conn=object(),
        )

    create_mock.assert_called_once()
    assert receipt["plan_manifest_id"] == "plan_manifest_123"
    assert receipt["plan_authority"]["manifest_id"] == "plan_manifest_123"


def test_execute_data_job_persists_canonical_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _write_receipt(payload: dict[str, object]) -> None:
        captured.update(payload)

    monkeypatch.setattr("runtime.receipt_store.write_receipt", _write_receipt)

    receipt = execute_data_job(
        {
            "operation": "profile",
            "records": [
                {"id": "1", "status": "active"},
            ],
        }
    )

    assert receipt["ok"] is True
    assert captured["workflow_id"] == "data:profile-data-job"
    assert captured["agent_slug"] == "integration/praxis_data/profile"
    assert captured["status"] == "succeeded"
    assert isinstance(captured["outputs"], dict)
    assert captured["outputs"]["stats"]["row_count"] == 1


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


def test_execute_data_job_repairs_and_backfills_inline_records() -> None:
    repair_receipt = execute_data_job(
        {
            "operation": "repair",
            "records": [
                {"id": "1", "status": "pending", "name": "Alice", "legacy": "yes"},
                {"id": "2", "status": "active", "name": "Bob", "legacy": "yes"},
            ],
            "predicates": [{"field": "status", "op": "equals", "value": "pending"}],
            "repairs": {"status": {"value": "active"}},
            "drop_fields": ["legacy"],
        }
    )

    assert repair_receipt["ok"] is True
    assert repair_receipt["stats"]["matched_rows"] == 1
    assert repair_receipt["stats"]["changed_rows"] == 1
    assert repair_receipt["records"][0]["status"] == "active"
    assert "legacy" not in repair_receipt["records"][0]
    assert repair_receipt["records"][1]["legacy"] == "yes"

    backfill_receipt = execute_data_job(
        {
            "operation": "backfill",
            "records": repair_receipt["records"],
            "backfill": {"country": {"value": "US"}},
        }
    )

    assert backfill_receipt["ok"] is True
    assert backfill_receipt["stats"]["filled_rows"] == 2
    assert all(row["country"] == "US" for row in backfill_receipt["records"])


def test_execute_data_job_checkpoint_and_replay_records(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "artifacts" / "data" / "events.checkpoint.json"
    with patch.object(
        data_plane,
        "create_data_checkpoint_manifest",
        return_value={
            "id": "checkpoint_manifest_123",
            "version": 1,
            "status": "active",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_checkpoint",
                "status": "active",
            },
        },
    ) as create_checkpoint_mock, patch.object(
        data_plane,
        "load_control_plane_manifest",
        return_value={
            "id": "checkpoint_manifest_123",
            "version": 1,
            "status": "active",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_checkpoint",
                "status": "active",
                "checkpoint": {
                    "row_count": 2,
                    "field_count": 2,
                    "content_hash": "abc123",
                    "key_fields": ["id"],
                    "cursor_field": "updated_at",
                    "cursor_min": "2025-01-01T00:00:00Z",
                    "cursor_max": "2025-01-02T00:00:00Z",
                    "watermark": "2025-01-02T00:00:00Z",
                },
            },
        },
    ):
        checkpoint_receipt = execute_data_job(
            {
                "operation": "checkpoint",
                "records": [
                    {"id": "1", "updated_at": "2025-01-01T00:00:00Z"},
                    {"id": "2", "updated_at": "2025-01-02T00:00:00Z"},
                ],
                "keys": ["id"],
                "cursor_field": "updated_at",
                "output_path": str(checkpoint_path.relative_to(tmp_path)),
            },
            workspace_root=tmp_path,
            pg_conn=object(),
        )

        assert checkpoint_receipt["ok"] is True
        assert checkpoint_receipt["checkpoint"]["watermark"] == "2025-01-02T00:00:00Z"
        assert checkpoint_receipt["checkpoint_manifest_id"] == "checkpoint_manifest_123"
        assert checkpoint_receipt["output"]["path"] == str(checkpoint_path)
        create_checkpoint_mock.assert_called_once()

        replay_receipt = execute_data_job(
            {
                "operation": "replay",
                "records": [
                    {"id": "1", "updated_at": "2025-01-01T00:00:00Z"},
                    {"id": "2", "updated_at": "2025-01-02T00:00:00Z"},
                    {"id": "3", "updated_at": "2025-01-03T00:00:00Z"},
                ],
                "cursor_field": "updated_at",
                "checkpoint_manifest_id": "checkpoint_manifest_123",
            },
            workspace_root=tmp_path,
            pg_conn=object(),
        )

        assert replay_receipt["ok"] is True
        assert replay_receipt["checkpoint_authority"]["manifest_id"] == "checkpoint_manifest_123"
        assert replay_receipt["replay_window"]["after"] == "2025-01-02T00:00:00Z"
        assert [row["id"] for row in replay_receipt["records"]] == ["3"]


def test_execute_data_job_approve_requires_registry_backend() -> None:
    with pytest.raises(data_plane.DataRuntimeBoundaryError, match="Postgres-backed registry"):
        execute_data_job(
            {
                "operation": "approve",
                "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
                "approved_by": "ops",
                "approval_reason": "Reviewed diff and counts",
            }
        )


def test_execute_data_job_apply_requires_registry_backend() -> None:
    with pytest.raises(data_plane.DataRuntimeBoundaryError, match="Postgres-backed registry"):
        execute_data_job(
            {
                "operation": "apply",
                "approval_manifest_id": "approval_manifest_123",
                "secondary_records": [{"id": "1", "email": "alice@old.example.com"}],
                "keys": ["id"],
            }
        )


def test_execute_data_job_approves_and_applies_manifest_backed_plan() -> None:
    plan_payload = {
        "create": [{"key": {"id": "2"}, "record": {"id": "2", "email": "bob@example.com"}}],
        "update": [
            {
                "key": {"id": "1"},
                "diff": {"email": {"source": "alice@example.com", "target": "alice@old.example.com"}},
                "source_record": {"id": "1", "email": "alice@example.com"},
                "target_record": {"id": "1", "email": "alice@old.example.com"},
            }
        ],
        "delete": [],
        "noop": [],
        "conflicts": [],
    }
    digest = data_plane.plan_digest(plan_payload)

    def _fake_load_control_manifest(_conn: object, *, manifest_id: str, expected_type: str) -> dict[str, object]:
        if expected_type == data_plane.DATA_PLAN_MANIFEST_TYPE:
            assert manifest_id == "plan_manifest_123"
            return {
                "id": "plan_manifest_123",
                "version": 2,
                "status": "draft",
                "manifest": {
                    "kind": "praxis_control_manifest",
                    "manifest_family": "control_plane",
                    "manifest_type": "data_plan",
                    "status": "draft",
                    "plan": plan_payload,
                    "plan_digest": digest,
                    "plan_summary": {"create_count": 1, "update_count": 1, "delete_count": 0, "noop_count": 0, "conflict_count": 0},
                },
            }
        assert expected_type == data_plane.DATA_APPROVAL_MANIFEST_TYPE
        assert manifest_id == "approval_manifest_123"
        return {
            "id": "approval_manifest_123",
            "version": 1,
            "status": "approved",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_approval",
                "status": "approved",
                "approval": {
                    "plan_manifest_id": "plan_manifest_123",
                    "plan_digest": digest,
                    "approved_by": "ops",
                    "approval_reason": "Reviewed diff and counts",
                    "approved_at": "2026-04-15T12:00:00+00:00",
                },
                "plan_manifest_id": "plan_manifest_123",
                "plan_digest": digest,
            },
        }

    with patch.object(data_plane, "load_control_plane_manifest", side_effect=_fake_load_control_manifest), patch.object(
        data_plane,
        "create_data_approval_manifest",
        return_value={
            "id": "approval_manifest_123",
            "version": 1,
            "status": "approved",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_approval",
                "status": "approved",
            },
        },
    ) as create_approval_mock, patch.object(data_plane, "transition_data_plan_status") as transition_mock:
        approval_receipt = execute_data_job(
            {
                "operation": "approve",
                "plan_manifest_id": "plan_manifest_123",
                "approved_by": "ops",
                "approval_reason": "Reviewed diff and counts",
            },
            pg_conn=object(),
        )

        apply_receipt = execute_data_job(
            {
                "operation": "apply",
                "approval_manifest_id": "approval_manifest_123",
                "secondary_records": [{"id": "1", "email": "alice@old.example.com"}],
                "keys": ["id"],
            },
            pg_conn=object(),
        )

    create_approval_mock.assert_called_once()
    assert approval_receipt["plan_manifest_id"] == "plan_manifest_123"
    assert approval_receipt["approval_manifest_id"] == "approval_manifest_123"
    assert approval_receipt["plan_authority"]["status"] == "approved"
    assert apply_receipt["plan_manifest_id"] == "plan_manifest_123"
    assert apply_receipt["approval_manifest_id"] == "approval_manifest_123"
    assert [row["id"] for row in apply_receipt["records"]] == ["1", "2"]
    assert transition_mock.call_count == 2


def test_execute_data_job_runs_repair_loop() -> None:
    receipt = execute_data_job(
        {
            "operation": "repair_loop",
            "records": [
                {"id": "1", "email": " Alice@Example.com ", "status": "pending"},
                {"id": "2", "email": "", "status": "pending"},
            ],
            "repairs": {"status": {"value": "active"}},
            "rules": {"email": ["trim", "lower"]},
            "schema": {"email": {"required": True, "regex": ".+@.+"}},
            "max_passes": 3,
        }
    )

    assert receipt["ok"] is True
    assert receipt["stats"]["pass_count"] >= 1
    assert receipt["stats"]["accepted_rows"] == 1
    assert receipt["stats"]["dead_letter_rows"] == 1
    assert receipt["records"] == [{"id": "1", "email": "alice@example.com", "status": "active"}]
    assert receipt["passes"][0]["repair"]["changed_rows"] == 2


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


def test_execute_data_job_runs_checkpointed_batch_sync(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "artifacts" / "data" / "source.checkpoint.json"
    with patch.object(
        data_plane,
        "create_data_checkpoint_manifest",
        return_value={
            "id": "checkpoint_manifest_123",
            "version": 1,
            "status": "active",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_checkpoint",
                "status": "active",
            },
        },
    ), patch.object(
        data_plane,
        "load_control_plane_manifest",
        return_value={
            "id": "checkpoint_manifest_123",
            "version": 1,
            "status": "active",
            "manifest": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_checkpoint",
                "status": "active",
                "checkpoint": {
                    "row_count": 2,
                    "field_count": 2,
                    "content_hash": "abc123",
                    "key_fields": ["id"],
                    "cursor_field": "updated_at",
                    "cursor_min": "2025-01-01T00:00:00Z",
                    "cursor_max": "2025-01-02T00:00:00Z",
                    "watermark": "2025-01-02T00:00:00Z",
                },
            },
        },
    ):
        checkpoint_receipt = execute_data_job(
            {
                "operation": "checkpoint",
                "records": [
                    {"id": "1", "updated_at": "2025-01-01T00:00:00Z"},
                    {"id": "2", "updated_at": "2025-01-02T00:00:00Z"},
                ],
                "keys": ["id"],
                "cursor_field": "updated_at",
                "output_path": str(checkpoint_path.relative_to(tmp_path)),
            },
            workspace_root=tmp_path,
            pg_conn=object(),
        )

        assert checkpoint_receipt["ok"] is True
        assert checkpoint_receipt["checkpoint_manifest_id"] == "checkpoint_manifest_123"

        sync_receipt = execute_data_job(
            {
                "operation": "sync",
                "records": [
                    {"id": "1", "email": "alice@example.com", "updated_at": "2025-01-01T00:00:00Z"},
                    {"id": "2", "email": "bob@example.com", "updated_at": "2025-01-02T00:00:00Z"},
                    {"id": "3", "email": "carol@example.com", "updated_at": "2025-01-03T00:00:00Z"},
                ],
                "secondary_records": [
                    {"id": "1", "email": "alice@old.example.com", "updated_at": "2024-12-31T00:00:00Z"},
                    {"id": "2", "email": "bob@old.example.com", "updated_at": "2024-12-31T00:00:00Z"},
                ],
                "keys": ["id"],
                "sync_mode": "upsert",
                "cursor_field": "updated_at",
                "checkpoint_manifest_id": "checkpoint_manifest_123",
                "batch_size": 1,
            },
            workspace_root=tmp_path,
            pg_conn=object(),
        )

        assert sync_receipt["ok"] is True
        assert sync_receipt["checkpoint_authority"]["manifest_id"] == "checkpoint_manifest_123"
        assert sync_receipt["stats"]["batch_count"] == 1
        assert sync_receipt["replay_window"]["after"] == "2025-01-02T00:00:00Z"
        assert sync_receipt["checkpoint"]["watermark"] == "2025-01-03T00:00:00Z"
        assert [row["id"] for row in sync_receipt["records"]] == ["1", "2", "3"]
        assert sync_receipt["records"][0]["email"] == "alice@old.example.com"
        assert sync_receipt["records"][2]["email"] == "carol@example.com"


def test_execute_data_job_routes_dead_letter_rows() -> None:
    receipt = execute_data_job(
        {
            "operation": "dead_letter",
            "records": [
                {"id": "1", "email": "alice@example.com", "status": "active"},
                {"id": "2", "email": "", "status": "active"},
                {"id": "3", "email": "carol@example.com", "status": "blocked"},
            ],
            "schema": {
                "email": {"required": True, "regex": ".+@.+"},
            },
            "predicates": [{"field": "status", "op": "equals", "value": "blocked"}],
            "predicate_mode": "any",
        }
    )

    assert receipt["ok"] is True
    assert receipt["partition_counts"] == {"accepted": 1, "dead_letter": 2}
    assert receipt["stats"]["violation_count"] == 1
    dead_preview = receipt["partitions_preview"]["dead_letter"]["records_preview"]
    assert any(row["id"] == "2" for row in dead_preview)
    assert any(row["id"] == "3" for row in dead_preview)


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
