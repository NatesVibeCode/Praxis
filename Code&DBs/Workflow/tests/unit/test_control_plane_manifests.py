from __future__ import annotations

from unittest.mock import patch

import pytest

import registry.control_plane_manifests as control_plane_manifests


def test_create_data_plan_manifest_shapes_raw_control_manifest() -> None:
    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock, patch.object(
        control_plane_manifests,
        "bootstrap_control_manifest_head_schema",
    ), patch.object(
        control_plane_manifests,
        "load_control_manifest_head_record",
        return_value=None,
    ), patch.object(
        control_plane_manifests,
        "upsert_control_manifest_head",
        return_value={
            "manifest_id": "data-plan-123",
            "head_status": "draft",
            "recorded_at": "2026-04-15T12:00:00+00:00",
        },
    ):
        row = control_plane_manifests.create_data_plan_manifest(
            object(),
            plan={"create": [{"key": {"id": "1"}, "record": {"id": "1"}}], "update": [], "delete": [], "noop": [], "conflicts": []},
            compare_fields=["email"],
            job={"operation": "reconcile"},
            workspace_root="/tmp/workspace",
            name="User reconcile plan",
        )

    create_mock.assert_called_once()
    history_mock.assert_called_once()
    manifest = create_mock.call_args.kwargs["manifest"]
    assert manifest["kind"] == "praxis_control_manifest"
    assert manifest["manifest_family"] == "control_plane"
    assert manifest["manifest_type"] == "data_plan"
    assert manifest["status"] == "draft"
    assert manifest["workspace_ref"] == "workspace_root:/tmp/workspace"
    assert manifest["scope_ref"] == "data_operation:reconcile"
    assert manifest["compare_fields"] == ["email"]
    assert row["manifest"]["plan_summary"]["create_count"] == 1
    assert row["is_current_head"] is True


def test_create_data_approval_manifest_links_to_parent_plan_manifest() -> None:
    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock, patch.object(
        control_plane_manifests,
        "load_control_plane_manifest",
        return_value={
            "workspace_ref": "workspace_root:/tmp/workspace",
            "scope_ref": "data_job:user-sync",
        },
    ), patch.object(
        control_plane_manifests,
        "bootstrap_control_manifest_head_schema",
    ), patch.object(
        control_plane_manifests,
        "load_control_manifest_head_record",
        return_value=None,
    ), patch.object(
        control_plane_manifests,
        "upsert_control_manifest_head",
        return_value={
            "manifest_id": "data-approval-123",
            "head_status": "approved",
            "recorded_at": "2026-04-15T12:00:00+00:00",
        },
    ):
        row = control_plane_manifests.create_data_approval_manifest(
            object(),
            plan_manifest_id="plan_manifest_123",
            plan={"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
            approved_by="ops",
            approval_reason="Reviewed plan",
            approved_at="2026-04-15T12:00:00+00:00",
        )

    create_mock.assert_called_once()
    history_mock.assert_called_once()
    assert create_mock.call_args.kwargs["parent_manifest_id"] == "plan_manifest_123"
    manifest = create_mock.call_args.kwargs["manifest"]
    assert manifest["manifest_type"] == "data_approval"
    assert manifest["plan_manifest_id"] == "plan_manifest_123"
    assert manifest["workspace_ref"] == "workspace_root:/tmp/workspace"
    assert manifest["scope_ref"] == "data_job:user-sync"
    assert manifest["approval"]["approved_by"] == "ops"
    assert row["parent_manifest_id"] == "plan_manifest_123"


def test_create_data_checkpoint_manifest_shapes_control_manifest() -> None:
    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock, patch.object(
        control_plane_manifests,
        "bootstrap_control_manifest_head_schema",
    ), patch.object(
        control_plane_manifests,
        "load_control_manifest_head_record",
        return_value=None,
    ), patch.object(
        control_plane_manifests,
        "upsert_control_manifest_head",
        return_value={
            "manifest_id": "data-checkpoint-123",
            "head_status": "active",
            "recorded_at": "2026-04-15T12:00:00+00:00",
        },
    ):
        row = control_plane_manifests.create_data_checkpoint_manifest(
            object(),
            checkpoint={
                "row_count": 2,
                "field_count": 2,
                "content_hash": "abc123",
                "cursor_field": "updated_at",
                "watermark": "2025-01-02T00:00:00Z",
            },
            job={"operation": "checkpoint", "job_name": "events-checkpoint"},
            workspace_root="/tmp/workspace",
        )

    create_mock.assert_called_once()
    history_mock.assert_called_once()
    manifest = create_mock.call_args.kwargs["manifest"]
    assert manifest["manifest_type"] == "data_checkpoint"
    assert manifest["status"] == "active"
    assert manifest["workspace_ref"] == "workspace_root:/tmp/workspace"
    assert manifest["scope_ref"] == "data_job:events-checkpoint"
    assert manifest["checkpoint"]["content_hash"] == "abc123"
    assert row["is_current_head"] is True


def test_load_control_plane_manifest_rejects_wrong_kind() -> None:
    with patch.object(
        control_plane_manifests,
        "load_app_manifest_record",
        return_value={
            "id": "manifest_123",
            "name": "Bad Manifest",
            "status": "draft",
            "version": 1,
            "manifest": {"kind": "helm_surface_bundle"},
        },
    ):
        with pytest.raises(control_plane_manifests.ControlPlaneManifestBoundaryError, match="praxis_control_manifest"):
            control_plane_manifests.load_control_plane_manifest(
                object(),
                manifest_id="manifest_123",
                expected_type=control_plane_manifests.DATA_PLAN_MANIFEST_TYPE,
            )


def test_create_data_plan_manifest_supersedes_prior_head() -> None:
    existing_head = {
        "id": "plan_manifest_old",
        "head_manifest_id": "plan_manifest_old",
        "name": "Old Plan",
        "description": "old",
        "status": "approved",
        "version": 2,
        "parent_manifest_id": None,
        "recorded_at": "2026-04-14T12:00:00+00:00",
        "manifest": {
            "kind": "praxis_control_manifest",
            "manifest_family": "control_plane",
            "manifest_type": "data_plan",
            "schema_version": 1,
            "workspace_ref": "workspace_root:/tmp/workspace",
            "scope_ref": "data_job:user-sync",
            "status": "approved",
            "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
            "plan_digest": "abc",
            "plan_summary": {"create_count": 0, "update_count": 0, "delete_count": 0, "noop_count": 0, "conflict_count": 0},
        },
    }

    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock, patch.object(
        control_plane_manifests,
        "bootstrap_control_manifest_head_schema",
    ), patch.object(
        control_plane_manifests,
        "load_control_manifest_head_record",
        return_value=existing_head,
    ), patch.object(control_plane_manifests, "upsert_app_manifest") as upsert_manifest_mock, patch.object(
        control_plane_manifests,
        "upsert_control_manifest_head",
        return_value={
            "manifest_id": "data-plan-123",
            "head_status": "draft",
            "recorded_at": "2026-04-15T12:00:00+00:00",
        },
    ):
        control_plane_manifests.create_data_plan_manifest(
            object(),
            plan={"create": [{"key": {"id": "1"}, "record": {"id": "1"}}], "update": [], "delete": [], "noop": [], "conflicts": []},
            job={"job_name": "user-sync"},
            workspace_root="/tmp/workspace",
        )

    create_mock.assert_called_once()
    history_mock.assert_called()
    assert upsert_manifest_mock.call_count == 1
    assert upsert_manifest_mock.call_args.kwargs["status"] == "superseded"


def test_transition_control_manifest_status_updates_storage_owner_and_head() -> None:
    existing_row = {
        "id": "plan_manifest_123",
        "name": "User reconcile plan",
        "description": "desc",
        "status": "draft",
        "version": 2,
        "parent_manifest_id": None,
        "head_manifest_id": "plan_manifest_123",
        "recorded_at": "2026-04-15T11:00:00+00:00",
        "manifest": {
            "kind": "praxis_control_manifest",
            "manifest_family": "control_plane",
            "manifest_type": "data_plan",
            "schema_version": 1,
            "workspace_ref": "workspace_root:/tmp/workspace",
            "scope_ref": "data_job:user-sync",
            "status": "draft",
            "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
            "plan_digest": "abc",
            "plan_summary": {"create_count": 0, "update_count": 0, "delete_count": 0, "noop_count": 0, "conflict_count": 0},
        },
    }

    with patch.object(
        control_plane_manifests,
        "load_app_manifest_record",
        return_value=existing_row,
    ), patch.object(control_plane_manifests, "upsert_app_manifest") as upsert_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock, patch.object(
        control_plane_manifests,
        "load_control_manifest_head_record",
        return_value=existing_row,
    ), patch.object(
        control_plane_manifests,
        "upsert_control_manifest_head",
        return_value={
            "manifest_id": "plan_manifest_123",
            "head_status": "approved",
            "recorded_at": "2026-04-15T12:00:00+00:00",
        },
    ) as head_mock:
        row = control_plane_manifests.transition_control_manifest_status(
            object(),
            manifest_id="plan_manifest_123",
            to_status="approved",
            changed_by="ops",
            change_description="Approved data plan",
        )

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args.kwargs["status"] == "approved"
    assert upsert_mock.call_args.kwargs["version"] == 3
    history_mock.assert_called_once()
    head_mock.assert_called_once()
    assert row["status"] == "approved"
    assert row["manifest"]["status"] == "approved"
    assert row["head_status"] == "approved"
