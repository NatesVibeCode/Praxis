from __future__ import annotations

from unittest.mock import patch

import pytest

import runtime.control_plane_manifests as control_plane_manifests


def test_create_data_plan_manifest_shapes_raw_control_manifest() -> None:
    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock:
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
    assert manifest["compare_fields"] == ["email"]
    assert row["manifest"]["plan_summary"]["create_count"] == 1


def test_create_data_approval_manifest_links_to_parent_plan_manifest() -> None:
    with patch.object(control_plane_manifests, "create_app_manifest") as create_mock, patch.object(
        control_plane_manifests,
        "record_app_manifest_history",
    ) as history_mock:
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
    assert manifest["approval"]["approved_by"] == "ops"
    assert row["parent_manifest_id"] == "plan_manifest_123"


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


def test_transition_data_plan_status_updates_storage_owner_and_history() -> None:
    existing_row = {
        "id": "plan_manifest_123",
        "name": "User reconcile plan",
        "description": "desc",
        "status": "draft",
        "version": 2,
        "parent_manifest_id": None,
        "manifest": {
            "kind": "praxis_control_manifest",
            "manifest_family": "control_plane",
            "manifest_type": "data_plan",
            "schema_version": 1,
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
    ) as history_mock:
        row = control_plane_manifests.transition_data_plan_status(
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
    assert row["status"] == "approved"
    assert row["manifest"]["status"] == "approved"
