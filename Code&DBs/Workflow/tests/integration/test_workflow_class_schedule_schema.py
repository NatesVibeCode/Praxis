from __future__ import annotations

from storage.migrations import (
    workflow_migration_expected_objects,
    workflow_migration_manifest,
)


def test_workflow_class_schedule_schema_is_in_the_canonical_manifest() -> None:
    filenames = [entry.filename for entry in workflow_migration_manifest()]
    assert "008_workflow_class_and_schedule_schema.sql" in filenames


def test_workflow_class_schedule_schema_expected_objects_are_declared() -> None:
    objects = workflow_migration_expected_objects("008_workflow_class_and_schedule_schema.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "workflow_classes",
            "schedule_definitions",
            "recurring_run_windows",
            "workflow_classes_name_status_idx",
            "workflow_classes_kind_lane_idx",
            "schedule_definitions_workflow_class_status_idx",
            "schedule_definitions_target_kind_idx",
            "recurring_run_windows_schedule_status_idx",
            "recurring_run_windows_window_status_idx",
        }
    )
