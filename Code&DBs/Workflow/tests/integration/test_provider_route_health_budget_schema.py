from __future__ import annotations

from storage.migrations import (
    workflow_migration_expected_objects,
    workflow_migration_manifest,
)


def test_provider_route_health_budget_schema_is_in_the_canonical_manifest() -> None:
    filenames = [entry.filename for entry in workflow_migration_manifest()]
    assert "007_provider_route_health_budget.sql" in filenames


def test_provider_route_health_budget_schema_expected_objects_are_declared() -> None:
    objects = workflow_migration_expected_objects("007_provider_route_health_budget.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "provider_route_health_windows",
            "provider_budget_windows",
            "route_eligibility_states",
            "provider_route_health_windows_provider_status_idx",
            "provider_route_health_windows_candidate_window_idx",
            "provider_budget_windows_provider_scope_status_idx",
            "provider_budget_windows_policy_window_idx",
            "route_eligibility_states_profile_candidate_status_idx",
            "route_eligibility_states_decision_ref_idx",
        }
    )
