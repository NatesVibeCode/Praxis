from __future__ import annotations

import pytest

from storage.migrations import (
    WorkflowMigrationPathError,
    workflow_bootstrap_migration_path,
    workflow_migration_path,
)


CANONICAL_MIGRATION = "081_observability_lineage_and_metrics.sql"
BOOTSTRAP_ONLY_MIGRATION = "082_event_log.sql"


def test_canonical_lookup_allows_canonical_migrations() -> None:
    path = workflow_migration_path(CANONICAL_MIGRATION)
    assert path.name == CANONICAL_MIGRATION



def test_canonical_lookup_rejects_bootstrap_only_migrations() -> None:
    with pytest.raises(WorkflowMigrationPathError) as excinfo:
        workflow_migration_path(BOOTSTRAP_ONLY_MIGRATION)
    assert excinfo.value.reason_code == "workflow.migration_policy_forbidden"



def test_bootstrap_lookup_allows_bootstrap_only_migrations() -> None:
    path = workflow_bootstrap_migration_path(BOOTSTRAP_ONLY_MIGRATION)
    assert path.name == BOOTSTRAP_ONLY_MIGRATION
