from __future__ import annotations

import json
from pathlib import Path

from storage._generated_workflow_migration_authority import (
    WORKFLOW_MIGRATION_EXPECTED_OBJECTS,
    WORKFLOW_MIGRATION_POLICIES,
    WORKFLOW_MIGRATION_SEQUENCE,
    WORKFLOW_POLICY_BUCKETS,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
MIGRATION_ROOT = REPO_ROOT / "Code&DBs" / "Databases" / "migrations" / "workflow"


def test_generated_workflow_authority_matches_json_spec_without_db_bootstrap() -> None:
    spec = json.loads(
        (WORKFLOW_ROOT / "system_authority" / "workflow_migration_authority.json").read_text(
            encoding="utf-8"
        )
    )

    assert tuple(spec["canonical_manifest"]) == WORKFLOW_MIGRATION_SEQUENCE
    assert tuple(spec["expected_objects"].keys()) == WORKFLOW_MIGRATION_SEQUENCE
    assert {
        policy: tuple(filenames)
        for policy, filenames in spec["policy_buckets"].items()
    } == WORKFLOW_POLICY_BUCKETS
    assert {
        filename: [
            {"object_type": object_type, "object_name": object_name}
            for object_type, object_name in objects
        ]
        for filename, objects in WORKFLOW_MIGRATION_EXPECTED_OBJECTS.items()
    } == spec["expected_objects"]


def test_every_numbered_sql_file_is_classified_without_db_bootstrap() -> None:
    numbered_files = tuple(sorted(path.name for path in MIGRATION_ROOT.glob("[0-9][0-9][0-9]_*.sql")))
    classified_files = tuple(sorted(WORKFLOW_MIGRATION_POLICIES))

    assert classified_files == numbered_files
