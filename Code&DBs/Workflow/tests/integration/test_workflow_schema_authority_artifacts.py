from __future__ import annotations

import json
from pathlib import Path

from storage._generated_workflow_migration_authority import (
    WORKFLOW_FULL_BOOTSTRAP_SEQUENCE,
    WORKFLOW_MIGRATION_EXPECTED_OBJECTS,
    WORKFLOW_MIGRATION_POLICIES,
    WORKFLOW_MIGRATION_SEQUENCE,
    WORKFLOW_POLICY_BUCKETS,
    WORKFLOW_SCHEMA_READINESS_SEQUENCE,
)


def test_generated_workflow_authority_matches_json_spec() -> None:
    workflow_root = Path(__file__).resolve().parents[2]
    spec = json.loads(
        (workflow_root / "system_authority" / "workflow_migration_authority.json").read_text(
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


def test_generated_readiness_sequence_matches_manifest_and_expected_objects() -> None:
    assert tuple(filename for filename, _objects in WORKFLOW_SCHEMA_READINESS_SEQUENCE) == (
        WORKFLOW_MIGRATION_SEQUENCE
    )
    assert dict(WORKFLOW_SCHEMA_READINESS_SEQUENCE) == WORKFLOW_MIGRATION_EXPECTED_OBJECTS


def test_every_numbered_sql_file_is_classified_by_policy() -> None:
    workflow_root = Path(__file__).resolve().parents[2]
    migration_root = workflow_root.parent / "Databases" / "migrations" / "workflow"
    numbered_files = tuple(sorted(path.name for path in migration_root.glob("[0-9][0-9][0-9]_*.sql")))
    classified_files = tuple(sorted(WORKFLOW_MIGRATION_POLICIES))
    assert classified_files == numbered_files



def test_policy_buckets_partition_numbered_sql_files_without_overlap() -> None:
    all_bucketed = []
    for filenames in WORKFLOW_POLICY_BUCKETS.values():
        all_bucketed.extend(filenames)
    assert len(all_bucketed) == len(set(all_bucketed))
    assert tuple(sorted(all_bucketed)) == tuple(sorted(WORKFLOW_MIGRATION_POLICIES))



def test_full_bootstrap_sequence_is_derived_from_bootstrap_policies() -> None:
    expected = tuple(
        sorted(
            set(WORKFLOW_POLICY_BUCKETS["canonical"])
            | set(WORKFLOW_POLICY_BUCKETS["bootstrap_only"])
        )
    )
    assert WORKFLOW_FULL_BOOTSTRAP_SEQUENCE == expected
