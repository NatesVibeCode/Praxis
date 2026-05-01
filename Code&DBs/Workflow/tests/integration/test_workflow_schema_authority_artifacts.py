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
from storage.postgres.receipt_repository import (
    _COMPILE_AUTHORITY_READINESS_OBJECTS,
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


def test_mobile_v1_archive_updates_final_schema_expectations() -> None:
    assert "220_archive_mobile_v1.sql" in WORKFLOW_MIGRATION_SEQUENCE
    assert WORKFLOW_MIGRATION_EXPECTED_OBJECTS["220_archive_mobile_v1.sql"] == (
        ("absent_table", "mobile_session_budget_events"),
        ("absent_table", "mobile_sessions"),
        ("absent_table", "mobile_bootstrap_tokens"),
        ("absent_table", "webauthn_challenges"),
        ("absent_table", "approval_requests"),
        ("absent_table", "device_enrollments"),
    )
    assert WORKFLOW_MIGRATION_EXPECTED_OBJECTS["187_webauthn_challenges.sql"] == ()
    assert WORKFLOW_MIGRATION_EXPECTED_OBJECTS["188_mobile_sessions.sql"] == ()

    capability_objects = {
        object_name
        for object_type, object_name in WORKFLOW_MIGRATION_EXPECTED_OBJECTS[
            "185_mobile_capability_ledger.sql"
        ]
        if object_type == "table"
    }
    assert capability_objects == {"capability_grants"}


def test_row_expected_objects_are_inspectable_by_schema_readiness() -> None:
    from storage.postgres.schema import _ROW_EXPECTATION_KEY_COLUMNS

    row_tables = {
        object_name.partition(".")[0]
        for objects in WORKFLOW_MIGRATION_EXPECTED_OBJECTS.values()
        for object_type, object_name in objects
        if object_type == "row"
    }

    special_case_row_tables = {"private_provider_api_job_allowlist"}

    assert row_tables <= set(_ROW_EXPECTATION_KEY_COLUMNS) | special_case_row_tables


def test_new_non_archive_migrations_do_not_hide_empty_readiness_contracts() -> None:
    empty_readiness = {
        filename
        for filename, objects in WORKFLOW_MIGRATION_EXPECTED_OBJECTS.items()
        if not objects
    }

    assert empty_readiness == {
        "187_webauthn_challenges.sql",
        "188_mobile_sessions.sql",
    }


def test_proof_metrics_compile_authority_reads_readiness_tables_from_schema_authority() -> None:
    readiness_tables = {
        object_name
        for _, objects in WORKFLOW_SCHEMA_READINESS_SEQUENCE
        for object_type, object_name in objects
        if object_type == "table"
    }
    expected_compile_authority_objects = (
        "materialize_artifacts",
        "capability_catalog",
        "verify_refs",
        "verification_registry",
        "materialize_index_snapshots",
        "execution_packets",
        "repo_snapshots",
        "verifier_registry",
        "healer_registry",
        "verifier_healer_bindings",
        "verification_runs",
        "healing_runs",
    )
    assert _COMPILE_AUTHORITY_READINESS_OBJECTS == tuple(
        object_name
        for object_name in expected_compile_authority_objects
        if object_name in readiness_tables
    )


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
