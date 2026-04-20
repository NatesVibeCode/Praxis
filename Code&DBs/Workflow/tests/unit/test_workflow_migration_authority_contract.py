from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pytest

from storage._generated_workflow_migration_authority import (
    WORKFLOW_FULL_BOOTSTRAP_SEQUENCE,
    WORKFLOW_MIGRATION_EXPECTED_OBJECTS,
    WORKFLOW_MIGRATION_POLICIES,
    WORKFLOW_MIGRATION_SEQUENCE,
    WORKFLOW_MIGRATION_TIE_BREAK_ORDER,
    WORKFLOW_POLICY_BUCKETS,
)
from system_authority.generate_workflow_migration_authority import (
    _tie_break_aware_full_bootstrap,
    _validate_tie_break_coverage,
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


def test_tie_break_order_matches_json_spec_without_db_bootstrap() -> None:
    spec = json.loads(
        (WORKFLOW_ROOT / "system_authority" / "workflow_migration_authority.json").read_text(
            encoding="utf-8"
        )
    )

    declared = {
        str(prefix): tuple(str(name) for name in filenames)
        for prefix, filenames in (spec.get("tie_break_order") or {}).items()
    }

    assert declared == WORKFLOW_MIGRATION_TIE_BREAK_ORDER


def test_tie_break_order_covers_every_duplicate_prefix_group_without_db_bootstrap() -> None:
    groups: dict[str, list[str]] = defaultdict(list)
    for filename in WORKFLOW_FULL_BOOTSTRAP_SEQUENCE:
        prefix = filename[:3]
        if prefix.isdigit():
            groups[prefix].append(filename)

    duplicate_groups = {
        prefix: sorted(members) for prefix, members in groups.items() if len(members) >= 2
    }

    assert set(duplicate_groups.keys()) == set(WORKFLOW_MIGRATION_TIE_BREAK_ORDER.keys())
    for prefix, on_disk in duplicate_groups.items():
        declared = WORKFLOW_MIGRATION_TIE_BREAK_ORDER[prefix]
        assert sorted(declared) == on_disk
        assert len(set(declared)) == len(declared), f"duplicate entries for prefix {prefix!r}"


def test_full_bootstrap_sequence_respects_tie_break_order_without_db_bootstrap() -> None:
    full_bootstrap_set = frozenset(
        set(WORKFLOW_POLICY_BUCKETS["canonical"]) | set(WORKFLOW_POLICY_BUCKETS["bootstrap_only"])
    )

    recomputed = _tie_break_aware_full_bootstrap(
        full_bootstrap=full_bootstrap_set,
        tie_break_order=WORKFLOW_MIGRATION_TIE_BREAK_ORDER,
    )

    assert recomputed == WORKFLOW_FULL_BOOTSTRAP_SEQUENCE


def test_validate_tie_break_coverage_raises_for_undeclared_duplicate_group_without_db_bootstrap() -> None:
    with pytest.raises(SystemExit) as excinfo:
        _validate_tie_break_coverage(
            full_bootstrap_filenames={"200_foo.sql", "200_bar.sql"},
            tie_break_order={},
        )

    assert "prefix '200'" in str(excinfo.value)
    assert "no tie_break_order entry" in str(excinfo.value)


def test_validate_tie_break_coverage_raises_for_mismatched_declared_set_without_db_bootstrap() -> None:
    with pytest.raises(SystemExit) as excinfo:
        _validate_tie_break_coverage(
            full_bootstrap_filenames={"200_foo.sql", "200_bar.sql"},
            tie_break_order={"200": ("200_foo.sql", "200_baz.sql")},
        )

    assert "prefix '200' tie_break_order mismatch" in str(excinfo.value)


def test_validate_tie_break_coverage_raises_for_orphan_entries_without_db_bootstrap() -> None:
    with pytest.raises(SystemExit) as excinfo:
        _validate_tie_break_coverage(
            full_bootstrap_filenames={"200_foo.sql"},
            tie_break_order={"200": ("200_foo.sql", "200_bar.sql")},
        )

    assert "orphan tie_break_order entries" in str(excinfo.value)


def test_validate_tie_break_coverage_raises_for_internal_duplicates_without_db_bootstrap() -> None:
    with pytest.raises(SystemExit) as excinfo:
        _validate_tie_break_coverage(
            full_bootstrap_filenames={"200_foo.sql", "200_bar.sql"},
            tie_break_order={"200": ("200_foo.sql", "200_foo.sql")},
        )

    assert "duplicate entries" in str(excinfo.value)


def test_validate_tie_break_coverage_accepts_real_spec_without_db_bootstrap() -> None:
    full_bootstrap_set = frozenset(
        set(WORKFLOW_POLICY_BUCKETS["canonical"]) | set(WORKFLOW_POLICY_BUCKETS["bootstrap_only"])
    )

    # Real spec must not raise
    _validate_tie_break_coverage(
        full_bootstrap_filenames=full_bootstrap_set,
        tie_break_order=WORKFLOW_MIGRATION_TIE_BREAK_ORDER,
    )
