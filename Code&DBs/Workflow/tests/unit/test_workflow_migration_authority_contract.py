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


# Allowlist of bootstrap_only migrations that deliberately declare no
# expected_objects because they do not produce durable schema — renames,
# drops, data seeds, and branding/name cleanups. Any new bootstrap_only
# entry with empty expected_objects must be reviewed and explicitly added
# here, preventing the bucket from silently becoming a catch-all dumping
# ground again (closes decision.2026-04-19 bootstrap_only-entries-declare-
# or-retire).
_NON_DURABLE_BOOTSTRAP_ONLY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "043_workflow_runtime_notification_sync_rename.sql",
        "045_workflow_authority_rename.sql",
        "046_workflow_surface_rename.sql",
        "051_integration_registry_authority.sql",
        "056_native_runtime_profile_authority_repair.sql",
        "057_remove_legacy_dispatch_completion_triggers.sql",
        "058_retire_stale_legacy_runner_runs.sql",
        "061_drop_stale_dispatch_completion_function.sql",
        "062_add_research_task_type_route_profile.sql",
        "063_drop_system_events_processed.sql",
        "063_remove_stale_anthropic_provider_disabled_eligibility.sql",
        "064_prune_stale_completion_functions.sql",
        "065_daily_maintenance_review_dispatch.sql",
        "067_rename_maintenance_start_terms.sql",
        "068_rename_maintenance_start_config_keys.sql",
        "091_openrouter_deepseek_onboarding.sql",
        "093_deepseek_direct_provider.sql",
        "099_webhook_notify_trigger.sql",
        "101_analysis_classify_routing.sql",
        "107_name_cleanup_praxis_branding.sql",
        "107_workflow_notifications_resource_telemetry.sql",
    }
)


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


def test_bootstrap_only_entries_either_declare_objects_or_are_allowlisted() -> None:
    """bootstrap_only migrations must declare expected_objects or be allowlisted.

    The bucket previously served as a cosmetic dumping ground: all 97 entries
    carried empty expected_objects and therefore escaped readiness-gating.
    The reclassification commit promoted 76 durable-schema migrations to
    canonical and left 21 non-durable ones (renames, drops, data seeds) in
    bootstrap_only with intentionally empty expected_objects. This test pins
    those 21 so:

      * adding a new bootstrap_only entry with empty expected_objects now
        requires an explicit allowlist update — forcing human review that
        the migration really has no durable schema effect; and
      * removing or reclassifying an existing allowlist entry must also
        update the allowlist — preventing silent drift.

    Pins decision.2026-04-19.bootstrap-only-entries-declare-or-retire.
    """
    current_empty_bootstrap_only = frozenset(
        migration
        for migration in WORKFLOW_POLICY_BUCKETS["bootstrap_only"]
        if not WORKFLOW_MIGRATION_EXPECTED_OBJECTS.get(migration)
    )

    assert current_empty_bootstrap_only == _NON_DURABLE_BOOTSTRAP_ONLY_ALLOWLIST


def test_non_durable_bootstrap_only_allowlist_members_remain_in_bucket() -> None:
    """Every allowlisted migration must still sit in bootstrap_only.

    Catches the case where an allowlisted migration is silently reclassified
    or removed without updating the pin — the allowlist would then reference
    stale file names that no longer correspond to bootstrap_only entries.
    """
    bootstrap_only_set = frozenset(WORKFLOW_POLICY_BUCKETS["bootstrap_only"])
    orphans = _NON_DURABLE_BOOTSTRAP_ONLY_ALLOWLIST - bootstrap_only_set

    assert not orphans, (
        "Allowlisted non-durable bootstrap_only migrations no longer sit in "
        "the bootstrap_only bucket — remove them from the allowlist or "
        "reclassify them explicitly:\n  - " + "\n  - ".join(sorted(orphans))
    )


def test_operation_replay_receipt_index_allows_repeated_read_only_observations() -> None:
    sql_text = (MIGRATION_ROOT / "212_operation_replay_receipts.sql").read_text(
        encoding="utf-8"
    )

    assert "execution_status = 'completed'" in sql_text
    assert "operation_kind = 'command'" in sql_text
    assert "execution_status IN ('completed', 'replayed')" not in sql_text
