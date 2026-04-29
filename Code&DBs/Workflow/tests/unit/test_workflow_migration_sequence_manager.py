from __future__ import annotations

import json
from pathlib import Path

import pytest

from system_authority.workflow_migration_sequence_manager import (
    normalize_workflow_migration_slug,
    propose_workflow_migration_filename,
    raise_for_unmanaged_duplicate_prefixes,
    renumber_unmanaged_duplicate_prefixes,
    load_workflow_migration_authority_spec,
    workflow_migration_sequence_state,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"


def _write_fake_workflow_tree(
    tmp_path: Path,
    *,
    filenames: list[str],
    tie_break_order: dict[str, list[str]] | None = None,
) -> Path:
    workflow_root = tmp_path / "Code&DBs" / "Workflow"
    migration_root = tmp_path / "Code&DBs" / "Databases" / "migrations" / "workflow"
    authority_root = workflow_root / "system_authority"
    migration_root.mkdir(parents=True)
    authority_root.mkdir(parents=True)
    for name in filenames:
        (migration_root / name).write_text("-- test\n", encoding="utf-8")
    spec = {
        "canonical_manifest": filenames,
        "policy_buckets": {"canonical": filenames, "bootstrap_only": [], "deprecated": [], "dead": []},
        "expected_objects": {name: [] for name in filenames},
        "tie_break_order": tie_break_order or {},
    }
    (authority_root / "workflow_migration_authority.json").write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return workflow_root


def test_normalize_workflow_migration_slug_normalizes_words() -> None:
    assert normalize_workflow_migration_slug("Repo Policy Onboarding!") == "repo_policy_onboarding"


def test_normalize_workflow_migration_slug_rejects_empty_input() -> None:
    with pytest.raises(ValueError):
        normalize_workflow_migration_slug("...")


def test_sequence_state_reports_managed_and_unmanaged_duplicate_prefixes(tmp_path: Path) -> None:
    workflow_root = _write_fake_workflow_tree(
        tmp_path,
        filenames=[
            "100_first.sql",
            "101_alpha.sql",
            "101_beta.sql",
            "102_gamma.sql",
            "102_delta.sql",
        ],
        tie_break_order={"101": ["101_alpha.sql", "101_beta.sql"]},
    )

    state = workflow_migration_sequence_state(workflow_root)

    assert state.highest_prefix == 102
    assert state.next_prefix == 103
    assert state.managed_duplicate_prefixes == {"101": ("101_alpha.sql", "101_beta.sql")}
    assert state.unmanaged_duplicate_prefixes == {"102": ("102_delta.sql", "102_gamma.sql")}


def test_legacy_duplicate_guard_auto_repairs_instead_of_failing_closed(tmp_path: Path) -> None:
    workflow_root = _write_fake_workflow_tree(
        tmp_path,
        filenames=["100_first.sql", "100_second.sql", "101_next.sql"],
    )

    raise_for_unmanaged_duplicate_prefixes(workflow_root)
    state = workflow_migration_sequence_state(workflow_root)
    migration_root = workflow_root.parent / "Databases" / "migrations" / "workflow"

    assert state.unmanaged_duplicate_prefixes == {}
    assert not (migration_root / "100_second.sql").exists()
    assert (migration_root / "102_second.sql").exists()


def test_renumber_unmanaged_duplicate_prefixes_repairs_files_and_spec(tmp_path: Path) -> None:
    workflow_root = _write_fake_workflow_tree(
        tmp_path,
        filenames=["100_alpha.sql", "100_beta.sql", "101_next.sql"],
    )

    dry_run = renumber_unmanaged_duplicate_prefixes(workflow_root)

    assert [action.to_dict() for action in dry_run] == [
        {
            "old_filename": "100_beta.sql",
            "new_filename": "102_beta.sql",
            "reason": "unmanaged duplicate prefix 100; kept 100_alpha.sql",
        }
    ]

    applied = renumber_unmanaged_duplicate_prefixes(workflow_root, apply=True)
    state = workflow_migration_sequence_state(workflow_root)
    spec = load_workflow_migration_authority_spec(workflow_root)
    migration_root = workflow_root.parent / "Databases" / "migrations" / "workflow"

    assert tuple(action.to_dict() for action in applied) == tuple(action.to_dict() for action in dry_run)
    assert not (migration_root / "100_beta.sql").exists()
    assert (migration_root / "102_beta.sql").exists()
    assert state.unmanaged_duplicate_prefixes == {}
    assert "102_beta.sql" in spec["canonical_manifest"]
    assert "100_beta.sql" not in spec["canonical_manifest"]


def test_propose_workflow_migration_filename_uses_real_repo_next_prefix() -> None:
    state = workflow_migration_sequence_state(WORKFLOW_ROOT)
    proposed = propose_workflow_migration_filename(
        slug="repo policy onboarding",
        workflow_root=WORKFLOW_ROOT,
    )

    assert proposed == f"{state.next_prefix:03d}_repo_policy_onboarding.sql"
    assert state.unmanaged_duplicate_prefixes == {}
