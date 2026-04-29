from __future__ import annotations

import json
from pathlib import Path

import pytest

from system_authority.workflow_migration_sequence_manager import (
    normalize_workflow_migration_slug,
    propose_workflow_migration_filename,
    raise_for_unmanaged_duplicate_prefixes,
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


def test_raise_for_unmanaged_duplicate_prefixes_points_to_allocator(tmp_path: Path) -> None:
    workflow_root = _write_fake_workflow_tree(
        tmp_path,
        filenames=["100_first.sql", "100_second.sql"],
    )

    with pytest.raises(ValueError) as excinfo:
        raise_for_unmanaged_duplicate_prefixes(workflow_root)

    assert "workflow schema next-migration <slug>" in str(excinfo.value)


def test_propose_workflow_migration_filename_uses_real_repo_next_prefix() -> None:
    state = workflow_migration_sequence_state(WORKFLOW_ROOT)
    proposed = propose_workflow_migration_filename(
        slug="repo policy onboarding",
        workflow_root=WORKFLOW_ROOT,
    )

    assert proposed == f"{state.next_prefix:03d}_repo_policy_onboarding.sql"
    assert state.unmanaged_duplicate_prefixes == {}
