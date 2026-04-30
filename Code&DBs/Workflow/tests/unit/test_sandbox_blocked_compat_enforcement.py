"""Pass 5 tests: sandbox refuses extensions of blocked compat paths."""

from __future__ import annotations

import pytest

from runtime.sandbox_runtime import (
    _authority_binding_blocked_paths,
    _validated_blocked_compat_refs,
)


def _binding_with_blocked(*entries: dict[str, object]) -> dict[str, object]:
    return {
        "canonical_write_scope": [],
        "predecessor_obligations": [],
        "blocked_compat_units": list(entries),
        "unresolved_targets": [],
        "notes": [],
    }


def _blocked_entry(
    *,
    predecessor_path: str,
    successor_kind: str = "source_path",
    successor_ref: str = "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py",
    status: str = "compat",
    summary: str | None = "preserve legacy field shape",
) -> dict[str, object]:
    return {
        "predecessor_unit_kind": "source_path",
        "predecessor_unit_ref": predecessor_path,
        "successor_unit_kind": successor_kind,
        "successor_unit_ref": successor_ref,
        "supersession_status": status,
        "obligation_summary": summary,
        "obligation_evidence": {},
    }


def test_blocked_paths_returns_empty_for_missing_metadata() -> None:
    assert _authority_binding_blocked_paths(None) == ()
    assert _authority_binding_blocked_paths({}) == ()
    assert _authority_binding_blocked_paths({"authority_binding": None}) == ()
    assert _authority_binding_blocked_paths({"authority_binding": {}}) == ()


def test_blocked_paths_filters_to_source_path_entries_only() -> None:
    metadata = {
        "authority_binding": _binding_with_blocked(
            _blocked_entry(predecessor_path="Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"),
            {
                "predecessor_unit_kind": "operation_ref",
                "predecessor_unit_ref": "old_op",
                "successor_unit_kind": "operation_ref",
                "successor_unit_ref": "new_op",
                "supersession_status": "compat",
                "obligation_summary": "n/a",
            },
        )
    }
    blocked = _authority_binding_blocked_paths(metadata)
    assert len(blocked) == 1
    assert blocked[0]["blocked_path"] == "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"
    assert blocked[0]["successor_unit_ref"] == "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py"


def test_validated_blocked_compat_refs_passes_when_no_overlap() -> None:
    blocked = (
        {
            "blocked_path": "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py",
            "successor_unit_kind": "source_path",
            "successor_unit_ref": "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py",
            "supersession_status": "compat",
            "obligation_summary": None,
        },
    )
    drift = _validated_blocked_compat_refs(
        ["Code&DBs/Workflow/runtime/operations/commands/foo_v2.py", "docs/README.md"],
        blocked_compat=blocked,
        submission_required=True,
    )
    assert drift == ()


def test_validated_blocked_compat_refs_captures_drift_on_submission_contract() -> None:
    blocked = tuple(_authority_binding_blocked_paths({
        "authority_binding": _binding_with_blocked(
            _blocked_entry(predecessor_path="Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"),
        )
    }))
    drift = _validated_blocked_compat_refs(
        ["Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"],
        blocked_compat=blocked,
        submission_required=True,
    )
    assert len(drift) == 1
    record = drift[0]
    assert record["reason"] == "blocked_compat_path_extension"
    assert record["blocked_predecessor_path"] == "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"
    assert record["canonical_successor"]["unit_ref"] == "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py"
    assert "do_not_imitate" in record["guidance"]
    assert record["submission_required"] is True


def test_validated_blocked_compat_refs_raises_on_legacy_contract() -> None:
    blocked = tuple(_authority_binding_blocked_paths({
        "authority_binding": _binding_with_blocked(
            _blocked_entry(predecessor_path="Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"),
        )
    }))
    with pytest.raises(RuntimeError, match="blocked compat predecessor paths"):
        _validated_blocked_compat_refs(
            ["Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"],
            blocked_compat=blocked,
            submission_required=False,
        )


def test_validated_blocked_compat_refs_treats_subpath_as_blocked() -> None:
    blocked = tuple(_authority_binding_blocked_paths({
        "authority_binding": _binding_with_blocked(
            _blocked_entry(predecessor_path="Code&DBs/Workflow/runtime/legacy_module"),
        )
    }))
    drift = _validated_blocked_compat_refs(
        ["Code&DBs/Workflow/runtime/legacy_module/handlers.py"],
        blocked_compat=blocked,
        submission_required=True,
    )
    assert len(drift) == 1
    assert drift[0]["artifact_ref"] == "Code&DBs/Workflow/runtime/legacy_module/handlers.py"


def test_validated_blocked_compat_refs_dedupes_and_skips_empties() -> None:
    blocked = tuple(_authority_binding_blocked_paths({
        "authority_binding": _binding_with_blocked(
            _blocked_entry(predecessor_path="Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"),
        )
    }))
    drift = _validated_blocked_compat_refs(
        [
            "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py",
            "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py",
            "",
        ],
        blocked_compat=blocked,
        submission_required=True,
    )
    assert len(drift) == 1


def test_validated_blocked_compat_refs_no_op_when_no_blocked_paths() -> None:
    assert (
        _validated_blocked_compat_refs(
            ["any.py"],
            blocked_compat=(),
            submission_required=True,
        )
        == ()
    )
    assert (
        _validated_blocked_compat_refs(
            ["any.py"],
            blocked_compat=(),
            submission_required=False,
        )
        == ()
    )
