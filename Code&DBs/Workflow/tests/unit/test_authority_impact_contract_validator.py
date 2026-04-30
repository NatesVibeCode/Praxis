"""Unit tests for the agent-declared authority impact contract validator."""

from __future__ import annotations

import pytest

from runtime.workflow.submission_capture import (
    WorkflowSubmissionServiceError,
    _normalize_authority_impact_rows,
)


_AUTHORITY_BEARING = ["Code&DBs/Databases/migrations/workflow/999_x.sql"]
_NON_AUTHORITY_BEARING = ["docs/notes.md", "README.md"]


def test_empty_rows_allowed_when_no_authority_bearing_paths() -> None:
    assert _normalize_authority_impact_rows([], intended_files=_NON_AUTHORITY_BEARING) == []
    assert _normalize_authority_impact_rows(None, intended_files=_NON_AUTHORITY_BEARING) == []


def test_empty_rows_rejected_when_authority_bearing_paths_present() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows([], intended_files=_AUTHORITY_BEARING)
    assert exc_info.value.reason_code == "code_change_candidate.authority_impact_contract_required"


def test_extend_intent_with_required_fields_normalizes() -> None:
    rows = _normalize_authority_impact_rows(
        [
            {
                "intent": "EXTEND",
                "unit_kind": "Operation_Ref",
                "unit_ref": "sample.thing_commit",
                "dispatch_effect": "register",
            }
        ],
        intended_files=_AUTHORITY_BEARING,
    )
    assert rows == [
        {
            "intent": "extend",
            "unit_kind": "operation_ref",
            "unit_ref": "sample.thing_commit",
            "predecessor_unit_kind": None,
            "predecessor_unit_ref": None,
            "dispatch_effect": "register",
            "subsumption_evidence_ref": None,
            "rollback_path": None,
        }
    ]


def test_replace_intent_requires_predecessor() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [
                {
                    "intent": "replace",
                    "unit_kind": "operation_ref",
                    "unit_ref": "new.op",
                    "dispatch_effect": "reroute",
                }
            ],
            intended_files=_AUTHORITY_BEARING,
        )
    assert (
        exc_info.value.reason_code
        == "code_change_candidate.authority_impact_predecessor_required"
    )


def test_retire_intent_requires_predecessor() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [
                {
                    "intent": "retire",
                    "unit_kind": "operation_ref",
                    "unit_ref": "old.op",
                    "dispatch_effect": "retire",
                }
            ],
            intended_files=_AUTHORITY_BEARING,
        )
    assert (
        exc_info.value.reason_code
        == "code_change_candidate.authority_impact_predecessor_required"
    )


def test_fix_intent_forbids_predecessor() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [
                {
                    "intent": "fix",
                    "unit_kind": "operation_ref",
                    "unit_ref": "some.op",
                    "dispatch_effect": "none",
                    "predecessor_unit_kind": "operation_ref",
                    "predecessor_unit_ref": "some.op",
                }
            ],
            intended_files=_AUTHORITY_BEARING,
        )
    assert (
        exc_info.value.reason_code
        == "code_change_candidate.authority_impact_predecessor_forbidden"
    )


def test_invalid_intent_rejected() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [{"intent": "redesign", "unit_kind": "operation_ref", "unit_ref": "x", "dispatch_effect": "none"}],
            intended_files=_AUTHORITY_BEARING,
        )
    assert exc_info.value.reason_code == "code_change_candidate.authority_impact_intent_invalid"


def test_invalid_unit_kind_rejected() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [{"intent": "extend", "unit_kind": "operation", "unit_ref": "x", "dispatch_effect": "register"}],
            intended_files=_AUTHORITY_BEARING,
        )
    assert exc_info.value.reason_code == "code_change_candidate.authority_impact_unit_kind_invalid"


def test_invalid_dispatch_effect_rejected() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [{"intent": "extend", "unit_kind": "operation_ref", "unit_ref": "x", "dispatch_effect": "fan_out"}],
            intended_files=_AUTHORITY_BEARING,
        )
    assert (
        exc_info.value.reason_code
        == "code_change_candidate.authority_impact_dispatch_effect_invalid"
    )


def test_missing_unit_ref_rejected() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows(
            [{"intent": "extend", "unit_kind": "operation_ref", "unit_ref": "  ", "dispatch_effect": "register"}],
            intended_files=_AUTHORITY_BEARING,
        )
    assert (
        exc_info.value.reason_code == "code_change_candidate.authority_impact_unit_ref_missing"
    )


def test_replace_intent_with_predecessor_normalizes_with_subsumption_ref() -> None:
    rows = _normalize_authority_impact_rows(
        [
            {
                "intent": "replace",
                "unit_kind": "operation_ref",
                "unit_ref": "new.op",
                "dispatch_effect": "reroute",
                "predecessor_unit_kind": "operation_ref",
                "predecessor_unit_ref": "old.op",
                "subsumption_evidence_ref": "verifier_run:abc-123",
                "rollback_path": "git revert <commit>",
            }
        ],
        intended_files=_AUTHORITY_BEARING,
    )
    assert rows[0]["predecessor_unit_ref"] == "old.op"
    assert rows[0]["subsumption_evidence_ref"] == "verifier_run:abc-123"
    assert rows[0]["rollback_path"] == "git revert <commit>"


def test_single_dict_treated_as_single_row_list() -> None:
    rows = _normalize_authority_impact_rows(
        {
            "intent": "extend",
            "unit_kind": "operation_ref",
            "unit_ref": "single.op",
            "dispatch_effect": "register",
        },
        intended_files=_AUTHORITY_BEARING,
    )
    assert len(rows) == 1
    assert rows[0]["unit_ref"] == "single.op"


def test_string_payload_rejected_as_malformed() -> None:
    with pytest.raises(WorkflowSubmissionServiceError) as exc_info:
        _normalize_authority_impact_rows("not a list", intended_files=_AUTHORITY_BEARING)
    assert (
        exc_info.value.reason_code == "code_change_candidate.authority_impact_contract_malformed"
    )
