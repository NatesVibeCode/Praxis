"""Pass 7 tests: compact authority_binding summary surfaced on run-jobs API."""

from __future__ import annotations

import json

from surfaces.api.frontdoor import _authority_binding_summary


_FULL_BINDING = {
    "canonical_write_scope": [
        {
            "unit_kind": "operation_ref",
            "unit_ref": "compose_plan",
            "requested_target": {"unit_kind": "operation_ref", "unit_ref": "old_compose"},
            "was_redirected": True,
        },
        {
            "unit_kind": "handler_ref",
            "unit_ref": "Code&DBs/Workflow/runtime/operations/commands/foo.py",
            "requested_target": {
                "unit_kind": "handler_ref",
                "unit_ref": "Code&DBs/Workflow/runtime/operations/commands/foo.py",
            },
            "was_redirected": False,
        },
    ],
    "predecessor_obligations": [
        {
            "predecessor_unit_kind": "operation_ref",
            "predecessor_unit_ref": "old_compose",
            "successor_unit_kind": "operation_ref",
            "successor_unit_ref": "compose_plan",
            "supersession_status": "compat",
            "obligation_summary": "preserve legacy intent shape",
            "obligation_evidence": {},
            "source_candidate_id": None,
            "source_impact_id": None,
            "source_decision_ref": None,
        }
    ],
    "blocked_compat_units": [
        {
            "predecessor_unit_kind": "source_path",
            "predecessor_unit_ref": "Code&DBs/Workflow/runtime/legacy_module/handlers.py",
            "successor_unit_kind": "source_path",
            "successor_unit_ref": "Code&DBs/Workflow/runtime/operations/commands/foo.py",
            "supersession_status": "pending_retire",
            "obligation_summary": "callers expect dict shape",
        }
    ],
    "unresolved_targets": [],
    "notes": [],
}


def test_returns_none_for_null_binding() -> None:
    assert _authority_binding_summary(None) is None


def test_returns_none_for_invalid_type() -> None:
    assert _authority_binding_summary(42) is None
    assert _authority_binding_summary([]) is None


def test_returns_none_for_unparseable_string() -> None:
    assert _authority_binding_summary("not json") is None


def test_returns_compact_summary_for_dict_binding() -> None:
    summary = _authority_binding_summary(_FULL_BINDING)
    assert summary == {
        "bound": True,
        "canonical_count": 2,
        "predecessor_count": 1,
        "blocked_compat_count": 1,
        "redirected_count": 1,
    }


def test_returns_compact_summary_for_json_string_binding() -> None:
    summary = _authority_binding_summary(json.dumps(_FULL_BINDING, sort_keys=True))
    assert summary is not None
    assert summary["bound"] is True
    assert summary["redirected_count"] == 1


def test_summary_handles_empty_lists_safely() -> None:
    summary = _authority_binding_summary(
        {
            "canonical_write_scope": [],
            "predecessor_obligations": [],
            "blocked_compat_units": [],
            "unresolved_targets": [],
            "notes": [],
        }
    )
    assert summary == {
        "bound": True,
        "canonical_count": 0,
        "predecessor_count": 0,
        "blocked_compat_count": 0,
        "redirected_count": 0,
    }


def test_summary_handles_missing_keys() -> None:
    summary = _authority_binding_summary({})
    assert summary == {
        "bound": True,
        "canonical_count": 0,
        "predecessor_count": 0,
        "blocked_compat_count": 0,
        "redirected_count": 0,
    }


def test_summary_handles_non_list_field_values() -> None:
    summary = _authority_binding_summary(
        {
            "canonical_write_scope": None,
            "predecessor_obligations": "not a list",
            "blocked_compat_units": 0,
        }
    )
    assert summary["canonical_count"] == 0
    assert summary["predecessor_count"] == 0
    assert summary["blocked_compat_count"] == 0


def test_redirected_count_only_counts_was_redirected_true() -> None:
    summary = _authority_binding_summary(
        {
            "canonical_write_scope": [
                {"unit_kind": "operation_ref", "unit_ref": "a", "was_redirected": True},
                {"unit_kind": "operation_ref", "unit_ref": "b", "was_redirected": False},
                {"unit_kind": "operation_ref", "unit_ref": "c", "was_redirected": True},
                {"unit_kind": "operation_ref", "unit_ref": "d"},  # missing key
            ],
            "predecessor_obligations": [],
            "blocked_compat_units": [],
        }
    )
    assert summary["canonical_count"] == 4
    assert summary["redirected_count"] == 2
