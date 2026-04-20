"""Unit tests for the roadmap `proof_kind` opt-in plumbing.

The `proof_kind = 'capability_delivered_by_decision_filing'` marker on a roadmap
row's `acceptance_criteria` is the only opt-in the closeout gate honors to skip
the `source_bug` + `validates_fix` evidence requirement. These tests pin the
surface-level helpers that construct / read that marker so the opt-in cannot
regress silently.
"""

from __future__ import annotations

import pytest

from surfaces.api.operator_write import (
    _CAPABILITY_DELIVERED_BY_DECISION_FILING,
    _acceptance_payload,
    _roadmap_acceptance_proof_kind,
)


def _base_kwargs() -> dict[str, object]:
    return {
        "tier": "tier_1",
        "phase_ready": False,
        "approval_tag": "test-tag",
        "outcome_gate": "outcome",
        "phase_order": "1",
        "reference_doc": None,
        "must_have": ("test must",),
    }


def test_acceptance_payload_omits_proof_kind_when_not_set() -> None:
    """Default rows stay tight: no `proof_kind` key leaks into acceptance_criteria."""
    payload = _acceptance_payload(**_base_kwargs())
    assert "proof_kind" not in payload


def test_acceptance_payload_includes_proof_kind_when_set() -> None:
    """Explicit opt-in writes `proof_kind` into the canonical acceptance_criteria shape."""
    payload = _acceptance_payload(
        **_base_kwargs(),
        proof_kind=_CAPABILITY_DELIVERED_BY_DECISION_FILING,
    )
    assert payload["proof_kind"] == _CAPABILITY_DELIVERED_BY_DECISION_FILING


def test_acceptance_payload_drops_empty_proof_kind() -> None:
    """Whitespace / empty proof_kind must not add a bogus opt-in marker."""
    payload = _acceptance_payload(**_base_kwargs(), proof_kind="")
    assert "proof_kind" not in payload


@pytest.mark.parametrize(
    "value, expected",
    [
        ({"proof_kind": "capability_delivered_by_decision_filing"},
         "capability_delivered_by_decision_filing"),
        ({"proof_kind": "  capability_delivered_by_decision_filing  "},
         "capability_delivered_by_decision_filing"),
        ({"proof_kind": "something_else"}, "something_else"),
        ({}, None),
        ({"proof_kind": ""}, None),
        ({"proof_kind": None}, None),
        ({"proof_kind": 42}, None),
        (None, None),
        ("", None),
    ],
)
def test_roadmap_acceptance_proof_kind_reads_mapping_shape(
    value: object, expected: str | None
) -> None:
    """_roadmap_acceptance_proof_kind reads mapping-shaped acceptance_criteria."""
    assert _roadmap_acceptance_proof_kind(value) == expected


def test_roadmap_acceptance_proof_kind_reads_historical_array_shape() -> None:
    """Historical rows store acceptance_criteria as a JSON array of dicts.

    The reader must still find `proof_kind` in the first mapping-valued element.
    """
    value = [
        {"tier": "tier_1"},
        {"proof_kind": "capability_delivered_by_decision_filing"},
    ]
    assert (
        _roadmap_acceptance_proof_kind(value)
        == _CAPABILITY_DELIVERED_BY_DECISION_FILING
    )


def test_roadmap_acceptance_proof_kind_parses_json_string() -> None:
    """Values persisted as raw JSON strings are decoded before lookup."""
    assert (
        _roadmap_acceptance_proof_kind(
            '{"proof_kind": "capability_delivered_by_decision_filing"}'
        )
        == _CAPABILITY_DELIVERED_BY_DECISION_FILING
    )
