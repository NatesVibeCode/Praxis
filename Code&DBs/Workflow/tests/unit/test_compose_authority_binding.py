"""Unit tests for the compose-time canonical authority resolver."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from runtime.workflow.compose_authority_binding import (
    ComposeAuthorityBinding,
    PredecessorObligation,
    resolve_compose_authority_binding,
)


@dataclass
class _FakeConn:
    """Minimal fake conn that serves canned successor + obligation rows."""

    successor_map: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    obligations_for_successor: dict[tuple[str, str], list[dict[str, Any]]] = field(
        default_factory=dict
    )

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "FROM authority_canonical_successor_for" in query
        unit_kind, unit_ref = args
        return self.successor_map.get((unit_kind, unit_ref))

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "FROM authority_active_predecessor_obligations" in query
        kinds, refs = args
        results: list[dict[str, Any]] = []
        for kind, ref in zip(kinds, refs, strict=False):
            results.extend(self.obligations_for_successor.get((kind, ref), []))
        return results


def _obligation_row(
    *,
    successor_unit_kind: str,
    successor_unit_ref: str,
    predecessor_unit_kind: str,
    predecessor_unit_ref: str,
    supersession_status: str = "compat",
    obligation_summary: str | None = "preserve invariants",
    obligation_evidence: dict[str, Any] | None = None,
    source_candidate_id: str = "cand:abc",
    source_impact_id: str = "imp:abc",
) -> dict[str, Any]:
    return {
        "successor_unit_kind": successor_unit_kind,
        "successor_unit_ref": successor_unit_ref,
        "predecessor_unit_kind": predecessor_unit_kind,
        "predecessor_unit_ref": predecessor_unit_ref,
        "supersession_status": supersession_status,
        "obligation_summary": obligation_summary,
        "obligation_evidence": obligation_evidence or {"note": "test"},
        "source_candidate_id": source_candidate_id,
        "source_impact_id": source_impact_id,
        "source_decision_ref": "decision.test",
    }


def test_resolver_returns_empty_binding_for_no_targets() -> None:
    binding = resolve_compose_authority_binding(_FakeConn(), raw_targets=[])
    assert binding.canonical_write_scope == []
    assert binding.predecessor_obligations == []
    assert binding.blocked_compat_units == []
    assert binding.notes == ["no_targets_supplied"]


def test_resolver_returns_target_as_canonical_when_no_supersession() -> None:
    conn = _FakeConn()
    binding = resolve_compose_authority_binding(
        conn,
        raw_targets=[{"unit_kind": "operation_ref", "unit_ref": "compose_plan"}],
    )
    assert len(binding.canonical_write_scope) == 1
    canonical = binding.canonical_write_scope[0]
    assert canonical.unit_kind == "operation_ref"
    assert canonical.unit_ref == "compose_plan"
    assert canonical.was_redirected is False
    assert binding.predecessor_obligations == []


def test_resolver_redirects_target_to_canonical_successor() -> None:
    conn = _FakeConn(
        successor_map={
            ("operation_ref", "old_compose"): {
                "successor_unit_kind": "operation_ref",
                "successor_unit_ref": "compose_plan",
                "supersession_status": "compat",
                "supersession_id": "ss:1",
                "updated_at": "2026-04-29T00:00:00+00:00",
            }
        },
        obligations_for_successor={
            ("operation_ref", "compose_plan"): [
                _obligation_row(
                    successor_unit_kind="operation_ref",
                    successor_unit_ref="compose_plan",
                    predecessor_unit_kind="operation_ref",
                    predecessor_unit_ref="old_compose",
                    obligation_summary="must accept legacy intent string shape",
                )
            ]
        },
    )
    binding = resolve_compose_authority_binding(
        conn,
        raw_targets=[{"unit_kind": "operation_ref", "unit_ref": "old_compose"}],
    )
    assert len(binding.canonical_write_scope) == 1
    canonical = binding.canonical_write_scope[0]
    assert (canonical.unit_kind, canonical.unit_ref) == ("operation_ref", "compose_plan")
    assert canonical.was_redirected is True
    assert any(
        "old_compose_superseded_by_operation_ref:compose_plan" in note
        for note in binding.notes
    )
    assert len(binding.predecessor_obligations) == 1
    obligation = binding.predecessor_obligations[0]
    assert obligation.predecessor_unit_ref == "old_compose"
    assert obligation.successor_unit_ref == "compose_plan"
    assert obligation.obligation_summary == "must accept legacy intent string shape"


def test_resolver_dedupes_canonical_when_two_predecessors_share_successor() -> None:
    conn = _FakeConn(
        successor_map={
            ("operation_ref", "v1"): {
                "successor_unit_kind": "operation_ref",
                "successor_unit_ref": "v3",
                "supersession_status": "compat",
                "supersession_id": "ss:1",
                "updated_at": "2026-04-29T00:00:00+00:00",
            },
            ("operation_ref", "v2"): {
                "successor_unit_kind": "operation_ref",
                "successor_unit_ref": "v3",
                "supersession_status": "compat",
                "supersession_id": "ss:2",
                "updated_at": "2026-04-29T00:00:01+00:00",
            },
        },
        obligations_for_successor={
            ("operation_ref", "v3"): [
                _obligation_row(
                    successor_unit_kind="operation_ref",
                    successor_unit_ref="v3",
                    predecessor_unit_kind="operation_ref",
                    predecessor_unit_ref="v1",
                ),
                _obligation_row(
                    successor_unit_kind="operation_ref",
                    successor_unit_ref="v3",
                    predecessor_unit_kind="operation_ref",
                    predecessor_unit_ref="v2",
                ),
            ]
        },
    )
    binding = resolve_compose_authority_binding(
        conn,
        raw_targets=[
            {"unit_kind": "operation_ref", "unit_ref": "v1"},
            {"unit_kind": "operation_ref", "unit_ref": "v2"},
        ],
    )
    assert len(binding.canonical_write_scope) == 1
    assert binding.canonical_write_scope[0].unit_ref == "v3"
    assert {ob.predecessor_unit_ref for ob in binding.predecessor_obligations} == {"v1", "v2"}


def test_resolver_blocks_source_path_predecessors() -> None:
    conn = _FakeConn(
        obligations_for_successor={
            ("source_path", "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py"): [
                _obligation_row(
                    successor_unit_kind="source_path",
                    successor_unit_ref="Code&DBs/Workflow/runtime/operations/commands/foo_v2.py",
                    predecessor_unit_kind="source_path",
                    predecessor_unit_ref="Code&DBs/Workflow/runtime/operations/commands/foo_v1.py",
                    supersession_status="pending_retire",
                    obligation_summary="legacy v1 callers still hit it; preserve return shape",
                ),
            ]
        }
    )
    binding = resolve_compose_authority_binding(
        conn,
        raw_targets=[
            {
                "unit_kind": "source_path",
                "unit_ref": "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py",
            }
        ],
    )
    assert len(binding.canonical_write_scope) == 1
    assert len(binding.predecessor_obligations) == 1
    assert len(binding.blocked_compat_units) == 1
    assert binding.blocked_compat_units[0].predecessor_unit_kind == "source_path"
    assert "foo_v1.py" in binding.blocked_compat_units[0].predecessor_unit_ref


def test_resolver_rejects_unknown_unit_kind() -> None:
    with pytest.raises(ValueError, match="not a valid authority unit kind"):
        resolve_compose_authority_binding(
            _FakeConn(),
            raw_targets=[{"unit_kind": "made_up", "unit_ref": "x"}],
        )


def test_resolver_rejects_missing_unit_ref() -> None:
    with pytest.raises(ValueError, match="unit_ref is required"):
        resolve_compose_authority_binding(
            _FakeConn(),
            raw_targets=[{"unit_kind": "operation_ref", "unit_ref": ""}],
        )


def test_binding_to_dict_round_trip_shapes() -> None:
    binding = ComposeAuthorityBinding(
        predecessor_obligations=[
            PredecessorObligation(
                predecessor_unit_kind="operation_ref",
                predecessor_unit_ref="old",
                successor_unit_kind="operation_ref",
                successor_unit_ref="new",
                supersession_status="compat",
                obligation_summary="preserve",
                obligation_evidence={"k": "v"},
                source_candidate_id="cand:1",
                source_impact_id="imp:1",
                source_decision_ref="decision.test",
            )
        ]
    )
    payload = binding.to_dict()
    assert payload["predecessor_obligations"][0]["predecessor_unit_ref"] == "old"
    assert payload["predecessor_obligations"][0]["successor_unit_ref"] == "new"
    assert payload["canonical_write_scope"] == []
