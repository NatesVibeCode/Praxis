"""Unit tests for materialize-time supersession registry population."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from runtime.workflow.candidate_materialization import (
    _populate_authority_supersession_registry,
)


@dataclass
class _FakeConn:
    impact_rows: list[dict[str, Any]] = field(default_factory=list)
    inserted: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "FROM candidate_authority_impacts" in query
        return list(self.impact_rows)

    def execute(self, query: str, *args: Any) -> None:
        assert "INSERT INTO authority_supersession_registry" in query
        self.inserted.append((" ".join(query.split()), args))


def _impact(
    *,
    impact_id: str = "imp:1",
    intent: str = "replace",
    successor_unit_kind: str = "operation_ref",
    successor_unit_ref: str = "compose_plan_v3",
    predecessor_unit_kind: str = "operation_ref",
    predecessor_unit_ref: str = "compose_plan_v1",
    subsumption_evidence_ref: str | None = "verifier_run:abc",
    rollback_path: str | None = "git revert <sha>",
    notes: str | None = "preserve legacy field foo",
) -> dict[str, Any]:
    return {
        "impact_id": impact_id,
        "intent": intent,
        "successor_unit_kind": successor_unit_kind,
        "successor_unit_ref": successor_unit_ref,
        "predecessor_unit_kind": predecessor_unit_kind,
        "predecessor_unit_ref": predecessor_unit_ref,
        "subsumption_evidence_ref": subsumption_evidence_ref,
        "rollback_path": rollback_path,
        "notes": notes,
    }


def test_no_inserts_when_no_replace_or_retire_impacts() -> None:
    conn = _FakeConn(impact_rows=[])
    written = _populate_authority_supersession_registry(
        conn,
        candidate_id="11111111-1111-1111-1111-111111111111",
        promotion_decision_id="prom:1",
        materialized_by="human:nate",
    )
    assert written == 0
    assert conn.inserted == []


def test_replace_intent_inserts_compat_supersession_row() -> None:
    conn = _FakeConn(impact_rows=[_impact(intent="replace")])
    written = _populate_authority_supersession_registry(
        conn,
        candidate_id="11111111-1111-1111-1111-111111111111",
        promotion_decision_id="prom:1",
        materialized_by="human:nate",
    )
    assert written == 1
    assert len(conn.inserted) == 1
    _, args = conn.inserted[0]
    successor_kind, successor_ref, predecessor_kind, predecessor_ref, status = args[:5]
    assert successor_kind == "operation_ref"
    assert successor_ref == "compose_plan_v3"
    assert predecessor_kind == "operation_ref"
    assert predecessor_ref == "compose_plan_v1"
    assert status == "compat"

    obligation_summary = args[5]
    assert "preserve legacy field foo" in obligation_summary
    assert "intent=replace" in obligation_summary

    evidence = json.loads(args[6])
    assert evidence["candidate_id"] == "11111111-1111-1111-1111-111111111111"
    assert evidence["promotion_decision_id"] == "prom:1"
    assert evidence["intent"] == "replace"
    assert evidence["subsumption_evidence_ref"] == "verifier_run:abc"
    assert evidence["rollback_path"] == "git revert <sha>"


def test_retire_intent_inserts_pending_retire_row_with_placeholder_successor() -> None:
    conn = _FakeConn(
        impact_rows=[
            _impact(
                intent="retire",
                successor_unit_kind="operation_ref",  # ignored for retire
                successor_unit_ref="ignored",
                predecessor_unit_kind="operation_ref",
                predecessor_unit_ref="dead_op",
            )
        ]
    )
    written = _populate_authority_supersession_registry(
        conn,
        candidate_id="22222222-2222-2222-2222-222222222222",
        promotion_decision_id="prom:2",
        materialized_by="system:auto_retire",
    )
    assert written == 1
    _, args = conn.inserted[0]
    successor_kind, successor_ref, predecessor_kind, predecessor_ref, status = args[:5]
    assert successor_kind == "operation_ref"
    assert successor_ref == "retired:22222222-2222-2222-2222-222222222222"
    assert predecessor_kind == "operation_ref"
    assert predecessor_ref == "dead_op"
    assert status == "pending_retire"


def test_multiple_impacts_each_become_supersession_rows() -> None:
    conn = _FakeConn(
        impact_rows=[
            _impact(impact_id="imp:1", intent="replace", predecessor_unit_ref="v1"),
            _impact(impact_id="imp:2", intent="replace", predecessor_unit_ref="v2"),
            _impact(
                impact_id="imp:3",
                intent="retire",
                predecessor_unit_ref="dead_handler",
            ),
        ]
    )
    written = _populate_authority_supersession_registry(
        conn,
        candidate_id="33333333-3333-3333-3333-333333333333",
        promotion_decision_id="prom:3",
        materialized_by="llm:agent",
    )
    assert written == 3
    statuses = [args[4] for _, args in conn.inserted]
    assert statuses == ["compat", "compat", "pending_retire"]
