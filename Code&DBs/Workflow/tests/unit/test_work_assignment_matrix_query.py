from __future__ import annotations

from typing import Any

from runtime.operations.queries.operator_support import (
    QueryWorkAssignmentMatrix,
    handle_query_work_assignment_matrix,
)


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        assert "FROM work_item_assignment_matrix" in query
        return [
            {
                "item_kind": "bug",
                "item_id": "BUG-EBE27625",
                "item_key": "bug-system-title-provider-capability-matrix",
                "title": "Provider capability matrix missing",
                "status": "OPEN",
                "severity": "P1",
                "priority": "P1",
                "category": "ARCHITECTURE",
                "audit_group": "A_provider_catalog_authority",
                "group_sort_order": 10,
                "recommended_model_tier": "frontier",
                "recommended_model_tier_group": "frontier",
                "suggested_sequence": 1,
                "assignment_reason": "Defines the authority contract.",
                "task_type": "architecture",
                "can_delegate_to_less_than_frontier": False,
                "grouping_source": "audit",
                "implementation_status": "",
                "visibility_state": "active",
                "updated_at": "2026-04-26T00:00:00Z",
                "source_ref": "table.bugs.resume_context",
            },
            {
                "item_kind": "bug",
                "item_id": "BUG-C3800386",
                "item_key": "bug-system-title-dashboard-quick-reference",
                "title": "Dashboard quick reference drift",
                "status": "OPEN",
                "severity": "P3",
                "priority": "P3",
                "category": "VERIFY",
                "audit_group": "F_picker_helper_leaks",
                "group_sort_order": 60,
                "recommended_model_tier": "cheap_model_or_junior",
                "recommended_model_tier_group": "cheap_or_junior",
                "suggested_sequence": 4,
                "assignment_reason": "Static docs cleanup.",
                "task_type": "verify",
                "can_delegate_to_less_than_frontier": True,
                "grouping_source": "audit",
                "implementation_status": "",
                "visibility_state": "active",
                "updated_at": "2026-04-26T00:00:00Z",
                "source_ref": "table.bugs.resume_context",
            },
        ]


class _FakeSubsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self.conn


def test_work_assignment_matrix_groups_rows_by_audit_group_and_tier() -> None:
    conn = _FakeConn()

    payload = handle_query_work_assignment_matrix(
        QueryWorkAssignmentMatrix(open_only=True),
        _FakeSubsystems(conn),
    )

    assert payload["operation"] == "operator.work_assignment_matrix"
    assert payload["authority"] == "view.work_item_assignment_matrix"
    assert payload["count"] == 2
    assert payload["tier_counts"] == {
        "cheap_or_junior": 1,
        "frontier": 1,
    }
    assert payload["groups"] == [
        {
            "audit_group": "A_provider_catalog_authority",
            "count": 1,
            "tiers": {"frontier": 1},
        },
        {
            "audit_group": "F_picker_helper_leaks",
            "count": 1,
            "tiers": {"cheap_or_junior": 1},
        },
    ]
    assert payload["rows"][0]["can_delegate_to_less_than_frontier"] is False


def test_work_assignment_matrix_passes_filters_to_projection_query() -> None:
    conn = _FakeConn()

    handle_query_work_assignment_matrix(
        QueryWorkAssignmentMatrix(
            status="OPEN",
            audit_group="A_provider_catalog_authority",
            recommended_model_tier="frontier",
            open_only=False,
            limit=25,
        ),
        _FakeSubsystems(conn),
    )

    assert conn.calls[0][1] == (
        "OPEN",
        "A_provider_catalog_authority",
        "frontier",
        False,
        25,
    )
