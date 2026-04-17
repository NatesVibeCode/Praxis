from __future__ import annotations

from typing import Any

import pytest

from storage.postgres.operation_catalog_repository import (
    list_operation_catalog_records,
    list_operation_source_policy_records,
    load_operation_catalog_record_by_name,
)
from storage.postgres.validators import PostgresWriteError


_VALID_OPERATION_ROW = {
    "operation_ref": "workflow-build-suggest-next",
    "operation_name": "workflow_build.suggest_next",
    "source_kind": "operation_query",
    "operation_kind": "query",
    "http_method": "POST",
    "http_path": "/api/workflows/{workflow_id}/build/suggest-next",
    "input_model_ref": "runtime.operations.commands.suggest_next.SuggestNextNodesCommand",
    "handler_ref": "runtime.operations.commands.suggest_next.handle_suggest_next_nodes",
    "authority_ref": "authority.capability_catalog",
    "projection_ref": "projection.capability_catalog",
    "posture": None,
    "idempotency_policy": None,
    "enabled": True,
    "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
    "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
}

_VALID_SOURCE_POLICY_ROW = {
    "policy_ref": "operation-query",
    "source_kind": "operation_query",
    "posture": "observe",
    "idempotency_policy": "read_only",
    "enabled": True,
    "binding_revision": "binding.operation_catalog_source_policy_registry.bootstrap.20260416",
    "decision_ref": "decision.operation_catalog_source_policy_registry.bootstrap.20260416",
}


class FakeConn:
    def __init__(
        self,
        *,
        operation_rows: list[dict[str, Any]] | None = None,
        source_policy_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.operation_rows = list(operation_rows or [])
        self.source_policy_rows = list(source_policy_rows or [])

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        normalized = " ".join(query.split())
        if "FROM operation_catalog_registry WHERE operation_name = $1" in normalized:
            target = args[0]
            for row in self.operation_rows:
                if row["operation_name"] == target:
                    return row
            return None
        raise AssertionError(f"unexpected fetchrow query: {query}")

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        if "FROM operation_catalog_registry" in normalized:
            rows = list(self.operation_rows)
            if "source_kind = $1" in normalized:
                rows = [row for row in rows if row["source_kind"] == args[0]]
            if "enabled = TRUE" in normalized:
                rows = [row for row in rows if row["enabled"]]
            return rows[: args[-1]]
        if "FROM operation_catalog_source_policy_registry" in normalized:
            rows = list(self.source_policy_rows)
            if "enabled = TRUE" in normalized:
                rows = [row for row in rows if row["enabled"]]
            return rows[: args[-1]]
        raise AssertionError(f"unexpected fetch query: {query}")


def test_list_operation_catalog_records_filters_and_normalizes() -> None:
    conn = FakeConn(
        operation_rows=[
            dict(_VALID_OPERATION_ROW),
            {
                **_VALID_OPERATION_ROW,
                "operation_ref": "workflow-build-mutate",
                "operation_name": "workflow_build.mutate",
                "source_kind": "operation_command",
                "operation_kind": "command",
                "enabled": False,
            },
        ]
    )

    rows = list_operation_catalog_records(conn, source_kind="operation_query", limit=10)

    assert rows == [dict(_VALID_OPERATION_ROW)]


def test_load_operation_catalog_record_by_name_returns_none_when_missing() -> None:
    conn = FakeConn(operation_rows=[dict(_VALID_OPERATION_ROW)])

    row = load_operation_catalog_record_by_name(conn, operation_name="operator.roadmap_tree")

    assert row is None


def test_operation_repository_rejects_invalid_operation_kind() -> None:
    conn = FakeConn(
        operation_rows=[{**_VALID_OPERATION_ROW, "operation_kind": "mutate"}]
    )

    with pytest.raises(PostgresWriteError) as exc_info:
        list_operation_catalog_records(conn, include_disabled=True)

    assert exc_info.value.reason_code == "operation_catalog.invalid_submission"
    assert "operation_kind" in str(exc_info.value)


def test_source_policy_repository_rejects_invalid_posture() -> None:
    conn = FakeConn(
        source_policy_rows=[{**_VALID_SOURCE_POLICY_ROW, "posture": "telemetry"}]
    )

    with pytest.raises(PostgresWriteError) as exc_info:
        list_operation_source_policy_records(conn, include_disabled=True)

    assert exc_info.value.reason_code == "operation_catalog.invalid_submission"
    assert "posture" in str(exc_info.value)
