from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from runtime.operations.queries.data_dictionary import (
    QueryDataDictionary,
    handle_query_data_dictionary,
)


class _MockConn:
    def __init__(
        self,
        *,
        entity_rows: list[dict[str, Any]],
        edge_rows: list[dict[str, Any]],
    ) -> None:
        self._entity_rows = entity_rows
        self._edge_rows = edge_rows

    def execute(self, sql: str, *args: object) -> list[dict[str, Any]]:
        if "FROM memory_entities" in sql:
            return list(self._entity_rows)
        if "FROM memory_edges" in sql:
            table_entity_id = args[0] if args else None
            return [
                row
                for row in self._edge_rows
                if row["source_id"] == table_entity_id or row["target_id"] == table_entity_id
            ]
        raise AssertionError(f"unexpected SQL: {sql}")


class _MockSubsystems:
    def __init__(self, conn: _MockConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _MockConn:
        return self._conn


def _table_row(
    *,
    entity_id: str,
    name: str,
    updated_at: datetime,
    approx_rows: int = 0,
) -> dict[str, Any]:
    return {
        "id": entity_id,
        "name": name,
        "content": f"{name} summary",
        "metadata": {
            "columns": [{"name": "id", "type": "uuid"}],
            "indexes": [{"name": f"{name}_pkey"}],
            "triggers": [{"name": f"{name}_notify"}],
            "used_by": {"python": [f"{name}_service"]},
            "approx_rows": approx_rows,
            "valid_values": {"status": ["active"]},
            "pg_notify_channels": [f"{name}_changed"],
        },
        "updated_at": updated_at,
    }


def test_operation_query_data_dictionary_dispatch_returns_versioned_contract() -> None:
    conn = _MockConn(
        entity_rows=[
            _table_row(
                entity_id="table:orders",
                name="orders",
                updated_at=datetime(2026, 4, 16, 8, 0, tzinfo=timezone.utc),
                approx_rows=42,
            ),
            _table_row(
                entity_id="table:customers",
                name="customers",
                updated_at=datetime(2026, 4, 16, 7, 0, tzinfo=timezone.utc),
                approx_rows=10,
            ),
        ],
        edge_rows=[
            {
                "source_id": "table:orders",
                "target_id": "table:customers",
                "relation_type": "foreign_key",
                "metadata": {"column": "customer_id"},
            }
        ],
    )
    result = handle_query_data_dictionary(
        QueryDataDictionary(table_name="orders"),
        _MockSubsystems(conn),
    )

    assert result["routed_to"] == "data_dictionary"
    assert result["contract_version"] == 1
    assert result["contract"]["query_path"] == "/api/operator/data-dictionary"
    assert result["contract"]["sources"]["table_projection"] == "memory_entities"
    assert result["scope"] == "table"
    assert result["requested_table"] == "orders"
    assert result["count"] == 1
    assert result["total_tables"] == 2
    assert result["freshness"]["projection_updated_at_min"] == "2026-04-16T07:00:00+00:00"
    assert result["freshness"]["projection_updated_at_max"] == "2026-04-16T08:00:00+00:00"

    table = result["tables"][0]
    assert table["entity_id"] == "table:orders"
    assert table["column_count"] == 1
    assert table["relationships"]["depends_on"] == [
        {
            "table": "customers",
            "relation": "foreign_key",
            "metadata": {"column": "customer_id"},
        }
    ]
    assert table["relationship_counts"] == {"depends_on": 1, "referenced_by": 0}
    assert table["lifecycle"]["detail_level"] == "detail"
    assert table["lifecycle"]["contract_version"] == 1


def test_query_data_dictionary_missing_match_keeps_contract_and_projection_freshness() -> None:
    conn = _MockConn(
        entity_rows=[
            _table_row(
                entity_id="table:orders",
                name="orders",
                updated_at=datetime(2026, 4, 16, 8, 0, tzinfo=timezone.utc),
                approx_rows=42,
            )
        ],
        edge_rows=[],
    )
    result = handle_query_data_dictionary(
        QueryDataDictionary(table_name="payments"),
        _MockSubsystems(conn),
    )

    assert result["scope"] == "missing"
    assert result["contract_version"] == 1
    assert result["requested_table"] == "payments"
    assert result["tables"] == []
    assert result["count"] == 0
    assert result["total_tables"] == 1
    assert result["hint"] == "No exact table match. Try one of: orders"
    assert result["freshness"]["projection_updated_at_min"] == "2026-04-16T08:00:00+00:00"
    assert result["freshness"]["projection_updated_at_max"] == "2026-04-16T08:00:00+00:00"
