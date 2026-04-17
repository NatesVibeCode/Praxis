from __future__ import annotations

import json
from typing import Any

from runtime.object_schema import list_compiled_object_fields, list_compiled_object_types, load_compiled_object_type


class _FakeConn:
    def fetchrow(self, query: str, *params: Any) -> dict[str, Any] | None:
        normalized = " ".join(query.split())
        if normalized == "SELECT type_id, name, description, icon, created_at FROM object_types WHERE type_id = $1":
            if params[0] == "ticket":
                return {
                    "type_id": "ticket",
                    "name": "Ticket",
                    "description": "Support ticket",
                    "icon": "ticket",
                    "created_at": "now",
                }
            return None
        raise AssertionError(f"unexpected fetchrow query: {normalized}")

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        normalized = " ".join(query.split())
        if normalized == "SELECT type_id, name, description, icon, created_at FROM object_types ORDER BY name LIMIT $1":
            return [
                {
                    "type_id": "ticket",
                    "name": "Ticket",
                    "description": "Support ticket",
                    "icon": "ticket",
                    "created_at": "now",
                }
            ]
        if normalized == "SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry WHERE retired_at IS NULL ORDER BY type_id ASC, display_order ASC, field_name ASC":
            return [
                {
                    "type_id": "ticket",
                    "field_name": "title",
                    "label": "Title",
                    "field_kind": "text",
                    "description": "Display title",
                    "required": True,
                    "default_value": None,
                    "options": json.dumps([]),
                    "display_order": 10,
                    "retired_at": None,
                },
                {
                    "type_id": "ticket",
                    "field_name": "status",
                    "label": "Status",
                    "field_kind": "enum",
                    "description": "Workflow state",
                    "required": False,
                    "default_value": json.dumps("open"),
                    "options": json.dumps(["open", "closed"]),
                    "display_order": 20,
                    "retired_at": None,
                },
            ]
        if normalized == "SELECT type_id, field_name, label, field_kind, description, required, default_value, options, display_order, retired_at FROM object_field_registry WHERE type_id = $1 AND retired_at IS NULL ORDER BY display_order ASC, field_name ASC":
            assert params[0] == "ticket"
            return [
                {
                    "type_id": "ticket",
                    "field_name": "title",
                    "label": "Title",
                    "field_kind": "text",
                    "description": "Display title",
                    "required": True,
                    "default_value": None,
                    "options": json.dumps([]),
                    "display_order": 10,
                    "retired_at": None,
                },
                {
                    "type_id": "ticket",
                    "field_name": "status",
                    "label": "Status",
                    "field_kind": "enum",
                    "description": "Workflow state",
                    "required": False,
                    "default_value": json.dumps("open"),
                    "options": json.dumps(["open", "closed"]),
                    "display_order": 20,
                    "retired_at": None,
                },
            ]
        raise AssertionError(f"unexpected execute query: {normalized}")


def test_list_compiled_object_types_reads_field_registry() -> None:
    rows = list_compiled_object_types(_FakeConn(), limit=10)

    assert rows[0]["type_id"] == "ticket"
    assert [field["name"] for field in rows[0]["fields"]] == ["title", "status"]
    assert "property_definitions" not in rows[0]


def test_load_compiled_object_type_reads_one_type() -> None:
    row = load_compiled_object_type(_FakeConn(), type_id="ticket")

    assert row is not None
    assert row["fields"][1]["default"] == "open"
    assert "property_definitions" not in row
    assert list_compiled_object_fields(_FakeConn(), type_id="ticket")[0]["name"] == "title"
