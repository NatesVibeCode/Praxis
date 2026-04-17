from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any

_runtime_pkg = types.ModuleType("runtime")
_runtime_pkg.__path__ = [str(Path(__file__).resolve().parents[2] / "runtime")]
sys.modules.setdefault("runtime", _runtime_pkg)

_spec = importlib.util.spec_from_file_location(
    "registry.reference_catalog_sync",
    Path(__file__).resolve().parents[2] / "registry" / "reference_catalog_sync.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["registry.reference_catalog_sync"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

from registry.reference_catalog_sync import sync_reference_catalog


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "information_schema.columns" in query:
            return [
                {"column_name": "slug"},
                {"column_name": "ref_type"},
                {"column_name": "display_name"},
                {"column_name": "description"},
                {"column_name": "resolved_table"},
                {"column_name": "resolved_id"},
                {"column_name": "schema_def"},
                {"column_name": "examples"},
                {"column_name": "updated_at"},
            ]
        if "FROM integration_registry" in query:
            return [
                {
                    "id": "gmail",
                    "name": "Gmail",
                    "provider": "google",
                    "capabilities": json.dumps(["search_emails", "read_thread"]),
                    "auth_status": "connected",
                    "description": "Mailbox tools",
                }
            ]
        if "FROM object_types" in query:
            return [
                {
                    "type_id": "ticket",
                    "name": "Ticket",
                    "description": "Support ticket",
                    "icon": "",
                    "created_at": "now",
                }
            ]
        if "FROM object_field_registry" in query:
            return [
                {
                    "type_id": "ticket",
                    "field_name": "status",
                    "label": "Status",
                    "field_kind": "text",
                    "description": "Workflow state",
                    "required": False,
                    "default_value": None,
                    "options": json.dumps([]),
                    "display_order": 10,
                    "retired_at": None,
                }
            ]
        if "FROM task_type_routing" in query:
            return [{"task_type": "build"}]
        return []

    def execute_many(self, query: str, rows: list[tuple[Any, ...]]) -> None:
        self.batch_calls.append((query, rows))


def test_sync_reference_catalog_upserts_integration_object_and_agent_rows() -> None:
    conn = _FakeConn()

    inserted = sync_reference_catalog(conn)

    assert inserted == 5
    assert len(conn.batch_calls) == 1
    _, rows = conn.batch_calls[0]
    slugs = {row[0] for row in rows}
    assert "@gmail/search_emails" in slugs
    assert "@gmail/read_thread" in slugs
    assert "#ticket" in slugs
    assert "#ticket/status" in slugs
    assert "auto/build" in slugs
