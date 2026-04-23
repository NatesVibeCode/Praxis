from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from surfaces.api.handlers import workflow_query_core


@pytest.mark.parametrize(
    ("question", "table_name"),
    [
        ("What columns does public.bugs have?", "bugs"),
        ("fields for workflow_runs", "workflow_runs"),
        ("schema of workflow_runs", "workflow_runs"),
    ],
)
def test_schema_and_column_questions_route_to_data_dictionary(
    monkeypatch: pytest.MonkeyPatch,
    question: str,
    table_name: str,
) -> None:
    import runtime.operation_catalog_gateway as operation_catalog_gateway

    captured: dict[str, Any] = {}

    def _execute_operation_from_subsystems(subsystems, *, operation_name, payload=None):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = dict(payload or {})
        return {
            "routed_to": "operator.data_dictionary",
            "table_name": captured["payload"].get("table_name"),
        }

    monkeypatch.setattr(
        operation_catalog_gateway,
        "execute_operation_from_subsystems",
        _execute_operation_from_subsystems,
    )

    subs = SimpleNamespace(get_pg_conn=lambda: object())
    payload = workflow_query_core.handle_query(subs, {"question": question})

    assert payload["routed_to"] == "operator.data_dictionary"
    assert payload["table_name"] == table_name
    assert captured == {
        "subsystems": subs,
        "operation_name": "operator.data_dictionary",
        "payload": {
            "table_name": table_name,
            "include_relationships": True,
        },
    }
