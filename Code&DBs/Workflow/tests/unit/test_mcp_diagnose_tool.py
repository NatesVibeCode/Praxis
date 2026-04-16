from __future__ import annotations

from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.tools import diagnose as diagnose_mod
from surfaces.mcp.tools import query as query_mod


def test_praxis_diagnose_is_catalogued() -> None:
    definition = get_tool_catalog()["praxis_diagnose"]

    assert definition.cli_entrypoint == "workflow diagnose"
    assert definition.cli_recommended_alias == "diagnose"
    assert definition.selector_field is None
    assert definition.example_input() == {"run_id": "run_abc123"}


def test_praxis_query_routes_diagnose_requests(monkeypatch) -> None:
    monkeypatch.setattr(
        diagnose_mod,
        "tool_praxis_diagnose",
        lambda params: {"diagnosis": {"run_id": params["run_id"], "receipt_found": True}},
    )

    result = query_mod.tool_praxis_query({"question": "diagnose run run_abc123"})

    assert result["routed_to"] == "workflow_diagnose"
    assert result["run_id"] == "run_abc123"
    assert result["diagnosis"]["diagnosis"]["run_id"] == "run_abc123"
    assert result["diagnosis"]["diagnosis"]["receipt_found"] is True


def test_praxis_query_delegates_to_shared_query_core(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _handle_query(subs, body):
        captured["subs"] = subs
        captured["body"] = dict(body)
        return {"routed_to": "shared_core", "question": body["question"]}

    monkeypatch.setattr(query_mod.workflow_query_core, "handle_query", _handle_query)

    result = query_mod.tool_praxis_query({"question": "show me the issue backlog"})

    assert captured["subs"] is query_mod._subs
    assert captured["body"] == {"question": "show me the issue backlog"}
    assert result == {"routed_to": "shared_core", "question": "show me the issue backlog"}
