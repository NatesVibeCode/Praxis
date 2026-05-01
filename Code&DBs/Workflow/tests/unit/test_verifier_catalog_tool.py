"""Unit tests for the praxis_verifier_catalog MCP wrapper.

The wrapper is a thin gateway dispatch — its only job is to forward params
to ``verifier.catalog.list``. The CQRS handler itself is exercised by
runtime tests; here we just verify the wrapper drops None values, picks
the right operation_name, and returns the gateway payload unchanged.
"""

from __future__ import annotations

from typing import Any

from surfaces.mcp.tools import verifier_catalog


def _stub_gateway(captured: dict[str, Any]) -> Any:
    def stub(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "ok": True,
            "operation": "verifier.catalog.list",
            "count": 2,
            "items": [
                {"verifier_ref": "verifier.platform.schema_authority", "enabled": True},
                {"verifier_ref": "verifier.job.python.pytest_file", "enabled": True},
            ],
        }

    return stub


def test_tool_dispatches_to_verifier_catalog_list(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(verifier_catalog, "execute_operation_from_env", _stub_gateway(captured))
    monkeypatch.setattr(verifier_catalog, "workflow_database_env", lambda: object())

    result = verifier_catalog.tool_praxis_verifier_catalog({"enabled": True, "limit": 50})

    assert captured["operation_name"] == "verifier.catalog.list"
    assert captured["payload"] == {"enabled": True, "limit": 50}
    assert result["ok"] is True
    assert result["operation"] == "verifier.catalog.list"
    assert result["count"] == 2
    assert any(it["verifier_ref"].startswith("verifier.platform.") for it in result["items"])


def test_tool_drops_none_values_from_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(verifier_catalog, "execute_operation_from_env", _stub_gateway(captured))
    monkeypatch.setattr(verifier_catalog, "workflow_database_env", lambda: object())

    verifier_catalog.tool_praxis_verifier_catalog({"enabled": None, "limit": 100, "extra": None})

    assert "enabled" not in captured["payload"]
    assert "extra" not in captured["payload"]
    assert captured["payload"] == {"limit": 100}


def test_tool_passes_empty_payload_when_no_args(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(verifier_catalog, "execute_operation_from_env", _stub_gateway(captured))
    monkeypatch.setattr(verifier_catalog, "workflow_database_env", lambda: object())

    verifier_catalog.tool_praxis_verifier_catalog({})

    assert captured["operation_name"] == "verifier.catalog.list"
    assert captured["payload"] == {}


def test_tools_dict_registers_search_kind() -> None:
    """The TOOLS dict is parsed by ast.literal_eval at catalog-build time —
    keep the metadata shape simple and ensure kind=search so the tool sorts
    into the search bucket alongside praxis_search and praxis_discover."""

    tools = verifier_catalog.TOOLS
    assert "praxis_verifier_catalog" in tools
    handler, meta = tools["praxis_verifier_catalog"]
    assert callable(handler)
    assert meta["kind"] == "search"
    assert meta["operation_names"] == ["verifier.catalog.list"]
    schema = meta["inputSchema"]
    assert schema["type"] == "object"
    props = schema["properties"]
    assert set(props.keys()) == {"enabled", "limit"}
    assert props["enabled"]["type"] == "boolean"
    assert props["limit"]["minimum"] == 1
    assert props["limit"]["maximum"] == 500
    assert schema.get("additionalProperties") is False
