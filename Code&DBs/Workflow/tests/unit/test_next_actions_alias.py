from __future__ import annotations

from typing import Any

from surfaces.mcp.catalog import get_tool_catalog


def test_next_actions_alias_points_to_praxis_next() -> None:
    get_tool_catalog.cache_clear()
    definition = get_tool_catalog()["praxis_next_actions"]

    assert definition.cli_surface == "operator"
    assert definition.cli_tier == "stable"
    assert definition.cli_recommended_alias == "next-actions"
    assert definition.cli_replacement == "praxis_next"
    assert definition.risk_levels == ("read",)
    assert "Deprecated compatibility alias" in definition.description


def test_next_actions_alias_delegates_to_operator_next(monkeypatch: Any) -> None:
    from surfaces.mcp.tools import next_actions

    calls: list[dict[str, Any]] = []

    def fake_execute_operation_from_subsystems(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"ok": True, "tool": "praxis_next", "action": kwargs["payload"]["action"]}

    monkeypatch.setattr(
        next_actions,
        "execute_operation_from_subsystems",
        fake_execute_operation_from_subsystems,
    )

    result = next_actions.tool_praxis_next_actions({"intent": "fix retries"})

    assert result == {"ok": True, "tool": "praxis_next", "action": "next", "_meta": {"dispatch_path": "gateway"}}
    assert calls[0]["operation_name"] == "operator.next"
    assert calls[0]["payload"]["action"] == "next"
    assert calls[0]["payload"]["intent"] == "fix retries"
