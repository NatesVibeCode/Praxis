from __future__ import annotations

from surfaces.mcp.tools import operator


def test_provider_availability_refresh_tool_dispatches_cqrs_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute(_subsystems, *, operation_name: str, payload: dict):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "status": "succeeded"}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _fake_execute)

    result = operator.tool_praxis_provider_availability_refresh(
        {
            "provider_slugs": ["OpenAI"],
            "adapter_types": "cli_llm",
            "timeout_s": "30",
            "max_concurrency": "1",
            "refresh_control_plane": "true",
            "include_snapshots": "false",
        }
    )

    assert result == {"ok": True, "status": "succeeded"}
    assert captured["operation_name"] == "operator.provider_availability_refresh"
    assert captured["payload"] == {
        "provider_slugs": ["OpenAI"],
        "adapter_types": ["cli_llm"],
        "timeout_s": 30,
        "max_concurrency": 1,
        "refresh_control_plane": True,
        "runtime_profile_ref": None,
        "include_snapshots": False,
    }


def test_provider_availability_refresh_tool_rejects_bad_concurrency() -> None:
    result = operator.tool_praxis_provider_availability_refresh({"max_concurrency": "zero"})

    assert result["ok"] is False
    assert result["error_code"] == "operator.provider_availability_refresh.invalid_input"
