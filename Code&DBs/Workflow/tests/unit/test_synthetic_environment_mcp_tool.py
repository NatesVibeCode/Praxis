from __future__ import annotations

from surfaces.mcp.tools import synthetic_environment


def test_synthetic_environment_mcp_tools_use_gateway(monkeypatch) -> None:
    captured: list[tuple[str, dict]] = []

    def _execute(*, env, operation_name: str, payload: dict):
        captured.append((operation_name, payload))
        return {"ok": True, "operation": operation_name}

    monkeypatch.setattr(synthetic_environment, "execute_operation_from_env", _execute)

    assert synthetic_environment.tool_praxis_synthetic_environment_create(
        {"dataset_ref": "synthetic_dataset.demo", "ignored": None}
    ) == {"ok": True, "operation": "synthetic_environment_create"}
    assert synthetic_environment.tool_praxis_synthetic_environment_clear(
        {"environment_ref": "synthetic_environment.demo"}
    ) == {"ok": True, "operation": "synthetic_environment_clear"}
    assert synthetic_environment.tool_praxis_synthetic_environment_reset(
        {"environment_ref": "synthetic_environment.demo"}
    ) == {"ok": True, "operation": "synthetic_environment_reset"}
    assert synthetic_environment.tool_praxis_synthetic_environment_event_inject(
        {"environment_ref": "synthetic_environment.demo", "event_type": "payment.failed"}
    ) == {"ok": True, "operation": "synthetic_environment_event_inject"}
    assert synthetic_environment.tool_praxis_synthetic_environment_clock_advance(
        {"environment_ref": "synthetic_environment.demo", "seconds": 60}
    ) == {"ok": True, "operation": "synthetic_environment_clock_advance"}
    assert synthetic_environment.tool_praxis_synthetic_environment_read(
        {"action": "diff", "environment_ref": "synthetic_environment.demo"}
    ) == {"ok": True, "operation": "synthetic_environment_read"}

    assert captured == [
        ("synthetic_environment_create", {"dataset_ref": "synthetic_dataset.demo"}),
        ("synthetic_environment_clear", {"environment_ref": "synthetic_environment.demo"}),
        ("synthetic_environment_reset", {"environment_ref": "synthetic_environment.demo"}),
        (
            "synthetic_environment_event_inject",
            {"environment_ref": "synthetic_environment.demo", "event_type": "payment.failed"},
        ),
        ("synthetic_environment_clock_advance", {"environment_ref": "synthetic_environment.demo", "seconds": 60}),
        (
            "synthetic_environment_read",
            {"action": "diff", "environment_ref": "synthetic_environment.demo"},
        ),
    ]
