from __future__ import annotations

from surfaces.mcp.tools import synthetic_data


def test_synthetic_data_mcp_tools_use_gateway(monkeypatch) -> None:
    captured: list[tuple[str, dict]] = []

    def _execute(*, env, operation_name: str, payload: dict):
        captured.append((operation_name, payload))
        return {"ok": True, "operation": operation_name}

    monkeypatch.setattr(synthetic_data, "execute_operation_from_env", _execute)

    assert synthetic_data.tool_praxis_synthetic_data_generate(
        {"intent": "renewal risk", "namespace": "demo", "ignored": None}
    ) == {"ok": True, "operation": "synthetic_data_generate"}
    assert synthetic_data.tool_praxis_synthetic_data_read(
        {"action": "describe_dataset", "dataset_ref": "synthetic_dataset.demo"}
    ) == {"ok": True, "operation": "synthetic_data_read"}

    assert captured == [
        ("synthetic_data_generate", {"intent": "renewal risk", "namespace": "demo"}),
        ("synthetic_data_read", {"action": "describe_dataset", "dataset_ref": "synthetic_dataset.demo"}),
    ]
