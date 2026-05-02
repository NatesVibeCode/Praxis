"""Unit tests for the praxis_verifier_register + praxis_healer_register
forge-path MCP wrappers.

Each is a thin gateway dispatch — verify the right operation_name is
picked, payload normalization (None drop) works, and the Pydantic
command-model validators reject malformed inputs.
"""

from __future__ import annotations

from typing import Any

from surfaces.mcp.tools import verifier_register


def _stub_gateway(captured: dict[str, Any], stub_response: dict[str, Any]):
    def stub(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return stub_response

    return stub


# =====================================================================
# praxis_verifier_register
# =====================================================================


def test_verifier_register_dispatches(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    response = {"ok": True, "operation": "verifier.registered",
                "verifier_ref": "verifier.test.x", "verifier_kind": "builtin",
                "enabled": True, "bound_healer_refs": []}
    monkeypatch.setattr(verifier_register, "execute_operation_from_env",
                        _stub_gateway(captured, response))
    monkeypatch.setattr(verifier_register, "workflow_database_env", lambda: object())

    result = verifier_register.tool_praxis_verifier_register({
        "verifier_ref": "verifier.test.x",
        "display_name": "Test",
        "verifier_kind": "builtin",
        "builtin_ref": "verify_schema_authority",
        "decision_ref": "decision.test",
    })

    assert captured["operation_name"] == "verifier.register"
    assert captured["payload"]["verifier_ref"] == "verifier.test.x"
    assert captured["payload"]["verifier_kind"] == "builtin"
    assert result["ok"] is True
    assert result["operation"] == "verifier.registered"


def test_verifier_register_drops_none(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(verifier_register, "execute_operation_from_env",
                        _stub_gateway(captured, {"ok": True}))
    monkeypatch.setattr(verifier_register, "workflow_database_env", lambda: object())

    verifier_register.tool_praxis_verifier_register({
        "verifier_ref": "v.x",
        "display_name": "x",
        "verifier_kind": "builtin",
        "builtin_ref": "f",
        "decision_ref": "d",
        "verification_ref": None,
        "default_inputs": None,
    })

    assert "verification_ref" not in captured["payload"]
    assert "default_inputs" not in captured["payload"]


def test_verifier_register_tools_dict_shape() -> None:
    handler, meta = verifier_register.TOOLS["praxis_verifier_register"]
    assert callable(handler)
    assert meta["kind"] == "write"
    assert meta["operation_names"] == ["verifier.register"]
    schema = meta["inputSchema"]
    assert set(schema["required"]) == {"verifier_ref", "display_name", "verifier_kind", "decision_ref"}
    props = schema["properties"]
    assert props["verifier_kind"]["enum"] == ["builtin", "verification_ref"]
    assert "bind_healer_refs" in props
    assert props["bind_healer_refs"]["type"] == "array"
    assert schema.get("additionalProperties") is False


def test_verifier_register_command_model_kind_target_check() -> None:
    """Pydantic model enforces verifier_kind ↔ builtin_ref/verification_ref."""
    from runtime.operations.commands.verifier_register import VerifierRegisterCommand
    import pytest as _pytest

    # builtin kind requires builtin_ref
    cmd = VerifierRegisterCommand(
        verifier_ref="v.x", display_name="x", verifier_kind="builtin",
        builtin_ref="verify_schema_authority", decision_ref="d",
    )
    assert cmd.builtin_ref == "verify_schema_authority"

    # builtin kind without builtin_ref fails
    with _pytest.raises(Exception):
        VerifierRegisterCommand(
            verifier_ref="v.x", display_name="x", verifier_kind="builtin",
            decision_ref="d",
        )

    # builtin kind WITH verification_ref fails (mutually exclusive)
    with _pytest.raises(Exception):
        VerifierRegisterCommand(
            verifier_ref="v.x", display_name="x", verifier_kind="builtin",
            builtin_ref="f", verification_ref="v.ref", decision_ref="d",
        )

    # verification_ref kind requires verification_ref
    cmd2 = VerifierRegisterCommand(
        verifier_ref="v.x", display_name="x", verifier_kind="verification_ref",
        verification_ref="v.ref", decision_ref="d",
    )
    assert cmd2.verification_ref == "v.ref"

    # verification_ref kind without verification_ref fails
    with _pytest.raises(Exception):
        VerifierRegisterCommand(
            verifier_ref="v.x", display_name="x", verifier_kind="verification_ref",
            decision_ref="d",
        )

    # Bad enum value
    with _pytest.raises(Exception):
        VerifierRegisterCommand(
            verifier_ref="v.x", display_name="x", verifier_kind="bogus",  # type: ignore[arg-type]
            decision_ref="d",
        )


# =====================================================================
# praxis_healer_register
# =====================================================================


def test_healer_register_dispatches(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    response = {"ok": True, "operation": "healer.registered",
                "healer_ref": "healer.test.x", "executor_kind": "builtin",
                "auto_mode": "manual", "safety_mode": "guarded", "enabled": True}
    monkeypatch.setattr(verifier_register, "execute_operation_from_env",
                        _stub_gateway(captured, response))
    monkeypatch.setattr(verifier_register, "workflow_database_env", lambda: object())

    result = verifier_register.tool_praxis_healer_register({
        "healer_ref": "healer.test.x",
        "display_name": "Test",
        "action_ref": "heal_schema_bootstrap",
        "decision_ref": "decision.test",
    })

    assert captured["operation_name"] == "healer.register"
    assert captured["payload"]["healer_ref"] == "healer.test.x"
    assert result["ok"] is True
    assert result["operation"] == "healer.registered"


def test_healer_register_tools_dict_shape() -> None:
    handler, meta = verifier_register.TOOLS["praxis_healer_register"]
    assert callable(handler)
    assert meta["kind"] == "write"
    assert meta["operation_names"] == ["healer.register"]
    schema = meta["inputSchema"]
    assert set(schema["required"]) == {"healer_ref", "display_name", "action_ref", "decision_ref"}
    props = schema["properties"]
    assert props["executor_kind"]["enum"] == ["builtin"]
    assert props["auto_mode"]["enum"] == ["manual", "assisted", "automatic"]
    assert props["safety_mode"]["enum"] == ["guarded", "unsafe"]
    assert props["auto_mode"]["default"] == "manual"
    assert props["safety_mode"]["default"] == "guarded"
    assert schema.get("additionalProperties") is False


def test_healer_register_command_model_validates_enums() -> None:
    """Pydantic model enforces enum values for auto_mode and safety_mode."""
    from runtime.operations.commands.healer_register import HealerRegisterCommand
    import pytest as _pytest

    # Defaults
    cmd = HealerRegisterCommand(
        healer_ref="h.x", display_name="x", action_ref="heal_x", decision_ref="d",
    )
    assert cmd.auto_mode == "manual"
    assert cmd.safety_mode == "guarded"
    assert cmd.executor_kind == "builtin"

    # Bad auto_mode
    with _pytest.raises(Exception):
        HealerRegisterCommand(
            healer_ref="h.x", display_name="x", action_ref="heal_x",
            auto_mode="bogus", decision_ref="d",  # type: ignore[arg-type]
        )

    # Bad safety_mode
    with _pytest.raises(Exception):
        HealerRegisterCommand(
            healer_ref="h.x", display_name="x", action_ref="heal_x",
            safety_mode="bogus", decision_ref="d",  # type: ignore[arg-type]
        )

    # Missing required action_ref
    with _pytest.raises(Exception):
        HealerRegisterCommand(
            healer_ref="h.x", display_name="x", decision_ref="d",  # type: ignore[call-arg]
        )
