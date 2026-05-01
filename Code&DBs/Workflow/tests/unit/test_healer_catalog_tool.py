"""Unit tests for the praxis_healer_* MCP wrappers.

Mirrors test_verifier_catalog_tool. Each wrapper is a thin gateway
dispatch — verify the right operation_name is picked, None values are
dropped from the payload, and the TOOLS-dict shape is sound.
"""

from __future__ import annotations

from typing import Any

from surfaces.mcp.tools import healer_catalog


def _stub_gateway(captured: dict[str, Any], stub_response: dict[str, Any]):
    def stub(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return stub_response

    return stub


# =====================================================================
# praxis_healer_catalog (read)
# =====================================================================


def test_catalog_dispatches_to_healer_catalog_list(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    response = {
        "ok": True,
        "operation": "healer.catalog.list",
        "count": 3,
        "items": [
            {"healer_ref": "healer.platform.schema_bootstrap", "enabled": True,
             "bound_verifier_refs": ["verifier.platform.schema_authority"]},
        ],
    }
    monkeypatch.setattr(healer_catalog, "execute_operation_from_env",
                        _stub_gateway(captured, response))
    monkeypatch.setattr(healer_catalog, "workflow_database_env", lambda: object())

    result = healer_catalog.tool_praxis_healer_catalog({"enabled": True, "limit": 50})

    assert captured["operation_name"] == "healer.catalog.list"
    assert captured["payload"] == {"enabled": True, "limit": 50}
    assert result["ok"] is True
    assert result["count"] == 3


def test_catalog_drops_none(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(healer_catalog, "execute_operation_from_env",
                        _stub_gateway(captured, {"ok": True, "count": 0, "items": []}))
    monkeypatch.setattr(healer_catalog, "workflow_database_env", lambda: object())

    healer_catalog.tool_praxis_healer_catalog({"enabled": None, "limit": 100, "extra": None})

    assert captured["payload"] == {"limit": 100}


def test_catalog_tools_dict_shape() -> None:
    handler, meta = healer_catalog.TOOLS["praxis_healer_catalog"]
    assert callable(handler)
    assert meta["kind"] == "search"
    assert meta["operation_names"] == ["healer.catalog.list"]
    schema = meta["inputSchema"]
    assert set(schema["properties"].keys()) == {"enabled", "limit"}
    assert schema.get("additionalProperties") is False


# =====================================================================
# praxis_healer_runs_list (read)
# =====================================================================


def test_runs_list_dispatches(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    response = {"ok": True, "operation": "healer.runs.list", "count": 1, "items": []}
    monkeypatch.setattr(healer_catalog, "execute_operation_from_env",
                        _stub_gateway(captured, response))
    monkeypatch.setattr(healer_catalog, "workflow_database_env", lambda: object())

    result = healer_catalog.tool_praxis_healer_runs_list({
        "verifier_ref": "verifier.platform.receipt_provenance",
        "status": "succeeded",
        "limit": 20,
    })

    assert captured["operation_name"] == "healer.runs.list"
    assert captured["payload"] == {
        "verifier_ref": "verifier.platform.receipt_provenance",
        "status": "succeeded",
        "limit": 20,
    }
    assert result["ok"] is True


def test_runs_list_tools_dict_shape() -> None:
    handler, meta = healer_catalog.TOOLS["praxis_healer_runs_list"]
    assert callable(handler)
    assert meta["kind"] == "search"
    assert meta["operation_names"] == ["healer.runs.list"]
    schema = meta["inputSchema"]
    props = schema["properties"]
    assert set(props.keys()) == {
        "healer_ref", "verifier_ref", "target_kind", "target_ref",
        "status", "since_iso", "limit",
    }
    assert props["target_kind"]["enum"] == ["platform", "receipt", "run", "path"]
    assert props["status"]["enum"] == ["succeeded", "failed", "skipped", "error"]
    assert schema.get("additionalProperties") is False


# =====================================================================
# praxis_healer_run (write)
# =====================================================================


def test_run_dispatches(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    response = {
        "ok": True,
        "operation": "healer.run.completed",
        "verifier_ref": "verifier.platform.schema_authority",
        "healer_ref": "healer.platform.schema_bootstrap",
        "healing_run_id": "healing_run:xyz",
        "status": "succeeded",
    }
    monkeypatch.setattr(healer_catalog, "execute_operation_from_env",
                        _stub_gateway(captured, response))
    monkeypatch.setattr(healer_catalog, "workflow_database_env", lambda: object())

    result = healer_catalog.tool_praxis_healer_run({
        "verifier_ref": "verifier.platform.schema_authority",
    })

    assert captured["operation_name"] == "healer.run"
    assert captured["payload"] == {"verifier_ref": "verifier.platform.schema_authority"}
    assert result["ok"] is True
    assert result["status"] == "succeeded"


def test_run_tools_dict_shape() -> None:
    handler, meta = healer_catalog.TOOLS["praxis_healer_run"]
    assert callable(handler)
    assert meta["kind"] == "write"
    assert meta["operation_names"] == ["healer.run"]
    schema = meta["inputSchema"]
    assert schema["required"] == ["verifier_ref"]
    props = schema["properties"]
    assert set(props.keys()) == {
        "verifier_ref", "healer_ref", "target_kind", "target_ref",
        "inputs", "record_run",
    }
    assert props["target_kind"]["enum"] == ["platform", "receipt", "run", "path"]
    assert props["target_kind"]["default"] == "platform"
    assert props["record_run"]["default"] is True
    assert schema.get("additionalProperties") is False


def test_run_command_model_requires_verifier_ref() -> None:
    """Healer.run requires verifier_ref (NOT healer_ref — that's optional)."""
    from runtime.operations.commands.healer_run import HealerRunCommand
    import pytest as _pytest

    # Valid call with just verifier_ref
    cmd = HealerRunCommand(verifier_ref="verifier.platform.schema_authority")
    assert cmd.healer_ref is None  # auto-resolve
    assert cmd.target_kind == "platform"
    assert cmd.record_run is True

    # Both refs explicit
    cmd2 = HealerRunCommand(
        verifier_ref="verifier.platform.receipt_provenance",
        healer_ref="healer.platform.receipt_provenance_backfill",
    )
    assert cmd2.healer_ref == "healer.platform.receipt_provenance_backfill"

    # Missing required verifier_ref
    with _pytest.raises(Exception):
        HealerRunCommand()  # type: ignore[call-arg]


def test_query_runs_list_model_validates_status_enum() -> None:
    """The Pydantic query model rejects out-of-enum status values."""
    from runtime.operations.queries.healer_catalog import QueryHealerRunsList
    import pytest as _pytest

    q = QueryHealerRunsList(status="succeeded")
    assert q.status == "succeeded"

    # 'passed' is verifier-status, not healer-status — must be rejected
    with _pytest.raises(Exception):
        QueryHealerRunsList(status="passed")

    with _pytest.raises(Exception):
        QueryHealerRunsList(status="bogus")
