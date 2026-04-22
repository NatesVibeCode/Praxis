"""CLI and MCP front-door CQRS authority boundary tests."""

from __future__ import annotations

import json
import importlib
import sys
from io import StringIO
from pathlib import Path

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.cli.frontdoor_authority import (
    CliFrontdoorAuthorityError,
    build_cli_frontdoor_authority_payload,
    classify_cli_frontdoors,
    command_authority,
    workflow_command_ref,
)
from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog
from surfaces.mcp.frontdoor_authority import (
    FrontdoorAuthorityError,
    assert_mcp_tool_authority_contract,
    build_mcp_tool_authority_payload,
    classify_mcp_tool_catalog,
)
from surfaces.mcp import invocation

workflow_cli = importlib.import_module("surfaces.cli.main")


def _definition(
    *,
    name: str,
    surface: str,
    risks: dict[str, object],
    actions: list[str] | None = None,
) -> McpToolDefinition:
    properties: dict[str, object] = {}
    if actions is not None:
        properties["action"] = {
            "type": "string",
            "enum": actions,
            "default": actions[0],
        }
    return McpToolDefinition(
        name=name,
        module_name="surfaces.mcp.tools.test",
        handler_name="handler",
        metadata={
            "description": f"{name} test tool",
            "inputSchema": {"type": "object", "properties": properties},
            "cli": {
                "surface": surface,
                "tier": "advanced",
                "risks": risks,
            },
        },
        selector_defaults={},
    )


def test_mcp_catalog_frontdoor_authority_classifies_all_mutating_tools() -> None:
    payload = classify_mcp_tool_catalog()

    assert payload["drift"]["unknown_mutating_contracts"] == []
    assert payload["mutating_contract_count"] > 0

    rows = {
        (row["tool_name"], row["selector_value"]): row
        for row in payload["contracts"]
    }
    assert rows[("praxis_workflow", "run")]["authority_domain_ref"] == "authority.workflow_runs"
    assert rows[("praxis_session_context", "write")]["authority_domain_ref"] == "authority.workflow_mcp_session"
    assert rows[("praxis_submit_code_change", "submit_code_change")]["authority_domain_ref"] == "authority.workflow_submissions"


def test_mcp_mutating_tool_with_unknown_surface_fails_closed() -> None:
    definition = _definition(
        name="praxis_unknown_mutator",
        surface="unknown_surface",
        risks={"default": "write"},
    )

    with pytest.raises(FrontdoorAuthorityError) as exc_info:
        assert_mcp_tool_authority_contract(definition, {})

    drift = exc_info.value.drift
    assert drift["unknown_mutating_contracts"][0]["tool_name"] == "praxis_unknown_mutator"


def test_mcp_invocation_records_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _definition(
        name="praxis_known_mutator",
        surface="operator",
        risks={"default": "write"},
    )
    recorded: dict[str, object] = {}

    monkeypatch.setattr(invocation, "get_tool_catalog", lambda: {definition.name: definition})
    monkeypatch.setattr(invocation, "resolve_tool_entry", lambda _name: (lambda _payload: {"ok": True}, {}))

    def _record_tool_usage(**kwargs: object) -> None:
        recorded.update(kwargs)

    monkeypatch.setattr(invocation, "_record_tool_usage", _record_tool_usage)

    assert invocation.invoke_tool(definition.name, {}) == {"ok": True}
    contract = recorded["authority_contract"]
    assert isinstance(contract, dict)
    assert contract["authority_domain_ref"] == "authority.operator_control"
    assert contract["risk"] == "write"


def test_workflow_cli_commands_are_authority_classified() -> None:
    command_refs = [workflow_command_ref(command) for command in workflow_cli._known_root_commands()]
    payload = build_cli_frontdoor_authority_payload(command_refs)

    assert payload["ok"] is True
    assert payload["drift"]["unknown_command_refs"] == []
    rows = {row["command_ref"]: row for row in payload["contracts"]}
    assert rows["workflow run"]["authority_domain_ref"] == "authority.workflow_runs"
    assert rows["workflow tools"]["cqrs_entrypoint"] == "mcp_frontdoor_authority"


def test_unclassified_workflow_cli_command_is_reported_by_single_gate() -> None:
    payload = classify_cli_frontdoors(["workflow unsafe"])

    assert payload["drift"]["unknown_command_refs"] == ["workflow unsafe"]


def test_root_praxis_namespaces_are_authority_classified() -> None:
    payload = classify_cli_frontdoors(
        [
            "praxis workflow",
            "praxis db",
            "praxis launcher",
            "praxis registry",
            "praxis objects",
        ]
    )

    assert payload["drift"]["unknown_command_refs"] == []


def test_unclassified_root_praxis_namespace_contract_fails_closed() -> None:
    with pytest.raises(CliFrontdoorAuthorityError):
        command_authority("praxis unsafe")


def test_tools_authority_json_is_queryable() -> None:
    stdout = StringIO()

    assert workflow_cli.main(["tools", "authority", "--risk", "launch", "--json"], stdout=stdout) == 0
    payload = json.loads(stdout.getvalue())

    assert payload["ok"] is True
    assert payload["routed_to"] == "mcp_frontdoor_authority"
    assert payload["filtered_count"] > 0
    assert all(row["risk"] == "launch" for row in payload["contracts"])


def test_mcp_authority_payload_matches_live_catalog_count() -> None:
    payload = build_mcp_tool_authority_payload()

    assert payload["ok"] is True
    assert payload["tool_count"] == len(get_tool_catalog())
