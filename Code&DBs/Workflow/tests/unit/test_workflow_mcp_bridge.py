from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

import pytest

from runtime.workflow import mcp_bridge


def test_augment_cli_command_uses_adapter_side_mcp_template_helper(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod

    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_URL", "http://mcp.local/mcp?existing=1")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_mcp_args_template",
        lambda provider_slug: ["--mcp-config", "{mcp_json}", "--strict-mcp-config"]
        if provider_slug == "openai"
        else [],
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "get_profile",
        lambda _provider_slug: (_ for _ in ()).throw(
            AssertionError("mcp_bridge should not read provider profile fields directly")
        ),
    )

    command = mcp_bridge.augment_cli_command_for_workflow_mcp(
        provider_slug="openai",
        command_parts=["codex", "exec", "-"],
        execution_bundle={
            "run_id": "run.alpha",
            "workflow_id": "workflow.alpha",
            "job_label": "job.alpha",
            "mcp_tool_names": ["search", "read"],
        },
        prefer_docker=False,
    )

    assert command[:3] == ["codex", "exec", "-"]
    assert command[3] == "--mcp-config"
    assert command[5] == "--strict-mcp-config"

    config = json.loads(command[4])
    mcp_url = config["mcpServers"]["dag-workflow"]["url"]
    parsed = urlsplit(mcp_url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "http"
    assert parsed.netloc == "mcp.local"
    assert parsed.path == "/mcp"
    assert query["existing"] == ["1"]
    assert query["allowed_tools"] == ["search,read"]
    assert "workflow_token" in query


def test_augment_cli_command_returns_base_command_when_no_template(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod

    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setattr(provider_registry_mod, "resolve_mcp_args_template", lambda _provider_slug: [])

    command = mcp_bridge.augment_cli_command_for_workflow_mcp(
        provider_slug="openai",
        command_parts=["codex", "exec", "-"],
        execution_bundle={"job_label": "job.alpha", "mcp_tool_names": ["search"]},
        prefer_docker=False,
    )

    assert command == ["codex", "exec", "-"]


def test_augment_cli_command_requires_workflow_mcp_url_when_tools_are_injected(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod

    monkeypatch.delenv("PRAXIS_WORKFLOW_MCP_URL", raising=False)
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_mcp_args_template",
        lambda provider_slug: ["--mcp-config", "{mcp_json}"]
        if provider_slug == "openai"
        else [],
    )

    with pytest.raises(RuntimeError, match="PRAXIS_WORKFLOW_MCP_URL is required"):
        mcp_bridge.augment_cli_command_for_workflow_mcp(
            provider_slug="openai",
            command_parts=["codex", "exec", "-"],
            execution_bundle={"job_label": "job.alpha", "mcp_tool_names": ["search"]},
            prefer_docker=False,
        )


def test_google_cli_mcp_overlay_uses_project_settings_json(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_URL", "http://mcp.local/mcp")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")

    overlays = mcp_bridge.workflow_mcp_workspace_overlays(
        provider_slug="google",
        execution_bundle={
            "run_id": "run.alpha",
            "workflow_id": "workflow.alpha",
            "job_label": "job.alpha",
            "mcp_tool_names": ["praxis_query", "praxis_discover"],
        },
        prefer_docker=False,
    )

    assert len(overlays) == 1
    assert overlays[0]["relative_path"] == ".gemini/settings.json"
    payload = json.loads(overlays[0]["content"])
    assert payload["mcpServers"]["dag-workflow"]["type"] == "http"
    assert payload["mcpServers"]["dag-workflow"]["includeTools"] == [
        "praxis_query",
        "praxis_discover",
    ]
    parsed = urlsplit(payload["mcpServers"]["dag-workflow"]["url"])
    query = parse_qs(parsed.query)
    assert query["allowed_tools"] == ["praxis_query,praxis_discover"]
    assert "workflow_token" in query


def test_google_cli_mcp_command_uses_allowed_server_names(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod

    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_URL", "http://mcp.local/mcp")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_mcp_args_template",
        lambda provider_slug: ["--allowed-mcp-server-names", "dag-workflow"]
        if provider_slug == "google"
        else [],
    )

    command = mcp_bridge.augment_cli_command_for_workflow_mcp(
        provider_slug="google",
        command_parts=["gemini", "-p", ".", "-o", "json"],
        execution_bundle={"job_label": "job.alpha", "mcp_tool_names": ["search"]},
        prefer_docker=False,
    )

    assert command[-2:] == ["--allowed-mcp-server-names", "dag-workflow"]
