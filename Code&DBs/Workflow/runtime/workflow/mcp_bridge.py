"""Workflow MCP bridge helpers for Docker-executed model jobs.

This module builds the bounded MCP client config that workflow jobs hand to
provider CLIs. The authority comes from the execution bundle, not from ambient
user config on the host.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from runtime.workflow.mcp_session import mint_workflow_mcp_session_token

_MCP_URL_ENV = "PRAXIS_WORKFLOW_MCP_URL"
_MCP_HOST_ENV = "PRAXIS_WORKFLOW_MCP_HOST"
_MCP_PORT_ENV = "PRAXIS_WORKFLOW_MCP_PORT"
_MCP_SCHEME_ENV = "PRAXIS_WORKFLOW_MCP_SCHEME"
_DEFAULT_MCP_PATH = "/mcp"


def _normalize_tool_names(values: object) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raw_items = [part.strip() for part in values.replace("\n", ",").split(",")]
    elif isinstance(values, Sequence):
        raw_items = [str(part or "").strip() for part in values]
    else:
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw_items:
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)
    return normalized


def workflow_mcp_tool_names(execution_bundle: Mapping[str, object] | None) -> list[str]:
    if not isinstance(execution_bundle, Mapping):
        return []
    return _normalize_tool_names(execution_bundle.get("mcp_tool_names"))


def build_workflow_mcp_url(
    *,
    tool_names: Sequence[str],
    prefer_docker: bool,
    workflow_token: str | None = None,
) -> str:
    base_url = str(os.environ.get(_MCP_URL_ENV, "")).strip()
    if not base_url:
        scheme = str(os.environ.get(_MCP_SCHEME_ENV, "http")).strip() or "http"
        host_default = "host.docker.internal" if prefer_docker else "127.0.0.1"
        host = str(os.environ.get(_MCP_HOST_ENV, host_default)).strip() or host_default
        port = str(os.environ.get(_MCP_PORT_ENV, "8420")).strip() or "8420"
        base_url = f"{scheme}://{host}:{port}{_DEFAULT_MCP_PATH}"

    split = urlsplit(base_url)
    query_items = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=False)
        if key not in {"allowed_tools", "workflow_token"}
    ]
    normalized_tools = _normalize_tool_names(tool_names)
    if normalized_tools:
        query_items.append(("allowed_tools", ",".join(normalized_tools)))
    normalized_token = str(workflow_token or "").strip()
    if normalized_token:
        query_items.append(("workflow_token", normalized_token))
    return urlunsplit(
        (
            split.scheme,
            split.netloc,
            split.path or _DEFAULT_MCP_PATH,
            urlencode(query_items, doseq=True),
            split.fragment,
        )
    )


def _mcp_config_json(*, mcp_url: str) -> str:
    return json.dumps(
        {
            "mcpServers": {
                "dag-workflow": {
                    "type": "http",
                    "url": mcp_url,
                }
            }
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _render_mcp_args(template: list[str], *, mcp_url: str) -> list[str]:
    """Render {mcp_url}, {mcp_json}, {mcp_url_quoted} placeholders in a template."""
    mcp_json = _mcp_config_json(mcp_url=mcp_url)
    mcp_url_quoted = json.dumps(mcp_url)
    rendered: list[str] = []
    for arg in template:
        rendered.append(
            arg.replace("{mcp_url}", mcp_url)
            .replace("{mcp_json}", mcp_json)
            .replace("{mcp_url_quoted}", mcp_url_quoted)
        )
    return rendered


def augment_cli_command_for_workflow_mcp(
    *,
    provider_slug: str | None,
    command_parts: Sequence[str],
    execution_bundle: Mapping[str, object] | None,
    prefer_docker: bool,
) -> list[str]:
    """Add provider-specific MCP client config derived from the execution bundle.

    The adapter layer owns the provider MCP argument template. Supported
    placeholders: {mcp_url}, {mcp_json}, {mcp_url_quoted}.
    """

    base_parts = [str(part) for part in command_parts]
    if not base_parts:
        return base_parts

    tool_names = workflow_mcp_tool_names(execution_bundle)
    if not tool_names:
        return base_parts

    provider = str(provider_slug or "").strip().lower()
    workflow_token = mint_workflow_mcp_session_token(
        run_id=str(execution_bundle.get("run_id") or "").strip() or None,
        workflow_id=str(execution_bundle.get("workflow_id") or "").strip() or None,
        job_label=str(execution_bundle.get("job_label") or "").strip(),
        allowed_tools=tool_names,
    )
    mcp_url = build_workflow_mcp_url(
        tool_names=tool_names,
        prefer_docker=prefer_docker,
        workflow_token=workflow_token,
    )

    from adapters.provider_registry import resolve_mcp_args_template

    template = resolve_mcp_args_template(provider)
    if not template:
        return base_parts

    rendered = _render_mcp_args(template, mcp_url=mcp_url)
    return [*base_parts, *rendered]
