"""Shared display-name helpers for integration and MCP tool surfaces."""

from __future__ import annotations

from typing import Any, Mapping

_PRAXIS_MCP_SERVER_ID = "praxis-workflow-mcp"


def _text(value: object) -> str:
    return str(value or "").strip()


def _titleize_identifier(identifier: str) -> str:
    parts = [part for part in identifier.split("_") if part]
    return " ".join(part.capitalize() for part in parts)


def _strip_praxis_prefix(name: str) -> str:
    lowered = name.lower()
    if lowered.startswith("praxis:"):
        return name.split(":", 1)[1].strip()
    if lowered.startswith("praxis "):
        return name[7:].strip()
    return name


def is_praxis_tool_integration(row: Mapping[str, Any]) -> bool:
    integration_id = _text(row.get("id")).lower()
    manifest_source = _text(row.get("manifest_source")).lower()
    mcp_server_id = _text(row.get("mcp_server_id")).lower()
    return (
        integration_id.startswith("praxis_")
        or manifest_source == "mcp_tool"
        or mcp_server_id == _PRAXIS_MCP_SERVER_ID
    )


def base_integration_name(row: Mapping[str, Any]) -> str:
    integration_id = _text(row.get("id"))
    if integration_id.lower().startswith("praxis_"):
        derived = _titleize_identifier(integration_id[7:])
        if derived:
            return derived

    name = _text(row.get("name"))
    if is_praxis_tool_integration(row):
        name = _strip_praxis_prefix(name)
    return name or integration_id or "Tool"


def display_name_for_integration(row: Mapping[str, Any]) -> str:
    base_name = base_integration_name(row)
    if is_praxis_tool_integration(row):
        return f"Praxis: {base_name}"
    return base_name


__all__ = [
    "base_integration_name",
    "display_name_for_integration",
    "is_praxis_tool_integration",
]
