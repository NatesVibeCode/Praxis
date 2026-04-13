"""Compatibility helpers for the shared MCP tool catalog."""
from __future__ import annotations

from typing import Any

from .catalog import get_tool_catalog, resolve_tool_entry

_ALL_TOOLS: dict[str, tuple[callable, dict[str, Any]]] | None = None


def get_all_tools() -> dict[str, tuple[callable, dict[str, Any]]]:
    """Return the merged TOOLS dict from the shared catalog (cached after first call)."""
    global _ALL_TOOLS
    if _ALL_TOOLS is not None:
        return _ALL_TOOLS

    _ALL_TOOLS = {
        name: resolve_tool_entry(name)
        for name in get_tool_catalog()
    }
    return _ALL_TOOLS
