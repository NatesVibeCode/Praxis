"""Praxis policy enforcement surface.

This package is the harness-neutral home for the JIT trigger-matching layer
that surfaces operator standing orders at the moment of action. Every entry
point that an agent might use to act on Praxis (MCP tools via
`surfaces.mcp.invocation.invoke_tool`, the praxis CLI, HTTP /mcp,
`bin/praxis-agent`, or per-harness PreToolUse hooks for Claude / Codex /
Gemini) calls into `trigger_check.check(...)` to evaluate whether the
proposed action matches any registered standing-order trigger.

Per `architecture-policy::surfaces::cli-mcp-parallel`: surfaces are siblings
over the same gateway, so policy enforcement must live below them, not at
any one surface tier. This module is that floor.
"""

from .trigger_check import (  # noqa: F401  (re-export)
    TriggerMatch,
    check,
    load_registry,
    render_additional_context,
)
