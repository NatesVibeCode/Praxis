"""MCP integration gate probes.

Checks that Claude Code's per-user .mcp.json has a valid praxis entry pointing
at this repo's workflow root. When missing, operators cannot use
``praxis_*`` MCP tools inside Claude Code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from .graph import (
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
    gate_result,
)


_CLAUDE_CODE_MCP = GateProbe(
    gate_ref="mcp.claude_code",
    domain="mcp",
    title="Claude Code .mcp.json has praxis entry",
    purpose=(
        "Claude Code reads ~/.claude/.mcp.json to discover MCP servers. "
        "Without a praxis entry pointing at this repo's Code&DBs/Workflow "
        "with a valid WORKFLOW_DATABASE_URL, praxis_* MCP tools are unreachable "
        "from Claude Code."
    ),
    ok_cache_ttl_s=600,
)


def _mcp_path(env: Mapping[str, str]) -> Path:
    claude_home = env.get("CLAUDE_HOME") or str(Path.home() / ".claude")
    return Path(claude_home) / ".mcp.json"


def _workflow_root(repo_root: Path) -> Path:
    return repo_root / "Code&DBs" / "Workflow"


def probe_claude_code_mcp(env: Mapping[str, str], repo_root: Path) -> GateResult:
    mcp_path = _mcp_path(env)
    expected_cwd = _workflow_root(repo_root)
    remediation_write = (
        f"Write {mcp_path} with a praxis mcpServers entry: command=python, "
        f'args=["-m","surfaces.mcp.server"], cwd={expected_cwd}, '
        "env.WORKFLOW_DATABASE_URL=<your DSN>. See SETUP.md section MCP Setup."
    )
    if not mcp_path.exists():
        return gate_result(
            _CLAUDE_CODE_MCP,
            status="missing",
            observed_state={"mcp_json_path": str(mcp_path), "exists": False},
            remediation_hint=remediation_write,
        )
    try:
        raw = mcp_path.read_text(encoding="utf-8")
        config = json.loads(raw) if raw.strip() else {}
    except (OSError, json.JSONDecodeError) as exc:
        return gate_result(
            _CLAUDE_CODE_MCP,
            status="blocked",
            observed_state={"mcp_json_path": str(mcp_path), "parse_error": str(exc)},
            remediation_hint=(
                f"{mcp_path} is not valid JSON. Fix the syntax or rewrite the praxis entry per SETUP.md."
            ),
        )
    servers = (config or {}).get("mcpServers") or {}
    praxis_entry = servers.get("praxis")
    if not isinstance(praxis_entry, dict):
        return gate_result(
            _CLAUDE_CODE_MCP,
            status="missing",
            observed_state={"mcp_json_path": str(mcp_path), "praxis_entry_present": False},
            remediation_hint=remediation_write,
        )
    configured_cwd = str(praxis_entry.get("cwd") or "").strip()
    entry_env = praxis_entry.get("env") or {}
    database_url = str(entry_env.get("WORKFLOW_DATABASE_URL") or "").strip()
    issues: list[str] = []
    if not configured_cwd:
        issues.append("cwd missing")
    elif Path(configured_cwd).resolve() != expected_cwd.resolve():
        issues.append(f"cwd points at {configured_cwd!r}, expected {str(expected_cwd)!r}")
    if not database_url:
        issues.append("env.WORKFLOW_DATABASE_URL missing")
    if issues:
        return gate_result(
            _CLAUDE_CODE_MCP,
            status="blocked",
            observed_state={
                "mcp_json_path": str(mcp_path),
                "praxis_entry_present": True,
                "configured_cwd": configured_cwd,
                "database_url_set": bool(database_url),
                "issues": issues,
            },
            remediation_hint=(
                f"Praxis mcpServers entry has issues ({'; '.join(issues)}). "
                f"Update {mcp_path}: cwd={expected_cwd}, env.WORKFLOW_DATABASE_URL=<your DSN>."
            ),
        )
    return gate_result(
        _CLAUDE_CODE_MCP,
        status="ok",
        observed_state={
            "mcp_json_path": str(mcp_path),
            "configured_cwd": configured_cwd,
            "database_url_set": True,
        },
    )


def register(graph=ONBOARDING_GRAPH) -> None:
    graph.register(_CLAUDE_CODE_MCP, probe_claude_code_mcp)
