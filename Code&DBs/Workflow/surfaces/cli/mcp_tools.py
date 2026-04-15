"""Shared CLI helpers for catalog-backed MCP tool execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TextIO

from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog
from surfaces.mcp.invocation import ToolInvocationError, invoke_tool


def print_json(stdout: TextIO, payload: Any) -> None:
    stdout.write(json.dumps(payload, indent=2, default=str) + "\n")


def load_json_file(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("input file must contain a JSON object")
    return payload


def run_cli_tool(
    tool_name: str,
    params: dict[str, Any] | None = None,
    *,
    workflow_token: str = "",
) -> tuple[int, dict[str, Any]]:
    try:
        result = invoke_tool(tool_name, params or {}, workflow_token=workflow_token)
    except ToolInvocationError as exc:
        return 1, exc.to_payload()
    if isinstance(result, dict) and result.get("error"):
        return 1, result
    return 0, result


def render_bug_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    bugs = payload.get("bugs")
    if not isinstance(bugs, list):
        print_json(stdout, payload)
        return
    if not bugs:
        stdout.write("no bugs found\n")
        return
    header = f"{'BUG ID':<16} {'SEV':>3} {'STATUS':<12} {'CATEGORY':<12} TITLE"
    stdout.write(header + "\n")
    stdout.write("-" * (len(header) + 20) + "\n")
    for bug in bugs:
        if not isinstance(bug, dict):
            continue
        title = str(bug.get("title") or "")[:60]
        stdout.write(
            f"{str(bug.get('bug_id') or ''):<16} "
            f"{str(bug.get('severity') or ''):>3} "
            f"{str(bug.get('status') or ''):<12} "
            f"{str(bug.get('category') or ''):<12} "
            f"{title}\n"
        )
    total = payload.get("returned_count", len(bugs))
    stdout.write(f"\n{total} bug(s)\n")


def render_recall_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    results = payload.get("results")
    if not isinstance(results, list):
        print_json(stdout, payload)
        return
    if not results:
        stdout.write("no results found\n")
        return
    for result in results:
        if not isinstance(result, dict):
            continue
        score = float(result.get("score") or 0)
        result_type = str(result.get("type") or "")
        name = str(result.get("name") or "(unnamed)")
        stdout.write(f"  [{score:.2f}] {result_type:<12} {name}\n")
        content = str(result.get("content") or "").strip()
        if content:
            stdout.write(f"           {content[:120].replace(chr(10), ' ')}\n")
    stdout.write(f"\n{len(results)} result(s)\n")


def render_discover_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    results = payload.get("results")
    if not isinstance(results, list):
        print_json(stdout, payload)
        return
    if not results:
        stdout.write("no matches found\n")
        return
    for result in results:
        if not isinstance(result, dict):
            continue
        similarity = float(result.get("similarity") or 0)
        kind = str(result.get("kind") or "")
        name = str(result.get("name") or "")
        path = str(result.get("path") or "")
        stdout.write(f"  [{similarity:.2f}] {kind:<10} {name}\n")
        if path:
            stdout.write(f"           {path}\n")
        description = str(result.get("description") or "").strip()
        if description:
            stdout.write(f"           {description[:100]}\n")
    stdout.write(f"\n{len(results)} match(es)\n")


def render_artifacts_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        print_json(stdout, payload)
        return
    note = str(payload.get("note") or "").strip()
    sandbox_id = str(payload.get("sandbox_id") or "").strip()
    if note:
        stdout.write(f"{note}\n")
    if sandbox_id:
        stdout.write(f"sandbox: {sandbox_id}\n")
        if artifacts:
            stdout.write("\n")
    if not artifacts:
        stdout.write(str(payload.get("message") or "no matching artifacts\n").rstrip() + "\n")
        return
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("file_path") or "")
        byte_count = artifact.get("byte_count")
        line_count = artifact.get("line_count")
        details = []
        if byte_count is not None:
            details.append(f"{byte_count} bytes")
        if line_count is not None:
            details.append(f"{line_count} lines")
        suffix = f" ({', '.join(details)})" if details else ""
        stdout.write(f"  {path}{suffix}\n")
    stdout.write(f"\n{len(artifacts)} artifact(s)\n")


def render_health_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    preflight = payload.get("preflight")
    if not isinstance(preflight, dict):
        print_json(stdout, payload)
        return
    overall = str(preflight.get("overall") or "unknown").upper()
    stdout.write(f"=== System Health: {overall} ===\n\n")
    checks = preflight.get("checks")
    if isinstance(checks, list) and checks:
        stdout.write("Probes\n")
        stdout.write("-" * 50 + "\n")
        for check in checks:
            if not isinstance(check, dict):
                continue
            marker = "OK" if check.get("passed") else "FAIL"
            stdout.write(
                f"  [{marker:>4}] {str(check.get('name') or '?')}: "
                f"{str(check.get('message') or '')}\n"
            )
    snapshot = payload.get("operator_snapshot")
    if isinstance(snapshot, dict):
        stdout.write("\nOperator\n")
        stdout.write("-" * 50 + "\n")
        for key in (
            "total_runs",
            "succeeded",
            "failed",
            "pass_rate",
            "adjusted_pass_rate",
            "avg_cost",
            "avg_tool_uses",
        ):
            if key in snapshot:
                stdout.write(f"  {key}: {snapshot[key]}\n")
    provider_registry = payload.get("provider_registry")
    if isinstance(provider_registry, dict):
        stdout.write("\nRouting\n")
        stdout.write("-" * 50 + "\n")
        default_provider = str(provider_registry.get("default_provider_slug") or "").strip()
        default_adapter = str(provider_registry.get("default_adapter_type") or "").strip()
        if default_provider:
            suffix = f" ({default_adapter})" if default_adapter else ""
            stdout.write(f"  default: {default_provider}{suffix}\n")
        providers = provider_registry.get("providers")
        if isinstance(providers, list):
            for provider in providers:
                if not isinstance(provider, dict):
                    continue
                provider_slug = str(provider.get("provider_slug") or "").strip()
                adapters = provider.get("adapters")
                if not provider_slug or not isinstance(adapters, list):
                    continue
                adapter_text = ", ".join(str(adapter) for adapter in adapters if adapter)
                stdout.write(f"  {provider_slug}: {adapter_text or 'no admitted adapters'}\n")
    trend = payload.get("trend_observability")
    if isinstance(trend, dict):
        summary = trend.get("summary") if isinstance(trend.get("summary"), dict) else {}
        stdout.write("\nTrends\n")
        stdout.write("-" * 50 + "\n")
        stdout.write(
            "  total_trends: "
            f"{summary.get('total_trends', 0)} "
            f"critical={summary.get('critical_trends', 0)} "
            f"warning={summary.get('warning_trends', 0)} "
            f"info={summary.get('info_trends', 0)}\n"
        )
        stdout.write(
            "  direction: "
            f"degrading={summary.get('degrading_trends', 0)} "
            f"accelerating={summary.get('accelerating_trends', 0)} "
            f"improving={summary.get('improving_trends', 0)}\n"
        )
        digest = str(trend.get("trend_digest") or "").strip()
        if digest:
            first_line = digest.splitlines()[0]
            stdout.write(f"  digest: {first_line}\n")
    lane = payload.get("lane_recommendation")
    if isinstance(lane, dict):
        posture = lane.get("recommended_posture")
        reasons = lane.get("reasons") or []
        stdout.write(f"\nLane: {posture}")
        if reasons:
            stdout.write(f" - {reasons[0]}")
        stdout.write("\n")


def format_badges(definition: McpToolDefinition) -> str:
    return ", ".join(definition.cli_badges)


def tool_preflight_lines(definition: McpToolDefinition, params: dict[str, Any]) -> list[str]:
    selector_field = definition.selector_field
    selector_value = None
    if selector_field is not None:
        selector_value = params.get(selector_field, definition.selector_default or definition.default_action)
    lines = [
        f"tool: {definition.name}",
        f"surface: {definition.cli_surface}",
        f"tier: {definition.cli_tier}",
        f"risk: {definition.risk_for_params(params)}",
    ]
    if selector_field is not None:
        lines.append(f"{selector_field}: {selector_value}")
    if definition.cli_when_to_use:
        lines.append(f"when_to_use: {definition.cli_when_to_use}")
    return lines


def require_confirmation(
    definition: McpToolDefinition,
    params: dict[str, Any],
    *,
    confirmed: bool,
    stdout: TextIO,
) -> int | None:
    risk = definition.risk_for_params(params)
    if risk not in {"write", "dispatch"} or confirmed:
        return None
    for line in tool_preflight_lines(definition, params):
        stdout.write(line + "\n")
    stdout.write("confirmation required: rerun with --yes\n")
    return 2


def get_definition(tool_name: str) -> McpToolDefinition | None:
    catalog = get_tool_catalog()
    definition = catalog.get(tool_name)
    if definition is not None:
        return definition

    alias = str(tool_name or "").strip()
    if not alias:
        return None
    for candidate in catalog.values():
        if candidate.cli_recommended_alias == alias:
            return candidate
    return None
