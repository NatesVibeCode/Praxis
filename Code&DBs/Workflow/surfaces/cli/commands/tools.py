"""Catalog-backed CLI discovery and execution for Praxis tools."""

from __future__ import annotations

import json
from typing import Any, TextIO

from surfaces.cli.mcp_tools import (
    format_badges,
    get_definition,
    load_json_file,
    print_json,
    require_confirmation,
    run_cli_tool,
)
from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog


def _tools_quickstart_text() -> str:
    definitions = _filtered_tools()
    alias_definitions = [definition for definition in definitions if definition.cli_recommended_alias]
    direct_entrypoints = [
        definition
        for definition in alias_definitions
        if definition.cli_tier == "stable"
    ][:6]
    if not direct_entrypoints:
        direct_entrypoints = alias_definitions[:6]

    lines = [
        "usage: workflow tools [list|search|describe|call]",
        "",
        "Tool discovery quickstart:",
        "  workflow tools list",
        "  workflow tools search <topic> [--surface <surface>] [--tier <tier>] [--risk <risk>]",
        "  workflow tools describe <tool|alias>",
        "  workflow tools call <tool|alias> --input-json '<json>' --yes",
        "",
    ]
    if direct_entrypoints:
        lines.append("Common direct entrypoints:")
        for definition in direct_entrypoints:
            lines.append(f"  {definition.cli_entrypoint}  ->  {definition.name}")
        lines.append("")
    lines.extend(
        [
            f"Catalog size: {len(definitions)} tools",
            "Tip: run `workflow tools list --json` for machine-readable discovery.",
            "Tip: run `workflow tools search --surface query --tier stable` to browse a filtered slice.",
            "Tip: run `workflow help <alias>` or `workflow <alias> --help` for alias-specific usage.",
        ]
    )
    return "\n".join(lines)


def _tools_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_tools_quickstart_text() + "\n")
        return 0

    subcommand = args[0]
    tail = args[1:]
    if subcommand == "list":
        return _tools_list_command(tail, stdout=stdout)
    if subcommand == "search":
        return _tools_search_command(tail, stdout=stdout)
    if subcommand == "describe":
        return _tools_describe_command(tail, stdout=stdout)
    if subcommand == "call":
        return _tools_call_command(tail, stdout=stdout)
    stdout.write(f"unknown tools subcommand: {subcommand}\n")
    return 2


def _filtered_tools(
    *,
    surface: str | None = None,
    tier: str | None = None,
    risk: str | None = None,
    search_text: str | None = None,
) -> list[McpToolDefinition]:
    catalog = get_tool_catalog()
    needle = str(search_text or "").strip().lower()
    rows: list[McpToolDefinition] = []
    for definition in sorted(catalog.values(), key=lambda item: item.name):
        if surface and definition.cli_surface != surface:
            continue
        if tier and tier != "all" and definition.cli_tier != tier:
            continue
        if risk and risk != "all" and risk not in definition.risk_levels:
            continue
        if needle and needle not in definition.cli_search_text().lower():
            continue
        rows.append(definition)
    return rows


def _tools_list_command(args: list[str], *, stdout: TextIO) -> int:
    surface = None
    tier = "all"
    risk = "all"
    as_json = False
    i = 0
    while i < len(args):
        if args[i] == "--surface" and i + 1 < len(args):
            surface = args[i + 1].strip()
            i += 2
        elif args[i] == "--tier" and i + 1 < len(args):
            tier = args[i + 1].strip()
            i += 2
        elif args[i] == "--risk" and i + 1 < len(args):
            risk = args[i + 1].strip()
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    definitions = _filtered_tools(surface=surface, tier=tier, risk=risk)
    if as_json:
        payload = [
            {
                "name": definition.name,
                "surface": definition.cli_surface,
                "tier": definition.cli_tier,
                "recommended_alias": definition.cli_recommended_alias,
                "entrypoint": definition.cli_entrypoint,
                "describe_command": definition.cli_describe_command,
                "risk_levels": list(definition.risk_levels),
                "selector_field": definition.selector_field,
                "selector_enum": list(definition.selector_enum),
                "required_args": list(definition.required_args),
                "description": definition.description,
            }
            for definition in definitions
        ]
        print_json(stdout, payload)
        return 0

    header = f"{'TOOL':<32} {'ENTRYPOINT':<40} {'SURFACE':<12} {'TIER':<9} {'RISK':<20} DESCRIPTION"
    stdout.write(header + "\n")
    stdout.write("-" * 160 + "\n")
    for definition in definitions:
        risks = "/".join(definition.risk_levels)
        description = definition.description.split("\n", 1)[0]
        stdout.write(
            f"{definition.name:<32} {definition.cli_entrypoint:<40} {definition.cli_surface:<12} "
            f"{definition.cli_tier:<9} {risks:<20} {description[:50]}\n"
        )
    stdout.write(f"\n{len(definitions)} tool(s)\n")
    return 0


def _tools_search_command(args: list[str], *, stdout: TextIO) -> int:
    as_json = False
    surface = None
    tier = "all"
    risk = "all"
    query_parts: list[str] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--json":
            as_json = True
            i += 1
        elif arg == "--surface" and i + 1 < len(args):
            surface = args[i + 1].strip()
            i += 2
        elif arg == "--tier" and i + 1 < len(args):
            tier = args[i + 1].strip()
            i += 2
        elif arg == "--risk" and i + 1 < len(args):
            risk = args[i + 1].strip()
            i += 2
        elif arg in {"--surface", "--tier", "--risk"}:
            stdout.write(f"{arg} requires a value\n")
            return 2
        elif arg.startswith("--"):
            stdout.write(f"unknown argument: {arg}\n")
            return 2
        else:
            query_parts.append(arg)
            i += 1
    query = " ".join(query_parts).strip()
    if not query and surface is None and tier == "all" and risk == "all":
        stdout.write(
            "usage: workflow tools search <text> [--surface <surface>] [--tier <tier>] [--risk <risk>] [--json]\n"
        )
        return 2

    definitions = _filtered_tools(surface=surface, tier=tier, risk=risk, search_text=query or None)
    if as_json:
        print_json(
            stdout,
            [
                {
                    "name": definition.name,
                    "surface": definition.cli_surface,
                    "tier": definition.cli_tier,
                    "entrypoint": definition.cli_entrypoint,
                    "recommended_alias": definition.cli_recommended_alias,
                    "badges": list(definition.cli_badges),
                    "when_to_use": definition.cli_when_to_use,
                }
                for definition in definitions
            ],
        )
        return 0

    for definition in definitions:
        stdout.write(f"{definition.name} [{format_badges(definition)}]\n")
        stdout.write(f"  entrypoint: {definition.cli_entrypoint}\n")
        stdout.write(f"  {definition.cli_when_to_use or definition.description.splitlines()[0]}\n")
    stdout.write(f"\n{len(definitions)} tool(s)\n")
    return 0


def _tools_describe_command(args: list[str], *, stdout: TextIO) -> int:
    if not args:
        stdout.write("usage: workflow tools describe <tool|alias> [--json]\n")
        return 2
    tool_name = args[0].strip()
    as_json = "--json" in args[1:]
    definition = get_definition(tool_name)
    if definition is None:
        stdout.write(f"unknown tool: {tool_name}\n")
        return 2

    payload = {
        "name": definition.name,
        "description": definition.description,
        "surface": definition.cli_surface,
        "tier": definition.cli_tier,
        "recommended_alias": definition.cli_recommended_alias,
        "entrypoint": definition.cli_entrypoint,
        "describe_command": definition.cli_describe_command,
        "badges": list(definition.cli_badges),
        "when_to_use": definition.cli_when_to_use,
        "when_not_to_use": definition.cli_when_not_to_use,
        "selector_field": definition.selector_field,
        "selector_enum": list(definition.selector_enum),
        "selector_default": definition.selector_default or definition.default_action,
        "required_args": list(definition.required_args),
        "input_schema": definition.input_schema,
        "risk_levels": list(definition.risk_levels),
        "requires_workflow_token": definition.requires_workflow_token,
        "examples": list(definition.cli_examples),
        "example_input": definition.example_input(),
    }
    if as_json:
        print_json(stdout, payload)
        return 0

    stdout.write(f"{definition.name}\n")
    stdout.write(f"badges: {format_badges(definition)}\n")
    stdout.write(f"entrypoint: {definition.cli_entrypoint}\n")
    stdout.write(f"describe_command: {definition.cli_describe_command}\n")
    stdout.write(f"description: {definition.description.splitlines()[0]}\n")
    if definition.cli_when_to_use:
        stdout.write(f"when_to_use: {definition.cli_when_to_use}\n")
    if definition.cli_when_not_to_use:
        stdout.write(f"when_not_to_use: {definition.cli_when_not_to_use}\n")
    if definition.selector_field:
        stdout.write(
            f"selector: {definition.selector_field} "
            f"(default: {definition.selector_default or definition.default_action}; "
            f"values: {', '.join(definition.selector_enum)})\n"
        )
    else:
        stdout.write("selector: none\n")
    stdout.write(f"required_args: {', '.join(definition.required_args) or '(none)'}\n")
    stdout.write(f"workflow_token_required: {'yes' if definition.requires_workflow_token else 'no'}\n")
    stdout.write("example_input:\n")
    stdout.write(json.dumps(definition.example_input(), indent=2, default=str) + "\n")
    stdout.write("input_schema:\n")
    stdout.write(json.dumps(definition.input_schema, indent=2, default=str) + "\n")
    return 0


def _tools_call_command(args: list[str], *, stdout: TextIO) -> int:
    if not args:
        stdout.write(
            "usage: workflow tools call <tool|alias> [--input-json <json> | --input-file <path>] [--workflow-token <token>] [--yes] [--json]\n"
        )
        return 2
    tool_name = args[0].strip()
    definition = get_definition(tool_name)
    if definition is None:
        stdout.write(f"unknown tool: {tool_name}\n")
        return 2

    input_json = None
    input_file = None
    workflow_token = ""
    confirmed = False
    i = 1
    while i < len(args):
        if args[i] == "--input-json" and i + 1 < len(args):
            input_json = args[i + 1]
            i += 2
        elif args[i] == "--input-file" and i + 1 < len(args):
            input_file = args[i + 1]
            i += 2
        elif args[i] == "--workflow-token" and i + 1 < len(args):
            workflow_token = args[i + 1]
            i += 2
        elif args[i] == "--yes":
            confirmed = True
            i += 1
        elif args[i] == "--json":
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    if input_json is not None and input_file is not None:
        stdout.write("pass only one of --input-json or --input-file\n")
        return 2

    try:
        params: dict[str, Any]
        if input_json is not None:
            parsed = json.loads(input_json)
            if not isinstance(parsed, dict):
                stdout.write("--input-json must decode to a JSON object\n")
                return 2
            params = parsed
        elif input_file is not None:
            params = load_json_file(input_file)
        else:
            params = {}
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        stdout.write(f"invalid tool input: {exc}\n")
        return 2

    if definition.requires_workflow_token and not workflow_token:
        stdout.write(f"workflow token required for {definition.name}\n")
        return 2
    confirmation_result = require_confirmation(
        definition,
        params,
        confirmed=confirmed,
        stdout=stdout,
    )
    if confirmation_result is not None:
        return confirmation_result

    exit_code, payload = run_cli_tool(definition.name, params, workflow_token=workflow_token)
    print_json(stdout, payload)
    return exit_code
