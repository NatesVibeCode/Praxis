"""Catalog-backed CLI discovery and execution for Praxis tools."""

from __future__ import annotations

import json
from typing import Any, TextIO

from runtime.interpretive_context import (
    attach_interpretive_context_to_items,
    build_tool_interpretive_context,
    tool_catalog_item_candidates,
)
from surfaces.cli.mcp_tools import (
    format_badges,
    get_definition,
    load_json_file,
    print_json,
    require_confirmation,
    run_cli_tool,
)
from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog


_TOOL_LIST_CONTEXT_LIMIT = 5
_TOOL_LIST_CONTEXT_FIELD_LIMIT = 4
_TOOL_DESCRIBE_CONTEXT_FIELD_LIMIT = 8


def _normalize_search_text(value: str | None) -> str:
    return " ".join(str(value or "").lower().split())


def _tool_exact_matches(definition: McpToolDefinition, needle: str) -> bool:
    normalized_needle = _normalize_search_text(needle)
    if not normalized_needle:
        return False

    exact_fields = [
        definition.name,
        definition.display_name,
        definition.cli_recommended_alias or "",
        definition.cli_entrypoint,
        definition.cli_describe_command,
    ]
    return any(
        _normalize_search_text(field) == normalized_needle
        for field in exact_fields
        if field
    )


def _tool_search_rank(definition: McpToolDefinition, needle: str) -> tuple[int, int, int, str]:
    normalized_needle = _normalize_search_text(needle)
    if not normalized_needle:
        return (0, 0, 0, definition.name)

    ranked_fields = [
        definition.cli_recommended_alias or "",
        definition.name,
        definition.cli_entrypoint,
        definition.cli_describe_command,
        definition.cli_when_to_use,
        definition.description,
        definition.cli_when_not_to_use,
    ]

    for field_rank, raw_text in enumerate(ranked_fields):
        normalized_text = _normalize_search_text(raw_text)
        if not normalized_text:
            continue
        if normalized_text == normalized_needle:
            return (0, field_rank, 0, definition.name)
        if normalized_text.startswith(normalized_needle):
            return (1, field_rank, 0, definition.name)

    best_rank = (2, len(ranked_fields), len(normalized_needle), definition.name)
    for field_rank, raw_text in enumerate(ranked_fields):
        normalized_text = _normalize_search_text(raw_text)
        if not normalized_text:
            continue
        index = normalized_text.find(normalized_needle)
        if index >= 0:
            candidate = (2, field_rank, index, definition.name)
            if candidate < best_rank:
                best_rank = candidate
    return best_rank


def _tool_lookup_rank(definition: McpToolDefinition, needle: str) -> tuple[int, int, int, str] | None:
    normalized_needle = _normalize_search_text(needle)
    if not normalized_needle:
        return None

    ranked_fields = [
        definition.name,
        definition.display_name,
        definition.cli_recommended_alias or "",
        definition.cli_entrypoint,
        definition.cli_describe_command,
    ]
    best_rank: tuple[int, int, int, str] | None = None
    for field_rank, raw_text in enumerate(ranked_fields):
        normalized_text = _normalize_search_text(raw_text)
        if not normalized_text:
            continue
        if normalized_text == normalized_needle:
            return (0, field_rank, 0, definition.name)
        if normalized_text.startswith(normalized_needle):
            candidate = (1, field_rank, 0, definition.name)
            if best_rank is None or candidate < best_rank:
                best_rank = candidate
            continue
        index = normalized_text.find(normalized_needle)
        if index >= 0:
            candidate = (2, field_rank, index, definition.name)
            if best_rank is None or candidate < best_rank:
                best_rank = candidate
    return best_rank


def _resolve_tool_definition(tool_name: str) -> tuple[McpToolDefinition | None, list[McpToolDefinition]]:
    exact_definition = get_definition(tool_name)
    if exact_definition is not None:
        return exact_definition, []

    ranked_candidates: list[tuple[tuple[int, int, int, str], McpToolDefinition]] = []
    for definition in get_tool_catalog().values():
        rank = _tool_lookup_rank(definition, tool_name)
        if rank is not None:
            ranked_candidates.append((rank, definition))

    ranked_candidates.sort(key=lambda item: item[0])
    candidates: list[McpToolDefinition] = []
    seen: set[str] = set()
    for _, definition in ranked_candidates:
        if definition.name in seen:
            continue
        seen.add(definition.name)
        candidates.append(definition)

    if len(candidates) == 1:
        return candidates[0], []
    return None, candidates


def _render_tool_lookup_failure(
    tool_name: str,
    candidates: list[McpToolDefinition],
    *,
    stdout: TextIO,
) -> int:
    if not candidates:
        stdout.write(f"unknown tool: {tool_name}\n")
        stdout.write("tip: run `workflow tools search <text>` to browse matching tools.\n")
        return 2

    stdout.write(f"ambiguous tool name: {tool_name}\n")
    stdout.write("did you mean:\n")
    for definition in candidates[:5]:
        stdout.write(f"  {definition.name} [{format_badges(definition)}]\n")
        stdout.write(f"    entrypoint: {definition.cli_entrypoint}\n")
        stdout.write(f"    describe: {definition.cli_describe_command}\n")
    stdout.write("tip: add more characters or run `workflow tools search <text>`.\n")
    return 2


def _render_tools_search_no_matches(
    query: str,
    *,
    exact: bool,
    surface: str | None,
    tier: str,
    risk: str,
    stdout: TextIO,
) -> None:
    if exact:
        stdout.write(f"no exact matches found for {query}\n")
    else:
        target = f"{query!r}" if query else "the current filters"
        stdout.write(f"no tools matched {target}\n")

    active_filters: list[str] = []
    if surface:
        active_filters.append(f"--surface {surface}")
    if tier != "all":
        active_filters.append(f"--tier {tier}")
    if risk != "all":
        active_filters.append(f"--risk {risk}")
    if active_filters:
        stdout.write(f"active filters: {' '.join(active_filters)}\n")

    stdout.write("tips:\n")
    stdout.write("  workflow tools list --json\n")
    stdout.write("  workflow tools search <broader text>\n")
    if exact:
        stdout.write("  add --exact only when you already know the alias, tool name, or entrypoint\n")
    if active_filters:
        stdout.write("  rerun without one or more filters to widen the catalog slice\n")


def _split_tool_reference_and_flags(args: list[str]) -> tuple[str, list[str]]:
    """Split a tool reference from trailing flags.

    Tool entrypoints can contain spaces, so we treat the leading non-flag
    tokens as the tool reference and leave the rest for option parsing.
    """

    reference_tokens: list[str] = []
    for index, arg in enumerate(args):
        if arg.startswith("--"):
            return " ".join(reference_tokens).strip(), args[index:]
        reference_tokens.append(arg)
    return " ".join(reference_tokens).strip(), []


def _tools_help_text(topic: str | None = None) -> str:
    if topic is None:
        return _tools_quickstart_text()

    topic = topic.strip().lower()
    if topic == "list":
        return "\n".join(
            [
                "usage: workflow tools list [--surface <surface>] [--tier <tier>] [--risk <risk>] [--json]",
                "",
                "Browse the catalog or a filtered slice of it.",
                "Tip: add --json for machine-readable discovery.",
            ]
        )
    if topic == "search":
        return "\n".join(
            [
                "usage: workflow tools search <text> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>] [--json]",
                "",
                "Search by topic, alias, entrypoint, or describe-command text.",
                "Tip: add --exact only when you already know the alias, tool name, or entrypoint.",
                "Tip: the CLI prints a best-next-step block when the top match is an exact or prefix hit.",
            ]
        )
    if topic == "describe":
        return "\n".join(
            [
                "usage: workflow tools describe <tool|alias|entrypoint> [--json]",
                "",
                "Show the canonical entrypoint, input schema, and example input for one tool.",
                "Tip: accepts a unique prefix of the alias, tool name, or entrypoint.",
                "Tip: multi-word entrypoints such as `workflow query` can be passed directly without quotes.",
            ]
        )
    if topic == "call":
        return "\n".join(
            [
                "usage: workflow tools call <tool|alias|entrypoint> [--input-json <json> | --input-file <path>] [--workflow-token <token>] [--yes] [--json]",
                "",
                "Execute a tool directly from the catalog.",
                "Tip: add --yes for write or launch tools; add --workflow-token when the catalog requires it.",
                "Tip: multi-word entrypoints such as `workflow query` can be passed directly without quotes.",
            ]
        )
    return "\n".join(
        [
            f"unknown tools help topic: {topic}",
            "",
            "Supported topics:",
            "  workflow tools help list",
            "  workflow tools help search",
            "  workflow tools help describe",
            "  workflow tools help call",
        ]
    )


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
        "usage: workflow tools [list|search|describe|call|help]",
        "",
        "Tool discovery quickstart:",
        "  workflow tools list",
        "  workflow tools search <topic> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>]",
        "  workflow tools describe <tool|alias>",
        "  workflow tools call <tool|alias> --input-json '<json>' --yes",
        "  workflow tools help <list|search|describe|call>",
        "  workflow tools help <tool|alias>",
        "",
        "Tip: each subcommand also accepts --help for command-specific usage.",
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
            "Tip: list/search JSON include when_to_use and when_not_to_use guidance for each tool.",
            "Tip: run `workflow tools search --surface query --tier stable` to browse a filtered slice.",
            "Tip: add `--exact` when you already know the alias, tool name, or entrypoint.",
            "Tip: if a search returns no matches, the CLI prints broadening hints instead of leaving you at zero.",
            "Tip: `workflow tools describe` and `workflow tools call` also accept a unique prefix of the alias, tool name, or entrypoint.",
            "Tip: single-result searches print the direct describe and entrypoint commands next, and the top exact or prefix hit does too.",
            "Tip: search results are relevance-ranked; exact alias and entrypoint matches rise first.",
            "Tip: run `workflow help <alias>` or `workflow <alias> --help` for alias-specific usage.",
            "Tip: `workflow mcp` is the root alias for the same tool-discovery surface.",
        ]
    )
    return "\n".join(lines)


def _tool_catalog_brief_payload(definition: McpToolDefinition) -> dict[str, object]:
    return {
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
        "when_to_use": definition.cli_when_to_use,
        "when_not_to_use": definition.cli_when_not_to_use,
    }


def _tools_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_tools_quickstart_text() + "\n")
        return 0
    if args[0] == "help":
        if len(args) == 1:
            stdout.write(_tools_quickstart_text() + "\n")
            return 0
        topic = args[1].strip()
        if topic in {"list", "search", "describe", "call"}:
            stdout.write(_tools_help_text(topic) + "\n")
            return 0
        return _tools_describe_command(args[1:], stdout=stdout)

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
    needle = _normalize_search_text(search_text)
    rows: list[McpToolDefinition] = []
    for definition in sorted(catalog.values(), key=lambda item: item.name):
        if surface and definition.cli_surface != surface:
            continue
        if tier and tier != "all" and definition.cli_tier != tier:
            continue
        if risk and risk != "all" and risk not in definition.risk_levels:
            continue
        if needle and needle not in _normalize_search_text(definition.cli_search_text()):
            continue
        rows.append(definition)
    return rows


def _workflow_conn():
    from storage.postgres import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool())


def _tool_interpretive_context(definition: McpToolDefinition) -> dict[str, Any]:
    try:
        return build_tool_interpretive_context(
            _workflow_conn(),
            tool_name=definition.name,
            reason="tool_catalog.describe",
            max_fields_per_object=_TOOL_DESCRIBE_CONTEXT_FIELD_LIMIT,
        )
    except Exception:
        return {}


def _attach_tool_list_interpretive_context(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    try:
        return attach_interpretive_context_to_items(
            _workflow_conn(),
            rows,
            candidate_fn=tool_catalog_item_candidates,
            max_context_items=_TOOL_LIST_CONTEXT_LIMIT,
            max_objects_per_item=1,
            max_fields_per_object=_TOOL_LIST_CONTEXT_FIELD_LIMIT,
        )
    except Exception:
        return rows


def _tools_list_command(args: list[str], *, stdout: TextIO) -> int:
    if any(arg in {"-h", "--help"} for arg in args):
        stdout.write(_tools_help_text("list") + "\n")
        return 0
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
        payload = _attach_tool_list_interpretive_context([
            _tool_catalog_brief_payload(definition)
            for definition in definitions
        ])
        print_json(stdout, payload)
        return 0

    header = f"{'TOOL':<32} {'ENTRYPOINT':<28} {'ALIAS':<18} {'SURFACE':<12} {'TIER':<9} {'RISK':<20} DESCRIPTION"
    stdout.write(header + "\n")
    stdout.write("-" * 176 + "\n")
    for definition in definitions:
        risks = "/".join(definition.risk_levels)
        description = definition.description.split("\n", 1)[0]
        alias = definition.cli_recommended_alias or "-"
        stdout.write(
            f"{definition.name:<32} {definition.cli_entrypoint:<28} {alias:<18} {definition.cli_surface:<12} "
            f"{definition.cli_tier:<9} {risks:<20} {description[:50]}\n"
        )
    stdout.write(f"\n{len(definitions)} tool(s)\n")
    return 0


def _tools_search_command(args: list[str], *, stdout: TextIO) -> int:
    if any(arg in {"-h", "--help"} for arg in args):
        stdout.write(_tools_help_text("search") + "\n")
        return 0
    as_json = False
    exact = False
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
        elif arg == "--exact":
            exact = True
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
    if exact and not query:
        stdout.write(
            "usage: workflow tools search <text> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>] [--json]\n"
        )
        stdout.write("error: --exact requires a search text\n")
        return 2
    if not query and surface is None and tier == "all" and risk == "all":
        stdout.write(
            "usage: workflow tools search <text> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>] [--json]\n"
        )
        return 2

    definitions = _filtered_tools(surface=surface, tier=tier, risk=risk, search_text=None if exact else query or None)
    if query and not exact:
        definitions = sorted(definitions, key=lambda definition: _tool_search_rank(definition, query))
    if exact:
        definitions = [definition for definition in definitions if _tool_exact_matches(definition, query)]
    if as_json:
        print_json(
            stdout,
            [
                {
                    **_tool_catalog_brief_payload(definition),
                    "badges": list(definition.cli_badges),
                }
                for definition in definitions
            ],
        )
        return 0

    if exact and not definitions:
        _render_tools_search_no_matches(
            query,
            exact=True,
            surface=surface,
            tier=tier,
            risk=risk,
            stdout=stdout,
        )
        stdout.write("\n0 tool(s)\n")
        return 0
    if not definitions:
        _render_tools_search_no_matches(
            query,
            exact=False,
            surface=surface,
            tier=tier,
            risk=risk,
            stdout=stdout,
        )
        stdout.write("\n0 tool(s)\n")
        return 0
    for definition in definitions:
        stdout.write(f"{definition.name} [{format_badges(definition)}]\n")
        stdout.write(f"  entrypoint: {definition.cli_entrypoint}\n")
        stdout.write(f"  describe: {definition.cli_describe_command}\n")
        stdout.write(f"  {definition.cli_when_to_use or definition.description.splitlines()[0]}\n")
    best_definition = None
    if len(definitions) == 1:
        best_definition = definitions[0]
    elif query:
        top_rank = _tool_search_rank(definitions[0], query)
        if top_rank[0] <= 1:
            best_definition = definitions[0]
    if best_definition is not None:
        stdout.write("\nBest next step:\n")
        stdout.write(f"  {best_definition.cli_describe_command}\n")
        stdout.write(f"  {best_definition.cli_entrypoint}\n")
    stdout.write(f"\n{len(definitions)} tool(s)\n")
    return 0


def _tools_describe_command(args: list[str], *, stdout: TextIO) -> int:
    if any(arg in {"-h", "--help"} for arg in args):
        stdout.write(_tools_help_text("describe") + "\n")
        return 0
    if not args:
        stdout.write("usage: workflow tools describe <tool|alias|entrypoint> [--json]\n")
        return 2
    tool_name, remainder = _split_tool_reference_and_flags(args)
    if not tool_name:
        stdout.write("usage: workflow tools describe <tool|alias|entrypoint> [--json]\n")
        return 2
    as_json = "--json" in remainder
    unknown_args = [arg for arg in remainder if arg != "--json"]
    if unknown_args:
        stdout.write(f"unknown argument: {unknown_args[0]}\n")
        return 2
    definition, candidates = _resolve_tool_definition(tool_name)
    if definition is None:
        return _render_tool_lookup_failure(tool_name, candidates, stdout=stdout)

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
    interpretive_context = _tool_interpretive_context(definition)
    if interpretive_context:
        payload["interpretive_context"] = interpretive_context
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
    if interpretive_context:
        stdout.write("interpretive_context:\n")
        stdout.write(json.dumps(interpretive_context, indent=2, default=str) + "\n")
    return 0


def _tools_call_command(args: list[str], *, stdout: TextIO) -> int:
    if any(arg in {"-h", "--help"} for arg in args):
        stdout.write(_tools_help_text("call") + "\n")
        return 0
    if not args:
        stdout.write(
            "usage: workflow tools call <tool|alias|entrypoint> [--input-json <json> | --input-file <path>] [--workflow-token <token>] [--yes] [--json]\n"
        )
        return 2
    tool_name, remainder = _split_tool_reference_and_flags(args)
    if not tool_name:
        stdout.write(
            "usage: workflow tools call <tool|alias|entrypoint> [--input-json <json> | --input-file <path>] [--workflow-token <token>] [--yes] [--json]\n"
        )
        return 2
    definition, candidates = _resolve_tool_definition(tool_name)
    if definition is None:
        return _render_tool_lookup_failure(tool_name, candidates, stdout=stdout)

    input_json = None
    input_file = None
    workflow_token = ""
    confirmed = False
    i = 0
    while i < len(remainder):
        if remainder[i] == "--input-json" and i + 1 < len(remainder):
            input_json = remainder[i + 1]
            i += 2
        elif remainder[i] == "--input-file" and i + 1 < len(remainder):
            input_file = remainder[i + 1]
            i += 2
        elif remainder[i] == "--workflow-token" and i + 1 < len(remainder):
            workflow_token = remainder[i + 1]
            i += 2
        elif remainder[i] == "--yes":
            confirmed = True
            i += 1
        elif remainder[i] == "--json":
            i += 1
        else:
            stdout.write(f"unknown argument: {remainder[i]}\n")
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
