"""Generated documentation helpers for the Praxis MCP catalog."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable

from .catalog import McpToolDefinition, get_tool_catalog


def render_mcp_markdown() -> str:
    catalog = get_tool_catalog()
    definitions = sorted(catalog.values(), key=lambda item: (item.cli_surface, item.name))
    grouped: dict[str, list[McpToolDefinition]] = defaultdict(list)
    for definition in definitions:
        grouped[definition.cli_surface].append(definition)

    lines: list[str] = [
        "# Praxis MCP Tools",
        "",
        (
            f"Praxis exposes {len(definitions)} catalog-backed tools via the "
            "[Model Context Protocol](https://modelcontextprotocol.io/)."
        ),
        "",
        "CLI discovery is generated from the same catalog metadata:",
        "",
        "- `workflow tools list`",
        "- `workflow tools search <text>`",
        "- `workflow tools describe <tool|alias>`",
        "- `workflow tools call <tool|alias> --input-json '{...}'`",
        "",
        "## Catalog Summary",
        "",
        "| Tool | Surface | Tier | Alias | Risks | Description |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for definition in definitions:
        alias = f"`workflow {definition.cli_recommended_alias}`" if definition.cli_recommended_alias else "-"
        risks = ", ".join(f"`{risk}`" for risk in definition.risk_levels)
        description = definition.description.split("\n", 1)[0]
        lines.append(
            f"| `{definition.name}` | `{definition.cli_surface}` | `{definition.cli_tier}` | {alias} | {risks} | {description} |"
        )

    lines.extend(["", "## Tool Reference", ""])
    for surface in sorted(grouped):
        lines.append(f"### {surface.title()}")
        lines.append("")
        for definition in grouped[surface]:
            lines.extend(_tool_section(definition))

    return "\n".join(lines).rstrip() + "\n"


def _tool_section(definition: McpToolDefinition) -> list[str]:
    lines = [f"#### `{definition.name}`", ""]
    lines.append(f"- Surface: `{definition.cli_surface}`")
    lines.append(f"- Tier: `{definition.cli_tier}`")
    lines.append(f"- Badges: {', '.join(f'`{badge}`' for badge in definition.cli_badges)}")
    lines.append(f"- Risks: {', '.join(f'`{risk}`' for risk in definition.risk_levels)}")
    lines.append(f"- CLI entrypoint: `{definition.cli_entrypoint}`")
    lines.append(f"- CLI schema help: `{definition.cli_describe_command}`")
    if definition.cli_when_to_use:
        lines.append(f"- When to use: {definition.cli_when_to_use}")
    if definition.cli_when_not_to_use:
        lines.append(f"- When not to use: {definition.cli_when_not_to_use}")
    if definition.cli_recommended_alias:
        lines.append(f"- Recommended alias: `workflow {definition.cli_recommended_alias}`")
    if definition.selector_field:
        lines.append(
            f"- Selector: `{definition.selector_field}`; default `{definition.selector_default or definition.default_action}`; values {_render_code_list(definition.selector_enum)}"
        )
    else:
        lines.append("- Selector: none")
    lines.append(f"- Required args: {_render_code_list(definition.required_args) if definition.required_args else '(none)'}")
    lines.append("")
    lines.append("Example input:")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(definition.example_input(), indent=2, default=str))
    lines.append("```")
    lines.append("")
    return lines


def _render_code_list(values: Iterable[str]) -> str:
    items = [str(value) for value in values if str(value)]
    return ", ".join(f"`{item}`" for item in items)


__all__ = ["render_mcp_markdown"]
