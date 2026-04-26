"""Generate operator-facing surface docs from live catalog metadata."""

from __future__ import annotations

import sys
from pathlib import Path


WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = WORKFLOW_ROOT.parents[1]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from surfaces.api.rest import list_api_routes
from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog
from surfaces.mcp.docs import render_mcp_markdown


def _render_code_list(values: list[str] | tuple[str, ...]) -> str:
    return ", ".join(f"`{value}`" for value in values if value) or "-"


def _sorted_tools() -> list[McpToolDefinition]:
    return sorted(
        get_tool_catalog().values(),
        key=lambda item: (item.cli_surface, item.cli_tier, item.cli_entrypoint, item.name),
    )


def render_cli_markdown() -> str:
    definitions = _sorted_tools()
    lines: list[str] = [
        "# Praxis CLI Surface",
        "",
        "The authoritative operator front door is `praxis workflow`.",
        "",
        "This file is generated from the MCP/catalog metadata used by `workflow tools`.",
        "If it disagrees with runtime output, trust the runtime and regenerate this file.",
        "",
        "## Discovery Commands",
        "",
        "- `praxis workflow tools list`",
        "- `praxis workflow tools search <text> [--exact]`",
        "- `praxis workflow tools describe <tool|alias|entrypoint>`",
        "- `praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}'`",
        "- `praxis workflow routes --json` for the live HTTP API route catalog",
        "",
        "## Stable Aliases",
        "",
        "| Command | Tool | Surface | Risk | When To Use |",
        "| --- | --- | --- | --- | --- |",
    ]
    for definition in definitions:
        if not definition.cli_recommended_alias:
            continue
        risks = _render_code_list(definition.risk_levels)
        lines.append(
            "| "
            f"`praxis {definition.cli_entrypoint}` | "
            f"`{definition.name}` | "
            f"`{definition.cli_surface}` | "
            f"{risks} | "
            f"{definition.cli_when_to_use or definition.description.splitlines()[0]} |"
        )

    lines.extend(["", "## Full Catalog Entrypoints", ""])
    current_surface = ""
    for definition in definitions:
        if definition.cli_surface != current_surface:
            if current_surface:
                lines.append("")
            current_surface = definition.cli_surface
            lines.extend([f"### {current_surface.title()}", ""])
            lines.append("| Entrypoint | Tool | Tier | Selector | Risks |")
            lines.append("| --- | --- | --- | --- | --- |")
        selector = definition.selector_field or "-"
        if definition.selector_field and definition.selector_enum:
            selector = f"{definition.selector_field}: {', '.join(definition.selector_enum)}"
        lines.append(
            "| "
            f"`praxis {definition.cli_entrypoint}` | "
            f"`{definition.name}` | "
            f"`{definition.cli_tier}` | "
            f"{selector} | "
            f"{_render_code_list(definition.risk_levels)} |"
        )

    return "\n".join(lines).rstrip() + "\n"


def render_api_markdown() -> str:
    public_routes = list_api_routes(visibility="public")
    all_routes = list_api_routes(visibility="all")
    lines: list[str] = [
        "# Praxis API Surface",
        "",
        "The HTTP API is a client surface over Praxis runtime authority.",
        "",
        "This file is generated from the live FastAPI route catalog exposed by `GET /api/routes`.",
        "If it disagrees with runtime output, trust `praxis workflow routes --json` and regenerate this file.",
        "",
        "## Discovery Commands",
        "",
        "- `praxis workflow routes --json`",
        "- `praxis workflow api routes --search <text> --method GET --tag <tag>`",
        "- `GET /api/routes`",
        "- `GET /api/routes?visibility=all` for internal and public routes",
        f"- Interactive docs: `{public_routes.get('docs_url') or '/docs'}`",
        f"- OpenAPI JSON: `{public_routes.get('openapi_url') or '/openapi.json'}`",
        f"- ReDoc: `{public_routes.get('redoc_url') or '/redoc'}`",
        "",
        "## Public Routes",
        "",
    ]
    lines.extend(_route_table(public_routes.get("routes", [])))
    lines.extend(["", "## All Routes", ""])
    lines.append(
        f"Public route count: `{public_routes.get('count')}`. "
        f"All route count: `{all_routes.get('count')}`."
    )
    lines.append("")
    lines.extend(_route_table(all_routes.get("routes", [])))
    return "\n".join(lines).rstrip() + "\n"


def _route_table(routes: object) -> list[str]:
    if not isinstance(routes, list) or not routes:
        return ["No routes matched."]
    lines = [
        "| Methods | Path | Visibility | Tags | Summary |",
        "| --- | --- | --- | --- | --- |",
    ]
    for route in routes:
        if not isinstance(route, dict):
            continue
        methods = ", ".join(str(method) for method in route.get("methods", []))
        tags = ", ".join(f"`{tag}`" for tag in route.get("tags", []) if str(tag))
        summary = str(route.get("summary") or route.get("description") or route.get("name") or "")
        lines.append(
            "| "
            f"`{methods}` | "
            f"`{route.get('path')}` | "
            f"`{route.get('visibility')}` | "
            f"{tags or '-'} | "
            f"{summary.splitlines()[0] if summary else '-'} |"
        )
    return lines


def main() -> int:
    outputs = {
        REPO_ROOT / "docs" / "MCP.md": render_mcp_markdown(),
        REPO_ROOT / "docs" / "CLI.md": render_cli_markdown(),
        REPO_ROOT / "docs" / "API.md": render_api_markdown(),
    }
    for output_path, content in outputs.items():
        output_path.write_text(content, encoding="utf-8")
        print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
