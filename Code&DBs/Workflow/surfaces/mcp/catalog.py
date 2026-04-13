"""Shared MCP tool catalog authority.

The literal ``TOOLS`` declarations inside ``surfaces/mcp/tools/*.py`` are the
one source of truth for MCP tool metadata. This module reads that metadata
without importing the tool modules, so other startup paths can project the
catalog into Postgres without accidentally constructing the live MCP surface.
"""

from __future__ import annotations

import ast
import importlib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

_TOOLS_ROOT = Path(__file__).resolve().parent / "tools"
_MCP_SERVER_ID = "praxis-workflow-mcp"


@dataclass(frozen=True, slots=True)
class McpToolDefinition:
    """Parsed MCP tool definition from one ``TOOLS`` literal entry."""

    name: str
    module_name: str
    handler_name: str
    metadata: dict[str, Any]
    selector_defaults: dict[str, object]

    @property
    def description(self) -> str:
        return str(self.metadata.get("description") or "")

    @property
    def input_schema(self) -> dict[str, Any]:
        schema = self.metadata.get("inputSchema")
        return dict(schema) if isinstance(schema, dict) else {}

    def _selector_enum(self, field_name: str) -> tuple[str, ...]:
        properties = self.input_schema.get("properties")
        if not isinstance(properties, dict):
            return ()
        selector_prop = properties.get(field_name)
        if not isinstance(selector_prop, dict):
            return ()
        raw_enum = selector_prop.get("enum")
        if not isinstance(raw_enum, list):
            return ()
        return tuple(
            _slugify_action(value)
            for value in raw_enum
            if _slugify_action(value)
        )

    @property
    def action_enum(self) -> tuple[str, ...]:
        return self._selector_enum("action")

    @property
    def view_enum(self) -> tuple[str, ...]:
        return self._selector_enum("view")

    @property
    def selector_field(self) -> str | None:
        if self.action_enum:
            return "action"
        if self.view_enum:
            return "view"
        return None

    @property
    def selector_enum(self) -> tuple[str, ...]:
        field_name = self.selector_field
        if field_name is None:
            return ()
        return self._selector_enum(field_name)

    @property
    def selector_default(self) -> str | None:
        field_name = self.selector_field
        if field_name is None:
            return None
        properties = self.input_schema.get("properties")
        if isinstance(properties, dict):
            selector_prop = properties.get(field_name)
            if isinstance(selector_prop, dict):
                default = _slugify_action(selector_prop.get("default"))
                if default:
                    return default
        default = _slugify_action(self.selector_defaults.get(field_name))
        return default or None

    @property
    def selector_defaults_to_empty(self) -> bool:
        field_name = self.selector_field
        if field_name is None:
            return False
        return self.selector_defaults.get(field_name) == ""

    @property
    def default_action(self) -> str:
        selector_default = self.selector_default
        if selector_default:
            return selector_default
        if self.selector_defaults_to_empty:
            derived = _strip_tool_prefix(self.name)
            return _slugify_action(derived) or "call"
        selector_enum = self.selector_enum
        if selector_enum:
            return selector_enum[0]
        derived = _strip_tool_prefix(self.name)
        return _slugify_action(derived) or "call"

    @property
    def required_args(self) -> tuple[str, ...]:
        raw_required = self.input_schema.get("required")
        if not isinstance(raw_required, list):
            return ()
        return tuple(
            str(value).strip()
            for value in raw_required
            if str(value).strip() and str(value).strip() != "action"
        )

    @property
    def inputs(self) -> tuple[str, ...]:
        properties = self.input_schema.get("properties")
        if not isinstance(properties, dict):
            return ()
        return tuple(name for name in properties if name != "action")

    @property
    def display_name(self) -> str:
        raw = _strip_tool_prefix(self.name)
        parts = [part for part in raw.split("_") if part]
        if not parts:
            return "Praxis Tool"
        return "Praxis " + " ".join(part.capitalize() for part in parts)

    @property
    def supports_action_argument(self) -> bool:
        return self.selector_field == "action"

    @property
    def supports_view_argument(self) -> bool:
        return self.selector_field == "view"

    def capability_rows(self) -> list[dict[str, Any]]:
        actions = list(self.selector_enum or ())
        default_action = self.default_action
        if default_action and default_action not in actions:
            actions.insert(0, default_action)
        if not actions:
            actions = [default_action]
        description = self.description
        inputs = list(self.inputs)
        required_args = list(self.required_args)
        return [
            {
                "action": action,
                "description": description,
                "inputs": inputs,
                "requiredArgs": required_args,
                **(
                    {"selectorField": self.selector_field}
                    if self.selector_field and self.selector_field != "action"
                    else {}
                ),
            }
            for action in actions
        ]

    def integration_row(self) -> dict[str, Any]:
        return {
            "id": self.name,
            "name": self.display_name,
            "description": self.description,
            "provider": "mcp",
            "capabilities": self.capability_rows(),
            "auth_status": "connected",
            "icon": "tool",
            "mcp_server_id": _MCP_SERVER_ID,
        }


def _slugify_action(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return text.replace(" ", "_").replace("-", "_")


def canonical_tool_name(tool_name: object) -> str:
    """Return the canonical Praxis tool id for a tool name."""

    return str(tool_name or "").strip()


def _strip_tool_prefix(tool_name: object) -> str:
    text = canonical_tool_name(tool_name)
    if text.startswith("praxis_"):
        return text[7:]
    return text


def _tool_paths() -> list[Path]:
    return sorted(
        path for path in _TOOLS_ROOT.glob("*.py")
        if not path.name.startswith("_") and path.name != "__init__.py"
    )


def _load_tool_defs(path: Path) -> list[McpToolDefinition]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    tools_node: ast.AST | None = None
    function_defaults: dict[str, dict[str, object]] = {}

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            function_defaults[node.name] = _selector_defaults_for_handler(node)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TOOLS":
                    tools_node = node.value
                    break
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "TOOLS":
                tools_node = node.value
        if tools_node is not None:
            break

    if tools_node is None:
        return []
    if not isinstance(tools_node, ast.Dict):
        raise ValueError(f"TOOLS in {path} must be a dict literal")

    module_name = f"surfaces.mcp.tools.{path.stem}"
    definitions: list[McpToolDefinition] = []
    for key_node, value_node in zip(tools_node.keys, tools_node.values, strict=False):
        tool_name = ast.literal_eval(key_node)
        if not isinstance(tool_name, str) or not tool_name.strip():
            raise ValueError(f"Invalid tool name in {path}")
        if not isinstance(value_node, ast.Tuple) or len(value_node.elts) != 2:
            raise ValueError(f"Tool entry {tool_name} in {path} must be a 2-tuple")
        handler_node, metadata_node = value_node.elts
        if not isinstance(handler_node, ast.Name):
            raise ValueError(f"Tool handler for {tool_name} in {path} must be a named function")
        metadata = ast.literal_eval(metadata_node)
        if not isinstance(metadata, dict):
            raise ValueError(f"Tool metadata for {tool_name} in {path} must be a dict literal")
        definitions.append(
            McpToolDefinition(
                name=tool_name,
                module_name=module_name,
                handler_name=handler_node.id,
                metadata=dict(metadata),
                selector_defaults=function_defaults.get(handler_node.id, {}),
            )
        )
    return definitions


def _rewrite_tool_text(value: Any, alias_map: dict[str, str]) -> Any:
    if isinstance(value, str):
        rewritten = value
        for source, target in sorted(alias_map.items(), key=lambda item: (-len(item[0]), item[0])):
            rewritten = rewritten.replace(source, target)
        return rewritten
    if isinstance(value, dict):
        return {key: _rewrite_tool_text(item, alias_map) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite_tool_text(item, alias_map) for item in value]
    if isinstance(value, tuple):
        return tuple(_rewrite_tool_text(item, alias_map) for item in value)
    return value


def _selector_defaults_for_handler(node: ast.FunctionDef) -> dict[str, object]:
    defaults: dict[str, object] = {}
    for statement in node.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            continue
        target_name = statement.targets[0].id
        if target_name not in {"action", "view"}:
            continue
        call = statement.value
        if not isinstance(call, ast.Call):
            continue
        if not isinstance(call.func, ast.Attribute) or call.func.attr != "get":
            continue
        if not isinstance(call.func.value, ast.Name) or call.func.value.id != "params":
            continue
        if not call.args:
            continue
        selector_name = ast.literal_eval(call.args[0])
        if selector_name != target_name:
            continue
        if len(call.args) < 2:
            continue
        try:
            default_value = ast.literal_eval(call.args[1])
        except Exception:
            continue
        if isinstance(default_value, str):
            defaults[target_name] = default_value
    return defaults


@lru_cache(maxsize=1)
def get_tool_catalog() -> dict[str, McpToolDefinition]:
    """Return the parsed MCP tool catalog keyed by tool name."""

    raw_catalog: dict[str, McpToolDefinition] = {}
    for path in _tool_paths():
        for definition in _load_tool_defs(path):
            if definition.name in raw_catalog:
                raise ValueError(
                    f"Duplicate tool name '{definition.name}' in {definition.module_name}",
                )
            raw_catalog[definition.name] = definition

    alias_map = {
        name: canonical_tool_name(name)
        for name in raw_catalog
        if canonical_tool_name(name) != name
    }
    catalog: dict[str, McpToolDefinition] = {}
    for name, definition in raw_catalog.items():
        canonical_name = canonical_tool_name(name)
        metadata = _rewrite_tool_text(definition.metadata, alias_map)
        canonical_definition = McpToolDefinition(
            name=canonical_name,
            module_name=definition.module_name,
            handler_name=definition.handler_name,
            metadata=dict(metadata) if isinstance(metadata, dict) else dict(definition.metadata),
            selector_defaults=definition.selector_defaults,
        )
        if canonical_name in catalog:
            raise ValueError(
                f"Duplicate canonical tool name '{canonical_name}' in {definition.module_name}",
            )
        catalog[canonical_name] = canonical_definition
    return catalog


@lru_cache(maxsize=128)
def resolve_tool_entry(tool_name: str) -> tuple[Callable[[dict], Any], dict[str, Any]]:
    """Import one tool module lazily and resolve its handler/metadata pair."""

    definition = get_tool_catalog().get(canonical_tool_name(tool_name))
    if definition is None:
        raise KeyError(tool_name)
    module = importlib.import_module(definition.module_name)
    handler = getattr(module, definition.handler_name)
    return handler, dict(definition.metadata)


def projected_mcp_integrations() -> list[dict[str, Any]]:
    """Return one integration-registry row per MCP tool."""

    return [
        definition.integration_row()
        for _, definition in sorted(get_tool_catalog().items())
    ]


__all__ = [
    "McpToolDefinition",
    "get_tool_catalog",
    "projected_mcp_integrations",
    "resolve_tool_entry",
]
