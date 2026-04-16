"""Praxis root authoring commands for schema, data shape, and page scaffolds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TextIO

from runtime.canonical_manifests import ManifestRuntimeBoundaryError, save_manifest
from runtime.object_lifecycle import ObjectLifecycleBoundaryError, upsert_object_type
from runtime.praxis_authoring import (
    plan_data_shape,
    scaffold_hierarchy,
    scaffold_object_type,
    scaffold_page,
    scaffold_primitive,
    scaffold_table,
    scaffold_view,
)
from surfaces.cli._db import cli_sync_conn
from surfaces.cli.commands.authority import _catalog_command, _object_command, _object_type_command, _registry_command, _schema_command
from surfaces.cli.mcp_tools import print_json


def _sync_conn():
    return cli_sync_conn()


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _load_json_object(*, spec_json: str | None, spec_file: str | None) -> dict[str, Any]:
    if spec_json and spec_file:
        raise ValueError("pass only one of --spec-json or --spec-file")
    if spec_file:
        payload = json.loads(Path(spec_file).read_text(encoding="utf-8"))
    elif spec_json:
        payload = json.loads(spec_json)
    else:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError("spec must be a JSON object")
    return dict(payload)


def _parse_field_token(token: str) -> dict[str, Any]:
    parts = [part.strip() for part in token.split(":") if part.strip()]
    if not parts:
        raise ValueError("field tokens must look like name:type[:required]")
    field_type = parts[1] if len(parts) >= 2 else "text"
    values: list[str] = []
    if field_type.startswith("enum(") and field_type.endswith(")"):
        values = [item.strip() for item in field_type[5:-1].split(",") if item.strip()]
        field_type = "enum"
    return {
        "name": parts[0],
        "type": field_type,
        "required": any(part.lower() == "required" for part in parts[2:]),
        "values": values,
    }


def _write_payload(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    Path(path).write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")


def _render_confirmation(*, stdout: TextIO) -> int:
    stdout.write("confirmation required: rerun with --apply --yes\n")
    return 2


def _scaffold_options(
    args: list[str],
    *,
    positional_key: str | None = None,
) -> tuple[dict[str, Any], bool, bool, str | None, list[str]]:
    spec_json: str | None = None
    spec_file: str | None = None
    write_path: str | None = None
    apply = False
    confirmed = False
    positional: list[str] = []
    field_tokens: list[str] = []
    metrics: list[str] = []
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--spec-json" and i + 1 < len(args):
            spec_json = args[i + 1]
            i += 2
            continue
        if token == "--spec-file" and i + 1 < len(args):
            spec_file = args[i + 1]
            i += 2
            continue
        if token == "--write" and i + 1 < len(args):
            write_path = args[i + 1]
            i += 2
            continue
        if token == "--field" and i + 1 < len(args):
            field_tokens.append(args[i + 1])
            i += 2
            continue
        if token == "--metric" and i + 1 < len(args):
            metrics.append(args[i + 1])
            i += 2
            continue
        if token == "--apply":
            apply = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        positional.append(token)
        i += 1

    spec = _load_json_object(spec_json=spec_json, spec_file=spec_file)
    if positional_key and positional and positional_key not in spec:
        spec[positional_key] = " ".join(positional)
    if field_tokens and "fields" not in spec:
        spec["fields"] = [_parse_field_token(token) for token in field_tokens]
    if metrics and "metrics" not in spec:
        spec["metrics"] = metrics
    return spec, apply, confirmed, write_path, positional


def _db_help_text() -> str:
    return "\n".join(
        [
            "usage: praxis db <status|plan|apply|describe|primitive|table|view> [args]",
            "",
            "Schema authority:",
            "  praxis db status|plan|apply|describe ...        Delegate to workflow schema authority",
            "",
            "Scaffolds:",
            "  praxis db primitive scaffold --spec-json '{...}'",
            "  praxis db table scaffold --spec-json '{...}'",
            "  praxis db view scaffold --spec-json '{...}'",
            "",
            "Notes:",
            "  - scaffold commands generate SQL and migration names; they do not execute schema changes",
            "  - use `praxis db apply --yes` for canonical migration/bootstrap authority",
        ]
    )


def _object_type_scaffold_help_text() -> str:
    return "\n".join(
        [
            "usage: praxis object-type scaffold [name] [--field name:type[:required]] [--spec-json '{...}'] [--apply --yes]",
            "",
            "Examples:",
            "  praxis object-type scaffold customer --field customer_id:text:required --field name:text:required",
            "  praxis object-type scaffold --spec-file customer_type.json --apply --yes",
        ]
    )


def _page_help_text() -> str:
    return "\n".join(
        [
            "usage: praxis page scaffold [intent] [--metric LABEL] [--spec-json '{...}'] [--apply --yes]",
            "",
            "Examples:",
            "  praxis page scaffold 'customer health dashboard'",
            "  praxis page scaffold --spec-file page.json --apply --yes",
        ]
    )


def _data_help_text() -> str:
    return "\n".join(
        [
            "usage: praxis data <shape ... | workflow-data-action>",
            "",
            "Authoring:",
            "  praxis data shape plan --spec-json '{...}'",
            "",
            "Fallback:",
            "  praxis data <existing workflow data args>",
        ]
    )


def _hierarchy_help_text() -> str:
    return "usage: praxis hierarchy scaffold <parent_type_id> <child_type_id> [--spec-json '{...}']"


def _handle_scaffold_result(
    payload: dict[str, Any],
    *,
    write_path: str | None,
    stdout: TextIO,
) -> int:
    _write_payload(write_path, payload)
    print_json(stdout, payload)
    return 0


def _primitive_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: praxis db primitive scaffold [name] [--field name:type[:required]] [--spec-json '{...}']\n")
        return 2
    if args[0] != "scaffold":
        stdout.write(f"unknown db primitive action: {args[0]}\n")
        return 2
    spec, _apply, _confirmed, write_path, _positional = _scaffold_options(args[1:], positional_key="primitive_type")
    payload = scaffold_primitive(spec)
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _table_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: praxis db table scaffold [name] [--field name:type[:required]] [--spec-json '{...}']\n")
        return 2
    if args[0] != "scaffold":
        stdout.write(f"unknown db table action: {args[0]}\n")
        return 2
    spec, _apply, _confirmed, write_path, _positional = _scaffold_options(args[1:], positional_key="table_name")
    payload = scaffold_table(spec)
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _view_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: praxis db view scaffold [name] [--spec-json '{...}']\n")
        return 2
    if args[0] != "scaffold":
        stdout.write(f"unknown db view action: {args[0]}\n")
        return 2
    spec, _apply, _confirmed, write_path, _positional = _scaffold_options(args[1:], positional_key="view_name")
    payload = scaffold_view(spec)
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _db_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_db_help_text() + "\n")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    if args[0] in {"status", "plan", "apply", "describe"}:
        return _schema_command(args, stdout=stdout)
    if args[0] == "primitive":
        return _primitive_command(args[1:], stdout=stdout)
    if args[0] == "table":
        return _table_command(args[1:], stdout=stdout)
    if args[0] == "view":
        return _view_command(args[1:], stdout=stdout)
    stdout.write(f"unknown db action: {args[0]}\n")
    return 2


def _data_shape_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write("usage: praxis data shape plan [target_name] [--spec-json '{...}'] [--write path]\n")
        return 2
    if args[0] != "plan":
        stdout.write(f"unknown data shape action: {args[0]}\n")
        return 2
    spec, _apply, _confirmed, write_path, _positional = _scaffold_options(args[1:], positional_key="target_name")
    payload = plan_data_shape(spec)
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _data_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_data_help_text() + "\n")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    if args[0] == "shape":
        return _data_shape_command(args[1:], stdout=stdout)
    from surfaces.cli.commands.data import _data_command as _workflow_data_command

    return _workflow_data_command(args, stdout=stdout)


def _object_type_scaffold_command(args: list[str], *, stdout: TextIO) -> int:
    if args and args[0] in {"-h", "--help"}:
        stdout.write(_object_type_scaffold_help_text() + "\n")
        return 0
    spec, apply, confirmed, write_path, _positional = _scaffold_options(args, positional_key="name")
    payload = scaffold_object_type(spec)
    if apply and not confirmed:
        return _render_confirmation(stdout=stdout)
    if apply:
        try:
            record = upsert_object_type(_sync_conn(), **payload["object_type"])
        except ObjectLifecycleBoundaryError as exc:
            stdout.write(str(exc) + "\n")
            return exc.status_code if exc.status_code >= 400 else 1
        payload["saved_object_type"] = record
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _hierarchy_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_hierarchy_help_text() + "\n")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    if args[0] != "scaffold":
        stdout.write(f"unknown hierarchy action: {args[0]}\n")
        return 2
    spec, _apply, _confirmed, write_path, positional = _scaffold_options(args[1:])
    if "parent_type_id" not in spec and positional:
        spec["parent_type_id"] = positional[0]
    if "child_type_id" not in spec and len(positional) >= 2:
        spec["child_type_id"] = positional[1]
    payload = scaffold_hierarchy(spec)
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _page_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_page_help_text() + "\n")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    if args[0] != "scaffold":
        stdout.write(f"unknown page action: {args[0]}\n")
        return 2
    spec, apply, confirmed, write_path, _positional = _scaffold_options(args[1:], positional_key="intent")
    if "title" not in spec and "intent" in spec:
        spec["title"] = spec["intent"]
    payload = scaffold_page(spec)
    if apply and not confirmed:
        return _render_confirmation(stdout=stdout)
    if apply:
        conn = _sync_conn()
        try:
            saved_types = [
                upsert_object_type(conn, **object_type)
                for object_type in payload.get("object_types", [])
                if isinstance(object_type, dict)
            ]
            saved_manifest = save_manifest(
                conn,
                manifest_id=payload["manifest_id"],
                name=payload["name"],
                description=payload["description"],
                manifest=payload["manifest"],
            )
        except (ObjectLifecycleBoundaryError, ManifestRuntimeBoundaryError) as exc:
            stdout.write(str(exc) + "\n")
            return getattr(exc, "status_code", 1)
        payload["saved_object_types"] = saved_types
        payload["saved_manifest"] = saved_manifest
    return _handle_scaffold_result(payload, write_path=write_path, stdout=stdout)


def _registry_command_passthrough(args: list[str], *, stdout: TextIO) -> int:
    return _registry_command(args, stdout=stdout)


def _catalog_command_passthrough(args: list[str], *, stdout: TextIO) -> int:
    return _catalog_command(args, stdout=stdout)


def _object_command_passthrough(args: list[str], *, stdout: TextIO) -> int:
    return _object_command(args, stdout=stdout)


def _object_type_command_passthrough(args: list[str], *, stdout: TextIO) -> int:
    if args and args[0] == "scaffold":
        return _object_type_scaffold_command(args[1:], stdout=stdout)
    return _object_type_command(args, stdout=stdout)


__all__ = [
    "_catalog_command_passthrough",
    "_data_command",
    "_db_command",
    "_hierarchy_command",
    "_object_command_passthrough",
    "_object_type_command_passthrough",
    "_page_command",
    "_registry_command_passthrough",
]
