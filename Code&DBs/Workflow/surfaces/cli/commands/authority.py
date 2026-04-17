"""Direct CLI front doors for schema, registry, object, and catalog authority."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, TextIO

from runtime import operation_catalog_gateway
from runtime.app_manifest_registry import (
    AppManifestRegistryBoundaryError,
    get_app_manifest,
    list_app_manifests,
    retire_app_manifest,
    upsert_registry_manifest,
)
from runtime.object_lifecycle import (
    ObjectLifecycleBoundaryError,
    create_object,
    delete_object,
    get_object,
    list_objects,
    update_object,
)
from runtime.surface_catalog import (
    SurfaceCatalogBoundaryError,
    get_surface_catalog_item,
    list_surface_catalog_items,
    retire_surface_catalog_item,
    upsert_surface_catalog_item,
)
from storage.migrations import workflow_migration_expected_objects
from storage.postgres.connection import connect_workflow_database
from storage.postgres.schema import (
    bootstrap_control_plane_schema,
    bootstrap_workflow_schema,
    inspect_control_plane_schema,
    inspect_workflow_schema,
)
from storage.postgres.validators import PostgresConfigurationError, PostgresStorageError
from surfaces.cli._db import cli_sync_conn
from surfaces.cli.mcp_tools import load_json_file, print_json
from surfaces.mcp.tools.health import tool_praxis_reload

from .data import _data_command


def _sync_conn():
    return cli_sync_conn()


def _operation_env() -> dict[str, str]:
    return dict(os.environ)


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _load_json_input(*, file_path: str | None, inline_json: str | None, field_name: str) -> dict[str, Any]:
    if file_path and inline_json:
        raise ValueError(f"pass only one of --{field_name}-file or --{field_name}-json")
    if file_path:
        payload = load_json_file(file_path)
    elif inline_json:
        payload = json.loads(inline_json)
    else:
        raise ValueError(f"one of --{field_name}-file or --{field_name}-json is required")
    if not isinstance(payload, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(payload)


def _load_json_value(*, file_path: str | None, inline_json: str | None, field_name: str) -> Any:
    if file_path and inline_json:
        raise ValueError(f"pass only one of --{field_name}-file or --{field_name}-json")
    if file_path:
        return json.loads(Path(file_path).read_text(encoding="utf-8"))
    if inline_json:
        return json.loads(inline_json)
    raise ValueError(f"one of --{field_name}-file or --{field_name}-json is required")


def _render_confirmation(*, stdout: TextIO) -> int:
    stdout.write("confirmation required: rerun with --yes\n")
    return 2


def _schema_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow schema <status|plan|apply|describe> [args]",
            "",
            "Schema authority:",
            "  workflow schema status [--scope workflow|control] [--json]",
            "  workflow schema plan [--scope workflow|control] [--json]",
            "  workflow schema apply [--scope workflow|control] [--yes] [--json]",
            "  workflow schema describe <object-name|migration.sql> [--scope workflow|control] [--json]",
        ]
    )


async def _schema_status_payload(*, scope: str) -> dict[str, Any]:
    conn = await connect_workflow_database()
    try:
        if scope == "control":
            readiness = await inspect_control_plane_schema(conn)
            return {
                "scope": scope,
                "bootstrapped": readiness.is_bootstrapped,
                "expected_count": len(readiness.expected_objects),
                "missing_objects": [
                    {"object_type": item.object_type, "object_name": item.object_name}
                    for item in readiness.missing_objects
                ],
            }
        readiness = await inspect_workflow_schema(conn)
        return {
            "scope": scope,
            "bootstrapped": readiness.is_bootstrapped,
            "expected_count": len(readiness.expected_objects),
            "missing_objects": [
                {"object_type": item.object_type, "object_name": item.object_name}
                for item in readiness.missing_objects
            ],
            "missing_by_migration": {
                filename: [
                    {"object_type": item.object_type, "object_name": item.object_name}
                    for item in objects
                ]
                for filename, objects in readiness.missing_by_migration.items()
            },
        }
    finally:
        await conn.close()


async def _schema_apply_payload(*, scope: str) -> dict[str, Any]:
    conn = await connect_workflow_database()
    try:
        if scope == "control":
            await bootstrap_control_plane_schema(conn)
        else:
            await bootstrap_workflow_schema(conn)
        return await _schema_status_payload(scope=scope)
    finally:
        await conn.close()


async def _schema_describe_payload(*, scope: str, target: str) -> dict[str, Any]:
    conn = await connect_workflow_database()
    try:
        if target.endswith(".sql"):
            expected = workflow_migration_expected_objects(target)
        else:
            if scope == "control":
                readiness = await inspect_control_plane_schema(conn)
            else:
                readiness = await inspect_workflow_schema(conn)
            expected = tuple(
                item
                for item in readiness.expected_objects
                if item.object_name == target or item.object_name.startswith(f"{target}.")
            )
        columns = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
            """,
            target,
        )
        indexes = await conn.fetch(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = $1
            ORDER BY indexname
            """,
            target,
        )
        constraints = await conn.fetch(
            """
            SELECT conname
            FROM pg_catalog.pg_constraint AS con
            JOIN pg_catalog.pg_class AS cls ON cls.oid = con.conrelid
            JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public' AND cls.relname = $1
            ORDER BY conname
            """,
            target,
        )
        return {
            "scope": scope,
            "target": target,
            "expected_objects": [
                {"object_type": item.object_type, "object_name": item.object_name}
                for item in expected
            ],
            "actual_columns": [dict(row) for row in columns],
            "actual_indexes": [dict(row) for row in indexes],
            "actual_constraints": [dict(row) for row in constraints],
        }
    finally:
        await conn.close()


def _schema_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_schema_help_text() + "\n")
        return 2
    action = args[0]
    scope = "workflow"
    as_json = False
    confirmed = False
    target = ""
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--scope" and i + 1 < len(args):
            scope = _text(args[i + 1]) or "workflow"
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if not target:
            target = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    if scope not in {"workflow", "control"}:
        stdout.write("--scope must be one of: workflow, control\n")
        return 2
    try:
        if action == "status":
            payload = asyncio.run(_schema_status_payload(scope=scope))
        elif action == "plan":
            payload = asyncio.run(_schema_status_payload(scope=scope))
            payload["pending_migrations"] = sorted(payload.get("missing_by_migration", {}).keys())
        elif action == "apply":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            payload = asyncio.run(_schema_apply_payload(scope=scope))
            payload["applied"] = True
        elif action == "describe":
            if not target:
                stdout.write("usage: workflow schema describe <object-name|migration.sql> [--scope workflow|control] [--json]\n")
                return 2
            payload = asyncio.run(_schema_describe_payload(scope=scope, target=target))
        else:
            stdout.write(_schema_help_text() + "\n")
            return 2
    except (PostgresConfigurationError, PostgresStorageError, ValueError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    stdout.write(
        f"scope={payload.get('scope')} bootstrapped={payload.get('bootstrapped', False)} "
        f"expected={payload.get('expected_count', 0)} missing={len(payload.get('missing_objects') or [])}\n"
    )
    if payload.get("pending_migrations"):
        stdout.write("pending_migrations:\n")
        for filename in payload["pending_migrations"]:
            stdout.write(f"  {filename}\n")
    if action == "describe":
        stdout.write(
            f"columns={len(payload.get('actual_columns') or [])} "
            f"indexes={len(payload.get('actual_indexes') or [])} "
            f"constraints={len(payload.get('actual_constraints') or [])}\n"
        )
    return 0


def _registry_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow registry <list|get|upsert|retire> [args]",
            "",
            "Registry authority:",
            "  workflow registry list [--status STATUS] [--manifest-kind KIND] [--manifest-family FAMILY] [--manifest-type TYPE] [--query TEXT] [--limit N] [--json]",
            "  workflow registry get <manifest-id> [--json]",
            "  workflow registry upsert --id ID --manifest-file <path>|--manifest-json '<json>' [--name NAME] [--description TEXT] [--status STATUS] [--parent-manifest-id ID] [--changed-by NAME] [--change-description TEXT] [--yes] [--json]",
            "  workflow registry retire <manifest-id> [--changed-by NAME] [--change-description TEXT] [--yes] [--json]",
        ]
    )


def _registry_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_registry_help_text() + "\n")
        return 2
    action = args[0]
    as_json = False
    confirmed = False
    manifest_id = ""
    name = None
    description = None
    status = None
    manifest_kind = None
    manifest_family = None
    manifest_type = None
    query = None
    limit = 20
    parent_manifest_id = None
    changed_by = "workflow_registry"
    change_description = None
    manifest_file = None
    manifest_json = None
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if token == "--id" and i + 1 < len(args):
            manifest_id = args[i + 1]
            i += 2
            continue
        if token == "--name" and i + 1 < len(args):
            name = args[i + 1]
            i += 2
            continue
        if token == "--description" and i + 1 < len(args):
            description = args[i + 1]
            i += 2
            continue
        if token == "--status" and i + 1 < len(args):
            status = args[i + 1]
            i += 2
            continue
        if token == "--manifest-kind" and i + 1 < len(args):
            manifest_kind = args[i + 1]
            i += 2
            continue
        if token == "--manifest-family" and i + 1 < len(args):
            manifest_family = args[i + 1]
            i += 2
            continue
        if token == "--manifest-type" and i + 1 < len(args):
            manifest_type = args[i + 1]
            i += 2
            continue
        if token == "--query" and i + 1 < len(args):
            query = args[i + 1]
            i += 2
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        if token == "--parent-manifest-id" and i + 1 < len(args):
            parent_manifest_id = args[i + 1]
            i += 2
            continue
        if token == "--changed-by" and i + 1 < len(args):
            changed_by = args[i + 1]
            i += 2
            continue
        if token == "--change-description" and i + 1 < len(args):
            change_description = args[i + 1]
            i += 2
            continue
        if token == "--manifest-file" and i + 1 < len(args):
            manifest_file = args[i + 1]
            i += 2
            continue
        if token == "--manifest-json" and i + 1 < len(args):
            manifest_json = args[i + 1]
            i += 2
            continue
        if not manifest_id:
            manifest_id = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    try:
        if action == "list":
            conn = _sync_conn()
            payload = {
                "manifests": list_app_manifests(
                    conn,
                    status=status,
                    manifest_kind=manifest_kind,
                    manifest_family=manifest_family,
                    manifest_type=manifest_type,
                    query=query,
                    limit=limit,
                )
            }
        elif action == "get":
            conn = _sync_conn()
            payload = {"manifest": get_app_manifest(conn, manifest_id=manifest_id)}
        elif action == "upsert":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            payload = {
                "manifest": upsert_registry_manifest(
                    conn,
                    manifest_id=manifest_id,
                    manifest=_load_json_input(
                        file_path=manifest_file,
                        inline_json=manifest_json,
                        field_name="manifest",
                    ),
                    name=name,
                    description=description,
                    status=status,
                    parent_manifest_id=parent_manifest_id,
                    changed_by=changed_by,
                    change_description=change_description or "Updated registry manifest",
                )
            }
        elif action == "retire":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            payload = {
                "manifest": retire_app_manifest(
                    conn,
                    manifest_id=manifest_id,
                    changed_by=changed_by,
                    change_description=change_description or "Retired registry manifest",
                )
            }
        else:
            stdout.write(_registry_help_text() + "\n")
            return 2
    except (AppManifestRegistryBoundaryError, ValueError, json.JSONDecodeError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "list":
        manifests = payload["manifests"]
        stdout.write(f"{len(manifests)} manifest(s)\n")
        for item in manifests:
            stdout.write(
                f"  {item['id']}  status={item['status']} kind={item.get('kind') or '-'} "
                f"type={item.get('manifest_type') or '-'}\n"
            )
        return 0
    manifest = payload["manifest"]
    stdout.write(
        f"{manifest['id']} status={manifest['status']} version={manifest['version']} "
        f"kind={manifest.get('kind') or '-'} type={manifest.get('manifest_type') or '-'}\n"
    )
    return 0


def _object_type_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow object-type <list|get|upsert> [args]",
            "",
            "Object type authority:",
            "  workflow object-type list [--query TEXT] [--limit N] [--json]",
            "  workflow object-type get <type-id> [--json]",
            "  workflow object-type upsert [--type-id ID] --name NAME [--description TEXT] [--icon TEXT] [--fields-file <path>|--fields-json '<json>'] [--yes] [--json]",
        ]
    )


def _object_type_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_object_type_help_text() + "\n")
        return 2
    action = args[0]
    query = ""
    limit = 100
    as_json = False
    confirmed = False
    type_id = ""
    name = ""
    description = ""
    icon = ""
    fields_file = None
    fields_json = None
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--query" and i + 1 < len(args):
            query = args[i + 1]
            i += 2
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if token == "--type-id" and i + 1 < len(args):
            type_id = args[i + 1]
            i += 2
            continue
        if token == "--name" and i + 1 < len(args):
            name = args[i + 1]
            i += 2
            continue
        if token == "--description" and i + 1 < len(args):
            description = args[i + 1]
            i += 2
            continue
        if token == "--icon" and i + 1 < len(args):
            icon = args[i + 1]
            i += 2
            continue
        if token == "--fields-file" and i + 1 < len(args):
            fields_file = args[i + 1]
            i += 2
            continue
        if token == "--fields-json" and i + 1 < len(args):
            fields_json = args[i + 1]
            i += 2
            continue
        if action == "get" and not type_id:
            type_id = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    try:
        if action == "list":
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name="object_schema.type_list",
                payload={"q": query, "limit": limit},
            )
        elif action == "get":
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name="object_schema.type_get",
                payload={"type_id": type_id},
            )
        elif action == "upsert":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            fields = (
                _load_json_value(
                    file_path=fields_file,
                    inline_json=fields_json,
                    field_name="fields",
                )
                if fields_file or fields_json
                else []
            )
            operation_name = "object_schema.type_upsert_by_id" if type_id else "object_schema.type_upsert"
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name=operation_name,
                payload={
                    "type_id": type_id or None,
                    "name": name,
                    "description": description,
                    "icon": icon,
                    "fields": fields,
                },
            )
        else:
            stdout.write(_object_type_help_text() + "\n")
            return 2
    except (ObjectLifecycleBoundaryError, ValueError, json.JSONDecodeError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "list":
        stdout.write(f"{payload['count']} object type(s)\n")
        for item in payload["types"]:
            stdout.write(f"  {item['type_id']}  {item['name']}\n")
        return 0
    item = payload["type"]
    stdout.write(f"{item['type_id']}  {item['name']}\n")
    return 0


def _object_field_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow object-field <list|upsert|retire> [args]",
            "",
            "Object field authority:",
            "  workflow object-field list --type-id TYPE [--include-retired] [--json]",
            "  workflow object-field upsert --type-id TYPE --field-name NAME --field-kind KIND [--label TEXT] [--description TEXT] [--required] [--default-file <path>|--default-json '<json>'] [--options-file <path>|--options-json '<json>'] [--display-order N] [--yes] [--json]",
            "  workflow object-field retire --type-id TYPE --field-name NAME [--yes] [--json]",
        ]
    )


def _object_field_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_object_field_help_text() + "\n")
        return 2
    action = args[0]
    type_id = ""
    field_name = ""
    field_kind = ""
    label = ""
    description = ""
    required = False
    display_order = 100
    include_retired = False
    default_file = None
    default_json = None
    options_file = None
    options_json = None
    as_json = False
    confirmed = False
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--type-id" and i + 1 < len(args):
            type_id = args[i + 1]
            i += 2
            continue
        if token == "--field-name" and i + 1 < len(args):
            field_name = args[i + 1]
            i += 2
            continue
        if token == "--field-kind" and i + 1 < len(args):
            field_kind = args[i + 1]
            i += 2
            continue
        if token == "--label" and i + 1 < len(args):
            label = args[i + 1]
            i += 2
            continue
        if token == "--description" and i + 1 < len(args):
            description = args[i + 1]
            i += 2
            continue
        if token == "--required":
            required = True
            i += 1
            continue
        if token == "--display-order" and i + 1 < len(args):
            display_order = int(args[i + 1])
            i += 2
            continue
        if token == "--include-retired":
            include_retired = True
            i += 1
            continue
        if token == "--default-file" and i + 1 < len(args):
            default_file = args[i + 1]
            i += 2
            continue
        if token == "--default-json" and i + 1 < len(args):
            default_json = args[i + 1]
            i += 2
            continue
        if token == "--options-file" and i + 1 < len(args):
            options_file = args[i + 1]
            i += 2
            continue
        if token == "--options-json" and i + 1 < len(args):
            options_json = args[i + 1]
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    try:
        if action == "list":
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name="object_schema.field_list",
                payload={
                    "type_id": type_id,
                    "include_retired": include_retired,
                },
            )
        elif action == "upsert":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            default_value = (
                _load_json_value(
                    file_path=default_file,
                    inline_json=default_json,
                    field_name="default",
                )
                if default_file or default_json
                else None
            )
            options = (
                _load_json_value(
                    file_path=options_file,
                    inline_json=options_json,
                    field_name="options",
                )
                if options_file or options_json
                else []
            )
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name="object_schema.field_upsert",
                payload={
                    "type_id": type_id,
                    "field_name": field_name,
                    "field_kind": field_kind,
                    "label": label,
                    "description": description,
                    "required": required,
                    "default_value": default_value,
                    "options": options,
                    "display_order": display_order,
                },
            )
        elif action == "retire":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            payload = operation_catalog_gateway.execute_operation_from_env(
                env=_operation_env(),
                operation_name="object_schema.field_retire",
                payload={
                    "type_id": type_id,
                    "field_name": field_name,
                },
            )
        else:
            stdout.write(_object_field_help_text() + "\n")
            return 2
    except (ObjectLifecycleBoundaryError, ValueError, json.JSONDecodeError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "list":
        stdout.write(f"{payload['count']} field(s) for {payload['type_id']}\n")
        for item in payload["fields"]:
            status = " retired" if item.get("retired") else ""
            stdout.write(f"  {item['name']}  {item['type']}{status}\n")
        return 0
    if action == "upsert":
        item = payload["field"]
        stdout.write(f"{payload['type_id']}.{item['name']}  {item['type']}\n")
        return 0
    stdout.write(f"{payload['type_id']}.{payload['field_name']} retired\n")
    return 0


def _object_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow object <list|get|upsert|delete> [args]",
            "",
            "Object authority:",
            "  workflow object list --type-id TYPE [--status STATUS] [--query TEXT] [--limit N] [--json]",
            "  workflow object get <object-id> [--json]",
            "  workflow object upsert [--object-id ID] [--type-id TYPE] --properties-file <path>|--properties-json '<json>' [--yes] [--json]",
            "  workflow object delete <object-id> [--yes] [--json]",
        ]
    )


def _object_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_object_help_text() + "\n")
        return 2
    action = args[0]
    as_json = False
    confirmed = False
    object_id = ""
    type_id = ""
    status = "active"
    query = ""
    limit = 100
    props_file = None
    props_json = None
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if token == "--object-id" and i + 1 < len(args):
            object_id = args[i + 1]
            i += 2
            continue
        if token == "--type-id" and i + 1 < len(args):
            type_id = args[i + 1]
            i += 2
            continue
        if token == "--status" and i + 1 < len(args):
            status = args[i + 1]
            i += 2
            continue
        if token == "--query" and i + 1 < len(args):
            query = args[i + 1]
            i += 2
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        if token == "--properties-file" and i + 1 < len(args):
            props_file = args[i + 1]
            i += 2
            continue
        if token == "--properties-json" and i + 1 < len(args):
            props_json = args[i + 1]
            i += 2
            continue
        if action in {"get", "delete"} and not object_id:
            object_id = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    try:
        if action == "list":
            conn = _sync_conn()
            payload = list_objects(conn, type_id=type_id, status=status, query=query, limit=limit)
        elif action == "get":
            conn = _sync_conn()
            payload = {"object": get_object(conn, object_id=object_id)}
        elif action == "upsert":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            properties = _load_json_input(
                file_path=props_file,
                inline_json=props_json,
                field_name="properties",
            )
            if object_id:
                payload = {"object": update_object(conn, object_id=object_id, properties=properties)}
            else:
                payload = {"object": create_object(conn, type_id=type_id, properties=properties)}
        elif action == "delete":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            payload = delete_object(conn, object_id=object_id)
        else:
            stdout.write(_object_help_text() + "\n")
            return 2
    except (ObjectLifecycleBoundaryError, ValueError, json.JSONDecodeError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "list":
        stdout.write(f"{payload['count']} object(s)\n")
        for item in payload["objects"]:
            stdout.write(f"  {item['object_id']}  type={item['type_id']} status={item['status']}\n")
        return 0
    if action == "delete":
        stdout.write("deleted\n")
        return 0
    item = payload["object"]
    stdout.write(f"{item['object_id']}  type={item['type_id']} status={item['status']}\n")
    return 0


def _catalog_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow catalog <list|get|upsert|retire> [args]",
            "",
            "Surface catalog authority:",
            "  workflow catalog list [--surface SURFACE] [--include-disabled] [--limit N] [--json]",
            "  workflow catalog get <catalog-item-id> [--json]",
            "  workflow catalog upsert --item-file <path>|--item-json '<json>' [--yes] [--json]",
            "  workflow catalog retire <catalog-item-id> [--yes] [--json]",
        ]
    )


def _catalog_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_catalog_help_text() + "\n")
        return 2
    action = args[0]
    surface_name = "moon"
    include_disabled = False
    limit = 100
    as_json = False
    confirmed = False
    catalog_item_id = ""
    item_file = None
    item_json = None
    i = 1
    while i < len(args):
        token = args[i]
        if token == "--surface" and i + 1 < len(args):
            surface_name = args[i + 1]
            i += 2
            continue
        if token == "--include-disabled":
            include_disabled = True
            i += 1
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if token == "--item-file" and i + 1 < len(args):
            item_file = args[i + 1]
            i += 2
            continue
        if token == "--item-json" and i + 1 < len(args):
            item_json = args[i + 1]
            i += 2
            continue
        if not catalog_item_id:
            catalog_item_id = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2
    try:
        if action == "list":
            conn = _sync_conn()
            payload = {
                "items": list_surface_catalog_items(
                    conn,
                    surface_name=surface_name,
                    include_disabled=include_disabled,
                    limit=limit,
                )
            }
        elif action == "get":
            conn = _sync_conn()
            payload = {"item": get_surface_catalog_item(conn, catalog_item_id=catalog_item_id)}
        elif action == "upsert":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            payload = {
                "item": upsert_surface_catalog_item(
                    conn,
                    item=_load_json_input(
                        file_path=item_file,
                        inline_json=item_json,
                        field_name="item",
                    ),
                )
            }
        elif action == "retire":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            payload = {"item": retire_surface_catalog_item(conn, catalog_item_id=catalog_item_id)}
        else:
            stdout.write(_catalog_help_text() + "\n")
            return 2
    except (SurfaceCatalogBoundaryError, ValueError, json.JSONDecodeError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "list":
        stdout.write(f"{len(payload['items'])} catalog item(s)\n")
        for item in payload["items"]:
            stdout.write(
                f"  {item['catalog_item_id']}  {item['label']} "
                f"surface={item['surface_name']} enabled={item['enabled']}\n"
            )
        return 0
    item = payload["item"]
    stdout.write(
        f"{item['catalog_item_id']}  {item['label']} "
        f"surface={item['surface_name']} enabled={item['enabled']}\n"
    )
    return 0


def _reload_command(args: list[str], *, stdout: TextIO) -> int:
    as_json = False
    for token in args:
        if token == "--json":
            as_json = True
            continue
        stdout.write("usage: workflow reload [--json]\n")
        return 2
    payload = tool_praxis_reload({})
    if as_json:
        print_json(stdout, payload)
        return 0
    stdout.write(", ".join(payload.get("reloaded") or []) + "\n")
    return 0


def _reconcile_command(args: list[str], *, stdout: TextIO) -> int:
    return _data_command(["reconcile", *args], stdout=stdout)


__all__ = [
    "_catalog_command",
    "_object_command",
    "_object_field_command",
    "_object_type_command",
    "_reconcile_command",
    "_registry_command",
    "_reload_command",
    "_schema_command",
]
