"""Direct CLI front doors for schema, registry, object, and catalog authority."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, TextIO

from runtime.authority_memory_projection import refresh_authority_memory_projection
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
    workflow_migration_audit,
)
from storage.postgres.validators import PostgresConfigurationError, PostgresStorageError
from surfaces.cli._db import cli_sync_conn
from surfaces.cli.mcp_tools import load_json_file, print_json
from surfaces.mcp.tools.data_dictionary import tool_praxis_data_dictionary
from surfaces.mcp.tools.health import tool_praxis_reload
from system_authority.workflow_migration_sequence_manager import (
    normalize_workflow_migration_slug,
    propose_workflow_migration_filename,
    workflow_migration_sequence_state,
)

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
            "usage: workflow schema <status|plan|apply|describe|next-migration> [args]",
            "",
            "Schema authority:",
            "  workflow schema status [--scope workflow|control] [--json]",
            "  workflow schema plan [--scope workflow|control] [--json]",
            "  workflow schema apply [--scope workflow|control] [--yes] [--json]",
            "  workflow schema describe <object-name|migration.sql> [--scope workflow|control] [--json]",
            "  workflow schema next-migration <slug> [--json]",
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
        migration_audit = await workflow_migration_audit(conn)
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
            "migration_audit": {
                "declared_count": len(migration_audit.declared),
                "applied_count": len(migration_audit.applied),
                "missing": list(migration_audit.missing),
                "drifted": [row.filename for row in migration_audit.drifted],
                "extra": [row.filename for row in migration_audit.extra],
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


def _schema_next_migration_payload(*, slug: str) -> dict[str, Any]:
    normalized_slug = normalize_workflow_migration_slug(slug)
    state = workflow_migration_sequence_state()
    return {
        "scope": "workflow",
        "migration_manager": "deterministic_numeric_prefix_allocator",
        "requested_slug": slug,
        "normalized_slug": normalized_slug,
        "next_prefix": state.next_prefix,
        "proposed_filename": propose_workflow_migration_filename(slug=slug),
        "managed_duplicate_prefixes": {
            prefix: list(filenames)
            for prefix, filenames in state.managed_duplicate_prefixes.items()
        },
        "unmanaged_duplicate_prefixes": {
            prefix: list(filenames)
            for prefix, filenames in state.unmanaged_duplicate_prefixes.items()
        },
    }


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
            migration_audit = payload.get("migration_audit") or {}
            payload["pending_migrations"] = sorted(
                {
                    *payload.get("missing_by_migration", {}).keys(),
                    *migration_audit.get("missing", ()),
                }
            )
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
        elif action == "next-migration":
            if scope != "workflow":
                stdout.write("workflow schema next-migration only supports --scope workflow\n")
                return 2
            if not target:
                stdout.write("usage: workflow schema next-migration <slug> [--json]\n")
                return 2
            payload = _schema_next_migration_payload(slug=target)
        else:
            stdout.write(_schema_help_text() + "\n")
            return 2
    except (PostgresConfigurationError, PostgresStorageError, ValueError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1
    if as_json:
        print_json(stdout, payload)
        return 0
    if action == "next-migration":
        stdout.write(
            f"next_prefix={payload.get('next_prefix')} "
            f"proposed_filename={payload.get('proposed_filename')}\n"
        )
        if payload.get("managed_duplicate_prefixes"):
            stdout.write("managed_duplicate_prefixes:\n")
            for prefix, filenames in sorted(payload["managed_duplicate_prefixes"].items()):
                stdout.write(f"  {prefix}: {', '.join(filenames)}\n")
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


def _data_dictionary_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow dictionary <list|describe|set-override|clear-override|reproject> [args]",
            "",
            "Data dictionary authority:",
            "  workflow dictionary list [--category CATEGORY] [--json]",
            "  workflow dictionary describe <object-kind> [--include-layers] [--json]",
            "  workflow dictionary set-override <object-kind> <field-path> [--field-kind KIND] [--label TEXT] [--description TEXT] [--required] [--default-json JSON] [--valid-values-json JSON] [--examples-json JSON] [--deprecation-notes TEXT] [--display-order N] [--metadata-json JSON] [--yes] [--json]",
            "  workflow dictionary clear-override <object-kind> <field-path> [--yes] [--json]",
            "  workflow dictionary reproject [--yes] [--json]",
        ]
    )


def _data_dictionary_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_data_dictionary_help_text() + "\n")
        return 2

    action = args[0]
    as_json = False
    confirmed = False
    category = ""
    include_layers = False
    object_kind = ""
    field_path = ""
    field_kind = None
    label = None
    description = None
    required = None
    default_value = None
    valid_values = None
    examples = None
    deprecation_notes = None
    display_order = None
    metadata = None
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
        if token == "--category" and i + 1 < len(args):
            category = args[i + 1]
            i += 2
            continue
        if token == "--include-layers":
            include_layers = True
            i += 1
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
        if token == "--default-json" and i + 1 < len(args):
            default_value = json.loads(args[i + 1])
            i += 2
            continue
        if token == "--valid-values-json" and i + 1 < len(args):
            valid_values = json.loads(args[i + 1])
            i += 2
            continue
        if token == "--examples-json" and i + 1 < len(args):
            examples = json.loads(args[i + 1])
            i += 2
            continue
        if token == "--deprecation-notes" and i + 1 < len(args):
            deprecation_notes = args[i + 1]
            i += 2
            continue
        if token == "--display-order" and i + 1 < len(args):
            display_order = int(args[i + 1])
            i += 2
            continue
        if token == "--metadata-json" and i + 1 < len(args):
            metadata = json.loads(args[i + 1])
            i += 2
            continue
        if not object_kind:
            object_kind = token
            i += 1
            continue
        if not field_path:
            field_path = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2

    try:
        if action == "list":
            payload = tool_praxis_data_dictionary({"action": "list", "category": category or None})
        elif action == "describe":
            if not object_kind:
                stdout.write("usage: workflow dictionary describe <object-kind> [--include-layers] [--json]\n")
                return 2
            payload = tool_praxis_data_dictionary({
                "action": "describe",
                "object_kind": object_kind,
                "include_layers": include_layers,
            })
        elif action == "set-override":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            if not object_kind or not field_path:
                stdout.write("usage: workflow dictionary set-override <object-kind> <field-path> [--yes] [--json]\n")
                return 2
            payload = tool_praxis_data_dictionary({
                "action": "set_override",
                "object_kind": object_kind,
                "field_path": field_path,
                "field_kind": field_kind,
                "label": label,
                "description": description,
                "required": required,
                "default_value": default_value,
                "valid_values": valid_values,
                "examples": examples,
                "deprecation_notes": deprecation_notes,
                "display_order": display_order,
                "metadata": metadata,
            })
        elif action == "clear-override":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            if not object_kind or not field_path:
                stdout.write("usage: workflow dictionary clear-override <object-kind> <field-path> [--yes] [--json]\n")
                return 2
            payload = tool_praxis_data_dictionary({
                "action": "clear_override",
                "object_kind": object_kind,
                "field_path": field_path,
            })
        elif action == "reproject":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            payload = tool_praxis_data_dictionary({"action": "reproject"})
        else:
            stdout.write(_data_dictionary_help_text() + "\n")
            return 2
    except (json.JSONDecodeError, ValueError) as exc:
        print_json(stdout, {"error": str(exc)})
        return 1

    if action == "describe":
        if isinstance(payload, dict) and payload.get("error"):
            if as_json:
                print_json(
                    stdout,
                    {
                        "action": "describe",
                        "status": "not_found" if payload.get("status_code") == 404 else "error",
                        "reason_code": "data_dictionary.object_not_found"
                        if payload.get("status_code") == 404
                        else "data_dictionary.error",
                        "object_kind": object_kind,
                        **payload,
                    },
                )
            else:
                stdout.write(
                    f"data dictionary describe failed for {object_kind}: "
                    f"{payload.get('error')}\n"
                )
            return 1
        obj = payload.get("object") if isinstance(payload, dict) else {}
        if not isinstance(obj, dict):
            if as_json:
                print_json(
                    stdout,
                    {
                        "action": "describe",
                        "status": "not_found",
                        "reason_code": "data_dictionary.object_not_found",
                        "object_kind": object_kind,
                        "fields": [],
                    },
                )
            else:
                stdout.write(f"data dictionary object not found: {object_kind}\n")
            return 1

    if as_json:
        print_json(stdout, payload)
        return 0

    if action == "list":
        objects = payload.get("objects") if isinstance(payload, dict) else []
        count = payload.get("count", len(objects)) if isinstance(payload, dict) else len(objects)
        stdout.write(f"{count} object kind(s)\n")
        for item in objects or []:
            if not isinstance(item, dict):
                continue
            summary = str(item.get("summary") or "").strip()
            category_name = str(item.get("category") or "").strip()
            stdout.write(
                f"  {str(item.get('object_kind') or ''):<32} "
                f"{category_name:<12} {str(item.get('label') or '')}\n"
            )
            if summary:
                stdout.write(f"           {summary}\n")
        return 0

    if action == "describe":
        obj = payload.get("object") if isinstance(payload, dict) else {}
        fields = payload.get("fields") if isinstance(payload, dict) else []
        stdout.write(
            f"{str(obj.get('object_kind') or object_kind)} "
            f"fields={len(fields)} source_rows={payload.get('entries_by_source', {})}\n"
        )
        for field in fields or []:
            if not isinstance(field, dict):
                continue
            stdout.write(
                f"  {str(field.get('field_path') or ''):<32} "
                f"{str(field.get('effective_source') or ''):<10} "
                f"{str(field.get('field_kind') or ''):<10} "
                f"{str(field.get('label') or '')}\n"
            )
        return 0

    if action == "reproject":
        stdout.write(
            f"reprojected ok={bool(payload.get('ok'))} "
            f"duration_ms={payload.get('duration_ms')}\n"
        )
        return 0

    kind = str(payload.get("object_kind") or object_kind)
    path = str(payload.get("field_path") or field_path)
    stdout.write(f"{action.replace('-', ' ')} {kind}.{path}\n")
    return 0


def _authority_memory_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow authority-memory refresh [--json]",
            "",
            "Authority-memory projection:",
            "  workflow authority-memory refresh [--json]",
            "",
            "Refreshes canonical FK projections from authority tables into memory_edges.",
        ]
    )


def _authority_memory_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_authority_memory_help_text() + "\n")
        return 2

    action = args[0]
    as_json = False
    if len(args) > 2:
        stdout.write(f"unexpected argument: {args[2]}\n")
        return 2
    if len(args) == 2:
        if args[1] != "--json":
            stdout.write(f"unexpected argument: {args[1]}\n")
            return 2
        as_json = True

    if action != "refresh":
        stdout.write(_authority_memory_help_text() + "\n")
        return 2

    try:
        result = asyncio.run(refresh_authority_memory_projection())
    except Exception as exc:
        print_json(stdout, {"error": str(exc)})
        return 1

    payload = result.to_json()
    if as_json:
        print_json(stdout, payload)
        return 0

    stdout.write(
        f"projection_id={payload.get('projection_id')} "
        f"upserted={payload.get('total_upserted', 0)} "
        f"deactivated={payload.get('total_deactivated', 0)}\n"
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
    if not args:
        stdout.write(_object_help_text() + "\n")
        return 2
    if args[0] in {"-h", "--help"}:
        stdout.write(_object_help_text() + "\n")
        return 0
    if any(token in {"-h", "--help"} for token in args[1:]):
        stdout.write(_object_help_text() + "\n")
        return 0
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
