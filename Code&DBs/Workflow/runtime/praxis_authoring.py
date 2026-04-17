"""Deterministic scaffold helpers for Praxis authoring surfaces."""

from __future__ import annotations

import re
from typing import Any


_SCALAR_SQL_TYPES = {
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "currency": "NUMERIC(18,2)",
    "date": "DATE",
    "email": "TEXT",
    "integer": "BIGINT",
    "int": "BIGINT",
    "json": "JSONB",
    "jsonb": "JSONB",
    "number": "DOUBLE PRECISION",
    "numeric": "DOUBLE PRECISION",
    "text": "TEXT",
    "timestamp": "TIMESTAMPTZ",
    "timestamptz": "TIMESTAMPTZ",
}


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _slug(value: Any, *, separator: str = "_", fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", separator, _text(value).lower()).strip(separator)
    return normalized or fallback


def _title_from_slug(value: str) -> str:
    return " ".join(part.capitalize() for part in value.replace("-", "_").split("_") if part)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _field_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    fields: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = _slug(item.get("name"), fallback="field")
        field_type = _slug(item.get("type") or "text", separator="_", fallback="text")
        values = _string_list(item.get("values"))
        fields.append(
            {
                "name": name,
                "label": _text(item.get("label")) or _title_from_slug(name),
                "type": field_type,
                "required": bool(item.get("required")),
                "default": item.get("default"),
                "values": values,
                "description": _text(item.get("description")),
            }
        )
    return fields


def _sql_type(field: dict[str, Any]) -> str:
    if field["type"] == "enum":
        return "TEXT"
    return _SCALAR_SQL_TYPES.get(field["type"], "TEXT")


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _sql_column_definition(field: dict[str, Any]) -> str:
    parts = [f"{field['name']} {_sql_type(field)}"]
    if field["required"]:
        parts.append("NOT NULL")
    if field["default"] is not None:
        parts.append(f"DEFAULT {_sql_literal(field['default'])}")
    if field["type"] == "enum" and field["values"]:
        allowed = ", ".join(_sql_literal(value) for value in field["values"])
        parts.append(f"CHECK ({field['name']} IN ({allowed}))")
    return " ".join(parts)


def _write_sql_block(*, statements: list[str]) -> str:
    return "\n".join(statement.rstrip() for statement in statements if statement).strip() + "\n"


def scaffold_primitive(spec: dict[str, Any]) -> dict[str, Any]:
    primitive_type = _slug(
        spec.get("primitive_type") or spec.get("name") or spec.get("table_name"),
        fallback="primitive",
    )
    title = _text(spec.get("title")) or _title_from_slug(primitive_type)
    table_name = _slug(spec.get("table_name") or f"{primitive_type}_primitives", fallback="runtime_primitives")
    canonical_key = _slug(spec.get("canonical_key") or "canonical_key", fallback="canonical_key")
    extra_fields = _field_list(spec.get("fields"))

    column_lines = [
        "primitive_id TEXT PRIMARY KEY",
        "primitive_type TEXT NOT NULL",
        "title TEXT NOT NULL",
        "summary TEXT",
        "status TEXT NOT NULL DEFAULT 'active'",
        "parent_primitive_id TEXT",
        "source_system TEXT",
        "source_record_id TEXT",
        f"{canonical_key} TEXT",
        "attributes JSONB NOT NULL DEFAULT '{}'::jsonb",
        "measures JSONB NOT NULL DEFAULT '{}'::jsonb",
        "dimensions JSONB NOT NULL DEFAULT '{}'::jsonb",
        "display JSONB NOT NULL DEFAULT '{}'::jsonb",
        "lineage JSONB NOT NULL DEFAULT '{}'::jsonb",
        "created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    ]
    column_lines.extend(_sql_column_definition(field) for field in extra_fields)
    sql = _write_sql_block(
        statements=[
            f"CREATE TABLE IF NOT EXISTS {table_name} (",
            "    " + ",\n    ".join(column_lines),
            ");",
            "",
            f"CREATE INDEX IF NOT EXISTS {table_name}_type_status_idx ON {table_name} (primitive_type, status);",
            f"CREATE INDEX IF NOT EXISTS {table_name}_parent_idx ON {table_name} (parent_primitive_id);",
            f"CREATE INDEX IF NOT EXISTS {table_name}_canonical_key_idx ON {table_name} ({canonical_key});",
            f"CREATE INDEX IF NOT EXISTS {table_name}_attributes_gin_idx ON {table_name} USING GIN (attributes);",
        ]
    )
    return {
        "primitive": {
            "primitive_type": primitive_type,
            "title": title,
            "table_name": table_name,
            "canonical_key": canonical_key,
            "extra_fields": extra_fields,
        },
        "sql": sql,
        "migration_name": f"create_{table_name}.sql",
    }


def scaffold_table(spec: dict[str, Any]) -> dict[str, Any]:
    table_name = _slug(spec.get("table_name") or spec.get("name"), fallback="runtime_table")
    primary_key = _slug(spec.get("primary_key") or "id", fallback="id")
    include_audit_columns = spec.get("include_audit_columns", True) is not False
    fields = _field_list(spec.get("fields"))
    if not any(field["name"] == primary_key for field in fields):
        fields.insert(
            0,
            {
                "name": primary_key,
                "label": _title_from_slug(primary_key),
                "type": "text",
                "required": True,
                "default": None,
                "values": [],
                "description": "Primary key",
            },
        )

    column_lines = []
    for field in fields:
        column_sql = _sql_column_definition(field)
        if field["name"] == primary_key:
            column_sql += " PRIMARY KEY"
        column_lines.append(column_sql)
    if include_audit_columns:
        column_lines.extend(
            [
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()",
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
            ]
        )
    sql = _write_sql_block(
        statements=[
            f"CREATE TABLE IF NOT EXISTS {table_name} (",
            "    " + ",\n    ".join(column_lines),
            ");",
        ]
    )
    return {
        "table": {
            "table_name": table_name,
            "primary_key": primary_key,
            "fields": fields,
            "include_audit_columns": include_audit_columns,
        },
        "sql": sql,
        "migration_name": f"create_{table_name}.sql",
    }


def scaffold_view(spec: dict[str, Any]) -> dict[str, Any]:
    view_name = _slug(spec.get("view_name") or spec.get("name"), fallback="runtime_view")
    from_relation = _slug(spec.get("from") or spec.get("table_name") or spec.get("source"), fallback="runtime_table")
    columns = spec.get("columns")
    column_defs: list[dict[str, Any]] = []
    if isinstance(columns, list):
        for item in columns:
            if not isinstance(item, dict):
                continue
            expression = _text(item.get("expression") or item.get("source"))
            alias = _slug(item.get("alias") or item.get("name") or expression, fallback="column")
            if not expression:
                expression = alias
            column_defs.append({"expression": expression, "alias": alias})
    if not column_defs:
        column_defs = [{"expression": "*", "alias": "*"}]
    filters = _string_list(spec.get("filters"))
    group_by = _string_list(spec.get("group_by"))
    select_lines = []
    for column in column_defs:
        if column["alias"] == "*" and column["expression"] == "*":
            select_lines.append("    *")
        else:
            select_lines.append(f"    {column['expression']} AS {column['alias']}")
    sql_parts = [
        f"CREATE OR REPLACE VIEW {view_name} AS",
        "SELECT",
        ",\n".join(select_lines),
        f"FROM {from_relation}",
    ]
    if filters:
        sql_parts.append("WHERE " + " AND ".join(filters))
    if group_by:
        sql_parts.append("GROUP BY " + ", ".join(group_by))
    sql_parts[-1] = sql_parts[-1] + ";"
    sql = _write_sql_block(statements=sql_parts)
    return {
        "view": {
            "view_name": view_name,
            "from": from_relation,
            "columns": column_defs,
            "filters": filters,
            "group_by": group_by,
        },
        "sql": sql,
        "migration_name": f"create_{view_name}.sql",
    }


def scaffold_object_type(spec: dict[str, Any]) -> dict[str, Any]:
    type_id = _slug(spec.get("type_id") or spec.get("name"), fallback="object_type")
    name = _text(spec.get("name")) or _title_from_slug(type_id)
    fields = _field_list(spec.get("fields"))
    display_field = _slug(spec.get("display_field") or "name", fallback="name")
    key_field = _slug(spec.get("key_field") or "id", fallback="id")
    if not any(field["name"] == display_field for field in fields):
        fields.insert(
            0,
            {
                "name": display_field,
                "label": _title_from_slug(display_field),
                "type": "text",
                "required": True,
                "default": None,
                "values": [],
                "description": "Display field",
            },
        )
    if not any(field["name"] == key_field for field in fields):
        fields.insert(
            0,
            {
                "name": key_field,
                "label": _title_from_slug(key_field),
                "type": "text",
                "required": True,
                "default": None,
                "values": [],
                "description": "Canonical key field",
            },
        )
    return {
        "object_type": {
            "type_id": type_id,
            "name": name,
            "description": _text(spec.get("description")),
            "icon": _text(spec.get("icon")),
            "fields": fields,
        },
        "recommended_modules": [
            {"module": "search-panel", "config": {"objectType": type_id}},
            {"module": "data-table", "config": {"objectType": type_id, "publishSelection": type_id}},
            {"module": "key-value", "config": {"subscribeSelection": type_id}},
        ],
    }


def scaffold_hierarchy(spec: dict[str, Any]) -> dict[str, Any]:
    parent_type_id = _slug(spec.get("parent_type_id"), fallback="parent")
    child_type_id = _slug(spec.get("child_type_id"), fallback="child")
    relationship_name = _slug(spec.get("relationship_name") or f"{parent_type_id}_{child_type_id}", fallback="hierarchy")
    parent_field = _slug(spec.get("parent_field") or f"{parent_type_id}_id", fallback=f"{parent_type_id}_id")
    rollup_fields = _string_list(spec.get("rollup_fields"))
    view_name = _slug(spec.get("view_name") or f"{relationship_name}_tree", fallback=f"{relationship_name}_tree")
    sql = _write_sql_block(
        statements=[
            f"CREATE OR REPLACE VIEW {view_name} AS",
            "SELECT",
            "    child.object_id AS child_object_id,",
            "    child.type_id AS child_type_id,",
            f"    child.properties->>'{parent_field}' AS parent_object_id,",
            "    parent.type_id AS parent_type_id,",
            "    child.properties AS child_properties,",
            "    parent.properties AS parent_properties",
            "FROM objects AS child",
            "LEFT JOIN objects AS parent",
            f"  ON parent.object_id = child.properties->>'{parent_field}'",
            f"WHERE child.type_id = '{child_type_id}';",
        ]
    )
    return {
        "hierarchy": {
            "relationship_name": relationship_name,
            "parent_type_id": parent_type_id,
            "child_type_id": child_type_id,
            "child_fields": [
                {
                    "name": parent_field,
                    "label": _title_from_slug(parent_field),
                    "type": "text",
                    "required": False,
                    "default": None,
                    "values": [],
                    "description": f"Reference to parent {parent_type_id}",
                },
                {
                    "name": "sort_order",
                    "label": "Sort Order",
                    "type": "integer",
                    "required": False,
                    "default": 0,
                    "values": [],
                    "description": "Stable sibling ordering",
                },
            ],
            "rollup_fields": rollup_fields,
        },
        "view": {"view_name": view_name, "sql": sql},
    }


def plan_data_shape(spec: dict[str, Any]) -> dict[str, Any]:
    target_name = _slug(spec.get("target_name") or spec.get("name"), fallback="canonical_shape")
    target_fields = _field_list(spec.get("target_fields"))
    sources = spec.get("sources")
    if not isinstance(sources, list):
        sources = []

    source_plans: list[dict[str, Any]] = []
    inferred_fields: dict[str, dict[str, Any]] = {field["name"]: field for field in target_fields}
    for raw_source in sources:
        if not isinstance(raw_source, dict):
            continue
        source_name = _slug(raw_source.get("name"), fallback="source")
        relation = _slug(raw_source.get("relation") or raw_source.get("table_name") or source_name, fallback=source_name)
        field_map = raw_source.get("field_map") if isinstance(raw_source.get("field_map"), dict) else {}
        normalized_map: dict[str, str] = {}
        for source_field, target_field in field_map.items():
            source_key = _slug(source_field, fallback="source_field")
            target_key = _slug(target_field, fallback="target_field")
            normalized_map[source_key] = target_key
            inferred_fields.setdefault(
                target_key,
                {
                    "name": target_key,
                    "label": _title_from_slug(target_key),
                    "type": "text",
                    "required": False,
                    "default": None,
                    "values": [],
                    "description": f"Mapped from {source_name}.{source_key}",
                },
            )
        source_plans.append(
            {
                "name": source_name,
                "relation": relation,
                "field_map": normalized_map,
            }
        )

    canonical_fields = list(inferred_fields.values())
    if not canonical_fields:
        canonical_fields = [
            {
                "name": "canonical_key",
                "label": "Canonical Key",
                "type": "text",
                "required": True,
                "default": None,
                "values": [],
                "description": "Primary identity across sources",
            },
            {
                "name": "title",
                "label": "Title",
                "type": "text",
                "required": True,
                "default": None,
                "values": [],
                "description": "Display title",
            },
        ]

    select_blocks: list[str] = []
    target_field_names = [field["name"] for field in canonical_fields]
    for source in source_plans:
        select_lines = [f"    '{source['name']}'::text AS source_system"]
        for target_field in target_field_names:
            source_field = next(
                (field for field, mapped in source["field_map"].items() if mapped == target_field),
                None,
            )
            if source_field:
                select_lines.append(f"  , {source_field}::text AS {target_field}")
            else:
                select_lines.append(f"  , NULL::text AS {target_field}")
        select_blocks.append("SELECT\n" + "\n".join(select_lines) + f"\nFROM {source['relation']}")

    view_name = _slug(spec.get("view_name") or f"{target_name}_canonical_v", fallback=f"{target_name}_canonical_v")
    sql = _write_sql_block(
        statements=[
            f"CREATE OR REPLACE VIEW {view_name} AS",
            "\nUNION ALL\n".join(select_blocks) + ";"
            if select_blocks
            else "SELECT 'manual'::text AS source_system;",
        ]
    )
    return {
        "target_name": target_name,
        "canonical_fields": canonical_fields,
        "sources": source_plans,
        "materialization": {
            "view_name": view_name,
            "sql": sql,
        },
    }


def scaffold_page(spec: dict[str, Any]) -> dict[str, Any]:
    title = _text(spec.get("title") or spec.get("name") or spec.get("intent")) or "Praxis Page"
    manifest_id = _slug(spec.get("manifest_id") or title, separator="-", fallback="praxis-page")
    description = _text(spec.get("description") or spec.get("intent"))

    object_type_payloads: list[dict[str, Any]] = []
    raw_object_types = spec.get("object_types")
    if isinstance(raw_object_types, list):
        for item in raw_object_types:
            if isinstance(item, dict):
                object_type_payloads.append(scaffold_object_type(item)["object_type"])
    primary_type = _slug(
        spec.get("primary_type")
        or (object_type_payloads[0]["type_id"] if object_type_payloads else ""),
        fallback="record",
    )

    metrics = _string_list(spec.get("metrics"))
    if not metrics:
        metrics = ["Total", "Active"]

    manifest = {
        "version": 2,
        "grid": "4x4",
        "title": title,
        "quadrants": {
            "A1": {
                "module": "search-panel",
                "span": "2x1",
                "config": {
                    "objectType": primary_type,
                    "placeholder": f"Search {primary_type.replace('_', ' ')}",
                },
            },
            "C1": {
                "module": "metric",
                "config": {
                    "label": metrics[0],
                    "value": "0",
                    "color": "#2563eb",
                },
            },
            "D1": {
                "module": "button-row",
                "config": {
                    "actions": [
                        {
                            "label": f"New {primary_type.replace('_', ' ').title()}",
                            "variant": "primary",
                            "createObject": {"typeId": primary_type},
                        }
                    ]
                },
            },
            "A2": {
                "module": "data-table",
                "span": "3x3",
                "config": {
                    "objectType": primary_type,
                    "publishSelection": primary_type,
                    "title": title,
                },
            },
            "D2": {
                "module": "key-value",
                "span": "1x2",
                "config": {
                    "subscribeSelection": primary_type,
                    "title": "Details",
                },
            },
            "D4": {
                "module": "activity-feed",
                "config": {"title": "Recent activity"},
            },
        },
    }
    if len(metrics) > 1:
        manifest["quadrants"]["B1"] = {
            "module": "metric",
            "config": {
                "label": metrics[1],
                "value": "0",
                "color": "#16a34a",
            },
        }
    return {
        "manifest_id": manifest_id,
        "name": title,
        "description": description,
        "manifest": manifest,
        "object_types": object_type_payloads,
        "bindings": {
            "primary_type": primary_type,
            "module_count": len(manifest["quadrants"]),
        },
    }


__all__ = [
    "plan_data_shape",
    "scaffold_hierarchy",
    "scaffold_object_type",
    "scaffold_page",
    "scaffold_primitive",
    "scaffold_table",
    "scaffold_view",
]
