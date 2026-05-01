"""Projects field descriptors into the unified data dictionary authority.

Walks every injection site in Praxis — Postgres tables, object_types,
integration manifests, authority_domains (for lineage governance targets),
type-contract slugs harvested from the MCP catalog, dataset families,
ingest payload kinds, operator decision kinds, receipt kinds, and MCP tool
input schemas — and writes an
`auto`-layer row per (object_kind, field_path). Operator overrides in the
`operator` source layer are left untouched.

Runs as a HeartbeatModule so dictionary entries stay current as migrations,
manifests, and tool schemas evolve.
"""

from __future__ import annotations

import logging
import time
import traceback
from pathlib import Path
from typing import Any, Iterable

from runtime.data_dictionary import apply_projection
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


class DataDictionaryProjector(HeartbeatModule):
    """Project field descriptors from every injection site."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        for label, fn in [
            ("tables", self._project_tables),
            ("object_types", self._project_object_types),
            ("integrations", self._project_integration_manifests),
            ("authority_domains", self._project_authority_domains),
            ("datasets", self._project_dataset_families),
            ("ingest", self._project_ingest_kinds),
            ("decisions", self._project_decision_kinds),
            ("receipts", self._project_receipt_kinds),
            ("type_contract_slugs", self._project_type_contract_slugs),
            ("tools", self._project_mcp_tools),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc(limit=3)}")
                logger.exception("data dictionary projector step %s failed", label)
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -- Postgres tables --------------------------------------------------

    def _project_tables(self) -> None:
        rows = self._conn.execute(
            """
            SELECT c.table_name,
                   c.column_name,
                   c.data_type,
                   c.is_nullable,
                   c.column_default,
                   c.ordinal_position,
                   pgd.description AS column_description
              FROM information_schema.columns c
              LEFT JOIN pg_catalog.pg_statio_all_tables st
                     ON st.schemaname = c.table_schema
                    AND st.relname = c.table_name
              LEFT JOIN pg_catalog.pg_description pgd
                     ON pgd.objoid = st.relid
                    AND pgd.objsubid = c.ordinal_position
             WHERE c.table_schema = 'public'
             ORDER BY c.table_name, c.ordinal_position
            """,
        )
        by_table: dict[str, list[dict[str, Any]]] = {}
        for r in rows or []:
            by_table.setdefault(r["table_name"], []).append(r)

        check_values = self._collect_check_constraints()
        table_comments = self._collect_table_comments()

        for table_name, columns in by_table.items():
            entries = [
                {
                    "field_path": str(c["column_name"]),
                    "field_kind": _sql_to_field_kind(c["data_type"]),
                    "description": str(c.get("column_description") or ""),
                    "required": (c.get("is_nullable") != "YES"),
                    "default_value": c.get("column_default"),
                    "valid_values": check_values.get(table_name, {}).get(c["column_name"], []) or [],
                    "display_order": int(c.get("ordinal_position") or 100),
                    "origin_ref": {
                        "projector": "data_dictionary_projector",
                        "source": "information_schema.columns",
                        "table": table_name,
                    },
                    "metadata": {"sql_type": str(c.get("data_type") or "")},
                }
                for c in columns
            ]
            apply_projection(
                self._conn,
                object_kind=f"table:{table_name}",
                category="table",
                entries=entries,
                source="auto",
                label=table_name,
                summary=table_comments.get(table_name, f"Postgres table {table_name!r}."),
                origin_ref={"projector": "schema_projector", "table": table_name},
                metadata={"column_count": len(entries)},
            )

    def _collect_check_constraints(self) -> dict[str, dict[str, list[str]]]:
        import re

        rows = self._conn.execute(
            """
            SELECT conrelid::regclass::text AS table_name,
                   pg_get_constraintdef(oid) AS check_def
              FROM pg_constraint
             WHERE contype = 'c'
               AND connamespace = 'public'::regnamespace
            """
        )
        result: dict[str, dict[str, list[str]]] = {}
        for r in rows or []:
            defn = r["check_def"] or ""
            arr = re.search(r"ARRAY\[(.+?)\]", defn)
            col = re.search(r"\(+\s*\(?(\w+)\)?", defn)
            if not arr or not col:
                continue
            values = re.findall(r"'([^']+)'", arr.group(1))
            if values:
                result.setdefault(r["table_name"], {})[col.group(1)] = values
        return result

    def _collect_table_comments(self) -> dict[str, str]:
        rows = self._conn.execute(
            """
            SELECT c.relname AS table_name,
                   obj_description(c.oid) AS table_comment
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relkind = 'r'
            """,
        )
        return {
            str(r["table_name"]): str(r["table_comment"] or "")
            for r in rows or []
            if r.get("table_comment")
        }

    # -- object_types authority -------------------------------------------

    def _project_object_types(self) -> None:
        types = self._conn.execute(
            "SELECT type_id, name, description, icon FROM object_types ORDER BY type_id",
        )
        for t in types or []:
            type_id = str(t["type_id"])
            fields = self._conn.execute(
                """
                SELECT field_name, label, field_kind, description, required,
                       default_value, options, display_order
                  FROM object_field_registry
                 WHERE type_id = $1 AND retired_at IS NULL
                 ORDER BY display_order, field_name
                """,
                type_id,
            )
            entries = [
                {
                    "field_path": str(f["field_name"]),
                    "field_kind": str(f["field_kind"]),
                    "label": str(f.get("label") or ""),
                    "description": str(f.get("description") or ""),
                    "required": bool(f.get("required", False)),
                    "default_value": f.get("default_value"),
                    "valid_values": f.get("options") or [],
                    "display_order": int(f.get("display_order") or 100),
                    "origin_ref": {
                        "projector": "data_dictionary_projector",
                        "source": "object_field_registry",
                        "type_id": type_id,
                    },
                }
                for f in fields or []
            ]
            apply_projection(
                self._conn,
                object_kind=f"object_type:{type_id}",
                category="object_type",
                entries=entries,
                source="auto",
                label=str(t.get("name") or type_id),
                summary=str(t.get("description") or ""),
                origin_ref={"projector": "object_field_registry", "type_id": type_id},
                metadata={"icon": str(t.get("icon") or "")},
            )

    # -- integration manifests --------------------------------------------

    def _project_integration_manifests(self) -> None:
        from runtime.integration_manifest import load_manifest_report

        report = load_manifest_report()
        if report.errors:
            raise RuntimeError(
                "integration manifest projection aborted due to malformed manifest(s): "
                + "; ".join(report.errors)
            )
        for manifest in report.manifests:
            entries: list[dict[str, Any]] = [
                {
                    "field_path": "id",
                    "field_kind": "text",
                    "required": True,
                    "description": "Stable manifest id.",
                    "display_order": 10,
                },
                {
                    "field_path": "name",
                    "field_kind": "text",
                    "description": "Human label.",
                    "display_order": 20,
                },
                {
                    "field_path": "provider",
                    "field_kind": "text",
                    "description": "Provider class (http, oauth2, ...).",
                    "display_order": 30,
                },
                {
                    "field_path": "auth_shape.kind",
                    "field_kind": "enum",
                    "valid_values": ["env_var", "oauth2", "api_key"],
                    "description": "Credential resolution strategy.",
                    "display_order": 40,
                },
            ]
            for idx, cap in enumerate(manifest.capabilities or ()):
                entries.append({
                    "field_path": f"capabilities.{cap.action}",
                    "field_kind": "object",
                    "description": str(cap.description or ""),
                    "display_order": 100 + idx * 10,
                    "metadata": {"method": str(cap.method or ""), "path": str(cap.path or "")},
                })
            apply_projection(
                self._conn,
                object_kind=f"integration:{manifest.id}",
                category="integration",
                entries=entries,
                source="auto",
                label=str(manifest.name or manifest.id),
                summary=str(manifest.description or ""),
                origin_ref={"projector": "integration_manifest", "id": manifest.id},
                metadata={
                    "provider": str(manifest.provider or ""),
                    "icon": str(manifest.icon or ""),
                },
            )

    # -- dataset refinery families ----------------------------------------

    def _project_dataset_families(self) -> None:
        specialists = self._conn.execute(
            """
            SELECT DISTINCT specialist_target, dataset_family
              FROM dataset_promotions
             WHERE specialist_target IS NOT NULL
            """,
        )
        for r in specialists or []:
            specialist = str(r["specialist_target"] or "")
            family = str(r["dataset_family"] or "")
            if not specialist:
                continue
            entries = [
                {
                    "field_path": "candidate_ids",
                    "field_kind": "array",
                    "required": True,
                    "description": "Raw candidate ids bundled into this promotion.",
                    "display_order": 10,
                },
                {
                    "field_path": "dataset_family",
                    "field_kind": "enum",
                    "valid_values": ["sft", "preference", "eval", "routing"],
                    "required": True,
                    "description": "Training family this promotion belongs to.",
                    "display_order": 20,
                },
                {
                    "field_path": "specialist_target",
                    "field_kind": "text",
                    "required": True,
                    "description": "Type key the promotion targets.",
                    "display_order": 30,
                },
                {
                    "field_path": "payload",
                    "field_kind": "json",
                    "description": "Promotion payload schema (specialist-specific).",
                    "display_order": 40,
                },
            ]
            apply_projection(
                self._conn,
                object_kind=f"dataset:{specialist}",
                category="dataset",
                entries=entries,
                source="auto",
                label=specialist,
                summary=f"Dataset promotion family for specialist {specialist!r}.",
                origin_ref={"projector": "dataset_promotions", "specialist": specialist},
                metadata={"dataset_family": family},
            )

    # -- memory ingest kinds ---------------------------------------------

    def _project_ingest_kinds(self) -> None:
        try:
            from memory.ingest import IngestKind
        except Exception:
            return
        entries = [
            {"field_path": "kind", "field_kind": "enum",
             "valid_values": [k.value for k in IngestKind],
             "required": True, "description": "Routing discriminator.", "display_order": 10},
            {"field_path": "content", "field_kind": "text",
             "required": True, "description": "Raw content being ingested.", "display_order": 20},
            {"field_path": "source", "field_kind": "text",
             "required": True, "description": "Provenance tag.", "display_order": 30},
            {"field_path": "metadata", "field_kind": "json",
             "description": "Structured metadata attached to this payload.", "display_order": 40},
            {"field_path": "timestamp", "field_kind": "datetime",
             "required": True, "description": "Event timestamp (UTC).", "display_order": 50},
            {"field_path": "idempotency_key", "field_kind": "text",
             "description": "Deduplication key; null = auto-hash.", "display_order": 60},
        ]
        apply_projection(
            self._conn,
            object_kind="ingest:IngestPayload",
            category="ingest",
            entries=entries,
            source="auto",
            label="IngestPayload",
            summary="Memory-graph ingestion payload (memory/ingest.py).",
            origin_ref={"projector": "memory.ingest", "dataclass": "IngestPayload"},
        )

    # -- operator decision kinds -----------------------------------------

    def _project_decision_kinds(self) -> None:
        rows = self._conn.execute(
            """
            SELECT DISTINCT decision_kind
              FROM operator_decisions
             WHERE decision_kind IS NOT NULL
             ORDER BY decision_kind
            """,
        )
        for r in rows or []:
            kind = str(r["decision_kind"] or "")
            if not kind:
                continue
            entries = [
                {"field_path": "decision_key", "field_kind": "text", "required": True,
                 "description": "Stable identifier.", "display_order": 10},
                {"field_path": "title", "field_kind": "text",
                 "description": "Short human label.", "display_order": 20},
                {"field_path": "rationale", "field_kind": "text",
                 "description": "Full reasoning for the decision.", "display_order": 30},
                {"field_path": "decision_scope_kind", "field_kind": "text",
                 "description": "Scope discriminator for this decision kind.", "display_order": 40},
                {"field_path": "decision_scope_ref", "field_kind": "text",
                 "description": "Scope target id.", "display_order": 50},
                {"field_path": "decided_by", "field_kind": "text",
                 "description": "Operator or agent that recorded the decision.", "display_order": 60},
                {"field_path": "effective_from", "field_kind": "datetime",
                 "description": "When the decision takes effect.", "display_order": 70},
                {"field_path": "effective_to", "field_kind": "datetime",
                 "description": "When the decision expires (null = open-ended).", "display_order": 80},
                {"field_path": "scope_clamp", "field_kind": "json",
                 "description": "Structured scope: {applies_to:[...], does_not_apply_to:[...]}. Per migration 264 this is preserved verbatim — the operator-authored rationale must not be auto-rewritten.",
                 "display_order": 90},
                {"field_path": "decision_provenance", "field_kind": "text",
                 "required": True,
                 "description": "How the decision landed: 'explicit' (operator unequivocally said so) or 'inferred' (model guessed from conversation/debate). Consumers weight explicit higher than inferred. Per migration 302.",
                 "display_order": 100,
                 "valid_values": ["explicit", "inferred"]},
                {"field_path": "decision_why", "field_kind": "text",
                 "description": "Deeper motivation, separate from rationale (which captures the rule). Per migration 302; nullable until operator-authored.",
                 "display_order": 110},
            ]
            apply_projection(
                self._conn,
                object_kind=f"decision:{kind}",
                category="decision",
                entries=entries,
                source="auto",
                label=kind,
                summary=f"operator_decisions row of kind {kind!r}.",
                origin_ref={"projector": "operator_decisions", "kind": kind},
            )

    # -- receipt kinds ---------------------------------------------------

    def _project_receipt_kinds(self) -> None:
        try:
            rows = self._conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'receipts'",
            )
        except Exception:
            return
        entries = [
            {
                "field_path": str(r["column_name"]),
                "field_kind": _sql_to_field_kind(r["data_type"]),
                "display_order": idx * 10 + 10,
                "description": "",
            }
            for idx, r in enumerate(rows or [])
        ]
        if not entries:
            return
        apply_projection(
            self._conn,
            object_kind="receipt:evidence",
            category="receipt",
            entries=entries,
            source="auto",
            label="receipt",
            summary="Evidence receipt columns.",
            origin_ref={"projector": "information_schema", "table": "receipts"},
        )

    # -- authority domains (for lineage governed_by edges) ---------------

    def _project_authority_domains(self) -> None:
        rows = self._conn.execute(
            """
            SELECT authority_domain_ref, owner_ref, event_stream_ref,
                   enabled, decision_ref
              FROM authority_domains
             ORDER BY authority_domain_ref
            """,
        )
        for r in rows or []:
            ref = str(r.get("authority_domain_ref") or "").strip()
            if not ref:
                continue
            entries = [
                {
                    "field_path": "authority_domain_ref",
                    "field_kind": "text",
                    "description": "Stable authority domain reference.",
                    "required": True,
                    "display_order": 10,
                    "origin_ref": {"projector": "authority_domains", "ref": ref},
                },
            ]
            apply_projection(
                self._conn,
                object_kind=ref,
                category="definition",
                entries=entries,
                source="auto",
                label=ref,
                summary=f"Authority domain {ref!r}.",
                origin_ref={"projector": "authority_domains", "ref": ref},
                metadata={
                    "owner_ref": str(r.get("owner_ref") or ""),
                    "event_stream_ref": str(r.get("event_stream_ref") or ""),
                    "enabled": bool(r.get("enabled", True)),
                    "decision_ref": str(r.get("decision_ref") or ""),
                },
            )

    # -- type-contract slugs (consumes/produces in MCP catalog) ----------

    def _project_type_contract_slugs(self) -> None:
        try:
            from surfaces.mcp.catalog import get_tool_catalog
            definitions = list(get_tool_catalog().values())
        except Exception:
            return
        slugs: set[str] = set()
        for tool in definitions:
            for _action, contract in tool.type_contract.items():
                slugs.update(contract.get("consumes", []))
                slugs.update(contract.get("produces", []))
        for slug in sorted(slugs):
            slug = slug.strip()
            if not slug:
                continue
            kind = f"type_slug:{slug}"
            apply_projection(
                self._conn,
                object_kind=kind,
                category="object",
                entries=[
                    {
                        "field_path": "slug",
                        "field_kind": "text",
                        "description": "Praxis typed slug from tool type_contract metadata.",
                        "required": True,
                        "display_order": 10,
                        "origin_ref": {"projector": "type_contract_slugs", "slug": slug},
                    },
                ],
                source="auto",
                label=slug,
                summary=f"Type slug {slug!r} referenced by MCP tool type_contract.",
                origin_ref={"projector": "mcp_catalog_type_slugs", "slug": slug},
                metadata={"slug": slug},
            )

    # -- MCP tool input schemas ------------------------------------------

    def _project_mcp_tools(self) -> None:
        try:
            from surfaces.mcp.catalog import get_tool_catalog
            definitions = list(get_tool_catalog().values())
        except Exception:
            return

        for tool in definitions:
            name = getattr(tool, "name", None)
            if not name:
                continue
            schema = getattr(tool, "input_schema", None) or {}
            props = schema.get("properties") if isinstance(schema, dict) else {}
            required = set(schema.get("required", [])) if isinstance(schema, dict) else set()
            entries: list[dict[str, Any]] = []
            for idx, (prop_name, prop) in enumerate(sorted((props or {}).items())):
                prop = prop if isinstance(prop, dict) else {}
                entries.append({
                    "field_path": str(prop_name),
                    "field_kind": _json_schema_to_field_kind(prop),
                    "description": str(prop.get("description") or ""),
                    "required": prop_name in required,
                    "default_value": prop.get("default"),
                    "valid_values": list(prop.get("enum", []) or []),
                    "display_order": (idx + 1) * 10,
                    "metadata": {"json_schema": prop},
                })
            if not entries:
                continue
            raw_ops = tool.metadata.get("operation_names")
            op_names: list[str] = []
            if isinstance(raw_ops, list):
                op_names = [str(x).strip() for x in raw_ops if str(x).strip()]
            tool_metadata: dict[str, Any] = {
                "cli_surface": tool.cli_surface,
                "cli_tier": tool.cli_tier,
                "risk_levels": list(tool.risk_levels),
                "use_when": tool.cli_when_to_use or "",
                "when_not_to_use": tool.cli_when_not_to_use or "",
                "kind": tool.kind,
                "operation_names": op_names,
                "type_contract": tool.type_contract,
            }
            apply_projection(
                self._conn,
                object_kind=f"tool:{name}",
                category="tool",
                entries=entries,
                source="auto",
                label=str(name),
                summary=str(getattr(tool, "description", "") or "")[:300],
                origin_ref={"projector": "mcp_catalog", "tool": str(name)},
                metadata=tool_metadata,
            )


# --- helpers --------------------------------------------------------------


_SQL_KIND_MAP = {
    "integer": "number", "bigint": "number", "smallint": "number",
    "numeric": "number", "real": "number", "double precision": "number",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "datetime", "timestamp without time zone": "datetime",
    "timestamp with time zone": "datetime", "time": "datetime",
    "jsonb": "json", "json": "json",
    "uuid": "text", "text": "text", "character varying": "text",
    "character": "text", "bytea": "text",
    "ARRAY": "array",
}


def _sql_to_field_kind(data_type: Any) -> str:
    text = str(data_type or "").strip().lower()
    return _SQL_KIND_MAP.get(text, _SQL_KIND_MAP.get(str(data_type or ""), "text"))


def _json_schema_to_field_kind(prop: dict[str, Any]) -> str:
    if "enum" in prop:
        return "enum"
    kind = str(prop.get("type") or "").strip().lower()
    return {
        "string": "text",
        "integer": "number",
        "number": "number",
        "boolean": "boolean",
        "array": "array",
        "object": "object",
    }.get(kind, "text")


__all__ = ["DataDictionaryProjector"]
