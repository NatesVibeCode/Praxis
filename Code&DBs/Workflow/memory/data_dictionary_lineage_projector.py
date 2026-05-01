"""Projects lineage edges into the data_dictionary_lineage authority.

Walks the live surface of Praxis and emits auto-layer edges:

* **FK constraints** → `table:<src> -> table:<dst>` with kind ``references``
  (field-level, using column names on both sides).
* **Postgres views** → `table:<base> -> table:<view>` via `pg_depend` —
  `derives_from` edges that track SQL-level derivation.
* **dataset_promotions** → `dataset:<specialist> -> object_type:<target>`
  edges (`promotes_to`) for every specialist target that appears in a
  promotion row.
* **Integration manifests** → `integration:<id> -> tool:<tool>` edges
  (`references`) for every manifest capability that names a bound tool.
* **MCP tools** → `tool:<name> -> table:<t>` or `-> object_type:<t>` when
  the tool's input schema references a known table/object_type by name.
* **MCP operation_names** → `tool:<name> -> operation.<op>` (`dispatches`)
  when the catalog lists gateway operations for the tool and the operation
  row exists in the data dictionary.
* **MCP type_contract** → `tool:<name> -> type_slug:<slug>` as ``consumes``
  or ``produces`` for typed slugs registered from catalog metadata.
* **Operation catalog** → `operation.<name> -> <authority_domain_ref>`
  (`governed_by`) from ``operation_catalog_registry`` when the domain is
  projected as a ``definition`` object.

Operator overrides in the `operator` source layer are untouched. Each
projector step writes with `origin_ref.projector = <step_tag>` so
replace_projected_edges can idempotently prune stale rows.

Runs as a HeartbeatModule so the lineage graph stays current as FK
topology and manifests evolve.
"""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any, Iterable

from runtime.data_dictionary_lineage import apply_projected_edges
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


# Map `object_kind` values by category so projector steps can look up
# "is this a known table/object_type/integration/tool/dataset" in O(1).
def _known_by_category(conn: Any) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT category, object_kind FROM data_dictionary_objects"
    )
    by_category: dict[str, set[str]] = {}
    for r in rows or []:
        by_category.setdefault(str(r["category"]), set()).add(str(r["object_kind"]))
    return by_category


class DataDictionaryLineageProjector(HeartbeatModule):
    """Project lineage edges from FKs, view deps, and manifests."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_lineage_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        known = _known_by_category(self._conn)
        for label, fn in [
            ("fk_edges", lambda: self._project_fk_edges(known)),
            ("view_deps", lambda: self._project_view_dependencies(known)),
            ("dataset_promotions", lambda: self._project_dataset_promotions(known)),
            ("integration_manifests", lambda: self._project_integration_manifests(known)),
            ("tool_schema_refs", lambda: self._project_tool_schema_refs(known)),
            ("tool_operation_edges", lambda: self._project_tool_operation_edges(known)),
            ("tool_type_contract_edges", lambda: self._project_tool_type_contract_edges(known)),
            ("operation_authority_edges", lambda: self._project_operation_authority_edges(known)),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc(limit=3)}")
                logger.exception(
                    "data dictionary lineage projector step %s failed", label
                )
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -- FK constraint edges ------------------------------------------------

    def _project_fk_edges(self, known: dict[str, set[str]]) -> None:
        known_tables = known.get("table", set())
        rows = self._conn.execute(
            """
            SELECT
                con.conname                     AS constraint_name,
                src_tbl.relname                 AS src_table,
                dst_tbl.relname                 AS dst_table,
                src_att.attname                 AS src_column,
                dst_att.attname                 AS dst_column
              FROM pg_constraint con
              JOIN pg_class src_tbl  ON src_tbl.oid = con.conrelid
              JOIN pg_class dst_tbl  ON dst_tbl.oid = con.confrelid
              JOIN pg_namespace ns   ON ns.oid = src_tbl.relnamespace
              JOIN LATERAL unnest(con.conkey, con.confkey)
                   WITH ORDINALITY AS cols(src_attnum, dst_attnum, ord) ON TRUE
              JOIN pg_attribute src_att
                   ON src_att.attrelid = con.conrelid
                  AND src_att.attnum   = cols.src_attnum
              JOIN pg_attribute dst_att
                   ON dst_att.attrelid = con.confrelid
                  AND dst_att.attnum   = cols.dst_attnum
             WHERE con.contype = 'f'
               AND ns.nspname = 'public'
             ORDER BY src_tbl.relname, con.conname, cols.ord
            """
        )
        edges: list[dict[str, Any]] = []
        for r in rows or []:
            src_kind = f"table:{r['src_table']}"
            dst_kind = f"table:{r['dst_table']}"
            if src_kind not in known_tables or dst_kind not in known_tables:
                continue
            edges.append({
                "src_object_kind": src_kind,
                "src_field_path": str(r["src_column"]),
                "dst_object_kind": dst_kind,
                "dst_field_path": str(r["dst_column"]),
                "edge_kind": "references",
                "origin_ref": {
                    "projector": "lineage_fk_edges",
                    "constraint": str(r["constraint_name"]),
                },
                "metadata": {},
            })
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_fk_edges",
            edges=edges,
            source="auto",
        )

    # -- pg_depend view dependency edges -----------------------------------

    def _project_view_dependencies(self, known: dict[str, set[str]]) -> None:
        known_tables = known.get("table", set())
        rows = self._conn.execute(
            """
            SELECT DISTINCT
                base.relname  AS base_table,
                view.relname  AS view_name,
                view.relkind  AS view_kind
              FROM pg_depend d
              JOIN pg_rewrite r ON r.oid = d.objid
              JOIN pg_class view ON view.oid = r.ev_class
              JOIN pg_class base ON base.oid = d.refobjid
              JOIN pg_namespace bn ON bn.oid = base.relnamespace
              JOIN pg_namespace vn ON vn.oid = view.relnamespace
             WHERE d.classid = 'pg_rewrite'::regclass
               AND d.refclassid = 'pg_class'::regclass
               AND d.deptype = 'n'
               AND base.relkind = 'r'
               AND view.relkind IN ('v', 'm')
               AND bn.nspname = 'public'
               AND vn.nspname = 'public'
               AND base.oid <> view.oid
            """
        )
        edges: list[dict[str, Any]] = []
        for r in rows or []:
            src_kind = f"table:{r['base_table']}"
            dst_kind = f"table:{r['view_name']}"
            if src_kind not in known_tables or dst_kind not in known_tables:
                continue
            edges.append({
                "src_object_kind": src_kind,
                "dst_object_kind": dst_kind,
                "edge_kind": "derives_from",
                "origin_ref": {"projector": "lineage_view_deps"},
                "metadata": {"view_kind": str(r["view_kind"] or "")},
            })
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_view_deps",
            edges=edges,
            source="auto",
        )

    # -- dataset_promotions edges ------------------------------------------

    def _project_dataset_promotions(self, known: dict[str, set[str]]) -> None:
        known_datasets = known.get("dataset", set())
        known_object_types = known.get("object_type", set())
        rows = self._conn.execute(
            """
            SELECT DISTINCT specialist_target, dataset_family
              FROM dataset_promotions
             WHERE specialist_target IS NOT NULL
            """
        )
        edges: list[dict[str, Any]] = []
        for r in rows or []:
            specialist = str(r["specialist_target"] or "")
            if not specialist:
                continue
            src_kind = f"dataset:{specialist}"
            dst_kind = f"object_type:{specialist}"
            if src_kind not in known_datasets:
                continue
            if dst_kind not in known_object_types:
                continue
            edges.append({
                "src_object_kind": src_kind,
                "dst_object_kind": dst_kind,
                "edge_kind": "promotes_to",
                "origin_ref": {
                    "projector": "lineage_dataset_promotions",
                    "specialist": specialist,
                },
                "metadata": {"dataset_family": str(r["dataset_family"] or "")},
            })
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_dataset_promotions",
            edges=edges,
            source="auto",
        )

    # -- integration manifest edges ----------------------------------------

    def _project_integration_manifests(self, known: dict[str, set[str]]) -> None:
        from runtime.integration_manifest import load_manifest_report

        report = load_manifest_report()
        if report.errors:
            raise RuntimeError(
                "integration manifest lineage aborted due to malformed manifest(s): "
                + "; ".join(report.errors)
            )
        known_integrations = known.get("integration", set())
        known_tools = known.get("tool", set())
        edges: list[dict[str, Any]] = []
        for manifest in report.manifests:
            src_kind = f"integration:{manifest.id}"
            if src_kind not in known_integrations:
                continue
            seen_tools: set[str] = set()
            for cap in manifest.capabilities or ():
                tool_ref = getattr(cap, "tool", None) or getattr(cap, "binding", None)
                if not tool_ref:
                    continue
                dst_kind = f"tool:{tool_ref}"
                if dst_kind not in known_tools or dst_kind in seen_tools:
                    continue
                seen_tools.add(dst_kind)
                edges.append({
                    "src_object_kind": src_kind,
                    "dst_object_kind": dst_kind,
                    "edge_kind": "references",
                    "origin_ref": {
                        "projector": "lineage_integration_manifests",
                        "manifest": manifest.id,
                    },
                    "metadata": {"action": str(getattr(cap, "action", "") or "")},
                })
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_integration_manifests",
            edges=edges,
            source="auto",
        )

    # -- tool input-schema edges -------------------------------------------

    def _project_tool_schema_refs(self, known: dict[str, set[str]]) -> None:
        try:
            from surfaces.mcp.catalog import get_tool_catalog
            definitions = list(get_tool_catalog().values())
        except Exception:
            return
        known_tables = known.get("table", set())
        known_object_types = known.get("object_type", set())
        edges: list[dict[str, Any]] = []
        for tool in definitions:
            name = getattr(tool, "name", None)
            if not name:
                continue
            src_kind = f"tool:{name}"
            schema = getattr(tool, "input_schema", None) or {}
            refs = _collect_string_values(schema)
            for ref in refs:
                if not isinstance(ref, str):
                    continue
                if f"table:{ref}" in known_tables:
                    edges.append({
                        "src_object_kind": src_kind,
                        "dst_object_kind": f"table:{ref}",
                        "edge_kind": "references",
                        "origin_ref": {
                            "projector": "lineage_tool_schema_refs",
                            "tool": str(name),
                        },
                    })
                elif f"object_type:{ref}" in known_object_types:
                    edges.append({
                        "src_object_kind": src_kind,
                        "dst_object_kind": f"object_type:{ref}",
                        "edge_kind": "references",
                        "origin_ref": {
                            "projector": "lineage_tool_schema_refs",
                            "tool": str(name),
                        },
                    })
        # Dedupe — the same tool may name the same target in multiple places.
        edges = _dedupe_edges(edges)
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_tool_schema_refs",
            edges=edges,
            source="auto",
        )

    # -- tool -> operation (catalog operation_names) -----------------------

    def _project_tool_operation_edges(self, known: dict[str, set[str]]) -> None:
        try:
            from surfaces.mcp.catalog import get_tool_catalog
            definitions = list(get_tool_catalog().values())
        except Exception:
            return
        known_tools = known.get("tool", set())
        known_ops = known.get("command", set()) | known.get("query", set())
        edges: list[dict[str, Any]] = []
        for tool in definitions:
            name = getattr(tool, "name", None)
            if not name:
                continue
            src_kind = f"tool:{name}"
            if src_kind not in known_tools:
                continue
            raw_ops = tool.metadata.get("operation_names")
            if not isinstance(raw_ops, list):
                continue
            for op in raw_ops:
                op_name = str(op).strip()
                if not op_name:
                    continue
                dst_kind = f"operation.{op_name}"
                if dst_kind not in known_ops:
                    continue
                edges.append({
                    "src_object_kind": src_kind,
                    "dst_object_kind": dst_kind,
                    "edge_kind": "dispatches",
                    "origin_ref": {
                        "projector": "lineage_tool_operation_edges",
                        "tool": str(name),
                        "operation_name": op_name,
                    },
                    "metadata": {},
                })
        edges = _dedupe_edges(edges)
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_tool_operation_edges",
            edges=edges,
            source="auto",
        )

    # -- tool -> type_slug (catalog type_contract) -----------------------

    def _project_tool_type_contract_edges(self, known: dict[str, set[str]]) -> None:
        try:
            from surfaces.mcp.catalog import get_tool_catalog
            definitions = list(get_tool_catalog().values())
        except Exception:
            return
        known_tools = known.get("tool", set())
        known_slugs = known.get("object", set())
        edges: list[dict[str, Any]] = []
        for tool in definitions:
            name = getattr(tool, "name", None)
            if not name:
                continue
            src_kind = f"tool:{name}"
            if src_kind not in known_tools:
                continue
            for action, contract in tool.type_contract.items():
                action_tag = str(action).strip()
                for slug in contract.get("consumes", []):
                    slug = str(slug).strip()
                    if not slug:
                        continue
                    dst_kind = f"type_slug:{slug}"
                    if dst_kind not in known_slugs:
                        continue
                    edges.append({
                        "src_object_kind": src_kind,
                        "src_field_path": action_tag,
                        "dst_object_kind": dst_kind,
                        "edge_kind": "consumes",
                        "origin_ref": {
                            "projector": "lineage_tool_type_contract_edges",
                            "tool": str(name),
                            "action": action_tag,
                        },
                        "metadata": {},
                    })
                for slug in contract.get("produces", []):
                    slug = str(slug).strip()
                    if not slug:
                        continue
                    dst_kind = f"type_slug:{slug}"
                    if dst_kind not in known_slugs:
                        continue
                    edges.append({
                        "src_object_kind": src_kind,
                        "src_field_path": action_tag,
                        "dst_object_kind": dst_kind,
                        "edge_kind": "produces",
                        "origin_ref": {
                            "projector": "lineage_tool_type_contract_edges",
                            "tool": str(name),
                            "action": action_tag,
                        },
                        "metadata": {},
                    })
        edges = _dedupe_edges(edges)
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_tool_type_contract_edges",
            edges=edges,
            source="auto",
        )

    # -- operation -> authority domain (catalog) -------------------------

    def _project_operation_authority_edges(self, known: dict[str, set[str]]) -> None:
        known_ops = known.get("command", set()) | known.get("query", set())
        known_defs = known.get("definition", set())
        rows = self._conn.execute(
            """
            SELECT operation_name, authority_domain_ref
              FROM operation_catalog_registry
             WHERE enabled IS TRUE
               AND authority_domain_ref IS NOT NULL
               AND btrim(authority_domain_ref) <> ''
            """,
        )
        edges: list[dict[str, Any]] = []
        for r in rows or []:
            op_name = str(r.get("operation_name") or "").strip()
            domain = str(r.get("authority_domain_ref") or "").strip()
            if not op_name or not domain:
                continue
            src_kind = f"operation.{op_name}"
            if src_kind not in known_ops or domain not in known_defs:
                continue
            edges.append({
                "src_object_kind": src_kind,
                "dst_object_kind": domain,
                "edge_kind": "governed_by",
                "origin_ref": {
                    "projector": "lineage_operation_authority_edges",
                    "operation_name": op_name,
                },
                "metadata": {"authority_domain_ref": domain},
            })
        edges = _dedupe_edges(edges)
        apply_projected_edges(
            self._conn,
            projector_tag="lineage_operation_authority_edges",
            edges=edges,
            source="auto",
        )


# --- helpers --------------------------------------------------------------


def _collect_string_values(obj: Any, *, max_depth: int = 6) -> list[str]:
    """Walk a JSON-shaped dict/list and return distinct string leaves."""
    seen: set[str] = set()
    stack: list[tuple[Any, int]] = [(obj, 0)]
    while stack:
        cur, depth = stack.pop()
        if depth > max_depth:
            continue
        if isinstance(cur, str):
            value = cur.strip()
            if value and " " not in value and len(value) < 120:
                seen.add(value)
        elif isinstance(cur, dict):
            for v in cur.values():
                stack.append((v, depth + 1))
        elif isinstance(cur, (list, tuple)):
            for v in cur:
                stack.append((v, depth + 1))
    return list(seen)


def _dedupe_edges(edges: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for e in edges:
        key = (
            str(e.get("src_object_kind") or ""),
            str(e.get("src_field_path") or ""),
            str(e.get("dst_object_kind") or ""),
            str(e.get("dst_field_path") or ""),
            str(e.get("edge_kind") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


__all__ = ["DataDictionaryLineageProjector"]
