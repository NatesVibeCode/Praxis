"""Projects auto-layer quality rules into data_dictionary_quality_rules.

Walks Postgres schema metadata and emits declarative check rules:

* **NOT NULL constraints** (`quality_not_null_from_pg_attribute`) —
  for every NOT NULL column, emit a `not_null` rule.
* **UNIQUE indexes** (`quality_unique_from_pg_index`) — for every unique
  single-column index (including primary keys), emit a `unique` rule.
* **Foreign keys** (`quality_referential_from_pg_constraint`) — for every
  single-column FK, emit a `referential` rule that points at the parent
  (table, column).

Operator rules in the `operator` source layer are never touched. Each
step writes with origin_ref.projector = <step_tag> so
`replace_projected_rules` can idempotently prune stale rows.
"""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

from runtime.data_dictionary_quality import apply_projected_rules
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


def _known_tables(conn: Any) -> set[str]:
    rows = conn.execute(
        "SELECT object_kind FROM data_dictionary_objects WHERE category = 'table'"
    )
    return {str(r["object_kind"]) for r in rows or []}


class DataDictionaryQualityProjector(HeartbeatModule):
    """Project auto-layer quality rules from schema metadata."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_quality_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        known = _known_tables(self._conn)
        for label, fn in [
            ("not_null", lambda: self._project_not_null(known)),
            ("unique", lambda: self._project_unique(known)),
            ("referential", lambda: self._project_referential(known)),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc(limit=3)}")
                logger.exception(
                    "data dictionary quality projector step %s failed", label
                )
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -- NOT NULL ---------------------------------------------------------

    def _project_not_null(self, known: set[str]) -> None:
        rows = self._conn.execute(
            """
            SELECT c.relname AS table_name, a.attname AS column_name
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public'
               AND c.relkind = 'r'
               AND a.attnum > 0
               AND NOT a.attisdropped
               AND a.attnotnull = true
             ORDER BY c.relname, a.attnum
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows or []:
            object_kind = f"table:{r['table_name']}"
            if object_kind not in known:
                continue
            out.append({
                "object_kind": object_kind,
                "field_path": str(r["column_name"]),
                "rule_kind": "not_null",
                "expression": {},
                "severity": "error",
                "description": "Column is NOT NULL in Postgres schema.",
                "enabled": True,
                "origin_ref": {"projector": "quality_not_null_from_pg_attribute"},
                "metadata": {},
            })
        apply_projected_rules(
            self._conn,
            projector_tag="quality_not_null_from_pg_attribute",
            rules=out,
            source="auto",
        )

    # -- UNIQUE -----------------------------------------------------------

    def _project_unique(self, known: set[str]) -> None:
        rows = self._conn.execute(
            """
            SELECT c.relname AS table_name, a.attname AS column_name
              FROM pg_index i
              JOIN pg_class c ON c.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_attribute a
                   ON a.attrelid = i.indrelid
                  AND a.attnum = ANY(i.indkey)
             WHERE i.indisunique = true
               AND c.relkind = 'r'
               AND n.nspname = 'public'
               AND array_length(i.indkey, 1) = 1
               AND NOT a.attisdropped
             ORDER BY c.relname, a.attname
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows or []:
            object_kind = f"table:{r['table_name']}"
            if object_kind not in known:
                continue
            out.append({
                "object_kind": object_kind,
                "field_path": str(r["column_name"]),
                "rule_kind": "unique",
                "expression": {},
                "severity": "error",
                "description": "Column has a single-column unique index in Postgres.",
                "enabled": True,
                "origin_ref": {"projector": "quality_unique_from_pg_index"},
                "metadata": {},
            })
        apply_projected_rules(
            self._conn,
            projector_tag="quality_unique_from_pg_index",
            rules=out,
            source="auto",
        )

    # -- FK referential ---------------------------------------------------

    def _project_referential(self, known: set[str]) -> None:
        rows = self._conn.execute(
            """
            SELECT
                src_tbl.relname        AS src_table,
                dst_tbl.relname        AS dst_table,
                src_att.attname        AS src_column,
                dst_att.attname        AS dst_column
              FROM pg_constraint con
              JOIN pg_class src_tbl  ON src_tbl.oid = con.conrelid
              JOIN pg_class dst_tbl  ON dst_tbl.oid = con.confrelid
              JOIN pg_namespace ns   ON ns.oid = src_tbl.relnamespace
              JOIN LATERAL unnest(con.conkey, con.confkey)
                   AS cols(src_attnum, dst_attnum) ON TRUE
              JOIN pg_attribute src_att
                   ON src_att.attrelid = con.conrelid
                  AND src_att.attnum   = cols.src_attnum
              JOIN pg_attribute dst_att
                   ON dst_att.attrelid = con.confrelid
                  AND dst_att.attnum   = cols.dst_attnum
             WHERE con.contype = 'f'
               AND ns.nspname = 'public'
               AND array_length(con.conkey, 1) = 1
             ORDER BY src_tbl.relname, src_att.attname
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows or []:
            object_kind = f"table:{r['src_table']}"
            if object_kind not in known:
                continue
            out.append({
                "object_kind": object_kind,
                "field_path": str(r["src_column"]),
                "rule_kind": "referential",
                "expression": {
                    "references": {
                        "table": str(r["dst_table"]),
                        "column": str(r["dst_column"]),
                    },
                },
                "severity": "error",
                "description": (
                    f"Foreign key: {r['src_table']}.{r['src_column']} → "
                    f"{r['dst_table']}.{r['dst_column']}."
                ),
                "enabled": True,
                "origin_ref": {"projector": "quality_referential_from_pg_constraint"},
                "metadata": {},
            })
        apply_projected_rules(
            self._conn,
            projector_tag="quality_referential_from_pg_constraint",
            rules=out,
            source="auto",
        )


__all__ = ["DataDictionaryQualityProjector"]
