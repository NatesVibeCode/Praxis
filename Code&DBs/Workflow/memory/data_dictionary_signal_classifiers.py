"""Cheap SQL-driven auto-tag projectors.

Each class here is an independent HeartbeatModule that emits one tag
category into `data_dictionary_classifications` at source=auto. They
share this file because the implementation shape is identical: one
SELECT, one UNNEST into apply_projected_classifications.

Current projectors:

* DecisionBackedClassifier
    Tag: decision_backed = governed
    Signal: the object_kind appears in either operator_decisions.decision_key
    / decision_scope_ref, or in bugs.decision_ref. These are tables the
    operator has explicitly written policy about.

* ReceiptActiveClassifier
    Tag: receipt_active = active
    Signal: the object_kind (stripped of the 'table:' prefix) appears as a
    substring of receipts.inputs / outputs / decision_refs in recent runs.
    These are tables actually involved in live workflow execution.

* AuditColumnsClassifier
    Tag: schema_shape = has_audit_columns
    Signal: the table has at least two of (created_at, updated_at,
    created_by, updated_by, deleted_at, filed_by). Traceability signal.

Every projector is idempotent — re-running replaces its own rows via
projector_tag in origin_ref. Operator-layer tags are never touched.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterable

from runtime.data_dictionary_classifications import apply_projected_classifications
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# decision_backed — governed by an operator decision or cited in a bug
# ---------------------------------------------------------------------------

class DecisionBackedClassifier(HeartbeatModule):
    """Tag tables that appear in operator_decisions or bug decision_refs."""

    PROJECTOR_TAG = "decision_backed_classifier"

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_decision_backed_classifier"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            # Collect every table name mentioned anywhere in operator
            # decisions or bug decision_refs. Decision keys and refs are
            # structured like "architecture-policy::foo::bar" so we do a
            # substring match against the table name.
            rows = self._conn.execute(
                """
                SELECT object_kind
                FROM data_dictionary_objects
                WHERE object_kind LIKE 'table:%'
                """
            ) or []
            all_tables = [str(r["object_kind"]) for r in rows]

            hits: dict[str, dict[str, Any]] = {}
            for object_kind in all_tables:
                table_name = object_kind[len("table:"):]
                if not table_name:
                    continue

                decisions = self._conn.execute(
                    """
                    SELECT COUNT(*)::int AS c
                    FROM operator_decisions
                    WHERE decision_key LIKE '%' || $1 || '%'
                       OR (decision_scope_ref IS NOT NULL
                           AND decision_scope_ref LIKE '%' || $1 || '%')
                    """,
                    table_name,
                )
                d_count = int(decisions[0]["c"]) if decisions else 0

                bug_refs = self._conn.execute(
                    """
                    SELECT COUNT(DISTINCT decision_ref)::int AS c
                    FROM bugs
                    WHERE decision_ref LIKE '%' || $1 || '%'
                      AND decision_ref <> ''
                    """,
                    table_name,
                )
                b_count = int(bug_refs[0]["c"]) if bug_refs else 0

                total = d_count + b_count
                if total == 0:
                    continue

                hits[object_kind] = {
                    "object_kind": object_kind,
                    "field_path": "",
                    "tag_key": "decision_backed",
                    "tag_value": "governed",
                    "confidence": 0.8,
                    "origin_ref": {
                        "projector": self.PROJECTOR_TAG,
                        "operator_decisions": d_count,
                        "bug_decision_refs": b_count,
                    },
                }

            apply_projected_classifications(
                self._conn,
                projector_tag=self.PROJECTOR_TAG,
                entries=list(hits.values()),
                source="auto",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


# ---------------------------------------------------------------------------
# receipt_active — referenced in recent workflow receipts
# ---------------------------------------------------------------------------

class ReceiptActiveClassifier(HeartbeatModule):
    """Tag tables that appear in recent workflow receipts."""

    PROJECTOR_TAG = "receipt_active_classifier"

    def __init__(self, conn: Any, *, lookback_days: int = 30) -> None:
        self._conn = conn
        self._lookback_days = int(lookback_days)

    @property
    def name(self) -> str:
        return "data_dictionary_receipt_active_classifier"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            rows = self._conn.execute(
                """
                SELECT object_kind
                FROM data_dictionary_objects
                WHERE object_kind LIKE 'table:%'
                """
            ) or []
            all_tables = [str(r["object_kind"]) for r in rows]

            hits: list[dict[str, Any]] = []
            for object_kind in all_tables:
                table_name = object_kind[len("table:"):]
                if not table_name:
                    continue

                # Substring-match against receipt jsonb payloads within
                # the lookback window. Using `::text LIKE '%name%'` is
                # coarse but cheap; good enough for a "has this table
                # shown up in real runs lately" signal.
                cnt_rows = self._conn.execute(
                    f"""
                    SELECT COUNT(*)::int AS c
                    FROM receipts
                    WHERE started_at > now() - interval '{self._lookback_days} days'
                      AND (
                        inputs::text        LIKE '%' || $1 || '%'
                        OR outputs::text    LIKE '%' || $1 || '%'
                        OR decision_refs::text LIKE '%' || $1 || '%'
                      )
                    """,
                    table_name,
                )
                cnt = int(cnt_rows[0]["c"]) if cnt_rows else 0
                if cnt == 0:
                    continue

                hits.append({
                    "object_kind": object_kind,
                    "field_path": "",
                    "tag_key": "receipt_active",
                    "tag_value": "active",
                    "confidence": 0.7,
                    "origin_ref": {
                        "projector": self.PROJECTOR_TAG,
                        "recent_receipts": cnt,
                        "lookback_days": self._lookback_days,
                    },
                })

            apply_projected_classifications(
                self._conn,
                projector_tag=self.PROJECTOR_TAG,
                entries=hits,
                source="auto",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


# ---------------------------------------------------------------------------
# has_audit_columns — schema-shape signal
# ---------------------------------------------------------------------------

_AUDIT_COLUMN_NAMES = frozenset({
    "created_at", "updated_at", "deleted_at",
    "created_by", "updated_by", "deleted_by",
    "filed_by", "submitted_by", "assigned_to",
})


class AuditColumnsClassifier(HeartbeatModule):
    """Tag tables that carry at least two standard audit columns."""

    PROJECTOR_TAG = "audit_columns_classifier"

    def __init__(self, conn: Any, *, min_matches: int = 2) -> None:
        self._conn = conn
        self._min_matches = max(1, int(min_matches))

    @property
    def name(self) -> str:
        return "data_dictionary_audit_columns_classifier"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            rows = self._conn.execute(
                """
                SELECT object_kind, field_path
                FROM data_dictionary_effective
                WHERE object_kind LIKE 'table:%'
                  AND field_path <> ''
                """
            ) or []

            # Group fields by table
            by_table: dict[str, set[str]] = {}
            for r in rows:
                ok = str(r["object_kind"])
                fp = str(r["field_path"])
                if fp in _AUDIT_COLUMN_NAMES:
                    by_table.setdefault(ok, set()).add(fp)

            hits: list[dict[str, Any]] = []
            for object_kind, matched in by_table.items():
                if len(matched) < self._min_matches:
                    continue
                hits.append({
                    "object_kind": object_kind,
                    "field_path": "",
                    "tag_key": "schema_shape",
                    "tag_value": "has_audit_columns",
                    "confidence": 0.9,
                    "origin_ref": {
                        "projector": self.PROJECTOR_TAG,
                        "matched_columns": sorted(matched),
                    },
                })

            apply_projected_classifications(
                self._conn,
                projector_tag=self.PROJECTOR_TAG,
                entries=hits,
                source="auto",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = [
    "AuditColumnsClassifier",
    "DecisionBackedClassifier",
    "ReceiptActiveClassifier",
]
