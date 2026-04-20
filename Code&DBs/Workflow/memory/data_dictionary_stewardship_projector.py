"""Projects stewardship rows into data_dictionary_stewardship.

Three auto-layer steps, each writing with its own projector_tag so
`replace_projected_stewards` prunes its own stale rows idempotently:

* **Audit-column publishers** (`stewardship_audit_column_publishers`) —
  for every field whose name is an audit / principal column
  (`created_by`, `filed_by`, `assigned_to`, …), emit an object-level
  `publisher` steward whose id is the column name and whose type is
  `role`. Tells readers: "to find who publishes to this asset, look at
  this column."

* **Namespace owners** (`stewardship_namespace_owners`) — assign a
  service-tier owner to each table based on its name prefix, reflecting
  the subsystem that owns the table in the Praxis codebase.

* **Projector publishers** (`stewardship_projector_publishers`) —
  tables that are (re-)built by a specific HeartbeatModule receive an
  `agent`-type publisher steward whose id is the projector module name.

Operator stewards (source=`operator`) are never touched.
"""

from __future__ import annotations

import logging
import re
import time
import traceback
from typing import Any, Iterable

from runtime.data_dictionary_stewardship import apply_projected_stewards
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


# --- audit-column principal names -----------------------------------------

_AUDIT_COLUMN_NAMES = frozenset({
    "created_by",
    "updated_by",
    "deleted_by",
    "filed_by",
    "submitted_by",
    "assigned_to",
    "owner_id",
    "owned_by",
})


# --- namespace prefix → owner service ------------------------------------
#
# First match wins; order matters (longer/more specific prefixes first).

_NAMESPACE_OWNERS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^data_dictionary(_|$)"),    "data_dictionary_authority"),
    (re.compile(r"^operator_"),               "operator_authority"),
    (re.compile(r"^workflow_"),               "praxis_engine"),
    (re.compile(r"^(praxis_)?heartbeat"),     "heartbeat_runner"),
    (re.compile(r"^bug(s|_)"),                "bug_authority"),
    (re.compile(r"^capability_"),             "capability_authority"),
    (re.compile(r"^cutover_"),                "capability_authority"),
    (re.compile(r"^adapter_"),                "adapter_authority"),
    (re.compile(r"^connector_"),              "connector_authority"),
    (re.compile(r"^conversation"),            "conversation_authority"),
    (re.compile(r"^context_"),                "context_authority"),
    (re.compile(r"^receipt"),                 "receipt_authority"),
    (re.compile(r"^friction"),                "friction_authority"),
    (re.compile(r"^agent_"),                  "agent_authority"),
    (re.compile(r"^constraint"),              "constraint_authority"),
    # Added to close the sensitive-without-owner governance cluster
    # (BUG-EEA0502A / BUG-80C6B62F / BUG-48241FEA / BUG-61D7951E /
    # BUG-A75077EE / BUG-D577C0FA / BUG-F333D3A1 / BUG-E5E236F5 /
    # BUG-5535F587). Each orphan table's name prefix now maps to a named
    # service-tier owner, so the namespace projector assigns an ``owner``
    # steward and the governance scanner stops flagging "no owner".
    (re.compile(r"^provider_"),               "provider_authority"),
    (re.compile(r"^webhook"),                 "webhook_authority"),
    (re.compile(r"^market_"),                 "market_authority"),
    (re.compile(r"^registry_"),               "registry_authority"),
    (re.compile(r"^credential"),              "credential_authority"),
]


# Object kinds outside the ``table:`` namespace (e.g. ``tool:*``,
# ``object_type:*``) that still need explicit owner stewardship. The
# namespace projector cannot derive these from table-name prefixes, so they
# live here as direct ``object_kind -> owner`` assignments. Closes
# BUG-13492A13 (tool:praxis_provider_onboard) and BUG-A745DE65
# (object_type:contact).
_EXPLICIT_OWNERS: dict[str, str] = {
    "tool:praxis_provider_onboard": "provider_authority",
    "object_type:contact":          "data_dictionary_authority",
}


def _namespace_owner(table_name: str) -> str | None:
    for pattern, owner in _NAMESPACE_OWNERS:
        if pattern.match(table_name):
            return owner
    return None


# --- tables explicitly published by a projector module -------------------

_PROJECTOR_PUBLISHERS: dict[str, str] = {
    "data_dictionary_lineage":         "data_dictionary_lineage_projector",
    "data_dictionary_classifications": "data_dictionary_classifications_projector",
    "data_dictionary_quality_rules":   "data_dictionary_quality_projector",
    "data_dictionary_quality_runs":    "data_dictionary_quality_evaluator",
    "data_dictionary_entries":         "data_dictionary_schema_projector",
    "data_dictionary_objects":         "data_dictionary_schema_projector",
    "data_dictionary_stewardship":     "data_dictionary_stewardship_projector",
}


class DataDictionaryStewardshipProjector(HeartbeatModule):
    """Project stewardship rows from name / namespace / catalog heuristics."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_stewardship_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        known_tables = self._known_tables()
        entries = self._load_entries()
        for label, fn in [
            ("audit_column_publishers", lambda: self._project_audit_columns(entries)),
            ("namespace_owners",         lambda: self._project_namespace_owners(known_tables)),
            ("projector_publishers",     lambda: self._project_projector_publishers(known_tables)),
            ("explicit_owners",          lambda: self._project_explicit_owners()),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc(limit=3)}")
                logger.exception(
                    "data dictionary stewardship projector step %s failed", label
                )
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -- inventory loads ---------------------------------------------------

    def _known_tables(self) -> set[str]:
        rows = self._conn.execute(
            """
            SELECT object_kind
              FROM data_dictionary_objects
             WHERE object_kind LIKE 'table:%'
            """
        )
        return {str(r["object_kind"]) for r in rows or []}

    def _load_entries(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT object_kind, field_path, field_kind
              FROM data_dictionary_effective
             WHERE field_path <> ''
            """
        )
        return [dict(r) for r in rows or []]

    # -- audit-column publishers -------------------------------------------

    def _project_audit_columns(self, entries: Iterable[dict[str, Any]]) -> None:
        out: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for e in entries:
            object_kind = str(e.get("object_kind") or "")
            field_path = str(e.get("field_path") or "").strip()
            if not object_kind or field_path not in _AUDIT_COLUMN_NAMES:
                continue
            key = (object_kind, field_path)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "object_kind": object_kind,
                "field_path": "",        # object-level stewardship
                "steward_kind": "publisher",
                "steward_id": field_path,  # column name identifies role
                "steward_type": "role",
                "confidence": 0.7,
                "origin_ref": {
                    "projector": "stewardship_audit_column_publishers",
                    "rule": f"audit_column:{field_path}",
                },
            })
        apply_projected_stewards(
            self._conn,
            projector_tag="stewardship_audit_column_publishers",
            entries=out,
            source="auto",
        )

    # -- namespace prefix owners -------------------------------------------

    def _project_namespace_owners(self, known_tables: Iterable[str]) -> None:
        out: list[dict[str, Any]] = []
        for object_kind in known_tables:
            name = object_kind.split(":", 1)[1] if ":" in object_kind else object_kind
            owner = _namespace_owner(name)
            if not owner:
                continue
            out.append({
                "object_kind": object_kind,
                "field_path": "",
                "steward_kind": "owner",
                "steward_id": owner,
                "steward_type": "service",
                "confidence": 0.6,
                "origin_ref": {
                    "projector": "stewardship_namespace_owners",
                    "rule": f"namespace_prefix:{owner}",
                },
            })
        apply_projected_stewards(
            self._conn,
            projector_tag="stewardship_namespace_owners",
            entries=out,
            source="auto",
        )

    # -- projector-module publishers ---------------------------------------

    def _project_projector_publishers(self, known_tables: Iterable[str]) -> None:
        out: list[dict[str, Any]] = []
        for object_kind in known_tables:
            name = object_kind.split(":", 1)[1] if ":" in object_kind else object_kind
            module = _PROJECTOR_PUBLISHERS.get(name)
            if not module:
                continue
            out.append({
                "object_kind": object_kind,
                "field_path": "",
                "steward_kind": "publisher",
                "steward_id": module,
                "steward_type": "agent",
                "confidence": 0.95,
                "origin_ref": {
                    "projector": "stewardship_projector_publishers",
                    "rule": f"heartbeat_module:{module}",
                },
            })
        apply_projected_stewards(
            self._conn,
            projector_tag="stewardship_projector_publishers",
            entries=out,
            source="auto",
        )

    # -- explicit owners (non-table object kinds) --------------------------

    def _project_explicit_owners(self) -> None:
        """Emit owner stewardship for object kinds outside ``table:*``.

        The namespace projector derives owners from ``table:<prefix>``
        patterns, so ``tool:*`` and ``object_type:*`` objects never get an
        owner from it. ``_EXPLICIT_OWNERS`` holds direct
        ``object_kind -> owner`` mappings for those kinds. Closes
        BUG-13492A13 (tool:praxis_provider_onboard) and BUG-A745DE65
        (object_type:contact).
        """
        out: list[dict[str, Any]] = []
        for object_kind, owner in _EXPLICIT_OWNERS.items():
            out.append({
                "object_kind": object_kind,
                "field_path": "",
                "steward_kind": "owner",
                "steward_id": owner,
                "steward_type": "service",
                "confidence": 0.7,
                "origin_ref": {
                    "projector": "stewardship_explicit_owners",
                    "rule": f"explicit:{object_kind}",
                },
            })
        apply_projected_stewards(
            self._conn,
            projector_tag="stewardship_explicit_owners",
            entries=out,
            source="auto",
        )


__all__ = [
    "DataDictionaryStewardshipProjector",
    "_namespace_owner",
    "_AUDIT_COLUMN_NAMES",
    "_PROJECTOR_PUBLISHERS",
    "_EXPLICIT_OWNERS",
]
