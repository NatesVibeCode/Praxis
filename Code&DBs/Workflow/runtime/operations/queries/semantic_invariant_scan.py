"""Gateway-dispatched query wrappers for the semantic invariant scanner.

Auto-mounts the scanner at REST/MCP/CLI through the operation catalog.  One
declaration in operation_catalog_registry surfaces ``semantic_invariant.scan``
on every surface, so any operator can run::

    praxis workflow tools call semantic_invariant.scan --input-json '{}' --yes

or hit ``GET /api/semantic-invariants/scan`` and get the live findings list.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel


# Resolve the workflow root from this file's location: runtime/operations/queries/.. -> Workflow root.
_WORKFLOW_ROOT = Path(__file__).resolve().parents[3]


class ScanSemanticInvariantsQuery(BaseModel):
    predicate_slug: str | None = None
    workflow_root: str | None = None


def _decode_predicate_row(row: dict[str, Any]) -> dict[str, Any]:
    record = dict(row)
    policy = record.get("propagation_policy")
    if isinstance(policy, str):
        try:
            record["propagation_policy"] = json.loads(policy)
        except json.JSONDecodeError:
            record["propagation_policy"] = {}
    elif not isinstance(policy, dict):
        record["propagation_policy"] = {}
    return record


def handle_scan_semantic_invariants(
    command: ScanSemanticInvariantsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    """Load enabled invariant predicates and run the scanner against the live tree.

    Returns ``{predicate_count, findings_count, findings, predicates_scanned}``
    so callers can render the inventory without re-querying.
    """

    from runtime.semantic_invariant_scanner import scan_invariant_predicate

    conn = subsystems.get_pg_conn()
    if command.predicate_slug:
        rows = conn.execute(
            """
            SELECT predicate_slug, predicate_kind, applies_to_kind, applies_to_ref,
                   summary, propagation_policy, decision_ref
              FROM semantic_predicate_catalog
             WHERE enabled = TRUE
               AND predicate_kind = 'invariant'
               AND predicate_slug = $1
             ORDER BY predicate_slug
            """,
            command.predicate_slug,
        )
    else:
        rows = conn.execute(
            """
            SELECT predicate_slug, predicate_kind, applies_to_kind, applies_to_ref,
                   summary, propagation_policy, decision_ref
              FROM semantic_predicate_catalog
             WHERE enabled = TRUE
               AND predicate_kind = 'invariant'
             ORDER BY predicate_slug
            """,
        )

    predicates = [_decode_predicate_row(row) for row in rows or []]
    workflow_root = (
        Path(command.workflow_root).resolve() if command.workflow_root else _WORKFLOW_ROOT
    )

    findings: list[dict[str, Any]] = []
    scanned: list[str] = []
    for predicate in predicates:
        slug = predicate.get("predicate_slug") or ""
        scanned.append(slug)
        findings.extend(
            scan_invariant_predicate(
                predicate=predicate,
                workflow_root=workflow_root,
            )
        )

    return {
        "predicate_count": len(predicates),
        "findings_count": len(findings),
        "predicates_scanned": scanned,
        "findings": findings,
        "workflow_root": str(workflow_root),
    }
