"""Gateway-dispatched command wrappers for the semantic predicate catalog.

These let `semantic_predicate.record` flow through
`operation_catalog_gateway.execute_operation_*` so the gateway records an
authority operation receipt for every catalog mutation.

The underlying business logic lives in `runtime.semantic_predicate_authority`.
This module is the gateway-friendly seam (Pydantic input + (command,
subsystems) handler).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RecordSemanticPredicateCommand(BaseModel):
    predicate_slug: str
    predicate_kind: str
    applies_to_kind: str
    summary: str
    rationale: str
    decision_ref: str
    applies_to_ref: str | None = None
    validator_ref: str | None = None
    propagation_policy: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


def handle_record_semantic_predicate(
    command: RecordSemanticPredicateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Dispatch ``runtime.semantic_predicate_authority.record_predicate`` through
    the catalog gateway.

    The gateway wraps this call in an authority operation receipt; the
    semantic_predicate catalog itself is therefore queryable through the same
    receipt-backed surfaces it governs.
    """

    from runtime.semantic_predicate_authority import (
        SemanticPredicateAuthorityError,
        record_predicate,
    )

    conn = subsystems.get_pg_conn()
    try:
        return record_predicate(
            conn,
            predicate_slug=command.predicate_slug,
            predicate_kind=command.predicate_kind,
            applies_to_kind=command.applies_to_kind,
            applies_to_ref=command.applies_to_ref,
            summary=command.summary,
            rationale=command.rationale,
            validator_ref=command.validator_ref,
            propagation_policy=command.propagation_policy,
            decision_ref=command.decision_ref,
            metadata=command.metadata,
            enabled=command.enabled,
        )
    except SemanticPredicateAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }
