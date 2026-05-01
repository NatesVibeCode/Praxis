"""CQRS queries for Workflow Context authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.workflow_context import WorkflowContextError, guardrail_check
from storage.postgres.workflow_context_repository import (
    list_context_packs,
    load_context_pack,
)


TruthState = Literal[
    "none",
    "inferred",
    "synthetic",
    "documented",
    "anonymized_operational",
    "schema_bound",
    "observed",
    "verified",
    "promoted",
    "stale",
    "contradicted",
    "blocked",
]


class QueryWorkflowContextRead(BaseModel):
    """Read Workflow Context packs, entities, bindings, transitions, and packets."""

    context_ref: str | None = None
    workflow_ref: str | None = None
    truth_state: TruthState | None = None
    include_entities: bool = True
    include_bindings: bool = True
    include_transitions: bool = True
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("context_ref", "workflow_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("context filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _require_filter(self) -> "QueryWorkflowContextRead":
        if not (self.context_ref or self.workflow_ref or self.truth_state):
            raise ValueError("provide context_ref, workflow_ref, or truth_state")
        return self


class QueryWorkflowContextGuardrailCheck(BaseModel):
    """Read allowed next actions and blockers for Workflow Context transitions."""

    context_ref: str
    target_truth_state: TruthState | None = None
    risk_disposition: str | None = None
    requested_action: str | None = None

    @field_validator("context_ref", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("context_ref must be a non-empty string")
        return value.strip()

    @field_validator("risk_disposition", "requested_action", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional guardrail fields must be non-empty strings when supplied")
        return value.strip()


def handle_workflow_context_read(
    query: QueryWorkflowContextRead,
    subsystems: Any,
) -> dict[str, Any]:
    """Read Workflow Context authority records."""

    conn = subsystems.get_pg_conn()
    if query.context_ref:
        context_pack = load_context_pack(
            conn,
            context_ref=query.context_ref,
            include_entities=query.include_entities,
            include_bindings=query.include_bindings,
            include_transitions=query.include_transitions,
        )
        return {
            "ok": context_pack is not None,
            "operation": "workflow_context_read",
            "context_ref": query.context_ref,
            "context_pack": context_pack,
            "error_code": None if context_pack is not None else "workflow_context.context_not_found",
        }
    packs = list_context_packs(
        conn,
        workflow_ref=query.workflow_ref,
        truth_state=query.truth_state,
        limit=query.limit,
    )
    if query.include_entities or query.include_bindings or query.include_transitions:
        packs = [
            load_context_pack(
                conn,
                context_ref=str(pack["context_ref"]),
                include_entities=query.include_entities,
                include_bindings=query.include_bindings,
                include_transitions=query.include_transitions,
            )
            or pack
            for pack in packs
        ]
    return {
        "ok": True,
        "operation": "workflow_context_read",
        "count": len(packs),
        "context_packs": packs,
        "filters": {
            "workflow_ref": query.workflow_ref,
            "truth_state": query.truth_state,
            "limit": query.limit,
        },
    }


def handle_workflow_context_guardrail_check(
    query: QueryWorkflowContextGuardrailCheck,
    subsystems: Any,
) -> dict[str, Any]:
    """Return backend-enforced guardrail state for one context pack."""

    context_pack = load_context_pack(
        subsystems.get_pg_conn(),
        context_ref=query.context_ref,
        include_entities=True,
        include_bindings=True,
        include_transitions=False,
    )
    if context_pack is None:
        return {
            "ok": False,
            "operation": "workflow_context_guardrail_check",
            "error_code": "workflow_context.context_not_found",
            "context_ref": query.context_ref,
        }
    try:
        guardrail = guardrail_check(
            context_pack,
            target_truth_state=query.target_truth_state,
            risk_disposition=query.risk_disposition,
            requested_action=query.requested_action,
        )
    except WorkflowContextError as exc:
        return {
            "ok": False,
            "operation": "workflow_context_guardrail_check",
            "error_code": exc.reason_code,
            "error": str(exc),
            "details": exc.details,
        }
    return {
        "ok": True,
        "operation": "workflow_context_guardrail_check",
        "context_ref": query.context_ref,
        "guardrail": guardrail,
        "truth_state": context_pack.get("truth_state"),
        "confidence": context_pack.get("confidence"),
        "review_packet": context_pack.get("review_packet"),
    }


__all__ = [
    "QueryWorkflowContextGuardrailCheck",
    "QueryWorkflowContextRead",
    "handle_workflow_context_guardrail_check",
    "handle_workflow_context_read",
]
