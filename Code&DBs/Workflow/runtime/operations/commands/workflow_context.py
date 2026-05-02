"""CQRS commands for Workflow Context authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from runtime.workflow_context import (
    WorkflowContextError,
    build_binding,
    build_review_packet,
    compile_workflow_context,
    transition_context_pack,
)
from storage.postgres.workflow_context_repository import (
    find_context_entity,
    load_context_pack,
    persist_context_binding,
    persist_context_pack,
    persist_context_transition,
)


ContextMode = Literal["standalone", "inferred", "synthetic", "bound", "hybrid"]
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


class CompileWorkflowContextCommand(BaseModel):
    """Compile intent and optional graph into durable Workflow Context."""

    intent: str = Field(description="Workflow intent to infer context from.")
    workflow_ref: str | None = Field(default=None)
    graph: dict[str, Any] = Field(default_factory=dict)
    context_mode: ContextMode = "inferred"
    scenario_pack_refs: list[str] = Field(default_factory=list)
    seed: str | None = None
    source_prompt_ref: str | None = None
    evidence: list[dict[str, Any] | str] = Field(default_factory=list)
    unknown_mutator_risk: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("intent", mode="before")
    @classmethod
    def _normalize_intent(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("intent must be a non-empty string")
        return value.strip()

    @field_validator("workflow_ref", "seed", "source_prompt_ref", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator("graph", "metadata", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("graph and metadata must be JSON objects")

    @field_validator("scenario_pack_refs", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("scenario_pack_refs must be a list")
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("evidence", mode="before")
    @classmethod
    def _normalize_evidence_list(cls, value: object) -> list[dict[str, Any] | str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("evidence must be a list")
        if not all(isinstance(item, (dict, str)) for item in value):
            raise ValueError("evidence entries must be JSON objects or refs")
        return [dict(item) if isinstance(item, dict) else str(item) for item in value]


class MaterializeWorkflowContextCommand(CompileWorkflowContextCommand):
    """Materialize intent and optional graph into durable Workflow Context."""


class TransitionWorkflowContextCommand(BaseModel):
    """Transition a context pack truth state through guardrail policy."""

    context_ref: str
    to_truth_state: TruthState
    transition_reason: str
    evidence: list[dict[str, Any] | str] = Field(default_factory=list)
    risk_disposition: str | None = None
    decision_ref: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("context_ref", "transition_reason", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("context_ref and transition_reason must be non-empty strings")
        return value.strip()

    @field_validator("risk_disposition", "decision_ref", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator("evidence", mode="before")
    @classmethod
    def _normalize_evidence_list(cls, value: object) -> list[dict[str, Any] | str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("evidence must be a list")
        if not all(isinstance(item, (dict, str)) for item in value):
            raise ValueError("evidence entries must be JSON objects or refs")
        return [dict(item) if isinstance(item, dict) else str(item) for item in value]


class BindWorkflowContextCommand(BaseModel):
    """Bind one Workflow Context entity to Object Truth or an authority ref."""

    context_ref: str
    entity_ref: str
    target_ref: str
    target_authority_domain: str = "authority.object_truth"
    evidence: list[dict[str, Any] | str] = Field(default_factory=list)
    risk_level: Literal["low", "medium", "high", "critical"] = "medium"
    binding_state: Literal["proposed", "accepted", "rejected", "revoked"] = "proposed"
    reversible: bool = True
    reviewed_by_ref: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("context_ref", "entity_ref", "target_ref", "target_authority_domain", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("binding refs must be non-empty strings")
        return value.strip()

    @field_validator("reviewed_by_ref", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator("evidence", mode="before")
    @classmethod
    def _normalize_evidence_list(cls, value: object) -> list[dict[str, Any] | str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("evidence must be a list")
        if not all(isinstance(item, (dict, str)) for item in value):
            raise ValueError("evidence entries must be JSON objects or refs")
        return [dict(item) if isinstance(item, dict) else str(item) for item in value]


def handle_workflow_context_compile(
    command: CompileWorkflowContextCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Compile and persist Workflow Context through authority.workflow_context."""

    try:
        context_pack = compile_workflow_context(
            intent=command.intent,
            workflow_ref=command.workflow_ref,
            graph=command.graph,
            context_mode=command.context_mode,
            scenario_pack_refs=command.scenario_pack_refs,
            seed=command.seed,
            source_prompt_ref=command.source_prompt_ref,
            evidence=command.evidence,
            unknown_mutator_risk=command.unknown_mutator_risk,
            metadata=command.metadata,
        )
        persisted = persist_context_pack(
            subsystems.get_pg_conn(),
            context_pack=context_pack,
            observed_by_ref=command.observed_by_ref,
            source_ref=command.source_ref,
        )
    except WorkflowContextError as exc:
        return _domain_error("workflow_context_compile", exc)
    event_payload = {
        "context_ref": persisted["context_ref"],
        "workflow_ref": persisted.get("workflow_ref"),
        "context_mode": persisted["context_mode"],
        "truth_state": persisted["truth_state"],
        "confidence_score": persisted["confidence_score"],
        "entity_count": len(persisted.get("entities") or []),
        "scenario_pack_refs": persisted.get("scenario_pack_refs") or [],
    }
    return {
        "ok": True,
        "operation": "workflow_context_compile",
        "context_ref": persisted["context_ref"],
        "context_pack": persisted,
        "event_payload": event_payload,
    }


def handle_workflow_context_materialize(
    command: MaterializeWorkflowContextCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Canonical materialize-named handler for Workflow Context authority."""

    payload = handle_workflow_context_compile(command, subsystems)
    if isinstance(payload, dict) and payload.get("operation") == "workflow_context_compile":
        payload = {**payload, "operation": "workflow_context_materialize"}
    return payload


def handle_workflow_context_transition(
    command: TransitionWorkflowContextCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Persist one guarded truth-state transition."""

    conn = subsystems.get_pg_conn()
    current = load_context_pack(conn, context_ref=command.context_ref)
    if current is None:
        return {
            "ok": False,
            "operation": "workflow_context_transition",
            "error_code": "workflow_context.context_not_found",
            "context_ref": command.context_ref,
        }
    try:
        updated, transition = transition_context_pack(
            current,
            to_truth_state=command.to_truth_state,
            transition_reason=command.transition_reason,
            evidence=command.evidence,
            risk_disposition=command.risk_disposition,
            decision_ref=command.decision_ref,
        )
        persisted = persist_context_transition(
            conn,
            context_pack=updated,
            transition=transition,
            observed_by_ref=command.observed_by_ref,
            source_ref=command.source_ref,
        )
    except WorkflowContextError as exc:
        return _domain_error("workflow_context_transition", exc)
    event_payload = {
        "context_ref": command.context_ref,
        "transition_ref": persisted["transition"]["transition_ref"],
        "from_truth_state": persisted["transition"]["from_truth_state"],
        "to_truth_state": persisted["transition"]["to_truth_state"],
        "confidence_score": persisted["context_pack"]["confidence_score"],
    }
    return {
        "ok": True,
        "operation": "workflow_context_transition",
        "context_ref": command.context_ref,
        "context_pack": persisted["context_pack"],
        "transition": persisted["transition"],
        "event_payload": event_payload,
    }


def handle_workflow_context_bind(
    command: BindWorkflowContextCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Persist one context-to-authority binding proposal or accepted binding."""

    conn = subsystems.get_pg_conn()
    current = load_context_pack(conn, context_ref=command.context_ref)
    if current is None:
        return {
            "ok": False,
            "operation": "workflow_context_bind",
            "error_code": "workflow_context.context_not_found",
            "context_ref": command.context_ref,
        }
    entity = find_context_entity(
        conn,
        context_ref=command.context_ref,
        entity_ref=command.entity_ref,
    )
    if entity is None:
        return {
            "ok": False,
            "operation": "workflow_context_bind",
            "error_code": "workflow_context.entity_not_found",
            "context_ref": command.context_ref,
            "entity_ref": command.entity_ref,
        }
    try:
        binding = build_binding(
            pack=current,
            entity=entity,
            target_ref=command.target_ref,
            target_authority_domain=command.target_authority_domain,
            evidence=command.evidence,
            risk_level=command.risk_level,
            binding_state=command.binding_state,
            reversible=command.reversible,
            reviewed_by_ref=command.reviewed_by_ref,
        )
        current = {
            **current,
            "review_packet": build_review_packet(
                context_ref=command.context_ref,
                truth_state=str(current.get("truth_state") or "none"),
                confidence=dict(current.get("confidence") or {}),
                blockers=list(current.get("blockers") or []),
                guardrail=binding.get("guardrail"),
                binding=binding,
            ),
        }
        persisted = persist_context_binding(
            conn,
            binding=binding,
            context_pack=current,
            observed_by_ref=command.observed_by_ref,
            source_ref=command.source_ref,
        )
    except WorkflowContextError as exc:
        return _domain_error("workflow_context_bind", exc)
    event_payload = {
        "context_ref": command.context_ref,
        "binding_ref": persisted["binding"]["binding_ref"],
        "entity_ref": command.entity_ref,
        "target_authority_domain": command.target_authority_domain,
        "target_ref": command.target_ref,
        "binding_state": persisted["binding"]["binding_state"],
        "requires_review": persisted["binding"]["requires_review"],
    }
    return {
        "ok": True,
        "operation": "workflow_context_bind",
        "context_ref": command.context_ref,
        "binding": persisted["binding"],
        "context_pack": persisted["context_pack"],
        "review_packet": current.get("review_packet"),
        "event_payload": event_payload,
    }


def _domain_error(operation: str, exc: WorkflowContextError) -> dict[str, Any]:
    return {
        "ok": False,
        "operation": operation,
        "error_code": exc.reason_code,
        "error": str(exc),
        "details": exc.details,
    }


__all__ = [
    "BindWorkflowContextCommand",
    "CompileWorkflowContextCommand",
    "MaterializeWorkflowContextCommand",
    "TransitionWorkflowContextCommand",
    "handle_workflow_context_bind",
    "handle_workflow_context_compile",
    "handle_workflow_context_materialize",
    "handle_workflow_context_transition",
]
