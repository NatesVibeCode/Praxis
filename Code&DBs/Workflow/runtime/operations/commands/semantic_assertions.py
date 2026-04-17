from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class RegisterSemanticPredicateCommand(BaseModel):
    predicate_slug: str
    subject_kind_allowlist: tuple[str, ...]
    object_kind_allowlist: tuple[str, ...]
    cardinality_mode: str = "many"
    predicate_status: str = "active"
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RecordSemanticAssertionCommand(BaseModel):
    predicate_slug: str
    subject_kind: str
    subject_ref: str
    object_kind: str
    object_ref: str
    qualifiers_json: dict[str, Any] | None = None
    source_kind: str
    source_ref: str
    evidence_ref: str | None = None
    bound_decision_id: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    assertion_status: str = "active"
    semantic_assertion_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RetractSemanticAssertionCommand(BaseModel):
    semantic_assertion_id: str
    retracted_at: datetime | None = None
    updated_at: datetime | None = None


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


async def handle_register_semantic_predicate(
    command: RegisterSemanticPredicateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.semantic_assertions import SemanticAssertionFrontdoor

    return await SemanticAssertionFrontdoor().register_predicate_async(
        predicate_slug=command.predicate_slug,
        subject_kind_allowlist=command.subject_kind_allowlist,
        object_kind_allowlist=command.object_kind_allowlist,
        cardinality_mode=command.cardinality_mode,
        predicate_status=command.predicate_status,
        description=command.description,
        created_at=command.created_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


async def handle_record_semantic_assertion(
    command: RecordSemanticAssertionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.semantic_assertions import SemanticAssertionFrontdoor

    return await SemanticAssertionFrontdoor().record_assertion_async(
        predicate_slug=command.predicate_slug,
        subject_kind=command.subject_kind,
        subject_ref=command.subject_ref,
        object_kind=command.object_kind,
        object_ref=command.object_ref,
        qualifiers_json=command.qualifiers_json,
        source_kind=command.source_kind,
        source_ref=command.source_ref,
        evidence_ref=command.evidence_ref,
        bound_decision_id=command.bound_decision_id,
        valid_from=command.valid_from,
        valid_to=command.valid_to,
        assertion_status=command.assertion_status,
        semantic_assertion_id=command.semantic_assertion_id,
        created_at=command.created_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


async def handle_retract_semantic_assertion(
    command: RetractSemanticAssertionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.semantic_assertions import SemanticAssertionFrontdoor

    return await SemanticAssertionFrontdoor().retract_assertion_async(
        semantic_assertion_id=command.semantic_assertion_id,
        retracted_at=command.retracted_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


__all__ = [
    "RecordSemanticAssertionCommand",
    "RegisterSemanticPredicateCommand",
    "RetractSemanticAssertionCommand",
    "handle_record_semantic_assertion",
    "handle_register_semantic_predicate",
    "handle_retract_semantic_assertion",
]
