"""Gateway-dispatched command wrapper for the primitive_catalog."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RecordPrimitiveCommand(BaseModel):
    primitive_slug: str
    primitive_kind: str
    summary: str
    rationale: str
    decision_ref: str
    spec: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[Any] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


def handle_record_primitive(
    command: RecordPrimitiveCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.primitive_authority import (
        PrimitiveAuthorityError,
        record_primitive,
    )

    conn = subsystems.get_pg_conn()
    try:
        return record_primitive(
            conn,
            primitive_slug=command.primitive_slug,
            primitive_kind=command.primitive_kind,
            summary=command.summary,
            rationale=command.rationale,
            spec=command.spec,
            depends_on=command.depends_on,
            decision_ref=command.decision_ref,
            metadata=command.metadata,
            enabled=command.enabled,
        )
    except PrimitiveAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }
