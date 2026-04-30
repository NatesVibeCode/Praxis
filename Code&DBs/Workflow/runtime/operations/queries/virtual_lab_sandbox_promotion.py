"""CQRS queries for Virtual Lab sandbox promotion authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.virtual_lab_sandbox_promotion_repository import (
    list_virtual_lab_sandbox_drift_classifications,
    list_virtual_lab_sandbox_handoffs,
    list_virtual_lab_sandbox_promotion_records,
    list_virtual_lab_sandbox_readback_evidence,
    load_virtual_lab_sandbox_promotion_record,
)


ReadAction = Literal[
    "list_records",
    "describe_record",
    "list_drift",
    "list_handoffs",
    "list_readback_evidence",
]


class QueryVirtualLabSandboxPromotionRead(BaseModel):
    """Read persisted sandbox promotion, readback, and drift records."""

    action: ReadAction = "list_records"
    promotion_record_id: str | None = None
    manifest_id: str | None = None
    candidate_id: str | None = None
    simulation_run_id: str | None = None
    recommendation: str | None = None
    comparison_status: str | None = None
    reason_code: str | None = None
    severity: str | None = None
    layer: str | None = None
    disposition: str | None = None
    handoff_kind: str | None = None
    handoff_status: str | None = None
    available: bool | None = None
    trusted: bool | None = None
    include_candidates: bool = True
    include_executions: bool = True
    include_readback: bool = True
    include_reports: bool = True
    include_drift: bool = True
    include_handoffs: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator(
        "promotion_record_id",
        "manifest_id",
        "candidate_id",
        "simulation_run_id",
        "recommendation",
        "comparison_status",
        "reason_code",
        "severity",
        "layer",
        "disposition",
        "handoff_kind",
        "handoff_status",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read filters must be non-empty strings when supplied")
        return value.strip()

    @model_validator(mode="after")
    def _validate_action(self) -> "QueryVirtualLabSandboxPromotionRead":
        if self.action == "describe_record" and not self.promotion_record_id:
            raise ValueError("promotion_record_id is required for describe_record")
        return self


def handle_virtual_lab_sandbox_promotion_read(
    query: QueryVirtualLabSandboxPromotionRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe_record":
        record = load_virtual_lab_sandbox_promotion_record(
            conn,
            promotion_record_id=str(query.promotion_record_id),
            include_candidates=query.include_candidates,
            include_executions=query.include_executions,
            include_readback=query.include_readback,
            include_reports=query.include_reports,
            include_drift=query.include_drift,
            include_handoffs=query.include_handoffs,
        )
        return {
            "ok": record is not None,
            "operation": "virtual_lab_sandbox_promotion_read",
            "action": "describe_record",
            "promotion_record_id": query.promotion_record_id,
            "record": record,
            "error_code": None if record is not None else "virtual_lab_sandbox_promotion.record_not_found",
        }
    if query.action == "list_drift":
        items = list_virtual_lab_sandbox_drift_classifications(
            conn,
            promotion_record_id=query.promotion_record_id,
            candidate_id=query.candidate_id,
            reason_code=query.reason_code,
            severity=query.severity,
            layer=query.layer,
            disposition=query.disposition,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_sandbox_promotion_read",
            "action": "list_drift",
            "count": len(items),
            "items": items,
        }
    if query.action == "list_handoffs":
        items = list_virtual_lab_sandbox_handoffs(
            conn,
            promotion_record_id=query.promotion_record_id,
            candidate_id=query.candidate_id,
            handoff_kind=query.handoff_kind,
            status=query.handoff_status,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_sandbox_promotion_read",
            "action": "list_handoffs",
            "count": len(items),
            "items": items,
        }
    if query.action == "list_readback_evidence":
        items = list_virtual_lab_sandbox_readback_evidence(
            conn,
            promotion_record_id=query.promotion_record_id,
            candidate_id=query.candidate_id,
            available=query.available,
            trusted=query.trusted,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_sandbox_promotion_read",
            "action": "list_readback_evidence",
            "count": len(items),
            "items": items,
        }

    items = list_virtual_lab_sandbox_promotion_records(
        conn,
        manifest_id=query.manifest_id,
        candidate_id=query.candidate_id,
        simulation_run_id=query.simulation_run_id,
        recommendation=query.recommendation,
        comparison_status=query.comparison_status,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "virtual_lab_sandbox_promotion_read",
        "action": "list_records",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryVirtualLabSandboxPromotionRead",
    "handle_virtual_lab_sandbox_promotion_read",
]
