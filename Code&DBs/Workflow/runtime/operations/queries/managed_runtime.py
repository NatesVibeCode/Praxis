"""CQRS queries for managed-runtime accounting and observability authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.managed_runtime_repository import (
    list_managed_runtime_audit_events,
    list_managed_runtime_heartbeats,
    list_managed_runtime_meter_events,
    list_managed_runtime_pool_health,
    list_managed_runtime_pricing_schedules,
    list_managed_runtime_records,
    load_managed_runtime_record,
)


ReadAction = Literal[
    "list_records",
    "describe_record",
    "list_meter_events",
    "list_heartbeats",
    "list_pool_health",
    "list_audit_events",
    "list_pricing_schedules",
]


class ReadManagedRuntimeQuery(BaseModel):
    """Read managed-runtime accounting and observability records."""

    action: ReadAction = "list_records"
    runtime_record_id: str | None = None
    run_id: str | None = None
    receipt_id: str | None = None
    tenant_ref: str | None = None
    environment_ref: str | None = None
    workflow_ref: str | None = None
    execution_mode: str | None = None
    configured_mode: str | None = None
    terminal_status: str | None = None
    cost_status: str | None = None
    source_ref: str | None = None
    event_kind: str | None = None
    pool_ref: str | None = None
    worker_ref: str | None = None
    health_state: str | None = None
    schedule_ref: str | None = None
    pricing_schedule_version_ref: str | None = None
    include_meter_events: bool = True
    include_heartbeats: bool = True
    include_pool_health: bool = True
    include_audit_events: bool = True
    include_pricing_schedule: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator(
        "runtime_record_id",
        "run_id",
        "receipt_id",
        "tenant_ref",
        "environment_ref",
        "workflow_ref",
        "execution_mode",
        "configured_mode",
        "terminal_status",
        "cost_status",
        "source_ref",
        "event_kind",
        "pool_ref",
        "worker_ref",
        "health_state",
        "schedule_ref",
        "pricing_schedule_version_ref",
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
    def _validate_action(self) -> "ReadManagedRuntimeQuery":
        if self.action == "describe_record" and not self.runtime_record_id:
            raise ValueError("runtime_record_id is required for describe_record")
        return self


def handle_read_managed_runtime(
    query: ReadManagedRuntimeQuery,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe_record":
        record = load_managed_runtime_record(
            conn,
            runtime_record_id=str(query.runtime_record_id),
            include_meter_events=query.include_meter_events,
            include_heartbeats=query.include_heartbeats,
            include_pool_health=query.include_pool_health,
            include_audit_events=query.include_audit_events,
            include_pricing_schedule=query.include_pricing_schedule,
        )
        return {
            "ok": record is not None,
            "operation": "authority.managed_runtime.read",
            "action": "describe_record",
            "runtime_record_id": query.runtime_record_id,
            "record": record,
            "error_code": None if record is not None else "managed_runtime.record_not_found",
        }
    if query.action == "list_meter_events":
        return _list_result(
            query.action,
            list_managed_runtime_meter_events(
                conn,
                runtime_record_id=query.runtime_record_id,
                run_id=query.run_id,
                tenant_ref=query.tenant_ref,
                environment_ref=query.environment_ref,
                workflow_ref=query.workflow_ref,
                execution_mode=query.execution_mode,
                event_kind=query.event_kind,
                limit=query.limit,
            ),
        )
    if query.action == "list_heartbeats":
        return _list_result(
            query.action,
            list_managed_runtime_heartbeats(
                conn,
                runtime_record_id=query.runtime_record_id,
                tenant_ref=query.tenant_ref,
                environment_ref=query.environment_ref,
                pool_ref=query.pool_ref,
                worker_ref=query.worker_ref,
                limit=query.limit,
            ),
        )
    if query.action == "list_pool_health":
        return _list_result(
            query.action,
            list_managed_runtime_pool_health(
                conn,
                runtime_record_id=query.runtime_record_id,
                tenant_ref=query.tenant_ref,
                environment_ref=query.environment_ref,
                pool_ref=query.pool_ref,
                health_state=query.health_state,
                limit=query.limit,
            ),
        )
    if query.action == "list_audit_events":
        return _list_result(
            query.action,
            list_managed_runtime_audit_events(
                conn,
                runtime_record_id=query.runtime_record_id,
                run_id=query.run_id,
                tenant_ref=query.tenant_ref,
                environment_ref=query.environment_ref,
                limit=query.limit,
            ),
        )
    if query.action == "list_pricing_schedules":
        return _list_result(
            query.action,
            list_managed_runtime_pricing_schedules(
                conn,
                schedule_ref=query.schedule_ref,
                version_ref=query.pricing_schedule_version_ref,
                limit=query.limit,
            ),
        )
    return _list_result(
        "list_records",
        list_managed_runtime_records(
            conn,
            runtime_record_id=query.runtime_record_id,
            run_id=query.run_id,
            receipt_id=query.receipt_id,
            tenant_ref=query.tenant_ref,
            environment_ref=query.environment_ref,
            workflow_ref=query.workflow_ref,
            execution_mode=query.execution_mode,
            configured_mode=query.configured_mode,
            terminal_status=query.terminal_status,
            cost_status=query.cost_status,
            source_ref=query.source_ref,
            limit=query.limit,
        ),
    )


def _list_result(action: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "ok": True,
        "operation": "authority.managed_runtime.read",
        "action": action,
        "count": len(items),
        "items": items,
    }


__all__ = [
    "ReadManagedRuntimeQuery",
    "handle_read_managed_runtime",
]
