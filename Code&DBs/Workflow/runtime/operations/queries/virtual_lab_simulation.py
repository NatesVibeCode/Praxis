"""CQRS queries for Virtual Lab simulation authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from storage.postgres.virtual_lab_simulation_repository import (
    list_virtual_lab_simulation_blockers,
    list_virtual_lab_simulation_events,
    list_virtual_lab_simulation_runs,
    list_virtual_lab_simulation_verifiers,
    load_virtual_lab_simulation_run,
)


ReadAction = Literal[
    "list_runs",
    "describe_run",
    "list_events",
    "list_verifiers",
    "list_blockers",
]


class QueryVirtualLabSimulationRead(BaseModel):
    """Read persisted Virtual Lab simulation runs and proof artifacts."""

    action: ReadAction = "list_runs"
    run_id: str | None = None
    scenario_id: str | None = None
    environment_id: str | None = None
    revision_id: str | None = None
    status: str | None = None
    event_type: str | None = None
    source_area: str | None = None
    blocker_code: str | None = None
    include_events: bool = True
    include_state_events: bool = True
    include_transitions: bool = True
    include_actions: bool = True
    include_automation: bool = True
    include_assertions: bool = True
    include_verifiers: bool = True
    include_gaps: bool = True
    include_blockers: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @field_validator(
        "run_id",
        "scenario_id",
        "environment_id",
        "revision_id",
        "status",
        "event_type",
        "source_area",
        "blocker_code",
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
    def _validate_action(self) -> "QueryVirtualLabSimulationRead":
        if self.action in {"describe_run", "list_events", "list_verifiers", "list_blockers"} and not self.run_id:
            raise ValueError(f"run_id is required for {self.action}")
        return self


def handle_virtual_lab_simulation_read(
    query: QueryVirtualLabSimulationRead,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    if query.action == "describe_run":
        run = load_virtual_lab_simulation_run(
            conn,
            run_id=str(query.run_id),
            include_events=query.include_events,
            include_state_events=query.include_state_events,
            include_transitions=query.include_transitions,
            include_actions=query.include_actions,
            include_automation=query.include_automation,
            include_assertions=query.include_assertions,
            include_verifiers=query.include_verifiers,
            include_gaps=query.include_gaps,
            include_blockers=query.include_blockers,
        )
        return {
            "ok": run is not None,
            "operation": "virtual_lab_simulation_read",
            "action": "describe_run",
            "run_id": query.run_id,
            "run": run,
            "error_code": None if run is not None else "virtual_lab_simulation.run_not_found",
        }
    if query.action == "list_events":
        items = list_virtual_lab_simulation_events(
            conn,
            run_id=str(query.run_id),
            event_type=query.event_type,
            source_area=query.source_area,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_simulation_read",
            "action": "list_events",
            "count": len(items),
            "items": items,
        }
    if query.action == "list_verifiers":
        items = list_virtual_lab_simulation_verifiers(
            conn,
            run_id=str(query.run_id),
            status=query.status,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_simulation_read",
            "action": "list_verifiers",
            "count": len(items),
            "items": items,
        }
    if query.action == "list_blockers":
        items = list_virtual_lab_simulation_blockers(
            conn,
            run_id=str(query.run_id),
            code=query.blocker_code,
            limit=query.limit,
        )
        return {
            "ok": True,
            "operation": "virtual_lab_simulation_read",
            "action": "list_blockers",
            "count": len(items),
            "items": items,
        }

    items = list_virtual_lab_simulation_runs(
        conn,
        status=query.status,
        scenario_id=query.scenario_id,
        environment_id=query.environment_id,
        revision_id=query.revision_id,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "virtual_lab_simulation_read",
        "action": "list_runs",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryVirtualLabSimulationRead",
    "handle_virtual_lab_simulation_read",
]
