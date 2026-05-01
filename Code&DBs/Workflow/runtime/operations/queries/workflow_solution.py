"""CQRS query handlers for workflow Solution status.

Solution is the operator-facing name for a coordinated answer under proof:
one durable container with one or more workflow executions and phase gates.
The current storage authority is the existing workflow_chain subsystem; this
query translates that backing shape into Solution language at the surface.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class WorkflowSolutionStatusQuery(BaseModel):
    """Read one Solution by id, or list recent Solutions when no id is supplied."""

    solution_id: str | None = Field(
        default=None,
        description="Solution identifier. Currently backed by workflow_chains.chain_id.",
    )
    chain_id: str | None = Field(
        default=None,
        description="Backward-facing chain id accepted as an internal backing ref.",
    )
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("solution_id", "chain_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("solution_id/chain_id must be non-empty strings when provided")
        return value.strip()

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 20
        if isinstance(value, bool):
            raise ValueError("limit must be an integer")
        try:
            return max(1, min(int(value), 100))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc

    @model_validator(mode="after")
    def _ids_must_match_when_both_supplied(self) -> "WorkflowSolutionStatusQuery":
        if self.solution_id and self.chain_id and self.solution_id != self.chain_id:
            raise ValueError("solution_id and chain_id must match when both are supplied")
        return self


def _pg_conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def _workflow_run_payload(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run.get("run_id"),
        "workflow_id": run.get("workflow_id"),
        "spec_workflow_id": run.get("spec_workflow_id"),
        "spec_path": run.get("spec_path"),
        "spec_name": run.get("spec_name"),
        "queue_id": run.get("queue_id"),
        "submission_status": run.get("submission_status"),
        "run_status": run.get("run_status"),
        "completed_jobs": run.get("completed_jobs"),
        "total_jobs": run.get("total_jobs"),
        "started_at": run.get("started_at"),
        "completed_at": run.get("completed_at"),
        "updated_at": run.get("updated_at"),
    }


def _phase_payload(phase: dict[str, Any]) -> dict[str, Any]:
    workflows = [
        _workflow_run_payload(dict(run))
        for run in (phase.get("runs") or [])
        if isinstance(run, dict)
    ]
    return {
        "phase_id": phase.get("wave_id"),
        "status": phase.get("status"),
        "depends_on": list(phase.get("depends_on") or []),
        "blocked_by": phase.get("blocked_by"),
        "workflows": workflows,
        "workflow_count": len(workflows),
        "started_at": phase.get("started_at"),
        "completed_at": phase.get("completed_at"),
        "updated_at": phase.get("updated_at"),
    }


def _solution_payload(state: dict[str, Any]) -> dict[str, Any]:
    phases = [
        _phase_payload(dict(phase))
        for phase in (state.get("waves") or [])
        if isinstance(phase, dict)
    ]
    completed = sum(1 for phase in phases if str(phase.get("status") or "") == "succeeded")
    return {
        "solution_id": state.get("chain_id"),
        "backing_chain_id": state.get("chain_id"),
        "authority": "workflow_chain",
        "name": state.get("program"),
        "status": state.get("status"),
        "current_phase": state.get("current_wave"),
        "coordination_path": state.get("coordination_path"),
        "why": state.get("why"),
        "mode": state.get("mode"),
        "phases": phases,
        "phases_total": len(phases),
        "phases_completed": completed,
        "created_at": state.get("created_at"),
        "updated_at": state.get("updated_at"),
        "started_at": state.get("started_at"),
        "finished_at": state.get("finished_at"),
        "last_error_code": state.get("last_error_code"),
        "last_error_detail": state.get("last_error_detail"),
    }


def _solution_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "solution_id": row.get("chain_id"),
        "backing_chain_id": row.get("chain_id"),
        "authority": "workflow_chain",
        "name": row.get("program"),
        "status": row.get("status"),
        "current_phase": row.get("current_wave_id"),
        "coordination_path": row.get("coordination_path"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
    }


def handle_query_workflow_solution_status(
    query: WorkflowSolutionStatusQuery,
    subsystems: Any,
) -> dict[str, Any]:
    """Return Solution status/list views using workflow_chain as backing authority."""

    from runtime.workflow_chain import get_workflow_chain_status, list_workflow_chains

    conn = _pg_conn(subsystems)
    solution_id = query.solution_id or query.chain_id
    if solution_id:
        state = get_workflow_chain_status(conn, solution_id)
        if state is None:
            return {
                "ok": False,
                "view": "workflow_solution",
                "error_code": "workflow_solution.not_found",
                "error": f"Solution not found: {solution_id}",
                "solution_id": solution_id,
            }
        payload = _solution_payload(state)
        payload["ok"] = True
        payload["view"] = "workflow_solution"
        return payload

    rows = list_workflow_chains(conn, limit=query.limit)
    solutions = [_solution_summary(dict(row)) for row in rows]
    return {
        "ok": True,
        "view": "workflow_solutions",
        "solutions": solutions,
        "count": len(solutions),
        "limit": query.limit,
        "authority": "workflow_chain",
    }


__all__ = [
    "WorkflowSolutionStatusQuery",
    "handle_query_workflow_solution_status",
]
