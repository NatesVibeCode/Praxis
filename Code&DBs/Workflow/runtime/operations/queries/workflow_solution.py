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


def _solution_authority_payload() -> dict[str, str]:
    return {
        "authority": "workflow_solution",
        "storage_authority": "workflow_chain",
    }


def _legacy_group_id(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text


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


def _solution_workflows(state: dict[str, Any]) -> list[dict[str, Any]]:
    groups = [
        dict(group)
        for group in (state.get("waves") or [])
        if isinstance(group, dict)
    ]
    workflows_by_group: dict[str, list[dict[str, Any]]] = {}
    for group in groups:
        group_id = _legacy_group_id(group.get("wave_id"))
        if not group_id:
            continue
        workflows_by_group[group_id] = [
            _workflow_run_payload(dict(run))
            for run in (group.get("runs") or [])
            if isinstance(run, dict)
        ]

    workflows: list[dict[str, Any]] = []
    for group in groups:
        group_id = _legacy_group_id(group.get("wave_id"))
        if not group_id:
            continue
        depends_on_workflow_ids: list[str] = []
        for depends_on_group_id in group.get("depends_on") or []:
            normalized_depends_on_group_id = _legacy_group_id(depends_on_group_id)
            for workflow in workflows_by_group.get(normalized_depends_on_group_id or "", []):
                workflow_id = str(workflow.get("workflow_id") or "").strip()
                if workflow_id and workflow_id not in depends_on_workflow_ids:
                    depends_on_workflow_ids.append(workflow_id)
        blocked_by_workflow_ids: list[str] = []
        blocked_by_group_id = _legacy_group_id(group.get("blocked_by"))
        if blocked_by_group_id:
            for workflow in workflows_by_group.get(blocked_by_group_id, []):
                workflow_id = str(workflow.get("workflow_id") or "").strip()
                if workflow_id and workflow_id not in blocked_by_workflow_ids:
                    blocked_by_workflow_ids.append(workflow_id)

        for workflow in workflows_by_group.get(group_id, []):
            workflow["solution_group_status"] = group.get("status")
            workflow["depends_on_workflow_ids"] = depends_on_workflow_ids
            workflow["blocked_by_workflow_ids"] = blocked_by_workflow_ids
            workflows.append(workflow)
    return workflows


def _workflow_ids(workflows: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for workflow in workflows:
        workflow_id = str(workflow.get("workflow_id") or "").strip()
        if workflow_id and workflow_id not in ids:
            ids.append(workflow_id)
    return ids


def _active_run_ids(workflows: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    for workflow in workflows:
        run_id = str(workflow.get("run_id") or "").strip()
        if (
            run_id
            and run_id not in ids
            and str(workflow.get("run_status") or "") in {"queued", "running"}
        ):
            ids.append(run_id)
    return ids


def _current_workflow_ids(state: dict[str, Any], workflows: list[dict[str, Any]]) -> list[str]:
    current_group_id = _legacy_group_id(state.get("current_wave"))
    if not current_group_id:
        return []
    group_runs = []
    for group in (state.get("waves") or []):
        if not isinstance(group, dict):
            continue
        if _legacy_group_id(group.get("wave_id")) != current_group_id:
            continue
        group_runs = [
            _workflow_run_payload(dict(run))
            for run in (group.get("runs") or [])
            if isinstance(run, dict)
        ]
        break
    current_ids = _workflow_ids(group_runs)
    if current_ids:
        return current_ids
    return [
        workflow_id
        for workflow_id in _workflow_ids(workflows)
        if any(
            str(workflow.get("workflow_id") or "").strip() == workflow_id
            and str(workflow.get("run_status") or "") in {"queued", "running"}
            for workflow in workflows
        )
    ]


def _workflow_count_completed(workflows: list[dict[str, Any]]) -> int:
    return sum(1 for workflow in workflows if str(workflow.get("run_status") or "") == "succeeded")


def _phase_payload(phase: dict[str, Any]) -> dict[str, Any]:
    workflows = [
        _workflow_run_payload(dict(run))
        for run in (phase.get("runs") or [])
        if isinstance(run, dict)
    ]
    workflow_ids = [
        str(workflow.get("run_id")).strip()
        for workflow in workflows
        if str(workflow.get("run_id") or "").strip()
    ]
    active_workflow_ids = [
        str(workflow.get("run_id")).strip()
        for workflow in workflows
        if str(workflow.get("run_id") or "").strip()
        and str(workflow.get("run_status") or "") in {"queued", "running"}
    ]
    return {
        "phase_id": _legacy_group_id(phase.get("wave_id")),
        "status": phase.get("status"),
        "depends_on": list(phase.get("depends_on") or []),
        "blocked_by": _legacy_group_id(phase.get("blocked_by")),
        "workflows": workflows,
        "workflow_ids": workflow_ids,
        "active_workflow_ids": active_workflow_ids,
        "workflow_count": len(workflows),
        "started_at": phase.get("started_at"),
        "completed_at": phase.get("completed_at"),
        "updated_at": phase.get("updated_at"),
    }


def _solution_payload(state: dict[str, Any]) -> dict[str, Any]:
    workflows = _solution_workflows(state)
    return {
        "solution_id": state.get("chain_id"),
        **_solution_authority_payload(),
        "name": state.get("program"),
        "status": state.get("status"),
        "current_workflow_ids": _current_workflow_ids(state, workflows),
        "workflow_ids": _workflow_ids(workflows),
        "active_run_ids": _active_run_ids(workflows),
        "coordination_path": state.get("coordination_path"),
        "why": state.get("why"),
        "mode": state.get("mode"),
        "workflows": workflows,
        "workflows_total": len(workflows),
        "workflows_completed": _workflow_count_completed(workflows),
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
        **_solution_authority_payload(),
        "name": row.get("program"),
        "status": row.get("status"),
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
        **_solution_authority_payload(),
    }


__all__ = [
    "WorkflowSolutionStatusQuery",
    "handle_query_workflow_solution_status",
]
