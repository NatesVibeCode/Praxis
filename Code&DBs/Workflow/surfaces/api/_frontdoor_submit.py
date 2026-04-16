"""Submit helpers for the native workflow frontdoor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Callable

from runtime.intake import WorkflowIntakePlanner


def build_submit_payload(
    frontdoor: Any,
    *,
    request_payload: Mapping[str, Any],
    env: Mapping[str, str] | None,
    now: Callable[[], Any],
    request_from_mapping: Callable[[Mapping[str, Any]], Any],
    submission_from_outcome: Callable[..., Any],
    serialize_decision: Callable[[Any], dict[str, Any]],
    load_sync_status: Callable[[str], Mapping[str, Any]],
) -> dict[str, Any]:
    source, instance = frontdoor._resolve_instance(env=env)
    request = request_from_mapping(request_payload)
    requested_at = request.requested_at or now()
    request = replace(request, requested_at=requested_at)
    planner = WorkflowIntakePlanner(registry=frontdoor._require_registry())
    outcome = planner.plan(request=request)
    submission = submission_from_outcome(
        outcome=outcome,
        requested_at=requested_at,
    )
    write_result = frontdoor._run_sync_submission(source, submission=submission)
    sync_payload = load_sync_status(write_result.run_id)
    return {
        "native_instance": instance.to_contract(),
        "run": {
            "run_id": write_result.run_id,
            "workflow_id": outcome.workflow_request.workflow_id,
            "request_id": outcome.workflow_request.request_id,
            "current_state": outcome.current_state.value,
            "workflow_definition_id": submission.run.workflow_definition_id,
            "admitted_definition_hash": submission.run.admitted_definition_hash,
            "persisted": True,
            "sync_status": sync_payload["sync_status"],
            "sync_cycle_id": sync_payload["sync_cycle_id"],
            "sync_error_count": sync_payload["sync_error_count"],
        },
        "admission_decision": serialize_decision(outcome.admission_decision),
    }


__all__ = ["build_submit_payload"]
