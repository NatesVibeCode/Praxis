"""Built-in platform integration executors.

These bindings own the static integrations projected into ``integration_registry``
so the registry does not advertise actions with no execution path.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from runtime.notifications import dispatch_notification_payload

logger = logging.getLogger(__name__)


def _as_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _json_clone(value: object) -> object:
    return json.loads(json.dumps(value, default=str))


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _load_workflow_record(pg: Any, *, workflow_id: str) -> dict[str, Any] | None:
    from storage.postgres.workflow_runtime_repository import load_workflow_record

    return load_workflow_record(pg, workflow_id=workflow_id)


def _current_compiled_spec(definition: dict[str, Any], compiled_spec: Any) -> dict[str, Any] | None:
    from runtime.operating_model_planner import current_compiled_spec

    return current_compiled_spec(definition, compiled_spec)


def _missing_execution_plan_message(workflow_name: str | None = None) -> str:
    from runtime.operating_model_planner import missing_execution_plan_message

    return missing_execution_plan_message(workflow_name)


def _submit_workflow_inline(
    pg: Any,
    spec_dict: dict[str, Any],
    *,
    parent_run_id: str | None,
    trigger_depth: int,
    packet_provenance: dict[str, Any],
) -> dict[str, Any]:
    from runtime.workflow.unified import submit_workflow_inline

    return submit_workflow_inline(
        pg,
        spec_dict,
        parent_run_id=parent_run_id,
        trigger_depth=trigger_depth,
        packet_provenance=packet_provenance,
    )


def _record_workflow_invocation(pg: Any, *, workflow_id: str) -> None:
    from storage.postgres.workflow_runtime_repository import record_workflow_invocation

    record_workflow_invocation(pg, workflow_id=workflow_id)


def _record_system_event(
    pg: Any,
    *,
    event_type: str,
    source_id: str,
    source_type: str,
    payload: dict[str, Any],
) -> None:
    from storage.postgres.workflow_runtime_repository import record_system_event

    record_system_event(
        pg,
        event_type=event_type,
        source_id=source_id,
        source_type=source_type,
        payload=payload,
    )


def _coerce_trigger_depth(args: dict[str, Any]) -> int:
    raw = args.get("trigger_depth")
    if raw is None:
        raw = _mapping(args.get("_trigger_event")).get("trigger_depth", 0)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(value, 0)


def execute_notification(args: dict[str, Any], _pg: Any) -> dict[str, Any]:
    """Send an explicit operator notification through the configured channels."""
    message = _as_text(args.get("message")) or _as_text(args.get("summary")) or _as_text(args.get("title"))
    if not message:
        return {
            "status": "failed",
            "data": None,
            "summary": "Notification send requires message, summary, or title.",
            "error": "missing_message",
        }

    metadata = _mapping(args.get("metadata"))
    trigger_event = _mapping(args.get("_trigger_event"))
    if trigger_event:
        metadata.setdefault("trigger_event", trigger_event)

    payload = {
        "kind": "integration_notification",
        "title": _as_text(args.get("title")) or message[:80],
        "message": message,
        "status": _as_text(args.get("status")) or "info",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata,
    }
    delivered = dispatch_notification_payload(payload)
    if delivered <= 0:
        return {
            "status": "skipped",
            "data": {"configured_channels": 0, "payload": payload},
            "summary": "Notifications are not configured; nothing was sent.",
            "error": None,
        }

    return {
        "status": "succeeded",
        "data": {"configured_channels": delivered, "payload": payload},
        "summary": f"Notification sent via {delivered} configured channel(s).",
        "error": None,
    }


def execute_workflow_invoke(args: dict[str, Any], pg: Any) -> dict[str, Any]:
    """Invoke a saved workflow through the runtime's canonical workflow authority."""
    workflow_id = (
        _as_text(args.get("workflow_id"))
        or _as_text(args.get("id"))
        or _as_text(args.get("target_workflow_id"))
    )
    if not workflow_id:
        return {
            "status": "failed",
            "data": None,
            "summary": "Workflow invoke requires workflow_id.",
            "error": "missing_workflow_id",
        }

    workflow_row = _load_workflow_record(pg, workflow_id=workflow_id)
    if workflow_row is None:
        return {
            "status": "failed",
            "data": None,
            "summary": f"Workflow not found: {workflow_id}",
            "error": "workflow_not_found",
        }

    definition_row = _mapping(workflow_row.get("definition"))
    compiled_spec_row = workflow_row.get("compiled_spec")
    spec = _current_compiled_spec(definition_row, compiled_spec_row)
    if spec is None:
        return {
            "status": "failed",
            "data": None,
            "summary": _missing_execution_plan_message(_as_text(workflow_row.get("name")) or workflow_id),
            "error": "workflow_execution_plan_missing",
        }

    trigger_event = _mapping(args.get("_trigger_event"))
    parent_run_id = _as_text(args.get("parent_run_id")) or _as_text(trigger_event.get("run_id"))
    trigger_depth = _coerce_trigger_depth(args)
    input_payload = (
        _mapping(args.get("payload"))
        or _mapping(args.get("input"))
        or _mapping(args.get("inputs"))
        or _mapping(args.get("variables"))
    )

    spec_to_submit = _json_clone(spec)
    packet_provenance = {
        "source_kind": "integration_invoke",
        "integration_id": "workflow",
        "integration_action": "invoke",
        "workflow_row": dict(workflow_row),
        "input_payload": input_payload,
        "trigger_event": trigger_event,
    }

    try:
        result = _submit_workflow_inline(
            pg,
            spec_to_submit,
            parent_run_id=parent_run_id or None,
            trigger_depth=trigger_depth,
            packet_provenance=packet_provenance,
        )
    except Exception as exc:
        logger.warning("workflow invoke failed for %s: %s", workflow_id, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"Workflow invoke failed: {exc}",
            "error": "workflow_invoke_failed",
        }

    run_id = _as_text(result.get("run_id"))
    if not run_id:
        return {
            "status": "failed",
            "data": result,
            "summary": "Workflow invoke did not return a run_id.",
            "error": "workflow_run_missing",
        }

    try:
        _record_workflow_invocation(pg, workflow_id=str(workflow_row["id"]))
        _record_system_event(
            pg,
            event_type="integration.workflow.invoke",
            source_id=str(workflow_row["id"]),
            source_type="integration",
            payload={
                "workflow_id": workflow_row["id"],
                "workflow_name": workflow_row.get("name"),
                "run_id": run_id,
                "parent_run_id": parent_run_id,
                "trigger_depth": trigger_depth,
                "input_payload": input_payload,
            },
        )
    except Exception as exc:
        logger.warning("workflow invoke bookkeeping failed for %s: %s", workflow_id, exc)

    return {
        "status": "succeeded",
        "data": {
            "workflow_id": workflow_row["id"],
            "workflow_name": workflow_row.get("name"),
            "run_id": run_id,
        },
        "summary": f"Invoked workflow {workflow_row.get('name') or workflow_id} -> {run_id}",
        "error": None,
    }


__all__ = ["execute_notification", "execute_workflow_invoke"]
