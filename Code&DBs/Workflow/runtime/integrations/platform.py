"""Built-in platform integration executors.

These bindings own the static integrations projected into ``integration_registry``
so the registry does not advertise actions with no execution path.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from pathlib import Path

from runtime.notifications import dispatch_notification_payload

logger = logging.getLogger(__name__)


def _as_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _json_clone(value: object) -> object:
    return json.loads(json.dumps(value, default=str))


def _coerce_int(value: object, *, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return default if minimum == 0 else minimum
    return parsed


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on", "y"}:
            return True
        if lowered in {"false", "0", "no", "off", "n"}:
            return False
    return default if value is None else bool(value)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _load_workflow_spec_file(path_value: str) -> dict[str, Any]:
    spec_path = Path(path_value).expanduser()
    with spec_path.open("r", encoding="utf-8") as file_obj:
        raw = file_obj.read()
    return _json_load(raw)


def _json_load(value: object) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return dict(parsed)


def _load_workflow_record(pg: Any, *, workflow_id: str) -> dict[str, Any] | None:
    from storage.postgres.workflow_runtime_repository import load_workflow_record

    return load_workflow_record(pg, workflow_id=workflow_id)


def _current_compiled_spec(definition: dict[str, Any], compiled_spec: Any) -> dict[str, Any] | None:
    from runtime.operating_model_planner import current_compiled_spec

    return current_compiled_spec(definition, compiled_spec)


def _latest_execution_manifest(
    pg: Any,
    *,
    workflow_id: str,
    definition_revision: str | None,
) -> dict[str, Any] | None:
    normalized_workflow_id = _as_text(workflow_id)
    normalized_definition_revision = _as_text(definition_revision)
    if not normalized_workflow_id or not normalized_definition_revision:
        return None
    try:
        from storage.postgres.workflow_build_planning_repository import (
            load_latest_workflow_build_execution_manifest,
        )
    except Exception:
        return None
    try:
        return load_latest_workflow_build_execution_manifest(
            pg,
            workflow_id=normalized_workflow_id,
            definition_revision=normalized_definition_revision,
        )
    except Exception:
        return None


def _missing_execution_plan_message(workflow_name: str | None = None) -> str:
    from runtime.operating_model_planner import missing_execution_plan_message

    return missing_execution_plan_message(workflow_name)


def _submit_workflow_inline(
    pg: Any,
    spec_dict: dict[str, Any],
    *,
    parent_run_id: str | None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    trigger_depth: int,
    lineage_depth: int | None = None,
    packet_provenance: dict[str, Any],
) -> dict[str, Any]:
    from runtime.workflow.unified import submit_workflow_inline

    return submit_workflow_inline(
        pg,
        spec_dict,
        parent_run_id=parent_run_id,
        parent_job_label=parent_job_label,
        dispatch_reason=dispatch_reason,
        trigger_depth=trigger_depth,
        lineage_depth=lineage_depth,
        packet_provenance=packet_provenance,
    )


def _search_receipts(
    query: str,
    *,
    limit: int,
    status: str | None = None,
    agent: str | None = None,
    workflow_id: str | None = None,
) -> list[dict[str, Any]]:
    from runtime.receipt_store import search_receipts

    return [
        row.to_search_result()
        for row in search_receipts(
            query,
            limit=limit,
            status=status,
            agent=agent,
            workflow_id=workflow_id,
        )
    ]


def _get_run_status(pg: Any, run_id: str) -> dict[str, Any] | None:
    from runtime.workflow.unified import get_run_status

    return get_run_status(pg, run_id)


def _cancel_workflow_run(pg: Any, run_id: str, *, include_running: bool = False) -> dict[str, Any]:
    from runtime.workflow.unified import cancel_run

    return cancel_run(pg, run_id, include_running=include_running)


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


def _extract_workflow_spec(args: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("spec", "workflow_spec", "inline_spec", "definition", "manifest"):
        spec = _mapping(args.get(key))
        if spec:
            return _json_clone(spec)

    for key in ("spec_json", "spec_text"):
        parsed = _json_load(args.get(key))
        if parsed:
            return parsed

    spec_path = _as_text(args.get("spec_path"))
    if spec_path:
        try:
            loaded = _load_workflow_spec_file(spec_path)
            if loaded:
                return loaded
        except Exception:
            return {}

    return None


def execute_dispatch_job(args: dict[str, Any], pg: Any) -> dict[str, Any]:
    """Submit an inline workflow spec and return its run metadata."""
    spec = _extract_workflow_spec(args)
    if not spec:
        return {
            "status": "failed",
            "data": None,
            "summary": "dispatch_job requires a workflow spec in spec, workflow_spec, inline_spec, definition, manifest, spec_json, or spec_path.",
            "error": "dispatch_job_missing_spec",
        }

    trigger_event = _mapping(args.get("_trigger_event"))
    parent_run_id = _as_text(args.get("parent_run_id")) or _as_text(trigger_event.get("run_id"))
    parent_job_label = _as_text(args.get("parent_job_label")) or _as_text(trigger_event.get("job_label"))
    trigger_depth = _coerce_trigger_depth(args)
    input_payload = (
        _mapping(args.get("payload"))
        or _mapping(args.get("input"))
        or _mapping(args.get("inputs"))
        or _mapping(args.get("variables"))
    )

    spec_to_submit = _json_clone(spec)
    packet_provenance = {
        "source_kind": "integration_dispatch",
        "integration_id": "praxis-dispatch",
        "integration_action": "dispatch_job",
        "input_payload": input_payload,
        "trigger_event": trigger_event,
    }

    try:
        result = _submit_workflow_inline(
            pg,
            spec_to_submit,
            parent_run_id=parent_run_id or None,
            parent_job_label=parent_job_label or None,
            dispatch_reason="integration.dispatch_job",
            trigger_depth=trigger_depth,
            packet_provenance=packet_provenance,
        )
    except Exception as exc:
        logger.warning("dispatch_job failed: %s", exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"dispatch_job failed: {exc}",
            "error": "dispatch_job_failed",
        }

    run_id = _as_text(result.get("run_id"))
    if not run_id:
        return {
            "status": "failed",
            "data": result,
            "summary": "dispatch_job did not return a run_id.",
            "error": "dispatch_job_run_missing",
        }

    payload = {"run_id": run_id}
    if "command_id" in result:
        payload["command_id"] = result["command_id"]
    if "command_status" in result:
        payload["command_status"] = result["command_status"]
    if "status" in result:
        payload["status"] = result["status"]
    else:
        payload["status"] = "queued"

    return {
        "status": "succeeded",
        "data": payload,
        "summary": f"Dispatched workflow spec via integration -> {run_id}",
        "error": None,
    }


def execute_check_status(args: dict[str, Any], pg: Any) -> dict[str, Any]:
    """Return run status from the unified workflow runtime."""
    run_id = _as_text(args.get("run_id")) or _as_text(args.get("id")) or _as_text(args.get("target_run_id"))
    if not run_id:
        return {
            "status": "failed",
            "data": None,
            "summary": "check_status requires run_id.",
            "error": "check_status_missing_run_id",
        }

    try:
        status = _get_run_status(pg, run_id)
    except Exception as exc:
        logger.warning("check_status failed for %s: %s", run_id, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"check_status failed: {exc}",
            "error": "check_status_failed",
        }
    if status is None:
        return {
            "status": "failed",
            "data": None,
            "summary": f"Run not found: {run_id}",
            "error": "run_not_found",
        }

    return {
        "status": "succeeded",
        "data": status,
        "summary": f"Run {run_id} status retrieved.",
        "error": None,
    }


def execute_search_receipts(args: dict[str, Any], pg: Any) -> dict[str, Any]:
    """Search workflow receipts with optional status/agent/workflow_id filters."""
    query = _as_text(args.get("query"))
    if not query:
        return {
            "status": "failed",
            "data": None,
            "summary": "search_receipts requires query.",
            "error": "search_receipts_missing_query",
        }

    limit = _coerce_int(args.get("limit"), default=50, minimum=1)
    status = _as_text(args.get("status")) or None
    agent = _as_text(args.get("agent")) or None
    workflow_id = _as_text(args.get("workflow_id")) or None

    try:
        results = _search_receipts(
            query,
            limit=limit,
            status=status,
            agent=agent,
            workflow_id=workflow_id,
        )
    except Exception as exc:
        logger.warning("search_receipts failed for query %s: %s", query, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"search_receipts failed: {exc}",
            "error": "search_receipts_failed",
        }

    return {
        "status": "succeeded",
        "data": {"query": query, "results": results, "count": len(results)},
        "summary": f"search_receipts found {len(results)} result(s).",
        "error": None,
    }


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
    parent_job_label = _as_text(args.get("parent_job_label")) or _as_text(trigger_event.get("job_label"))
    trigger_depth = _coerce_trigger_depth(args)
    input_payload = (
        _mapping(args.get("payload"))
        or _mapping(args.get("input"))
        or _mapping(args.get("inputs"))
        or _mapping(args.get("variables"))
    )

    spec_to_submit = _json_clone(spec)
    execution_manifest = _latest_execution_manifest(
        pg,
        workflow_id=str(workflow_row["id"]),
        definition_revision=_as_text(spec.get("definition_revision")),
    )
    if isinstance(execution_manifest, dict):
        spec_to_submit["execution_manifest"] = _json_clone(execution_manifest)
        spec_to_submit["execution_manifest_ref"] = _as_text(execution_manifest.get("execution_manifest_ref")) or None
    packet_provenance = {
        "source_kind": "integration_invoke",
        "integration_id": "workflow",
        "integration_action": "invoke",
        "workflow_row": dict(workflow_row),
        "input_payload": input_payload,
        "trigger_event": trigger_event,
    }
    if isinstance(execution_manifest, dict):
        packet_provenance["execution_manifest"] = _json_clone(execution_manifest)

    try:
        result = _submit_workflow_inline(
            pg,
            spec_to_submit,
            parent_run_id=parent_run_id or None,
            parent_job_label=parent_job_label or None,
            dispatch_reason="integration.workflow.invoke",
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


def execute_workflow_cancel(args: dict[str, Any], pg: Any) -> dict[str, Any]:
    """Cancel a workflow run through the unified workflow control path."""
    run_id = (
        _as_text(args.get("run_id"))
        or _as_text(args.get("id"))
        or _as_text(args.get("target_run_id"))
    )
    if not run_id:
        return {
            "status": "failed",
            "data": None,
            "summary": "Workflow cancel requires run_id.",
            "error": "workflow_cancel_missing_run_id",
        }

    include_running = _coerce_bool(args.get("include_running"), default=False)
    try:
        result = _cancel_workflow_run(pg, run_id, include_running=include_running)
    except Exception as exc:
        logger.warning("workflow cancel failed for %s: %s", run_id, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"Workflow cancel failed: {exc}",
            "error": "workflow_cancel_failed",
        }

    try:
        _record_system_event(
            pg,
            event_type="integration.workflow.cancel",
            source_id=run_id,
            source_type="integration",
            payload={
                "run_id": run_id,
                "include_running": include_running,
                "result": result,
            },
        )
    except Exception as exc:
        logger.warning("workflow cancel bookkeeping failed for %s: %s", run_id, exc)

    return {
        "status": "succeeded",
        "data": result,
        "summary": f"Workflow run {run_id} cancel requested.",
        "error": None,
    }


__all__ = [
    "execute_notification",
    "execute_workflow_invoke",
    "execute_workflow_cancel",
    "execute_dispatch_job",
    "execute_check_status",
    "execute_search_receipts",
]
