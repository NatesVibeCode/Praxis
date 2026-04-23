"""Shared API surface-usage telemetry helpers."""

from __future__ import annotations

import json
from typing import Any

_RECORDER_FAILURES: dict[str, Any] = {
    "dropped_event_count": 0,
    "durable_event_count": 0,
    "durable_error_count": 0,
    "last_error": None,
    "last_entrypoint": None,
    "last_surface_kind": None,
    "last_friction_event_id": None,
    "last_durable_error": None,
}

_TRACKED_API_ROUTES = frozenset(
    {
        "/api/trigger/:workflow_id",
        "/orient",
        "/query",
    }
)


def _normalize_path(path: str) -> str:
    normalized = str(path or "").split("?", 1)[0].rstrip("/") or "/"
    if normalized.startswith("/api/trigger/") and normalized != "/api/trigger":
        return "/api/trigger/:workflow_id"
    return normalized


def _mapping_payload(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_size_bytes(value: Any) -> int:
    if value is None:
        return 0
    try:
        return len(json.dumps(value, sort_keys=True, default=str).encode("utf-8"))
    except TypeError:
        return 0


def _list_count(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _header_value(headers: Any, *names: str) -> str:
    if headers is None:
        return ""
    for name in names:
        try:
            value = headers.get(name)  # type: ignore[call-arg]
        except Exception:
            value = None
        if isinstance(value, str) and value.strip():
            return value.strip()
    lowered = {str(key).lower(): value for key, value in dict(headers).items()} if headers else {}
    for name in names:
        value = lowered.get(name.lower())
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _base_definition_metrics(definition: dict[str, Any]) -> dict[str, Any]:
    from runtime.compile_reuse import stable_hash

    execution_setup = definition.get("execution_setup") if isinstance(definition.get("execution_setup"), dict) else {}
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    capabilities = definition.get("capabilities") if isinstance(definition.get("capabilities"), list) else []
    unresolved_count = 0
    for reference in references:
        if not isinstance(reference, dict):
            continue
        if reference.get("resolved") is False or not str(reference.get("resolved_to") or "").strip():
            unresolved_count += 1
    return {
        "definition_hash": stable_hash(definition) if definition else "",
        "definition_revision": str(definition.get("definition_revision") or "").strip(),
        "capability_count": len(capabilities),
        "reference_count": len(references),
        "unresolved_count": unresolved_count,
        "task_class": str(execution_setup.get("task_class") or "").strip(),
        "planner_required": bool(execution_setup.get("planner_required")),
    }


def _definition_from_request_body(request_body: dict[str, Any]) -> dict[str, Any]:
    definition = request_body.get("definition")
    build_graph = request_body.get("build_graph")
    if isinstance(build_graph, dict):
        from runtime.canonical_workflows import materialize_definition_from_build_graph

        base_definition = definition if isinstance(definition, dict) else {}
        try:
            materialized = materialize_definition_from_build_graph(
                base_definition,
                build_graph=build_graph,
            )
        except Exception:
            return {}
        return dict(materialized) if isinstance(materialized, dict) else {}
    return dict(definition) if isinstance(definition, dict) else {}


def _definition_from_workflow_row(conn: Any, workflow_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if conn is None or not workflow_id:
        return {}, {}
    row = None
    query = "SELECT definition, compiled_spec FROM public.workflows WHERE id = $1"
    try:
        if hasattr(conn, "fetchrow"):
            row = conn.fetchrow(query, workflow_id)
        elif hasattr(conn, "execute"):
            rows = conn.execute(query, workflow_id) or []
            row = rows[0] if rows else None
    except Exception:
        return {}, {}
    if not isinstance(row, dict):
        return {}, {}
    definition = row.get("definition")
    compiled_spec = row.get("compiled_spec")
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
        except json.JSONDecodeError:
            definition = {}
    if isinstance(compiled_spec, str):
        try:
            compiled_spec = json.loads(compiled_spec)
        except json.JSONDecodeError:
            compiled_spec = {}
    return (
        dict(definition) if isinstance(definition, dict) else {},
        dict(compiled_spec) if isinstance(compiled_spec, dict) else {},
    )


def _query_result_count(payload: dict[str, Any]) -> int:
    count = payload.get("count")
    if isinstance(count, int) and count >= 0:
        return count
    for key in ("results", "bugs", "agents"):
        values = payload.get(key)
        if isinstance(values, list):
            return len(values)
    return 0


def _query_result_state(status_code: int, payload: dict[str, Any]) -> str:
    if status_code >= 400:
        return "error"
    status = str(payload.get("status") or "").strip().lower()
    if status in {"ok", "empty", "unavailable", "error"}:
        return status
    reason_code = str(payload.get("reason_code") or "").strip().lower()
    if reason_code.endswith(".unavailable"):
        return "unavailable"
    if _query_result_count(payload) == 0:
        if payload.get("results") == [] or ("rollup" in payload and payload.get("rollup") is None):
            return "empty"
    return "ok"


def _trigger_route_metrics(
    *,
    conn: Any,
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    workflow_id = str(response_payload.get("workflow_id") or "").strip()
    definition, compiled_spec = _definition_from_workflow_row(conn, workflow_id)
    metrics = _base_definition_metrics(definition) if definition else {}
    return {
        **metrics,
        "workflow_id": workflow_id,
        "run_id": str(response_payload.get("run_id") or "").strip(),
        "compiled_job_count": _list_count(compiled_spec.get("jobs")),
        "trigger_count": _list_count(compiled_spec.get("triggers")),
        "has_current_plan": bool(compiled_spec),
        "metadata": {
            "workflow_name": str(response_payload.get("workflow_name") or "").strip() or None,
        },
    }


def _query_route_metrics(
    *,
    request_body: dict[str, Any],
    response_payload: dict[str, Any],
    status_code: int,
) -> dict[str, Any]:
    result_count = _query_result_count(response_payload)
    metadata: dict[str, Any] = {}
    view = str(response_payload.get("view") or "").strip()
    if view:
        metadata["view"] = view
    return {
        "query_chars": len(str(request_body.get("question") or "")),
        "result_count": result_count,
        "routed_to": str(response_payload.get("routed_to") or "").strip(),
        "reason_code": str(response_payload.get("reason_code") or "").strip(),
        "result_state": _query_result_state(status_code, response_payload),
        "metadata": metadata,
    }


def _metadata_value_is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def record_surface_usage_failure(
    *,
    surface_kind: str,
    entrypoint_name: str,
    error: Exception,
    conn: Any | None = None,
) -> None:
    _RECORDER_FAILURES["dropped_event_count"] = int(
        _RECORDER_FAILURES.get("dropped_event_count") or 0
    ) + 1
    _RECORDER_FAILURES["last_error"] = f"{type(error).__name__}: {error}"
    _RECORDER_FAILURES["last_entrypoint"] = entrypoint_name
    _RECORDER_FAILURES["last_surface_kind"] = surface_kind
    _RECORDER_FAILURES["last_friction_event_id"] = None
    _RECORDER_FAILURES["last_durable_error"] = None
    if conn is None:
        _RECORDER_FAILURES["durable_error_count"] = int(
            _RECORDER_FAILURES.get("durable_error_count") or 0
        ) + 1
        _RECORDER_FAILURES["last_durable_error"] = "surface usage failure had no Postgres connection for friction ledger"
        return
    try:
        from runtime.friction_ledger import FrictionLedger, FrictionType

        message = json.dumps(
            {
                "reason_code": "surface_usage.record_failed",
                "surface_kind": surface_kind,
                "entrypoint_name": entrypoint_name,
                "error_type": type(error).__name__,
                "error": str(error),
            },
            sort_keys=True,
        )
        event = FrictionLedger(conn).record(
            FrictionType.HARD_FAILURE,
            source="surface_usage_recorder",
            job_label=f"{surface_kind}:{entrypoint_name}",
            message=message,
        )
        _RECORDER_FAILURES["durable_event_count"] = int(
            _RECORDER_FAILURES.get("durable_event_count") or 0
        ) + 1
        _RECORDER_FAILURES["last_friction_event_id"] = event.event_id
    except Exception as durable_exc:
        _RECORDER_FAILURES["durable_error_count"] = int(
            _RECORDER_FAILURES.get("durable_error_count") or 0
        ) + 1
        _RECORDER_FAILURES["last_durable_error"] = f"{type(durable_exc).__name__}: {durable_exc}"


def surface_usage_recorder_health() -> dict[str, Any]:
    dropped = int(_RECORDER_FAILURES.get("dropped_event_count") or 0)
    durable_events = int(_RECORDER_FAILURES.get("durable_event_count") or 0)
    durable_errors = int(_RECORDER_FAILURES.get("durable_error_count") or 0)
    return {
        "authority_ready": dropped == 0,
        "observability_state": "ready" if dropped == 0 else "degraded",
        "dropped_event_count": dropped,
        "durable_event_count": durable_events,
        "durable_error_count": durable_errors,
        "backup_authority_ready": dropped == 0 or durable_events >= dropped,
        "last_error": _RECORDER_FAILURES.get("last_error"),
        "last_entrypoint": _RECORDER_FAILURES.get("last_entrypoint"),
        "last_surface_kind": _RECORDER_FAILURES.get("last_surface_kind"),
        "last_friction_event_id": _RECORDER_FAILURES.get("last_friction_event_id"),
        "last_durable_error": _RECORDER_FAILURES.get("last_durable_error"),
    }


def _reset_surface_usage_recorder_health_for_tests() -> None:
    _RECORDER_FAILURES.update({
        "dropped_event_count": 0,
        "durable_event_count": 0,
        "durable_error_count": 0,
        "last_error": None,
        "last_entrypoint": None,
        "last_surface_kind": None,
        "last_friction_event_id": None,
        "last_durable_error": None,
    })


def record_api_route_usage(
    subsystems: Any,
    *,
    path: str,
    method: str,
    status_code: int,
    conn: Any | None = None,
    request_body: dict[str, Any] | None = None,
    response_payload: dict[str, Any] | None = None,
    headers: Any | None = None,
) -> None:
    normalized_path = _normalize_path(path)
    if normalized_path not in _TRACKED_API_ROUTES:
        return
    route_conn = conn
    if route_conn is None:
        if subsystems is None or not hasattr(subsystems, "get_pg_conn"):
            record_surface_usage_failure(
                surface_kind="api",
                entrypoint_name=normalized_path,
                error=RuntimeError("surface usage postgres connection unavailable"),
            )
            return
        try:
            route_conn = subsystems.get_pg_conn()
        except Exception as exc:
            record_surface_usage_failure(
                surface_kind="api",
                entrypoint_name=normalized_path,
                error=exc,
            )
            return
    body_payload = _mapping_payload(request_body)
    response = _mapping_payload(response_payload)
    event_payload: dict[str, Any] = {
        "surface_kind": "api",
        "transport_kind": "http",
        "entrypoint_kind": "route",
        "entrypoint_name": normalized_path,
        "caller_kind": "direct",
        "http_method": str(method or "").strip().upper(),
        "status_code": int(status_code),
        "request_id": _header_value(headers, "x-request-id", "x-correlation-id"),
        "client_version": _header_value(headers, "x-client-version", "x-praxis-client-version"),
        "payload_size_bytes": _json_size_bytes(body_payload),
        "response_size_bytes": _json_size_bytes(response),
        "result_state": "error" if int(status_code) >= 400 else "ok",
        "reason_code": str(response.get("reason_code") or "").strip(),
        "metadata": {},
    }
    if normalized_path == "/api/trigger/:workflow_id":

        event_payload.update(
            _trigger_route_metrics(
                conn=route_conn,
                response_payload=response,
            )
        )
    elif normalized_path == "/query":
        event_payload.update(
            _query_route_metrics(
                request_body=body_payload,
                response_payload=response,
                status_code=int(status_code),
            )
        )
    metadata = event_payload.get("metadata")
    event_payload["metadata"] = {
        key: value
        for key, value in (metadata.items() if isinstance(metadata, dict) else [])
        if _metadata_value_is_present(value)
    }
    try:
        from storage.postgres import PostgresWorkflowSurfaceUsageRepository

        PostgresWorkflowSurfaceUsageRepository(route_conn).record_event(**event_payload)
    except Exception as exc:
        record_surface_usage_failure(
            surface_kind="api",
            entrypoint_name=normalized_path,
            error=exc,
            conn=route_conn,
        )
        return


__all__ = [
    "record_api_route_usage",
    "record_surface_usage_failure",
    "surface_usage_recorder_health",
]
