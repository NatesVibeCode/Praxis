"""Shared workflow validation helpers."""

from __future__ import annotations

from typing import Any

from runtime.workflow.execution_bundle import _VERIFICATION_REQUIRED_TASK_TYPES


def _authority_error_result(spec, message: str) -> dict[str, Any]:
    summary = spec.summary()
    details: list[dict[str, Any]] = []
    agent_resolution: dict[str, str] = {}
    for job in getattr(spec, "jobs", ()):
        label = str(job.get("label") or "")
        requested_slug = str(job.get("agent") or "").strip()
        detail = {
            "label": label,
            "requested_slug": requested_slug,
            "resolved_slug": None,
            "status": "authority_error",
            "message": message,
        }
        agent_resolution[requested_slug] = "authority_error"
        details.append(detail)
    return {
        "valid": False,
        "summary": summary,
        "agent_resolution": agent_resolution,
        "agent_resolution_details": details,
        "error": f"agent authority unavailable: {message}",
    }


def validate_workflow_spec(spec, *, pg_conn) -> dict[str, Any]:
    """Validate a loaded workflow spec against live Postgres authority."""
    from contracts.domain import validate_workflow_request
    from registry.agent_config import AgentRegistry
    from registry.native_runtime_profile_sync import (
        NativeRuntimeProfileSyncError,
        default_native_runtime_profile_ref,
    )
    from runtime.workflow_graph_compiler import (
        GraphWorkflowCompileError,
        compile_graph_workflow_request,
        spec_uses_graph_runtime,
    )

    summary = spec.summary()
    if spec_uses_graph_runtime(getattr(spec, "_raw", {})):
        try:
            request = compile_graph_workflow_request(spec._raw, conn=pg_conn)
        except GraphWorkflowCompileError as exc:
            return {
                "valid": False,
                "summary": summary,
                "graph_runtime": True,
                "error": str(exc),
                "reason_code": exc.reason_code,
                "details": dict(exc.details),
            }
        validation = validate_workflow_request(request)
        return {
            "valid": validation.is_valid,
            "summary": summary,
            "graph_runtime": True,
            "request_digest": validation.request_digest,
            "workflow_id": request.workflow_id,
            "request_id": request.request_id,
            "workflow_definition_id": request.workflow_definition_id,
            "reason_code": validation.reason_code,
            "errors": list(validation.errors),
            "node_count": len(request.nodes),
            "edge_count": len(request.edges),
        }

    try:
        registry = AgentRegistry.load_from_postgres(pg_conn)
    except Exception as exc:
        return _authority_error_result(spec, f"{type(exc).__name__}: {exc}")

    runtime_profile_ref = getattr(spec, "runtime_profile_ref", None)
    router = None
    unresolved = False
    agent_resolution: dict[str, str] = {}
    details: list[dict[str, Any]] = []

    for job in getattr(spec, "jobs", ()):
        label = str(job.get("label") or "")
        requested_slug = str(job.get("agent") or "").strip()
        detail = {
            "label": label,
            "requested_slug": requested_slug,
            "resolved_slug": None,
            "status": "unresolved",
        }
        if not requested_slug:
            detail["message"] = "Job is missing an agent route."
            unresolved = True
        elif requested_slug == "human" or requested_slug.startswith("integration/"):
            detail["status"] = "resolved"
            detail["resolved_slug"] = requested_slug
            detail["message"] = "Direct route does not require model authority."
        elif requested_slug.startswith("auto/"):
            try:
                if router is None:
                    from runtime.task_type_router import TaskTypeRouter

                    router = TaskTypeRouter(pg_conn)
                if not runtime_profile_ref:
                    try:
                        runtime_profile_ref = default_native_runtime_profile_ref(pg_conn)
                    except (AttributeError, NativeRuntimeProfileSyncError):
                        runtime_profile_ref = None
                chain = router.resolve_failover_chain(
                    requested_slug,
                    runtime_profile_ref=runtime_profile_ref,
                )
            except Exception as exc:
                chain = ()
                detail["message"] = str(exc)
            if chain:
                primary = chain[0]
                detail["status"] = "resolved"
                detail["resolved_slug"] = f"{primary.provider_slug}/{primary.model_slug}"
            else:
                unresolved = True
                detail.setdefault(
                    "message",
                    "No eligible agent route was found for the auto lane.",
                )
        else:
            resolved = registry.get(requested_slug)
            if resolved is None:
                unresolved = True
                detail["message"] = "Agent slug was not found in Postgres authority."
            else:
                resolved_slug = str(getattr(resolved, "slug", requested_slug) or requested_slug)
                detail["resolved_slug"] = resolved_slug
                detail["status"] = "aliased" if resolved_slug != requested_slug else "resolved"
        agent_resolution[requested_slug] = detail["status"]
        details.append(detail)

    # ── verify_refs enforcement for code task types ────────────────────────
    verification_warnings: list[str] = []
    for job in getattr(spec, "jobs", ()):
        task_type = str(job.get("task_type") or "").strip().lower()
        job_verify_refs = job.get("verify_refs") or []
        job_label = str(job.get("label") or "?")
        if task_type in _VERIFICATION_REQUIRED_TASK_TYPES and not job_verify_refs:
            verification_warnings.append(
                f"job '{job_label}': task_type '{task_type}' requires verify_refs "
                f"but none are specified — job will fail at the verification gate"
            )

    result: dict[str, Any] = {
        "valid": not unresolved,
        "summary": summary,
        "agent_resolution": agent_resolution,
        "agent_resolution_details": details,
    }
    if verification_warnings:
        result["verification_warnings"] = verification_warnings
    if unresolved:
        result["error"] = "one or more agent routes could not be resolved from Postgres authority"
    return result
