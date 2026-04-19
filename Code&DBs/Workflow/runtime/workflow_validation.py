"""Shared workflow validation helpers."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any

from runtime.workspace_paths import authority_workspace_roots, container_workspace_root
from runtime.workflow.execution_bundle import _VERIFICATION_REQUIRED_TASK_TYPES


# --------------------------------------------------------------------------
# Preflight checks
# --------------------------------------------------------------------------
# Each `_preflight_*` helper inspects the spec + live DB to catch a class of
# errors that would otherwise only surface much later — typically at run time
# with an opaque failure. They emit a list of warning dicts:
#   {"kind": str, "severity": "warning"|"error", "label": str|None, "message": str}
# `severity == "error"` contributes to `valid=False`.


def _preflight_deterministic_builders(spec) -> list[dict[str, Any]]:
    """For each deterministic_task job, confirm the dotted-path builder in
    `input_payload.deterministic_builder` imports to a callable. Missing
    builders cause the adapter to silently echo `expected_outputs` back (see
    `adapters/deterministic.py::DeterministicTaskAdapter.execute`), which is
    the biggest source of "green receipts over phantom work" in the system.
    """
    warnings: list[dict[str, Any]] = []
    for job in getattr(spec, "jobs", ()) or ():
        adapter_type = str(job.get("adapter_type") or "").strip()
        if adapter_type != "deterministic_task":
            continue
        label = str(job.get("label") or "?")
        input_payload = job.get("inputs") or job.get("input_payload") or {}
        if isinstance(input_payload, dict):
            builder_path = str(input_payload.get("deterministic_builder") or "").strip()
        else:
            builder_path = ""
        if not builder_path:
            # Not an error per se — passthrough-echo is legal for smoke runs.
            # But flag at warning severity so the operator sees it.
            warnings.append({
                "kind": "deterministic_builder_missing",
                "severity": "warning",
                "label": label,
                "message": (
                    f"job '{label}' has adapter_type=deterministic_task but no "
                    f"'deterministic_builder' in inputs; this node will echo "
                    f"expected_outputs rather than run real work"
                ),
            })
            continue
        module_name, _, function_name = builder_path.rpartition(".")
        if not module_name or not function_name:
            warnings.append({
                "kind": "deterministic_builder_malformed",
                "severity": "error",
                "label": label,
                "message": (
                    f"job '{label}': deterministic_builder {builder_path!r} is not "
                    f"a dotted module path like 'pkg.mod.function'"
                ),
            })
            continue
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            warnings.append({
                "kind": "deterministic_builder_import_failed",
                "severity": "error",
                "label": label,
                "message": (
                    f"job '{label}': cannot import builder module "
                    f"{module_name!r}: {exc}"
                ),
            })
            continue
        builder = getattr(module, function_name, None)
        if not callable(builder):
            warnings.append({
                "kind": "deterministic_builder_not_callable",
                "severity": "error",
                "label": label,
                "message": (
                    f"job '{label}': {module_name}.{function_name} is not a "
                    f"callable attribute (found {type(builder).__name__})"
                ),
            })
    return warnings


def _preflight_provider_admissions(spec, *, pg_conn) -> list[dict[str, Any]]:
    """Query `provider_transport_admissions` for each agent's provider_slug
    + adapter_type and warn if `admitted_by_policy=false`. This was the
    root cause of `adapter.transport_unsupported` dead-ends in-session.
    """
    warnings: list[dict[str, Any]] = []
    provider_adapter_pairs: set[tuple[str, str]] = set()
    for job in getattr(spec, "jobs", ()) or ():
        adapter_type = str(job.get("adapter_type") or "").strip()
        if adapter_type not in {"cli_llm", "llm_task"}:
            continue
        agent = str(job.get("agent") or "").strip()
        if not agent or "/" not in agent:
            continue
        provider_slug = agent.split("/", 1)[0].strip().lower()
        if not provider_slug:
            continue
        provider_adapter_pairs.add((provider_slug, adapter_type))

    if not provider_adapter_pairs:
        return warnings

    try:
        cur = pg_conn.cursor()
        cur.execute(
            """
            SELECT provider_slug, adapter_type, admitted_by_policy, policy_reason
            FROM provider_transport_admissions
            WHERE (provider_slug, adapter_type) = ANY($1::record[])
            """,
            (list(provider_adapter_pairs),),
        )
        rows = {(str(r[0]), str(r[1])): (bool(r[2]), str(r[3] or "")) for r in cur.fetchall()}
    except Exception as exc:
        # Don't fail preflight on DB hiccup — that's not what this check is
        # for. Surface a non-fatal warning so the operator knows the gate
        # couldn't be evaluated.
        warnings.append({
            "kind": "provider_admission_query_failed",
            "severity": "warning",
            "label": None,
            "message": f"could not check provider_transport_admissions: {type(exc).__name__}: {exc}",
        })
        return warnings

    for provider_slug, adapter_type in sorted(provider_adapter_pairs):
        entry = rows.get((provider_slug, adapter_type))
        if entry is None:
            warnings.append({
                "kind": "provider_admission_missing",
                "severity": "error",
                "label": None,
                "message": (
                    f"provider_transport_admissions has no row for "
                    f"({provider_slug}, {adapter_type}); onboard the provider "
                    f"via 'praxis_provider_onboard' before submitting"
                ),
            })
            continue
        admitted, reason = entry
        if not admitted:
            warnings.append({
                "kind": "provider_admission_denied",
                "severity": "error",
                "label": None,
                "message": (
                    f"provider_transport_admissions.admitted_by_policy is false "
                    f"for ({provider_slug}, {adapter_type})"
                    + (f": {reason}" if reason else "")
                    + "; re-run 'praxis_provider_onboard' or fix the credential source"
                ),
            })
    return warnings


def _preflight_workdir_drift(spec) -> list[dict[str, Any]]:
    """Warn when the spec's workdir (top-level or per-job) references a path
    that doesn't exist in the current process's filesystem view.

    This catches the most common cross-environment footgun:
      - spec authored from one filesystem view
      - submitted via an MCP/CLI running in another filesystem view
      - bundle hash includes the absolute workdir, so host-submitted runs
        produce a digest the container worker cannot reproduce, failing with
        `evidence.route_identity_mismatch` at a confusing point downstream

    We only emit warnings here (not errors) because an operator who knows
    what they're doing can legitimately submit from a path-rebasing wrapper.
    """
    warnings: list[dict[str, Any]] = []

    def _check_path(label: str | None, field: str, value: str) -> None:
        path = (value or "").strip()
        if not path or not os.path.isabs(path):
            return
        if os.path.exists(path):
            return
        # Path does not exist at the current vantage — suggest the translation
        # if the path looks like a known host-mount sibling.
        suggestion: str | None = None
        path_obj = Path(path)
        for prefix in authority_workspace_roots():
            try:
                rel = path_obj.relative_to(prefix)
            except ValueError:
                continue
            if rel == Path("."):
                suggestion = str(container_workspace_root())
            else:
                suggestion = str(container_workspace_root() / rel)
                break
        message = (
            f"{field}={path!r} does not exist in the current process filesystem; "
            "this usually means the spec was authored on the host but is being "
            "submitted from inside the worker container (or vice-versa). "
            "The bundle hash includes this absolute path — a drift here causes "
            "`evidence.route_identity_mismatch` failures at run time."
        )
        if suggestion:
            message += f" Try: {field}={suggestion!r}"
        warnings.append({
            "kind": "workdir_path_missing",
            "severity": "warning",
            "label": label,
            "message": message,
        })

    top_workdir = str(getattr(spec, "workdir", "") or (getattr(spec, "_raw", {}) or {}).get("workdir") or "")
    _check_path(None, "workdir", top_workdir)

    for job in getattr(spec, "jobs", ()) or ():
        job_workdir = str(job.get("workdir") or "")
        if not job_workdir or job_workdir == top_workdir:
            continue
        _check_path(str(job.get("label") or "?"), "job.workdir", job_workdir)

    return warnings


def _preflight_workflow_id_collision(spec, *, pg_conn) -> list[dict[str, Any]]:
    """Warn if the spec's workflow_id already has a registered definition.
    Paired with the `WorkflowSubmitConflict` translator — preflight catches
    this before the user has wasted a submission round-trip.
    """
    warnings: list[dict[str, Any]] = []
    workflow_id = str(
        getattr(spec, "workflow_id", "")
        or (getattr(spec, "_raw", {}) or {}).get("workflow_id")
        or ""
    ).strip()
    if not workflow_id:
        return warnings
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT definition_version, status FROM workflow_definitions WHERE workflow_id = $1",
            (workflow_id,),
        )
        rows = cur.fetchall()
    except Exception as exc:
        warnings.append({
            "kind": "workflow_id_collision_query_failed",
            "severity": "warning",
            "label": None,
            "message": f"could not check workflow_definitions: {type(exc).__name__}: {exc}",
        })
        return warnings
    if rows:
        versions = ", ".join(str(r[0]) for r in rows) or "unknown"
        warnings.append({
            "kind": "workflow_id_already_registered",
            "severity": "warning",
            "label": None,
            "message": (
                f"workflow_id {workflow_id!r} already has registered definition(s) "
                f"(version(s): {versions}); submit will raise "
                f"WorkflowSubmitConflict unless you bump workflow_id or drop "
                f"existing definitions"
            ),
        })
    return warnings


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

    # --- Additional preflight checks -----------------------------------
    # These catch classes of errors that would otherwise only surface at
    # run time (builder import failure → silent echo, admission denial →
    # adapter.transport_unsupported mid-run, workflow_id collision →
    # psycopg UniqueViolation on submit).
    preflight_warnings: list[dict[str, Any]] = []
    preflight_warnings.extend(_preflight_deterministic_builders(spec))
    preflight_warnings.extend(_preflight_provider_admissions(spec, pg_conn=pg_conn))
    preflight_warnings.extend(_preflight_workflow_id_collision(spec, pg_conn=pg_conn))
    preflight_warnings.extend(_preflight_workdir_drift(spec))
    preflight_errors = [w for w in preflight_warnings if w.get("severity") == "error"]

    result: dict[str, Any] = {
        "valid": not unresolved and not preflight_errors,
        "summary": summary,
        "agent_resolution": agent_resolution,
        "agent_resolution_details": details,
    }
    if verification_warnings:
        result["verification_warnings"] = verification_warnings
    if preflight_warnings:
        result["preflight_warnings"] = preflight_warnings
    if unresolved:
        result["error"] = "one or more agent routes could not be resolved from Postgres authority"
    elif preflight_errors:
        first = preflight_errors[0]
        label = first.get("label")
        prefix = f"job '{label}': " if label else ""
        result["error"] = f"{prefix}{first['message']}"
        result["error_kind"] = first.get("kind")
    return result
