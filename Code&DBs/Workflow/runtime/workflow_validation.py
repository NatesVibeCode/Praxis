"""Shared workflow validation helpers."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from collections.abc import Mapping
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
    builders are errors unless the job explicitly opts into smoke-only
    passthrough echo with `allow_passthrough_echo=true`.
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
            allow_passthrough_echo = input_payload.get("allow_passthrough_echo") is True
        else:
            builder_path = ""
            allow_passthrough_echo = False
        if not builder_path:
            severity = "warning" if allow_passthrough_echo else "error"
            warnings.append({
                "kind": (
                    "deterministic_builder_passthrough_echo"
                    if allow_passthrough_echo
                    else "deterministic_builder_missing"
                ),
                "severity": severity,
                "label": label,
                "message": (
                    f"job '{label}' has adapter_type=deterministic_task but no "
                    f"'deterministic_builder' in inputs; "
                    + (
                        "allow_passthrough_echo=true permits this smoke-only node "
                        "to echo expected_outputs"
                        if allow_passthrough_echo
                        else "add a deterministic_builder or set allow_passthrough_echo=true "
                        "only for explicit smoke runs"
                    )
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

    provider_slugs = sorted({provider_slug for provider_slug, _adapter_type in provider_adapter_pairs})
    adapter_types = sorted({adapter_type for _provider_slug, adapter_type in provider_adapter_pairs})
    try:
        raw_rows = pg_conn.execute(
            """
            SELECT provider_slug, adapter_type, admitted_by_policy, policy_reason
            FROM provider_transport_admissions
            WHERE provider_slug = ANY($1::text[])
              AND adapter_type = ANY($2::text[])
            """,
            provider_slugs,
            adapter_types,
        )
        rows: dict[tuple[str, str], tuple[bool, str]] = {}
        for row in raw_rows or []:
            item = _row_mapping(row)
            provider_slug = str(item.get("provider_slug") or "").strip()
            adapter_type = str(item.get("adapter_type") or "").strip()
            if not provider_slug or not adapter_type:
                continue
            rows[(provider_slug, adapter_type)] = (
                bool(item.get("admitted_by_policy")),
                str(item.get("policy_reason") or ""),
            )
    except Exception as exc:
        warnings.append({
            "kind": "provider_admission_query_failed",
            "severity": "error",
            "label": None,
            "message": (
                "could not check provider_transport_admissions; provider "
                "transport admission authority is unavailable: "
                f"{type(exc).__name__}: {exc}"
            ),
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


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _provider_slug_from_agent(agent_slug: str) -> str:
    agent = str(agent_slug or "").strip()
    if not agent or agent == "human" or agent.startswith("integration/") or agent.startswith("auto/"):
        return ""
    if "/" not in agent:
        return ""
    provider_slug = agent.split("/", 1)[0].strip().lower()
    return provider_slug


def _provider_usage_detail_excerpt(details: object) -> str:
    if isinstance(details, str):
        return details[:240]
    if isinstance(details, Mapping):
        for key in ("stderr_excerpt", "error", "detail", "message"):
            value = details.get(key)
            if value not in (None, ""):
                return str(value)[:240]
        if details.get("rate_limited") is True:
            return "rate_limited=true"
    return ""


def _provider_refs_from_jobs(
    spec,
    *,
    agent_resolution_details: list[dict[str, Any]] | None = None,
    pg_conn: Any | None = None,
) -> dict[str, set[str]]:
    """Map provider_slug -> set of job labels that route through that provider.

    When pg_conn is supplied, jobs whose (provider, model) is registered with a
    non-empty cli_config.cmd_template are EXCLUDED from the refs. The graph
    compiler upgrades those jobs from llm_task to cli_llm at compile time
    (commit 91955353), so they do not consume the API lane and the
    provider-availability breaker should not block them.
    """
    refs: dict[str, set[str]] = {}

    def _record(label: str, agent_slug: str) -> None:
        provider_slug = _provider_slug_from_agent(agent_slug)
        if not provider_slug:
            return
        if pg_conn is not None:
            model_slug = ""
            if "/" in agent_slug:
                _provider, _, _rest = agent_slug.partition("/")
                model_slug = _rest.strip()
            try:
                from runtime.workflow_graph_materializer import _provider_uses_cli_transport

                if _provider_uses_cli_transport(provider_slug, model_slug, pg_conn):
                    return
            except Exception:
                pass
        refs.setdefault(provider_slug, set()).add(label)

    for job in getattr(spec, "jobs", ()) or ():
        label = str(job.get("label") or "?")
        _record(label, str(job.get("agent") or ""))

    for detail in agent_resolution_details or []:
        label = str(detail.get("label") or "?")
        _record(label, str(detail.get("resolved_slug") or ""))
    return refs


def _runtime_profile_ref_from_spec(spec, *, pg_conn) -> str | None:
    raw_snapshot = getattr(spec, "_raw", {})
    if isinstance(raw_snapshot, Mapping):
        runtime_profile_ref = raw_snapshot.get("runtime_profile_ref")
        if isinstance(runtime_profile_ref, str) and runtime_profile_ref.strip():
            return runtime_profile_ref.strip()
    runtime_profile_ref = getattr(spec, "runtime_profile_ref", None)
    if isinstance(runtime_profile_ref, str) and runtime_profile_ref.strip():
        return runtime_profile_ref.strip()

    try:
        from registry.native_runtime_profile_sync import default_native_runtime_profile_ref

        return default_native_runtime_profile_ref(pg_conn)
    except Exception:
        return None


def _selected_agent_slugs(
    spec,
    *,
    agent_resolution_details: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    selected: dict[str, str] = {}
    details_by_label = {
        str(detail.get("label") or "?"): str(detail.get("resolved_slug") or "").strip()
        for detail in agent_resolution_details or []
    }
    for job in getattr(spec, "jobs", ()) or ():
        label = str(job.get("label") or "?")
        raw_agent = str(job.get("agent") or "").strip()
        resolved_slug = details_by_label.get(label) or raw_agent
        provider_slug = _provider_slug_from_agent(resolved_slug)
        if provider_slug:
            selected[label] = resolved_slug
    return selected


def _preflight_runtime_profile_route_admission(
    spec,
    *,
    pg_conn,
    agent_resolution_details: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Confirm direct provider/model routes are admitted by the runtime profile."""

    selected = _selected_agent_slugs(
        spec,
        agent_resolution_details=agent_resolution_details,
    )
    if not selected:
        return []

    runtime_profile_ref = _runtime_profile_ref_from_spec(spec, pg_conn=pg_conn)
    if not runtime_profile_ref:
        return [{
            "kind": "runtime_profile_admission_unavailable",
            "severity": "warning",
            "label": None,
            "message": "could not resolve a runtime profile for provider/model admission preflight",
        }]

    try:
        projection_rows = pg_conn.execute(
            """
            SELECT provider_slug, model_slug
            FROM runtime_profile_admitted_routes
            WHERE runtime_profile_ref = $1
            """,
            runtime_profile_ref,
        )
    except Exception as exc:
        return [{
            "kind": "runtime_profile_admitted_routes_projection_unavailable",
            "severity": "error",
            "label": None,
            "message": (
                f"could not read admitted-route projection for runtime profile "
                f"{runtime_profile_ref!r}: {type(exc).__name__}: {exc}"
            ),
        }]

    admitted_slugs: set[str] = set()
    for row in projection_rows or []:
        item = _row_mapping(row)
        provider_slug = str(item.get("provider_slug") or "").strip()
        model_slug = str(item.get("model_slug") or "").strip()
        if provider_slug and model_slug:
            admitted_slugs.add(f"{provider_slug}/{model_slug}")
    warnings: list[dict[str, Any]] = []
    for label, selected_slug in sorted(selected.items()):
        if selected_slug in admitted_slugs:
            continue
        warnings.append({
            "kind": "runtime_profile_candidate_not_admitted",
            "severity": "error",
            "label": label,
            "message": (
                f"job '{label}' selects {selected_slug!r}, but runtime profile "
                f"{runtime_profile_ref!r} does not admit that provider/model. "
                "Run provider onboarding sync or fix provider_names/allowed_models "
                "authority before submitting."
            ),
            "runtime_profile_ref": runtime_profile_ref,
            "selected_slug": selected_slug,
            "admitted_candidate_slugs": sorted(admitted_slugs),
        })
    return warnings


def _preflight_provider_availability(
    spec,
    *,
    pg_conn,
    agent_resolution_details: list[dict[str, Any]] | None = None,
    circuit_breakers: Any | None = None,
) -> list[dict[str, Any]]:
    """Fail closed when a selected provider is known unavailable right now.

    Registration/admission answers "may this provider be used in principle".
    This check answers the operator-facing launch question: "is this provider
    usable at this moment?"  The durable source is the provider_usage heartbeat
    snapshots table; process-local circuit-breaker state is consulted only as
    a best-effort second read model for manual/open-circuit decisions.
    """
    warnings: list[dict[str, Any]] = []
    provider_refs = _provider_refs_from_jobs(
        spec,
        agent_resolution_details=agent_resolution_details,
        pg_conn=pg_conn,
    )
    if not provider_refs:
        return warnings

    provider_slugs = sorted(provider_refs)

    try:
        raw_rows = pg_conn.execute(
            """
            SELECT DISTINCT ON (subject_id)
                   subject_id,
                   subject_sub,
                   status,
                   summary,
                   details,
                   captured_at
              FROM heartbeat_probe_snapshots
             WHERE probe_kind = 'provider_usage'
               AND subject_id = ANY($1::text[])
               AND captured_at >= now() - interval '24 hours'
             ORDER BY subject_id, captured_at DESC
            """,
            provider_slugs,
        )
    except Exception as exc:
        warnings.append({
            "kind": "provider_availability_query_failed",
            "severity": "error",
            "label": None,
            "message": (
                "could not check provider_usage heartbeat snapshots; provider "
                "availability authority is unavailable: "
                f"{type(exc).__name__}: {exc}"
            ),
        })
        raw_rows = ()

    for row in raw_rows or ():
        item = _row_mapping(row)
        provider_slug = str(item.get("subject_id") or "").strip().lower()
        if provider_slug not in provider_refs:
            continue
        status = str(item.get("status") or "").strip().lower()
        if status not in {"degraded", "failed", "warning"}:
            continue
        labels = ", ".join(sorted(provider_refs[provider_slug])[:5])
        details = item.get("details")
        detail_excerpt = _provider_usage_detail_excerpt(details)
        captured_at = item.get("captured_at")
        severity = "error" if status in {"degraded", "failed"} else "warning"
        message = (
            f"provider {provider_slug!r} has latest provider_usage status "
            f"{status!r}; affected job(s): {labels or 'unknown'}"
        )
        if item.get("summary"):
            message += f"; summary: {item.get('summary')}"
        if detail_excerpt:
            message += f"; detail: {detail_excerpt}"
        if captured_at:
            message += f"; captured_at: {captured_at}"
        warnings.append({
            "kind": "provider_unavailable",
            "severity": severity,
            "label": None,
            "message": message,
        })

    durable_circuit_states: dict[str, dict[str, Any]] = {}
    try:
        raw_circuit_rows = pg_conn.execute(
            """
            SELECT
                provider_slug,
                effective_state AS state,
                runtime_state,
                manual_override_state,
                manual_override_reason
            FROM effective_provider_circuit_breaker_state
            WHERE provider_slug = ANY($1::text[])
            """,
            provider_slugs,
        )
        for row in raw_circuit_rows or ():
            item = _row_mapping(row)
            provider_slug = str(item.get("provider_slug") or "").strip().lower()
            if provider_slug:
                durable_circuit_states[provider_slug] = item
    except Exception as exc:
        warnings.append({
            "kind": "provider_circuit_query_failed",
            "severity": "error",
            "label": None,
            "message": (
                "could not check durable circuit-breaker authority: "
                f"{type(exc).__name__}: {exc}"
            ),
        })

    if circuit_breakers is None:
        try:
            from runtime.workflow._shared import _circuit_breakers

            circuit_breakers = _circuit_breakers()
            if circuit_breakers is None:
                warnings.append({
                    "kind": "provider_circuit_query_unavailable",
                    "severity": "warning",
                    "label": None,
                    "message": (
                        "could not load process-local circuit-breaker state; "
                        "continued provider availability preflight using durable "
                        "provider_usage and circuit-breaker authority only"
                    ),
                })
        except Exception as exc:
            warnings.append({
                "kind": "provider_circuit_query_failed",
                "severity": "error",
                "label": None,
                "message": (
                    "could not check circuit-breaker state: "
                    f"{type(exc).__name__}: {exc}"
                ),
            })
            circuit_breakers = None

    states = dict(durable_circuit_states)
    if circuit_breakers is not None:
        try:
            runtime_states = circuit_breakers.all_states()
            for provider_slug, state in runtime_states.items():
                states.setdefault(str(provider_slug).strip().lower(), state)
        except Exception as exc:
            warnings.append({
                "kind": "provider_circuit_query_failed",
                "severity": "error",
                "label": None,
                "message": (
                    "could not check circuit-breaker state: "
                    f"{type(exc).__name__}: {exc}"
                ),
            })
            states = dict(durable_circuit_states)
        for provider_slug in provider_slugs:
            state = _row_mapping(states.get(provider_slug))
            manual_state = str(state.get("manual_override_state") or "").upper()
            if str(state.get("state") or "").upper() != "OPEN":
                if manual_state != "OPEN":
                    continue
            if manual_state == "OPEN":
                state = {
                    **state,
                    "manual_override": {
                        "override_state": "OPEN",
                        "rationale": state.get("manual_override_reason") or "",
                    },
                }
            if str(state.get("state") or "").upper() != "OPEN" and manual_state != "OPEN":
                continue
            labels = ", ".join(sorted(provider_refs[provider_slug])[:5])
            override = state.get("manual_override")
            rationale = ""
            if isinstance(override, Mapping):
                rationale = str(override.get("rationale") or "").strip()
            message = (
                f"provider {provider_slug!r} circuit breaker is OPEN; "
                f"affected job(s): {labels or 'unknown'}"
            )
            if rationale:
                message += f"; rationale: {rationale}"
            warnings.append({
                "kind": "provider_circuit_open",
                "severity": "error",
                "label": None,
                "message": message,
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
    workspace_roots_cache: tuple[Path, ...] | None = None
    workspace_roots_failed = False

    def _authority_workspace_roots() -> tuple[Path, ...]:
        nonlocal workspace_roots_cache
        nonlocal workspace_roots_failed
        if workspace_roots_cache is not None:
            return workspace_roots_cache
        if workspace_roots_failed:
            return ()
        try:
            workspace_roots_cache = authority_workspace_roots()
            return workspace_roots_cache
        except Exception as exc:
            workspace_roots_failed = True
            warnings.append({
                "kind": "workdir_authority_unavailable",
                "severity": "warning",
                "label": None,
                "message": (
                    "could not resolve authority workspace roots while checking "
                    f"workdir drift: {type(exc).__name__}: {exc}"
                ),
            })
            return ()

    def _is_host_specific_user_path(path: Path) -> bool:
        parts = path.parts
        return (
            len(parts) >= 3
            and (
                parts[0:2] == ("/", "Users")
                or parts[0:3] == ("/", "Volumes", "Users")
                or parts[0:2] == ("/", "home")
            )
        )

    def _suggest_container_path(path_obj: Path, roots: tuple[Path, ...]) -> str | None:
        for prefix in roots:
            try:
                rel = path_obj.relative_to(prefix)
            except ValueError:
                continue
            if rel == Path("."):
                return str(container_workspace_root())
            return str(container_workspace_root() / rel)
        return None

    def _check_path(label: str | None, field: str, value: str) -> None:
        path = (value or "").strip()
        if not path or not os.path.isabs(path):
            return
        path_obj = Path(path)
        authority_roots = _authority_workspace_roots()
        suggestion = _suggest_container_path(path_obj, authority_roots)
        if _is_host_specific_user_path(path_obj) and authority_roots and suggestion is None:
            warnings.append({
                "kind": "workspace_path_outside_authority",
                "severity": "warning",
                "label": label,
                "message": (
                    f"{field}={path!r} is a host-specific absolute path outside "
                    "the active workspace authority. Prefer a repo-relative path, "
                    "the runtime materialized workdir, or PRAXIS_HOST_WORKSPACE_ROOT "
                    "instead of baking a user-local checkout path into the spec."
                ),
            })
            return
        if os.path.exists(path):
            return
        # Path does not exist at the current vantage — suggest the translation
        # if the path looks like a known host-mount sibling.
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

    raw = getattr(spec, "_raw", {}) or {}
    top_workdir = str(getattr(spec, "workdir", "") or raw.get("workdir") or "")
    _check_path(None, "workdir", top_workdir)
    _check_path(None, "target_repo", str(raw.get("target_repo") or ""))

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
        rows = pg_conn.execute(
            "SELECT definition_version, status FROM workflow_definitions WHERE workflow_id = $1",
            workflow_id,
        )
    except Exception as exc:
        warnings.append({
            "kind": "workflow_id_collision_query_failed",
            "severity": "warning",
            "label": None,
            "message": f"could not check workflow_definitions: {type(exc).__name__}: {exc}",
        })
        return warnings
    if rows:
        versions = ", ".join(
            str(_row_mapping(row).get("definition_version") or "")
            for row in rows
        ).strip(", ") or "unknown"
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
    lowered_message = message.lower()
    if any(
        needle in lowered_message
        for needle in (
            "gaierror",
            "nodename nor servname",
            "name or service not known",
            "temporary failure in name resolution",
        )
    ):
        remediation_hint = (
            "The resolved WORKFLOW_DATABASE_URL host is not reachable from this shell. "
            "Fix the registry/runtime DSN or the DNS/network route for that host, then rerun validate."
        )
    elif "workflow_database_url" in lowered_message or "postgresconfigurationerror" in lowered_message:
        remediation_hint = (
            "Resolve WORKFLOW_DATABASE_URL from the registry/runtime authority, then rerun validate. "
            "If the URL came from a repo .env or launcher seed, update that source."
        )
    else:
        remediation_hint = (
            "Resolve the workflow database authority and rerun validate."
        )
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
        "remediation_hint": remediation_hint,
    }


def validate_workflow_spec(spec, *, pg_conn) -> dict[str, Any]:
    """Validate a loaded workflow spec against live Postgres authority.

    Accepts either a ``WorkflowSpec`` instance or a raw dict (which is coerced
    via ``WorkflowSpec.from_dict``). This is the front-door validator used by
    MCP surfaces, CLI tools, and standalone scripts — so dicts must not crash.
    """
    from contracts.domain import validate_workflow_request
    from registry.agent_config import AgentRegistry
    from registry.native_runtime_profile_sync import (
        NativeRuntimeProfileSyncError,
        default_native_runtime_profile_ref,
    )
    from runtime.workflow_graph_materializer import (
        GraphWorkflowCompileError,
        compile_graph_workflow_request,
        spec_uses_graph_runtime,
    )
    from runtime.workflow_type_contracts import validate_workflow_request_type_flow
    from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError

    if isinstance(spec, dict):
        try:
            spec = WorkflowSpec.from_dict(spec)
        except WorkflowSpecError as exc:
            return {
                "valid": False,
                "error": f"invalid workflow spec dict: {exc}",
                "reason_code": "workflow.spec.invalid_dict",
            }

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
        type_flow_errors = validate_workflow_request_type_flow(request)
        errors = list(validation.errors) + type_flow_errors
        return {
            "valid": validation.is_valid and not type_flow_errors,
            "summary": summary,
            "graph_runtime": True,
            "request_digest": validation.request_digest,
            "workflow_id": request.workflow_id,
            "request_id": request.request_id,
            "workflow_definition_id": request.workflow_definition_id,
            "reason_code": (
                "workflow.type_flow.invalid"
                if validation.is_valid and type_flow_errors
                else validation.reason_code
            ),
            "errors": errors,
            "type_flow_errors": type_flow_errors,
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
    spec_verify_refs = getattr(spec, "verify_refs", None) or []
    verification_errors: list[str] = []
    verification_preflight_errors: list[dict[str, Any]] = []
    for job in getattr(spec, "jobs", ()):
        task_type = str(job.get("task_type") or "").strip().lower()
        job_verify_refs = job.get("verify_refs") or []
        job_label = str(job.get("label") or "?")
        if task_type in _VERIFICATION_REQUIRED_TASK_TYPES and not (job_verify_refs or spec_verify_refs):
            message = (
                f"task_type '{task_type}' requires verify_refs but none are specified"
            )
            verification_errors.append(f"job '{job_label}': {message}")
            verification_preflight_errors.append({
                "kind": "verify_refs_missing",
                "severity": "error",
                "label": job_label,
                "message": message,
            })

    # --- Additional preflight checks -----------------------------------
    # These catch classes of errors that would otherwise only surface at
    # run time (builder import failure → silent echo, admission denial →
    # adapter.transport_unsupported mid-run, workflow_id collision →
    # psycopg UniqueViolation on submit).
    preflight_warnings: list[dict[str, Any]] = []
    preflight_warnings.extend(verification_preflight_errors)
    preflight_warnings.extend(_preflight_deterministic_builders(spec))
    preflight_warnings.extend(_preflight_provider_admissions(spec, pg_conn=pg_conn))
    preflight_warnings.extend(
        _preflight_runtime_profile_route_admission(
            spec,
            pg_conn=pg_conn,
            agent_resolution_details=details,
        )
    )
    preflight_warnings.extend(
        _preflight_provider_availability(
            spec,
            pg_conn=pg_conn,
            agent_resolution_details=details,
        )
    )
    preflight_warnings.extend(_preflight_workflow_id_collision(spec, pg_conn=pg_conn))
    preflight_warnings.extend(_preflight_workdir_drift(spec))
    preflight_errors = [w for w in preflight_warnings if w.get("severity") == "error"]

    result: dict[str, Any] = {
        "valid": not unresolved and not preflight_errors,
        "summary": summary,
        "agent_resolution": agent_resolution,
        "agent_resolution_details": details,
    }
    if verification_errors:
        result["verification_errors"] = verification_errors
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
