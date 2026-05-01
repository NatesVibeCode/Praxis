"""Execution helpers backed by the unified sandbox runtime."""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from adapters.docker_runner import normalize_command_parts_for_docker
from registry.provider_execution_registry import build_command, resolve_api_key_env_vars
from runtime.load_balancer import get_load_balancer
from runtime.execution_transport import resolve_execution_transport
from runtime.host_resource_admission import (
    HostResourceAdmissionError,
    HostResourceAdmissionUnavailable,
    HostResourceCapacityError,
    hold_host_resources_for_sandbox,
)
from runtime.sandbox_runtime import (
    SandboxRuntime,
    derive_sandbox_identity,
    _execution_shard_paths,
    _path_matches_filter,
)
from runtime.workflow.mcp_bridge import (
    augment_cli_command_for_workflow_mcp,
    workflow_mcp_workspace_overlays,
)
from runtime.workflow.mcp_session import mint_workflow_mcp_session_token
from runtime.workflow.decision_context import decision_workspace_overlays
from runtime.workspace_paths import workflow_root


logger = logging.getLogger(__name__)
_PRE_RELOAD_PROVIDER_SLOT_ACQUISITION_ERROR = globals().get(
    "ProviderSlotAcquisitionError"
)

_WORKFLOW_MODEL_NETWORK_ENV = "PRAXIS_WORKFLOW_MODEL_NETWORK"
_EXECUTION_BUNDLE_ENV = "PRAXIS_EXECUTION_BUNDLE"
_ALLOWED_MCP_TOOLS_ENV = "PRAXIS_ALLOWED_MCP_TOOLS"
_LEGACY_ALLOWED_MCP_TOOLS_ENV = "PRAXIS_ALLOWED_MCP_TOOLS"
_ALLOWED_SKILLS_ENV = "PRAXIS_ALLOWED_SKILLS"
# Uniform shell-tool surface: sandbox-side `praxis` CLI reads these to POST
# into the workflow MCP bridge. Replaces per-provider MCP-config plumbing.
_MCP_URL_ENV = "PRAXIS_WORKFLOW_MCP_URL"
_MCP_TOKEN_ENV = "PRAXIS_WORKFLOW_MCP_TOKEN"
_SANDBOX_PATH_PREFIX_ENV = "PRAXIS_SANDBOX_PATH_PREFIX"
_PROVIDER_SLOT_BYPASS: ContextVar[bool] = ContextVar(
    "workflow_provider_slot_bypass",
    default=False,
)


class WorkflowMcpSessionTokenError(RuntimeError):
    """Raised when workflow MCP tools were requested but auth could not be minted."""

    reason_code = "workflow_mcp.session_token_unavailable"

    def __init__(self, message: str, *, details: dict[str, Any]) -> None:
        super().__init__(message)
        self.details = details


def _env_flag_enabled(name: str, *, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _load_env_secret_from_keychain(env: dict[str, str], key_name: str) -> None:
    try:
        from adapters.keychain import resolve_secret

        value = resolve_secret(key_name)
    except Exception:
        return
    if value:
        env[key_name] = value.strip()


def _resolve_api_key_env_name(provider_slug: str, env: dict[str, str]) -> str | None:
    env_names = resolve_api_key_env_vars(provider_slug)
    for env_name in env_names:
        if str(env.get(env_name, "")).strip():
            return env_name
    return env_names[0] if env_names else None


def _sandbox_path_prefix() -> str:
    prefix = str(os.environ.get(_SANDBOX_PATH_PREFIX_ENV, "")).strip()
    if not prefix:
        return ""
    return prefix if prefix.endswith(":") else f"{prefix}:"


def _sanitize_base_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if os.environ.get(_MCP_URL_ENV):
        env[_MCP_URL_ENV] = str(os.environ[_MCP_URL_ENV]).strip()
    for key in (
        "CLAUDECODE",
        "CLAUDE_CODE_ENTRYPOINT",
        "CLAUDE_CODE_ENTRY_POINT",
        "CLAUDE_AGENT_SDK_VERSION",
        "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST",
        "CLAUDE_CODE_ENABLE_ASK_USER_QUESTION_TOOL",
        "CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES",
        "CLAUDE_CODE_DISABLE_CRON",
    ):
        env.pop(key, None)
    env["PATH"] = _sandbox_path_prefix() + os.environ.get("PATH", "")
    return env


def _ripgrep_config_for_workdir(
    workdir: str,
    *,
    execution_bundle: dict[str, Any] | None = None,
) -> str | None:
    current = Path(workdir).resolve()
    shard_filter = _execution_shard_paths({"execution_bundle": execution_bundle or {}})
    for directory in (current, *current.parents):
        candidate = directory / ".ripgreprc"
        if candidate.is_file():
            try:
                relpath = os.path.relpath(candidate, current)
            except ValueError:
                relpath = str(candidate)
            if shard_filter and (
                relpath.startswith("..")
                or os.path.isabs(relpath)
                or not _path_matches_filter(relpath, shard_filter)
            ):
                return None
            return relpath
        if (directory / ".git").exists():
            break
    return None


def _provider_api_key_names(provider_slug: str) -> tuple[str, ...]:
    if not provider_slug:
        return ()
    try:
        from registry import provider_execution_registry as provider_registry_mod

        profile = provider_registry_mod.get_profile(provider_slug)
    except Exception:
        profile = None
    if profile is None:
        return ()
    return tuple(profile.api_key_env_vars)


def _build_execution_env(
    agent_config,
    *,
    workdir: str,
    execution_bundle: dict[str, Any] | None,
) -> dict[str, str]:
    env = _sanitize_base_env()
    provider_slug = str(getattr(agent_config, "provider", "") or "").strip().lower()
    provider_api_key_names = _provider_api_key_names(provider_slug)
    export_names = set(provider_api_key_names)
    policy = getattr(agent_config, "sandbox_policy", None)
    secret_allowlist = tuple(getattr(policy, "secret_allowlist", ()) or ())
    bundle_sandbox_profile = _sandbox_profile_from_bundle(execution_bundle)
    if isinstance(bundle_sandbox_profile, dict):
        bundle_allowlist = bundle_sandbox_profile.get("secret_allowlist")
        if isinstance(bundle_allowlist, list):
            secret_allowlist = tuple(
                list(secret_allowlist)
                + [str(name).strip() for name in bundle_allowlist if str(name).strip()]
            )
    export_names.update(str(name).strip() for name in secret_allowlist if str(name).strip())
    for key_name in sorted(export_names):
        _load_env_secret_from_keychain(env, key_name)
    selected_api_key_env = _resolve_api_key_env_name(provider_slug, env)

    sandbox_env = {
        key_name: str(env[key_name]).strip()
        for key_name in sorted(export_names)
        if str(env.get(key_name, "")).strip()
    }
    if selected_api_key_env:
        for key_name in provider_api_key_names:
            if key_name != selected_api_key_env:
                sandbox_env.pop(key_name, None)
    if execution_bundle:
        encoded_bundle = json.dumps(
            execution_bundle,
            sort_keys=True,
            default=str,
        )
        sandbox_env[_EXECUTION_BUNDLE_ENV] = encoded_bundle
        mcp_tool_names = execution_bundle.get("mcp_tool_names")
        if isinstance(mcp_tool_names, list) and mcp_tool_names:
            allowed_tools = ",".join(str(name) for name in mcp_tool_names)
            sandbox_env[_ALLOWED_MCP_TOOLS_ENV] = allowed_tools
            sandbox_env[_LEGACY_ALLOWED_MCP_TOOLS_ENV] = allowed_tools
            # Uniform shell-tool surface: every sandbox (codex/claude/gemini)
            # gets a `praxis` CLI shim baked into its image. The shim POSTs
            # to the workflow MCP bridge using these two env vars. This
            # replaces per-provider MCP-config wiring (which rotted on every
            # CLI upgrade — see architecture-policy::sandbox::
            # uniform-shell-tool-surface). The CLI itself does not need any
            # MCP configuration; the agent invokes tools via the shell.
            mcp_url = str(env.get(_MCP_URL_ENV, "")).strip()
            if mcp_url:
                sandbox_env[_MCP_URL_ENV] = mcp_url
                try:
                    sandbox_env[_MCP_TOKEN_ENV] = mint_workflow_mcp_session_token(
                        run_id=str(execution_bundle.get("run_id") or "").strip() or None,
                        workflow_id=str(execution_bundle.get("workflow_id") or "").strip() or None,
                        job_label=str(execution_bundle.get("job_label") or "").strip(),
                        allowed_tools=[str(name) for name in mcp_tool_names],
                        source_refs=[
                            str(ref)
                            for ref in execution_bundle.get("source_refs", [])
                            if str(ref).strip()
                        ]
                        if isinstance(execution_bundle.get("source_refs"), list)
                        else [],
                        access_policy=execution_bundle.get("access_policy")
                        if isinstance(execution_bundle.get("access_policy"), dict)
                        else {},
                        agent_slug=provider_slug,
                    )
                except Exception as exc:
                    raise WorkflowMcpSessionTokenError(
                        (
                            "workflow MCP session token minting failed; refusing "
                            "legacy MCP fallback"
                        ),
                        details={
                            "reason_code": WorkflowMcpSessionTokenError.reason_code,
                            "provider_slug": provider_slug,
                            "run_id": str(execution_bundle.get("run_id") or ""),
                            "workflow_id": str(execution_bundle.get("workflow_id") or ""),
                            "job_label": str(execution_bundle.get("job_label") or ""),
                            "allowed_tools": [str(name) for name in mcp_tool_names],
                            "cause_type": type(exc).__name__,
                            "cause": str(exc),
                        },
                    ) from exc
        skill_refs = execution_bundle.get("skill_refs")
        if isinstance(skill_refs, list) and skill_refs:
            sandbox_env[_ALLOWED_SKILLS_ENV] = ",".join(str(name) for name in skill_refs)
    # Apply provider-specific sandbox env overrides from profile
    from registry.provider_execution_registry import get_profile as _get_env_profile
    _env_profile = _get_env_profile(provider_slug)
    if _env_profile and _env_profile.sandbox_env_overrides:
        overrides = _env_profile.sandbox_env_overrides
        for key in overrides.get("strip", []):
            sandbox_env.pop(key, None)
        for key, value in overrides.get("set", {}).items():
            sandbox_env[key] = str(value)
        if overrides.get("set_home"):
            sandbox_env["HOME"] = os.path.expanduser("~")
    sandbox_env["PYTHONPATH"] = str(workflow_root())
    sandbox_env["PATH"] = env["PATH"]
    ripgrep_config = _ripgrep_config_for_workdir(
        workdir,
        execution_bundle=execution_bundle,
    )
    if ripgrep_config:
        sandbox_env["RIPGREP_CONFIG_PATH"] = ripgrep_config
    return sandbox_env


def _sandbox_profile_from_bundle(
    execution_bundle: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(execution_bundle, dict):
        return None
    value = execution_bundle.get("sandbox_profile")
    return dict(value) if isinstance(value, dict) else None


def _sandbox_provider_for_execution(
    agent_config,
    execution_bundle: dict[str, Any] | None,
) -> str:
    bundle_profile = _sandbox_profile_from_bundle(execution_bundle)
    if isinstance(bundle_profile, dict):
        explicit = str(bundle_profile.get("sandbox_provider") or "").strip()
        if explicit:
            return explicit
    return resolve_execution_transport(agent_config).sandbox_provider


@contextmanager
def provider_slot_bypass():
    """Disable provider load-balancer acquisition for one nested call chain."""

    token = _PROVIDER_SLOT_BYPASS.set(True)
    try:
        yield
    finally:
        _PROVIDER_SLOT_BYPASS.reset(token)


class ProviderSlotAcquisitionError(RuntimeError):
    """Raised when the provider load balancer cannot resolve a slot.

    BUG-3C9ECE97: distinct from the normal "at capacity" path (which
    returns ``False`` from the slot context manager). This signals that
    the load balancer itself — the admission authority — is unreachable
    or broken. Callers must fail closed with a structured failure dict,
    not silently treat admission as granted.
    """

    def __init__(self, provider_slug: str, cause: BaseException):
        self.provider_slug = provider_slug
        RuntimeError.__init__(
            self,
            f"load balancer unavailable for provider={provider_slug!r}: {cause}"
        )


if isinstance(_PRE_RELOAD_PROVIDER_SLOT_ACQUISITION_ERROR, type):
    _PRE_RELOAD_PROVIDER_SLOT_ACQUISITION_ERROR.__doc__ = (
        ProviderSlotAcquisitionError.__doc__
    )
    _PRE_RELOAD_PROVIDER_SLOT_ACQUISITION_ERROR.__init__ = (
        ProviderSlotAcquisitionError.__init__
    )
    ProviderSlotAcquisitionError = _PRE_RELOAD_PROVIDER_SLOT_ACQUISITION_ERROR


def _provider_slot(provider_slug: str):
    """Acquire the provider concurrency slot unless a parent already owns it.

    Returns a context manager whose ``__enter__`` yields ``True`` on slot
    granted or ``False`` when the provider is at capacity.

    The explicit ``provider_slot_bypass()`` contextvar is the only
    supported admission bypass — used by nested calls that already hold
    the parent's slot. Infrastructure failures (load balancer offline,
    Postgres down behind the balancer, etc.) are NOT a bypass case; they
    raise :class:`ProviderSlotAcquisitionError` so the caller fails
    closed with a structured error.
    """

    if _PROVIDER_SLOT_BYPASS.get() or not provider_slug:
        return nullcontext(True)
    try:
        return get_load_balancer().slot(provider_slug)
    except Exception as exc:
        # BUG-3C9ECE97: never mask load-balancer failures as admission
        # success. The previous branch returned nullcontext(True) on any
        # exception, which let infrastructure outages silently overcommit
        # providers. Surface a typed error so call sites can emit a
        # structured failure record and so tests can pin the contract.
        logger.warning(
            "provider slot acquisition failed for %s (load balancer unavailable): %s",
            provider_slug,
            exc,
        )
        raise ProviderSlotAcquisitionError(provider_slug, exc) from exc


def _provider_capacity_failure(provider_slug: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "exit_code": 1,
        "stdout": "",
        "stderr": f"Provider at capacity: {provider_slug}",
        "error_code": "provider.capacity",
    }


def _provider_slot_acquisition_failure(
    provider_slug: str,
    exc: BaseException,
) -> dict[str, Any]:
    """Structured failure record for a :class:`ProviderSlotAcquisitionError`.

    Uses ``error_code=provider_slot_acquisition_error`` (distinct from
    ``route.unhealthy``) so dashboards and healers can tell
    infrastructure-level admission outages apart from simple capacity
    pressure.
    """

    return {
        "status": "failed",
        "exit_code": 1,
        "stdout": "",
        "stderr": (
            f"Provider slot acquisition failed (load balancer unavailable) "
            f"for {provider_slug}: {exc}"
        ),
        "error_code": "provider_slot_acquisition_error",
    }


def _host_resource_admission_failure(exc: HostResourceAdmissionError) -> dict[str, Any]:
    return {
        "status": "failed",
        "exit_code": 1,
        "stdout": "",
        "stderr": str(exc),
        "error_code": exc.reason_code,
        "host_resource_admission": exc.to_dict(),
    }


def _workflow_mcp_session_token_failure(exc: WorkflowMcpSessionTokenError) -> dict[str, Any]:
    return {
        "status": "failed",
        "exit_code": 1,
        "stdout": "",
        "stderr": str(exc),
        "error_code": exc.reason_code,
        "reason_code": exc.reason_code,
        "workflow_mcp_session": dict(exc.details),
    }


def _sandbox_policy_value(
    agent_config,
    field_name: str,
    default: str,
    *,
    execution_bundle: dict[str, Any] | None = None,
) -> str:
    bundle_profile = _sandbox_profile_from_bundle(execution_bundle)
    if isinstance(bundle_profile, dict):
        value = str(bundle_profile.get(field_name) or "").strip()
        if value:
            return value
    policy = getattr(agent_config, "sandbox_policy", None)
    if policy is None:
        return default
    value = getattr(policy, field_name, default)
    return str(value or default).strip() or default


def _workspace_materialization_value(
    agent_config,
    *,
    execution_bundle: dict[str, Any] | None,
) -> str:
    materialization = _sandbox_policy_value(
        agent_config,
        "workspace_materialization",
        "copy",
        execution_bundle=execution_bundle,
    )
    if materialization.strip().lower() == "none":
        return "none"
    shard_paths = _execution_shard_paths({"execution_bundle": execution_bundle or {}})
    if shard_paths:
        return materialization
    return "none"


def _sandbox_image(
    agent_config,
    *,
    execution_bundle: dict[str, Any] | None,
) -> str | None:
    bundle_profile = _sandbox_profile_from_bundle(execution_bundle)
    if isinstance(bundle_profile, dict):
        explicit = str(bundle_profile.get("docker_image") or "").strip()
        if explicit:
            return explicit
    image = getattr(agent_config, "docker_image", None)
    normalized = str(image or "").strip()
    return normalized or None


def _parse_llm_output(stdout: str) -> tuple[str, dict[str, Any]]:
    parsed_stdout = stdout
    telemetry: dict[str, Any] = {}
    data = _parse_llm_json(stdout)
    if data is None:
        return parsed_stdout, telemetry
    if not isinstance(data, dict):
        return parsed_stdout, telemetry
    usage = data.get("usage", {})
    input_tokens = (
        usage.get("input_tokens", 0)
        or usage.get("prompt_tokens", 0)
        or usage.get("inputTokens", 0)
    )
    output_tokens = (
        usage.get("output_tokens", 0)
        or usage.get("completion_tokens", 0)
        or usage.get("outputTokens", 0)
    )
    cache_read_tokens = usage.get("cache_read_input_tokens", 0) or usage.get("cacheReadTokens", 0)
    cache_creation_tokens = usage.get("cache_creation_input_tokens", 0) or usage.get("cacheWriteTokens", 0)
    if not input_tokens:
        stats = data.get("stats", {})
        for model_stats in stats.get("models", {}).values():
            tokens = model_stats.get("tokens", {})
            input_tokens = tokens.get("input", 0) or tokens.get("prompt", 0)
            output_tokens = tokens.get("candidates", 0) or tokens.get("output", 0)
            break
    telemetry = {
        "token_input": input_tokens,
        "token_output": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "cost_usd": data.get("total_cost_usd", 0.0) or data.get("cost_usd", 0.0),
    }
    for key in ("result", "response", "output", "text"):
        if key in data and isinstance(data[key], str) and data[key].strip():
            parsed_stdout = data[key]
            break
    # CLI error envelope: extract error messages so they're not silently lost
    if not parsed_stdout.strip() and data.get("errors"):
        errors = data["errors"]
        if isinstance(errors, list):
            parsed_stdout = "\n".join(str(e) for e in errors[:5])
    return parsed_stdout, telemetry


def _parse_llm_json(stdout: str) -> Any | None:
    try:
        return json.loads(stdout)
    except (json.JSONDecodeError, TypeError):
        pass

    decoder = json.JSONDecoder()
    text = str(stdout or "").strip()
    fallback: Any | None = None
    for index, char in enumerate(text):
        if char not in "{[":
            continue
        try:
            data, _end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            fallback = data
            if any(key in data for key in ("result", "response", "output", "text", "usage", "stats", "errors")):
                return data
    return fallback


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _execute_api_control_plane(
    *,
    prompt: str,
    api_protocol: str,
    api_endpoint: str,
    api_key_env: str,
    api_key: str | None,
    model_slug: str,
    max_output_tokens: int,
    timeout: int,
    workdir: str,
    reasoning_effort: str | None,
    sandbox_session_id: str,
    sandbox_group_id: str,
    provider_slug: str,
) -> dict[str, Any]:
    started_at = _utc_now_iso()
    started = time.monotonic()
    try:
        from runtime.http_transport import call_transport

        stdout = call_transport(
            api_protocol,
            prompt,
            model=model_slug,
            max_tokens=max_output_tokens,
            timeout=timeout,
            api_endpoint=api_endpoint,
            api_key=api_key,
            api_key_env=api_key_env,
            workdir=workdir,
            reasoning_effort=reasoning_effort,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "exit_code": 1,
            "stdout": "",
            "stderr": str(exc),
            "error_code": getattr(exc, "reason_code", "api_transport_failed"),
            "execution_mode": "control_plane",
            "sandbox_provider": "control_plane",
            "execution_transport": "api",
            "sandbox_session_id": sandbox_session_id,
            "sandbox_group_id": sandbox_group_id,
            "artifact_refs": [],
            "started_at": started_at,
            "finished_at": _utc_now_iso(),
            "workspace_snapshot_ref": "",
            "workspace_snapshot_cache_hit": False,
            "network_policy": "provider_only",
            "provider_latency_ms": round((time.monotonic() - started) * 1000, 2),
            "container_cpu_percent": None,
            "container_mem_bytes": None,
            "workspace_materialization": "none",
            "api_execution_mode": "control_plane_transport",
            "provider_slug": provider_slug,
        }
    return {
        "status": "succeeded",
        "exit_code": 0,
        "stdout": stdout,
        "stderr": "",
        "error_code": "",
        "execution_mode": "control_plane",
        "sandbox_provider": "control_plane",
        "execution_transport": "api",
        "sandbox_session_id": sandbox_session_id,
        "sandbox_group_id": sandbox_group_id,
        "artifact_refs": [],
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
        "workspace_snapshot_ref": "",
        "workspace_snapshot_cache_hit": False,
        "network_policy": "provider_only",
        "provider_latency_ms": round((time.monotonic() - started) * 1000, 2),
        "container_cpu_percent": None,
        "container_mem_bytes": None,
        "workspace_materialization": "none",
        "api_execution_mode": "control_plane_transport",
        "provider_slug": provider_slug,
    }


def _result_payload(result, *, timeout: int, parse_json_output: bool) -> dict[str, Any]:
    # Use stderr as fallback source if stdout is empty but stderr looks like JSON output
    raw_stdout = result.stdout
    if not raw_stdout.strip() and result.stderr.strip().startswith("{"):
        raw_stdout = result.stderr
    stdout = raw_stdout
    telemetry: dict[str, Any] = {}
    if parse_json_output:
        stdout, telemetry = _parse_llm_output(raw_stdout)
    stderr = result.stderr
    artifact_scope_drift = [
        dict(entry) for entry in getattr(result, "artifact_scope_drift", ())
    ]
    status = "succeeded" if result.exit_code == 0 and not result.timed_out else "failed"
    error_code = ""
    if result.timed_out:
        stderr = stderr or f"timed out after {timeout}s"
        error_code = "workflow.timeout"
    elif artifact_scope_drift:
        status = "failed"
        error_code = "workflow_scope.out_of_scope_write"
        drift_refs = ", ".join(
            str(entry.get("artifact_ref") or "<unknown>") for entry in artifact_scope_drift[:5]
        )
        stderr = (
            stderr
            + (
                "\nsandbox artifact scope drift: "
                f"{drift_refs or 'artifact outside declared write_scope'}"
            )
        ).strip()
    elif status == "failed":
        from runtime.failure_classifier import classify_failure_from_stderr

        classification = classify_failure_from_stderr(stderr, exit_code=result.exit_code)
        error_code = classification.category.value

    return {
        "status": status,
        "exit_code": result.exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "error_code": error_code,
        "execution_mode": result.execution_mode,
        "sandbox_provider": result.sandbox_provider,
        "execution_transport": result.execution_transport,
        "sandbox_session_id": result.sandbox_session_id,
        "sandbox_group_id": result.sandbox_group_id,
        "artifact_refs": list(result.artifact_refs),
        "artifact_scope_drift": artifact_scope_drift,
        "workspace_manifest_audit": dict(getattr(result, "workspace_manifest_audit", {}) or {}),
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "workspace_snapshot_ref": getattr(result, "workspace_snapshot_ref", ""),
        "workspace_snapshot_cache_hit": bool(getattr(result, "workspace_snapshot_cache_hit", False)),
        "network_policy": result.network_policy,
        "provider_latency_ms": result.provider_latency_ms,
        "container_cpu_percent": result.container_cpu_percent,
        "container_mem_bytes": result.container_mem_bytes,
        **telemetry,
    }


def execute_integration(job: dict[str, Any], conn, *, logger: logging.Logger | None = None) -> dict[str, Any]:
    """Execute a job via the integration tool registry without any LLM."""
    from runtime.integrations import (
        execute_integration as run_integration,
        integration_result_error_code,
        integration_result_succeeded,
        integration_result_status,
    )

    log = logger or logging.getLogger(__name__)
    integration_id = job["integration_id"]
    action = job["integration_action"]
    args_raw = job.get("integration_args") or {}

    if isinstance(args_raw, str):
        try:
            args_raw = json.loads(args_raw)
        except (json.JSONDecodeError, TypeError):
            args_raw = {}

    log.info(
        "Integration execution: %s/%s (job %s)",
        integration_id,
        action,
        job.get("id"),
    )

    result = run_integration(integration_id, action, args_raw, conn)
    status = integration_result_status(result)
    succeeded = integration_result_succeeded(result)
    summary = result.get("summary", "")
    data = result.get("data")
    error_code = integration_result_error_code(result)

    stdout = f"Integration status: {status}"
    if summary:
        stdout += f"\n{summary}"
    if data:
        try:
            stdout += "\n\n" + json.dumps(data, indent=2, default=str)
        except Exception:
            stdout += f"\n\n{data}"

    return {
        "status": "succeeded" if succeeded else "failed",
        "exit_code": 0 if succeeded else 1,
        "stdout": stdout,
        "stderr": "" if succeeded else summary,
        "error_code": error_code,
        "integration_status": status,
        "token_input": 0,
        "token_output": 0,
        "cost_usd": 0.0,
    }


def execute_cli(
    agent_config,
    prompt: str,
    workdir: str,
    *,
    execution_bundle: dict[str, Any] | None = None,
    artifact_store: Any | None = None,
) -> dict[str, Any]:
    """Run workflow-managed CLI models through the normalized sandbox contract."""
    provider_slug = str(getattr(agent_config, "provider", "") or "").strip().lower()
    try:
        slot_cm = _provider_slot(provider_slug)
    except ProviderSlotAcquisitionError as exc:
        return _provider_slot_acquisition_failure(provider_slug, exc)
    with slot_cm as acquired:
        if not acquired:
            return _provider_capacity_failure(provider_slug)

        try:
            env = _build_execution_env(
                agent_config,
                workdir=workdir,
                execution_bundle=execution_bundle,
            )
        except WorkflowMcpSessionTokenError as exc:
            return _workflow_mcp_session_token_failure(exc)

        stdin_text = prompt
        if getattr(agent_config, "wrapper_command", None):
            cmd = shlex.split(agent_config.wrapper_command)
            if "{prompt_file}" in cmd:
                return {
                    "status": "failed",
                    "exit_code": 1,
                    "stdout": "",
                    "stderr": "sandbox execution does not support {prompt_file}; use stdin or argv prompt delivery",
                    "error_code": "sandbox_error",
                }
            if "{prompt}" in cmd:
                cmd = [prompt if part == "{prompt}" else part for part in cmd]
                stdin_text = ""
        else:
            try:
                cmd = build_command(
                    provider_slug=provider_slug,
                    model=getattr(agent_config, "model", None),
                )
            except Exception as exc:
                return {
                    "status": "failed",
                    "exit_code": 1,
                    "stdout": "",
                    "stderr": str(exc),
                    "error_code": "sandbox_error",
                }

        cmd = normalize_command_parts_for_docker(cmd)

        decision_overlays = decision_workspace_overlays(execution_bundle)
        workspace_overlays = [
            *decision_overlays,
            *workflow_mcp_workspace_overlays(
                provider_slug=provider_slug,
                execution_bundle=execution_bundle,
                prefer_docker=_sandbox_provider_for_execution(agent_config, execution_bundle) == "docker_local",
            ),
        ]
        cmd = augment_cli_command_for_workflow_mcp(
            provider_slug=provider_slug,
            command_parts=cmd,
            execution_bundle=execution_bundle,
            prefer_docker=_sandbox_provider_for_execution(agent_config, execution_bundle) == "docker_local",
        )
        timeout = int(getattr(agent_config, "timeout_seconds", 900) or 900)
        network_policy = _sandbox_policy_value(
            agent_config,
            "network_policy",
            "provider_only"
            if _env_flag_enabled(_WORKFLOW_MODEL_NETWORK_ENV, default=True)
            else "disabled",
            execution_bundle=execution_bundle,
        )
        # Copy only when the execution bundle carries an enforceable shard.
        # Unscoped copy is blocked by SandboxRuntime; no-scope prompt-only
        # jobs should run with an empty workspace instead of asking for legacy
        # full-repo materialization.
        workspace_materialization = _workspace_materialization_value(
            agent_config,
            execution_bundle=execution_bundle,
        )
        sandbox_provider = _sandbox_provider_for_execution(agent_config, execution_bundle)
        sandbox_profile = _sandbox_profile_from_bundle(execution_bundle)
        sandbox_profile_ref = (
            str((sandbox_profile or {}).get("sandbox_profile_ref") or "").strip()
            if isinstance(sandbox_profile, dict)
            else ""
        )
        sandbox_session_id, sandbox_group_id = derive_sandbox_identity(
            workdir=workdir,
            execution_bundle=execution_bundle,
            execution_transport="cli",
            identity_payload={
                "provider_slug": provider_slug,
                "model_slug": getattr(agent_config, "model", None),
                "command": shlex.join(cmd),
                "stdin_text": stdin_text,
                "network_policy": network_policy,
                "workspace_materialization": workspace_materialization,
                "sandbox_provider": sandbox_provider,
                "sandbox_profile_ref": sandbox_profile_ref,
                "docker_image": _sandbox_image(agent_config, execution_bundle=execution_bundle),
            },
        )

        sandbox_metadata = {
            "provider_slug": provider_slug,
            "execution_bundle": execution_bundle or {},
            **({"workspace_overlays": workspace_overlays} if workspace_overlays else {}),
            **(dict(sandbox_profile or {}) if isinstance(sandbox_profile, dict) else {}),
        }
        resource_claim = None
        try:
            with hold_host_resources_for_sandbox(
                sandbox_provider=sandbox_provider,
                execution_transport="cli",
                sandbox_session_id=sandbox_session_id,
                timeout_seconds=timeout,
                metadata=sandbox_metadata,
            ) as resource_claim:
                result = SandboxRuntime().execute_command(
                    provider_name=sandbox_provider,
                    sandbox_session_id=sandbox_session_id,
                    sandbox_group_id=sandbox_group_id,
                    workdir=workdir,
                    command=shlex.join(cmd),
                    stdin_text=stdin_text,
                    env=env,
                    timeout_seconds=timeout,
                    network_policy=network_policy,
                    workspace_materialization=workspace_materialization,
                    execution_transport="cli",
                    image=_sandbox_image(agent_config, execution_bundle=execution_bundle),
                    metadata=sandbox_metadata,
                    artifact_store=artifact_store,
                )
        except (HostResourceCapacityError, HostResourceAdmissionUnavailable) as exc:
            return _host_resource_admission_failure(exc)
        except HostResourceAdmissionError as exc:
            return _host_resource_admission_failure(exc)
        except RuntimeError as exc:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": str(exc),
                "error_code": "sandbox_error",
            }
        except OSError as exc:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": str(exc),
                "error_code": "setup_failure",
            }

        payload = _result_payload(result, timeout=timeout, parse_json_output=True)
        if resource_claim is not None:
            payload["host_resource_admission"] = resource_claim.to_dict()
        if sandbox_profile_ref:
            payload["sandbox_profile_ref"] = sandbox_profile_ref
        if isinstance(sandbox_profile, dict):
            payload["workspace_materialization"] = workspace_materialization
            docker_image = _sandbox_image(agent_config, execution_bundle=execution_bundle)
            if docker_image:
                payload["docker_image"] = docker_image
        return payload


def execute_api(
    agent_config,
    prompt: str,
    *,
    workdir: str | None = None,
    execution_bundle: dict[str, Any] | None = None,
    artifact_store: Any | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    """Execute provider-backed API work inside the selected sandbox provider."""
    resolved_workdir = workdir or os.getcwd()
    provider_slug = str(getattr(agent_config, "provider", "") or "").strip().lower()
    try:
        slot_cm = _provider_slot(provider_slug)
    except ProviderSlotAcquisitionError as exc:
        return _provider_slot_acquisition_failure(provider_slug, exc)
    with slot_cm as acquired:
        if not acquired:
            return _provider_capacity_failure(provider_slug)

        try:
            env = _build_execution_env(
                agent_config,
                workdir=resolved_workdir,
                execution_bundle=execution_bundle,
            )
        except WorkflowMcpSessionTokenError as exc:
            return _workflow_mcp_session_token_failure(exc)
        selected_api_key_env = _resolve_api_key_env_name(provider_slug, env)
        model_slug = str(getattr(agent_config, "model", "") or "").strip()
        max_output_tokens = int(getattr(agent_config, "max_output_tokens", 4096) or 4096)
        timeout = int(getattr(agent_config, "timeout_seconds", 90) or 90)
        contract = resolve_execution_transport(agent_config)
        # Look up transport metadata from provider_cli_profiles.
        # The worker dispatches by protocol family — no provider names.
        from registry.provider_execution_registry import get_profile as _get_provider_profile
        _profile = _get_provider_profile(provider_slug)
        if not _profile or not _profile.api_protocol_family or not _profile.api_endpoint:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": f"provider_cli_profiles has no api_protocol_family/api_endpoint for '{provider_slug}'",
                "error_code": "transport_config_missing",
            }
        _api_protocol = _profile.api_protocol_family
        _api_endpoint = _profile.api_endpoint
        _api_key_env = selected_api_key_env or (
            _profile.api_key_env_vars[0] if _profile.api_key_env_vars else None
        )
        if not _api_key_env:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": (
                    "provider_cli_profiles has no api_key_env_vars for API "
                    f"transport '{provider_slug}'"
                ),
                "error_code": "transport_auth_config_missing",
            }
        _reasoning_flag = (
            f"--reasoning-effort {shlex.quote(reasoning_effort)}" if reasoning_effort else ""
        )
        network_policy = _sandbox_policy_value(
            agent_config,
            "network_policy",
            "provider_only",
            execution_bundle=execution_bundle,
        )
        workspace_materialization = _sandbox_policy_value(
            agent_config,
            "workspace_materialization",
            "copy",
            execution_bundle=execution_bundle,
        )
        sandbox_provider = _sandbox_provider_for_execution(agent_config, execution_bundle)
        sandbox_profile = _sandbox_profile_from_bundle(execution_bundle)
        sandbox_profile_ref = (
            str((sandbox_profile or {}).get("sandbox_profile_ref") or "").strip()
            if isinstance(sandbox_profile, dict)
            else ""
        )
        decision_overlays = decision_workspace_overlays(execution_bundle)
        python_executable = shlex.quote(sys.executable or "python3")
        command = (
            f"{python_executable} -m runtime.api_transport_worker "
            f"--api-protocol {shlex.quote(_api_protocol)} "
            f"--api-endpoint {shlex.quote(_api_endpoint)} "
            f"--api-key-env {shlex.quote(_api_key_env)} "
            f"--workdir {shlex.quote(resolved_workdir)} "
            f"--model {shlex.quote(model_slug)} "
            f"--max-output-tokens {max_output_tokens} "
            f"--timeout-seconds {timeout}"
            + (f" {_reasoning_flag}" if _reasoning_flag else "")
        )
        sandbox_session_id, sandbox_group_id = derive_sandbox_identity(
            workdir=resolved_workdir,
            execution_bundle=execution_bundle,
            execution_transport="api",
            identity_payload={
                "provider_slug": provider_slug,
                "model_slug": model_slug,
                "command": command,
                "stdin_text": prompt,
                "network_policy": network_policy,
                "workspace_materialization": workspace_materialization,
                "max_output_tokens": max_output_tokens,
                "reasoning_effort": reasoning_effort,
                "sandbox_provider": sandbox_provider,
                "sandbox_profile_ref": sandbox_profile_ref,
                "docker_image": _sandbox_image(agent_config, execution_bundle=execution_bundle),
            },
        )
        if workspace_materialization.strip().lower() == "none":
            payload = _execute_api_control_plane(
                prompt=prompt,
                api_protocol=_api_protocol,
                api_endpoint=_api_endpoint,
                api_key_env=_api_key_env,
                api_key=str(env.get(_api_key_env, "")).strip() or None,
                model_slug=model_slug,
                max_output_tokens=max_output_tokens,
                timeout=timeout,
                workdir=resolved_workdir,
                reasoning_effort=reasoning_effort,
                sandbox_session_id=sandbox_session_id,
                sandbox_group_id=sandbox_group_id,
                provider_slug=provider_slug,
            )
            if sandbox_profile_ref:
                payload["sandbox_profile_ref"] = sandbox_profile_ref
            return payload
        sandbox_metadata = {
            "provider_slug": provider_slug,
            "model_slug": model_slug,
            "execution_bundle": execution_bundle or {},
            **({"workspace_overlays": decision_overlays} if decision_overlays else {}),
            **(dict(sandbox_profile or {}) if isinstance(sandbox_profile, dict) else {}),
        }
        resource_claim = None
        try:
            with hold_host_resources_for_sandbox(
                sandbox_provider=sandbox_provider,
                execution_transport="api",
                sandbox_session_id=sandbox_session_id,
                timeout_seconds=timeout,
                metadata=sandbox_metadata,
            ) as resource_claim:
                result = SandboxRuntime().execute_command(
                    provider_name=sandbox_provider,
                    sandbox_session_id=sandbox_session_id,
                    sandbox_group_id=sandbox_group_id,
                    workdir=resolved_workdir,
                    command=command,
                    stdin_text=prompt,
                    env=env,
                    timeout_seconds=timeout,
                    network_policy=network_policy,
                    workspace_materialization=workspace_materialization,
                    execution_transport="api",
                    image=_sandbox_image(agent_config, execution_bundle=execution_bundle),
                    metadata=sandbox_metadata,
                    artifact_store=artifact_store,
                )
        except (HostResourceCapacityError, HostResourceAdmissionUnavailable) as exc:
            return _host_resource_admission_failure(exc)
        except HostResourceAdmissionError as exc:
            return _host_resource_admission_failure(exc)
        except RuntimeError as exc:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": str(exc),
                "error_code": "sandbox_error",
            }
        except OSError as exc:
            return {
                "status": "failed",
                "exit_code": 1,
                "stdout": "",
                "stderr": str(exc),
                "error_code": "setup_failure",
            }

        payload = _result_payload(result, timeout=timeout, parse_json_output=True)
        if resource_claim is not None:
            payload["host_resource_admission"] = resource_claim.to_dict()
        if sandbox_profile_ref:
            payload["sandbox_profile_ref"] = sandbox_profile_ref
        if isinstance(sandbox_profile, dict):
            payload["workspace_materialization"] = workspace_materialization
            docker_image = _sandbox_image(agent_config, execution_bundle=execution_bundle)
            if docker_image:
                payload["docker_image"] = docker_image
        if payload["status"] == "failed" and not payload["error_code"] and not payload["stdout"]:
            payload["error_code"] = "api_transport_failed"
        return payload
