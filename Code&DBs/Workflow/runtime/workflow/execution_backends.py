"""Execution helpers backed by the unified sandbox runtime."""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

from adapters.provider_registry import build_command
from runtime.execution_transport import resolve_execution_transport
from runtime.sandbox_runtime import SandboxRuntime, derive_sandbox_identity
from runtime.workflow.mcp_bridge import augment_cli_command_for_workflow_mcp


_WORKFLOW_MODEL_NETWORK_ENV = "PRAXIS_WORKFLOW_MODEL_NETWORK"
_EXECUTION_BUNDLE_ENV = "PRAXIS_EXECUTION_BUNDLE"
_ALLOWED_MCP_TOOLS_ENV = "PRAXIS_ALLOWED_MCP_TOOLS"
_LEGACY_ALLOWED_MCP_TOOLS_ENV = "PRAXIS_ALLOWED_MCP_TOOLS"
_ALLOWED_SKILLS_ENV = "PRAXIS_ALLOWED_SKILLS"
_SANDBOX_PATH_PREFIX = "/opt/homebrew/bin:/usr/local/bin:"


def _env_flag_enabled(name: str, *, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _load_env_secret_from_keychain(env: dict[str, str], key_name: str) -> None:
    if key_name in env and str(env.get(key_name, "")).strip():
        return
    try:
        value = subprocess.run(
            ["security", "find-generic-password", "-s", key_name, "-w"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return
    if value.returncode == 0 and value.stdout.strip():
        env[key_name] = value.stdout.strip()


def _sanitize_base_env() -> dict[str, str]:
    env = dict(os.environ)
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
    env["PATH"] = _SANDBOX_PATH_PREFIX + env.get("PATH", "")
    return env


def _ripgrep_config_for_workdir(workdir: str) -> str | None:
    current = Path(workdir).resolve()
    for directory in (current, *current.parents):
        candidate = directory / ".ripgreprc"
        if candidate.is_file():
            try:
                return os.path.relpath(candidate, current)
            except ValueError:
                return str(candidate)
        if (directory / ".git").exists():
            break
    return None


def _load_dotenv_exports(workdir: str, env: dict[str, str]) -> dict[str, str]:
    exports: dict[str, str] = {}
    dotenv_path = os.path.join(workdir, ".env")
    if not os.path.isfile(dotenv_path):
        return exports
    with open(dotenv_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in env:
                    env[key] = value
                if key and str(env.get(key, "")).strip():
                    exports[key] = str(env[key]).strip()
    return exports


def _provider_api_key_names(provider_slug: str) -> tuple[str, ...]:
    if not provider_slug:
        return ()
    try:
        from adapters import provider_registry as provider_registry_mod

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
    dotenv_exports = _load_dotenv_exports(workdir, env)
    provider_slug = str(getattr(agent_config, "provider", "") or "").strip().lower()
    export_names = {
        *_provider_api_key_names(provider_slug),
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        *dotenv_exports.keys(),
    }
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
    if "GEMINI_API_KEY" not in env and "GOOGLE_API_KEY" in env:
        env["GEMINI_API_KEY"] = env["GOOGLE_API_KEY"]

    sandbox_env = {
        key_name: str(env[key_name]).strip()
        for key_name in sorted(export_names)
        if str(env.get(key_name, "")).strip()
    }
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
        skill_refs = execution_bundle.get("skill_refs")
        if isinstance(skill_refs, list) and skill_refs:
            sandbox_env[_ALLOWED_SKILLS_ENV] = ",".join(str(name) for name in skill_refs)
    # Apply provider-specific sandbox env overrides from profile
    from adapters.provider_registry import get_profile as _get_env_profile
    _env_profile = _get_env_profile(provider_slug)
    if _env_profile and _env_profile.sandbox_env_overrides:
        overrides = _env_profile.sandbox_env_overrides
        for key in overrides.get("strip", []):
            sandbox_env.pop(key, None)
        for key, value in overrides.get("set", {}).items():
            sandbox_env[key] = str(value)
        if overrides.get("set_home"):
            sandbox_env["HOME"] = os.path.expanduser("~")
    sandbox_env["PYTHONPATH"] = "Code&DBs/Workflow"
    sandbox_env["PATH"] = env["PATH"]
    ripgrep_config = _ripgrep_config_for_workdir(workdir)
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
    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, TypeError):
        return parsed_stdout, telemetry
    if not isinstance(data, dict):
        return parsed_stdout, telemetry
    usage = data.get("usage", {})
    input_tokens = usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)
    output_tokens = usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)
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
    status = "succeeded" if result.exit_code == 0 and not result.timed_out else "failed"
    error_code = ""
    if result.timed_out:
        stderr = stderr or f"timed out after {timeout}s"
        error_code = "workflow.timeout"
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
    from runtime.integrations import execute_integration as run_integration

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
    status = result.get("status", "failed")
    summary = result.get("summary", "")
    data = result.get("data")
    error = result.get("error")

    stdout = summary
    if data:
        try:
            stdout += "\n\n" + json.dumps(data, indent=2, default=str)
        except Exception:
            stdout += f"\n\n{data}"

    return {
        "status": "succeeded" if status in ("succeeded", "skipped") else "failed",
        "exit_code": 0 if status in ("succeeded", "skipped") else 1,
        "stdout": stdout,
        "error_code": error or "",
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
    env = _build_execution_env(
        agent_config,
        workdir=workdir,
        execution_bundle=execution_bundle,
    )

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

    # Normalize codex flags for Docker-sandboxed execution.  The Docker
    # container IS the sandbox, so codex's internal bwrap sandbox must be
    # bypassed (it requires privileges Docker doesn't grant).  --full-auto
    # is mutually exclusive with --dangerously-bypass-approvals-and-sandbox,
    # so strip it if a legacy template still includes it.  Sandbox workspaces
    # also exclude .git, so codex needs --skip-git-repo-check.
    cmd0_name = os.path.basename(cmd[0]).strip().lower() if cmd else ""
    if cmd0_name == "codex":
        try:
            exec_idx = [p.strip().lower() for p in cmd].index("exec")
        except ValueError:
            exec_idx = -1
        if exec_idx >= 0:
            cmd = [part for part in cmd if part != "--full-auto"]
            if "--skip-git-repo-check" not in cmd:
                cmd.insert(exec_idx + 1, "--skip-git-repo-check")
            if "--dangerously-bypass-approvals-and-sandbox" not in cmd:
                cmd.insert(exec_idx + 1, "--dangerously-bypass-approvals-and-sandbox")

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

    try:
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
            metadata={
                "provider_slug": provider_slug,
                "execution_bundle": execution_bundle or {},
                **(dict(sandbox_profile or {}) if isinstance(sandbox_profile, dict) else {}),
            },
            artifact_store=artifact_store,
        )
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
    env = _build_execution_env(
        agent_config,
        workdir=resolved_workdir,
        execution_bundle=execution_bundle,
    )
    provider_slug = str(getattr(agent_config, "provider", "") or "").strip().lower()
    model_slug = str(getattr(agent_config, "model", "") or "").strip()
    max_output_tokens = int(getattr(agent_config, "max_output_tokens", 4096) or 4096)
    timeout = int(getattr(agent_config, "timeout_seconds", 90) or 90)
    contract = resolve_execution_transport(agent_config)
    # Look up transport metadata from provider_cli_profiles.
    # The worker dispatches by protocol family — no provider names.
    from adapters.provider_registry import get_profile as _get_provider_profile
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
    _api_key_env = _profile.api_key_env_vars[0] if _profile.api_key_env_vars else f"{provider_slug.upper()}_API_KEY"
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
    command = (
        "python3 -m runtime.api_transport_worker "
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
    try:
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
            metadata={
                "provider_slug": provider_slug,
                "model_slug": model_slug,
                "execution_bundle": execution_bundle or {},
                **(dict(sandbox_profile or {}) if isinstance(sandbox_profile, dict) else {}),
            },
            artifact_store=artifact_store,
        )
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
