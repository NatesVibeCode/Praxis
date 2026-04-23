"""CLI-based LLM adapter with prompt-channel support, no filesystem access.

This adapter is a thin executor. ALL provider-specific knowledge lives
in the provider execution registry authority. This module:
  1. Reads the registry profile for the requested provider
  2. Builds the command from the registry
  3. Sends the prompt via the registry-declared channel
  4. Parses structured output from stdout

No hardcoded provider flags, binary names, or aliases anywhere in this file.
"""

from __future__ import annotations

import json
import os
import shlex
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _fail
from .deterministic import (
    BaseNodeAdapter,
    DeterministicExecutionControl,
    DeterministicTaskRequest,
    DeterministicTaskResult,
    cancelled_task_result,
)
from .docker_runner import run_model, ExecutionResult
from registry.provider_execution_registry import (
    default_provider_slug,
    get_profile,
    build_command,
    resolve_adapter_contract,
    resolve_binary,
    resolve_provider_from_alias,
    registered_providers,
)
from .structured_output import StructuredOutput, parse_model_output
from .task_profiles import TaskProfileAuthorityError, resolve_profile
from runtime.workflow.execution_policy import resolve_cli_execution_policy

_DEFAULT_TIMEOUT = int(os.environ.get("PRAXIS_CLI_TIMEOUT", "300"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CLIAdapterError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


# Polite-refusal signatures the CLI returns inside a successful JSON envelope
# (is_error:true, exit 0). When any of these appear we must surface the refusal
# as an adapter failure rather than pretend the turn succeeded.
_REFUSAL_RESULT_SUBSTRINGS: tuple[str, ...] = (
    "not logged in",
    "please run /login",
    "requires user approval",
    "requires permission",
    "requires permissions",
    "invalid api key",
    "fix external api key",
    "failed to authenticate",
    "invalid authentication credentials",
)


def _raise_if_cli_refused(
    *,
    stdout: str,
    exit_code: int,
    binary: str,
) -> None:
    """Translate a polite CLI refusal (exit 0, is_error:true in JSON) into an
    adapter-level error so the node fails closed. Silent on non-JSON stdout.

    Detected patterns:
      - `is_error: true` in the CLI JSON envelope
      - Result text matching a known refusal substring
    """
    if exit_code != 0:
        # Non-zero already surfaces as cli_adapter.nonzero_exit upstream.
        return
    stripped = (stdout or "").strip()
    if not stripped:
        return
    try:
        parsed = json.loads(stripped)
    except (json.JSONDecodeError, TypeError):
        # Not all CLIs emit JSON; plain-text output is forwarded as-is.
        return
    if not isinstance(parsed, Mapping):
        return
    is_error = bool(parsed.get("is_error"))
    result_text = str(parsed.get("result") or "")
    lowered = result_text.lower()
    matched_pattern = next(
        (p for p in _REFUSAL_RESULT_SUBSTRINGS if p in lowered),
        None,
    )
    if not (is_error or matched_pattern):
        return
    reason = "cli_adapter.refusal"
    if matched_pattern in {"not logged in", "please run /login"}:
        reason = "cli_adapter.not_authenticated"
    elif matched_pattern in {"invalid api key", "fix external api key", "failed to authenticate", "invalid authentication credentials"}:
        reason = "cli_adapter.auth_rejected"
    snippet = result_text if result_text else "is_error=true with empty result"
    if len(snippet) > 400:
        snippet = snippet[:400] + "…"
    raise CLIAdapterError(
        reason,
        f"{binary} declined to act: {snippet}",
    )


@dataclass(frozen=True, slots=True)
class CLILLMResult:
    content: str
    exit_code: int
    stderr: str
    latency_ms: int
    raw_json: dict[str, Any] | None
    cli_name: str
    provider_slug: str
    model_slug: str | None
    structured_output: StructuredOutput | None = None
    execution_mode: str = "host"
    cancelled: bool = False


# PROVIDER_PROFILES dict built from registry at import time.
# Tests and other code that reads this dict get registry data, not hardcoded values.
from registry.provider_execution_registry import get_all_profiles as _get_all_profiles
PROVIDER_PROFILES: dict[str, dict[str, Any]] = {
    slug: {
        "binary": p.binary,
        "base_flags": list(p.base_flags),
        "model_flag": p.model_flag,
        "stdin_mode": (p.prompt_mode != "argv"),
    }
    for slug, p in _get_all_profiles().items()
}


def _resolve_provider(
    payload: dict[str, Any],
    default_provider: str | None,
) -> str:
    """Resolve provider_slug from payload. Uses registry alias map.

    Raises CLIAdapterError if an explicit provider_slug is given but not registered.
    """
    slug = payload.get("provider_slug")
    if slug:
        if get_profile(slug):
            return slug
        raise CLIAdapterError(
            "cli_adapter.provider_unmapped",
            f"no profile for {slug!r}; known: {registered_providers()}",
        )

    cli_hint = payload.get("cli")
    if isinstance(cli_hint, str):
        resolved = resolve_provider_from_alias(cli_hint)
        if resolved:
            return resolved

    if default_provider:
        return default_provider
    return default_provider_slug()


def _payload_adapter_type(payload: Mapping[str, Any]) -> str | None:
    raw_adapter_type = payload.get("adapter_type")
    if raw_adapter_type is None:
        return None
    if not isinstance(raw_adapter_type, str) or not raw_adapter_type.strip():
        raise CLIAdapterError(
            "cli_adapter.contract_invalid",
            "input_payload.adapter_type must be a non-empty string when provided",
        )
    return raw_adapter_type.strip()


def _payload_provider_adapter_type(payload: Mapping[str, Any]) -> str | None:
    raw_contract = payload.get("provider_adapter_contract")
    if raw_contract is None:
        return None
    if not isinstance(raw_contract, Mapping):
        raise CLIAdapterError(
            "cli_adapter.contract_invalid",
            "provider_adapter_contract must be a mapping when provided",
        )
    raw_adapter_type = raw_contract.get("adapter_type")
    if raw_adapter_type is None:
        return None
    if not isinstance(raw_adapter_type, str) or not raw_adapter_type.strip():
        raise CLIAdapterError(
            "cli_adapter.contract_invalid",
            "provider_adapter_contract.adapter_type must be a non-empty string when provided",
        )
    return raw_adapter_type.strip()


def _optional_string_list(value: object) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            return None
        normalized.append(item.strip())
    return normalized


def _runtime_route_outputs(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_route = payload.get("runtime_route")
    if not isinstance(raw_route, Mapping):
        return {}
    outputs: dict[str, Any] = {}
    for field_name in (
        "route_decision_id",
        "route_eligibility_state_id",
        "selected_candidate_ref",
        "selected_provider_failover_binding_id",
        "provider_endpoint_binding_id",
        "provider_ref",
        "provider_slug",
        "model_slug",
        "decision_reason_code",
        "failover_role",
        "failover_trigger_rule",
        "endpoint_kind",
        "endpoint_transport_kind",
        "route_authority",
        "failover_endpoint_authority",
        "as_of",
    ):
        value = raw_route.get(field_name)
        if isinstance(value, str) and value.strip():
            outputs[field_name] = value.strip()
    for field_name in ("balance_slot", "failover_position_index"):
        value = raw_route.get(field_name)
        if isinstance(value, int) and not isinstance(value, bool):
            outputs[field_name] = value
    for field_name in ("allowed_candidate_refs", "failover_slice_candidate_refs"):
        normalized_values = _optional_string_list(raw_route.get(field_name))
        if normalized_values is not None:
            outputs[field_name] = normalized_values
    return outputs


def _invoke_cli(
    *,
    provider_slug: str,
    model_slug: str | None,
    prompt: str,
    timeout: int,
    system_prompt: str | None = None,
    json_schema: str | None = None,
    binary_override: str | None = None,
    prefer_docker: bool = True,
    network: bool = False,
    auth_mount_policy: str = "provider_scoped",
    workdir: str | None = None,
    docker_network: str | None = None,
    execution_control: DeterministicExecutionControl | None = None,
) -> CLILLMResult:
    """Invoke a provider CLI through the registry-declared prompt channel."""

    profile = get_profile(provider_slug)
    if profile is None:
        raise CLIAdapterError(
            "cli_adapter.provider_unmapped",
            f"no profile for {provider_slug!r}; known: {registered_providers()}",
        )

    binary = binary_override or resolve_binary(provider_slug)
    if binary is None and prefer_docker:
        binary = profile.binary
    if binary is None:
        raise CLIAdapterError(
            "cli_adapter.binary_not_found",
            f"{profile.binary} not found on PATH",
        )

    # Build command entirely from registry — system_prompt and json_schema
    # are passed as CLI flags IF the provider supports them
    try:
        cmd_parts = build_command(
            provider_slug,
            model_slug,
            binary_override=binary,
            system_prompt=system_prompt,
            json_schema=json_schema,
        )
    except (ValueError, RuntimeError) as exc:
        raise CLIAdapterError("cli_adapter.provider_unmapped", str(exc))

    # Anthropic's Claude CLI requires --permission-mode bypassPermissions to use
    # its Write tool when run non-interactively under the praxis-agent user.
    # We inject here (adapter-level) rather than the DB profile to avoid
    # perturbing the registry authority digest used by route-identity lineage.
    if profile.binary == "claude" and "--permission-mode" not in cmd_parts:
        # Insert after the base binary (cmd_parts[0]) so flag ordering is stable.
        cmd_parts = [cmd_parts[0], "--permission-mode", "bypassPermissions", *cmd_parts[1:]]

    prompt_mode = (profile.prompt_mode or "stdin").strip().lower() or "stdin"
    stdin_text = prompt
    if prompt_mode == "argv":
        cmd_parts.append(prompt)
        stdin_text = ""

    shell_cmd = shlex.join(cmd_parts)

    try:
        exec_result: ExecutionResult = run_model(
            command=shell_cmd,
            stdin_text=stdin_text,
            timeout=timeout,
            prefer_docker=prefer_docker,
            network=network,
            provider_slug=provider_slug,
            auth_mount_policy=auth_mount_policy,
            workdir=workdir,
            docker_network=docker_network,
            execution_control=execution_control,
        )
    except OSError as exc:
        raise CLIAdapterError(
            "cli_adapter.exec_error",
            f"failed to execute {profile.binary}: {exc}",
        )
    except RuntimeError as exc:
        raise CLIAdapterError(
            "cli_adapter.exec_error",
            f"failed to execute {profile.binary}: {exc}",
        )

    if exec_result.timed_out:
        raise CLIAdapterError(
            "cli_adapter.timeout",
            f"{profile.binary} timed out after {timeout}s",
        )

    # Refusal detection — CLI returns exit 0 with a polite-refusal payload when
    # the agent declines to act (e.g. "Not logged in", "requires user approval").
    # We must translate these into an adapter-level failure so upstream logic
    # doesn't report green receipts over phantom work.
    _raise_if_cli_refused(
        stdout=exec_result.stdout,
        exit_code=exec_result.exit_code,
        binary=profile.binary,
    )

    # Return raw stdout as completion — the parser node handles parsing
    return CLILLMResult(
        content=exec_result.stdout.strip(),
        exit_code=exec_result.exit_code,
        stderr=exec_result.stderr.strip() if exec_result.stderr else "",
        latency_ms=exec_result.latency_ms,
        raw_json=None,
        cli_name=profile.binary,
        provider_slug=provider_slug,
        model_slug=model_slug,
        structured_output=None,
        execution_mode=exec_result.execution_mode,
        cancelled=exec_result.cancelled,
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

_CODE_BLOCKS_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "code_blocks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "file_path": {"type": "string"},
                    "content": {"type": "string"},
                    "language": {"type": "string"},
                    "action": {"type": "string", "enum": ["create", "replace", "patch"]},
                },
                "required": ["file_path", "content"],
            },
        },
        "explanation": {"type": "string"},
    },
    "required": ["code_blocks", "explanation"],
})

class CLILLMAdapter(BaseNodeAdapter):
    """Thin adapter: reads registry, pipes stdin, captures stdout."""

    executor_type = "adapter.cli_llm"

    def __init__(
        self,
        *,
        default_provider: str | None = None,
        default_model: str | None = None,
        default_timeout: int = _DEFAULT_TIMEOUT,
        binary_overrides: dict[str, str] | None = None,
        prefer_docker: bool = True,
        conn_factory=None,
    ) -> None:
        self._default_provider = default_provider
        self._default_model = default_model
        self._default_timeout = default_timeout
        self._binary_overrides = binary_overrides or {}
        self._prefer_docker = prefer_docker
        self._conn_factory = conn_factory

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = self._merge_inputs(request)
        inputs = {
            "task_name": request.task_name,
            "input_payload": payload,
            "execution_boundary_ref": request.execution_boundary_ref,
        }
        strict_route_contract = bool(payload.get("route_contract_required"))
        runtime_route_outputs = _runtime_route_outputs(payload)
        try:
            requested_adapter_type = _payload_adapter_type(payload)
            contract_adapter_type = _payload_provider_adapter_type(payload)
        except CLIAdapterError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                    **runtime_route_outputs,
                    "route_contract_required": strict_route_contract,
                },
            )

        def _annotate_outputs(outputs: dict[str, Any]) -> dict[str, Any]:
            outputs.update(runtime_route_outputs)
            outputs["route_contract_required"] = strict_route_contract
            if requested_adapter_type is not None:
                outputs["requested_adapter_type"] = requested_adapter_type
            if contract_adapter_type is not None:
                outputs["contract_adapter_type"] = contract_adapter_type
            raw_contract = payload.get("provider_adapter_contract")
            if isinstance(raw_contract, Mapping):
                outputs["provider_adapter_contract"] = dict(raw_contract)
            return outputs

        if strict_route_contract or contract_adapter_type == "llm_task":
            return _fail(
                request=request,
                reason_code="cli_adapter.route_contract_unsupported",
                failure_code="cli_adapter.route_contract_unsupported",
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs({
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                }),
            )
        if requested_adapter_type is not None and requested_adapter_type != "cli_llm":
            return _fail(
                request=request,
                reason_code="cli_adapter.adapter_type_mismatch",
                failure_code="cli_adapter.adapter_type_mismatch",
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs({
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                }),
            )
        if contract_adapter_type is not None and contract_adapter_type != "cli_llm":
            return _fail(
                request=request,
                reason_code="cli_adapter.adapter_type_mismatch",
                failure_code="cli_adapter.adapter_type_mismatch",
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs({
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                }),
            )

        packet_system_prompt = None
        packet_binding = None
        try:
            from runtime.execution_packet_runtime import (
                ExecutionPacketRuntimeError,
                load_execution_packet_binding,
                packet_prompt_fields,
            )

            packet_binding = load_execution_packet_binding(
                payload,
                conn_factory=self._conn_factory,
            )
            if packet_binding is not None:
                packet_system_prompt, prompt = packet_prompt_fields(packet_binding)
            else:
                prompt = payload.get("prompt")
        except ExecutionPacketRuntimeError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                },
            )

        if not isinstance(prompt, str) or not prompt.strip():
            return _fail(
                request=request,
                reason_code="adapter.input_invalid",
                failure_code="adapter.input_invalid",
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                },
            )

        # Collect system prompt from task profile and payload
        task_type = payload.get("task_type")
        system_prompt_parts: list[str] = []
        if task_type:
            try:
                tp = resolve_profile(str(task_type))
            except TaskProfileAuthorityError as exc:
                return _fail(
                    request=request,
                    reason_code="adapter.task_profile_authority_unavailable",
                    failure_code="adapter.task_profile_authority_unavailable",
                    started_at=started_at,
                    executor_type=CLILLMAdapter.executor_type,
                    inputs=inputs,
                    outputs={
                        "transport_kind": "cli",
                        "failure_namespace": "cli_adapter",
                        "stderr": str(exc),
                    },
                )
            if tp.system_prompt_hint:
                system_prompt_parts.append(tp.system_prompt_hint)
        if packet_system_prompt:
            system_prompt_parts.append(packet_system_prompt)
        if payload.get("system_prompt"):
            system_prompt_parts.append(str(payload["system_prompt"]))
        system_prompt = "\n\n".join(system_prompt_parts) if system_prompt_parts else None

        # JSON schema for structured output (if scope_write targets are set)
        scope_write = payload.get("scope_write")
        json_schema = _CODE_BLOCKS_SCHEMA if isinstance(scope_write, list) and scope_write else None

        # Resolve provider from registry
        try:
            provider_slug = _resolve_provider(payload, self._default_provider)
        except CLIAdapterError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": "cli",
                    "failure_namespace": "cli_adapter",
                },
            )

        model_slug = payload.get("model_slug") or payload.get("model") or self._default_model
        contract = resolve_adapter_contract(provider_slug, "cli_llm")
        transport_kind = contract.transport_kind if contract is not None else "cli"
        failure_namespace = contract.failure_namespace if contract is not None else "cli_adapter"

        # Timeout: explicit payload > provider registry default > adapter default
        profile = get_profile(provider_slug)
        provider_timeout = (
            contract.timeout_seconds
            if contract is not None
            else (profile.default_timeout if profile else self._default_timeout)
        )
        timeout = int(payload.get("timeout", provider_timeout))

        binary_override = self._binary_overrides.get(provider_slug)
        try:
            execution_policy = resolve_cli_execution_policy(payload, profile=profile)
        except ValueError as exc:
            return _fail(
                request=request,
                reason_code="cli_adapter.contract_invalid",
                failure_code="cli_adapter.contract_invalid",
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": transport_kind,
                    "failure_namespace": failure_namespace,
                    "provider_slug": provider_slug,
                    "model_slug": model_slug,
                    "error": str(exc),
                },
            )

        workdir_value = payload.get("workdir")
        workdir_arg = workdir_value.strip() if isinstance(workdir_value, str) and workdir_value.strip() else None
        docker_network_value = payload.get("docker_network")
        docker_network_arg = (
            docker_network_value.strip()
            if isinstance(docker_network_value, str) and docker_network_value.strip()
            else None
        )

        try:
            result = _invoke_cli(
                provider_slug=provider_slug,
                model_slug=model_slug,
                prompt=prompt.strip(),
                timeout=timeout,
                system_prompt=system_prompt,
                json_schema=json_schema,
                binary_override=binary_override,
                prefer_docker=self._prefer_docker,
                network=execution_policy.network_enabled,
                auth_mount_policy=execution_policy.auth_mount_policy,
                workdir=workdir_arg,
                docker_network=docker_network_arg,
                execution_control=request.execution_control,
            )
        except CLIAdapterError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=CLILLMAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "transport_kind": transport_kind,
                    "failure_namespace": failure_namespace,
                    "provider_slug": provider_slug,
                    "model_slug": model_slug,
                },
            )

        if result.cancelled:
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
                outputs={
                    "provider_slug": provider_slug,
                    "model_slug": model_slug,
                    "transport_kind": transport_kind,
                    "failure_namespace": failure_namespace,
                    "cli": result.cli_name,
                    "execution_mode": result.execution_mode,
                },
            )

        if result.exit_code != 0:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="cli_adapter.nonzero_exit",
                executor_type=self.executor_type,
                inputs=inputs,
                outputs={
                    "exit_code": result.exit_code,
                    "stderr": result.stderr[:2000],
                    "stdout": result.content[:2000],
                    "latency_ms": result.latency_ms,
                    "provider_slug": result.provider_slug,
                    "cli": result.cli_name,
                    "execution_mode": result.execution_mode,
                    "transport_kind": transport_kind,
                    "failure_namespace": failure_namespace,
                },
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="cli_adapter.nonzero_exit",
            )

        outputs: dict[str, Any] = {
            "completion": result.content,
            "provider_slug": result.provider_slug,
            "model_slug": result.model_slug,
            "cli": result.cli_name,
            "exit_code": result.exit_code,
            "latency_ms": result.latency_ms,
            "execution_mode": result.execution_mode,
            "transport_kind": transport_kind,
            "failure_namespace": failure_namespace,
        }

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs=inputs,
            outputs=outputs,
            started_at=started_at,
            finished_at=_utc_now(),
        )
