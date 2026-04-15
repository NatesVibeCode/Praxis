"""LLM task adapter for real API execution.

Translates a DeterministicTaskRequest into an LLM API call and returns
the response as a DeterministicTaskResult. The orchestrator and evidence
system are unchanged — only the adapter does real work.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _fail
from .credentials import CredentialResolutionError, resolve_credential
from .deterministic import (
    BaseNodeAdapter,
    DeterministicTaskRequest,
    DeterministicTaskResult,
    cancelled_task_result,
)
from .llm_client import LLMClientError, LLMRequest, LLMResponse, call_llm
from .provider_registry import (
    ProviderAdapterContract,
    default_model_for_provider,
    default_provider_slug,
    resolve_adapter_contract,
    resolve_api_endpoint,
    resolve_api_protocol_family,
    supports_adapter,
)
from .task_profiles import try_resolve_profile

_DEFAULT_MAX_TOKENS = 4096
_DEFAULT_TEMPERATURE = 0.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _extract_messages(
    input_payload: Mapping[str, Any],
) -> tuple[dict[str, str], ...] | None:
    """Extract chat messages from input_payload."""

    messages = input_payload.get("messages")
    if isinstance(messages, (list, tuple)) and messages:
        return tuple(
            {"role": str(m.get("role", "user")), "content": str(m.get("content", ""))}
            for m in messages
            if isinstance(m, Mapping)
        )

    prompt = input_payload.get("prompt")
    if isinstance(prompt, str) and prompt.strip():
        return ({"role": "user", "content": prompt.strip()},)

    return None


def _failure_outputs(
    *,
    transport_kind: str,
    failure_namespace: str,
    provider_slug: str,
    model_slug: str | None,
    endpoint_uri: str | None = None,
    protocol_family: str | None = None,
    status_code: int | None = None,
    stderr: str | None = None,
    auth_ref: str | None = None,
) -> dict[str, Any]:
    outputs: dict[str, Any] = {
        "transport_kind": transport_kind,
        "failure_namespace": failure_namespace,
        "provider_slug": provider_slug,
    }
    if model_slug is not None:
        outputs["model_slug"] = model_slug
    if endpoint_uri:
        outputs["endpoint_uri"] = endpoint_uri
    if protocol_family:
        outputs["protocol_family"] = protocol_family
    if status_code is not None:
        outputs["status_code"] = status_code
    if stderr:
        outputs["stderr"] = stderr
    if auth_ref:
        outputs["auth_ref"] = auth_ref
    return outputs


class _AdapterRuntimeContractError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _mapping_value(value: object, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    raise _AdapterRuntimeContractError(
        "adapter.contract_invalid",
        f"{field_name} must be a mapping",
    )


def _text_value(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            f"{field_name} must be a non-empty string",
        )
    return value.strip()


def _int_value(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            f"{field_name} must be an integer",
        )
    return value


def _string_tuple(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            f"{field_name} must be a list",
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_text_value(item, field_name=f"{field_name}[{index}]"))
    return tuple(normalized)


def _string_mapping(value: object, *, field_name: str) -> dict[str, str]:
    mapping = _mapping_value(value, field_name=field_name)
    normalized: dict[str, str] = {}
    for key, item in mapping.items():
        normalized[_text_value(key, field_name=f"{field_name}.key")] = _text_value(
            item,
            field_name=f"{field_name}.{key}",
        )
    return normalized


def _payload_adapter_type(input_payload: Mapping[str, Any]) -> str | None:
    raw_adapter_type = input_payload.get("adapter_type")
    if raw_adapter_type is None:
        return None
    return _text_value(raw_adapter_type, field_name="input_payload.adapter_type")


def _optional_string_list(value: object) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            return None
        normalized.append(item.strip())
    return normalized


def _payload_provider_adapter_contract(
    input_payload: Mapping[str, Any],
    *,
    provider_slug: str,
    strict_required: bool,
) -> ProviderAdapterContract | None:
    raw_contract = input_payload.get("provider_adapter_contract")
    if raw_contract is None:
        if strict_required:
            raise _AdapterRuntimeContractError(
                "adapter.contract_required",
                "provider_adapter_contract is required for a strict runtime route contract",
            )
        return None

    contract_payload = _mapping_value(
        raw_contract,
        field_name="provider_adapter_contract",
    )
    contract = ProviderAdapterContract(
        provider_slug=_text_value(
            contract_payload.get("provider_slug"),
            field_name="provider_adapter_contract.provider_slug",
        ),
        adapter_type=_text_value(
            contract_payload.get("adapter_type"),
            field_name="provider_adapter_contract.adapter_type",
        ),
        transport_kind=_text_value(
            contract_payload.get("transport_kind"),
            field_name="provider_adapter_contract.transport_kind",
        ),
        execution_kind=_text_value(
            contract_payload.get("execution_kind"),
            field_name="provider_adapter_contract.execution_kind",
        ),
        failure_namespace=_text_value(
            contract_payload.get("failure_namespace"),
            field_name="provider_adapter_contract.failure_namespace",
        ),
        prompt_envelope=_mapping_value(
            contract_payload.get("prompt_envelope"),
            field_name="provider_adapter_contract.prompt_envelope",
        ),
        tool_policy=_mapping_value(
            contract_payload.get("tool_policy"),
            field_name="provider_adapter_contract.tool_policy",
        ),
        structured_output=_mapping_value(
            contract_payload.get("structured_output"),
            field_name="provider_adapter_contract.structured_output",
        ),
        timeout_seconds=_int_value(
            contract_payload.get("timeout_seconds"),
            field_name="provider_adapter_contract.timeout_seconds",
        ),
        telemetry=_mapping_value(
            contract_payload.get("telemetry"),
            field_name="provider_adapter_contract.telemetry",
        ),
        retry_policy=_mapping_value(
            contract_payload.get("retry_policy"),
            field_name="provider_adapter_contract.retry_policy",
        ),
        failure_mapping=_string_mapping(
            contract_payload.get("failure_mapping"),
            field_name="provider_adapter_contract.failure_mapping",
        ),
        readiness=_mapping_value(
            contract_payload.get("readiness"),
            field_name="provider_adapter_contract.readiness",
        ),
        retryable_failure_codes=_string_tuple(
            contract_payload.get("retryable_failure_codes"),
            field_name="provider_adapter_contract.retryable_failure_codes",
        ),
        failover_failure_codes=_string_tuple(
            contract_payload.get("failover_failure_codes"),
            field_name="provider_adapter_contract.failover_failure_codes",
        ),
    )
    if contract.provider_slug != provider_slug:
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "provider_adapter_contract provider_slug does not match the payload provider_slug",
        )
    if contract.adapter_type != "llm_task":
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "provider_adapter_contract adapter_type must be llm_task for LLMTaskAdapter",
        )
    if contract.transport_kind != "http" or contract.execution_kind != "request":
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "provider_adapter_contract must describe an HTTP request adapter",
        )
    return contract


def _runtime_route_outputs(input_payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_route = input_payload.get("runtime_route")
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


def _strict_runtime_route_outputs(
    input_payload: Mapping[str, Any],
    *,
    provider_slug: str,
) -> dict[str, Any]:
    outputs = _runtime_route_outputs(input_payload)
    required_fields = (
        "route_decision_id",
        "route_eligibility_state_id",
        "selected_candidate_ref",
        "selected_provider_failover_binding_id",
        "provider_endpoint_binding_id",
        "provider_ref",
        "provider_slug",
        "model_slug",
        "balance_slot",
        "decision_reason_code",
        "allowed_candidate_refs",
        "failover_role",
        "failover_trigger_rule",
        "failover_position_index",
        "failover_slice_candidate_refs",
        "endpoint_kind",
        "endpoint_transport_kind",
        "route_authority",
        "failover_endpoint_authority",
        "as_of",
    )
    missing_fields = [field_name for field_name in required_fields if field_name not in outputs]
    if missing_fields:
        raise _AdapterRuntimeContractError(
            "adapter.runtime_route_required",
            "runtime_route is required and must expose the resolved route and failover authority for a strict runtime route contract",
        )
    if outputs["provider_slug"] != provider_slug:
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "runtime_route provider_slug does not match the payload provider_slug",
        )
    selected_candidate_ref = outputs["selected_candidate_ref"]
    allowed_candidate_refs = outputs["allowed_candidate_refs"]
    failover_slice_candidate_refs = outputs["failover_slice_candidate_refs"]
    failover_position_index = outputs["failover_position_index"]
    if selected_candidate_ref not in allowed_candidate_refs:
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "runtime_route selected_candidate_ref must be present in allowed_candidate_refs",
        )
    if selected_candidate_ref not in failover_slice_candidate_refs:
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "runtime_route selected_candidate_ref must be present in failover_slice_candidate_refs",
        )
    if failover_position_index >= len(failover_slice_candidate_refs):
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "runtime_route failover_position_index is out of range for failover_slice_candidate_refs",
        )
    if failover_slice_candidate_refs[failover_position_index] != selected_candidate_ref:
        raise _AdapterRuntimeContractError(
            "adapter.contract_invalid",
            "runtime_route failover_position_index must point at selected_candidate_ref",
        )
    return outputs


class LLMTaskAdapter(BaseNodeAdapter):
    """Adapter that calls a real LLM API and returns results as evidence."""

    executor_type = "adapter.llm_task"

    def __init__(
        self,
        *,
        default_provider: str | None = None,
        default_model: str | None = None,
        default_max_tokens: int = _DEFAULT_MAX_TOKENS,
        default_temperature: float = _DEFAULT_TEMPERATURE,
        credential_env: dict[str, str] | None = None,
        conn_factory=None,
    ) -> None:
        self._default_provider = default_provider or default_provider_slug()
        self._default_model = default_model or default_model_for_provider(self._default_provider)
        self._default_max_tokens = default_max_tokens
        self._default_temperature = default_temperature
        self._credential_env = credential_env
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
        if request.execution_control is not None and request.execution_control.cancel_requested():
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
            )
        strict_route_contract = bool(payload.get("route_contract_required"))
        provider_slug = str(payload.get("provider_slug") or self._default_provider)
        runtime_route_outputs = _runtime_route_outputs(payload)
        registry_contract = None if strict_route_contract else resolve_adapter_contract(
            provider_slug,
            "llm_task",
        )
        try:
            requested_adapter_type = _payload_adapter_type(payload)
            if requested_adapter_type is not None and requested_adapter_type != "llm_task":
                raise _AdapterRuntimeContractError(
                    "adapter.adapter_type_mismatch",
                    "input_payload.adapter_type must be llm_task for LLMTaskAdapter",
                )
            if strict_route_contract:
                provider_slug = _text_value(
                    payload.get("provider_slug"),
                    field_name="input_payload.provider_slug",
                )
                runtime_route_outputs = _strict_runtime_route_outputs(
                    payload,
                    provider_slug=provider_slug,
                )
            else:
                provider_slug = str(payload.get("provider_slug", self._default_provider))
                runtime_route_outputs = _runtime_route_outputs(payload)
            payload_contract = _payload_provider_adapter_contract(
                payload,
                provider_slug=provider_slug,
                strict_required=strict_route_contract,
            )
        except _AdapterRuntimeContractError as exc:
            transport_kind = (
                registry_contract.transport_kind if registry_contract is not None else "http"
            )
            failure_namespace = (
                registry_contract.failure_namespace
                if registry_contract is not None
                else "adapter"
            )
            outputs = _failure_outputs(
                transport_kind=transport_kind,
                failure_namespace=failure_namespace,
                provider_slug=provider_slug,
                model_slug=None,
                stderr=str(exc),
            )
            outputs.update(runtime_route_outputs)
            outputs["route_contract_required"] = strict_route_contract
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=outputs,
            )
        contract = payload_contract or registry_contract
        transport_kind = contract.transport_kind if contract is not None else "http"
        failure_namespace = contract.failure_namespace if contract is not None else "adapter"
        prompt_envelope = contract.prompt_envelope if contract is not None else {}
        retry_policy = contract.retry_policy if contract is not None else {}
        contract_payload = contract.to_contract() if contract is not None else None

        def _annotate_outputs(outputs: dict[str, Any]) -> dict[str, Any]:
            outputs.update(runtime_route_outputs)
            outputs["route_contract_required"] = strict_route_contract
            if contract_payload is not None:
                outputs["provider_adapter_contract"] = contract_payload
            return outputs

        try:
            from runtime.execution_packet_runtime import (
                ExecutionPacketRuntimeError,
                load_execution_packet_binding,
            )

            packet_binding = load_execution_packet_binding(
                payload,
                conn_factory=self._conn_factory,
            )
            if packet_binding is not None:
                messages = packet_binding.messages
            else:
                messages = _extract_messages(payload)
        except ExecutionPacketRuntimeError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=None,
                    stderr=str(exc),
                )),
            )
        if messages is None:
            return _fail(
                request=request,
                reason_code="adapter.input_invalid",
                failure_code="adapter.input_invalid",
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=None,
                )),
            )
        if not strict_route_contract and not supports_adapter(provider_slug, "llm_task"):
            return _fail(
                request=request,
                reason_code="adapter.transport_unsupported",
                failure_code="adapter.transport_unsupported",
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=None,
                )),
            )

        system_prompt_parts: list[str] = []
        task_type = payload.get("task_type")
        if task_type:
            tp = try_resolve_profile(str(task_type))
            if tp is not None and tp.system_prompt_hint:
                system_prompt_parts.append(tp.system_prompt_hint)
        if payload.get("system_prompt"):
            system_prompt_parts.append(str(payload["system_prompt"]))
        system_prompt = "\n\n".join(system_prompt_parts) if system_prompt_parts else None
        raw_auth_ref = payload.get("auth_ref")
        if strict_route_contract:
            auth_ref = str(raw_auth_ref or "").strip()
            if not auth_ref:
                return _fail(
                    request=request,
                    reason_code="adapter.auth_ref_required",
                    failure_code="adapter.auth_ref_required",
                    started_at=started_at,
                    executor_type=LLMTaskAdapter.executor_type,
                    inputs=inputs,
                    outputs=_annotate_outputs(_failure_outputs(
                        transport_kind=transport_kind,
                        failure_namespace=failure_namespace,
                        provider_slug=provider_slug,
                        model_slug=None,
                    )),
                )
        else:
            auth_ref = str(
                payload.get("auth_ref", f"secret.default-path.{provider_slug}")
            )

        raw_model_slug = payload.get("model_slug") or payload.get("model")
        if strict_route_contract:
            model_slug = str(raw_model_slug or "").strip()
        else:
            model_slug = str(
                raw_model_slug
                or self._default_model
                or default_model_for_provider(provider_slug)
                or ""
            ).strip()
        if not model_slug:
            return _fail(
                request=request,
                reason_code="adapter.model_required",
                failure_code="adapter.model_required",
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=None,
                    auth_ref=auth_ref,
                )),
            )
        if strict_route_contract and runtime_route_outputs.get("model_slug") != model_slug:
            return _fail(
                request=request,
                reason_code="adapter.contract_invalid",
                failure_code="adapter.contract_invalid",
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    auth_ref=auth_ref,
                    stderr="runtime_route model_slug does not match the payload model_slug",
                )),
            )

        max_tokens = int(payload.get("max_tokens", self._default_max_tokens))
        temperature = float(payload.get("temperature", self._default_temperature))
        raw_endpoint_uri = payload.get("endpoint_uri")
        if strict_route_contract:
            endpoint_uri = str(raw_endpoint_uri or "").strip()
        else:
            endpoint_uri = str(
                raw_endpoint_uri
                or resolve_api_endpoint(provider_slug, model_slug=model_slug)
                or ""
            ).strip()
        if not endpoint_uri:
            return _fail(
                request=request,
                reason_code=(
                    "adapter.endpoint_required"
                    if strict_route_contract
                    else "adapter.endpoint_unavailable"
                ),
                failure_code=(
                    "adapter.endpoint_required"
                    if strict_route_contract
                    else "adapter.endpoint_unavailable"
                ),
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    auth_ref=auth_ref,
                )),
            )

        try:
            credential = resolve_credential(auth_ref, env=self._credential_env)
        except CredentialResolutionError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    endpoint_uri=endpoint_uri,
                    auth_ref=auth_ref,
                    stderr=str(exc),
                )),
            )

        timeout_seconds = payload.get("timeout_seconds", payload.get("timeout"))
        if timeout_seconds is None and contract is not None:
            timeout_seconds = contract.timeout_seconds
        retry_attempts = retry_policy.get("retry_attempts")
        retry_backoff_seconds = retry_policy.get("backoff_seconds")
        retryable_status_codes = retry_policy.get("retryable_status_codes")
        try:
            if strict_route_contract:
                protocol_family = _text_value(
                    prompt_envelope.get("protocol_family"),
                    field_name="provider_adapter_contract.prompt_envelope.protocol_family",
                )
            else:
                protocol_family = (
                    prompt_envelope.get("protocol_family")
                    or resolve_api_protocol_family(provider_slug)
                )
        except _AdapterRuntimeContractError as exc:
            return _fail(
                request=request,
                reason_code=exc.reason_code,
                failure_code=exc.reason_code,
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    endpoint_uri=endpoint_uri,
                    auth_ref=auth_ref,
                    stderr=str(exc),
                )),
            )

        try:
            normalized_timeout_seconds = int(timeout_seconds) if timeout_seconds is not None else None
        except (TypeError, ValueError):
            normalized_timeout_seconds = contract.timeout_seconds if contract is not None else None

        try:
            normalized_retry_attempts = int(retry_attempts) if retry_attempts is not None else None
        except (TypeError, ValueError):
            normalized_retry_attempts = None

        llm_request = LLMRequest(
            endpoint_uri=endpoint_uri,
            api_key=credential.api_key,
            provider_slug=provider_slug,
            model_slug=model_slug,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            system_prompt=system_prompt,
            protocol_family=protocol_family,
            timeout_seconds=normalized_timeout_seconds,
            retry_attempts=normalized_retry_attempts,
            retry_backoff_seconds=tuple(retry_backoff_seconds) if retry_backoff_seconds is not None else None,
            retryable_status_codes=tuple(retryable_status_codes) if retryable_status_codes is not None else None,
            execution_control=request.execution_control,
        )

        if request.execution_control is not None and request.execution_control.cancel_requested():
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
                outputs=_annotate_outputs(
                    _failure_outputs(
                        transport_kind=transport_kind,
                        failure_namespace=failure_namespace,
                        provider_slug=provider_slug,
                        model_slug=model_slug,
                        endpoint_uri=endpoint_uri,
                        protocol_family=llm_request.protocol_family,
                    )
                ),
            )

        try:
            response = call_llm(llm_request)
        except LLMClientError as exc:
            if exc.reason_code == "llm_client.cancelled":
                return cancelled_task_result(
                    request=request,
                    executor_type=LLMTaskAdapter.executor_type,
                    started_at=started_at,
                    inputs=inputs,
                    outputs=_annotate_outputs(_failure_outputs(
                        transport_kind=transport_kind,
                        failure_namespace=failure_namespace,
                        provider_slug=provider_slug,
                        model_slug=model_slug,
                        endpoint_uri=endpoint_uri,
                        protocol_family=llm_request.protocol_family,
                    )),
                )
            if contract is not None:
                reason_code = contract.map_failure_code(exc.reason_code)
            elif exc.reason_code == "llm_client.http_error":
                reason_code = f"{failure_namespace}.http_error"
            elif exc.reason_code == "llm_client.network_error":
                reason_code = f"{failure_namespace}.network_error"
            elif exc.reason_code == "llm_client.timeout":
                reason_code = f"{failure_namespace}.timeout"
            elif exc.reason_code == "llm_client.response_parse_error":
                reason_code = f"{failure_namespace}.response_parse_error"
            else:
                reason_code = f"{failure_namespace}.network_error"
            return _fail(
                request=request,
                reason_code=reason_code,
                failure_code=reason_code,
                started_at=started_at,
                executor_type=LLMTaskAdapter.executor_type,
                inputs=inputs,
                outputs=_annotate_outputs(_failure_outputs(
                    transport_kind=transport_kind,
                    failure_namespace=failure_namespace,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    endpoint_uri=endpoint_uri,
                    protocol_family=llm_request.protocol_family,
                    status_code=exc.status_code,
                    stderr=str(exc),
                )),
            )

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs=inputs,
            outputs={
                "completion": response.content,
                "model": response.model,
                "provider": response.provider_slug,
                "usage": response.usage,
                "latency_ms": response.latency_ms,
                "transport_kind": transport_kind,
                "failure_namespace": failure_namespace,
                "endpoint_uri": endpoint_uri,
                "auth_ref": auth_ref,
                "route_contract_required": strict_route_contract,
                **runtime_route_outputs,
                **(
                    {"provider_adapter_contract": contract_payload}
                    if contract_payload is not None
                    else {}
                ),
            },
            started_at=started_at,
            finished_at=_utc_now(),
        )
