"""LLM request-contract compiler.

Provider adapters should not guess which knobs are safe for a model. This
module exposes a shaped request-intent JSON contract for LLM callers, then
compiles that intent through provider/model authority into the payload that is
safe to send. Runtime shaping is a backstop for old callers; the primary
control is not exposing invalid knobs in the first place.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any


_STRUCTURAL_KEYS = frozenset(
    {
        "model",
        "messages",
        "contents",
        "generationConfig",
        "system",
        "systemInstruction",
        "tools",
    }
)


class LLMRequestContractError(ValueError):
    """Raised when a request intent violates the provider contract."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class LLMRequestContract:
    contract_ref: str
    provider_slug: str
    model_slug: str
    transport_type: str = "API"
    protocol_family: str | None = None
    task_type: str = "*"
    runtime_profile_ref: str = "*"
    context_window_tokens: int | None = None
    max_output_tokens: int | None = None
    supported_parameters: frozenset[str] = frozenset()
    forbidden_parameters: frozenset[str] = frozenset()
    unsupported_parameter_policy: str = "omit"
    forbidden_parameter_policy: str = "omit"
    combination_violation_policy: str = "shape"
    sampling_policy: Mapping[str, Any] | None = None
    reasoning_policy: Mapping[str, Any] | None = None
    cache_policy: Mapping[str, Any] | None = None
    structured_output_policy: Mapping[str, Any] | None = None
    tool_call_policy: Mapping[str, Any] | None = None
    truncation_policy: Mapping[str, Any] | None = None
    state_carry_policy: Mapping[str, Any] | None = None
    streaming_policy: Mapping[str, Any] | None = None
    telemetry_policy: Mapping[str, Any] | None = None
    tokenizer_ref: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LLMRequestContract":
        provider_slug = _require_text(payload.get("provider_slug"), "provider_slug")
        model_slug = _require_text(payload.get("model_slug"), "model_slug")
        return cls(
            contract_ref=str(
                payload.get("contract_ref")
                or payload.get("llm_request_contract_id")
                or f"llm_request_contract.{provider_slug}.{model_slug}"
            ),
            provider_slug=provider_slug,
            model_slug=model_slug,
            transport_type=str(payload.get("transport_type") or "API").upper(),
            protocol_family=_optional_text(payload.get("protocol_family")),
            task_type=str(payload.get("task_type") or "*"),
            runtime_profile_ref=str(payload.get("runtime_profile_ref") or "*"),
            context_window_tokens=_optional_int(payload.get("context_window_tokens")),
            max_output_tokens=_optional_int(payload.get("max_output_tokens")),
            supported_parameters=frozenset(_string_set(payload.get("supported_parameters"))),
            forbidden_parameters=frozenset(_string_set(payload.get("forbidden_parameters"))),
            unsupported_parameter_policy=str(
                payload.get("unsupported_parameter_policy") or "omit"
            ).strip().lower(),
            forbidden_parameter_policy=str(
                payload.get("forbidden_parameter_policy") or "omit"
            ).strip().lower(),
            combination_violation_policy=str(
                payload.get("combination_violation_policy") or "shape"
            ).strip().lower(),
            sampling_policy=_mapping_or_none(payload.get("sampling_policy")),
            reasoning_policy=_mapping_or_none(payload.get("reasoning_policy")),
            cache_policy=_mapping_or_none(payload.get("cache_policy")),
            structured_output_policy=_mapping_or_none(
                payload.get("structured_output_policy")
            ),
            tool_call_policy=_mapping_or_none(payload.get("tool_call_policy")),
            truncation_policy=_mapping_or_none(payload.get("truncation_policy")),
            state_carry_policy=_mapping_or_none(payload.get("state_carry_policy")),
            streaming_policy=_mapping_or_none(payload.get("streaming_policy")),
            telemetry_policy=_mapping_or_none(payload.get("telemetry_policy")),
            tokenizer_ref=_optional_text(payload.get("tokenizer_ref")),
        )


@dataclass(frozen=True, slots=True)
class MaterializedLLMPayload:
    payload: dict[str, Any]
    contract_ref: str
    applied_parameters: tuple[str, ...]
    omitted_parameters: tuple[str, ...]
    policy_decisions: tuple[Mapping[str, Any], ...]

    def receipt_payload(self) -> dict[str, Any]:
        return {
            "contract_ref": self.contract_ref,
            "applied_parameters": list(self.applied_parameters),
            "omitted_parameters": list(self.omitted_parameters),
            "policy_decisions": [dict(item) for item in self.policy_decisions],
        }


def compile_llm_request_body(
    *,
    contract: LLMRequestContract | Mapping[str, Any],
    request_intent: Mapping[str, Any] | None,
    base_payload: Mapping[str, Any],
    provider_slug: str,
    model_slug: str,
    protocol_family: str,
) -> MaterializedLLMPayload:
    """Compile a provider body through the request contract."""

    normalized_contract = (
        contract if isinstance(contract, LLMRequestContract) else LLMRequestContract.from_mapping(contract)
    )
    normalized_provider = _require_text(provider_slug, "provider_slug").lower()
    normalized_model = _require_text(model_slug, "model_slug")
    if normalized_contract.provider_slug.lower() != normalized_provider:
        raise LLMRequestContractError(
            "llm_request_contract.provider_mismatch",
            "request provider does not match the LLM request contract",
            details={
                "request_provider_slug": normalized_provider,
                "contract_provider_slug": normalized_contract.provider_slug,
            },
        )
    if normalized_contract.model_slug != normalized_model:
        raise LLMRequestContractError(
            "llm_request_contract.model_mismatch",
            "request model does not match the LLM request contract",
            details={
                "request_model_slug": normalized_model,
                "contract_model_slug": normalized_contract.model_slug,
            },
        )

    payload = deepcopy(dict(base_payload))
    intent = dict(request_intent or {})
    applied: set[str] = set()
    omitted: set[str] = set()
    decisions: list[Mapping[str, Any]] = []

    _apply_sampling_intent(
        payload=payload,
        intent=intent,
        contract=normalized_contract,
        protocol_family=protocol_family,
        applied=applied,
        decisions=decisions,
    )
    _apply_output_ceiling_intent(
        payload=payload,
        intent=intent,
        protocol_family=protocol_family,
        applied=applied,
    )
    _apply_reasoning_intent(
        payload=payload,
        intent=intent,
        contract=normalized_contract,
        protocol_family=protocol_family,
        applied=applied,
    )
    _apply_structured_output_intent(
        payload=payload,
        intent=intent,
        contract=normalized_contract,
        protocol_family=protocol_family,
        applied=applied,
        decisions=decisions,
    )

    temperature = _extract_temperature(payload, protocol_family=protocol_family)
    max_tokens = _extract_max_tokens(payload, protocol_family=protocol_family)
    reasoning_budget = _extract_reasoning_budget(
        payload,
        intent=intent,
        protocol_family=protocol_family,
    )

    sampling_policy = dict(normalized_contract.sampling_policy or {})
    if temperature is not None and sampling_policy.get("temperature_mode") in {
        "provider_default_only",
        "locked_provider_default",
    }:
        _handle_shape_violation(
            contract=normalized_contract,
            reason_code="llm_request_contract.temperature_hidden_by_contract",
            message="temperature override is hidden by this request contract",
            details={
                "temperature": temperature,
                "temperature_mode": sampling_policy.get("temperature_mode"),
            },
        )
        _remove_canonical_parameter(payload, "temperature", protocol_family=protocol_family)
        omitted.add("temperature")
        decisions.append(
            {
                "policy": "sampling",
                "decision": "temperature_omitted",
                "reason": "provider_default_only",
            }
        )

    if (
        protocol_family == "anthropic_messages"
        and temperature is not None
        and _anthropic_thinking_enabled(payload, reasoning_budget=reasoning_budget)
        and dict(normalized_contract.reasoning_policy or {}).get(
            "forbid_temperature_when_thinking",
            True,
        )
    ):
        _handle_shape_violation(
            contract=normalized_contract,
            reason_code="llm_request_contract.temperature_hidden_by_thinking_policy",
            message="Anthropic thinking hides temperature for this contract",
            details={
                "temperature": temperature,
                "reasoning_budget_tokens": reasoning_budget,
            },
        )
        _remove_canonical_parameter(payload, "temperature", protocol_family=protocol_family)
        omitted.add("temperature")
        decisions.append(
            {
                "policy": "reasoning",
                "decision": "temperature_omitted",
                "reason": "anthropic_thinking_enabled",
            }
        )

    output_budget = _optional_int(intent.get("output_ceiling_tokens")) or max_tokens
    if (
        reasoning_budget is not None
        and output_budget is not None
        and reasoning_budget > output_budget
    ):
        _handle_shape_violation(
            contract=normalized_contract,
            reason_code="llm_request_contract.reasoning_budget_clamped",
            message="reasoning budget was shaped to the output budget",
            details={
                "reasoning_budget_tokens": reasoning_budget,
                "output_budget_tokens": output_budget,
            },
        )
        _set_reasoning_budget(
            payload,
            budget_tokens=output_budget,
            protocol_family=protocol_family,
        )
        decisions.append(
            {
                "policy": "reasoning",
                "decision": "reasoning_budget_clamped",
                "from_tokens": reasoning_budget,
                "to_tokens": output_budget,
            }
        )

    if (
        normalized_contract.max_output_tokens is not None
        and max_tokens is not None
        and max_tokens > normalized_contract.max_output_tokens
    ):
        _handle_shape_violation(
            contract=normalized_contract,
            reason_code="llm_request_contract.output_limit_exceeded",
            message="request output budget was shaped to the contract max_output_tokens",
            details={
                "max_tokens": max_tokens,
                "max_output_tokens": normalized_contract.max_output_tokens,
            },
        )
        _set_max_tokens(
            payload,
            max_tokens=normalized_contract.max_output_tokens,
            protocol_family=protocol_family,
        )
        decisions.append(
            {
                "policy": "output_ceiling",
                "decision": "max_tokens_clamped",
                "from_tokens": max_tokens,
                "to_tokens": normalized_contract.max_output_tokens,
            }
        )

    _enforce_parameter_sets(
        payload=payload,
        protocol_family=protocol_family,
        contract=normalized_contract,
        omitted=omitted,
        decisions=decisions,
    )

    applied.update(
        param
        for param in _present_canonical_parameters(payload, protocol_family=protocol_family)
        if param not in omitted
    )
    return MaterializedLLMPayload(
        payload=payload,
        contract_ref=normalized_contract.contract_ref,
        applied_parameters=tuple(sorted(applied)),
        omitted_parameters=tuple(sorted(omitted)),
        policy_decisions=tuple(decisions),
    )


def _apply_sampling_intent(
    *,
    payload: dict[str, Any],
    intent: Mapping[str, Any],
    contract: LLMRequestContract,
    protocol_family: str,
    applied: set[str],
    decisions: list[Mapping[str, Any]],
) -> None:
    sampling_intent = _mapping_or_none(intent.get("sampling_intent")) or {}
    temperature = sampling_intent.get("temperature")
    if temperature is None:
        return
    sampling_policy = dict(contract.sampling_policy or {})
    if sampling_policy.get("temperature_mode") in {
        "provider_default_only",
        "locked_provider_default",
    }:
        decisions.append(
            {
                "policy": "sampling",
                "decision": "temperature_intent_ignored",
                "reason": "provider_default_only",
            }
        )
        return
    try:
        normalized_temperature = float(temperature)
    except (TypeError, ValueError):
        return
    _set_temperature(
        payload,
        temperature=normalized_temperature,
        protocol_family=protocol_family,
    )
    applied.add("temperature")


def _apply_output_ceiling_intent(
    *,
    payload: dict[str, Any],
    intent: Mapping[str, Any],
    protocol_family: str,
    applied: set[str],
) -> None:
    output_ceiling = _optional_int(intent.get("output_ceiling_tokens"))
    if output_ceiling is None or output_ceiling <= 0:
        return
    _set_max_tokens(
        payload,
        max_tokens=output_ceiling,
        protocol_family=protocol_family,
    )
    applied.add("max_tokens")


def _apply_reasoning_intent(
    *,
    payload: dict[str, Any],
    intent: Mapping[str, Any],
    contract: LLMRequestContract,
    protocol_family: str,
    applied: set[str],
) -> None:
    reasoning_intent = _mapping_or_none(intent.get("reasoning_intent")) or {}
    effort = _optional_text(reasoning_intent.get("effort") or intent.get("reasoning_effort"))
    budget = _optional_int(
        reasoning_intent.get("budget_tokens")
        or intent.get("reasoning_budget_tokens")
    )
    if effort is None and budget is None:
        return

    policy = dict(contract.reasoning_policy or {})
    effort_payloads = _mapping_or_none(policy.get("effort_payloads")) or {}
    selected_payload = _mapping_or_none(effort_payloads.get(effort or "")) or {}
    if selected_payload:
        _deep_merge(payload, selected_payload)
        applied.update(_present_canonical_parameters(selected_payload, protocol_family=protocol_family))
        return

    if protocol_family == "openai_chat_completions" and effort:
        payload["reasoning_effort"] = effort
        applied.add("reasoning_effort")
    elif protocol_family == "anthropic_messages" and budget is not None:
        payload["thinking"] = {"type": "enabled", "budget_tokens": budget}
        applied.add("thinking")
    elif protocol_family == "google_generate_content" and budget is not None:
        generation_config = payload.setdefault("generationConfig", {})
        if isinstance(generation_config, dict):
            generation_config["thinkingConfig"] = {"thinkingBudget": budget}
            applied.add("thinking_budget")


def _apply_structured_output_intent(
    *,
    payload: dict[str, Any],
    intent: Mapping[str, Any],
    contract: LLMRequestContract,
    protocol_family: str,
    applied: set[str],
    decisions: list[Mapping[str, Any]],
) -> None:
    structured_intent = _mapping_or_none(intent.get("structured_output_intent")) or {}
    schema = _mapping_or_none(structured_intent.get("schema"))
    required = bool(structured_intent.get("required"))
    if schema is None and not required:
        return

    policy = dict(contract.structured_output_policy or {})
    strict_supported = bool(policy.get("strict_schema_supported"))
    json_mode_supported = bool(policy.get("json_mode_supported", strict_supported))
    fallback_allowed = bool(structured_intent.get("allow_json_mode_fallback", True))

    if protocol_family == "openai_chat_completions" and schema is not None and strict_supported:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": str(structured_intent.get("name") or "praxis_response"),
                "schema": schema,
                "strict": bool(structured_intent.get("strict", True)),
            },
        }
        applied.add("response_format")
        return
    if protocol_family == "openai_chat_completions" and json_mode_supported and fallback_allowed:
        payload["response_format"] = {"type": "json_object"}
        applied.add("response_format")
        decisions.append(
            {
                "policy": "structured_output",
                "decision": "json_mode_fallback",
                "strict_schema_supported": strict_supported,
            }
        )
        return
    if required:
        raise LLMRequestContractError(
            "llm_request_contract.structured_output_unsupported",
            "structured output was required but unsupported by this contract",
            details={"contract_ref": contract.contract_ref},
        )


def _enforce_parameter_sets(
    *,
    payload: dict[str, Any],
    protocol_family: str,
    contract: LLMRequestContract,
    omitted: set[str],
    decisions: list[Mapping[str, Any]],
) -> None:
    present = _present_canonical_parameters(payload, protocol_family=protocol_family)
    forbidden = present & contract.forbidden_parameters
    if forbidden:
        if contract.forbidden_parameter_policy in {"fail", "reject", "error"}:
            raise LLMRequestContractError(
                "llm_request_contract.forbidden_parameter",
                "request contains provider-forbidden parameters",
                details={
                    "contract_ref": contract.contract_ref,
                    "forbidden_parameters": sorted(forbidden),
                },
            )
        for param in forbidden:
            _remove_canonical_parameter(payload, param, protocol_family=protocol_family)
            omitted.add(param)
        decisions.append(
            {
                "policy": "parameters",
                "decision": "forbidden_parameters_omitted",
                "parameters": sorted(forbidden),
            }
        )

    supported = set(contract.supported_parameters)
    if not supported:
        return
    present = _present_canonical_parameters(payload, protocol_family=protocol_family)
    unsupported = {
        param
        for param in present
        if param not in supported and param not in _STRUCTURAL_KEYS
    }
    if not unsupported:
        return
    if contract.unsupported_parameter_policy in {"fail", "reject", "error"}:
        raise LLMRequestContractError(
            "llm_request_contract.unsupported_parameter",
            "request contains unsupported parameters",
            details={
                "contract_ref": contract.contract_ref,
                "unsupported_parameters": sorted(unsupported),
            },
        )
    for param in unsupported:
        _remove_canonical_parameter(payload, param, protocol_family=protocol_family)
        omitted.add(param)


def llm_request_intent_json_schema(
    contract: LLMRequestContract | Mapping[str, Any],
) -> dict[str, Any]:
    """Return the shaped JSON schema an LLM should see for request intent."""

    normalized_contract = (
        contract if isinstance(contract, LLMRequestContract) else LLMRequestContract.from_mapping(contract)
    )
    properties: dict[str, Any] = {
        "output_ceiling_tokens": {
            "type": "integer",
            "minimum": 1,
        },
    }
    if normalized_contract.max_output_tokens is not None:
        properties["output_ceiling_tokens"]["maximum"] = normalized_contract.max_output_tokens

    supported = set(normalized_contract.supported_parameters)
    forbidden = set(normalized_contract.forbidden_parameters)
    sampling_policy = dict(normalized_contract.sampling_policy or {})
    if sampling_policy.get("temperature_mode") not in {
        "provider_default_only",
        "locked_provider_default",
    } and (
        "temperature" in supported
        or sampling_policy.get("temperature_supported") is True
    ) and "temperature" not in forbidden:
        properties["sampling_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "temperature": {
                    "type": "number",
                    "minimum": float(sampling_policy.get("min_temperature", 0.0)),
                    "maximum": float(sampling_policy.get("max_temperature", 2.0)),
                }
            },
        }

    reasoning_policy = dict(normalized_contract.reasoning_policy or {})
    reasoning_supported = bool(reasoning_policy) or bool(
        supported
        & {
            "reasoning_effort",
            "thinking",
            "thinking_budget",
            "reasoning",
        }
    )
    if reasoning_supported and reasoning_policy.get("mode") != "disabled":
        reasoning_properties: dict[str, Any] = {
            "effort": {
                "type": "string",
                "enum": list(reasoning_policy.get("effort_slugs") or [
                    "instant",
                    "low",
                    "medium",
                    "high",
                    "max",
                ]),
            },
            "budget_tokens": {"type": "integer", "minimum": 0},
        }
        max_budget = reasoning_policy.get("max_budget_tokens")
        if max_budget is not None:
            reasoning_properties["budget_tokens"]["maximum"] = int(max_budget)
        elif normalized_contract.max_output_tokens is not None:
            reasoning_properties["budget_tokens"]["maximum"] = normalized_contract.max_output_tokens
        properties["reasoning_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": reasoning_properties,
        }

    cache_policy = dict(normalized_contract.cache_policy or {})
    if cache_policy.get("supported") or cache_policy.get("modes"):
        cache_properties: dict[str, Any] = {
            "mode": {
                "type": "string",
                "enum": list(cache_policy.get("modes") or ["auto", "disabled"]),
            }
        }
        properties["cache_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": cache_properties,
        }

    structured_policy = dict(normalized_contract.structured_output_policy or {})
    if structured_policy.get("strict_schema_supported") or structured_policy.get("json_mode_supported"):
        properties["structured_output_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "required": {"type": "boolean"},
                "name": {"type": "string"},
                "strict": {"type": "boolean"},
                "allow_json_mode_fallback": {"type": "boolean"},
                "schema": {"type": "object"},
            },
        }

    tool_call_policy = dict(normalized_contract.tool_call_policy or {})
    if tool_call_policy and tool_call_policy.get("supported", True) is not False:
        max_tool_calls = _optional_int(tool_call_policy.get("max_tool_calls"))
        tool_properties: dict[str, Any] = {
            "max_tool_calls": {"type": "integer", "minimum": 0},
        }
        if max_tool_calls is not None:
            tool_properties["max_tool_calls"]["maximum"] = max_tool_calls
        properties["tool_call_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": tool_properties,
        }

    state_carry_policy = dict(normalized_contract.state_carry_policy or {})
    if state_carry_policy and state_carry_policy.get("mode") not in {"disabled", "none"}:
        properties["state_carry_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "carry_provider_state": {
                    "type": "boolean",
                    "default": bool(state_carry_policy.get("required", False)),
                }
            },
        }

    streaming_policy = dict(normalized_contract.streaming_policy or {})
    if streaming_policy.get("supported"):
        properties["streaming_intent"] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "stream": {"type": "boolean"},
                "background": {"type": "boolean"},
            },
        }

    return {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }


def _handle_shape_violation(
    *,
    contract: LLMRequestContract,
    reason_code: str,
    message: str,
    details: Mapping[str, Any],
) -> None:
    if contract.combination_violation_policy in {"fail", "reject", "error"}:
        raise LLMRequestContractError(
            reason_code,
            message,
            details={"contract_ref": contract.contract_ref, **dict(details)},
        )


def _set_reasoning_budget(
    payload: dict[str, Any],
    *,
    budget_tokens: int,
    protocol_family: str,
) -> None:
    thinking = payload.get("thinking")
    if isinstance(thinking, dict):
        thinking["budget_tokens"] = budget_tokens
        if budget_tokens > 0:
            thinking["type"] = "enabled"
        return
    if protocol_family == "google_generate_content":
        generation_config = payload.setdefault("generationConfig", {})
        if isinstance(generation_config, dict):
            thinking_config = generation_config.setdefault("thinkingConfig", {})
            if isinstance(thinking_config, dict):
                thinking_config["thinkingBudget"] = budget_tokens


def _set_max_tokens(
    payload: dict[str, Any],
    *,
    max_tokens: int,
    protocol_family: str,
) -> None:
    if protocol_family == "google_generate_content":
        generation_config = payload.setdefault("generationConfig", {})
        if isinstance(generation_config, dict):
            generation_config["maxOutputTokens"] = max_tokens
            return
    payload["max_tokens"] = max_tokens


def _set_temperature(
    payload: dict[str, Any],
    *,
    temperature: float,
    protocol_family: str,
) -> None:
    if protocol_family == "google_generate_content":
        generation_config = payload.setdefault("generationConfig", {})
        if isinstance(generation_config, dict):
            generation_config["temperature"] = temperature
            return
    payload["temperature"] = temperature


def _present_canonical_parameters(
    payload: Mapping[str, Any],
    *,
    protocol_family: str,
) -> set[str]:
    present = {str(key) for key in payload.keys()}
    if "max_tokens" in payload:
        present.add("max_tokens")
    if "temperature" in payload:
        present.add("temperature")
    generation_config = payload.get("generationConfig")
    if isinstance(generation_config, Mapping):
        if "temperature" in generation_config:
            present.add("temperature")
        if "maxOutputTokens" in generation_config:
            present.add("max_tokens")
        if "thinkingConfig" in generation_config:
            present.add("thinking_budget")
    if "thinking" in payload:
        present.add("thinking")
    if "reasoning_effort" in payload:
        present.add("reasoning_effort")
    if "response_format" in payload:
        present.add("response_format")
    return present


def _remove_canonical_parameter(
    payload: dict[str, Any],
    parameter: str,
    *,
    protocol_family: str,
) -> None:
    payload.pop(parameter, None)
    if parameter == "temperature":
        payload.pop("temperature", None)
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, dict):
            generation_config.pop("temperature", None)
    elif parameter == "max_tokens":
        payload.pop("max_tokens", None)
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, dict):
            generation_config.pop("maxOutputTokens", None)
    elif parameter == "thinking_budget":
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, dict):
            generation_config.pop("thinkingConfig", None)


def _extract_temperature(
    payload: Mapping[str, Any],
    *,
    protocol_family: str,
) -> float | None:
    value = payload.get("temperature")
    if value is None and protocol_family == "google_generate_content":
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, Mapping):
            value = generation_config.get("temperature")
    return None if value is None else float(value)


def _extract_max_tokens(
    payload: Mapping[str, Any],
    *,
    protocol_family: str,
) -> int | None:
    value = payload.get("max_tokens")
    if value is None and protocol_family == "google_generate_content":
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, Mapping):
            value = generation_config.get("maxOutputTokens")
    return _optional_int(value)


def _extract_reasoning_budget(
    payload: Mapping[str, Any],
    *,
    intent: Mapping[str, Any],
    protocol_family: str,
) -> int | None:
    reasoning_intent = _mapping_or_none(intent.get("reasoning_intent")) or {}
    budget = _optional_int(
        reasoning_intent.get("budget_tokens")
        or intent.get("reasoning_budget_tokens")
    )
    if budget is not None:
        return budget
    thinking = payload.get("thinking")
    if isinstance(thinking, Mapping):
        return _optional_int(thinking.get("budget_tokens"))
    if protocol_family == "google_generate_content":
        generation_config = payload.get("generationConfig")
        if isinstance(generation_config, Mapping):
            thinking_config = generation_config.get("thinkingConfig")
            if isinstance(thinking_config, Mapping):
                return _optional_int(thinking_config.get("thinkingBudget"))
    return None


def _anthropic_thinking_enabled(
    payload: Mapping[str, Any],
    *,
    reasoning_budget: int | None,
) -> bool:
    thinking = payload.get("thinking")
    if isinstance(thinking, Mapping):
        thinking_type = str(thinking.get("type") or "").strip().lower()
        return thinking_type == "enabled" or bool(_optional_int(thinking.get("budget_tokens")))
    return bool(reasoning_budget and reasoning_budget > 0)


def _deep_merge(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for key, value in source.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            _deep_merge(existing, value)
        else:
            target[str(key)] = deepcopy(value)


def _mapping_or_none(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _string_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value.strip()} if value.strip() else set()
    if isinstance(value, Mapping):
        return {str(key).strip() for key, enabled in value.items() if enabled and str(key).strip()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return {str(item).strip() for item in value if str(item).strip()}
    return set()


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LLMRequestContractError(
            "llm_request_contract.invalid_contract",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "MaterializedLLMPayload",
    "LLMRequestContract",
    "LLMRequestContractError",
    "compile_llm_request_body",
    "llm_request_intent_json_schema",
]
