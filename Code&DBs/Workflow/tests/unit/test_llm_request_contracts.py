from __future__ import annotations

from runtime.llm_request_contracts import (
    LLMRequestContract,
    compile_llm_request_body,
    llm_request_intent_json_schema,
)


def _contract(**overrides):
    payload = {
        "contract_ref": "contract.test",
        "provider_slug": "openai",
        "model_slug": "gpt-5.4",
        "supported_parameters": [
            "model",
            "messages",
            "temperature",
            "max_tokens",
            "reasoning_effort",
            "response_format",
        ],
        "unsupported_parameter_policy": "fail",
    }
    payload.update(overrides)
    return LLMRequestContract.from_mapping(payload)


def test_anthropic_thinking_plus_temperature_is_shaped_out() -> None:
    contract = _contract(
        provider_slug="anthropic",
        model_slug="claude-sonnet-4-6",
        supported_parameters=["model", "messages", "max_tokens", "thinking"],
        reasoning_policy={"forbid_temperature_when_thinking": True},
    )

    compiled = compile_llm_request_body(
        contract=contract,
        request_intent={},
        base_payload={
            "model": "claude-sonnet-4-6",
            "messages": [],
            "max_tokens": 4096,
            "temperature": 0.2,
            "thinking": {"type": "enabled", "budget_tokens": 1024},
        },
        provider_slug="anthropic",
        model_slug="claude-sonnet-4-6",
        protocol_family="anthropic_messages",
    )

    assert "temperature" not in compiled.payload
    assert compiled.omitted_parameters == ("temperature",)
    assert compiled.policy_decisions[-1]["decision"] == "temperature_omitted"


def test_gemini_temperature_override_is_not_exposed_or_sent_when_policy_locks_default() -> None:
    contract = _contract(
        provider_slug="google",
        model_slug="gemini-3-pro",
        supported_parameters=["contents", "temperature", "max_tokens", "thinking_budget"],
        sampling_policy={"temperature_mode": "provider_default_only"},
    )

    schema = llm_request_intent_json_schema(contract)
    compiled = compile_llm_request_body(
        contract=contract,
        request_intent={},
        base_payload={
            "contents": [],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 2048},
        },
        provider_slug="google",
        model_slug="gemini-3-pro",
        protocol_family="google_generate_content",
    )

    assert "sampling_intent" not in schema["properties"]
    assert "temperature" not in compiled.payload["generationConfig"]
    assert compiled.omitted_parameters == ("temperature",)


def test_reasoning_budget_is_clamped_to_output_budget_for_provider_payloads() -> None:
    compiled = compile_llm_request_body(
        contract=_contract(
            provider_slug="anthropic",
            model_slug="claude-sonnet-4-6",
            supported_parameters=["model", "messages", "max_tokens", "thinking"],
            max_output_tokens=8192,
        ),
        request_intent={"reasoning_budget_tokens": 4097},
        base_payload={
            "model": "claude-sonnet-4-6",
            "messages": [],
            "max_tokens": 4096,
        },
        provider_slug="anthropic",
        model_slug="claude-sonnet-4-6",
        protocol_family="anthropic_messages",
    )

    assert compiled.payload["thinking"]["budget_tokens"] == 4096
    assert compiled.policy_decisions[-1]["decision"] == "reasoning_budget_clamped"


def test_max_tokens_is_clamped_to_contract_output_ceiling() -> None:
    compiled = compile_llm_request_body(
        contract=_contract(max_output_tokens=4096),
        request_intent={},
        base_payload={
            "model": "gpt-5.4",
            "messages": [],
            "max_tokens": 8192,
        },
        provider_slug="openai",
        model_slug="gpt-5.4",
        protocol_family="openai_chat_completions",
    )

    assert compiled.payload["max_tokens"] == 4096
    assert compiled.policy_decisions[-1]["decision"] == "max_tokens_clamped"


def test_unsupported_parameters_are_omitted_when_policy_allows() -> None:
    compiled = compile_llm_request_body(
        contract=_contract(
            supported_parameters=["model", "messages", "max_tokens"],
            unsupported_parameter_policy="omit",
        ),
        request_intent={},
        base_payload={
            "model": "gpt-5.4",
            "messages": [],
            "max_tokens": 4096,
            "temperature": 0.7,
        },
        provider_slug="openai",
        model_slug="gpt-5.4",
        protocol_family="openai_chat_completions",
    )

    assert "temperature" not in compiled.payload
    assert compiled.omitted_parameters == ("temperature",)


def test_sampling_and_output_intents_compile_to_provider_payload() -> None:
    compiled = compile_llm_request_body(
        contract=_contract(max_output_tokens=4096),
        request_intent={
            "sampling_intent": {"temperature": 0.4},
            "output_ceiling_tokens": 2048,
        },
        base_payload={
            "model": "gpt-5.4",
            "messages": [],
        },
        provider_slug="openai",
        model_slug="gpt-5.4",
        protocol_family="openai_chat_completions",
    )

    assert compiled.payload["temperature"] == 0.4
    assert compiled.payload["max_tokens"] == 2048
    assert "temperature" in compiled.applied_parameters
    assert "max_tokens" in compiled.applied_parameters


def test_forbidden_parameters_are_shaped_before_unsupported_policy_runs() -> None:
    compiled = compile_llm_request_body(
        contract=_contract(
            supported_parameters=["model", "messages", "max_tokens"],
            forbidden_parameters=["temperature"],
            unsupported_parameter_policy="fail",
            forbidden_parameter_policy="omit",
        ),
        request_intent={},
        base_payload={
            "model": "gpt-5.4",
            "messages": [],
            "max_tokens": 4096,
            "temperature": 0.7,
        },
        provider_slug="openai",
        model_slug="gpt-5.4",
        protocol_family="openai_chat_completions",
    )

    assert "temperature" not in compiled.payload
    assert compiled.policy_decisions[-1]["decision"] == "forbidden_parameters_omitted"


def test_intent_json_schema_exposes_only_contract_capabilities() -> None:
    schema = llm_request_intent_json_schema(
        _contract(
            max_output_tokens=4096,
            sampling_policy={"temperature_mode": "provider_default_only"},
            reasoning_policy={"effort_slugs": ["low", "medium"]},
            cache_policy={"supported": True, "modes": ["auto", "disabled"]},
            state_carry_policy={"required": True},
            streaming_policy={"supported": True},
            tool_call_policy={"max_tool_calls": 8},
        )
    )

    assert schema["additionalProperties"] is False
    assert "sampling_intent" not in schema["properties"]
    assert schema["properties"]["output_ceiling_tokens"]["maximum"] == 4096
    assert schema["properties"]["reasoning_intent"]["properties"]["budget_tokens"]["maximum"] == 4096
    assert schema["properties"]["cache_intent"]["properties"]["mode"]["enum"] == [
        "auto",
        "disabled",
    ]
    assert schema["properties"]["state_carry_intent"]["properties"]["carry_provider_state"][
        "default"
    ] is True
    assert schema["properties"]["streaming_intent"]["additionalProperties"] is False
    assert schema["properties"]["tool_call_intent"]["properties"]["max_tool_calls"][
        "maximum"
    ] == 8


def test_intent_json_schema_does_not_expose_unknown_knobs() -> None:
    schema = llm_request_intent_json_schema(
        _contract(
            supported_parameters=["model", "messages", "max_tokens"],
            sampling_policy={},
            reasoning_policy={},
            cache_policy={},
            state_carry_policy={},
            streaming_policy={},
            tool_call_policy={},
        )
    )

    assert "sampling_intent" not in schema["properties"]
    assert "reasoning_intent" not in schema["properties"]
    assert "tool_call_intent" not in schema["properties"]
    assert "state_carry_intent" not in schema["properties"]
