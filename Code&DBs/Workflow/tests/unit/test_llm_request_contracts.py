from __future__ import annotations

import pytest

from runtime.llm_request_contracts import (
    LLMRequestContract,
    LLMRequestContractError,
    compile_llm_request_body,
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


def test_anthropic_thinking_plus_temperature_fails_closed() -> None:
    contract = _contract(
        provider_slug="anthropic",
        model_slug="claude-sonnet-4-6",
        supported_parameters=["model", "messages", "max_tokens", "thinking"],
        reasoning_policy={"forbid_temperature_when_thinking": True},
    )

    with pytest.raises(LLMRequestContractError) as exc_info:
        compile_llm_request_body(
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

    assert exc_info.value.reason_code == "llm_request_contract.forbidden_combination"


def test_gemini_low_temperature_override_fails_when_sampling_policy_locks_default() -> None:
    contract = _contract(
        provider_slug="google",
        model_slug="gemini-3-pro",
        supported_parameters=["contents", "temperature", "max_tokens", "thinking_budget"],
        sampling_policy={"temperature_mode": "provider_default_only"},
    )

    with pytest.raises(LLMRequestContractError) as exc_info:
        compile_llm_request_body(
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

    assert exc_info.value.reason_code == "llm_request_contract.forbidden_combination"


def test_reasoning_budget_cannot_exceed_output_budget() -> None:
    with pytest.raises(LLMRequestContractError) as exc_info:
        compile_llm_request_body(
            contract=_contract(max_output_tokens=8192),
            request_intent={"reasoning_budget_tokens": 4097},
            base_payload={
                "model": "gpt-5.4",
                "messages": [],
                "max_tokens": 4096,
            },
            provider_slug="openai",
            model_slug="gpt-5.4",
            protocol_family="openai_chat_completions",
        )

    assert (
        exc_info.value.reason_code
        == "llm_request_contract.reasoning_budget_exceeds_output_budget"
    )


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
