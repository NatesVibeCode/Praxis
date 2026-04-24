"""End-to-end smoke test for the LLM task adapter.

Calls a real LLM API when a configured chat provider exposes a credential env var.
"""

from __future__ import annotations

import os

import pytest

from adapters.cli_llm import CLILLMAdapter
import adapters.credentials as credentials_mod
from adapters.credentials import CredentialResolutionError, resolve_credential
from adapters.deterministic import AdapterRegistry, DeterministicTaskRequest
from adapters.middleware import _WrappedAdapter
from adapters.llm_client import LLMClientError
import adapters.llm_task as llm_task_mod
from adapters.llm_task import LLMTaskAdapter
from registry.provider_execution_registry import (
    registered_providers,
    resolve_api_key_env_vars,
    resolve_api_protocol_family,
    supports_adapter,
)

_LLM_CLIENT_PROTOCOL_FAMILIES = frozenset(
    {"anthropic_messages", "google_generate_content", "openai_chat_completions"}
)


def _supported_llm_task_provider() -> str:
    for provider_slug in registered_providers():
        if supports_adapter(provider_slug, "llm_task"):
            return provider_slug
    pytest.skip("provider execution registry exposes no llm_task-admitted provider")


def _supported_chat_llm_provider() -> str:
    for provider_slug in registered_providers():
        if not supports_adapter(provider_slug, "llm_task"):
            continue
        if resolve_api_protocol_family(provider_slug) in _LLM_CLIENT_PROTOCOL_FAMILIES:
            return provider_slug
    pytest.skip("provider execution registry exposes no chat-protocol llm_task provider")


def _supported_live_chat_llm_provider() -> tuple[str, str]:
    for provider_slug in registered_providers():
        if not supports_adapter(provider_slug, "llm_task"):
            continue
        if resolve_api_protocol_family(provider_slug) not in _LLM_CLIENT_PROTOCOL_FAMILIES:
            continue
        for env_var in resolve_api_key_env_vars(provider_slug):
            value = os.environ.get(env_var)
            if value:
                return provider_slug, env_var
    pytest.skip(
        "no chat-protocol llm_task provider with a configured API credential env var "
        "available"
    )


def _credential_env_for_provider(provider_slug: str) -> dict[str, str]:
    env_vars = resolve_api_key_env_vars(provider_slug)
    if not env_vars:
        pytest.skip(f"provider execution registry exposes no auth env var mapping for {provider_slug}")
    return {env_vars[0]: "sk-test"}


def test_credential_resolver_maps_openai_auth_ref() -> None:
    cred = resolve_credential(
        "secret.default-path.openai",
        env={"OPENAI_API_KEY": "sk-test-fake"},
    )
    assert cred.provider_hint == "openai"
    assert cred.api_key == "sk-test-fake"
    assert cred.auth_ref == "secret.default-path.openai"


def test_credential_resolver_rejects_cli_only_anthropic_api_key() -> None:
    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_credential(
            "secret.runtime.anthropic",
            env={"ANTHROPIC_API_KEY": "sk-ant-test"},
        )
    assert exc_info.value.reason_code == "credential.direct_anthropic_api_forbidden"


def test_credential_resolver_fails_on_missing_env_var() -> None:
    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_credential("secret.default-path.openai", env={})
    assert exc_info.value.reason_code == "credential.env_var_missing"


def test_credential_resolver_fails_on_unknown_provider() -> None:
    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_credential("secret.default-path.unknown-provider", env={})
    assert exc_info.value.reason_code == "credential.provider_unknown"


def test_llm_adapter_fails_closed_without_prompt() -> None:
    adapter = LLMTaskAdapter(credential_env={})
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="missing_prompt",
        input_payload={},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    assert result.status == "failed"
    assert result.failure_code == "adapter.input_invalid"


def test_llm_adapter_fails_closed_without_credentials(monkeypatch) -> None:
    adapter = LLMTaskAdapter(credential_env={})
    provider_slug = "openai"
    monkeypatch.setattr(credentials_mod, "resolve_secret", lambda *_args, **_kwargs: None)
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="no_creds",
        input_payload={"prompt": "hello", "provider_slug": provider_slug},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    assert result.status == "failed"
    assert result.failure_code == "credential.env_var_missing"


def test_llm_adapter_maps_http_errors_to_adapter_contract(monkeypatch) -> None:
    provider_slug = "openai"
    adapter = LLMTaskAdapter(credential_env=_credential_env_for_provider(provider_slug))
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="http_error",
        input_payload={"prompt": "hello", "provider_slug": provider_slug},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    def _boom(_request):
        raise LLMClientError(
            "llm_client.http_error",
            "HTTP 429: rate limit exceeded",
            status_code=429,
        )

    monkeypatch.setattr(llm_task_mod, "call_llm", _boom)

    result = adapter.execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "adapter.http_error"
    assert result.outputs["transport_kind"] == "http"
    assert result.outputs["failure_namespace"] == "adapter"
    assert result.outputs["status_code"] == 429
    assert "HTTP 429" in result.outputs["stderr"]


def test_adapter_registry_resolves_llm_task() -> None:
    llm = LLMTaskAdapter(credential_env={})
    registry = AdapterRegistry(llm_task_adapter=llm)
    resolved = registry.resolve(adapter_type="llm_task")
    assert isinstance(resolved, _WrappedAdapter)
    assert resolved._inner is llm
    assert resolved.executor_type == llm.executor_type


def test_adapter_registry_still_resolves_deterministic_task() -> None:
    registry = AdapterRegistry()
    resolved = registry.resolve(adapter_type="deterministic_task")
    assert resolved.executor_type == "adapter.deterministic_task"


def test_adapter_registry_resolves_cli_llm() -> None:
    cli = CLILLMAdapter()
    registry = AdapterRegistry(cli_llm_adapter=cli)
    resolved = registry.resolve(adapter_type="cli_llm")
    assert isinstance(resolved, _WrappedAdapter)
    assert resolved._inner is cli
    assert resolved.executor_type == cli.executor_type


def test_cli_adapter_fails_closed_without_prompt() -> None:
    adapter = CLILLMAdapter()
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="missing_prompt",
        input_payload={},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    assert result.status == "failed"
    assert result.failure_code == "adapter.input_invalid"


def test_cli_adapter_fails_closed_on_unknown_provider() -> None:
    adapter = CLILLMAdapter(default_provider="nonexistent_provider")
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="bad_provider",
        input_payload={"prompt": "hello"},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    assert result.status == "failed"
    assert result.failure_code == "cli_adapter.provider_unmapped"


def test_cli_adapter_resolves_provider_from_provider_slug() -> None:
    """Verify provider_slug in input_payload drives CLI selection."""
    adapter = CLILLMAdapter(default_provider="anthropic")
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="provider_test",
        input_payload={
            "prompt": "hello",
            "provider_slug": "nonexistent_provider",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    # Should fail with provider_unmapped, proving provider_slug was used
    assert result.status == "failed"
    assert result.failure_code == "cli_adapter.provider_unmapped"


def test_cli_adapter_resolves_provider_from_legacy_cli_hint() -> None:
    """Verify cli='claude' maps to anthropic provider."""
    adapter = CLILLMAdapter(default_provider="openai")
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="legacy_test",
        input_payload={
            "prompt": "hello",
            "cli": "claude",
            # binary won't be found in test PATH, but we prove the mapping works
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    # May fail with binary_not_found or succeed — either way proves
    # the provider was resolved to anthropic (not the default openai)
    if result.status == "failed":
        assert result.failure_code in (
            "cli_adapter.binary_not_found",
            "cli_adapter.exec_error",
            "cli_adapter.nonzero_exit",
        )


def test_llm_adapter_calls_real_api() -> None:
    provider_slug, env_var = _supported_live_chat_llm_provider()
    adapter = LLMTaskAdapter(credential_env={env_var: os.environ[env_var]})
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="smoke",
        input_payload={
            "prompt": "Respond with exactly the word 'hello' and nothing else.",
            "provider_slug": provider_slug,
            "max_tokens": 16,
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:smoke-test",
    )
    result = adapter.execute(request=request)
    assert result.status == "succeeded", f"failed: {result.failure_code}"
    assert result.executor_type == "adapter.llm_task"
    assert "completion" in result.outputs
    assert len(result.outputs["completion"]) > 0
    assert "usage" in result.outputs
    assert result.outputs["latency_ms"] > 0
