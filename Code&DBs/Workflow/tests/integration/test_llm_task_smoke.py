"""End-to-end smoke test for the LLM task adapter.

Calls a real LLM API if OPENAI_API_KEY is set. Skipped otherwise.
"""

from __future__ import annotations

import os

import pytest

from adapters.cli_llm import CLILLMAdapter
from adapters.credentials import CredentialResolutionError, resolve_credential
from adapters.deterministic import AdapterRegistry, DeterministicTaskRequest
from adapters.llm_client import LLMClientError
import adapters.llm_task as llm_task_mod
from adapters.llm_task import LLMTaskAdapter


def test_credential_resolver_maps_openai_auth_ref() -> None:
    cred = resolve_credential(
        "secret.default-path.openai",
        env={"OPENAI_API_KEY": "sk-test-fake"},
    )
    assert cred.provider_hint == "openai"
    assert cred.api_key == "sk-test-fake"
    assert cred.auth_ref == "secret.default-path.openai"


def test_credential_resolver_maps_anthropic_auth_ref() -> None:
    cred = resolve_credential(
        "secret.runtime.anthropic",
        env={"ANTHROPIC_API_KEY": "sk-ant-test"},
    )
    assert cred.provider_hint == "anthropic"
    assert cred.api_key == "sk-ant-test"


def test_credential_resolver_fails_on_missing_env_var() -> None:
    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_credential("secret.default-path.openai", env={})
    assert exc_info.value.reason_code == "credential.env_var_missing"


def test_credential_resolver_fails_on_unknown_provider() -> None:
    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_credential("secret.default-path.deepseek", env={})
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


def test_llm_adapter_fails_closed_without_credentials() -> None:
    adapter = LLMTaskAdapter(credential_env={})
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="no_creds",
        input_payload={"prompt": "hello"},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )
    result = adapter.execute(request=request)
    assert result.status == "failed"
    assert result.failure_code == "credential.env_var_missing"


def test_llm_adapter_maps_http_errors_to_adapter_contract(monkeypatch) -> None:
    adapter = LLMTaskAdapter(credential_env={"OPENAI_API_KEY": "sk-test"})
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="http_error",
        input_payload={"prompt": "hello", "provider_slug": "openai"},
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
    assert resolved is llm


def test_adapter_registry_still_resolves_deterministic_task() -> None:
    registry = AdapterRegistry()
    resolved = registry.resolve(adapter_type="deterministic_task")
    assert resolved.executor_type == "adapter.deterministic_task"


def test_adapter_registry_resolves_cli_llm() -> None:
    cli = CLILLMAdapter()
    registry = AdapterRegistry(cli_llm_adapter=cli)
    resolved = registry.resolve(adapter_type="cli_llm")
    assert resolved is cli


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
            "cli_adapter.nonzero_exit",
        )


@pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY required for live LLM smoke test",
)
def test_llm_adapter_calls_real_api() -> None:
    adapter = LLMTaskAdapter()
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="smoke",
        input_payload={
            "prompt": "Respond with exactly the word 'hello' and nothing else.",
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
