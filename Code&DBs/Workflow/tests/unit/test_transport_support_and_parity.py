from __future__ import annotations

import json
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

import runtime.compile_index as compile_index
from adapters import provider_transport
from adapters.cli_llm import CLILLMAdapter, CLILLMResult
from adapters.credentials import resolve_credential
from adapters.deterministic import DeterministicTaskRequest
from adapters.http_transport import HTTPResponse
from adapters.llm_client import LLMClientError, LLMRequest, LLMResponse, call_llm, call_llm_streaming
from adapters.llm_task import LLMTaskAdapter
from adapters.provider_types import ProviderAdapterContract
from registry.provider_execution_registry import resolve_api_endpoint
from runtime.http_transport import TransportExecutionError
from runtime.task_type_router import TaskTypeRouter
from runtime.workflow.execution_policy import resolve_cli_execution_policy


def _provider_authority_row(
    *,
    provider_slug: str,
    binary_name: str,
    default_model: str | None,
    api_endpoint: str | None,
    api_protocol_family: str | None,
    api_key_env_vars: list[str],
    base_flags: list[str],
    model_flag: str | None,
    system_prompt_flag: str | None,
    json_schema_flag: str | None,
    output_format: str,
    output_envelope_key: str,
    forbidden_flags: list[str],
    default_timeout: int,
    lane_policies: dict[str, dict[str, object]],
    adapter_economics: dict[str, dict[str, object]],
    prompt_mode: str = "stdin",
    aliases: list[str] | None = None,
    mcp_config_style: str | None = None,
    mcp_args_template: list[str] | None = None,
    sandbox_env_overrides: dict[str, object] | None = None,
    exclude_from_rotation: bool = False,
) -> dict[str, object]:
    return {
        "provider_slug": provider_slug,
        "binary_name": binary_name,
        "default_model": default_model,
        "api_endpoint": api_endpoint,
        "api_protocol_family": api_protocol_family,
        "api_key_env_vars": api_key_env_vars,
        "prompt_mode": prompt_mode,
        "base_flags": base_flags,
        "model_flag": model_flag,
        "system_prompt_flag": system_prompt_flag,
        "json_schema_flag": json_schema_flag,
        "output_format": output_format,
        "output_envelope_key": output_envelope_key,
        "forbidden_flags": forbidden_flags,
        "default_timeout": default_timeout,
        "aliases": aliases or [],
        "mcp_config_style": mcp_config_style,
        "mcp_args_template": mcp_args_template,
        "sandbox_env_overrides": sandbox_env_overrides or {},
        "exclude_from_rotation": exclude_from_rotation,
        "lane_policies": lane_policies,
        "adapter_economics": adapter_economics,
    }


def _cli_lane_policy(reason: str = "Admitted local CLI lane.") -> dict[str, object]:
    return {
        "admitted_by_policy": True,
        "execution_topology": "local_cli",
        "transport_kind": "cli",
        "policy_reason": reason,
    }


def _http_lane_policy(
    *,
    execution_topology: str = "direct_http",
    reason: str = "Admitted direct HTTP lane.",
) -> dict[str, object]:
    return {
        "admitted_by_policy": True,
        "execution_topology": execution_topology,
        "transport_kind": "http",
        "policy_reason": reason,
    }


def _prepaid_economics(provider_slug: str, *, allow_payg_fallback: bool) -> dict[str, object]:
    return {
        "billing_mode": "subscription_included",
        "budget_bucket": f"{provider_slug}_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": True,
        "allow_payg_fallback": allow_payg_fallback,
    }


def _metered_economics(provider_slug: str) -> dict[str, object]:
    return {
        "billing_mode": "metered_api",
        "budget_bucket": f"{provider_slug}_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": False,
        "allow_payg_fallback": True,
    }


def _provider_authority_rows() -> tuple[dict[str, object], ...]:
    return (
        _provider_authority_row(
            provider_slug="anthropic",
            binary_name="claude",
            default_model="claude-sonnet-4-6",
            api_endpoint=None,
            api_protocol_family=None,
            api_key_env_vars=[],
            base_flags=["-p", "--output-format", "json"],
            model_flag="--model",
            system_prompt_flag="--system-prompt",
            json_schema_flag="--json-schema",
            output_format="json",
            output_envelope_key="result",
            forbidden_flags=[
                "--dangerously-skip-permissions",
                "--allow-dangerously-skip-permissions",
                "--add-dir",
            ],
            default_timeout=300,
            lane_policies={"cli_llm": _cli_lane_policy()},
            adapter_economics={
                "cli_llm": _prepaid_economics("anthropic", allow_payg_fallback=False)
            },
        ),
        _openai_provider_authority_row(),
        _provider_authority_row(
            provider_slug="cursor",
            binary_name="cursor-api",
            default_model="auto",
            api_endpoint="https://api.cursor.com/v0/agents",
            api_protocol_family="cursor_background_agent",
            api_key_env_vars=["CURSOR_API_KEY"],
            base_flags=[],
            model_flag=None,
            system_prompt_flag=None,
            json_schema_flag=None,
            output_format="text",
            output_envelope_key="text",
            forbidden_flags=[],
            default_timeout=900,
            lane_policies={
                "llm_task": _http_lane_policy(
                    execution_topology="repo_agent_http",
                    reason="Admitted Cursor background-agent API lane.",
                )
            },
            adapter_economics={
                "llm_task": _prepaid_economics("cursor", allow_payg_fallback=False)
            },
        ),
        _provider_authority_row(
            provider_slug="cursor_local",
            binary_name="cursor-agent",
            default_model="composer-2",
            api_endpoint=None,
            api_protocol_family=None,
            api_key_env_vars=["CURSOR_API_KEY"],
            base_flags=["-p", "--output-format", "json", "--mode", "ask", "-f", "--sandbox", "disabled"],
            model_flag="--model",
            system_prompt_flag=None,
            json_schema_flag=None,
            output_format="json",
            output_envelope_key="result",
            forbidden_flags=["--cloud", "--workspace", "-w", "--worktree"],
            default_timeout=900,
            aliases=["cursor-cli"],
            lane_policies={
                "cli_llm": _cli_lane_policy("Admitted local Cursor Agent CLI lane.")
            },
            adapter_economics={
                "cli_llm": _prepaid_economics("cursor", allow_payg_fallback=False)
            },
        ),
        _provider_authority_row(
            provider_slug="google",
            binary_name="gemini",
            default_model="gemini-2.5-flash",
            api_endpoint="https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            api_protocol_family="google_generate_content",
            api_key_env_vars=["GEMINI_API_KEY", "GOOGLE_API_KEY"],
            base_flags=["-p", ".", "-o", "json"],
            model_flag="--model",
            system_prompt_flag=None,
            json_schema_flag=None,
            output_format="json",
            output_envelope_key="response",
            forbidden_flags=["--approval-mode", "--yolo", "-y"],
            default_timeout=600,
            lane_policies={"cli_llm": _cli_lane_policy(), "llm_task": _http_lane_policy()},
            adapter_economics={
                "cli_llm": _prepaid_economics("google", allow_payg_fallback=True),
                "llm_task": _metered_economics("google"),
            },
            mcp_config_style="gemini_project_settings",
            mcp_args_template=["--allowed-mcp-server-names", "dag-workflow"],
            aliases=["gemini-cli"],
        ),
    )


def _authority_profiles_map():
    import registry.provider_execution_registry as provider_registry_authority

    return {
        profile.provider_slug: profile
        for profile in (
            provider_registry_authority._parse_profile_row(row)
            for row in _provider_authority_rows()
        )
    }


def _test_profiles_with_declared_auth():
    profiles = _authority_profiles_map()
    auth_envs = {
        "cursor": ("CURSOR_API_KEY",),
        "cursor_local": ("CURSOR_API_KEY",),
        "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
        "openai": ("OPENAI_API_KEY",),
    }
    return {
        slug: replace(profile, api_key_env_vars=auth_envs.get(slug, profile.api_key_env_vars))
        for slug, profile in profiles.items()
    }


def _openai_provider_authority_row() -> dict[str, object]:
    return {
        "provider_slug": "openai",
        "binary_name": "codex",
        "default_model": "gpt-4.1",
        "api_endpoint": "https://api.openai.com/v1/chat/completions",
        "api_protocol_family": "openai_chat_completions",
        "api_key_env_vars": ["OPENAI_API_KEY"],
        "prompt_mode": "stdin",
        "base_flags": ["exec", "-", "--json"],
        "model_flag": "--model",
        "system_prompt_flag": None,
        "json_schema_flag": None,
        "output_format": "ndjson",
        "output_envelope_key": "text",
        "forbidden_flags": ["--full-auto"],
        "default_timeout": 300,
        "aliases": [],
        "mcp_config_style": None,
        "mcp_args_template": None,
        "sandbox_env_overrides": {},
        "exclude_from_rotation": False,
        "lane_policies": {
            "cli_llm": {
                "admitted_by_policy": True,
                "execution_topology": "local_cli",
                "transport_kind": "cli",
                "policy_reason": "Admitted local CLI lane.",
            },
            "llm_task": {
                "admitted_by_policy": True,
                "execution_topology": "direct_http",
                "transport_kind": "http",
                "policy_reason": "Admitted direct HTTP lane.",
            },
        },
        "adapter_economics": {
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "openai_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": True,
            },
            "llm_task": {
                "billing_mode": "metered_api",
                "budget_bucket": "openai_api_payg",
                "effective_marginal_cost": 1.0,
                "prefer_prepaid": False,
                "allow_payg_fallback": True,
            },
        },
    }


@pytest.fixture(autouse=True)
def _builtin_provider_registry_fixture(monkeypatch):
    import adapters.cli_llm as cli_llm_mod
    import adapters.credentials as credentials_mod
    import adapters.llm_task as llm_task_mod
    import registry.provider_execution_registry as provider_registry_mod

    profiles = _test_profiles_with_declared_auth()

    def _get_profile(provider_slug: str):
        return profiles.get(provider_slug)

    def _registered_providers():
        return sorted(profiles)

    def _resolve_provider_from_alias(alias: str):
        for profile in profiles.values():
            if alias == profile.binary or alias in profile.aliases:
                return profile.provider_slug
        return None

    def _resolve_adapter_contract(provider_slug: str, adapter_type: str):
        return provider_transport.resolve_adapter_contract(
            provider_slug,
            adapter_type,
            profiles=profiles,
            adapter_config={},
            failure_mappings={},
        )

    def _supports_adapter(provider_slug: str, adapter_type: str) -> bool:
        return provider_transport.supports_adapter(
            provider_slug,
            adapter_type,
            profiles=profiles,
            adapter_config={},
            failure_mappings={},
        )

    def _resolve_api_endpoint(provider_slug: str, model_slug: str | None = None):
        return provider_transport.resolve_api_endpoint(
            provider_slug,
            profiles=profiles,
            model_slug=model_slug,
        )

    monkeypatch.setattr(provider_registry_mod, "get_profile", _get_profile)
    monkeypatch.setattr(provider_registry_mod, "registered_providers", _registered_providers)
    monkeypatch.setattr(provider_registry_mod, "resolve_provider_from_alias", _resolve_provider_from_alias)
    monkeypatch.setattr(provider_registry_mod, "resolve_adapter_contract", _resolve_adapter_contract)
    monkeypatch.setattr(provider_registry_mod, "supports_adapter", _supports_adapter)
    monkeypatch.setattr(provider_registry_mod, "resolve_api_endpoint", _resolve_api_endpoint)
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_adapter_economics",
        lambda provider_slug, adapter_type: provider_transport.resolve_adapter_economics(
            provider_slug,
            adapter_type,
            profiles=profiles,
        ),
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "default_provider_slug",
        lambda: "openai",
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_default_adapter_type",
        lambda provider_slug=None: provider_transport.default_adapter_type_for_provider(
            provider_slug or "openai",
            profiles=profiles,
        ) or provider_transport.default_llm_adapter_type(profiles),
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "default_model_for_provider",
        lambda provider_slug: provider_transport.default_model_for_provider(provider_slug, profiles),
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_api_protocol_family",
        lambda provider_slug: provider_transport.resolve_api_protocol_family(provider_slug, profiles=profiles),
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "resolve_api_key_env_vars",
        lambda provider_slug: provider_transport.resolve_api_key_env_vars(provider_slug, profiles=profiles),
    )
    monkeypatch.setattr(
        provider_registry_mod,
        "build_command",
        lambda provider_slug, model=None, **kwargs: provider_transport.build_command(
            provider_slug,
            profiles=profiles,
            model=model,
            **kwargs,
        ),
    )

    monkeypatch.setattr(cli_llm_mod, "get_profile", _get_profile)
    monkeypatch.setattr(cli_llm_mod, "registered_providers", _registered_providers)
    monkeypatch.setattr(cli_llm_mod, "resolve_provider_from_alias", _resolve_provider_from_alias)
    monkeypatch.setattr(cli_llm_mod, "resolve_adapter_contract", _resolve_adapter_contract)
    monkeypatch.setattr(cli_llm_mod, "build_command", provider_registry_mod.build_command)
    monkeypatch.setattr(llm_task_mod, "resolve_adapter_contract", _resolve_adapter_contract)
    monkeypatch.setattr(llm_task_mod, "supports_adapter", _supports_adapter)
    monkeypatch.setattr(llm_task_mod, "resolve_api_endpoint", _resolve_api_endpoint)
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_api_protocol_family",
        lambda provider_slug: provider_transport.resolve_api_protocol_family(provider_slug, profiles=profiles),
    )
    monkeypatch.setattr(
        llm_task_mod,
        "default_model_for_provider",
        lambda provider_slug: provider_transport.default_model_for_provider(provider_slug, profiles),
    )
    monkeypatch.setattr(
        credentials_mod,
        "resolve_api_key_env_vars",
        lambda provider_slug: provider_transport.resolve_api_key_env_vars(provider_slug, profiles=profiles),
    )
    monkeypatch.setattr(sys.modules[__name__], "resolve_api_endpoint", _resolve_api_endpoint)


def _strict_runtime_route_payload() -> dict[str, object]:
    return {
        "route_decision_id": "route_decision.default_path.alpha",
        "route_eligibility_state_id": "eligibility.default_path.alpha",
        "selected_candidate_ref": "candidate.openai.default-path.alpha.gpt54",
        "selected_provider_failover_binding_id": "provider_failover_binding.default-path.alpha",
        "provider_endpoint_binding_id": "provider_endpoint_binding.default-path.alpha",
        "provider_ref": "provider.openai",
        "provider_slug": "openai",
        "model_slug": "gpt-5.4",
        "balance_slot": 0,
        "decision_reason_code": "routing.preferred_candidate",
        "allowed_candidate_refs": [
            "candidate.openai.default-path.alpha.gpt54",
            "candidate.openai.default-path.alpha.gpt54mini",
        ],
        "failover_role": "primary",
        "failover_trigger_rule": "health_degraded",
        "failover_position_index": 0,
        "failover_slice_candidate_refs": [
            "candidate.openai.default-path.alpha.gpt54",
            "candidate.openai.default-path.alpha.gpt54mini",
        ],
        "endpoint_kind": "chat_completions",
        "endpoint_transport_kind": "https",
        "route_authority": "registry.provider_routing",
        "failover_endpoint_authority": "registry.endpoint_failover",
        "as_of": "2026-04-09T12:00:00+00:00",
    }


def test_google_credential_resolution_uses_registry_env_mapping() -> None:
    credential = resolve_credential(
        "secret.default-path.google",
        env={"GOOGLE_API_KEY": "google-test-key"},
    )
    assert credential.provider_hint == "google"
    assert credential.api_key == "google-test-key"


def test_google_protocol_family_uses_registry_endpoint_and_headers(monkeypatch) -> None:
    captured: dict[str, object] = {}
    import adapters.llm_client as llm_client_mod

    def _fake_http_request(*, request, body_bytes, headers, timeout_seconds):
        del timeout_seconds
        captured["url"] = request.endpoint_uri
        captured["body"] = json.loads(body_bytes.decode("utf-8"))
        captured["headers"] = {key.lower(): value for key, value in headers.items()}
        return HTTPResponse(
            status_code=200,
            headers={},
            body=json.dumps(
                {
                    "candidates": [
                        {
                            "content": {"parts": [{"text": "hello from google"}]},
                            "finishReason": "STOP",
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 3,
                        "candidatesTokenCount": 2,
                        "totalTokenCount": 5,
                    },
                }
            ).encode("utf-8"),
        )

    monkeypatch.setattr(llm_client_mod, "_perform_http_request", _fake_http_request)

    request = LLMRequest(
        endpoint_uri=resolve_api_endpoint("google", model_slug="gemini-2.5-flash") or "",
        api_key="google-test-key",
        provider_slug="google",
        model_slug="gemini-2.5-flash",
        messages=({"role": "user", "content": "say hello"},),
        protocol_family="google_generate_content",
        max_tokens=32,
    )
    response = call_llm(request)

    assert response.content == "hello from google"
    assert str(captured["url"]).endswith("/models/gemini-2.5-flash:generateContent")
    assert captured["body"]["contents"][0]["parts"][0]["text"] == "say hello"
    assert captured["headers"]["x-goog-api-key"] == "google-test-key"


def test_streaming_dispatch_uses_protocol_family_instead_of_provider_slug(monkeypatch) -> None:
    captured: dict[str, object] = {}
    import adapters.llm_client as llm_client_mod

    class _FakeStreamResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.headers = {}
            self._lines = [
                b'data: {"choices":[{"delta":{"content":"hel"}}]}\n',
                b'data: {"choices":[{"delta":{"content":"lo"}}]}\n',
                b"data: [DONE]\n",
            ]

        def iter_lines(self, max_line_bytes: int = 65_536):
            del max_line_bytes
            while self._lines:
                yield self._lines.pop(0)

        def close(self) -> None:
            return None

    def _fake_open_streaming_http_request(*, request, body_bytes, headers, timeout_seconds):
        del timeout_seconds
        captured["body"] = json.loads(body_bytes.decode("utf-8"))
        captured["headers"] = {key.lower(): value for key, value in headers.items()}
        return _FakeStreamResponse()

    monkeypatch.setattr(llm_client_mod, "_open_streaming_http_request", _fake_open_streaming_http_request)

    request = LLMRequest(
        endpoint_uri="https://example.invalid/v1/chat/completions",
        api_key="stream-test-key",
        provider_slug="anthropic",
        model_slug="gpt-5.4",
        messages=({"role": "user", "content": "say hello"},),
        protocol_family="openai_chat_completions",
        max_tokens=32,
    )

    events = list(call_llm_streaming(request))

    assert captured["body"]["messages"] == [{"role": "user", "content": "say hello"}]
    assert "system" not in captured["body"]
    assert captured["headers"]["authorization"] == "Bearer stream-test-key"
    assert "".join(event["text"] for event in events if event["type"] == "text_delta") == "hello"
    assert events[-1] == {"type": "message_stop", "stop_reason": "end_turn", "usage": {}}


def test_http_transport_requires_configured_protocol_family(monkeypatch) -> None:
    monkeypatch.setattr("adapters.llm_client.resolve_api_protocol_family", lambda _provider_slug: None)

    request = LLMRequest(
        endpoint_uri="https://example.invalid/v1/chat/completions",
        api_key="missing-family-key",
        provider_slug="mystery-provider",
        model_slug="model-x",
        messages=({"role": "user", "content": "say hello"},),
        protocol_family=None,
        max_tokens=32,
    )

    with pytest.raises(LLMClientError, match="has no configured protocol family"):
        call_llm(request)

    assert list(call_llm_streaming(request)) == [
        {
            "type": "error",
            "message": "streaming unsupported for protocol family: unknown",
        }
    ]


def test_db_backed_provider_profile_inherits_http_contract_fields(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_authority

    del monkeypatch
    profile = provider_registry_authority._parse_profile_row(
        {
            **_openai_provider_authority_row(),
            "default_model": "gpt-5.4",
        }
    )
    profiles = {"openai": profile}

    report = provider_transport.validate_profiles(
        profiles,
        adapter_config={},
        failure_mappings={},
    )

    assert profile.api_protocol_family == "openai_chat_completions"
    assert profile.api_key_env_vars == ("OPENAI_API_KEY",)
    assert provider_transport.resolve_adapter_economics(
        "openai",
        "llm_task",
        profiles=profiles,
    ) == {
        "billing_mode": "metered_api",
        "budget_bucket": "openai_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": False,
        "allow_payg_fallback": True,
    }
    assert report["openai"]["api_protocol_family"] == "openai_chat_completions"
    assert provider_transport.supports_adapter(
        "openai",
        "llm_task",
        profiles=profiles,
        adapter_config={},
        failure_mappings={},
    ) is True


def test_cursor_profile_is_registered_from_db_authority(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod
    import registry.provider_execution_registry as provider_registry_authority
    from _pg_test_conn import get_test_env

    monkeypatch.setenv("WORKFLOW_DATABASE_URL", get_test_env()["WORKFLOW_DATABASE_URL"])
    monkeypatch.setattr(provider_registry_authority, "_read_repo_env_file", lambda _path: {})
    provider_registry_authority.reload_from_db()

    health = provider_registry_mod.registry_health()

    assert health["status"] == "loaded_from_db"
    assert health["provider_count"] >= 1
    assert "cursor" in health["providers"]
    cursor_profile = provider_registry_authority.get_profile("cursor")
    assert cursor_profile is not None
    assert cursor_profile.api_protocol_family == "cursor_background_agent"
    assert provider_registry_mod.supports_adapter("cursor", "llm_task") is True
    assert provider_registry_mod.supports_adapter("cursor", "cli_llm") is False


def test_resolve_adapter_economics_rejects_sparse_authority_rows() -> None:
    """Sparse adapter_economics rows must fail closed (BUG-8DAA5468).

    Previously ``prefer_prepaid`` and ``allow_payg_fallback`` were silently
    defaulted to False when absent — and the router layer independently
    defaulted again, so any layer changing its default would silently
    disagree with its sibling. The contract now refuses sparse rows so the
    authority split cannot re-emerge.
    """
    profiles = _authority_profiles_map()
    openai_profile = profiles["openai"]
    profiles["openai"] = replace(
        openai_profile,
        adapter_economics={
            **dict(openai_profile.adapter_economics or {}),
            "llm_task": {
                "billing_mode": "metered_api",
                "budget_bucket": "openai_api_payg",
                "effective_marginal_cost": 1.0,
            },
        },
    )

    with pytest.raises(provider_transport.AdapterEconomicsAuthorityError) as excinfo:
        provider_transport.resolve_adapter_economics(
            "openai",
            "llm_task",
            profiles=profiles,
        )
    message = str(excinfo.value)
    assert "allow_payg_fallback" in message
    assert "prefer_prepaid" in message


def test_cursor_local_profile_is_registered_for_local_cli(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry_mod

    original_resolve_binary = provider_transport.resolve_binary
    monkeypatch.setattr(
        provider_transport,
        "resolve_binary",
        lambda provider_slug, *, profiles: "/usr/local/bin/cursor-agent"
        if provider_slug == "cursor_local"
        else original_resolve_binary(provider_slug, profiles=profiles),
    )

    profile = _authority_profiles_map()["cursor_local"]
    assert profile.binary == "cursor-agent"
    assert profile.prompt_mode == "stdin"
    assert provider_registry_mod.supports_adapter("cursor_local", "cli_llm") is True


def test_transport_support_handler_returns_provider_and_model_support(monkeypatch) -> None:
    monkeypatch.setenv(
        "WORKFLOW_DATABASE_URL",
        "postgresql://test@localhost:5432/praxis_test",
    )
    import runtime.operations.queries.operator_support as operator_support
    from runtime.operations.queries.operator_support import (
        QueryTransportSupport,
        handle_query_transport_support,
    )

    captured: dict[str, object] = {}
    fake_health_mod = object()
    fake_pg = object()

    class _FakeFrontdoor:
        def query_transport_support(self, **kwargs):
            captured.update(kwargs)
            return {
                "default_provider_slug": "openai",
                "default_adapter_type": "cli_llm",
                "providers": [{"provider_slug": "openai"}],
                "models": [{"provider_slug": "openai", "model_slug": "gpt-4.1"}],
                "route_preflight": {
                    "runtime_profile_ref": kwargs["runtime_profile_ref"],
                    "overall": "ready",
                    "jobs": list(kwargs["jobs"] or ()),
                },
                "count": {"providers": 1, "models": 1},
            }

    monkeypatch.setattr(operator_support, "TransportSupportFrontdoor", _FakeFrontdoor)

    class _FakeSubs:
        def get_health_mod(self):
            return fake_health_mod

        def get_pg_conn(self):
            return fake_pg

    payload = handle_query_transport_support(
        QueryTransportSupport(
            provider_slug="openai",
            model_slug="gpt-4.1",
            runtime_profile_ref="native",
            jobs=[{"label": "build", "agent": "auto/build"}],
        ),
        _FakeSubs(),
    )

    assert payload["default_provider_slug"] == "openai"
    assert payload["count"] == {"providers": 1, "models": 1}
    assert captured == {
        "health_mod": fake_health_mod,
        "pg": fake_pg,
        "provider_filter": "openai",
        "model_filter": "gpt-4.1",
        "runtime_profile_ref": "native",
        "jobs": [{"label": "build", "agent": "auto/build"}],
    }


def test_query_transport_support_uses_authority_and_repository(monkeypatch) -> None:
    monkeypatch.setenv(
        "WORKFLOW_DATABASE_URL",
        "postgresql://test@localhost:5432/praxis_test",
    )
    import surfaces.api.operator_read as operator_read

    captured: dict[str, object] = {}
    fake_pg = object()
    fake_health_mod = object()

    class _FakeRepository:
        def __init__(self, conn) -> None:
            captured["repository_conn"] = conn

    class _FakeAuthority:
        def to_json(self) -> dict[str, object]:
            return {"status": "ready", "count": {"providers": 1, "models": 2}}

    def _fake_load_transport_eligibility_authority(**kwargs):
        captured.update(kwargs)
        return _FakeAuthority()

    monkeypatch.setattr(
        operator_read,
        "PostgresTransportEligibilityRepository",
        _FakeRepository,
    )
    monkeypatch.setattr(
        operator_read,
        "load_transport_eligibility_authority",
        _fake_load_transport_eligibility_authority,
    )

    payload = operator_read.query_transport_support(
        health_mod=fake_health_mod,
        pg=fake_pg,
        provider_filter="openai",
        model_filter="gpt-5.4",
        runtime_profile_ref="native",
        jobs=[{"label": "verify", "agent": "auto/review"}],
    )

    assert payload == {"status": "ready", "count": {"providers": 1, "models": 2}}
    assert captured["repository_conn"] is fake_pg
    assert captured["repository"].__class__ is _FakeRepository
    assert captured["health_mod"] is fake_health_mod
    assert captured["pg"] is fake_pg
    assert captured["provider_filter"] == "openai"
    assert captured["model_filter"] == "gpt-5.4"
    assert captured["runtime_profile_ref"] == "native"
    assert captured["jobs"] == [{"label": "verify", "agent": "auto/review"}]


def test_transport_support_frontdoor_allows_repository_injection(monkeypatch) -> None:
    monkeypatch.setenv(
        "WORKFLOW_DATABASE_URL",
        "postgresql://test@localhost:5432/praxis_test",
    )
    import surfaces.api.operator_read as operator_read

    captured: dict[str, object] = {}
    fake_pg = object()
    fake_health_mod = object()

    class _FakeRepository:
        def __init__(self, conn) -> None:
            captured["repository_conn"] = conn

    class _FakeAuthority:
        def to_json(self) -> dict[str, object]:
            return {"status": "ready", "count": {"providers": 2, "models": 3}}

    def _fake_load_transport_eligibility_authority(**kwargs):
        captured.update(kwargs)
        return _FakeAuthority()

    monkeypatch.setattr(
        operator_read,
        "load_transport_eligibility_authority",
        _fake_load_transport_eligibility_authority,
    )

    payload = operator_read.TransportSupportFrontdoor(
        repository_factory=_FakeRepository,
    ).query_transport_support(
        health_mod=fake_health_mod,
        pg=fake_pg,
        provider_filter="openai",
        model_filter="gpt-5.4",
        runtime_profile_ref="native",
        jobs=[{"label": "verify", "agent": "auto/review"}],
    )

    assert payload == {"status": "ready", "count": {"providers": 2, "models": 3}}
    assert captured["repository_conn"] is fake_pg
    assert captured["repository"].__class__ is _FakeRepository
    assert captured["health_mod"] is fake_health_mod
    assert captured["pg"] is fake_pg
    assert captured["provider_filter"] == "openai"
    assert captured["model_filter"] == "gpt-5.4"
    assert captured["runtime_profile_ref"] == "native"
    assert captured["jobs"] == [{"label": "verify", "agent": "auto/review"}]


def test_cli_and_api_transports_can_be_compared_for_same_provider_and_model(monkeypatch) -> None:
    def _fake_invoke_cli(**kwargs):
        return CLILLMResult(
            content="shared completion",
            exit_code=0,
            stderr="",
            latency_ms=12,
            raw_json=None,
            cli_name="codex",
            provider_slug=str(kwargs["provider_slug"]),
            model_slug=str(kwargs["model_slug"]),
        )

    def _fake_call_llm(request: LLMRequest) -> LLMResponse:
        return LLMResponse(
            content="shared completion",
            model=request.model_slug,
            provider_slug=request.provider_slug,
            usage={"total_tokens": 5},
            raw_response={},
            latency_ms=9,
            status_code=200,
        )

    import adapters.cli_llm as cli_llm_mod
    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(cli_llm_mod, "_invoke_cli", _fake_invoke_cli)
    monkeypatch.setattr(llm_task_mod, "call_llm", _fake_call_llm)

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_parity",
        input_payload={
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    cli_result = CLILLMAdapter(default_provider="openai", prefer_docker=False).execute(request=request)
    api_result = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
    ).execute(request=request)

    def _normalize(result):
        return {
            "status": result.status,
            "completion": result.outputs.get("completion"),
            "provider_slug": result.outputs.get("provider_slug") or result.outputs.get("provider"),
            "model_slug": result.outputs.get("model_slug") or result.outputs.get("model"),
        }

    assert _normalize(cli_result) == _normalize(api_result)


def test_cli_transport_enables_network_for_remote_provider_cli(monkeypatch) -> None:
    captured: dict[str, object] = {}
    import adapters.cli_llm as cli_llm_mod

    def _fake_invoke_cli(**kwargs):
        captured["network"] = kwargs["network"]
        return CLILLMResult(
            content="shared completion",
            exit_code=0,
            stderr="",
            latency_ms=12,
            raw_json=None,
            cli_name="codex",
            provider_slug=str(kwargs["provider_slug"]),
            model_slug=str(kwargs["model_slug"]),
        )

    monkeypatch.setattr(cli_llm_mod, "_invoke_cli", _fake_invoke_cli)

    request = DeterministicTaskRequest(
        node_id="node_network",
        task_name="transport_network_policy",
        input_payload={
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(default_provider="openai", prefer_docker=True).execute(request=request)

    assert captured["network"] is True
    assert result.status == "succeeded"


def test_cli_transport_respects_disabled_network_override(monkeypatch) -> None:
    captured: dict[str, object] = {}
    import adapters.cli_llm as cli_llm_mod

    def _fake_invoke_cli(**kwargs):
        captured["network"] = kwargs["network"]
        return CLILLMResult(
            content="shared completion",
            exit_code=0,
            stderr="",
            latency_ms=12,
            raw_json=None,
            cli_name="codex",
            provider_slug=str(kwargs["provider_slug"]),
            model_slug=str(kwargs["model_slug"]),
        )

    monkeypatch.setattr(cli_llm_mod, "_invoke_cli", _fake_invoke_cli)

    request = DeterministicTaskRequest(
        node_id="node_network_override",
        task_name="transport_network_override",
        input_payload={
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "network_policy": "disabled",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(default_provider="openai", prefer_docker=True).execute(request=request)

    assert captured["network"] is False
    assert result.status == "succeeded"


def test_cli_adapter_fails_closed_when_runner_raises_runtime_error(monkeypatch) -> None:
    import adapters.cli_llm as cli_llm_mod

    monkeypatch.setattr(
        cli_llm_mod,
        "run_model",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("Docker is required for workflow model execution but is unavailable.")
        ),
    )

    request = DeterministicTaskRequest(
        node_id="node_cli_runtime_error",
        task_name="transport_runtime_error",
        input_payload={
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(default_provider="openai", prefer_docker=True).execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "cli_adapter.exec_error"
    assert result.outputs["transport_kind"] == "cli"
    assert result.outputs["failure_namespace"] == "cli_adapter"


def test_cli_execution_policy_uses_explicit_sandbox_contract() -> None:
    policy = resolve_cli_execution_policy(
        {
            "sandbox_profile": {
                "network_policy": "disabled",
                "auth_mount_policy": "none",
            }
        },
        profile=SimpleNamespace(
            api_endpoint="https://api.openai.com/v1",
            api_protocol_family="openai_responses",
            api_key_env_vars=("OPENAI_API_KEY",),
        ),
    )

    assert policy.network_policy == "disabled"
    assert policy.network_enabled is False
    assert policy.auth_mount_policy == "none"


def test_cli_execution_policy_defaults_cli_only_provider_to_networked_lane() -> None:
    profile = _authority_profiles_map()["anthropic"]

    policy = resolve_cli_execution_policy({}, profile=profile)

    assert profile.api_endpoint is None
    assert profile.api_protocol_family is None
    assert profile.api_key_env_vars == ()
    assert policy.network_policy == "provider_only"
    assert policy.network_enabled is True
    assert policy.auth_mount_policy == "provider_scoped"


def test_llm_task_uses_transport_registry_for_non_chat_protocols(monkeypatch) -> None:
    captured: dict[str, object] = {}
    import adapters.llm_task as llm_task_mod

    def _fake_call_transport(
        protocol_family: str,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        timeout: int,
        api_endpoint: str,
        api_key: str | None = None,
        api_key_env: str = "",
        workdir: str | None = None,
        reasoning_effort: str | None = None,
    ) -> str:
        captured["protocol_family"] = protocol_family
        captured["prompt"] = prompt
        captured["model"] = model
        captured["max_tokens"] = max_tokens
        captured["timeout"] = timeout
        captured["api_endpoint"] = api_endpoint
        captured["api_key"] = api_key
        captured["api_key_env"] = api_key_env
        captured["workdir"] = workdir
        captured["reasoning_effort"] = reasoning_effort
        return "CURSOR_TRANSPORT_OK"

    def _unexpected_call_llm(_request: LLMRequest) -> LLMResponse:
        raise AssertionError("call_llm should not be used for cursor_background_agent")

    monkeypatch.setattr(llm_task_mod, "call_transport", _fake_call_transport)
    monkeypatch.setattr(llm_task_mod, "call_llm", _unexpected_call_llm)
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_credential",
        lambda auth_ref, env=None: SimpleNamespace(
            auth_ref=auth_ref,
            api_key="cursor-test-key",
            provider_hint="cursor",
        ),
    )

    request = DeterministicTaskRequest(
        node_id="node_cursor",
        task_name="cursor_task",
        input_payload={
            "prompt": "Reply with CURSOR_TRANSPORT_OK",
            "provider_slug": "cursor",
            "model_slug": "auto",
            "workdir": "/tmp/repo",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = LLMTaskAdapter(
        default_provider="cursor",
        default_model="auto",
        credential_env={"CURSOR_API_KEY": "cursor-test-key"},
    ).execute(request=request)

    assert result.status == "succeeded"
    assert result.outputs["completion"] == "CURSOR_TRANSPORT_OK"
    assert result.outputs["provider"] == "cursor"
    assert result.outputs["model"] == "auto"
    assert captured == {
        "protocol_family": "cursor_background_agent",
        "prompt": "User:\nReply with CURSOR_TRANSPORT_OK",
        "model": "auto",
        "max_tokens": 4096,
        "timeout": 120,
        "api_endpoint": "https://api.cursor.com/v0/agents",
        "api_key": "cursor-test-key",
        "api_key_env": "secret.default-path.cursor",
        "workdir": "/tmp/repo",
        "reasoning_effort": None,
    }


def test_llm_task_maps_custom_transport_http_errors(monkeypatch) -> None:
    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(
        llm_task_mod,
        "call_transport",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            TransportExecutionError(
                "http_transport.http_error",
                "HTTP 400: cursor repository access denied",
                status_code=400,
            )
        ),
    )
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_credential",
        lambda auth_ref, env=None: SimpleNamespace(
            auth_ref=auth_ref,
            api_key="cursor-test-key",
            provider_hint="cursor",
        ),
    )

    result = LLMTaskAdapter(
        default_provider="cursor",
        default_model="auto",
    ).execute(
        request=DeterministicTaskRequest(
            node_id="node_cursor_error",
            task_name="cursor_task_error",
            input_payload={
                "prompt": "Reply with CURSOR_TRANSPORT_OK",
                "provider_slug": "cursor",
                "model_slug": "auto",
                "workdir": "/tmp/repo",
            },
            expected_outputs={},
            dependency_inputs={},
            execution_boundary_ref="workspace:test",
        )
    )

    assert result.status == "failed"
    assert result.reason_code == "adapter.http_error"
    assert result.failure_code == "adapter.http_error"
    assert result.outputs["status_code"] == 400
    assert result.outputs["stderr"] == "HTTP 400: cursor repository access denied"


def test_provider_adapter_contract_exposes_explicit_transport_surface() -> None:
    from registry.provider_execution_registry import resolve_adapter_contract

    cli_contract = resolve_adapter_contract("openai", "cli_llm")
    api_contract = resolve_adapter_contract("openai", "llm_task")

    assert cli_contract is not None
    assert cli_contract.transport_kind == "cli"
    assert cli_contract.failure_namespace == "cli_adapter"
    assert cli_contract.prompt_envelope["kind"] == "stdin_prompt"
    assert cli_contract.structured_output["source"] == "stdout"
    assert cli_contract.retry_policy["retry_attempts"] == 0
    assert api_contract is not None
    assert api_contract.transport_kind == "http"
    assert api_contract.failure_namespace == "adapter"
    assert api_contract.prompt_envelope["kind"] == "openai_chat_completions"
    assert api_contract.prompt_envelope["prompt_channel"] == "messages"
    assert api_contract.tool_policy["supports_tools"] is True
    assert api_contract.structured_output["source"] == "response_body"
    assert api_contract.retry_policy["retry_attempts"] >= 1
    assert "adapter.timeout" in api_contract.retryable_failure_codes
    assert api_contract.failover_failure_codes == (
        "adapter.timeout",
        "adapter.http_error",
        "adapter.network_error",
    )
    assert set(api_contract.failover_failure_codes).issubset(
        set(api_contract.retryable_failure_codes)
    )
    assert api_contract.failure_mapping["llm_client.timeout"] == "adapter.timeout"
    assert api_contract.failure_mapping["http_transport.http_error"] == "adapter.http_error"


def test_provider_adapter_contract_round_trips_through_contract_shape() -> None:
    contract = ProviderAdapterContract(
        provider_slug="openai",
        adapter_type="llm_task",
        transport_kind="http",
        execution_kind="request",
        failure_namespace="provider.openai",
        prompt_envelope={"protocol_family": "openai_chat_completions"},
        tool_policy={"mode": "catalog"},
        structured_output={"format": "json"},
        timeout_seconds=300,
        telemetry={"authority": "test"},
        retry_policy={"max_attempts": 3},
        failure_mapping={"timeout": "provider.openai.timeout"},
        readiness={"status": "ready"},
        retryable_failure_codes=(
            "provider.openai.timeout",
            "provider.openai.rate_limited",
            "provider.openai.service_unavailable",
        ),
        failover_failure_codes=(
            "provider.openai.timeout",
            "provider.openai.service_unavailable",
        ),
    )

    round_tripped = ProviderAdapterContract.from_contract(contract.to_contract())

    assert round_tripped == contract


def test_http_failure_mapping_merges_db_overrides_without_dropping_builtin_codes() -> None:
    mapping = provider_transport._http_failure_mapping(  # type: ignore[attr-defined]
        {"http": {"llm_client.timeout": "adapter.timeout.override"}}
    )

    assert mapping["llm_client.timeout"] == "adapter.timeout.override"
    assert mapping["http_transport.http_error"] == "adapter.http_error"


def test_route_economics_preserves_zero_cost_for_prepaid_lanes(monkeypatch) -> None:
    import runtime.routing_economics as routing_economics_mod

    monkeypatch.setattr(routing_economics_mod, "resolve_adapter_economics", lambda provider_slug, adapter_type: {
        "billing_mode": "subscription_included",
        "budget_bucket": f"{provider_slug}_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": True,
        "allow_payg_fallback": True,
    })
    monkeypatch.setattr(routing_economics_mod, "supports_adapter", lambda provider_slug, adapter_type: True)

    economics = routing_economics_mod.resolve_route_economics(
        provider_slug="openai",
        adapter_type=None,
        provider_policy_id=None,
        raw_cost_per_m_tokens=8.75,
        budget_authority=routing_economics_mod.BudgetAuthoritySnapshot.empty(),
        default_adapter="cli_llm",
    )

    assert economics["effective_marginal_cost"] == 0.0
    assert economics["allow_payg_fallback"] is True


def test_route_economics_rejects_sparse_authority_at_transport_contract(monkeypatch) -> None:
    """Sparse economics rows fail at the contract gate (BUG-8DAA5468).

    Previously ``resolve_route_economics`` silently defaulted missing
    ``allow_payg_fallback`` to False — duplicating the defaulting logic
    in ``provider_transport.resolve_adapter_economics`` and letting the
    two layers drift. With the :class:`AdapterEconomicsContract`, sparse
    rows raise before they can reach the routing surface, so no caller
    ever sees a partially-specified economics dict.
    """
    import runtime.routing_economics as routing_economics_mod

    def _sparse_profile_economics(provider_slug: str, adapter_type: str):
        # Simulates what would happen if a sparse DB row survived into the
        # transport layer: the contract rejects it there, and that error
        # propagates out of resolve_route_economics unchanged.
        raise provider_transport.AdapterEconomicsAuthorityError(
            f"adapter_economics for {provider_slug}/{adapter_type} "
            "must set ['allow_payg_fallback', 'prefer_prepaid']"
        )

    monkeypatch.setattr(
        routing_economics_mod, "resolve_adapter_economics", _sparse_profile_economics
    )
    monkeypatch.setattr(
        routing_economics_mod, "supports_adapter", lambda provider_slug, adapter_type: True
    )

    with pytest.raises(provider_transport.AdapterEconomicsAuthorityError) as excinfo:
        routing_economics_mod.resolve_route_economics(
            provider_slug="openai",
            adapter_type=None,
            provider_policy_id=None,
            raw_cost_per_m_tokens=8.75,
            budget_authority=routing_economics_mod.BudgetAuthoritySnapshot.empty(),
            default_adapter="cli_llm",
        )
    assert "allow_payg_fallback" in str(excinfo.value)


def test_route_economics_prefers_prepaid_adapter_over_metered_default(monkeypatch) -> None:
    import runtime.routing_economics as routing_economics_mod

    monkeypatch.setattr(
        routing_economics_mod,
        "supports_adapter",
        lambda provider_slug, adapter_type: adapter_type in {"cli_llm", "llm_task"},
    )

    def _economics(provider_slug: str, adapter_type: str) -> dict[str, object]:
        del provider_slug
        if adapter_type == "cli_llm":
            return {
                "billing_mode": "subscription_included",
                "budget_bucket": "openai_monthly",
                "effective_marginal_cost": 0.0,
                "prefer_prepaid": True,
                "allow_payg_fallback": True,
            }
        return {
            "billing_mode": "metered_api",
            "budget_bucket": "openai_api_payg",
            "effective_marginal_cost": 1.0,
            "prefer_prepaid": False,
            "allow_payg_fallback": True,
        }

    monkeypatch.setattr(routing_economics_mod, "resolve_adapter_economics", _economics)

    economics = routing_economics_mod.resolve_route_economics(
        provider_slug="openai",
        adapter_type=None,
        provider_policy_id=None,
        raw_cost_per_m_tokens=8.75,
        budget_authority=routing_economics_mod.BudgetAuthoritySnapshot.empty(),
        default_adapter="llm_task",
    )

    assert economics["adapter_type"] == "cli_llm"
    assert economics["billing_mode"] == "subscription_included"
    assert economics["effective_marginal_cost"] == 0.0


def test_llm_task_uses_contract_retry_policy_and_failure_mapping(monkeypatch) -> None:
    import registry.provider_execution_registry as _reg

    profiles = {"openai": _reg._parse_profile_row(_openai_provider_authority_row())}
    contract = provider_transport.resolve_adapter_contract(
        "openai",
        "llm_task",
        profiles=profiles,
        adapter_config={},
        failure_mappings={},
    )
    assert contract is not None

    captured: dict[str, object] = {}

    def _boom(request: LLMRequest) -> LLMResponse:
        captured["timeout_seconds"] = request.timeout_seconds
        captured["retry_attempts"] = request.retry_attempts
        captured["retry_backoff_seconds"] = request.retry_backoff_seconds
        captured["retryable_status_codes"] = request.retryable_status_codes
        captured["protocol_family"] = request.protocol_family
        raise LLMClientError("llm_client.timeout", "request timed out")

    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(llm_task_mod, "call_llm", _boom)
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_adapter_contract",
        lambda provider_slug, adapter_type: provider_transport.resolve_adapter_contract(
            provider_slug,
            adapter_type,
            profiles=profiles,
            adapter_config={},
            failure_mappings={},
        ),
    )
    monkeypatch.setattr(
        llm_task_mod,
        "supports_adapter",
        lambda provider_slug, adapter_type: provider_transport.supports_adapter(
            provider_slug,
            adapter_type,
            profiles=profiles,
            adapter_config={},
            failure_mappings={},
        ),
    )

    adapter = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
    )
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="contract_retry_policy",
        input_payload={"prompt": "hello", "provider_slug": "openai"},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = adapter.execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "adapter.timeout"
    assert captured["timeout_seconds"] == contract.timeout_seconds
    assert captured["retry_attempts"] == contract.retry_policy["retry_attempts"]
    assert captured["retry_backoff_seconds"] == tuple(contract.retry_policy["backoff_seconds"])
    assert captured["retryable_status_codes"] == tuple(contract.retry_policy["retryable_status_codes"])
    assert captured["protocol_family"] == contract.prompt_envelope["protocol_family"]


def test_provider_authority_row_preserves_openai_default_provider_contract() -> None:
    import registry.provider_execution_registry as _reg

    profile = _reg._parse_profile_row(_openai_provider_authority_row())

    assert profile.provider_slug == "openai"
    assert profile.api_protocol_family == "openai_chat_completions"
    assert profile.lane_policies["llm_task"]["admitted_by_policy"] is True


def test_llm_task_accepts_explicit_first_party_route_contract_without_registry_lookup(
    monkeypatch,
) -> None:
    from registry.provider_execution_registry import resolve_adapter_contract

    contract = resolve_adapter_contract("openai", "llm_task")
    assert contract is not None

    captured: dict[str, object] = {}

    def _fake_call_llm(request: LLMRequest) -> LLMResponse:
        captured["endpoint_uri"] = request.endpoint_uri
        captured["protocol_family"] = request.protocol_family
        captured["timeout_seconds"] = request.timeout_seconds
        return LLMResponse(
            content="strict runtime path",
            model=request.model_slug,
            provider_slug=request.provider_slug,
            usage={"total_tokens": 7},
            raw_response={},
            latency_ms=11,
            status_code=200,
        )

    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(llm_task_mod, "resolve_adapter_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        llm_task_mod,
        "supports_adapter",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict route contract should not consult supports_adapter")
        ),
    )
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_api_protocol_family",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict route contract should not consult resolve_api_protocol_family")
        ),
    )
    monkeypatch.setattr(
        llm_task_mod,
        "resolve_api_endpoint",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict route contract should not consult resolve_api_endpoint")
        ),
    )
    monkeypatch.setattr(llm_task_mod, "call_llm", _fake_call_llm)

    adapter = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
    )
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="strict_route_contract",
        input_payload={
            "adapter_type": "llm_task",
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "endpoint_uri": "https://api.example.test/v1/chat/completions",
            "auth_ref": "secret.default-path.openai",
            "timeout_seconds": 30,
            "route_contract_required": True,
            "provider_adapter_contract": contract.to_contract(),
            "runtime_route": _strict_runtime_route_payload(),
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = adapter.execute(request=request)

    assert result.status == "succeeded"
    assert captured["endpoint_uri"] == "https://api.example.test/v1/chat/completions"
    assert captured["protocol_family"] == contract.prompt_envelope["protocol_family"]
    assert captured["timeout_seconds"] == 30
    assert result.outputs["transport_kind"] == "http"
    assert result.outputs["route_contract_required"] is True
    assert result.outputs["selected_candidate_ref"] == "candidate.openai.default-path.alpha.gpt54"
    assert result.outputs["provider_endpoint_binding_id"] == "provider_endpoint_binding.default-path.alpha"
    assert result.outputs["decision_reason_code"] == "routing.preferred_candidate"
    assert result.outputs["allowed_candidate_refs"] == [
        "candidate.openai.default-path.alpha.gpt54",
        "candidate.openai.default-path.alpha.gpt54mini",
    ]
    assert result.outputs["failover_trigger_rule"] == "health_degraded"
    assert result.outputs["failover_slice_candidate_refs"] == [
        "candidate.openai.default-path.alpha.gpt54",
        "candidate.openai.default-path.alpha.gpt54mini",
    ]
    assert result.outputs["endpoint_transport_kind"] == "https"


def test_llm_task_strict_route_contract_requires_explicit_endpoint(monkeypatch) -> None:
    from registry.provider_execution_registry import resolve_adapter_contract

    contract = resolve_adapter_contract("openai", "llm_task")
    assert contract is not None

    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(llm_task_mod, "resolve_adapter_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(llm_task_mod, "resolve_api_endpoint", lambda *_args, **_kwargs: "https://fallback.example.test")
    monkeypatch.setattr(llm_task_mod, "supports_adapter", lambda *_args, **_kwargs: True)

    adapter = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
    )
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="strict_route_contract_missing_endpoint",
        input_payload={
            "adapter_type": "llm_task",
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "auth_ref": "secret.default-path.openai",
            "route_contract_required": True,
            "provider_adapter_contract": contract.to_contract(),
            "runtime_route": _strict_runtime_route_payload(),
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = adapter.execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "adapter.endpoint_required"
    assert result.outputs["route_contract_required"] is True
    assert result.outputs["selected_candidate_ref"] == "candidate.openai.default-path.alpha.gpt54"
    assert result.outputs["provider_endpoint_binding_id"] == "provider_endpoint_binding.default-path.alpha"
    assert result.outputs["failover_trigger_rule"] == "health_degraded"
    assert result.outputs["endpoint_transport_kind"] == "https"


def test_llm_task_strict_route_contract_requires_explicit_runtime_route(monkeypatch) -> None:
    from registry.provider_execution_registry import resolve_adapter_contract

    contract = resolve_adapter_contract("openai", "llm_task")
    assert contract is not None

    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(llm_task_mod, "resolve_adapter_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        llm_task_mod,
        "supports_adapter",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict route contract should not consult supports_adapter")
        ),
    )

    adapter = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
    )
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="strict_route_contract_missing_runtime_route",
        input_payload={
            "adapter_type": "llm_task",
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "endpoint_uri": "https://api.example.test/v1/chat/completions",
            "auth_ref": "secret.default-path.openai",
            "route_contract_required": True,
            "provider_adapter_contract": contract.to_contract(),
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = adapter.execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "adapter.runtime_route_required"
    assert result.outputs["route_contract_required"] is True


def test_cli_transport_refuses_first_party_route_contract_without_executing_cli(
    monkeypatch,
) -> None:
    from registry.provider_execution_registry import resolve_adapter_contract

    contract = resolve_adapter_contract("openai", "llm_task")
    assert contract is not None

    import adapters.cli_llm as cli_llm_mod

    monkeypatch.setattr(
        cli_llm_mod,
        "_invoke_cli",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict route contract must not execute through cli_llm")
        ),
    )

    adapter = CLILLMAdapter(default_provider="openai", prefer_docker=False)
    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="cli_route_contract_bridge_guard",
        input_payload={
            "adapter_type": "llm_task",
            "prompt": "hello",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "endpoint_uri": "https://api.example.test/v1/chat/completions",
            "auth_ref": "secret.default-path.openai",
            "route_contract_required": True,
            "provider_adapter_contract": contract.to_contract(),
            "runtime_route": _strict_runtime_route_payload(),
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = adapter.execute(request=request)

    assert result.status == "failed"
    assert result.failure_code == "cli_adapter.route_contract_unsupported"
    assert result.outputs["route_contract_required"] is True
    assert result.outputs["requested_adapter_type"] == "llm_task"
    assert result.outputs["selected_candidate_ref"] == "candidate.openai.default-path.alpha.gpt54"
    assert result.outputs["provider_endpoint_binding_id"] == "provider_endpoint_binding.default-path.alpha"
    assert result.outputs["decision_reason_code"] == "routing.preferred_candidate"
    assert result.outputs["failover_trigger_rule"] == "health_degraded"
    assert result.outputs["endpoint_transport_kind"] == "https"
    assert result.outputs["provider_adapter_contract"]["adapter_type"] == "llm_task"


def test_cli_and_api_transports_can_execute_from_same_execution_packet(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _PacketConn:
        def execute(self, query: str, *params: object):
            if "FROM execution_packets" not in query:
                return []
            assert params == ("packet_exec.alpha:1",)
            return [
                {
                    "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_alpha",
                    "packet_revision": "packet_exec.alpha:1",
                    "parent_artifact_ref": "packet_lineage.alpha",
                    "packet_version": 1,
                    "packet_hash": "packet_hash_alpha",
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "spec_name": "alpha",
                    "source_kind": "workflow_runtime",
                    "authority_refs": ["def_alpha", "plan_alpha"],
                    "model_messages": [
                        {
                            "messages": [
                                {"role": "system", "content": "packet system"},
                                {"role": "user", "content": "hello from packet"},
                            ]
                        }
                    ],
                    "reference_bindings": [],
                    "capability_bindings": [],
                    "verify_refs": [],
                    "authority_inputs": {},
                    "file_inputs": {},
                    "payload": {
                        "packet_revision": "packet_exec.alpha:1",
                        "packet_hash": "packet_hash_alpha",
                    },
                    "decision_ref": "decision.compile.packet.alpha",
                }
            ]

    def _fake_invoke_cli(**kwargs):
        captured["cli_prompt"] = kwargs["prompt"]
        captured["cli_system_prompt"] = kwargs["system_prompt"]
        return CLILLMResult(
            content="shared completion",
            exit_code=0,
            stderr="",
            latency_ms=12,
            raw_json=None,
            cli_name="codex",
            provider_slug=str(kwargs["provider_slug"]),
            model_slug=str(kwargs["model_slug"]),
        )

    def _fake_call_llm(request: LLMRequest) -> LLMResponse:
        captured["api_messages"] = request.messages
        captured["api_system_prompt"] = request.system_prompt
        return LLMResponse(
            content="shared completion",
            model=request.model_slug,
            provider_slug=request.provider_slug,
            usage={"total_tokens": 5},
            raw_response={},
            latency_ms=9,
            status_code=200,
        )

    import adapters.cli_llm as cli_llm_mod
    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(cli_llm_mod, "_invoke_cli", _fake_invoke_cli)
    monkeypatch.setattr(llm_task_mod, "call_llm", _fake_call_llm)

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_parity",
        input_payload={
            "packet_required": True,
            "execution_packet_ref": "packet_exec.alpha:1",
            "execution_packet_hash": "packet_hash_alpha",
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    cli_result = CLILLMAdapter(
        default_provider="openai",
        prefer_docker=False,
        conn_factory=_PacketConn,
    ).execute(request=request)
    api_result = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
        conn_factory=_PacketConn,
    ).execute(request=request)

    assert captured["cli_prompt"] == "hello from packet"
    assert captured["cli_system_prompt"] == "packet system"
    assert captured["api_messages"] == (
        {"role": "system", "content": "packet system"},
        {"role": "user", "content": "hello from packet"},
    )
    assert captured["api_system_prompt"] is None
    assert cli_result.outputs["completion"] == api_result.outputs["completion"] == "shared completion"


def test_api_transport_reads_execution_packet_from_dependency_inputs(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _PacketConn:
        def execute(self, query: str, *params: object):
            if "FROM execution_packets" not in query:
                return []
            assert params == ("packet_exec.alpha:1",)
            return [
                {
                    "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_alpha",
                    "packet_revision": "packet_exec.alpha:1",
                    "parent_artifact_ref": "packet_lineage.alpha",
                    "packet_version": 1,
                    "packet_hash": "packet_hash_alpha",
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "spec_name": "alpha",
                    "source_kind": "workflow_runtime",
                    "authority_refs": ["def_alpha", "plan_alpha"],
                    "model_messages": [{"messages": [{"role": "user", "content": "hello from packet"}]}],
                    "reference_bindings": [],
                    "capability_bindings": [],
                    "verify_refs": [],
                    "authority_inputs": {},
                    "file_inputs": {},
                    "payload": {
                        "packet_revision": "packet_exec.alpha:1",
                        "packet_hash": "packet_hash_alpha",
                    },
                    "decision_ref": "decision.compile.packet.alpha",
                }
            ]

    def _fake_call_llm(request: LLMRequest) -> LLMResponse:
        captured["messages"] = request.messages
        return LLMResponse(
            content="shared completion",
            model=request.model_slug,
            provider_slug=request.provider_slug,
            usage={"total_tokens": 5},
            raw_response={},
            latency_ms=9,
            status_code=200,
        )

    import adapters.llm_task as llm_task_mod

    monkeypatch.setattr(llm_task_mod, "call_llm", _fake_call_llm)

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_dependency_merge",
        input_payload={
            "packet_required": True,
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={
            "execution_packet_ref": "packet_exec.alpha:1",
            "execution_packet_hash": "packet_hash_alpha",
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
        },
        execution_boundary_ref="workspace:test",
    )

    result = LLMTaskAdapter(
        default_provider="openai",
        default_model="gpt-4.1",
        credential_env={"OPENAI_API_KEY": "sk-test"},
        conn_factory=_PacketConn,
    ).execute(request=request)

    assert result.status == "succeeded"
    assert captured["messages"] == ({"role": "user", "content": "hello from packet"},)


def test_cli_transport_fails_closed_when_packet_required_has_only_raw_prompt(monkeypatch) -> None:
    import adapters.cli_llm as cli_llm_mod

    monkeypatch.setattr(
        cli_llm_mod,
        "_invoke_cli",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("raw prompt fallback should not execute")),
    )

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_only_cutover",
        input_payload={
            "packet_required": True,
            "prompt": "this raw prompt must be ignored",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(
        default_provider="openai",
        prefer_docker=False,
        conn_factory=lambda: SimpleNamespace(execute=lambda *_args, **_kwargs: []),
    ).execute(request=request)

    assert result.status == "failed"
    assert result.reason_code == "execution_packet.ref_missing"
    assert result.failure_code == "execution_packet.ref_missing"


def test_cli_transport_fails_closed_when_execution_packet_plan_revision_drifts() -> None:
    class _PacketConn:
        def execute(self, query: str, *params: object):
            if "FROM execution_packets" not in query:
                return []
            assert params == ("packet_exec.alpha:1",)
            return [
                {
                    "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_other",
                    "packet_revision": "packet_exec.alpha:1",
                    "parent_artifact_ref": "packet_lineage.alpha",
                    "packet_version": 1,
                    "packet_hash": "packet_hash_alpha",
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "spec_name": "alpha",
                    "source_kind": "workflow_runtime",
                    "authority_refs": ["def_alpha", "plan_other"],
                    "model_messages": [{"messages": [{"role": "user", "content": "hello from packet"}]}],
                    "reference_bindings": [],
                    "capability_bindings": [],
                    "verify_refs": [],
                    "authority_inputs": {},
                    "file_inputs": {},
                    "payload": {
                        "packet_revision": "packet_exec.alpha:1",
                        "packet_hash": "packet_hash_alpha",
                    },
                    "decision_ref": "decision.compile.packet.alpha",
                }
            ]

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_drift",
        input_payload={
            "packet_required": True,
            "execution_packet_ref": "packet_exec.alpha:1",
            "execution_packet_hash": "packet_hash_alpha",
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(
        default_provider="openai",
        prefer_docker=False,
        conn_factory=_PacketConn,
    ).execute(request=request)

    assert result.status == "failed"
    assert result.reason_code == "execution_packet.plan_revision_mismatch"
    assert result.failure_code == "execution_packet.plan_revision_mismatch"


def test_cli_transport_fails_closed_when_workflow_packet_compile_index_is_stale(monkeypatch) -> None:
    class _PacketConn:
        def execute(self, query: str, *params: object):
            if "FROM execution_packets" not in query:
                return []
            assert params == ("packet_exec.alpha:1",)
            return [
                {
                    "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_alpha",
                    "packet_revision": "packet_exec.alpha:1",
                    "parent_artifact_ref": "packet_lineage.alpha",
                    "packet_version": 1,
                    "packet_hash": "packet_hash_alpha",
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "spec_name": "alpha",
                    "source_kind": "workflow_runtime",
                    "authority_refs": ["def_alpha", "plan_alpha"],
                    "model_messages": [{"messages": [{"role": "user", "content": "hello from packet"}]}],
                    "reference_bindings": [],
                    "capability_bindings": [],
                    "verify_refs": [],
                    "authority_inputs": {
                        "workflow_definition": {
                            "type": "operating_model",
                            "definition_revision": "def_alpha",
                            "compile_provenance": {
                                "compile_index_ref": "compile_index.alpha",
                                "compile_surface_revision": "surface.alpha",
                            },
                        }
                    },
                    "file_inputs": {},
                    "payload": {
                        "packet_revision": "packet_exec.alpha:1",
                        "packet_hash": "packet_hash_alpha",
                    },
                    "decision_ref": "decision.compile.packet.alpha",
                }
            ]

    monkeypatch.setattr(
        compile_index,
        "load_compile_index_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            compile_index.CompileIndexAuthorityError(
                "compile_index.snapshot_stale",
                "compile index snapshot is stale",
            )
        ),
    )

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_compile_index_stale",
        input_payload={
            "packet_required": True,
            "execution_packet_ref": "packet_exec.alpha:1",
            "execution_packet_hash": "packet_hash_alpha",
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(
        default_provider="openai",
        prefer_docker=False,
        conn_factory=_PacketConn,
    ).execute(request=request)

    assert result.status == "failed"
    assert result.reason_code == "execution_packet.compile_index_stale"
    assert result.failure_code == "execution_packet.compile_index_stale"


def test_cli_transport_fails_closed_when_workflow_packet_compile_index_is_missing() -> None:
    class _PacketConn:
        def execute(self, query: str, *params: object):
            if "FROM execution_packets" not in query:
                return []
            assert params == ("packet_exec.alpha:1",)
            return [
                {
                    "execution_packet_id": "execution_packet.run.alpha.packet_exec.alpha:1",
                    "definition_revision": "def_alpha",
                    "plan_revision": "plan_alpha",
                    "packet_revision": "packet_exec.alpha:1",
                    "parent_artifact_ref": "packet_lineage.alpha",
                    "packet_version": 1,
                    "packet_hash": "packet_hash_alpha",
                    "workflow_id": "workflow.alpha",
                    "run_id": "run.alpha",
                    "spec_name": "alpha",
                    "source_kind": "workflow_runtime",
                    "authority_refs": ["def_alpha", "plan_alpha"],
                    "model_messages": [{"messages": [{"role": "user", "content": "hello from packet"}]}],
                    "reference_bindings": [],
                    "capability_bindings": [],
                    "verify_refs": [],
                    "authority_inputs": {
                        "workflow_definition": {
                            "type": "operating_model",
                            "definition_revision": "def_alpha",
                        }
                    },
                    "file_inputs": {},
                    "payload": {
                        "packet_revision": "packet_exec.alpha:1",
                        "packet_hash": "packet_hash_alpha",
                    },
                    "decision_ref": "decision.compile.packet.alpha",
                }
            ]

    request = DeterministicTaskRequest(
        node_id="node_0",
        task_name="transport_packet_compile_index_missing",
        input_payload={
            "packet_required": True,
            "execution_packet_ref": "packet_exec.alpha:1",
            "execution_packet_hash": "packet_hash_alpha",
            "definition_revision": "def_alpha",
            "plan_revision": "plan_alpha",
            "provider_slug": "openai",
            "model_slug": "gpt-4.1",
        },
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="workspace:test",
    )

    result = CLILLMAdapter(
        default_provider="openai",
        prefer_docker=False,
        conn_factory=_PacketConn,
    ).execute(request=request)

    assert result.status == "failed"
    assert result.reason_code == "execution_packet.compile_index_missing"
    assert result.failure_code == "execution_packet.compile_index_missing"
