from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace

import pytest
from adapters.provider_types import ProviderCLIProfile

from registry import provider_onboarding
import registry.provider_onboarding._execute as provider_onboarding_execute
import registry.provider_onboarding._probe as provider_onboarding_probe
import registry.provider_onboarding._report as provider_onboarding_report
import surfaces.mcp.tools.provider_onboard as provider_onboard_tool
from surfaces.cli import native_operator
from surfaces.mcp.tools.provider_onboard import tool_praxis_provider_onboard


class _FakeTx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _FakeTx:
        return _FakeTx()

    async def execute(self, query: str, *params: object) -> str:
        self.executed.append((query, params))
        return "OK"

    async def fetch(self, query: str, *params: object):
        del params
        if "FROM market_model_registry" in query:
            return []
        return []

    async def fetchrow(self, query: str, *params: object):
        del params
        if "FROM market_benchmark_source_registry" in query:
            return {
                "source_slug": "artificial_analysis",
                "display_name": "Artificial Analysis",
                "api_key_env_var": "ARTIFICIAL_ANALYSIS_API_KEY",
                "enabled": True,
            }
        rows = await self.fetch(query)
        return rows[0] if rows else None

    async def close(self) -> None:
        return None


def _openai_cli_spec(
    *,
    benchmark_api_key: str | None = "aa-test-key",
) -> provider_onboarding.ProviderOnboardingSpec:
    return provider_onboarding.ProviderOnboardingSpec(
        provider_slug="openai",
        selected_transport="cli",
        requested_models=("gpt-4.1",),
        benchmark_source_slug="artificial_analysis",
        benchmark_api_key=benchmark_api_key,
    )


def _openai_api_spec(*, benchmark_api_key: str | None = "aa-test-key") -> provider_onboarding.ProviderOnboardingSpec:
    return provider_onboarding.ProviderOnboardingSpec(
        provider_slug="openai",
        selected_transport="api",
        requested_models=("gpt-4.1",),
        benchmark_source_slug="artificial_analysis",
        benchmark_api_key=benchmark_api_key,
    )


def _localcli_cli_spec() -> provider_onboarding.ProviderOnboardingSpec:
    return provider_onboarding.ProviderOnboardingSpec(
        provider_slug="localcli",
        provider_name="Local CLI",
        selected_transport="cli",
        binary_name="localcli-agent",
        base_flags=("--json",),
        output_format="json",
        output_envelope_key="result",
        default_timeout=900,
        model_flag="--model",
        default_model="localcli-1",
        requested_models=("localcli-1",),
        api_key_env_vars=("LOCALCLI_API_KEY",),
        provider_api_key="localcli-test-key",
        cli_prompt_mode="argv",
        adapter_economics={"cli_llm": {"billing_mode": "subscription_included"}},
    )


def test_provider_onboarding_model_discovery_uses_secret_resolution(monkeypatch) -> None:
    captured: dict[str, object] = {}
    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="cursor",
        selected_transport="api",
        requested_models=("auto",),
        api_endpoint="https://api.cursor.com/v0/agents",
        api_protocol_family="cursor_background_agent",
        api_key_env_vars=("CURSOR_API_KEY",),
    )

    monkeypatch.setattr(
        provider_onboarding_probe,
        "resolve_secret",
        lambda env_var, env=None: "cursor-secret-key" if env_var == "CURSOR_API_KEY" else None,
    )
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_http_get_json",
        lambda url, headers, timeout_seconds: (
            captured.update(
                {"url": url, "headers": headers, "timeout_seconds": timeout_seconds}
            )
            or {"models": ["auto"]}
        ),
    )

    models = provider_onboarding_probe._discover_api_models_impl(
        spec,
        env={},
        transport_details={},
    )

    assert models == ("auto",)
    assert captured["url"] == "https://api.cursor.com/v0/models"
    assert captured["headers"] == {"Authorization": "Bearer cursor-secret-key"}


def _seed_profile(provider_slug: str) -> ProviderCLIProfile | None:
    profiles = {
        "openai": ProviderCLIProfile(
            provider_slug="openai",
            binary="codex",
            default_model="gpt-4.1",
            api_endpoint="https://api.openai.com/v1/chat/completions",
            api_protocol_family="openai_chat_completions",
            api_key_env_vars=("OPENAI_API_KEY",),
            adapter_economics={
                "cli_llm": {"billing_mode": "subscription_included"},
                "llm_task": {"billing_mode": "metered_api"},
            },
            prompt_mode="stdin",
            base_flags=("exec", "-", "--json"),
            model_flag="--model",
            output_format="ndjson",
            output_envelope_key="text",
            forbidden_flags=("--full-auto",),
            default_timeout=300,
        ),
        "localcli": ProviderCLIProfile(
            provider_slug="localcli",
            binary="localcli-agent",
            default_model="localcli-1",
            api_key_env_vars=("LOCALCLI_API_KEY",),
            adapter_economics={
                "cli_llm": {"billing_mode": "subscription_included"},
            },
            prompt_mode="argv",
            base_flags=("--json",),
            model_flag="--model",
            output_format="json",
            output_envelope_key="result",
            forbidden_flags=("--workspace", "--worktree"),
            default_timeout=900,
        ),
    }
    return profiles.get(provider_slug)


@pytest.fixture(autouse=True)
def _provider_registry_seed_fixture(monkeypatch):
    monkeypatch.setattr(
        provider_onboarding.provider_registry_mod,
        "get_profile",
        lambda provider_slug: _seed_profile(provider_slug),
    )


def _install_cli_probe_stubs(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_find_binary",
        lambda _binary_name: "/usr/local/bin/codex",
    )
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_run_command",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout='{"result":"PROVIDER_WIZARD_OK"}',
            stderr="",
        ),
    )


def test_provider_onboarding_service_probes_openai_cli_and_writes_registry_rows(monkeypatch) -> None:
    fake_conn = _FakeConn()

    async def _fake_connect(_database_url: str):
        return fake_conn

    async def _fake_probe_benchmark(conn, *, spec, models):
        del conn, spec, models
        return (
            provider_onboarding.ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="succeeded",
                summary="benchmarked",
                details={"source": "artificial_analysis"},
            ),
            {
                "ok": True,
                "source": "artificial_analysis",
                "plans": [],
                "_plan": [],
                "_source_config": {},
            },
        )

    async def _fake_apply_benchmark_plan(conn, *, spec, benchmark_report):
        del conn, spec, benchmark_report
        return ([{"model_slug": "gpt-4.1", "match_kind": "source_unavailable"}], 0)

    async def _fake_verification_report(*, conn, spec, decision_ref):
        del conn, decision_ref
        return {
            "provider_report": {
                "binary_found": True,
                "binary": "codex",
                "default_model": spec.default_model,
            },
            "transport": {
                "cli_llm": {"supported": True, "status": "ok", "message": "ready", "details": {}},
                "llm_task": {"supported": True, "status": "ok", "message": "ready", "details": {}},
            },
            "model_visibility": {
                "count": 1,
                "models": [{"provider_slug": "openai", "model_slug": "gpt-4.1"}],
            },
            "model_profiles": {
                "count": 1,
                "profiles": [{"model_profile_id": "model_profile.provider-onboarding.openai.gpt-4-1"}],
            },
            "selected_transport_supported": True,
        }

    monkeypatch.setattr(provider_onboarding.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "reload_from_db", lambda: None)
    _install_cli_probe_stubs(monkeypatch)
    monkeypatch.setattr(provider_onboarding_report, "_probe_benchmark_impl", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding_report, "_apply_benchmark_plan_impl", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding_report, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=_openai_cli_spec(),
    )

    assert result.ok is True
    assert [step.status for step in result.steps] == [
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
    ]
    assert [step.step for step in result.steps] == [
        "authority_lookup",
        "transport_probe",
        "model_probe",
        "capacity_probe",
        "benchmark_probe",
        "registry_write",
        "verification",
    ]
    assert result.model_reports[0]["candidate_ref"] == "candidate.openai.gpt-4.1"
    assert result.model_reports[0]["model_profile_id"] == (
        "model_profile.provider-onboarding.openai.gpt-4-1"
    )
    assert result.model_reports[0]["route_tier"] == "high"
    assert result.model_reports[0]["latency_class"] == "reasoning"
    assert result.model_reports[0]["cli_config"]["prompt_mode"] == "stdin"
    assert any("INSERT INTO provider_cli_profiles" in query for query, _ in fake_conn.executed)
    assert any("INSERT INTO model_profiles" in query for query, _ in fake_conn.executed)
    assert any("INSERT INTO provider_model_candidates" in query for query, _ in fake_conn.executed)
    assert any("INSERT INTO model_profile_candidate_bindings" in query for query, _ in fake_conn.executed)

    provider_profile_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_cli_profiles" in query
    )
    assert provider_profile_insert[-1] == "stdin"

    candidate_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_model_candidates" in query
    )
    task_affinities = json.loads(candidate_insert[19])
    assert task_affinities["primary"] == ["build", "review", "architecture"]
    assert "analysis" in task_affinities["secondary"]
    assert task_affinities["avoid"] == []


def test_execute_provider_onboarding_serializes_result(monkeypatch) -> None:
    expected = provider_onboarding.ProviderOnboardingResult(
        ok=True,
        provider_slug="openai",
        provider_name="Openai",
        decision_ref="decision.provider-onboarding.openai.20260409T120000Z",
        dry_run=False,
        steps=(
            provider_onboarding.ProviderOnboardingStepResult(
                step="verification",
                status="succeeded",
                summary="ok",
                details={"binary_found": True},
            ),
        ),
    )

    monkeypatch.setattr(
        provider_onboarding_execute,
        "normalize_provider_onboarding_spec",
        lambda raw: _openai_cli_spec(),
    )
    monkeypatch.setattr(
        provider_onboarding_execute,
        "run_provider_onboarding",
        lambda **kwargs: expected,
    )
    monkeypatch.setattr(
        provider_onboarding_execute,
        "_post_onboarding_sync",
        lambda **kwargs: {"native_runtime_profiles": True},
    )

    payload = provider_onboarding_execute.execute_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec={
            "provider": {
                "provider_slug": "openai",
                "selected_transport": "cli",
            }
        },
        dry_run=False,
    )

    assert payload["provider_slug"] == "openai"
    assert payload["steps"][0]["summary"] == "ok"
    assert payload["steps"][0]["details"]["binary_found"] is True
    assert payload["post_onboarding"]["native_runtime_profiles"] is True


def test_provider_onboarding_mcp_tool_uses_operation_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        provider_onboard_tool,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://example.test/workflow", "PATH": ""},
    )

    def _execute(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "provider_slug": "openai"}

    monkeypatch.setattr(provider_onboard_tool, "execute_operation_from_env", _execute)

    payload = tool_praxis_provider_onboard({"action": "probe", "provider_slug": "openai", "transport": "api"})

    assert payload == {"ok": True, "provider_slug": "openai"}
    assert captured["operation_name"] == "operator.provider_onboarding"
    assert captured["payload"] == {
        "provider_slug": "openai",
        "dry_run": True,
        "transport": "api",
    }


def test_provider_onboarding_mcp_tool_does_not_force_cli_transport(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        provider_onboard_tool,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://example.test/workflow", "PATH": ""},
    )

    def _execute(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(provider_onboard_tool, "execute_operation_from_env", _execute)

    tool_praxis_provider_onboard(
        {
            "action": "probe",
            "provider_slug": "cursor",
        }
    )

    assert captured["payload"] == {"provider_slug": "cursor", "dry_run": True}


def test_provider_onboarding_resolve_spec_infers_single_declared_api_transport(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding.provider_registry_mod,
        "get_profile",
        lambda provider_slug: ProviderCLIProfile(
            provider_slug="cursor",
            binary="cursor-api",
            default_model="auto",
            api_endpoint="https://api.cursor.com/v0/agents",
            api_protocol_family="cursor_background_agent",
            api_key_env_vars=("CURSOR_API_KEY",),
            adapter_economics={"llm_task": {"billing_mode": "subscription_included"}},
            lane_policies={"llm_task": {"admitted_by_policy": True}},
        )
        if provider_slug == "cursor"
        else None,
    )

    resolved, template, transport_template, authority_step = provider_onboarding._resolve_spec(
        provider_onboarding.ProviderOnboardingSpec(provider_slug="cursor")
    )

    assert resolved.selected_transport == "api"
    assert sorted(template.transports) == ["api"]
    assert transport_template.transport == "api"
    assert authority_step.details["supported_transports"] == ["api"]


def test_provider_onboarding_resolve_spec_uses_openrouter_api_authority(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        provider_onboarding.provider_registry_mod,
        "get_profile",
        lambda provider_slug: ProviderCLIProfile(
            provider_slug="openrouter",
            binary="openrouter-api",
            default_model="openrouter/auto",
            api_endpoint="https://openrouter.ai/api/v1/chat/completions",
            api_protocol_family="openai_chat_completions",
            api_key_env_vars=("OPENROUTER_API_KEY",),
            adapter_economics={"llm_task": {"billing_mode": "metered_api"}},
            lane_policies={"llm_task": {"admitted_by_policy": True}},
        )
        if provider_slug == "openrouter"
        else None,
    )

    resolved, template, transport_template, authority_step = provider_onboarding._resolve_spec(
        provider_onboarding.ProviderOnboardingSpec(provider_slug="openrouter")
    )

    assert resolved.selected_transport == "api"
    assert resolved.default_model == "openrouter/auto"
    assert resolved.api_endpoint == "https://openrouter.ai/api/v1/chat/completions"
    assert resolved.api_key_env_vars == ("OPENROUTER_API_KEY",)
    assert sorted(template.transports) == ["api"]
    assert transport_template.api_protocol_family == "openai_chat_completions"
    assert authority_step.details["supported_transports"] == ["api"]
    assert "DEEPSEEK_API_KEY" not in authority_step.details["api_key_env_vars"]


def test_post_onboarding_sync_updates_native_runtime_allowed_models(monkeypatch) -> None:
    class _FakeSyncConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *params: object):
            self.calls.append((query, params))
            if "FROM registry_native_runtime_profile_authority" in query:
                return [{"runtime_profile_ref": "praxis", "allowed_models": '["gpt-5.4"]'}]
            return []

    fake_sync_conn = _FakeSyncConn()

    monkeypatch.setattr(
        "storage.postgres.connection.SyncPostgresConnection",
        lambda pool: fake_sync_conn,
    )
    monkeypatch.setattr(
        "storage.postgres.connection.get_workflow_pool",
        lambda env=None: object(),
    )

    result = provider_onboarding_execute._post_onboarding_sync(
        database_url="postgresql://example.test/workflow",
        provider_slug="openai",
        model_reports=({"model_slug": "gpt-5.4-mini"},),
    )

    assert result["native_runtime_profiles"] is True
    assert result["updated_runtime_profile_refs"] == ["praxis"]
    assert result["added_to_allowed_models"] == ["gpt-5.4-mini"]
    assert any(
        "UPDATE registry_native_runtime_profile_authority" in query
        for query, _ in fake_sync_conn.calls
    )


def test_provider_onboarding_warns_when_benchmark_key_is_missing(monkeypatch) -> None:
    fake_conn = _FakeConn()

    async def _fake_connect(_database_url: str):
        return fake_conn

    async def _fake_verification_report(*, conn, spec, decision_ref):
        del conn, spec, decision_ref
        return {
            "provider_report": {"binary_found": True},
            "transport": {},
            "model_visibility": {"count": 1, "models": []},
            "model_profiles": {"count": 1, "profiles": []},
            "selected_transport_supported": True,
        }

    monkeypatch.setattr(provider_onboarding.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "reload_from_db", lambda: None)
    monkeypatch.delenv("ARTIFICIAL_ANALYSIS_API_KEY", raising=False)
    _install_cli_probe_stubs(monkeypatch)
    monkeypatch.setattr(provider_onboarding_report, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=_openai_cli_spec(benchmark_api_key=None),
    )

    assert result.ok is True
    assert result.steps[4].status == "warning"
    assert "Go get a key" in result.steps[4].summary
    assert "ARTIFICIAL_ANALYSIS_API_KEY" in result.steps[4].summary


def test_provider_onboarding_service_probes_openai_api_and_writes_registry_rows(monkeypatch) -> None:
    from adapters import llm_client as llm_client_mod

    fake_conn = _FakeConn()

    async def _fake_connect(_database_url: str):
        return fake_conn

    async def _fake_probe_benchmark(conn, *, spec, models):
        del conn, spec, models
        return (
            provider_onboarding.ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="succeeded",
                summary="benchmarked",
                details={"source": "artificial_analysis"},
            ),
            {
                "ok": True,
                "source": "artificial_analysis",
                "plans": [],
                "_plan": [],
                "_source_config": {},
            },
        )

    async def _fake_apply_benchmark_plan(conn, *, spec, benchmark_report):
        del conn, spec, benchmark_report
        return ([], 0)

    async def _fake_verification_report(*, conn, spec, decision_ref):
        del conn, decision_ref
        return {
            "provider_report": {
                "default_model": spec.default_model,
                "api_endpoint": spec.api_endpoint,
                "api_protocol_family": spec.api_protocol_family,
            },
            "transport": {
                "cli_llm": {"supported": True, "status": "ok", "message": "ready", "details": {}},
                "llm_task": {"supported": True, "status": "ok", "message": "ready", "details": {}},
            },
            "model_visibility": {
                "count": 1,
                "models": [{"provider_slug": "openai", "model_slug": "gpt-4.1"}],
            },
            "model_profiles": {
                "count": 1,
                "profiles": [{"model_profile_id": "model_profile.provider-onboarding.openai.gpt-4-1"}],
            },
            "selected_transport_supported": True,
        }

    monkeypatch.setattr(provider_onboarding.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "reload_from_db", lambda: None)
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_discover_api_models",
        lambda spec, *, env, transport_details: ("gpt-4.1", "gpt-4.1-mini"),
    )
    monkeypatch.setattr(
        llm_client_mod,
        "call_llm",
        lambda request: llm_client_mod.LLMResponse(
            content="PROVIDER_WIZARD_OK",
            model=request.model_slug,
            provider_slug=request.provider_slug,
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            raw_response={},
            latency_ms=25,
            status_code=200,
        ),
    )
    monkeypatch.setattr(provider_onboarding_report, "_probe_benchmark_impl", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding_report, "_apply_benchmark_plan_impl", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding_report, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=_openai_api_spec(),
    )

    assert result.ok is True
    assert [step.status for step in result.steps] == [
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
    ]
    assert result.steps[2].details["discovered_models"] == ["gpt-4.1", "gpt-4.1-mini"]
    assert result.steps[3].details["api_protocol_family"] == "openai_chat_completions"
    assert result.model_reports[0]["candidate_ref"] == "candidate.openai.gpt-4.1"
    assert result.model_reports[0]["cli_config"] == {}

    provider_profile_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_cli_profiles" in query
    )
    assert provider_profile_insert[-1] == "stdin"

    candidate_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_model_candidates" in query
    )
    assert json.loads(candidate_insert[13]) == {}


def test_provider_onboarding_falls_back_to_argv_prompt_mode_when_stdin_probe_fails(monkeypatch) -> None:
    fake_conn = _FakeConn()

    async def _fake_connect(_database_url: str):
        return fake_conn

    async def _fake_probe_benchmark(conn, *, spec, models):
        del conn, spec, models
        return (
            provider_onboarding.ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="succeeded",
                summary="benchmarked",
                details={"source": "artificial_analysis"},
            ),
            {
                "ok": True,
                "source": "artificial_analysis",
                "plans": [],
                "_plan": [],
                "_source_config": {},
            },
        )

    async def _fake_apply_benchmark_plan(conn, *, spec, benchmark_report):
        del conn, spec, benchmark_report
        return ([], 0)

    async def _fake_verification_report(*, conn, spec, decision_ref):
        del conn, decision_ref
        return {
            "provider_report": {"binary_found": True, "default_model": spec.default_model},
            "transport": {
                "cli_llm": {"supported": True, "status": "ok", "message": "ready", "details": {}},
                "llm_task": {"supported": False, "status": "warning", "message": "unsupported", "details": {}},
            },
            "model_visibility": {"count": 1, "models": []},
            "model_profiles": {"count": 1, "profiles": []},
            "selected_transport_supported": True,
        }

    def _fake_run_command(cmd, *, env, input_text=None, timeout_seconds):
        del env, timeout_seconds
        if input_text is not None:
            return SimpleNamespace(returncode=1, stdout="", stderr="stdin not supported")
        assert cmd[-1] == "Reply with exactly PROVIDER_WIZARD_OK."
        return SimpleNamespace(returncode=0, stdout='{"result":"PROVIDER_WIZARD_OK"}', stderr="")

    monkeypatch.setattr(provider_onboarding.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "reload_from_db", lambda: None)
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_find_binary",
        lambda _binary_name: "/usr/local/bin/codex",
    )
    monkeypatch.setattr(provider_onboarding_probe, "_run_command", _fake_run_command)
    monkeypatch.setattr(provider_onboarding_report, "_probe_benchmark_impl", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding_report, "_apply_benchmark_plan_impl", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding_report, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=provider_onboarding.ProviderOnboardingSpec(
            provider_slug="openai",
            selected_transport="cli",
            requested_models=("gpt-4.1",),
            cli_prompt_mode=None,
            benchmark_source_slug="artificial_analysis",
            benchmark_api_key="aa-test-key",
        ),
    )

    assert result.ok is True
    assert result.steps[3].details["prompt_mode"] == "argv"
    assert result.model_reports[0]["cli_config"]["prompt_mode"] == "argv"
    provider_profile_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_cli_profiles" in query
    )
    assert provider_profile_insert[-1] == "argv"


def test_provider_onboarding_provisions_cli_registry_rows_when_capacity_probe_fails(monkeypatch) -> None:
    fake_conn = _FakeConn()

    async def _fake_connect(_database_url: str):
        return fake_conn

    def _fake_probe_transport(spec, transport_template):
        del transport_template
        return (
            provider_onboarding.ProviderOnboardingStepResult(
                step="transport_probe",
                status="succeeded",
                summary="cli ready",
                details={
                    "selected_transport": spec.selected_transport,
                    "binary_path": "/Users/praxis/.local/bin/localcli-agent",
                    "credential_source": "ambient_cli_session",
                },
            ),
            {},
        )

    def _fake_probe_models(spec, transport_template, *, env, transport_details):
        del spec, transport_template, env, transport_details
        return (
            provider_onboarding.ProviderOnboardingStepResult(
                step="model_probe",
                status="succeeded",
                summary="models discovered",
                details={
                    "selected_models": ["localcli-1"],
                    "default_model": "localcli-1",
                },
            ),
            (
                provider_onboarding.ProviderOnboardingModelSpec(
                    model_slug="localcli-1",
                    route_tier="high",
                    route_tier_rank=1,
                    latency_class="reasoning",
                    latency_rank=1,
                    context_window=200_000,
                ),
            ),
        )

    def _fake_probe_capacity(spec, transport_template, *, env, transport_details, models):
        del spec, transport_template, env, transport_details, models
        return provider_onboarding.ProviderOnboardingStepResult(
            step="capacity_probe",
            status="failed",
            summary="Prompt probe did not complete successfully for localcli/localcli-1",
            details={
                "selected_transport": "cli",
                "default_model": "localcli-1",
                "attempts": [
                    {
                        "prompt_mode": "argv",
                        "success": False,
                        "stderr_excerpt": "Authentication required.",
                    }
                ],
            },
        )

    async def _fake_verification_report(*, conn, spec, decision_ref):
        del conn, decision_ref
        return {
            "provider_report": {"binary_found": True, "default_model": spec.default_model},
            "transport": {
                "cli_llm": {"supported": False, "status": "warning", "message": "not admitted", "details": {}},
                "llm_task": {"supported": False, "status": "warning", "message": "unsupported", "details": {}},
            },
            "model_visibility": {"count": 1, "models": []},
            "model_profiles": {"count": 1, "profiles": []},
            "selected_transport_supported": False,
        }

    monkeypatch.setattr(provider_onboarding.asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "reload_from_db", lambda: None)
    monkeypatch.setattr(provider_onboarding_probe, "_probe_transport", _fake_probe_transport)
    monkeypatch.setattr(provider_onboarding_probe, "_probe_models", _fake_probe_models)
    monkeypatch.setattr(provider_onboarding_probe, "_probe_capacity", _fake_probe_capacity)
    monkeypatch.setattr(
        provider_onboarding_report,
        "_probe_benchmark_impl",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("benchmark should stay skipped")),
    )
    monkeypatch.setattr(provider_onboarding_report, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=_localcli_cli_spec(),
    )

    assert result.ok is False
    assert [step.step for step in result.steps] == [
        "authority_lookup",
        "transport_probe",
        "model_probe",
        "capacity_probe",
        "benchmark_probe",
        "registry_write",
        "verification",
    ]
    assert result.steps[4].status == "skipped"
    assert result.steps[5].status == "warning"
    assert result.steps[6].status == "warning"
    assert "lane admission remains disabled" in result.steps[5].summary
    assert "not admitted yet" in result.steps[6].summary
    assert any("INSERT INTO provider_cli_profiles" in query for query, _ in fake_conn.executed)
    assert any("INSERT INTO provider_model_candidates" in query for query, _ in fake_conn.executed)
    assert any("INSERT INTO provider_transport_admissions" in query for query, _ in fake_conn.executed)

    provider_profile_insert = next(
        params
        for query, params in fake_conn.executed
        if "INSERT INTO provider_cli_profiles" in query
    )
    assert provider_profile_insert[0] == "localcli"
    assert provider_profile_insert[-1] == "argv"


def test_registry_backed_cli_template_exposes_local_cli_contract(monkeypatch) -> None:
    localcli_profile = _seed_profile("localcli")
    assert localcli_profile is not None
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "get_profile", lambda _slug: localcli_profile)

    template = provider_onboarding._provider_template("localcli")

    assert template.provider_slug == "localcli"
    assert template.adapter_economics["cli_llm"]["billing_mode"] == "subscription_included"
    assert template.transports["cli"].supported is True
    assert template.transports["cli"].binary_name == "localcli-agent"
    assert template.transports["cli"].base_flags == ("--json",)
    assert template.transports["cli"].cli_prompt_modes == ("argv", "stdin")
    assert "api" not in template.transports


def test_provider_template_requires_explicit_registry_profile(monkeypatch) -> None:
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "get_profile", lambda _slug: None)

    with pytest.raises(ValueError, match="seed provider_cli_profiles in Postgres or provide explicit transport fields"):
        provider_onboarding._provider_template("localcli")


def test_provider_template_can_be_derived_from_explicit_cli_payload(monkeypatch) -> None:
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "get_profile", lambda _slug: None)

    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="examplecli",
        provider_name="Example CLI",
        selected_transport="cli",
        binary_name="examplecli-agent",
        base_flags=("--json",),
        output_format="json",
        output_envelope_key="result",
        default_timeout=900,
        model_flag="--model",
        forbidden_flags=("--workspace",),
        default_model="examplecli-1",
        requested_models=("examplecli-1",),
        api_key_env_vars=("EXAMPLECLI_API_KEY",),
        cli_prompt_mode="argv",
        adapter_economics={"cli_llm": {"billing_mode": "subscription_included"}},
    )

    template = provider_onboarding._provider_template("examplecli", explicit_spec=spec)

    assert template.provider_slug == "examplecli"
    assert template.provider_name == "Example CLI"
    assert template.transports["cli"].binary_name == "examplecli-agent"
    assert template.transports["cli"].cli_prompt_modes == ("argv", "stdin")
    assert "api" not in template.transports


def test_native_operator_provider_onboard_cli_uses_operation_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        native_operator,
        "load_provider_onboarding_spec_from_file",
        lambda _path: _openai_cli_spec(),
    )

    def _execute(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"provider_slug": "openai", "dry_run": True, "steps": [{"status": "planned"}]}

    monkeypatch.setattr(
        native_operator.operation_catalog_gateway,
        "execute_operation_from_env",
        _execute,
    )

    stdout = StringIO()
    exit_code = native_operator.main(
        ["provider-onboard", "provider-onboard-spec.json", "--dry-run"],
        env={"WORKFLOW_DATABASE_URL": "postgresql://example.test/workflow"},
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["provider_slug"] == "openai"
    assert payload["dry_run"] is True
    assert payload["steps"][0]["status"] == "planned"
    assert captured["operation_name"] == "operator.provider_onboarding"
    assert captured["payload"]["spec"]["provider_slug"] == "openai"


def test_discover_api_models_parses_openai_model_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_http_get_json",
        lambda url, *, headers, timeout_seconds: {
            "data": [
                {"id": "gpt-4.1"},
                {"id": "gpt-4.1-mini"},
            ]
        },
    )

    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="openai",
        selected_transport="api",
        api_protocol_family="openai_chat_completions",
        api_key_env_vars=("OPENAI_API_KEY",),
        default_timeout=30,
    )

    models = provider_onboarding_probe._discover_api_models(
        spec,
        env={"OPENAI_API_KEY": "test-key"},
        transport_details={"discovery_strategy": "openai_models_list"},
    )

    assert models == ("gpt-4.1", "gpt-4.1-mini")


def test_discover_api_models_filters_google_generate_content_models(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_http_get_json",
        lambda url, *, headers, timeout_seconds: {
            "models": [
                {
                    "name": "models/gemini-2.5-flash",
                    "baseModelId": "gemini-2.5-flash",
                    "supportedGenerationMethods": ["generateContent"],
                },
                {
                    "name": "models/text-embedding-004",
                    "baseModelId": "text-embedding-004",
                    "supportedGenerationMethods": ["embedContent"],
                },
            ]
        },
    )

    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="google",
        selected_transport="api",
        api_protocol_family="google_generate_content",
        api_key_env_vars=("GEMINI_API_KEY",),
        default_timeout=30,
    )

    models = provider_onboarding_probe._discover_api_models(
        spec,
        env={"GEMINI_API_KEY": "test-key"},
        transport_details={"discovery_strategy": "google_models_list"},
    )

    assert models == ("gemini-2.5-flash",)


def test_discover_api_models_lists_cursor_background_agent_models(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding_probe,
        "_http_get_json",
        lambda url, *, headers, timeout_seconds: {
            "models": ["claude-4-sonnet-thinking", "o3", "claude-4-opus-thinking"]
        },
    )

    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="cursor",
        selected_transport="api",
        api_protocol_family="cursor_background_agent",
        api_key_env_vars=("CURSOR_API_KEY",),
        default_timeout=30,
    )

    models = provider_onboarding_probe._discover_api_models(
        spec,
        env={"CURSOR_API_KEY": "test-key"},
        transport_details={"discovery_strategy": "cursor_models_list"},
    )

    assert models == (
        "claude-4-sonnet-thinking",
        "o3",
        "claude-4-opus-thinking",
    )


def test_probe_capacity_succeeds_for_cursor_background_agent_auth_probe() -> None:
    spec = provider_onboarding.ProviderOnboardingSpec(
        provider_slug="cursor",
        selected_transport="api",
        default_model="auto",
        api_endpoint="https://api.cursor.com/v0/agents",
        api_protocol_family="cursor_background_agent",
        api_key_env_vars=("CURSOR_API_KEY",),
        adapter_economics={"llm_task": {"billing_mode": "subscription_included"}},
    )
    transport_template = provider_onboarding.ProviderTransportAuthorityTemplate(
        transport="api",
        supported=True,
        api_endpoint="https://api.cursor.com/v0/agents",
        api_protocol_family="cursor_background_agent",
        api_key_env_vars=("CURSOR_API_KEY",),
        default_model="auto",
        discovery_strategy="cursor_models_list",
        prompt_probe_strategy="api_model_discovery_auth_probe",
    )

    result = provider_onboarding._probe_capacity(
        spec,
        transport_template,
        env={"CURSOR_API_KEY": "cursor-test-key"},
        transport_details={"discovery_strategy": "cursor_models_list"},
        models=(provider_onboarding.ProviderOnboardingModelSpec(model_slug="auto"),),
    )

    assert result.status == "succeeded"
    assert result.details["probe_strategy"] == "api_model_discovery_auth_probe"
