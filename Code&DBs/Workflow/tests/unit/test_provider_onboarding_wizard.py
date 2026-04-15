from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace

import pytest
from adapters import provider_transport

from registry import provider_onboarding
from surfaces.api.handlers import workflow_admin
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


def _cursor_cli_spec() -> provider_onboarding.ProviderOnboardingSpec:
    return provider_onboarding.ProviderOnboardingSpec(
        provider_slug="cursor",
        selected_transport="cli",
        requested_models=("composer-2",),
        provider_api_key="crsr-test-key",
    )


def _builtin_profile(provider_slug: str):
    return next(
        (
            profile
            for profile in provider_transport.BUILTIN_PROVIDER_PROFILES
            if profile.provider_slug == provider_slug
        ),
        None,
    )


@pytest.fixture(autouse=True)
def _provider_registry_seed_fixture(monkeypatch):
    monkeypatch.setattr(
        provider_onboarding.provider_registry_mod,
        "get_profile",
        lambda provider_slug: _builtin_profile(provider_slug),
    )


def _install_cli_probe_stubs(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding,
        "_find_binary",
        lambda _binary_name: "/usr/local/bin/codex",
    )
    monkeypatch.setattr(
        provider_onboarding,
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
    monkeypatch.setattr(provider_onboarding, "_probe_benchmark", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding, "_apply_benchmark_plan", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding, "_verification_report", _fake_verification_report)

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


def test_provider_onboarding_handler_serializes_result(monkeypatch) -> None:
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
        provider_onboarding,
        "normalize_provider_onboarding_spec",
        lambda raw: _openai_cli_spec(),
    )
    monkeypatch.setattr(
        provider_onboarding,
        "run_provider_onboarding",
        lambda **kwargs: expected,
    )
    monkeypatch.setattr(
        workflow_admin,
        "resolve_workflow_database_url",
        lambda: "postgresql://example.test/workflow",
    )

    payload = workflow_admin._handle_provider_onboarding_post(
        SimpleNamespace(),
        {
            "spec": {
                "provider": {
                    "provider_slug": "openai",
                    "selected_transport": "cli",
                }
            },
            "dry_run": False,
        },
    )

    assert payload["provider_slug"] == "openai"
    assert payload["steps"][0]["summary"] == "ok"
    assert payload["steps"][0]["details"]["binary_found"] is True


def test_provider_onboarding_mcp_tool_serializes_slots_dataclass(monkeypatch) -> None:
    expected = provider_onboarding.ProviderOnboardingResult(
        ok=True,
        provider_slug="openai",
        provider_name="Openai",
        decision_ref="decision.provider-onboarding.openai.20260409T120000Z",
        dry_run=True,
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
        provider_onboarding,
        "normalize_provider_onboarding_spec",
        lambda raw: _openai_api_spec(),
    )
    monkeypatch.setattr(
        provider_onboarding,
        "run_provider_onboarding",
        lambda **kwargs: expected,
    )
    monkeypatch.setattr(
        "surfaces.mcp.tools.provider_onboard.resolve_workflow_database_url",
        lambda env=None: "postgresql://example.test/workflow",
    )
    monkeypatch.setattr(
        "surfaces.mcp.tools.provider_onboard.workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://example.test/workflow", "PATH": ""},
    )

    payload = tool_praxis_provider_onboard(
        {
            "action": "probe",
            "provider_slug": "openai",
            "transport": "api",
        }
    )

    assert payload["provider_slug"] == "openai"
    assert payload["steps"][0]["summary"] == "ok"
    assert payload["steps"][0]["details"]["binary_found"] is True


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
    monkeypatch.setattr(provider_onboarding, "_verification_report", _fake_verification_report)

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
        provider_onboarding,
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
    monkeypatch.setattr(provider_onboarding, "_probe_benchmark", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding, "_apply_benchmark_plan", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding, "_verification_report", _fake_verification_report)

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
        provider_onboarding,
        "_find_binary",
        lambda _binary_name: "/usr/local/bin/codex",
    )
    monkeypatch.setattr(provider_onboarding, "_run_command", _fake_run_command)
    monkeypatch.setattr(provider_onboarding, "_probe_benchmark", _fake_probe_benchmark)
    monkeypatch.setattr(provider_onboarding, "_apply_benchmark_plan", _fake_apply_benchmark_plan)
    monkeypatch.setattr(provider_onboarding, "_verification_report", _fake_verification_report)

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
                    "binary_path": "/Users/nate/.local/bin/cursor-agent",
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
                    "selected_models": ["composer-2"],
                    "default_model": "composer-2",
                },
            ),
            (
                provider_onboarding.ProviderOnboardingModelSpec(
                    model_slug="composer-2",
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
            summary="Prompt probe did not complete successfully for cursor/composer-2",
            details={
                "selected_transport": "cli",
                "default_model": "composer-2",
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
    monkeypatch.setattr(provider_onboarding, "_probe_transport", _fake_probe_transport)
    monkeypatch.setattr(provider_onboarding, "_probe_models", _fake_probe_models)
    monkeypatch.setattr(provider_onboarding, "_probe_capacity", _fake_probe_capacity)
    monkeypatch.setattr(
        provider_onboarding,
        "_probe_benchmark",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("benchmark should stay skipped")),
    )
    monkeypatch.setattr(provider_onboarding, "_verification_report", _fake_verification_report)

    result = provider_onboarding.run_provider_onboarding(
        database_url="postgresql://example.test/workflow",
        spec=_cursor_cli_spec(),
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
    assert provider_profile_insert[0] == "cursor"
    assert provider_profile_insert[-1] == "argv"


def test_cursor_template_exposes_local_cli_contract(monkeypatch) -> None:
    cursor_profile = next(
        profile
        for profile in provider_transport.BUILTIN_PROVIDER_PROFILES
        if profile.provider_slug == "cursor"
    )
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "get_profile", lambda _slug: cursor_profile)

    template = provider_onboarding._provider_template("cursor")

    assert template.provider_slug == "cursor"
    assert template.adapter_economics["cli_llm"]["billing_mode"] == "subscription_included"
    assert template.transports["cli"].supported is True
    assert template.transports["cli"].binary_name == "cursor-agent"
    assert template.transports["cli"].base_flags == (
        "--trust",
        "-p",
        "--output-format",
        "json",
        "--sandbox",
        "disabled",
    )
    assert template.transports["cli"].cli_prompt_modes == ("argv", "stdin")
    assert template.transports["api"].supported is False
    assert template.transports["api"].unsupported_reason == (
        "This provider does not expose an admitted llm_task transport in the registry yet."
    )


def test_cursor_template_requires_explicit_registry_profile(monkeypatch) -> None:
    monkeypatch.setattr(provider_onboarding.provider_registry_mod, "get_profile", lambda _slug: None)

    with pytest.raises(ValueError, match="no onboarding authority template is registered for cursor"):
        provider_onboarding._provider_template("cursor")


def test_native_operator_provider_onboard_cli_uses_shared_wizard(monkeypatch) -> None:
    expected = provider_onboarding.ProviderOnboardingResult(
        ok=True,
        provider_slug="openai",
        provider_name="Openai",
        decision_ref="decision.provider-onboarding.openai.20260409T120000Z",
        dry_run=True,
        steps=(
            provider_onboarding.ProviderOnboardingStepResult(
                step="verification",
                status="planned",
                summary="planned",
                details={},
            ),
        ),
    )

    monkeypatch.setattr(
        native_operator,
        "load_provider_onboarding_spec_from_file",
        lambda _path: _openai_cli_spec(),
    )
    monkeypatch.setattr(
        native_operator,
        "run_provider_onboarding",
        lambda **kwargs: expected,
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


def test_discover_api_models_parses_openai_model_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding,
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

    models = provider_onboarding._discover_api_models(
        spec,
        env={"OPENAI_API_KEY": "test-key"},
        transport_details={"discovery_strategy": "openai_models_list"},
    )

    assert models == ("gpt-4.1", "gpt-4.1-mini")


def test_discover_api_models_filters_google_generate_content_models(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_onboarding,
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

    models = provider_onboarding._discover_api_models(
        spec,
        env={"GEMINI_API_KEY": "test-key"},
        transport_details={"discovery_strategy": "google_models_list"},
    )

    assert models == ("gemini-2.5-flash",)
