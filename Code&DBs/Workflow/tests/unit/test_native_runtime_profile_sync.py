from __future__ import annotations

from types import SimpleNamespace

from registry.native_runtime_profile_sync import (
    NativeRuntimeProfileConfig,
    _default_live_budget_window,
    _latest_budget_window_sync,
    _upsert_profile_authority_rows_sync,
)
from registry.runtime_profile_admission import _effective_provider_policy_name
from registry.runtime_profile_admission import _candidate_is_admitted_for_runtime_profile


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args: object) -> list[object]:
        self.calls.append((query, args))
        return []


def test_upsert_profile_authority_rows_sync_writes_multi_provider_authority_refs() -> None:
    conn = _FakeConn()
    config = NativeRuntimeProfileConfig(
        runtime_profile_ref="praxis",
        workspace_ref="praxis",
        model_profile_id="model_profile.praxis.default",
        provider_policy_id="provider_policy.praxis.default",
        provider_name="openai",
        provider_names=("openai", "anthropic", "google"),
        allowed_models=(
            "gpt-5.4",
            "claude-opus-4-6",
            "gemini-2.0-flash",
        ),
        repo_root=".",
        workdir=".",
    )
    candidates = (
        SimpleNamespace(provider_ref="provider.openai", provider_name="openai"),
        SimpleNamespace(provider_ref="provider.anthropic", provider_name="anthropic"),
        SimpleNamespace(provider_ref="provider.google", provider_name="google"),
    )

    _upsert_profile_authority_rows_sync(conn, config, candidates)

    assert len(conn.calls) == 2
    _, model_profile_args = conn.calls[0]
    _, provider_policy_args = conn.calls[1]

    assert model_profile_args[2] == "openai"
    assert provider_policy_args[2] == "openai"
    assert provider_policy_args[3] == '["provider.openai", "provider.anthropic", "provider.google"]'
    assert provider_policy_args[4] == "provider.openai"


def test_effective_provider_policy_name_defers_to_provider_refs_when_present() -> None:
    assert (
        _effective_provider_policy_name(
            runtime_profile_ref="praxis",
            provider_name="openai",
            allowed_provider_refs=("provider.openai", "provider.anthropic"),
        )
        is None
    )


def test_native_transport_ready_degraded_candidate_is_admitted_for_native_profile() -> None:
    assert _candidate_is_admitted_for_runtime_profile(
        runtime_profile_ref="praxis",
        eligibility_status="rejected",
        reason_code="provider_route_authority.health_degraded",
        source_window_refs=["transport:cli_llm", "binary:/usr/local/bin/codex"],
    ) is True
    assert _candidate_is_admitted_for_runtime_profile(
        runtime_profile_ref="praxis",
        eligibility_status="rejected",
        reason_code="provider_disabled",
        source_window_refs=["transport:cli_llm", "binary:/usr/local/bin/codex"],
    ) is False


def test_latest_budget_window_sync_synthesizes_default_when_missing() -> None:
    conn = _FakeConn()
    config = NativeRuntimeProfileConfig(
        runtime_profile_ref="praxis",
        workspace_ref="praxis",
        model_profile_id="model_profile.praxis.default",
        provider_policy_id="provider_policy.praxis.default",
        provider_name="openai",
        provider_names=("openai",),
        allowed_models=("gpt-5.4",),
        repo_root=".",
        workdir=".",
    )
    candidates = (
        SimpleNamespace(provider_ref="provider.openai", provider_name="openai"),
    )

    budget = _latest_budget_window_sync(conn, config, candidates=candidates)

    assert budget.provider_ref == "provider.openai"
    assert budget.budget_scope == "runtime"
    assert budget.budget_status == "available"
    assert budget.requests_used == 0
    assert budget.tokens_used == 0
    assert budget.spend_used_usd == "0.000000"


def test_default_live_budget_window_falls_back_to_provider_name() -> None:
    config = NativeRuntimeProfileConfig(
        runtime_profile_ref="praxis",
        workspace_ref="praxis",
        model_profile_id="model_profile.praxis.default",
        provider_policy_id="provider_policy.praxis.default",
        provider_name="openai",
        provider_names=("openai",),
        allowed_models=("gpt-5.4",),
        repo_root=".",
        workdir=".",
    )

    budget = _default_live_budget_window(config)

    assert budget.provider_ref == "provider.openai"
    assert budget.budget_scope == "runtime"
    assert budget.budget_status == "available"
