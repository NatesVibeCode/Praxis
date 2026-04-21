from __future__ import annotations

from types import SimpleNamespace

from registry.native_runtime_profile_sync import (
    NativeRuntimeProfileConfig,
    _default_live_budget_window,
    _default_sync_conn,
    _latest_budget_window_sync,
    _native_transport_ready_refs,
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


def _config(**overrides) -> NativeRuntimeProfileConfig:
    values = {
        "runtime_profile_ref": "praxis",
        "workspace_ref": "praxis",
        "sandbox_profile_ref": "sandbox_profile.praxis.default",
        "model_profile_id": "model_profile.praxis.default",
        "provider_policy_id": "provider_policy.praxis.default",
        "provider_name": "openai",
        "provider_names": ("openai",),
        "allowed_models": ("gpt-5.4",),
        "repo_root": ".",
        "workdir": ".",
        "instance_name": "praxis",
        "receipts_dir": "./artifacts/runtime_receipts",
        "topology_dir": "./artifacts/runtime_topology",
    }
    values.update(overrides)
    return NativeRuntimeProfileConfig(**values)


def test_upsert_profile_authority_rows_sync_writes_multi_provider_authority_refs() -> None:
    conn = _FakeConn()
    config = _config(
        provider_names=("openai", "anthropic", "google"),
        allowed_models=(
            "gpt-5.4",
            "claude-opus-4-6",
            "gemini-2.0-flash",
        ),
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


def test_native_transport_ready_degraded_candidate_is_admitted_for_native_profile(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "registry.runtime_profile_admission.is_native_runtime_profile_ref",
        lambda runtime_profile_ref: runtime_profile_ref == "praxis",
    )
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
    config = _config()
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
    config = _config()

    budget = _default_live_budget_window(config)

    assert budget.provider_ref == "provider.openai"
    assert budget.budget_scope == "runtime"
    assert budget.budget_status == "available"


def test_default_sync_conn_uses_runtime_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.resolve_runtime_database_url",
        lambda repo_root=None, required=True: "postgresql://127.0.0.1:5432/praxis",
    )
    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.ensure_postgres_available",
        lambda env=None: captured.setdefault("env", env) or object(),
    )

    conn = _default_sync_conn()

    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://127.0.0.1:5432/praxis",
    }
    assert conn == captured["env"]


def test_native_transport_ready_refs_uses_canonical_secret_resolver(monkeypatch) -> None:
    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.resolve_default_adapter_type",
        lambda provider_slug: "llm_task",
    )
    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.supports_adapter",
        lambda provider_slug, adapter_type: True,
    )
    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.get_profile",
        lambda provider_slug: SimpleNamespace(
            api_endpoint="https://api.openai.com/v1",
            api_protocol_family="openai_responses",
            api_key_env_vars=("OPENAI_API_KEY",),
        ),
    )
    monkeypatch.setattr(
        "registry.native_runtime_profile_sync.resolve_secret",
        lambda env_name, env=None: (
            "resolved-from-keychain" if env_name == "OPENAI_API_KEY" else None
        ),
    )

    assert _native_transport_ready_refs("openai") == (
        "transport:llm_task",
        "env:OPENAI_API_KEY",
    )
