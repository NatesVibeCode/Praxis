from __future__ import annotations

from types import SimpleNamespace

import pytest

import adapters.provider_registry as provider_registry_mod
from adapters.provider_types import ProviderCLIProfile
from surfaces.mcp.tools import health as health_tool
from surfaces.api.handlers import workflow_admin


class _FakeProbe:
    def __init__(self, payload):
        self.payload = payload


class _CapturingPreflightRunner:
    def __init__(self, probes):
        self.probes = probes

    def run(self):
        return SimpleNamespace(
            overall=SimpleNamespace(value="healthy"),
            checks=[
                SimpleNamespace(
                    name="postgres",
                    passed=True,
                    message="ok",
                    duration_ms=0.1,
                    status="ok",
                    details={},
                )
            ],
            timestamp=SimpleNamespace(isoformat=lambda: "2026-04-17T00:00:00+00:00"),
        )


def _transport_support_payload() -> dict[str, object]:
    return {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
        "providers": [
            {
                "provider_slug": "openai",
                "transports": {
                    "cli_llm": {"supported": True},
                    "llm_task": {"supported": True},
                },
            },
            {
                "provider_slug": "google",
                "transports": {
                    "cli_llm": {"supported": True},
                    "llm_task": {"supported": False},
                },
            },
        ],
    }


def test_transport_support_report_delegates_to_canonical_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeRepository:
        def __init__(self, conn) -> None:
            captured["repository_conn"] = conn

    class _FakeAuthority:
        def to_json(self) -> dict[str, object]:
            return {"status": "ready", "count": {"providers": 2}}

    def _fake_load_transport_eligibility_authority(**kwargs):
        captured.update(kwargs)
        return _FakeAuthority()

    monkeypatch.setattr(
        "storage.postgres.PostgresTransportEligibilityRepository",
        _FakeRepository,
        raising=False,
    )
    monkeypatch.setattr(
        "authority.transport_eligibility.load_transport_eligibility_authority",
        _fake_load_transport_eligibility_authority,
        raising=False,
    )

    payload = provider_registry_mod.transport_support_report(
        health_mod=object(),
        pg="pg-conn",
        provider_filter="openai",
        model_filter="gpt-5.4",
        runtime_profile_ref="native",
        jobs=[{"label": "verify", "agent": "auto/review"}],
    )

    assert payload == {"status": "ready", "count": {"providers": 2}}
    assert captured["repository_conn"] == "pg-conn"
    assert captured["provider_filter"] == "openai"
    assert captured["model_filter"] == "gpt-5.4"
    assert captured["runtime_profile_ref"] == "native"
    assert captured["jobs"] == [{"label": "verify", "agent": "auto/review"}]
    assert captured["provider_registry_mod"].__name__ == "registry.provider_execution_registry"


def test_default_provider_slug_raises_without_configured_priority_provider(monkeypatch) -> None:
    import registry.provider_execution_registry as execution_registry

    monkeypatch.setattr(execution_registry, "_load_from_db", lambda: None)
    monkeypatch.setattr(
        execution_registry,
        "_REGISTRY",
        {
            "localcli": ProviderCLIProfile(provider_slug="localcli", binary="localcli"),
            "self_hosted": ProviderCLIProfile(provider_slug="self_hosted", binary="self-hosted"),
        },
    )

    with pytest.raises(RuntimeError, match="no configured default provider"):
        execution_registry.default_provider_slug()


def test_admin_health_uses_transport_support_frontdoor_for_provider_probes(monkeypatch) -> None:
    captured: dict[str, object] = {}
    provider_probe_calls: list[tuple[str, str]] = []

    class _FakePanel:
        def snapshot(self):
            return {"posture": "build"}

        def recommend_lane(self):
            return SimpleNamespace(
                recommended_posture="build",
                confidence=1.0,
                reasons=("healthy",),
                degraded_cause=None,
            )

    def _fake_query_transport_support(**kwargs):
        captured["transport_kwargs"] = dict(kwargs)
        return _transport_support_payload()

    monkeypatch.setattr(
        workflow_admin,
        "query_transport_support",
        _fake_query_transport_support,
    )
    monkeypatch.setattr(
        workflow_admin,
        "dependency_truth_report",
        lambda scope="all": {"ok": True, "scope": scope},
    )
    monkeypatch.setattr(
        workflow_admin,
        "_workflow_env",
        lambda _subs: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"},
    )
    monkeypatch.setattr(
        workflow_admin,
        "workflow_database_status",
        lambda env=None: SimpleNamespace(
            schema_bootstrapped=True,
            missing_schema_objects=(),
            compile_artifact_authority_ready=True,
            compile_index_authority_ready=True,
            execution_packet_authority_ready=True,
            repo_snapshot_authority_ready=True,
            verification_registry_ready=True,
            verifier_authority_ready=True,
            healer_authority_ready=True,
        ),
    )
    monkeypatch.setattr(
        "runtime.receipt_store.proof_metrics",
        lambda since_hours=0: {"ok": True, "since_hours": since_hours},
        raising=False,
    )

    subs = SimpleNamespace(
        get_health_mod=lambda: SimpleNamespace(
            PostgresProbe=lambda db_url: _FakeProbe(("postgres", db_url)),
            PostgresConnectivityProbe=lambda db_url: _FakeProbe(("postgres_connectivity", db_url)),
            DiskSpaceProbe=lambda path: _FakeProbe(("disk", path)),
            ProviderTransportProbe=lambda provider_slug, adapter_type: (
                provider_probe_calls.append((provider_slug, adapter_type))
                or _FakeProbe(("provider_transport", provider_slug, adapter_type))
            ),
            PreflightRunner=_CapturingPreflightRunner,
        ),
        get_pg_conn=lambda: "pg-conn",
        get_operator_panel=lambda: _FakePanel(),
    )

    result = workflow_admin._handle_health(subs, {})

    assert captured["transport_kwargs"]["pg"] == "pg-conn"
    assert provider_probe_calls == [
        ("openai", "cli_llm"),
        ("openai", "llm_task"),
        ("google", "cli_llm"),
    ]
    assert result["provider_registry"] == {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "registered_providers": ["openai", "google"],
        "providers": [
            {"provider_slug": "openai", "adapters": ["cli_llm", "llm_task"]},
            {"provider_slug": "google", "adapters": ["cli_llm"]},
        ],
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
    }


def test_mcp_health_uses_transport_support_frontdoor_for_provider_probes(monkeypatch) -> None:
    provider_probe_calls: list[tuple[str, str]] = []

    class _FakePanel:
        def snapshot(self):
            return {"posture": "build"}

        def recommend_lane(self):
            return SimpleNamespace(
                recommended_posture="build",
                confidence=1.0,
                reasons=("healthy",),
                degraded_cause=None,
            )

    monkeypatch.setattr(
        health_tool,
        "query_transport_support",
        lambda **_kwargs: _transport_support_payload(),
    )
    monkeypatch.setattr(
        health_tool,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"},
    )
    monkeypatch.setattr(
        health_tool,
        "workflow_database_url_for_repo",
        lambda repo_root, env=None: "postgresql://repo.test/workflow",
    )
    monkeypatch.setattr(
        health_tool,
        "dependency_truth_report",
        lambda scope="all": {"ok": True, "scope": scope},
    )
    monkeypatch.setattr(health_tool, "get_context_cache", lambda: SimpleNamespace(stats=lambda: {}))
    monkeypatch.setattr(health_tool, "_serialize", lambda value: value)
    monkeypatch.setattr(
        health_tool,
        "build_trend_observability",
        lambda: {"summary": {"total_trends": 0}},
    )
    monkeypatch.setattr(
        health_tool,
        "_subs",
        SimpleNamespace(
            get_health_mod=lambda: SimpleNamespace(
                PostgresProbe=lambda db_url: _FakeProbe(("postgres", db_url)),
                PostgresConnectivityProbe=lambda db_url: _FakeProbe(("postgres_connectivity", db_url)),
                DiskSpaceProbe=lambda path: _FakeProbe(("disk", path)),
                ProviderTransportProbe=lambda provider_slug, adapter_type: (
                    provider_probe_calls.append((provider_slug, adapter_type))
                    or _FakeProbe(("provider_transport", provider_slug, adapter_type))
                ),
                PreflightRunner=_CapturingPreflightRunner,
            ),
            get_pg_conn=lambda: "pg-conn",
            get_operator_panel=lambda: _FakePanel(),
            get_memory_engine=lambda: SimpleNamespace(_connect=lambda: None),
        ),
    )

    result = health_tool.tool_praxis_health({})

    assert provider_probe_calls == [
        ("openai", "cli_llm"),
        ("openai", "llm_task"),
        ("google", "cli_llm"),
    ]
    assert result["provider_registry"] == {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "registered_providers": ["openai", "google"],
        "providers": [
            {"provider_slug": "openai", "adapters": ["cli_llm", "llm_task"]},
            {"provider_slug": "google", "adapters": ["cli_llm"]},
        ],
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
    }
