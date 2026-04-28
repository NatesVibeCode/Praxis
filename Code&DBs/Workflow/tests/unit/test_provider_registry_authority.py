from __future__ import annotations

from types import SimpleNamespace

import pytest

from adapters.provider_types import ProviderCLIProfile
from adapters import provider_transport
import registry.provider_execution_registry as provider_registry_mod
import runtime.health as runtime_health
from runtime.workflow import runtime_setup as workflow_runtime_setup
from runtime.workflow import _admission as workflow_admission
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


def test_default_adapter_type_for_provider_requires_admitted_lane_policy() -> None:
    profiles = {
        "openai": ProviderCLIProfile(
            provider_slug="openai",
            binary="codex",
            lane_policies={
                "cli_llm": {"transport_kind": "cli"},
                "llm_task": {"admitted_by_policy": False, "transport_kind": "http"},
            },
        ),
        "anthropic": ProviderCLIProfile(
            provider_slug="anthropic",
            binary="claude",
            lane_policies={
                "llm_task": {
                    "admitted_by_policy": True,
                    "transport_kind": "http",
                }
            },
        ),
    }

    assert provider_transport.default_adapter_type_for_provider(
        "openai",
        profiles=profiles,
    ) is None
    assert provider_transport.default_adapter_type_for_provider(
        "anthropic",
        profiles=profiles,
    ) == "llm_task"


def test_transport_no_longer_exposes_orphaned_default_provider_helper() -> None:
    assert not hasattr(provider_transport, "builtin_default_provider_slug")
    assert not hasattr(provider_transport, "BUILTIN_PROVIDER_PROFILES")


def test_provider_transport_probe_fails_when_runtime_registry_lacks_adapter(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.workflow._adapter_registry.runtime_supports_workflow_adapter_type",
        lambda _adapter_type: False,
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_lane_policy",
        lambda provider_slug, adapter_type: {"policy_reason": "supported"},
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_adapter_contract",
        lambda provider_slug, adapter_type: None,
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.supports_adapter",
        lambda provider_slug, adapter_type: True,
        raising=False,
    )

    check = runtime_health.ProviderTransportProbe("openai", "llm_task").check()

    assert check.passed is False
    assert check.status == "failed"
    assert check.message == "workflow runtime does not register adapter_type=llm_task"
    assert check.details["runtime_adapter_supported"] is False


def test_provider_transport_probe_fails_supported_api_without_ready_transport(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.workflow._adapter_registry.runtime_supports_workflow_adapter_type",
        lambda _adapter_type: True,
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_lane_policy",
        lambda provider_slug, adapter_type: {"policy_reason": "admitted"},
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_adapter_contract",
        lambda provider_slug, adapter_type: None,
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_adapter_economics",
        lambda provider_slug, adapter_type: {},
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.supports_adapter",
        lambda provider_slug, adapter_type: True,
        raising=False,
    )
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_api_endpoint",
        lambda provider_slug: "https://api.provider.test/v1",
        raising=False,
    )
    monkeypatch.setattr(
        runtime_health,
        "_provider_api_key_present",
        lambda provider_slug: False,
    )

    check = runtime_health.ProviderTransportProbe("openai", "llm_task").check()

    assert check.passed is False
    assert check.status == "failed"
    assert check.details["supported"] is True
    assert check.details["transport_ready"] is False
    assert check.details["credential_present"] is False


def test_workflow_runtime_uses_one_adapter_registry_authority(monkeypatch) -> None:
    monkeypatch.setattr("adapters.llm_task.default_provider_slug", lambda: "openai")
    monkeypatch.setattr(provider_registry_mod, "default_provider_slug", lambda: "openai")

    setup_registry = workflow_runtime_setup._build_adapter_registry(
        SimpleNamespace(
            adapter_type="cli_llm",
            scope_write=["README.md"],
            workdir="/tmp/workflow",
            verify_refs=["verifier.job.python.pytest_file"],
            packet_provenance=None,
            definition_revision=None,
            plan_revision=None,
            allowed_tools=(),
            capabilities=(),
            label=None,
            task_type="implement",
        )
    )
    graph_registry = workflow_admission._graph_adapter_registry(
        SimpleNamespace(
            nodes=[
                SimpleNamespace(adapter_type="context_compiler"),
                SimpleNamespace(adapter_type="cli_llm"),
                SimpleNamespace(adapter_type="output_parser"),
                SimpleNamespace(adapter_type="file_writer"),
                SimpleNamespace(adapter_type="verifier"),
            ]
        )
    )

    assert set(setup_registry._registry) == set(graph_registry._registry)


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
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
            "provider_count": 2,
            "providers": ["google", "openai"],
        },
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
    assert result["transport_support_summary"] == {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "registered_providers": ["openai", "google"],
        "providers": [
            {"provider_slug": "openai", "adapters": ["cli_llm", "llm_task"]},
            {"provider_slug": "google", "adapters": ["cli_llm"]},
        ],
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
        "provider_registry_status": "loaded_from_db",
        "provider_registry_authority_available": True,
        "provider_registry_fallback_active": False,
    }
    assert result["provider_registry"]["status"] == "loaded_from_db"
    assert result["content_health"] == {"status": "skipped", "reason": "no memory engine connection"}


def test_admin_health_degrades_when_provider_registry_load_fails(monkeypatch) -> None:
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
        workflow_admin,
        "query_transport_support",
        lambda **_kwargs: _transport_support_payload(),
    )
    monkeypatch.setattr(
        workflow_admin,
        "provider_registry_health",
        lambda: (_ for _ in ()).throw(RuntimeError("registry offline")),
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
            PostgresProbe=lambda db_url: runtime_health.StaticHealthProbe(
                name="postgres",
                passed=True,
                message="ok",
                status="ok",
            ),
            PostgresConnectivityProbe=lambda db_url: runtime_health.StaticHealthProbe(
                name="postgres_connectivity",
                passed=True,
                message="ok",
                status="ok",
            ),
            DiskSpaceProbe=lambda path: runtime_health.StaticHealthProbe(
                name="disk",
                passed=True,
                message="ok",
                status="ok",
            ),
            ProviderTransportProbe=lambda provider_slug, adapter_type: runtime_health.StaticHealthProbe(
                name=f"provider_transport:{provider_slug}:{adapter_type}",
                passed=True,
                message="ok",
                status="ok",
            ),
            StaticHealthProbe=runtime_health.StaticHealthProbe,
            PreflightRunner=runtime_health.PreflightRunner,
        ),
        get_pg_conn=lambda: "pg-conn",
        get_operator_panel=lambda: _FakePanel(),
    )

    result = workflow_admin._handle_health(subs, {})

    assert result["provider_registry"] == {
        "status": "load_failed",
        "error": "RuntimeError: registry offline",
        "authority_available": False,
        "fallback_active": False,
    }
    assert result["preflight"]["overall"] == "degraded"
    assert any(
        check["name"] == "provider_registry"
        and check["passed"] is False
        and check["status"] == "failed"
        for check in result["preflight"]["checks"]
    )


def test_admin_health_degrades_when_surface_usage_recorder_is_degraded(monkeypatch) -> None:
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

    degraded_recorder = {
        "authority_ready": False,
        "observability_state": "degraded",
        "dropped_event_count": 1,
        "durable_event_count": 0,
        "durable_error_count": 1,
        "backup_authority_ready": False,
        "last_error": "RuntimeError: surface usage table unavailable",
    }
    monkeypatch.setattr(workflow_admin, "surface_usage_recorder_health", lambda: degraded_recorder)
    monkeypatch.setattr(
        workflow_admin,
        "query_transport_support",
        lambda **_kwargs: _transport_support_payload(),
    )
    monkeypatch.setattr(
        workflow_admin,
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
        },
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
            PostgresProbe=lambda db_url: runtime_health.StaticHealthProbe(
                name="postgres",
                passed=True,
                message="ok",
                status="ok",
            ),
            PostgresConnectivityProbe=lambda db_url: runtime_health.StaticHealthProbe(
                name="postgres_connectivity",
                passed=True,
                message="ok",
                status="ok",
            ),
            DiskSpaceProbe=lambda path: runtime_health.StaticHealthProbe(
                name="disk",
                passed=True,
                message="ok",
                status="ok",
            ),
            ProviderTransportProbe=lambda provider_slug, adapter_type: runtime_health.StaticHealthProbe(
                name=f"provider_transport:{provider_slug}:{adapter_type}",
                passed=True,
                message="ok",
                status="ok",
            ),
            StaticHealthProbe=runtime_health.StaticHealthProbe,
            PreflightRunner=runtime_health.PreflightRunner,
        ),
        get_pg_conn=lambda: "pg-conn",
        get_operator_panel=lambda: _FakePanel(),
    )

    result = workflow_admin._handle_health(subs, {})

    assert result["preflight"]["overall"] == "degraded"
    assert result["surface_usage_recorder"] == degraded_recorder
    assert any(
        check["name"] == "surface_usage_recorder"
        and check["passed"] is False
        and check["details"] == degraded_recorder
        for check in result["preflight"]["checks"]
    )


def test_admin_health_includes_content_health_report(monkeypatch) -> None:
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
        workflow_admin,
        "build_content_health_report",
        lambda memory_engine: {"status": "ok", "memory_engine": "wired"},
    )
    monkeypatch.setattr(
        workflow_admin,
        "query_transport_support",
        lambda **_kwargs: _transport_support_payload(),
    )
    monkeypatch.setattr(
        workflow_admin,
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
            "provider_count": 2,
            "providers": ["google", "openai"],
        },
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
            ProviderTransportProbe=lambda provider_slug, adapter_type: _FakeProbe(("provider_transport", provider_slug, adapter_type)),
            PreflightRunner=_CapturingPreflightRunner,
        ),
        get_pg_conn=lambda: "pg-conn",
        get_operator_panel=lambda: _FakePanel(),
        get_memory_engine=lambda: SimpleNamespace(_connect=lambda: object()),
    )

    result = workflow_admin._handle_health(subs, {})

    assert result["content_health"] == {
        "status": "ok",
        "memory_engine": "wired",
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
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
            "provider_count": 2,
            "providers": ["google", "openai"],
        },
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
    assert result["transport_support_summary"] == {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "registered_providers": ["openai", "google"],
        "providers": [
            {"provider_slug": "openai", "adapters": ["cli_llm", "llm_task"]},
            {"provider_slug": "google", "adapters": ["cli_llm"]},
        ],
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
        "provider_registry_status": "loaded_from_db",
        "provider_registry_authority_available": True,
        "provider_registry_fallback_active": False,
    }
    assert result["provider_registry"]["status"] == "loaded_from_db"
