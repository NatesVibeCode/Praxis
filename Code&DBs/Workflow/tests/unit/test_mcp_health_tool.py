from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime, timezone

from surfaces.mcp.tools import health as health_tool
import runtime.missing_detector as missing_detector


class _FakeProbe:
    def __init__(self, payload):
        self.payload = payload


class _FakePreflightRunner:
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
                )
            ],
            timestamp=SimpleNamespace(isoformat=lambda: "2026-04-10T00:00:00+00:00"),
        )


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


def test_tool_dag_health_uses_workflow_database_env(monkeypatch) -> None:
    captured: dict[str, object] = {}
    provider_probe_calls: list[tuple[str, str]] = []

    class _CapturingPreflightRunner(_FakePreflightRunner):
        def __init__(self, probes):
            super().__init__(probes)
            captured["probe_payloads"] = [probe.payload for probe in probes]

    class _FakeConn:
        def execute(self, sql: str, *args):
            if "FROM memory_entities" in sql:
                if args:
                    ids = set(args[0] or [])
                    rows = [
                        {
                            "id": "doc-1",
                            "entity_type": "document",
                            "name": "weekly-plan",
                            "content": "weekly plan content",
                            "created_at": "2026-04-01T00:00:00+00:00",
                            "updated_at": "2026-04-05T00:00:00+00:00",
                        },
                        {
                            "id": "workflow-run-1",
                            "entity_type": "workflow_run",
                            "name": "run-1",
                            "content": "workflow run summary",
                            "created_at": "2026-04-02T00:00:00+00:00",
                            "updated_at": "2026-04-06T00:00:00+00:00",
                        },
                    ]
                    return [row for row in rows if row["id"] in ids]
                return [
                    {
                        "id": "doc-1",
                        "entity_type": "document",
                        "name": "weekly-plan",
                        "content": "weekly plan content",
                        "created_at": "2026-04-01T00:00:00+00:00",
                        "updated_at": "2026-04-05T00:00:00+00:00",
                    }
                ]
            if "FROM memory_edges" in sql:
                return [
                    {
                        "source_id": "doc-1",
                        "target_id": "workflow-run-1",
                    }
                ]
            raise AssertionError(f"Unexpected SQL in test stub: {sql}")

    def _fake_resolve(env=None):
        captured["env"] = env
        return "postgresql://repo.test/workflow"

    monkeypatch.setattr(
        health_tool,
        "dependency_truth_report",
        lambda scope="all": {"ok": True, "scope": scope},
    )
    monkeypatch.setattr(
        health_tool,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"},
    )
    monkeypatch.setattr(health_tool, "workflow_database_url_for_repo", lambda repo_root, env=None: _fake_resolve(env=env))
    monkeypatch.setattr(health_tool, "get_context_cache", lambda: SimpleNamespace(stats=lambda: {"hit_rate": 0.0}))
    monkeypatch.setattr(health_tool, "_serialize", lambda value: value)
    monkeypatch.setattr(
        health_tool,
        "get_route_outcomes",
        lambda: SimpleNamespace(
            summary=lambda **_kwargs: {
                "provider_count": 1,
                "healthy_provider_count": 1,
                "unhealthy_provider_count": 0,
                "provider_slugs": ["openai"],
                "providers": [
                    {
                        "provider_slug": "openai",
                        "consecutive_failures": 0,
                        "healthy": True,
                        "recent_outcomes": [],
                    }
                ],
                "recent_limit": 3,
            }
        ),
    )
    monkeypatch.setattr(missing_detector, "_now", lambda: datetime(2026, 4, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(
        health_tool,
        "query_transport_support",
        lambda **_kwargs: {
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
        },
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
            get_memory_engine=lambda: SimpleNamespace(_connect=lambda: _FakeConn()),
        ),
    )

    result = health_tool.tool_dag_health({})

    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"}
    assert captured["probe_payloads"] == [
        ("postgres", "postgresql://repo.test/workflow"),
        ("postgres_connectivity", "postgresql://repo.test/workflow"),
        ("disk", str(health_tool.REPO_ROOT)),
        ("provider_transport", "openai", "cli_llm"),
        ("provider_transport", "openai", "llm_task"),
        ("provider_transport", "google", "cli_llm"),
    ]
    assert result["preflight"]["overall"] == "healthy"
    assert result["lane_recommendation"]["recommended_posture"] == "build"
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
    assert result["dependency_truth"] == {"ok": True, "scope": "all"}
    assert provider_probe_calls == [
        ("openai", "cli_llm"),
        ("openai", "llm_task"),
        ("google", "cli_llm"),
    ]
    assert result["content_health"] == {
        "total_findings": 1,
        "top_findings": [
            {
                "finding_type": "weekly_gap",
                "description": "'document' has a 10-day gap (expected weekly)",
                "severity": "medium",
            }
        ],
        "edges": [
            {
                "source": "doc-1",
                "target": "workflow-run-1",
                "source_entity": {
                    "entity_id": "doc-1",
                    "entity_type": "document",
                    "name": "weekly-plan",
                    "summary": "weekly plan content",
                },
                "target_entity": {
                    "entity_id": "workflow-run-1",
                    "entity_type": "workflow_run",
                    "name": "run-1",
                    "summary": "workflow run summary",
                },
            }
        ],
    }
    assert result["route_outcomes"] == {
        "provider_count": 1,
        "healthy_provider_count": 1,
        "unhealthy_provider_count": 0,
        "provider_slugs": ["openai"],
        "providers": [
            {
                "provider_slug": "openai",
                "consecutive_failures": 0,
                "healthy": True,
                "recent_outcomes": [],
            }
        ],
        "recent_limit": 3,
    }


def test_tool_dag_health_reports_projection_freshness_sla(monkeypatch) -> None:
    from runtime import projection_freshness as projection_freshness_module
    from runtime.projection_freshness import EVENT_LOG_CURSOR, ProjectionFreshness

    class _FakeConfig:
        def get_float(self, key: str) -> float:
            values = {
                "observability.projection_freshness.warning_staleness_seconds": 300.0,
                "observability.projection_freshness.critical_staleness_seconds": 900.0,
            }
            return values[key]

        def get_int(self, key: str) -> int:
            values = {
                "observability.projection_freshness.warning_lag_events": 0,
                "observability.projection_freshness.critical_lag_events": 100,
                "health.max_consecutive_failures": 3,
            }
            return values[key]

    monkeypatch.setattr(
        projection_freshness_module,
        "collect_projection_freshness_sync",
        lambda _conn: (
            ProjectionFreshness(
                projection_id="operator_decisions_current",
                source_kind=EVENT_LOG_CURSOR,
                observed_at=datetime(2026, 4, 17, tzinfo=timezone.utc),
                staleness_seconds=901.0,
                lag_events=2,
            ),
        ),
    )
    monkeypatch.setattr(health_tool, "get_registry_config", lambda: _FakeConfig())
    monkeypatch.setattr(
        health_tool,
        "dependency_truth_report",
        lambda scope="all": {"ok": True, "scope": scope},
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
        "get_context_cache",
        lambda: SimpleNamespace(stats=lambda: {"hit_rate": 0.0}),
    )
    monkeypatch.setattr(health_tool, "_serialize", lambda value: value)
    monkeypatch.setattr(
        health_tool,
        "query_transport_support",
        lambda **_kwargs: {
            "default_provider_slug": "openai",
            "default_adapter_type": "cli_llm",
            "support_basis": "provider_execution_registry",
            "providers": [],
        },
    )
    monkeypatch.setattr(
        health_tool,
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
        },
    )
    monkeypatch.setattr(
        health_tool,
        "get_route_outcomes",
        lambda: SimpleNamespace(summary=lambda **_kwargs: {"provider_count": 0}),
    )
    monkeypatch.setattr(
        health_tool,
        "_subs",
        SimpleNamespace(
            get_health_mod=lambda: SimpleNamespace(
                PostgresProbe=lambda db_url: _FakeProbe(("postgres", db_url)),
                PostgresConnectivityProbe=lambda db_url: _FakeProbe(
                    ("postgres_connectivity", db_url)
                ),
                DiskSpaceProbe=lambda path: _FakeProbe(("disk", path)),
                ProviderTransportProbe=lambda provider_slug, adapter_type: _FakeProbe(
                    ("provider_transport", provider_slug, adapter_type)
                ),
                PreflightRunner=_FakePreflightRunner,
            ),
            get_pg_conn=lambda: "pg-conn",
            get_operator_panel=lambda: _FakePanel(),
            get_memory_engine=lambda: None,
        ),
    )

    result = health_tool.tool_dag_health({})

    assert result["projection_freshness_sla"]["status"] == "critical"
    assert result["projection_freshness_sla"]["read_side_circuit_breaker"] == "open"
    assert result["projection_freshness_sla"]["policy"]["policy_source"] == "platform_config"
    assert result["projection_freshness_sla"]["alerts"] == [
        {
            "projection_id": "operator_decisions_current",
            "status": "critical",
            "reason_code": "projection_staleness_seconds_critical",
            "source_kind": "event_log_cursor",
            "staleness_seconds": 901.0,
            "lag_events": 2,
            "read_side_circuit_breaker": "open",
        }
    ]


def test_tool_dag_health_enriches_edge_endpoints_with_entity_context(monkeypatch) -> None:
    class _FakeConn:
        def execute(self, sql: str, *args):
            if "FROM memory_entities" in sql and args:
                ids = set(args[0] or [])
                rows = [
                    {
                        "id": "doc-1",
                        "entity_type": "document",
                        "name": "weekly-plan",
                        "content": "weekly plan content",
                        "created_at": "2026-04-01T00:00:00+00:00",
                        "updated_at": "2026-04-05T00:00:00+00:00",
                    },
                    {
                        "id": "workflow-run-1",
                        "entity_type": "workflow_run",
                        "name": "run-1",
                        "content": "workflow run summary",
                        "created_at": "2026-04-02T00:00:00+00:00",
                        "updated_at": "2026-04-06T00:00:00+00:00",
                    },
                ]
                return [row for row in rows if row["id"] in ids]
            if "FROM memory_entities" in sql:
                return [
                    {
                        "id": "doc-1",
                        "entity_type": "document",
                        "name": "weekly-plan",
                        "content": "weekly plan content",
                        "created_at": "2026-04-01T00:00:00+00:00",
                        "updated_at": "2026-04-05T00:00:00+00:00",
                    }
                ]
            if "FROM memory_edges" in sql:
                return [
                    {
                        "source_id": "doc-1",
                        "target_id": "workflow-run-1",
                    }
                ]
            raise AssertionError(f"Unexpected SQL in test stub: {sql}")

    monkeypatch.setattr(
        health_tool,
        "dependency_truth_report",
        lambda scope="all": {"ok": True, "scope": scope},
    )
    monkeypatch.setattr(
        health_tool,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"},
    )
    monkeypatch.setattr(health_tool, "workflow_database_url_for_repo", lambda repo_root, env=None: "postgresql://repo.test/workflow")
    monkeypatch.setattr(health_tool, "get_context_cache", lambda: SimpleNamespace(stats=lambda: {"hit_rate": 0.0}))
    monkeypatch.setattr(health_tool, "_serialize", lambda value: value)
    monkeypatch.setattr(missing_detector, "_now", lambda: datetime(2026, 4, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(
        health_tool,
        "query_transport_support",
        lambda **_kwargs: {
            "default_provider_slug": "openai",
            "default_adapter_type": "cli_llm",
            "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
            "providers": [],
        },
    )
    monkeypatch.setattr(
        health_tool,
        "provider_registry_health",
        lambda: {
            "status": "loaded_from_db",
            "authority_available": True,
            "fallback_active": False,
            "provider_count": 0,
            "providers": [],
        },
    )
    monkeypatch.setattr(
        health_tool,
        "_subs",
        SimpleNamespace(
            get_health_mod=lambda: SimpleNamespace(
                PostgresProbe=lambda db_url: _FakeProbe(("postgres", db_url)),
                PostgresConnectivityProbe=lambda db_url: _FakeProbe(("postgres_connectivity", db_url)),
                DiskSpaceProbe=lambda path: _FakeProbe(("disk", path)),
                ProviderTransportProbe=lambda provider_slug, adapter_type: _FakeProbe(("provider_transport", provider_slug, adapter_type)),
                PreflightRunner=_FakePreflightRunner,
            ),
            get_pg_conn=lambda: "pg-conn",
            get_operator_panel=lambda: _FakePanel(),
            get_memory_engine=lambda: SimpleNamespace(_connect=lambda: _FakeConn()),
        ),
    )

    result = health_tool.tool_dag_health({})
    assert result["content_health"]["total_findings"] == 1
    assert result["content_health"].get("edges")[0]["source_entity"]["entity_type"] == "document"
    assert result["content_health"].get("edges")[0]["target_entity"]["entity_type"] == "workflow_run"
