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
        def execute(self, sql: str):
            if "FROM memory_entities" in sql:
                return [
                    {
                        "id": "doc-1",
                        "entity_type": "document",
                        "name": "weekly-plan",
                        "created_at": "2026-04-01T00:00:00+00:00",
                        "updated_at": "2026-04-05T00:00:00+00:00",
                    }
                ]
            if "FROM memory_edges" in sql:
                return []
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
    monkeypatch.setattr(missing_detector, "_now", lambda: datetime(2026, 4, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(
        health_tool,
        "provider_registry_mod",
        SimpleNamespace(
            registered_providers=lambda: ("openai", "google"),
            default_provider_slug=lambda: "openai",
            default_llm_adapter_type=lambda: "cli_llm",
            supports_adapter=lambda provider_slug, adapter_type: not (
                provider_slug == "google" and adapter_type == "llm_task"
            ),
        ),
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
    assert result["provider_registry"] == {
        "default_provider_slug": "openai",
        "default_adapter_type": "cli_llm",
        "registered_providers": ["openai", "google"],
        "providers": [
            {"provider_slug": "openai", "adapters": ["cli_llm", "llm_task"]},
            {"provider_slug": "google", "adapters": ["cli_llm"]},
        ],
    }
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
    }
