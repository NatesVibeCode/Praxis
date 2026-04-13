from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

_runtime_pkg = types.ModuleType("runtime")
_runtime_pkg.__path__ = [str(_WORKFLOW_ROOT / "runtime")]
sys.modules.setdefault("runtime", _runtime_pkg)

from surfaces._subsystems_base import _BaseSubsystems
import registry.integration_registry_sync as integration_registry_sync_mod
import registry.reference_catalog_sync as reference_catalog_mod
from registry.integration_registry_sync import sync_integration_registry
from storage.postgres import PostgresConfigurationError
from surfaces.mcp import subsystems as mcp_subsystems


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, query: str, *params: Any):
        self.executed.append((query, params))
        if "information_schema.columns" in query and "integration_registry" in query:
            return [
                {"column_name": "id"},
                {"column_name": "name"},
                {"column_name": "description"},
                {"column_name": "provider"},
                {"column_name": "capabilities"},
                {"column_name": "auth_status"},
                {"column_name": "icon"},
                {"column_name": "mcp_server_id"},
            ]
        return []

    def execute_many(self, query: str, rows: list[tuple[Any, ...]]) -> None:
        self.batch_calls.append((query, rows))


class _FakeRunner:
    def __init__(self) -> None:
        self.loop_calls: list[dict[str, Any]] = []

    def run_loop(self, **kwargs: Any) -> None:
        self.loop_calls.append(kwargs)


class _FakeThread:
    instances: list["_FakeThread"] = []

    def __init__(self, *, target, kwargs, daemon, name) -> None:
        self.target = target
        self.kwargs = kwargs
        self.daemon = daemon
        self.name = name
        self.started = False
        _FakeThread.instances.append(self)

    def start(self) -> None:
        self.started = True
        self.target(**self.kwargs)


class _TestSubsystems(_BaseSubsystems):
    def __init__(self) -> None:
        super().__init__(
            repo_root=Path(__file__).resolve().parents[4],
            workflow_root=_WORKFLOW_ROOT,
            receipts_dir=str(_WORKFLOW_ROOT / "artifacts" / "test_receipts"),
            default_database_url="postgresql://test@localhost:5432/praxis_test",
        )

    def _postgres_env(self) -> dict[str, str]:
        return {
            "WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test",
            "PATH": "",
        }


class _NoHeartbeatSubsystems(_TestSubsystems):
    def _should_start_heartbeat_background(self) -> bool:
        return False


class _DefaultDatabaseUrlSubsystems(_BaseSubsystems):
    def __init__(self) -> None:
        super().__init__(
            repo_root=Path(__file__).resolve().parents[4],
            workflow_root=_WORKFLOW_ROOT,
            receipts_dir=str(_WORKFLOW_ROOT / "artifacts" / "test_receipts"),
            default_database_url="postgresql://test@localhost:5432/praxis_test",
        )


def test_startup_wiring_permission_errors_are_downgraded_to_debug(caplog: pytest.LogCaptureFixture) -> None:
    subs = _DefaultDatabaseUrlSubsystems()

    with caplog.at_level("DEBUG"):
        subs._handle_startup_wiring_error(PermissionError("[Errno 1] Operation not permitted"))

    assert "startup wiring skipped under sandbox constraints" in caplog.text
    assert "WARNING" not in caplog.text


def test_startup_wiring_typed_authority_errors_are_downgraded_to_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    subs = _DefaultDatabaseUrlSubsystems()

    with caplog.at_level("DEBUG"):
        subs._handle_startup_wiring_error(
            PostgresConfigurationError(
                "postgres.authority_unavailable",
                "WORKFLOW_DATABASE_URL authority unavailable: PermissionError: [Errno 1] Operation not permitted",
            )
        )

    assert "startup wiring skipped under sandbox constraints" in caplog.text
    assert "WARNING" not in caplog.text


def test_sync_integration_registry_projects_static_and_mcp_rows() -> None:
    conn = _FakeConn()

    inserted = sync_integration_registry(conn)

    assert len(conn.batch_calls) == 1
    _, rows = conn.batch_calls[0]
    assert inserted == len(rows)
    by_id = {row[0]: row for row in rows}
    assert {"dag-dispatch", "notifications", "webhook", "workflow"} <= set(by_id)
    assert {"praxis_query", "praxis_status", "praxis_maintenance", "praxis_operator_view", "praxis_workflow"} <= set(by_id)

    praxis_query = by_id["praxis_query"]
    assert praxis_query[3] == "mcp"
    assert praxis_query[6] == "tool"
    assert praxis_query[7] == "praxis-workflow-mcp"
    assert json.loads(praxis_query[4])[0]["action"] == "query"

    praxis_status_caps = json.loads(by_id["praxis_status"][4])
    assert [cap["action"] for cap in praxis_status_caps] == ["status"]

    praxis_maintenance_caps = json.loads(by_id["praxis_maintenance"][4])
    assert {"reset_metrics", "backfill_bug_replay_provenance"} <= {
        cap["action"] for cap in praxis_maintenance_caps
    }

    operator_view_caps = json.loads(by_id["praxis_operator_view"][4])
    assert operator_view_caps[0]["selectorField"] == "view"
    assert [cap["action"] for cap in operator_view_caps] == [
        "status",
        "scoreboard",
        "graph",
        "replay_ready_bugs",
    ]


def test_startup_wiring_syncs_registry_before_reference_catalog_and_starts_heartbeat(monkeypatch) -> None:
    events: list[str] = []
    fake_conn = _FakeConn()
    fake_runner = _FakeRunner()

    monkeypatch.setattr(
        "storage.postgres.ensure_postgres_available",
        lambda env=None: fake_conn,
    )

    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: events.append("integration") or 1,
    )
    monkeypatch.setattr(
        reference_catalog_mod,
        "sync_reference_catalog",
        lambda conn: events.append("reference_catalog") or 1,
    )
    monkeypatch.setattr("surfaces._lifecycle.threading.Thread", _FakeThread)

    subs = _TestSubsystems()
    subs._build_heartbeat_runner = lambda: fake_runner
    subs._should_auto_startup_wiring = lambda: True

    subs._maybe_startup_wiring()
    subs._maybe_startup_wiring()

    assert events == ["integration", "reference_catalog"]
    assert subs._lifecycle.started is True
    assert subs._lifecycle._heartbeat_thread is not None
    assert len(_FakeThread.instances) == 1
    assert _FakeThread.instances[0].started is True
    assert fake_runner.loop_calls == [{"interval_seconds": 300}]


def test_startup_wiring_can_skip_heartbeat_background(monkeypatch) -> None:
    events: list[str] = []
    fake_conn = _FakeConn()

    monkeypatch.setattr(
        "storage.postgres.ensure_postgres_available",
        lambda env=None: fake_conn,
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: events.append("integration") or 1,
    )
    monkeypatch.setattr(
        reference_catalog_mod,
        "sync_reference_catalog",
        lambda conn: events.append("reference_catalog") or 1,
    )
    monkeypatch.setattr("surfaces._lifecycle.threading.Thread", _FakeThread)

    _FakeThread.instances.clear()
    subs = _NoHeartbeatSubsystems()
    subs._should_auto_startup_wiring = lambda: True

    subs._maybe_startup_wiring()

    assert events == ["integration", "reference_catalog"]
    assert subs._lifecycle.started is False
    assert subs._lifecycle._heartbeat_thread is None
    assert _FakeThread.instances == []


def test_postgres_env_falls_back_to_default_database_url_when_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    subs = _DefaultDatabaseUrlSubsystems()

    assert subs._postgres_env() == {
        "WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test",
        "PATH": "/usr/bin:/bin",
    }


def test_mcp_workflow_database_env_falls_back_to_repo_local_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(mcp_subsystems, "_REPO_ROOT", tmp_path)

    assert mcp_subsystems.workflow_database_env() == {
        "WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test",
        "PATH": "/usr/bin:/bin",
    }


def test_mcp_workflow_database_env_prefers_repo_env_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    (tmp_path / ".env").write_text("WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n", encoding="utf-8")
    monkeypatch.setattr(mcp_subsystems, "_REPO_ROOT", tmp_path)

    assert mcp_subsystems.workflow_database_env() == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "PATH": "/usr/bin:/bin",
    }
