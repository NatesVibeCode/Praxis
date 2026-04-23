from __future__ import annotations

import importlib
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
import registry.native_runtime_profile_sync as native_runtime_profile_sync_mod
import runtime.integrations.connector_registrar as connector_registrar_mod
import runtime.capability_catalog as capability_catalog_mod
import runtime.reference_catalog_seeder as reference_catalog_seeder_mod
from registry.integration_registry_sync import sync_integration_registry
from storage.postgres import PostgresConfigurationError
from surfaces.api.handlers import _subsystems as api_subsystems
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
    assert {"praxis-dispatch", "notifications", "webhook", "workflow"} <= set(by_id)
    assert {
        "praxis_query",
        "praxis_status_snapshot",
        "praxis_metrics_reset",
        "praxis_issue_backlog",
        "praxis_run_status",
        "praxis_workflow",
    } <= set(by_id)
    assert "praxis_status" not in by_id
    assert "praxis_maintenance" not in by_id
    assert "praxis_operator_view" not in by_id

    praxis_query = by_id["praxis_query"]
    assert praxis_query[3] == "mcp"
    assert praxis_query[6] == "tool"
    assert praxis_query[7] == "praxis-workflow-mcp"
    assert json.loads(praxis_query[4])[0]["action"] == "query"

    status_caps = json.loads(by_id["praxis_status_snapshot"][4])
    assert [cap["action"] for cap in status_caps] == ["status_snapshot"]

    metrics_caps = json.loads(by_id["praxis_metrics_reset"][4])
    assert [cap["action"] for cap in metrics_caps] == ["metrics_reset"]

    issue_backlog_caps = json.loads(by_id["praxis_issue_backlog"][4])
    assert [cap["action"] for cap in issue_backlog_caps] == ["issue_backlog"]

    run_status_caps = json.loads(by_id["praxis_run_status"][4])
    assert [cap["action"] for cap in run_status_caps] == ["run_status"]


def test_sync_integration_registry_aborts_on_malformed_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "good.toml").write_text(
        "[integration]\n"
        'id = "good-manifest"\n'
        'name = "Good Manifest"\n'
        'provider = "http"\n'
    )
    (tmp_path / "bad.toml").write_text("this is not [valid toml")

    monkeypatch.setattr(
        integration_registry_sync_mod.integration_manifest,
        "_MANIFEST_DIR",
        tmp_path,
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "projected_mcp_integrations",
        lambda: [],
    )

    conn = _FakeConn()

    with pytest.raises(RuntimeError, match="bad.toml"):
        sync_integration_registry(conn)

    assert conn.batch_calls == []


def test_startup_wiring_syncs_registry_before_reference_catalog_and_starts_heartbeat(monkeypatch) -> None:
    events: list[str] = []
    fake_conn = _FakeConn()
    fake_runner = _FakeRunner()

    monkeypatch.setattr(
        "surfaces._subsystems_base.bootstrap_pg_conn",
        lambda **_kwargs: fake_conn,
    )

    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: events.append("integration") or 1,
    )
    monkeypatch.setattr(
        capability_catalog_mod,
        "sync_capability_catalog",
        lambda conn: events.append("capability") or 1,
    )
    monkeypatch.setattr(
        native_runtime_profile_sync_mod,
        "sync_native_runtime_profile_authority",
        lambda conn: events.append("native_runtime_profile") or ("praxis",),
    )
    monkeypatch.setattr(
        connector_registrar_mod,
        "sync_built_connectors",
        lambda conn: events.append("connector_registry") or 1,
    )
    monkeypatch.setattr(
        reference_catalog_seeder_mod,
        "seed_reference_catalog",
        lambda conn: events.append("reference_catalog") or 1,
    )
    monkeypatch.setattr("surfaces._lifecycle.threading.Thread", _FakeThread)

    subs = _TestSubsystems()
    subs._build_heartbeat_runner = lambda: fake_runner
    subs._should_auto_startup_wiring = lambda: True

    subs.boot()
    subs.boot()

    assert events == [
        "integration",
        "capability",
        "native_runtime_profile",
        "connector_registry",
        "reference_catalog",
    ]
    assert subs._lifecycle.started is True
    assert subs._lifecycle._heartbeat_thread is not None
    assert len(_FakeThread.instances) == 1


def test_get_pg_conn_is_connection_only(monkeypatch) -> None:
    events: list[str] = []
    fake_conn = _FakeConn()

    monkeypatch.setattr(
        "surfaces._subsystems_base.create_pg_conn",
        lambda **_kwargs: fake_conn,
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: events.append("integration") or 1,
    )
    monkeypatch.setattr(
        capability_catalog_mod,
        "sync_capability_catalog",
        lambda conn: events.append("capability") or 1,
    )

    subs = _TestSubsystems()
    subs._should_auto_startup_wiring = lambda: True

    assert subs.get_pg_conn() is fake_conn
    assert subs.get_pg_conn() is fake_conn
    assert events == []
    assert subs._lifecycle.started is False


def test_boot_warns_when_registry_sync_steps_are_skipped(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_conn = _FakeConn()

    monkeypatch.setattr(
        "surfaces._subsystems_base.bootstrap_pg_conn",
        lambda **_kwargs: fake_conn,
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: 1,
    )
    monkeypatch.setattr(
        capability_catalog_mod,
        "sync_capability_catalog",
        lambda conn: (_ for _ in ()).throw(RuntimeError("catalog offline")),
    )
    monkeypatch.setattr(
        native_runtime_profile_sync_mod,
        "sync_native_runtime_profile_authority",
        lambda conn: 1,
    )
    monkeypatch.setattr(
        connector_registrar_mod,
        "sync_built_connectors",
        lambda conn: 1,
    )
    monkeypatch.setattr(
        reference_catalog_seeder_mod,
        "seed_reference_catalog",
        lambda conn: 1,
    )

    subs = _TestSubsystems()
    subs._should_auto_startup_wiring = lambda: False

    with caplog.at_level("WARNING"):
        result = subs.boot()

    assert "startup registry sync completed with skipped steps: capability_catalog" in caplog.text
    assert "catalog offline" in caplog.text
    assert result["registry_sync"]["skipped"] == ["capability_catalog"]
    assert result["registry_sync"]["failures"] == [
        {
            "component": "capability_catalog",
            "exception_type": "RuntimeError",
            "message": "catalog offline",
        }
    ]
    assert subs.boot()["registry_sync"]["failures"] == result["registry_sync"]["failures"]


def test_startup_wiring_can_skip_heartbeat_background(monkeypatch) -> None:
    events: list[str] = []
    fake_conn = _FakeConn()

    monkeypatch.setattr(
        "surfaces._subsystems_base.bootstrap_pg_conn",
        lambda **_kwargs: fake_conn,
    )
    monkeypatch.setattr(
        integration_registry_sync_mod,
        "sync_integration_registry",
        lambda conn: events.append("integration") or 1,
    )
    monkeypatch.setattr(
        capability_catalog_mod,
        "sync_capability_catalog",
        lambda conn: events.append("capability") or 1,
    )
    monkeypatch.setattr(
        native_runtime_profile_sync_mod,
        "sync_native_runtime_profile_authority",
        lambda conn: events.append("native_runtime_profile") or ("praxis",),
    )
    monkeypatch.setattr(
        connector_registrar_mod,
        "sync_built_connectors",
        lambda conn: events.append("connector_registry") or 1,
    )
    monkeypatch.setattr(
        reference_catalog_seeder_mod,
        "seed_reference_catalog",
        lambda conn: events.append("reference_catalog") or 1,
    )
    monkeypatch.setattr("surfaces._lifecycle.threading.Thread", _FakeThread)

    _FakeThread.instances.clear()
    subs = _NoHeartbeatSubsystems()
    subs._should_auto_startup_wiring = lambda: True

    subs.boot()

    assert events == [
        "integration",
        "capability",
        "native_runtime_profile",
        "connector_registry",
        "reference_catalog",
    ]
    assert subs._lifecycle.started is False
    assert subs._lifecycle._heartbeat_thread is None
    assert _FakeThread.instances == []


def test_postgres_env_requires_explicit_authority_when_env_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    subs = _DefaultDatabaseUrlSubsystems()
    subs._repo_root = tmp_path

    with pytest.raises(PostgresConfigurationError) as exc_info:
        subs._postgres_env()

    assert exc_info.value.reason_code == "postgres.config_missing"
    assert str(tmp_path / ".env") in str(exc_info.value)


def test_mcp_workflow_database_env_requires_explicit_authority(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(mcp_subsystems, "_REPO_ROOT", tmp_path)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        mcp_subsystems.workflow_database_env()

    assert exc_info.value.reason_code == "postgres.config_missing"
    assert str(tmp_path / ".env") in str(exc_info.value)


def test_mcp_workflow_database_env_prefers_repo_env_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    (tmp_path / ".env").write_text("WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n", encoding="utf-8")
    monkeypatch.setattr(mcp_subsystems, "_REPO_ROOT", tmp_path)

    assert mcp_subsystems.workflow_database_env() == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "WORKFLOW_DATABASE_AUTHORITY_SOURCE": f"repo_env:{tmp_path / '.env'}",
        "PATH": "/usr/bin:/bin",
    }


def test_mcp_workflow_database_env_invalid_explicit_env_wins_and_raises(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "praxis_test")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )
    importlib.reload(mcp_subsystems)
    monkeypatch.setattr(mcp_subsystems, "_REPO_ROOT", tmp_path)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        mcp_subsystems.workflow_database_env()

    assert exc_info.value.reason_code == "postgres.config_invalid"
    assert "postgres:// or postgresql://" in str(exc_info.value)


def test_api_workflow_database_env_requires_explicit_authority(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(api_subsystems, "REPO_ROOT", tmp_path)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        api_subsystems.workflow_database_env()

    assert exc_info.value.reason_code == "postgres.config_missing"
    assert str(tmp_path / ".env") in str(exc_info.value)


def test_api_workflow_database_env_prefers_repo_env_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    (tmp_path / ".env").write_text("WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n", encoding="utf-8")
    monkeypatch.setattr(api_subsystems, "REPO_ROOT", tmp_path)

    assert api_subsystems.workflow_database_env() == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "WORKFLOW_DATABASE_AUTHORITY_SOURCE": f"repo_env:{tmp_path / '.env'}",
        "PATH": "/usr/bin:/bin",
    }


def test_api_workflow_database_env_invalid_explicit_env_wins_and_raises(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "praxis_test")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    (tmp_path / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/workflow\n",
        encoding="utf-8",
    )
    importlib.reload(api_subsystems)
    monkeypatch.setattr(api_subsystems, "REPO_ROOT", tmp_path)

    with pytest.raises(PostgresConfigurationError) as exc_info:
        api_subsystems.workflow_database_env()

    assert exc_info.value.reason_code == "postgres.config_invalid"
    assert "postgres:// or postgresql://" in str(exc_info.value)
