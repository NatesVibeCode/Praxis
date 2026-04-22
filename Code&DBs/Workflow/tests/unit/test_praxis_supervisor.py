from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

from runtime import praxis_supervisor


def _paths(
    tmp_path: Path,
    *,
    db_url: str = "postgresql://test@localhost:5432/praxis_test",
    authority_source: str = "argument",
) -> praxis_supervisor.SupervisorPaths:
    repo_root = tmp_path / "Praxis"
    workflow_dir = repo_root / "Code&DBs" / "Workflow"
    state_dir = repo_root / ".cache" / "praxis-supervisor"
    launch_agents = tmp_path / "LaunchAgents"
    return praxis_supervisor.SupervisorPaths(
        repo_root=repo_root,
        workflow_dir=workflow_dir,
        pgdata=repo_root / "Code&DBs" / "Databases" / "postgres-dev" / "data",
        pg_log=repo_root / "Code&DBs" / "Databases" / "postgres-dev" / "log" / "postgres.log",
        launch_agents_dir=launch_agents,
        launch_agent_plist=launch_agents / f"{praxis_supervisor.SUPERVISOR_LABEL}.plist",
        wrapper_program=repo_root / "scripts" / praxis_supervisor.SUPERVISOR_PROGRAM_NAME,
        state_dir=state_dir,
        state_file=state_dir / "state.json",
        control_file=state_dir / "control.json",
        database_url=db_url,
        database_authority_source=authority_source,
    )


def test_render_launch_agent_plist_uses_single_praxis_authority(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    payload = plistlib.loads(praxis_supervisor.render_launch_agent_plist(paths).encode("utf-8"))

    assert payload["Label"] == "com.praxis.engine"
    assert payload["Program"] == str(paths.wrapper_program)
    assert payload["ProgramArguments"] == [str(paths.wrapper_program), "agent-run"]
    assert payload["EnvironmentVariables"]["WORKFLOW_DATABASE_URL"] == "postgresql://test@localhost:5432/praxis_test"
    assert payload["EnvironmentVariables"]["WORKFLOW_DATABASE_AUTHORITY_SOURCE"] == "argument"
    assert "praxis-wait-pg" not in praxis_supervisor.render_launch_agent_plist(paths)


def test_discover_database_url_uses_runtime_authority(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_runtime_database_authority(*, repo_root: Path, required: bool) -> praxis_supervisor.WorkflowDatabaseAuthority:
        captured["repo_root"] = repo_root
        captured["required"] = required
        return praxis_supervisor.WorkflowDatabaseAuthority(
            database_url="postgresql://localhost:5432/praxis_test",
            source="launchd:com.praxis.engine",
        )

    monkeypatch.setattr(
        praxis_supervisor,
        "resolve_runtime_database_authority",
        _fake_resolve_runtime_database_authority,
    )

    repo_root = tmp_path / "repo"

    assert praxis_supervisor.discover_database_url(repo_root) == "postgresql://localhost:5432/praxis_test"
    assert captured == {"repo_root": repo_root, "required": True}


def test_discover_database_authority_returns_runtime_source(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setattr(
        praxis_supervisor,
        "resolve_runtime_database_authority",
        lambda *, repo_root, required: praxis_supervisor.WorkflowDatabaseAuthority(
            database_url="postgresql://repo.test/praxis",
            source="repo_env:/tmp/repo/.env",
        ),
    )

    assert praxis_supervisor.discover_database_authority(repo_root) == praxis_supervisor.WorkflowDatabaseAuthority(
        database_url="postgresql://repo.test/praxis",
        source="repo_env:/tmp/repo/.env",
    )


def test_discover_database_url_requires_explicit_authority(monkeypatch, tmp_path: Path) -> None:
    def _raise_missing(*, repo_root: Path, required: bool) -> praxis_supervisor.WorkflowDatabaseAuthority:
        raise praxis_supervisor.PostgresConfigurationError(
            "postgres.config_missing",
            "WORKFLOW_DATABASE_URL must be set",
        )

    monkeypatch.setattr(
        praxis_supervisor,
        "resolve_runtime_database_authority",
        _raise_missing,
    )

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL authority"):
        praxis_supervisor.discover_database_url(tmp_path / "repo")


def test_build_paths_records_database_authority_source(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    monkeypatch.setattr(
        praxis_supervisor,
        "discover_database_authority",
        lambda _repo_root: praxis_supervisor.WorkflowDatabaseAuthority(
            database_url="postgresql://repo.test/praxis",
            source="repo_env:/tmp/repo/.env",
        ),
    )

    paths = praxis_supervisor.build_paths(repo_root)

    assert paths.database_url == "postgresql://repo.test/praxis"
    assert paths.database_authority_source == "repo_env:/tmp/repo/.env"
    assert paths.environment["WORKFLOW_DATABASE_AUTHORITY_SOURCE"] == "repo_env:/tmp/repo/.env"


def test_build_paths_honors_explicit_launchd_dir(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    launchd_dir = tmp_path / "LaunchAgents"
    monkeypatch.setenv("PRAXIS_LAUNCHD_DIR", str(launchd_dir))
    monkeypatch.setattr(
        praxis_supervisor,
        "discover_database_authority",
        lambda _repo_root: praxis_supervisor.WorkflowDatabaseAuthority(
            database_url="postgresql://repo.test/praxis",
            source="repo_env:/tmp/repo/.env",
        ),
    )

    paths = praxis_supervisor.build_paths(repo_root)

    assert paths.launch_agents_dir == launchd_dir
    assert paths.launch_agent_plist == launchd_dir / f"{praxis_supervisor.SUPERVISOR_LABEL}.plist"


def test_apply_control_action_updates_desired_state_and_restart_tokens(monkeypatch, tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setattr(praxis_supervisor, "current_supervisor_pid", lambda _: None)

    restarted = praxis_supervisor.apply_control_action(paths, "restart", "worker")

    assert restarted["desired"]["workflow-worker"] is True
    assert restarted["restart_tokens"]["workflow-worker"] == 1

    stopped = praxis_supervisor.apply_control_action(paths, "stop", "scheduler")

    assert stopped["desired"]["scheduler"] is False
    assert stopped["restart_tokens"]["workflow-worker"] == 1


def test_build_status_snapshot_projects_supervisor_state_to_compatibility_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    paths.launch_agent_plist.parent.mkdir(parents=True, exist_ok=True)
    paths.launch_agent_plist.write_text("installed", encoding="utf-8")
    control_payload = {
        "desired": {
            "postgres": True,
            "api-server": True,
            "workflow-worker": False,
            "scheduler": True,
        },
        "restart_tokens": {key: 0 for key in praxis_supervisor.COMPONENT_ORDER},
        "updated_at": "2026-04-12T00:00:00+00:00",
    }
    state_payload = {
        "supervisor": {"pid": 999, "started_at": "2026-04-12T00:00:00+00:00"},
        "components": {
            "postgres": {"pid": 111, "state": "running", "last_exit_code": None},
            "api-server": {"pid": None, "state": "waiting_on_postgres", "last_exit_code": None},
            "workflow-worker": {"pid": None, "state": "stopped", "last_exit_code": None},
            "scheduler": {"pid": None, "state": "idle", "last_exit_code": 7},
        },
    }

    def _fake_process_command(pid: int) -> str | None:
        return {
            111: f"postgres -D {paths.pgdata}",
            999: "python -m runtime.praxis_supervisor run",
        }.get(pid)

    monkeypatch.setattr(praxis_supervisor, "_process_command", _fake_process_command)
    monkeypatch.setattr(praxis_supervisor, "_find_matching_pid", lambda spec, paths: None)
    monkeypatch.setattr(praxis_supervisor, "_pg_ready", lambda paths: False)

    snapshot = praxis_supervisor.build_status_snapshot(
        paths,
        launchd_loaded=True,
        supervisor_pid=999,
        control_payload=control_payload,
        state_payload=state_payload,
    )

    services = {service["component"]: service for service in snapshot["services"]}

    assert services["postgres"]["label"] == "com.praxis.postgres"
    assert services["postgres"]["running"] is True
    assert services["api-server"]["state"] == "waiting_on_postgres"
    assert services["workflow-worker"]["state"] == "stopped"
    assert services["scheduler"]["state"] == "waiting_on_postgres"
