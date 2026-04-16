from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

from runtime import praxis_supervisor


def _paths(tmp_path: Path, *, db_url: str = "postgresql://test@localhost:5432/praxis_test") -> praxis_supervisor.SupervisorPaths:
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
    )


def test_render_launch_agent_plist_uses_single_praxis_authority(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    payload = plistlib.loads(praxis_supervisor.render_launch_agent_plist(paths).encode("utf-8"))

    assert payload["Label"] == "com.praxis.engine"
    assert payload["Program"] == str(paths.wrapper_program)
    assert payload["ProgramArguments"] == [str(paths.wrapper_program), "agent-run"]
    assert payload["EnvironmentVariables"]["WORKFLOW_DATABASE_URL"] == "postgresql://test@localhost:5432/praxis_test"
    assert "praxis-wait-pg" not in praxis_supervisor.render_launch_agent_plist(paths)


def test_discover_database_url_prefers_legacy_launch_agent_value(monkeypatch, tmp_path: Path) -> None:
    home_dir = tmp_path / "home"
    launch_agents = home_dir / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True)
    plist_path = launch_agents / "com.praxis.api-server.plist"
    plist_path.write_bytes(
        plistlib.dumps(
            {
                "EnvironmentVariables": {
                    "WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis_test",
                }
            }
        )
    )

    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(praxis_supervisor.Path, "home", classmethod(lambda cls: home_dir))
    monkeypatch.setattr(
        praxis_supervisor,
        "_database_authority_reachable",
        lambda database_url: database_url == "postgresql://localhost:5432/praxis_test",
    )

    assert praxis_supervisor.discover_database_url(tmp_path / "repo") == "postgresql://localhost:5432/praxis_test"


def test_discover_database_url_ignores_unreachable_launch_agent_value_and_uses_repo_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    home_dir = tmp_path / "home"
    launch_agents = home_dir / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True)
    plist_path = launch_agents / "com.praxis.api-server.plist"
    plist_path.write_bytes(
        plistlib.dumps(
            {
                "EnvironmentVariables": {
                    "WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/missing_db",
                }
            }
        )
    )
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    (repo_root / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/praxis\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(praxis_supervisor.Path, "home", classmethod(lambda cls: home_dir))
    monkeypatch.setattr(
        praxis_supervisor,
        "_database_authority_reachable",
        lambda _database_url: False,
    )

    assert praxis_supervisor.discover_database_url(repo_root) == "postgresql://repo.test/praxis"


def test_discover_database_url_uses_repo_env_when_process_authority_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(praxis_supervisor.Path, "home", classmethod(lambda cls: tmp_path / "home"))
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    (repo_root / ".env").write_text(
        "WORKFLOW_DATABASE_URL=postgresql://repo.test/praxis\n",
        encoding="utf-8",
    )

    assert praxis_supervisor.discover_database_url(repo_root) == "postgresql://repo.test/praxis"


def test_discover_database_url_requires_explicit_authority(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.setattr(praxis_supervisor.Path, "home", classmethod(lambda cls: tmp_path / "home"))

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL authority"):
        praxis_supervisor.discover_database_url(tmp_path / "repo")


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
