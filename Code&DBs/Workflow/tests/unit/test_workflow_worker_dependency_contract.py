from __future__ import annotations

from io import StringIO
from pathlib import Path
import tempfile

from runtime.workflow import _worker_loop as worker_loop
from surfaces.cli.commands import workflow as workflow_commands
from runtime import workflow_worker

WORKSPACE_ROOT = Path(tempfile.gettempdir()) / "praxis-workspace"
REQUIREMENTS_MANIFEST = WORKSPACE_ROOT / "requirements.runtime.txt"


def test_worker_concurrency_env_override_wins(monkeypatch) -> None:
    monkeypatch.setattr(worker_loop, "_worker_cpu_count", lambda: 8)
    monkeypatch.setattr(
        worker_loop,
        "_worker_available_memory_bytes",
        lambda: 6 * 1024**3,
    )

    decision = worker_loop.resolve_worker_concurrency({"PRAXIS_WORKER_MAX_PARALLEL": "3"})

    assert decision["max_concurrent"] == 3
    assert decision["source"] == "env:PRAXIS_WORKER_MAX_PARALLEL"
    assert decision["cpu_count"] == 8


def test_worker_concurrency_auto_balances_cpu_and_memory(monkeypatch) -> None:
    monkeypatch.setattr(worker_loop, "_worker_cpu_count", lambda: 6)
    monkeypatch.setattr(
        worker_loop,
        "_worker_available_memory_bytes",
        lambda: 5 * 1024**3,
    )

    decision = worker_loop.resolve_worker_concurrency({})

    assert decision["max_concurrent"] == 2
    assert decision["source"] == "resource:auto"
    assert decision["memory_slot_bytes"] == 2 * 1024**3


def test_local_worker_slots_are_capped_by_host_sandbox_admission() -> None:
    env = {"PRAXIS_HOST_DOCKER_SANDBOX_SLOTS": "2"}

    assert worker_loop._cap_local_slots_to_host_admission(6, env) == 2


def test_local_worker_slots_can_follow_explicit_host_sandbox_capacity() -> None:
    env = {"PRAXIS_HOST_DOCKER_SANDBOX_SLOTS": "4"}

    assert worker_loop._cap_local_slots_to_host_admission(6, env) == 4


def test_local_worker_slots_ignore_host_cap_when_admission_disabled() -> None:
    env = {
        "PRAXIS_HOST_RESOURCE_ADMISSION_DISABLED": "1",
        "PRAXIS_HOST_DOCKER_SANDBOX_SLOTS": "2",
    }

    assert worker_loop._cap_local_slots_to_host_admission(6, env) == 6


def test_compose_worker_does_not_pin_parallelism_by_default() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "PRAXIS_WORKER_MAX_PARALLEL:-8" not in compose_text
    assert "PRAXIS_WORKER_MAX_PARALLEL: ${PRAXIS_WORKER_MAX_PARALLEL:-}" in compose_text


def test_compose_builds_pass_layout_defaults_to_required_dockerfiles() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert (
        compose_text.count(
            "PRAXIS_CONTAINER_WORKSPACE_ROOT: ${PRAXIS_CONTAINER_WORKSPACE_ROOT:-/workspace}"
        )
        == 4
    )
    assert (
        compose_text.count(
            "PRAXIS_CONTAINER_HOME: ${PRAXIS_CONTAINER_HOME:-/home/praxis-agent}"
        )
        == 4
    )


def test_compose_allows_docker_specific_database_authority_override() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert (
        compose_text.count(
            "WORKFLOW_DATABASE_URL: ${PRAXIS_DOCKER_WORKFLOW_DATABASE_URL:-${WORKFLOW_DATABASE_URL:?WORKFLOW_DATABASE_URL must be set}}"
        )
        == 3
    )
    assert compose_text.count('"host.docker.internal:host-gateway"') == 3


def test_compose_declares_explicit_workflow_pool_caps_per_service() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "WORKFLOW_POOL_MAX_SIZE: ${PRAXIS_API_WORKFLOW_POOL_MAX_SIZE:-8}" in compose_text
    assert "WORKFLOW_POOL_MAX_SIZE: ${PRAXIS_SCHEDULER_WORKFLOW_POOL_MAX_SIZE:-2}" in compose_text
    assert "WORKFLOW_POOL_MAX_SIZE: ${PRAXIS_WORKER_WORKFLOW_POOL_MAX_SIZE:-4}" in compose_text
    assert compose_text.count("WORKFLOW_POOL_MIN_SIZE: ${") == 3


def test_compose_worker_receives_openrouter_credentials() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "OPENROUTER_API_KEY: ${OPENROUTER_API_KEY:-}" in compose_text


def test_start_worker_checks_dependency_contract_before_launch(monkeypatch) -> None:
    observed: dict[str, object] = {}
    monkeypatch.delenv("PRAXIS_WORKSPACE_BASE_PATH", raising=False)

    def _fake_require_runtime_dependencies(*, scope: str = "workflow_worker", manifest_path=None):
        observed["scope"] = scope
        observed["manifest_path"] = manifest_path
        return {
            "ok": True,
            "scope": scope,
            "manifest_path": str(REQUIREMENTS_MANIFEST),
            "required_count": 9,
            "available_count": 9,
            "missing_count": 0,
            "packages": [],
            "missing": [],
        }

    monkeypatch.setattr(workflow_worker, "require_runtime_dependencies", _fake_require_runtime_dependencies)
    monkeypatch.setattr(workflow_worker, "_build_worker_connection", lambda: "fake-conn")
    monkeypatch.setattr(
        workflow_worker,
        "_run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0: observed.update(
            {
                "conn": conn,
                "repo_root": repo_root,
                "poll_interval": poll_interval,
            }
        ),
    )

    workflow_worker.start_worker(
        poll_interval=0.5,
        file_path=str(WORKSPACE_ROOT / "Code&DBs/Workflow/runtime/workflow_worker.py"),
    )

    assert observed["scope"] == "workflow_worker"
    assert observed["manifest_path"] is None
    assert observed["conn"] == "fake-conn"
    assert observed["repo_root"] == str(WORKSPACE_ROOT)
    assert observed["poll_interval"] == 0.5


def test_start_worker_prefers_workspace_base_env(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setenv(
        "PRAXIS_WORKSPACE_BASE_PATH",
        str(WORKSPACE_ROOT),
    )
    monkeypatch.setattr(
        workflow_worker,
        "require_runtime_dependencies",
        lambda scope="workflow_worker", manifest_path=None: {
            "ok": True,
            "manifest_path": str(REQUIREMENTS_MANIFEST),
        },
    )
    monkeypatch.setattr(workflow_worker, "_build_worker_connection", lambda: "fake-conn")
    monkeypatch.setattr(
        workflow_worker,
        "_run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0: observed.update(
            {"repo_root": repo_root}
        ),
    )

    workflow_worker.start_worker(
        file_path=str(WORKSPACE_ROOT / "Code&DBs/Workflow/runtime/workflow_worker.py"),
    )

    assert observed["repo_root"] == str(WORKSPACE_ROOT)


def test_workflow_worker_uses_extracted_worker_loop(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        worker_loop,
        "run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0: observed.update(
            {
                "conn": conn,
                "repo_root": repo_root,
                "poll_interval": poll_interval,
            }
        ),
    )

    workflow_worker._run_worker_loop("fake-conn", "/repo", poll_interval=0.25)

    assert observed == {
        "conn": "fake-conn",
        "repo_root": "/repo",
        "poll_interval": 0.25,
    }


def test_queue_worker_command_uses_extracted_worker_loop(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(workflow_commands, "_workflow_runtime_conn", lambda: "fake-conn")
    monkeypatch.setattr(
        worker_loop,
        "run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0, worker_id=None, max_local_concurrent=4: (
            observed.update(
                {
                    "conn": conn,
                    "repo_root": repo_root,
                    "poll_interval": poll_interval,
                    "worker_id": worker_id,
                    "max_local_concurrent": max_local_concurrent,
                }
            )
        ),
    )
    stdout = StringIO()

    assert workflow_commands._queue_command(
        ["worker", "--max-concurrent", "3", "--poll-interval", "0.25"],
        stdout=stdout,
    ) == 0
    assert observed["conn"] == "fake-conn"
    assert observed["poll_interval"] == 0.25
    assert observed["max_local_concurrent"] == 3
    assert str(observed["worker_id"]).startswith("workflow-worker-")


def test_queue_worker_command_supports_concurrency_alias(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(workflow_commands, "_workflow_runtime_conn", lambda: "fake-conn")
    monkeypatch.setattr(
        worker_loop,
        "run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0, worker_id=None, max_local_concurrent=None: (
            observed.update(
                {
                    "max_local_concurrent": max_local_concurrent,
                }
            )
        ),
    )
    stdout = StringIO()

    assert workflow_commands._queue_command(
        ["worker", "--concurrency", "5"],
        stdout=stdout,
    ) == 0
    assert observed["max_local_concurrent"] == 5
    assert '"max_concurrent": 5' in stdout.getvalue()
    assert '"concurrency_source": "cli"' in stdout.getvalue()



def test_queue_worker_command_uses_resource_concurrency_when_unset(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(workflow_commands, "_workflow_runtime_conn", lambda: "fake-conn")
    monkeypatch.setattr(
        worker_loop,
        "resolve_worker_concurrency",
        lambda: {
            "max_concurrent": 2,
            "source": "resource:auto",
            "cpu_count": 4,
            "available_memory_bytes": 5 * 1024**3,
            "memory_slot_bytes": 2 * 1024**3,
        },
    )
    monkeypatch.setattr(
        worker_loop,
        "run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0, worker_id=None, max_local_concurrent=None: (
            observed.update(
                {
                    "conn": conn,
                    "repo_root": repo_root,
                    "poll_interval": poll_interval,
                    "worker_id": worker_id,
                    "max_local_concurrent": max_local_concurrent,
                }
            )
        ),
    )
    stdout = StringIO()

    assert (
        workflow_commands._queue_command(
            ["worker", "--poll-interval", "0.25"],
            stdout=stdout,
        )
        == 0
    )
    assert observed["conn"] == "fake-conn"
    assert observed["poll_interval"] == 0.25
    assert observed["max_local_concurrent"] is None
    assert '"max_concurrent": 2' in stdout.getvalue()
    assert '"concurrency_source": "resource:auto"' in stdout.getvalue()
