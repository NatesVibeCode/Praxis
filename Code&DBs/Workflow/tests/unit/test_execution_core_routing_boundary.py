from __future__ import annotations

from registry.agent_config import AgentRegistry
from runtime.workflow import _execution_core


class _MissingAgentRegistry:
    def get(self, _agent_slug: str):
        return None


class _FakeConn:
    def execute(self, sql: str, *params):
        if "FROM workflow_runs WHERE run_id = $1" in sql:
            return [
                {
                    "run_id": params[0],
                    "current_state": "running",
                    "request_envelope": {},
                }
            ]
        return []


def test_unresolved_auto_job_without_runtime_profile_fails_closed(monkeypatch) -> None:
    completions: list[dict[str, object]] = []
    monkeypatch.setattr(_execution_core, "mark_running", lambda _conn, _job_id: None)
    monkeypatch.setattr(
        AgentRegistry,
        "load_from_postgres",
        lambda _conn: _MissingAgentRegistry(),
    )
    monkeypatch.setattr(
        _execution_core,
        "_runtime_profile_ref_for_run",
        lambda _conn, _run_id: "",
    )
    monkeypatch.setattr(
        _execution_core,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completions.append(kwargs),
    )

    _execution_core.execute_job(
        _FakeConn(),
        {
            "id": 42,
            "label": "build",
            "agent_slug": "auto/build",
            "run_id": "run-test",
        },
        "/tmp",
    )

    assert completions
    assert completions[0]["status"] == "failed"
    assert completions[0]["error_code"] == "route_unresolved_missing_runtime_profile"
    assert "refusing broad provider catalog routing" in completions[0]["stdout_preview"]


def test_unresolved_auto_job_with_runtime_profile_fails_closed(monkeypatch) -> None:
    completions: list[dict[str, object]] = []
    monkeypatch.setattr(_execution_core, "mark_running", lambda _conn, _job_id: None)
    monkeypatch.setattr(
        AgentRegistry,
        "load_from_postgres",
        lambda _conn: _MissingAgentRegistry(),
    )
    monkeypatch.setattr(
        _execution_core,
        "_runtime_profile_ref_for_run",
        lambda _conn, _run_id: "runtime_profile.build",
    )
    monkeypatch.setattr(
        _execution_core,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completions.append(kwargs),
    )

    _execution_core.execute_job(
        _FakeConn(),
        {
            "id": 43,
            "label": "build",
            "agent_slug": "auto/build",
            "run_id": "run-test",
        },
        "/tmp",
    )

    assert completions
    assert completions[0]["status"] == "failed"
    assert completions[0]["error_code"] == "route_unresolved_runtime_profile"
    assert "runtime_profile.build" in completions[0]["stdout_preview"]
