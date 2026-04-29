from __future__ import annotations

from dataclasses import dataclass

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


class _RouteKnobConn:
    def execute(self, sql: str, *params):
        normalized = " ".join(sql.split())
        if "FROM task_type_routing" in normalized:
            return [
                {
                    "max_tokens": 32768,
                    "reasoning_control": {"default_level": "medium"},
                }
            ]
        if "FROM provider_model_candidates" in normalized:
            raise AssertionError("provider fallback should not run when route reasoning is explicit")
        raise AssertionError(f"Unexpected query: {normalized}")


@dataclass(frozen=True)
class _Agent:
    max_output_tokens: int
    model: str = "moonshotai/kimi-k2.6"


def test_route_execution_knobs_read_task_route_max_tokens_and_reasoning() -> None:
    knobs = _execution_core._route_execution_knobs(
        _RouteKnobConn(),
        route_task_type="build",
        provider_slug="openrouter",
        model_slug="moonshotai/kimi-k2.6",
    )

    assert knobs == {
        "max_output_tokens": 32768,
        "reasoning_effort": "medium",
    }


def test_agent_config_with_max_output_tokens_replaces_api_budget() -> None:
    adjusted = _execution_core._agent_config_with_max_output_tokens(
        _Agent(max_output_tokens=4096),
        32768,
    )

    assert adjusted.max_output_tokens == 32768
    assert adjusted.model == "moonshotai/kimi-k2.6"


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
