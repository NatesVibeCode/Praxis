from __future__ import annotations

from types import SimpleNamespace

from runtime.operations.queries import workflow_solution
from surfaces.mcp.tools import solution as solution_tool


def test_workflow_solution_status_lifts_chain_groups_to_workflows(monkeypatch) -> None:
    def _fake_status(_conn, chain_id: str):
        assert chain_id == "workflow_chain_123"
        return {
            "chain_id": "workflow_chain_123",
            "program": "roadmap_burndown",
            "status": "running",
            "current_wave": "group_a",
            "coordination_path": "artifacts/workflow/solution.json",
            "why": "Burn down routing authority work.",
            "mode": "roadmap_burndown",
            "waves": [
                {
                    "wave_id": "group_a",
                    "status": "running",
                    "depends_on": [],
                    "blocked_by": None,
                    "runs": [
                        {
                            "run_id": "workflow_001",
                            "workflow_id": "workflow.plan.001",
                            "spec_path": "artifacts/workflow/a.queue.json",
                            "spec_name": "A",
                            "submission_status": "running",
                            "run_status": "running",
                            "completed_jobs": 0,
                            "total_jobs": 1,
                        }
                    ],
                }
            ],
        }

    monkeypatch.setattr(
        "runtime.workflow_chain.get_workflow_chain_status",
        _fake_status,
    )

    result = workflow_solution.handle_query_workflow_solution_status(
        workflow_solution.WorkflowSolutionStatusQuery(solution_id="workflow_chain_123"),
        SimpleNamespace(get_pg_conn=lambda: object()),
    )

    assert result["ok"] is True
    assert result["view"] == "workflow_solution"
    assert result["solution_id"] == "workflow_chain_123"
    assert result["authority"] == "workflow_solution"
    assert result["storage_authority"] == "workflow_chain"
    assert result["current_workflow_ids"] == ["workflow.plan.001"]
    assert result["workflows_total"] == 1
    assert result["workflows"][0]["run_id"] == "workflow_001"
    assert result["active_run_ids"] == ["workflow_001"]
    assert "waves" not in result
    assert "phases" not in result


def test_workflow_solution_list_returns_solution_summaries(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.workflow_chain.list_workflow_chains",
        lambda _conn, *, limit: [
            {
                "chain_id": "workflow_chain_123",
                "program": "roadmap_burndown",
                "status": "queued",
                "current_wave_id": "group_a",
                "coordination_path": "artifacts/workflow/solution.json",
            }
        ],
    )

    result = workflow_solution.handle_query_workflow_solution_status(
        workflow_solution.WorkflowSolutionStatusQuery(limit=5),
        SimpleNamespace(get_pg_conn=lambda: object()),
    )

    assert result["ok"] is True
    assert result["view"] == "workflow_solutions"
    assert result["solutions"] == [
        {
            "solution_id": "workflow_chain_123",
            "authority": "workflow_solution",
            "storage_authority": "workflow_chain",
            "name": "roadmap_burndown",
            "status": "queued",
            "coordination_path": "artifacts/workflow/solution.json",
            "created_at": None,
            "updated_at": None,
            "started_at": None,
            "finished_at": None,
        }
    ]


def test_praxis_solution_submit_uses_solution_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "status": "queued",
            "chain_id": "workflow_chain_123",
            "program": "roadmap_burndown",
            "current_wave": "group_a",
            "waves_total": 2,
            "waves_completed": 0,
        }

    monkeypatch.setattr(solution_tool, "workflow_database_env", lambda: {"dsn": "test"})
    monkeypatch.setattr(solution_tool, "execute_operation_from_env", _execute)

    result = solution_tool.tool_praxis_solution(
        {
            "action": "submit",
            "coordination_path": "artifacts/workflow/solution.json",
            "adopt_active": False,
        }
    )

    assert captured["operation_name"] == "workflow_solution.submit"
    assert captured["payload"] == {
        "coordination_path": "artifacts/workflow/solution.json",
        "adopt_active": False,
        "requested_by_kind": "mcp",
        "requested_by_ref": "praxis_solution.submit",
    }
    assert result["solution_id"] == "workflow_chain_123"
    assert "chain_id" not in result
    assert "current_phase" not in result
    assert "phases_total" not in result
    assert result["authority"] == "workflow_solution"
    assert result["storage_authority"] == "workflow_chain"
    assert "current_wave" not in result
    assert "waves_total" not in result


def test_praxis_solution_status_uses_solution_query(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name: str, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "view": "workflow_solution", "solution_id": "workflow_chain_123"}

    monkeypatch.setattr(solution_tool, "workflow_database_env", lambda: {"dsn": "test"})
    monkeypatch.setattr(solution_tool, "execute_operation_from_env", _execute)

    result = solution_tool.tool_praxis_solution(
        {"action": "status", "solution_id": "workflow_chain_123"}
    )

    assert captured["operation_name"] == "workflow_solution.status"
    assert captured["payload"] == {"solution_id": "workflow_chain_123"}
    assert result["ok"] is True


def test_workflow_solution_lifts_chain_groups_to_solution_workflows(monkeypatch) -> None:
    def _fake_status(_conn, chain_id: str):
        assert chain_id == "workflow_chain_123"
        return {
            "chain_id": "workflow_chain_123",
            "program": "roadmap_burndown",
            "status": "running",
            "current_wave": "wave_1_validation",
            "coordination_path": "artifacts/workflow/solution.json",
            "why": "Burn down routing authority work.",
            "mode": "roadmap_burndown",
            "waves": [
                {
                    "wave_id": "wave_0_triage",
                    "status": "succeeded",
                    "depends_on": [],
                    "blocked_by": None,
                    "runs": [
                        {
                            "run_id": "run_000",
                            "workflow_id": "workflow_000",
                            "run_status": "succeeded",
                        }
                    ],
                },
                {
                    "wave_id": "wave_1_validation",
                    "status": "running",
                    "depends_on": ["wave_0_triage"],
                    "blocked_by": "wave_0_triage",
                    "runs": [
                        {
                            "run_id": "run_001",
                            "workflow_id": "workflow_001",
                            "run_status": "running",
                        }
                    ],
                },
            ],
        }

    monkeypatch.setattr(
        "runtime.workflow_chain.get_workflow_chain_status",
        _fake_status,
    )

    result = workflow_solution.handle_query_workflow_solution_status(
        workflow_solution.WorkflowSolutionStatusQuery(solution_id="workflow_chain_123"),
        SimpleNamespace(get_pg_conn=lambda: object()),
    )

    assert result["current_workflow_ids"] == ["workflow_001"]
    assert result["workflow_ids"] == ["workflow_000", "workflow_001"]
    assert result["workflows"][1]["depends_on_workflow_ids"] == ["workflow_000"]
    assert result["workflows"][1]["blocked_by_workflow_ids"] == ["workflow_000"]
