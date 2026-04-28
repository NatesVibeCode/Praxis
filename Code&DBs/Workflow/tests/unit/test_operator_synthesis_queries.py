from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from runtime.operations.queries import operator_synthesis


class _FakeTool:
    def __init__(
        self,
        *,
        name: str,
        action: str = "call",
        risk: str = "read",
        required: tuple[str, ...] = (),
        kind: str = "search",
    ) -> None:
        self.name = name
        self.default_action = action
        self._risk = risk
        self._required = required
        self.kind = kind
        self.cli_entrypoint = f"workflow tools call {name}"
        self.cli_describe_command = f"workflow tools describe {name}"
        self.cli_surface = "operator"
        self.cli_tier = "advanced"
        self.cli_replacement = None
        self.input_schema: dict[str, Any] = {}
        self.action_requirements: dict[str, Any] = {}

    def risk_for_selector(self, _selector: object | None = None) -> str:
        return self._risk

    def required_args_for_action(self, _action: object | None = None) -> tuple[str, ...]:
        return self._required


def test_legal_tools_returns_legal_blocked_and_gaps(monkeypatch) -> None:
    catalog = {
        "praxis_read": _FakeTool(name="praxis_read", action="read"),
        "praxis_run_view": _FakeTool(
            name="praxis_run_view",
            action="status",
            required=("run_id",),
        ),
        "praxis_write": _FakeTool(
            name="praxis_write",
            action="write",
            risk="write",
        ),
    }
    monkeypatch.setattr("surfaces.mcp.catalog.get_tool_catalog", lambda: catalog)
    monkeypatch.setattr(
        operator_synthesis,
        "_compile_preview",
        lambda _intent, _subsystems: (None, None),
    )

    result = operator_synthesis.handle_query_legal_tools(
        operator_synthesis.QueryLegalTools(limit=10),
        SimpleNamespace(get_pg_conn=lambda: object()),
    )

    assert [row["tool"] for row in result["legal_actions"]] == ["praxis_read"]
    blocked = {row["tool"]: row for row in result["blocked_actions"]}
    assert blocked["praxis_run_view"]["blocked_reasons"] == ["missing_required:run_id"]
    assert blocked["praxis_write"]["blocked_reasons"] == [
        "requires_mutating_or_session_scope"
    ]
    assert {
        (gap["tool"], gap["field"])
        for gap in result["typed_gaps"]
        if gap["gap_type"] == "missing_required_input"
    } == {("praxis_run_view", "run_id")}


class _FakeExecutionConn:
    def __init__(self, *, fired: bool) -> None:
        self._fired = fired
        self._now = datetime.now(timezone.utc)

    def execute(self, sql: str, *_args: object) -> list[dict[str, Any]]:
        if "FROM workflow_runs" in sql:
            return [
                {
                    "run_id": "run_1",
                    "workflow_id": "workflow.test",
                    "request_id": "req_1",
                    "current_state": "running" if self._fired else "queued",
                    "requested_at": self._now - timedelta(minutes=2),
                    "admitted_at": self._now - timedelta(minutes=2),
                    "started_at": self._now - timedelta(minutes=2) if self._fired else None,
                    "finished_at": None,
                    "last_event_id": None,
                    "request_envelope": {"name": "Test", "total_jobs": 1},
                }
            ]
        if "FROM workflow_claim_lease_proposal_runtime" in sql:
            return (
                [
                    {
                        "claim_id": "claim_1",
                        "sandbox_session_id": None,
                        "created_at": self._now - timedelta(minutes=1),
                        "updated_at": self._now - timedelta(minutes=1),
                    }
                ]
                if self._fired
                else []
            )
        if "COUNT(*) AS total_jobs" in sql and "FROM workflow_jobs" in sql:
            return [
                {
                    "total_jobs": 1 if self._fired else 0,
                    "started_jobs": 1 if self._fired else 0,
                    "heartbeat_jobs": 1 if self._fired else 0,
                    "observed_jobs": 1 if self._fired else 0,
                    "latest_heartbeat_at": (
                        self._now - timedelta(seconds=10) if self._fired else None
                    ),
                    "latest_started_at": (
                        self._now - timedelta(minutes=1) if self._fired else None
                    ),
                    "latest_finished_at": None,
                }
            ]
        if "SELECT label, job_type, status" in sql:
            return (
                [
                    {
                        "label": "job",
                        "job_type": "dispatch",
                        "status": "running",
                        "started_at": self._now - timedelta(minutes=1),
                        "heartbeat_at": self._now - timedelta(seconds=10),
                    }
                ]
                if self._fired
                else []
            )
        if "FROM workflow_job_submissions" in sql:
            return [
                {
                    "submission_count": 0,
                    "accepted_submission_count": 0,
                    "latest_submission_at": None,
                }
            ]
        if "FROM workflow_outbox" in sql:
            return [
                {
                    "outbox_count": 0,
                    "receipt_outbox_count": 0,
                    "latest_outbox_at": None,
                }
            ]
        if "FROM authority_events" in sql:
            return [{"event_count": 0, "latest_event_at": None}]
        if "FROM sandbox_sessions" in sql:
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def test_execution_proof_marks_current_execution_with_fresh_runtime_evidence() -> None:
    result = operator_synthesis.handle_query_execution_proof(
        operator_synthesis.QueryExecutionProof(
            run_id="run_1",
            stale_after_seconds=180,
            include_trace=False,
        ),
        SimpleNamespace(get_pg_conn=lambda: _FakeExecutionConn(fired=True)),
    )

    assert result["fired"] is True
    assert result["currently_executing"] is True
    assert result["verdict"] == "executing"


def test_execution_proof_does_not_treat_queued_label_as_fired() -> None:
    result = operator_synthesis.handle_query_execution_proof(
        operator_synthesis.QueryExecutionProof(run_id="run_1", include_trace=False),
        SimpleNamespace(get_pg_conn=lambda: _FakeExecutionConn(fired=False)),
    )

    assert result["fired"] is False
    assert result["currently_executing"] is False
    assert result["verdict"] == "not_fired"
    assert "started_or_heartbeat_job" in result["missing_evidence"]
