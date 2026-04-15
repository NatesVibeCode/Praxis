from __future__ import annotations

from datetime import datetime, timezone

from surfaces.mcp.tools import workflow as workflow_tools
from surfaces.workflow_bridge import WorkflowAcknowledgement, WorkflowClaimableWork
from runtime.claims import ClaimLeaseProposalSnapshot
from runtime.domain import RunState
from runtime.subscriptions import (
    WorkerInboxFact,
    WorkerSubscriptionAcknowledgement,
    WorkerSubscriptionBatch,
    WorkerSubscriptionCursor,
)
from runtime.workflow import _execution_core as _exec_mod


def test_run_status_payload_includes_submission_summary(monkeypatch) -> None:
    import runtime.workflow.unified as unified

    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        unified,
        "get_run_status",
        lambda _pg, _run_id: {
            "run_id": "run-1",
            "status": "running",
            "spec_name": "submission-spec",
            "total_jobs": 1,
            "completed_jobs": 0,
            "total_cost_usd": 0.0,
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "total_duration_ms": 0,
            "created_at": now,
            "lineage": {
                "child_run_id": "run-1",
                "child_workflow_id": "workflow.submission",
                "parent_run_id": "run-parent",
                "parent_job_label": "phase.dispatch",
                "dispatch_reason": "phase.spawn",
                "lineage_depth": 2,
            },
            "jobs": [
                {
                    "label": "build.codegen",
                    "status": "running",
                    "agent_slug": "agent",
                    "attempt": 1,
                    "duration_ms": 125,
                    "created_at": now,
                    "submission": {
                        "submission_id": "sub-1",
                        "result_kind": "code_change",
                        "summary": "sealed result",
                        "measured_summary": {"create": 0, "update": 1, "delete": 0, "rename": 0, "total": 1},
                        "comparison_status": "matched",
                        "latest_review": {"decision": "approve"},
                    },
                }
            ],
        },
    )
    monkeypatch.setattr(unified, "summarize_run_health", lambda *_args, **_kwargs: {"state": "healthy"})
    monkeypatch.setattr(unified, "summarize_run_recovery", lambda *_args, **_kwargs: {"mode": "monitor"})

    payload = workflow_tools._run_status_payload(object(), "run-1")

    assert payload["jobs"][0]["submission"] == {
        "submission_id": "sub-1",
        "result_kind": "code_change",
        "summary": "sealed result",
        "measured_summary": {"create": 0, "update": 1, "delete": 0, "rename": 0, "total": 1},
        "comparison_status": "matched",
        "integrity_status": "matched",
        "latest_review_decision": "approve",
    }
    assert payload["lineage"] == {
        "child_run_id": "run-1",
        "child_workflow_id": "workflow.submission",
        "parent_run_id": "run-parent",
        "parent_job_label": "phase.dispatch",
        "dispatch_reason": "phase.spawn",
        "lineage_depth": 2,
    }
    assert "submission-spec" in payload["dashboard"]
    assert "build.codegen" in payload["dashboard"]


def test_run_status_payload_omits_failure_classification_for_succeeded_jobs(monkeypatch) -> None:
    import runtime.workflow.unified as unified

    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        unified,
        "get_run_status",
        lambda _pg, _run_id: {
            "run_id": "run-2",
            "status": "succeeded",
            "spec_name": "success-spec",
            "total_jobs": 1,
            "completed_jobs": 1,
            "total_cost_usd": 0.0,
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "total_duration_ms": 0,
            "created_at": now,
            "jobs": [
                {
                    "label": "job.ok",
                    "status": "succeeded",
                    "agent_slug": "agent",
                    "attempt": 1,
                    "duration_ms": 10,
                    "created_at": now,
                    "stdout_preview": "ok",
                    "failure_category": "scope_violation",
                    "last_error_code": "scope_violation",
                }
            ],
        },
    )
    monkeypatch.setattr(unified, "summarize_run_health", lambda *_args, **_kwargs: {"state": "healthy"})
    monkeypatch.setattr(unified, "summarize_run_recovery", lambda *_args, **_kwargs: {"mode": "done"})

    payload = workflow_tools._run_status_payload(object(), "run-2")

    assert payload["jobs"][0]["job_label"] == "job.ok"
    assert "failure_classification" not in payload["jobs"][0]


def test_build_platform_context_warns_that_sandbox_commands_use_live_workspace() -> None:
    context = _exec_mod._build_platform_context("/Users/nate/Praxis")

    assert "Host repo root (persistence/output authority): /Users/nate/Praxis" in context
    assert "Command workspace: sandboxed workflow execution typically runs inside a hydrated workspace such as /workspace." in context
    assert "do not assume the host repo path exists inside the sandbox" in context


def test_dashboard_view_counts_missing_jobs_as_pending() -> None:
    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    view = workflow_tools._dashboard_view_from_status_data(
        {
            "run_id": "run-pending",
            "status": "queued",
            "spec_name": "pending-spec",
            "total_jobs": 3,
            "completed_jobs": 0,
            "created_at": now,
            "jobs": [
                {
                    "label": "job-a",
                    "status": "running",
                    "started_at": now,
                }
            ],
        },
        now=now,
    )

    assert view is not None
    assert view["pending_count"] == 2
    assert view["completed_jobs"] == 0
    assert view["total_jobs"] == 3


def test_run_submit_result_payload_uses_supplied_status_data_without_extra_query(monkeypatch) -> None:
    status_data = {
        "run_id": "dispatch_123",
        "status": "queued",
        "spec_name": "submit-spec",
        "total_jobs": 2,
        "completed_jobs": 0,
        "created_at": datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
        "jobs": [],
    }

    class _Pg:
        pass

    payload = workflow_tools._run_submit_result_payload(
        {
            "run_id": "dispatch_123",
            "status": "queued",
            "spec_name": "submit-spec",
            "total_jobs": 2,
        },
        pg=_Pg(),
        status_data=status_data,
        delivery={"dashboard_in_payload": True},
    )

    assert payload["run_id"] == "dispatch_123"
    assert "submit-spec" in payload["dashboard"]
    assert "2 pending" in payload["dashboard"]
    assert payload["delivery"] == {"dashboard_in_payload": True}


def test_next_poll_interval_resets_on_progress_and_backs_off_when_idle() -> None:
    assert workflow_tools._next_poll_interval(5.0, progress_changed=True) == 1.0
    assert workflow_tools._next_poll_interval(1.0, progress_changed=False) == 1.7
    assert workflow_tools._next_poll_interval(10.0, progress_changed=False) == 12.0


def test_dashboard_view_prefers_canonical_run_totals_over_partial_job_rows() -> None:
    now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

    view = workflow_tools._dashboard_view_from_status_data(
        {
            "run_id": "run-canonical",
            "status": "running",
            "spec_name": "canonical-spec",
            "total_jobs": 4,
            "completed_jobs": 2,
            "total_cost_usd": 4.25,
            "total_tokens_in": 1300,
            "total_tokens_out": 420,
            "created_at": now,
            "jobs": [
                {
                    "label": "job-a",
                    "status": "succeeded",
                    "duration_ms": 500,
                    "cost_usd": 1.25,
                    "token_input": 100,
                    "token_output": 40,
                },
                {
                    "label": "job-b",
                    "status": "running",
                    "started_at": now,
                },
            ],
        },
        now=now,
    )

    assert view is not None
    assert view["completed_jobs"] == 2
    assert view["pending_count"] == 1
    assert view["total_cost_usd"] == 4.25
    assert view["total_tokens_in"] == 1300
    assert view["total_tokens_out"] == 420


def test_tool_praxis_workflow_status_returns_structured_runtime_error(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"
        details = {"environment_variable": "WORKFLOW_DATABASE_URL"}

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "status", "run_id": "run-1"})

    assert payload == {
        "error": "db blocked",
        "error_code": "postgres.authority_unavailable",
        "details": {"environment_variable": "WORKFLOW_DATABASE_URL"},
    }


def test_tool_praxis_workflow_list_returns_structured_runtime_error(monkeypatch) -> None:
    class _BrokenConn:
        def execute(self, query: str, *args):
            raise RuntimeError("workflow_runs unavailable")

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: _BrokenConn())

    payload = workflow_tools.tool_praxis_workflow({"action": "list"})

    assert payload == {
        "error": "workflow_runs unavailable",
        "error_code": "workflow.list.failed",
    }


def test_tool_praxis_workflow_run_returns_structured_runtime_error_when_pg_unavailable(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[]}',
        encoding="utf-8",
    )

    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"
        details = {"environment_variable": "WORKFLOW_DATABASE_URL"}

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"spec_path": str(spec_path), "wait": False})

    assert payload == {
        "error": "db blocked",
        "error_code": "postgres.authority_unavailable",
        "details": {"environment_variable": "WORKFLOW_DATABASE_URL"},
    }


def test_tool_praxis_workflow_retry_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("retry db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "retry", "run_id": "run-1", "label": "build"})

    assert payload == {
        "error": "retry db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_cancel_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("cancel db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "cancel", "run_id": "run-1"})

    assert payload == {
        "error": "cancel db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_repair_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("repair db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "repair", "run_id": "run-1"})

    assert payload == {
        "error": "repair db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_inspect_returns_structured_runtime_error_when_pg_unavailable(monkeypatch) -> None:
    class _StructuredError(Exception):
        reason_code = "postgres.authority_unavailable"

    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: (_ for _ in ()).throw(_StructuredError("inspect db blocked")))

    payload = workflow_tools.tool_praxis_workflow({"action": "inspect", "run_id": "run-1"})

    assert payload == {
        "error": "inspect db blocked",
        "error_code": "postgres.authority_unavailable",
    }


def test_tool_praxis_workflow_claim_returns_serialized_bridge_work(monkeypatch) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    route_snapshot = ClaimLeaseProposalSnapshot(
        run_id="run-1",
        workflow_id="workflow-1",
        request_id="request-1",
        current_state=RunState.CLAIM_ACCEPTED,
        claim_id="claim-1",
        lease_id="lease-1",
        proposal_id="proposal-1",
        attempt_no=2,
        transition_seq=3,
        sandbox_group_id="sandbox-group-1",
        sandbox_session_id="sandbox-session-1",
        share_mode="shared",
        reuse_reason_code="packet.authoritative_fork",
        last_event_id="event-1",
    )
    cursor = WorkerSubscriptionCursor(
        subscription_id="dispatch:worker:bridge",
        run_id="run-1",
        last_acked_evidence_seq=1,
    )
    batch = WorkerSubscriptionBatch(
        cursor=cursor,
        next_cursor=WorkerSubscriptionCursor(
            subscription_id="dispatch:worker:bridge",
            run_id="run-1",
            last_acked_evidence_seq=2,
        ),
        facts=(
            WorkerInboxFact(
                inbox_fact_id="inbox:dispatch:worker:bridge:2",
                subscription_id="dispatch:worker:bridge",
                authority_table="workflow_outbox",
                authority_id="authority-1",
                envelope_kind="workflow.job.completed",
                workflow_id="workflow-1",
                run_id="run-1",
                request_id="request-1",
                evidence_seq=2,
                transition_seq=3,
                authority_recorded_at=now,
                envelope={"kind": "workflow.job.completed"},
            ),
        ),
        has_more=False,
    )
    work = WorkflowClaimableWork(route_snapshot=route_snapshot, inbox_batch=batch, claimable=True)

    class _Bridge:
        async def inspect_lane_catalog(self, *, as_of: datetime):  # pragma: no cover - not used here
            raise AssertionError("lane catalog should not be inspected for claim")

        def claimable_work(self, *, cursor: WorkerSubscriptionCursor, limit: int = 100):
            assert cursor.subscription_id == "dispatch:worker:bridge"
            assert cursor.run_id == "run-1"
            assert limit == 7
            return work

    monkeypatch.setattr(workflow_tools, "_build_workflow_bridge", lambda: _Bridge())

    payload = workflow_tools.tool_praxis_workflow(
        {
            "action": "claim",
            "subscription_id": "dispatch:worker:bridge",
            "run_id": "run-1",
            "last_acked_evidence_seq": 1,
            "limit": 7,
        }
    )

    assert payload["routed_to"] == "workflow_bridge"
    assert payload["view"] == "claimable_work"
    assert payload["claimable_work"]["claimable"] is True
    assert payload["claimable_work"]["inbox_batch"]["facts"][0]["inbox_fact_id"] == "inbox:dispatch:worker:bridge:2"


def test_tool_praxis_workflow_acknowledge_round_trips_claim_payload(monkeypatch) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    route_snapshot = ClaimLeaseProposalSnapshot(
        run_id="run-2",
        workflow_id="workflow-2",
        request_id="request-2",
        current_state=RunState.CLAIM_ACCEPTED,
        claim_id="claim-2",
        lease_id="lease-2",
        proposal_id="proposal-2",
        attempt_no=1,
        transition_seq=4,
        sandbox_group_id=None,
        sandbox_session_id=None,
        share_mode="exclusive",
        reuse_reason_code=None,
        last_event_id="event-2",
    )
    cursor = WorkerSubscriptionCursor(
        subscription_id="dispatch:worker:bridge",
        run_id="run-2",
        last_acked_evidence_seq=1,
    )
    batch = WorkerSubscriptionBatch(
        cursor=cursor,
        next_cursor=WorkerSubscriptionCursor(
            subscription_id="dispatch:worker:bridge",
            run_id="run-2",
            last_acked_evidence_seq=2,
        ),
        facts=(
            WorkerInboxFact(
                inbox_fact_id="inbox:dispatch:worker:bridge:2",
                subscription_id="dispatch:worker:bridge",
                authority_table="workflow_outbox",
                authority_id="authority-2",
                envelope_kind="workflow.job.completed",
                workflow_id="workflow-2",
                run_id="run-2",
                request_id="request-2",
                evidence_seq=2,
                transition_seq=4,
                authority_recorded_at=now,
                envelope={"kind": "workflow.job.completed"},
            ),
        ),
        has_more=False,
    )
    work = WorkflowClaimableWork(route_snapshot=route_snapshot, inbox_batch=batch, claimable=True)
    acknowledgement = WorkflowAcknowledgement(
        route_snapshot=route_snapshot,
        acknowledgement=WorkerSubscriptionAcknowledgement(
            subscription_id="dispatch:worker:bridge",
            run_id="run-2",
            through_evidence_seq=2,
            cursor=WorkerSubscriptionCursor(
                subscription_id="dispatch:worker:bridge",
                run_id="run-2",
                last_acked_evidence_seq=2,
            ),
        ),
    )
    captured: dict[str, object] = {}

    class _Bridge:
        def acknowledge(self, *, work: WorkflowClaimableWork, through_evidence_seq: int | None = None):
            captured["work"] = work
            captured["through_evidence_seq"] = through_evidence_seq
            return acknowledgement

    monkeypatch.setattr(workflow_tools, "_build_workflow_bridge", lambda: _Bridge())

    payload = workflow_tools.tool_praxis_workflow(
        {
            "action": "acknowledge",
            "work": workflow_tools._serialize(work),
            "through_evidence_seq": 2,
        }
    )

    assert captured["through_evidence_seq"] == 2
    assert captured["work"].inbox_batch.facts[0].evidence_seq == 2
    assert payload["routed_to"] == "workflow_bridge"
    assert payload["view"] == "acknowledge"
    assert payload["acknowledgement"]["acknowledgement"]["through_evidence_seq"] == 2


def test_tool_praxis_workflow_run_returns_async_payload_without_progress_emitter(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda *_args, **_kwargs: {
            "run_id": "dispatch_001",
            "status": "queued",
            "spec_name": "workflow",
            "total_jobs": 1,
            "command_id": "control.command.submit.1",
        },
    )

    payload = workflow_tools.tool_praxis_workflow({"spec_path": str(spec_path)})

    assert payload == {
        "run_id": "dispatch_001",
        "status": "queued",
        "spec_name": "workflow",
        "total_jobs": 1,
        "command_id": "control.command.submit.1",
        "command_status": "succeeded",
        "stream_url": "/api/workflow-runs/dispatch_001/stream",
        "status_url": "/api/workflow-runs/dispatch_001/status",
        "dashboard": "━━━ workflow | 0/1 | $0 | 0s ━━━\n  · 1 pending\n  ─ $0",
        "delivery": {
            "dashboard_in_payload": True,
            "live_channel": "none",
            "message_notifications": False,
            "progress_notifications": False,
            "wait_requested": True,
            "inline_polling": False,
        },
    }


def test_tool_praxis_workflow_run_passes_forced_run_id_to_submit(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda pg, *, spec_path, spec_name, total_jobs, run_id=None, force_fresh_run=False: captured.update(
            {
                "pg": pg,
                "spec_path": spec_path,
                "spec_name": spec_name,
                "total_jobs": total_jobs,
                "run_id": run_id,
                "force_fresh_run": force_fresh_run,
            }
        )
        or {
            "run_id": run_id or "dispatch_002",
            "status": "queued",
            "spec_name": spec_name,
            "total_jobs": total_jobs,
            "command_id": "control.command.submit.2",
        },
    )

    payload = workflow_tools.tool_praxis_workflow(
        {"spec_path": str(spec_path), "run_id": "workflow_forced_002"}
    )

    assert captured["run_id"] == "workflow_forced_002"
    assert captured["force_fresh_run"] is False
    assert payload["run_id"] == "workflow_forced_002"


def test_tool_praxis_workflow_run_passes_force_fresh_run_to_submit(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda pg, *, spec_path, spec_name, total_jobs, run_id=None, force_fresh_run=False: captured.update(
            {
                "pg": pg,
                "spec_path": spec_path,
                "spec_name": spec_name,
                "total_jobs": total_jobs,
                "run_id": run_id,
                "force_fresh_run": force_fresh_run,
            }
        )
        or {
            "run_id": "workflow_fresh_003",
            "status": "queued",
            "spec_name": spec_name,
            "total_jobs": total_jobs,
            "command_id": "control.command.submit.3",
        },
    )

    payload = workflow_tools.tool_praxis_workflow(
        {"spec_path": str(spec_path), "force_fresh_run": True}
    )

    assert captured["run_id"] is None
    assert captured["force_fresh_run"] is True
    assert payload["run_id"] == "workflow_fresh_003"


def test_tool_praxis_workflow_run_with_message_only_emitter_returns_async_payload(monkeypatch, tmp_path) -> None:
    spec_path = tmp_path / "workflow.queue.json"
    spec_path.write_text(
        '{"name":"workflow","workflow_id":"workflow","phase":"test","jobs":[{"label":"job-a"}]}',
        encoding="utf-8",
    )

    class _Spec:
        def __init__(self) -> None:
            self.name = "workflow"
            self.jobs = [{"label": "job-a"}]

        @classmethod
        def load(cls, _path: str):
            return cls()

    class _MessageOnlyEmitter:
        enabled = False
        progress_token = None

        def log(self, *_args, **_kwargs) -> None:
            raise AssertionError("message-only emitter should not trigger inline polling")

        def emit(self, *_args, **_kwargs) -> None:
            raise AssertionError("message-only emitter should not trigger inline polling")

    monkeypatch.setattr(workflow_tools, "_workflow_spec_mod", lambda: type("_SpecMod", (), {"WorkflowSpec": _Spec}))
    monkeypatch.setattr(workflow_tools._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(
        workflow_tools,
        "_submit_workflow_via_service_bus",
        lambda *_args, **_kwargs: {
            "run_id": "dispatch_002",
            "status": "queued",
            "spec_name": "workflow",
            "total_jobs": 1,
            "command_id": "control.command.submit.2",
        },
    )
    monkeypatch.setattr(
        workflow_tools,
        "_poll_run_to_completion",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inline polling should not run without progress token")),
    )

    payload = workflow_tools.tool_praxis_workflow(
        {"spec_path": str(spec_path)},
        _progress_emitter=_MessageOnlyEmitter(),
    )

    assert payload["run_id"] == "dispatch_002"
    assert payload["dashboard"] == "━━━ workflow | 0/1 | $0 | 0s ━━━\n  · 1 pending\n  ─ $0"
    assert payload["delivery"] == {
        "dashboard_in_payload": True,
        "live_channel": "notifications.message",
        "message_notifications": True,
        "progress_notifications": False,
        "wait_requested": True,
        "inline_polling": False,
    }


def test_poll_run_to_completion_uses_wakeup_listener_instead_of_blind_sleep(monkeypatch) -> None:
    import runtime.workflow.unified as unified
    import runtime.workflow_notifications as workflow_notifications

    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    statuses = [
        {
            "run_id": "dispatch_live",
            "status": "running",
            "spec_name": "workflow",
            "total_jobs": 2,
            "completed_jobs": 0,
            "created_at": now,
            "jobs": [],
        },
        {
            "run_id": "dispatch_live",
            "status": "running",
            "spec_name": "workflow",
            "total_jobs": 2,
            "completed_jobs": 1,
            "created_at": now,
            "jobs": [
                {
                    "label": "job-a",
                    "status": "succeeded",
                    "duration_ms": 400,
                    "cost_usd": 0.5,
                    "token_input": 100,
                    "token_output": 40,
                }
            ],
        },
        {
            "run_id": "dispatch_live",
            "status": "succeeded",
            "spec_name": "workflow",
            "total_jobs": 2,
            "completed_jobs": 2,
            "total_cost_usd": 0.75,
            "total_tokens_in": 180,
            "total_tokens_out": 70,
            "created_at": now,
            "finished_at": now,
            "jobs": [
                {
                    "label": "job-a",
                    "status": "succeeded",
                    "duration_ms": 400,
                    "cost_usd": 0.5,
                    "token_input": 100,
                    "token_output": 40,
                },
                {
                    "label": "job-b",
                    "status": "succeeded",
                    "duration_ms": 300,
                    "cost_usd": 0.25,
                    "token_input": 80,
                    "token_output": 30,
                },
            ],
        },
    ]

    monkeypatch.setattr(unified, "get_run_status", lambda _pg, _run_id: statuses.pop(0))
    monkeypatch.setattr(unified, "summarize_run_health", lambda *_args, **_kwargs: {"state": "healthy"})
    monkeypatch.setattr(unified, "summarize_run_recovery", lambda *_args, **_kwargs: {"mode": "done"})
    monkeypatch.setattr(
        "time.sleep",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("listener-backed wait should not fall back to sleep")),
    )

    class _ImmediateEvent:
        def __init__(self) -> None:
            self.wait_calls: list[float | None] = []

        def wait(self, timeout: float | None = None) -> bool:
            self.wait_calls.append(timeout)
            return True

        def clear(self) -> None:
            return None

        def set(self) -> None:
            return None

    fake_events: list[_ImmediateEvent] = []

    def _event_factory() -> _ImmediateEvent:
        event = _ImmediateEvent()
        fake_events.append(event)
        return event

    monkeypatch.setattr(workflow_tools.threading, "Event", _event_factory)

    class _FakeListener:
        def __init__(self) -> None:
            self.stop_called = False

        def stop(self) -> None:
            self.stop_called = True

    fake_listener = _FakeListener()
    captured_listener_kwargs: dict[str, object] = {}
    monkeypatch.setattr(
        workflow_notifications,
        "start_run_wakeup_listener",
        lambda **kwargs: captured_listener_kwargs.update(kwargs) or fake_listener,
    )

    logs: list[str] = []
    emits: list[dict[str, object]] = []

    class _Emitter:
        enabled = True

        def log(self, message: str, **_kwargs) -> None:
            logs.append(message)

        def emit(self, **kwargs) -> None:
            emits.append(dict(kwargs))

    payload = workflow_tools._poll_run_to_completion(
        object(),
        "dispatch_live",
        spec_name="workflow",
        total_jobs=2,
        emitter=_Emitter(),
        max_poll_seconds=5.0,
    )

    assert captured_listener_kwargs["run_id"] == "dispatch_live"
    assert fake_listener.stop_called is True
    assert fake_events
    assert fake_events[0].wait_calls
    assert payload["status"] == "succeeded"
    assert "job-b" in payload["dashboard"]
    assert any("1/2 jobs complete" in str(entry.get("message", "")) for entry in emits)
    assert any("Done: workflow" in message for message in logs)
