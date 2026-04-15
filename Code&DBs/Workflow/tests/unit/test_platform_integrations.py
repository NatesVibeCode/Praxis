from __future__ import annotations

import runtime.integrations as integrations_mod
import runtime.integrations.platform as platform_mod


def test_execute_integration_routes_notifications_send_to_notification_infra(monkeypatch) -> None:
    payloads: list[dict[str, object]] = []

    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "notifications",
            "auth_status": "connected",
            "capabilities": [{"action": "send"}],
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "dispatch_notification_payload",
        lambda payload: payloads.append(payload) or 2,
    )

    result = integrations_mod.execute_integration(
        "notifications",
        "send",
        {"message": "Database drift detected", "status": "warning"},
        object(),
    )

    assert result["status"] == "succeeded"
    assert result["error"] is None
    assert result["summary"] == "Notification sent via 2 configured channel(s)."
    assert payloads == [
        {
            "kind": "integration_notification",
            "title": "Database drift detected",
            "message": "Database drift detected",
            "status": "warning",
            "sent_at": payloads[0]["sent_at"],
            "metadata": {},
        }
    ]


def test_execute_integration_reports_notifications_unconfigured_as_skipped(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "notifications",
            "auth_status": "connected",
            "capabilities": [{"action": "send"}],
        },
    )
    monkeypatch.setattr(platform_mod, "dispatch_notification_payload", lambda payload: 0)

    result = integrations_mod.execute_integration(
        "notifications",
        "send",
        {"message": "No channels"},
        object(),
    )

    assert result["status"] == "skipped"
    assert result["error"] is None
    assert result["summary"] == "Notifications are not configured; nothing was sent."


def test_execute_integration_routes_workflow_invoke_to_saved_workflow(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "workflow",
            "auth_status": "connected",
            "capabilities": [{"action": "invoke"}],
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "_load_workflow_record",
        lambda _pg, workflow_id: {
            "id": workflow_id,
            "name": "Child Workflow",
            "definition": {"definition_revision": "def_123"},
            "compiled_spec": {"definition_revision": "def_123", "jobs": [{"label": "step-1"}]},
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "_current_compiled_spec",
        lambda definition, compiled_spec: {
            "name": "Child Workflow",
            "definition_revision": definition["definition_revision"],
            "jobs": list(compiled_spec["jobs"]),
        },
    )

    def _fake_submit(
        pg,
        spec_dict,
        run_id=None,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        trigger_depth=0,
        lineage_depth=None,
        packet_provenance=None,
    ):
        calls["spec_dict"] = spec_dict
        calls["parent_run_id"] = parent_run_id
        calls["parent_job_label"] = parent_job_label
        calls["dispatch_reason"] = dispatch_reason
        calls["trigger_depth"] = trigger_depth
        calls["lineage_depth"] = lineage_depth
        calls["packet_provenance"] = packet_provenance
        return {"run_id": "workflow_child_001"}

    monkeypatch.setattr(platform_mod, "_submit_workflow_inline", _fake_submit)
    monkeypatch.setattr(
        platform_mod,
        "_record_workflow_invocation",
        lambda _pg, workflow_id: calls.setdefault("workflow_id", workflow_id),
    )
    monkeypatch.setattr(
        platform_mod,
        "_record_system_event",
        lambda _pg, event_type, source_id, source_type, payload: calls.setdefault(
            "event",
            {
                "event_type": event_type,
                "source_id": source_id,
                "source_type": source_type,
                "payload": payload,
            },
        ),
    )

    result = integrations_mod.execute_integration(
        "workflow",
        "invoke",
        {
            "workflow_id": "wf_child",
            "parent_run_id": "run_parent",
            "payload": {"severity": "p1"},
        },
        object(),
    )

    assert result == {
        "status": "succeeded",
        "data": {
            "workflow_id": "wf_child",
            "workflow_name": "Child Workflow",
            "run_id": "workflow_child_001",
        },
        "summary": "Invoked workflow Child Workflow -> workflow_child_001",
        "error": None,
    }
    assert calls["parent_run_id"] == "run_parent"
    assert calls["parent_job_label"] is None
    assert calls["dispatch_reason"] == "integration.workflow.invoke"
    assert calls["trigger_depth"] == 0
    assert calls["lineage_depth"] is None
    assert calls["workflow_id"] == "wf_child"
    assert calls["packet_provenance"]["input_payload"] == {"severity": "p1"}
    assert calls["event"]["event_type"] == "integration.workflow.invoke"


def test_execute_integration_requires_workflow_id_for_workflow_invoke(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "workflow",
            "auth_status": "connected",
            "capabilities": [{"action": "invoke"}],
        },
    )

    result = integrations_mod.execute_integration(
        "workflow",
        "invoke",
        {},
        object(),
    )

    assert result["status"] == "failed"
    assert result["error"] == "missing_workflow_id"


def test_execute_integration_routes_workflow_cancel_to_runtime_cancel(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "workflow",
            "auth_status": "connected",
            "capabilities": [{"action": "cancel"}],
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "_cancel_workflow_run",
        lambda pg, run_id, include_running=False: {
            "run_id": run_id,
            "cancelled_jobs": 2,
            "labels": ["alpha", "beta"],
            "run_status": {"status": "cancelled"},
            "include_running": include_running,
        },
    )

    result = integrations_mod.execute_integration(
        "workflow",
        "cancel",
        {"run_id": "run_007", "include_running": True},
        object(),
    )

    assert result == {
        "status": "succeeded",
        "data": {
            "run_id": "run_007",
            "cancelled_jobs": 2,
            "labels": ["alpha", "beta"],
            "run_status": {"status": "cancelled"},
            "include_running": True,
        },
        "summary": "Workflow run run_007 cancel requested.",
        "error": None,
    }


def test_execute_integration_workflow_cancel_requires_run_id(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "workflow",
            "auth_status": "connected",
            "capabilities": [{"action": "cancel"}],
        },
    )

    result = integrations_mod.execute_integration(
        "workflow",
        "cancel",
        {},
        object(),
    )

    assert result["status"] == "failed"
    assert result["error"] == "workflow_cancel_missing_run_id"


def test_execute_integration_routes_dispatch_job_to_workflow_inline(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "praxis-dispatch",
            "auth_status": "connected",
            "capabilities": [{"action": "dispatch_job"}],
        },
    )

    def _fake_submit(
        pg,
        spec_dict,
        run_id=None,
        parent_run_id=None,
        parent_job_label=None,
        dispatch_reason=None,
        trigger_depth=0,
        lineage_depth=None,
        packet_provenance=None,
    ):
        calls["spec"] = spec_dict
        calls["parent_run_id"] = parent_run_id
        calls["parent_job_label"] = parent_job_label
        calls["dispatch_reason"] = dispatch_reason
        calls["trigger_depth"] = trigger_depth
        calls["lineage_depth"] = lineage_depth
        calls["packet_provenance"] = packet_provenance
        return {"run_id": "run_123", "command_status": "submitted"}

    monkeypatch.setattr(platform_mod, "_submit_workflow_inline", _fake_submit)

    result = integrations_mod.execute_integration(
        "praxis-dispatch",
        "dispatch_job",
        {
            "workflow_spec": {"name": "dispatch-demo", "jobs": [{"label": "job-a"}]},
            "parent_run_id": "run_parent",
            "trigger_depth": 2,
            "payload": {"hello": "world"},
        },
        object(),
    )

    assert result == {
        "status": "succeeded",
        "data": {
            "run_id": "run_123",
            "command_status": "submitted",
            "status": "queued",
        },
        "summary": "Dispatched workflow spec via integration -> run_123",
        "error": None,
    }
    assert calls["parent_run_id"] == "run_parent"
    assert calls["parent_job_label"] is None
    assert calls["dispatch_reason"] == "integration.dispatch_job"
    assert calls["trigger_depth"] == 2
    assert calls["lineage_depth"] is None
    assert calls["packet_provenance"]["input_payload"] == {"hello": "world"}


def test_execute_integration_dispatch_job_requires_workflow_spec(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "praxis-dispatch",
            "auth_status": "connected",
            "capabilities": [{"action": "dispatch_job"}],
        },
    )

    result = integrations_mod.execute_integration(
        "praxis-dispatch",
        "dispatch_job",
        {},
        object(),
    )

    assert result["status"] == "failed"
    assert result["error"] == "dispatch_job_missing_spec"


def test_execute_integration_routes_check_status_to_runtime_status(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "praxis-dispatch",
            "auth_status": "connected",
            "capabilities": [{"action": "check_status"}],
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "_get_run_status",
        lambda pg, run_id: {"run_id": run_id, "status": "running"},
    )

    result = integrations_mod.execute_integration(
        "praxis-dispatch",
        "check_status",
        {"run_id": "run_007"},
        object(),
    )

    assert result == {
        "status": "succeeded",
        "data": {"run_id": "run_007", "status": "running"},
        "summary": "Run run_007 status retrieved.",
        "error": None,
    }


def test_execute_integration_check_status_requires_run_id(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "praxis-dispatch",
            "auth_status": "connected",
            "capabilities": [{"action": "check_status"}],
        },
    )

    result = integrations_mod.execute_integration(
        "praxis-dispatch",
        "check_status",
        {},
        object(),
    )

    assert result["status"] == "failed"
    assert result["error"] == "check_status_missing_run_id"


def test_execute_integration_routes_search_receipts(monkeypatch) -> None:
    monkeypatch.setattr(
        integrations_mod,
        "_load_integration_authority",
        lambda _pg, _integration_id: {
            "id": "praxis-dispatch",
            "auth_status": "connected",
            "capabilities": [{"action": "search_receipts"}],
        },
    )
    monkeypatch.setattr(
        platform_mod,
        "_search_receipts",
        lambda query, *, limit, status=None, agent=None, workflow_id=None: [
            {"receipt_id": "r1", "status": status or "succeeded"},
            {"receipt_id": "r2", "workflow_id": workflow_id or "w1"},
        ] if query == "token" and limit == 2 else [],
    )

    result = integrations_mod.execute_integration(
        "praxis-dispatch",
        "search_receipts",
        {"query": "token", "limit": 2},
        object(),
    )

    assert result == {
        "status": "succeeded",
        "data": {
            "query": "token",
            "results": [
                {"receipt_id": "r1", "status": "succeeded"},
                {"receipt_id": "r2", "workflow_id": "w1"},
            ],
            "count": 2,
        },
        "summary": "search_receipts found 2 result(s).",
        "error": None,
    }
