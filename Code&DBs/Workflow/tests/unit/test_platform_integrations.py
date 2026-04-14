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

    def _fake_submit(pg, spec_dict, run_id=None, parent_run_id=None, trigger_depth=0, packet_provenance=None):
        calls["spec_dict"] = spec_dict
        calls["parent_run_id"] = parent_run_id
        calls["trigger_depth"] = trigger_depth
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
    assert calls["trigger_depth"] == 0
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
