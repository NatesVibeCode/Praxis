from __future__ import annotations

from runtime.operations.queries import operator_composed


def test_execution_truth_checks_status_and_run_trace(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    def _fake_component(_subsystems, *, operation_name: str, payload: dict | None = None):
        calls.append((operation_name, dict(payload or {})))
        if operation_name == "operator.status_snapshot":
            return {
                "total_workflows": 3,
                "pass_rate": 0.33,
                "queue_depth_total": 0,
                "queue_depth_status": "ok",
                "top_failure_codes": {"sandbox_error": 2},
                "operation_receipt": {"receipt_id": "status", "operation_name": operation_name},
            }
        if operation_name == "trace.walk":
            return {
                "nodes": [{"receipt_id": "r1"}],
                "events": [{"event_id": "e1"}],
                "operation_receipt": {"receipt_id": "trace", "operation_name": operation_name},
            }
        return {
            "payload": {},
            "operation_receipt": {"receipt_id": operation_name, "operation_name": operation_name},
        }

    monkeypatch.setattr(operator_composed, "_component", _fake_component)

    result = operator_composed.handle_query_execution_truth(
        operator_composed.QueryExecutionTruth(run_id="run_123"),
        object(),
    )

    assert result["view"] == "execution_truth"
    assert result["summary"]["truth_state"] == "run_observed"
    assert result["summary"]["actual_work_observed"] is True
    assert ("operator.status_snapshot", {"since_hours": 24}) in calls
    assert ("trace.walk", {"run_id": "run_123"}) in calls
    assert len(result["component_receipts"]) == 4


def test_next_work_ranks_assignment_bug_and_domain_candidates(monkeypatch) -> None:
    def _fake_component(_subsystems, *, operation_name: str, payload: dict | None = None):
        if operation_name == "operator.status_snapshot":
            return {"total_workflows": 1, "observability_state": "ready"}
        if operation_name == "operator.refactor_heatmap":
            return {
                "summary": {"top_domain": "provider_routing_admission"},
                "heatmap": [
                    {
                        "domain": "provider_routing_admission",
                        "title": "Provider routing/admission",
                        "score": 700,
                        "priority": "P1",
                        "metrics": {"open_architecture_bugs": 20, "p1_bugs": 14},
                        "recommended_change": "Move routing to one authority.",
                    }
                ],
            }
        if operation_name == "operator.bug_triage_packet":
            return {
                "summary": {"live_defect": 1},
                "bugs": [
                    {
                        "bug_id": "BUG-1",
                        "title": "Replay-ready failure",
                        "severity": "P1",
                        "status": "OPEN",
                        "classification": "live_defect",
                        "replay_ready": True,
                        "reason_codes": ["bug.replay_ready"],
                    }
                ],
            }
        if operation_name == "operator.work_assignment_matrix":
            return {
                "groups": [{"audit_group": "A_provider_catalog_authority", "count": 1}],
                "rows": [
                    {
                        "item_id": "BUG-2",
                        "title": "Provider catalog authority",
                        "priority": "P1",
                        "status": "OPEN",
                        "audit_group": "A_provider_catalog_authority",
                        "recommended_model_tier": "frontier",
                        "suggested_sequence": 1,
                        "assignment_reason": "Authority shape first.",
                    }
                ],
            }
        raise AssertionError(operation_name)

    monkeypatch.setattr(operator_composed, "_component", _fake_component)

    result = operator_composed.handle_query_next_work(
        operator_composed.QueryNextWork(limit=2),
        object(),
    )

    assert result["view"] == "next_work"
    assert result["summary"]["candidate_count"] == 3
    assert result["next_actions"][0]["candidate_kind"] == "bug"
    assert result["next_actions"][1]["candidate_kind"] == "work_item"


def test_provider_route_truth_reports_blocked_reasons(monkeypatch) -> None:
    def _fake_component(_subsystems, *, operation_name: str, payload: dict | None = None):
        if operation_name == "operator.provider_control_plane":
            return {
                "projection_freshness": {"freshness_status": "fresh"},
                "rows": [
                    {
                        "provider_slug": "openai",
                        "model_slug": "gpt-x",
                        "transport_type": "CLI",
                        "is_runnable": False,
                        "primary_removal_reason_code": "control.off",
                    },
                    {
                        "provider_slug": "openai",
                        "model_slug": "gpt-y",
                        "transport_type": "CLI",
                        "is_runnable": True,
                    },
                ],
            }
        if operation_name == "operator.model_access_control_matrix":
            return {"counts": {"by_control_state": {"off": 1, "on": 1}}}
        raise AssertionError(operation_name)

    monkeypatch.setattr(operator_composed, "_component", _fake_component)

    result = operator_composed.handle_query_provider_route_truth(
        operator_composed.QueryProviderRouteTruth(provider_slug="openai"),
        object(),
    )

    assert result["view"] == "provider_route_truth"
    assert result["summary"]["route_state"] == "mixed"
    assert result["summary"]["reason_counts"] == {"control.off": 1}
    assert result["summary"]["privacy_counts"]["by_state"] == {"not_required": 2}
    assert len(result["legal_routes"]) == 1
    assert len(result["blocked_routes"]) == 1


def test_provider_route_truth_applies_privacy_gate_to_runnable_api_route(monkeypatch) -> None:
    def _fake_component(_subsystems, *, operation_name: str, payload: dict | None = None):
        if operation_name == "operator.provider_control_plane":
            return {
                "projection_freshness": {"freshness_status": "fresh"},
                "rows": [
                    {
                        "provider_slug": "openrouter",
                        "model_slug": "unknown/model",
                        "transport_type": "API",
                        "is_runnable": True,
                        "capability_state": "runnable",
                        "effective_dispatch_state": "enabled",
                        "removal_reasons": [],
                        "source_refs": ["projection.private_provider_control_plane_snapshot"],
                    }
                ],
            }
        if operation_name == "operator.model_access_control_matrix":
            return {"counts": {"by_control_state": {"on": 1}}}
        raise AssertionError(operation_name)

    monkeypatch.setattr(operator_composed, "_component", _fake_component)

    result = operator_composed.handle_query_provider_route_truth(
        operator_composed.QueryProviderRouteTruth(provider_slug="openrouter"),
        object(),
    )

    assert result["summary"]["route_state"] == "blocked"
    assert result["summary"]["runnable_routes"] == 0
    assert result["summary"]["reason_counts"] == {"openrouter_policy.no_approved_endpoint": 1}
    assert result["summary"]["privacy_counts"]["by_state"] == {"blocked": 1}
    blocked = result["blocked_routes"][0]
    assert blocked["is_runnable"] is False
    assert blocked["capability_state"] == "removed"
    assert blocked["effective_dispatch_state"] == "disabled"
    assert blocked["privacy_posture"]["dispatch_allowed"] is False
    assert blocked["privacy_posture"]["reason_code"] == "openrouter_policy.no_approved_endpoint"
    assert blocked["removal_reasons"][0]["reason_code"] == "openrouter_policy.no_approved_endpoint"


def test_operation_forge_previews_register_payload_for_new_operation() -> None:
    class _Conn:
        def fetchrow(self, *_args):
            return None

    class _Subsystems:
        def get_pg_conn(self):
            return _Conn()

    result = operator_composed.handle_query_operation_forge(
        operator_composed.QueryOperationForge(
            operation_name="operator.example_truth",
            handler_ref="runtime.operations.queries.example.handle_query",
            input_model_ref="runtime.operations.queries.example.QueryExample",
            authority_domain_ref="authority.workflow_runs",
        ),
        _Subsystems(),
    )

    assert result["view"] == "operation_forge"
    assert result["state"] == "new_operation"
    assert result["ok_to_register"] is True
    assert result["register_operation_payload"]["operation_ref"] == "operator-example-truth"
    assert result["register_operation_payload"]["posture"] == "observe"
    assert result["register_operation_payload"]["idempotency_policy"] == "read_only"
    assert result["api_route"]["http_method"] == "GET"
    assert result["next_action_packet"]["register_command"].startswith(
        "praxis workflow tools call praxis_register_operation --input-json"
    )
    assert any(
        "scripts.generate_mcp_docs" in command
        for command in result["next_action_packet"]["fast_feedback_commands"]
    )
    assert "Do not dispatch from MCP directly into subsystem code." in result["reject_paths"]


def test_operation_forge_defaults_command_authority_and_requires_event_type() -> None:
    class _Conn:
        def fetchrow(self, *_args):
            return None

    class _Subsystems:
        def get_pg_conn(self):
            return _Conn()

    result = operator_composed.handle_query_operation_forge(
        operator_composed.QueryOperationForge(
            operation_name="operator.example_apply",
            operation_kind="command",
            tool_name="praxis_example_apply",
            recommended_alias="example-apply",
            handler_ref="runtime.operations.commands.example.handle_example_apply",
            input_model_ref="runtime.operations.commands.example.ExampleApplyCommand",
            authority_domain_ref="authority.workflow_runs",
        ),
        _Subsystems(),
    )

    assert result["state"] == "new_operation"
    assert result["ok_to_register"] is False
    assert result["register_operation_payload"]["operation_kind"] == "command"
    assert result["register_operation_payload"]["posture"] == "operate"
    assert result["register_operation_payload"]["idempotency_policy"] == "non_idempotent"
    assert result["api_route"]["http_method"] == "POST"
    assert result["tool"]["tool_name"] == "praxis_example_apply"
    assert result["tool"]["recommended_alias"] == "example-apply"
    assert "event_type" in result["missing_inputs"]
    assert "event_type" not in result["register_operation_payload_compact"]
    assert "operation_kind" in result["next_action_packet"]["register_command"]


def test_operation_forge_returns_real_tool_binding_for_existing_operation() -> None:
    class _Conn:
        def fetchrow(self, *_args):
            return {
                "operation_ref": "catalog.operation.register",
                "operation_name": "catalog_operation_register",
                "operation_kind": "command",
                "handler_ref": "runtime.operations.commands.catalog_operation_register.handle_register_operation",
                "input_model_ref": "runtime.operations.commands.catalog_operation_register.RegisterOperationCommand",
                "authority_domain_ref": "authority.cqrs",
                "http_method": "POST",
                "http_path": "/api/catalog_operation_register",
                "enabled": True,
            }

    class _Subsystems:
        def get_pg_conn(self):
            return _Conn()

    result = operator_composed.handle_query_operation_forge(
        operator_composed.QueryOperationForge(
            operation_name="catalog_operation_register",
            authority_domain_ref="authority.cqrs",
            operation_kind="command",
            posture="operate",
            idempotency_policy="idempotent",
        ),
        _Subsystems(),
    )

    assert result["state"] == "existing_operation"
    assert result["tool"]["binding_state"] == "catalog_bound"
    assert result["tool"]["tool_name"] == "praxis_register_operation"
    assert result["tool"]["risk"] == "write"
    assert result["tool"]["entrypoint"] == "workflow register-operation"
    assert result["tool"]["describe_command"] == "workflow tools describe praxis_register_operation"
    assert result["api_route"] == {
        "http_method": "POST",
        "http_path": "/api/catalog_operation_register",
    }
    assert result["tool_bindings"][0]["tool_name"] == "praxis_register_operation"
