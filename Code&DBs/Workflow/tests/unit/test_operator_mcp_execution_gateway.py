from __future__ import annotations

from datetime import datetime, timezone

from surfaces.mcp.tools import operator


def test_mcp_operator_write_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_write(
        {
            "action": "commit",
            "title": "Bound operator write",
            "intent_brief": "Use one execution gateway",
        }
    )

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.roadmap_write"
    assert captured["payload"]["action"] == "commit"
    assert captured["payload"]["title"] == "Bound operator write"


def test_mcp_operator_closeout_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_closeout(
        {
            "action": "preview",
            "bug_ids": ["bug.1"],
            "roadmap_item_ids": ["roadmap_item.1"],
        }
    )

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.work_item_closeout"
    assert captured["payload"]["bug_ids"] == ["bug.1"]


def test_mcp_operator_roadmap_view_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Subs:
        def get_pg_conn(self):
            class _Conn:
                def execute(self, *_args, **_kwargs):
                    return [{"roadmap_item_id": "roadmap_item.root"}]

            return _Conn()

    monkeypatch.setattr(operator, "_subs", _Subs())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"rendered_markdown": "# root"}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_roadmap_view({})

    assert result == {"rendered_markdown": "# root"}
    assert captured["operation_name"] == "operator.roadmap_tree"
    assert captured["payload"]["root_roadmap_item_id"] == "roadmap_item.root"


def test_mcp_operator_native_primary_cutover_gate_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_native_primary_cutover_gate(
        {
            "decided_by": "nate",
            "decision_source": "operator",
            "rationale": "Ready for cutover",
            "workflow_class_id": "workflow_class.runtime_probe",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.native_primary_cutover_gate"
    assert captured["operation_name"] == "operator.native_primary_cutover_gate"
    assert captured["payload"]["workflow_class_id"] == "workflow_class.runtime_probe"


def test_mcp_operator_decisions_record_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_decisions(
        {
            "action": "record",
            "decision_key": "architecture-policy::provider-onboarding::registry-owned-catalog-exposed",
            "decision_kind": "architecture_policy",
            "title": "Provider onboarding stays registry-owned and catalog-exposed",
            "rationale": "Canonical onboarding includes post-onboarding sync.",
            "decided_by": "codex",
            "decision_source": "implementation",
            "decision_scope_kind": "authority_domain",
            "decision_scope_ref": "provider_onboarding",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.decision_record"
    assert captured["operation_name"] == "operator.decision_record"
    assert captured["payload"]["decision_kind"] == "architecture_policy"


def test_mcp_operator_decisions_list_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_decisions(
        {
            "action": "list",
            "decision_kind": "architecture_policy",
            "decision_scope_kind": "authority_domain",
            "decision_scope_ref": "provider_onboarding",
            "limit": 50,
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.decision_list"
    assert captured["operation_name"] == "operator.decision_list"
    assert captured["payload"]["decision_scope_ref"] == "provider_onboarding"


def test_mcp_semantic_assertions_list_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_semantic_assertions(
        {
            "action": "list",
            "predicate_slug": "grouped_in",
            "subject_kind": "bug",
            "limit": 25,
        }
    )

    assert result["operation_receipt"]["operation_name"] == "semantic_assertions.list"
    assert captured["operation_name"] == "semantic_assertions.list"
    assert captured["payload"]["predicate_slug"] == "grouped_in"


def test_mcp_semantic_assertions_record_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_semantic_assertions(
        {
            "action": "record_assertion",
            "predicate_slug": "grouped_in",
            "subject_kind": "bug",
            "subject_ref": "bug.checkout.1",
            "object_kind": "functional_area",
            "object_ref": "functional_area.checkout",
            "source_kind": "operator",
            "source_ref": "nate",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "semantic_assertions.record"
    assert captured["operation_name"] == "semantic_assertions.record"
    assert captured["payload"]["subject_ref"] == "bug.checkout.1"


def test_mcp_maintenance_backfill_semantic_bridges_uses_operator_frontdoor(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Frontdoor:
        def backfill_semantic_bridges(self, **kwargs):
            captured.update(kwargs)
            return {"backfill": "semantic"}

    monkeypatch.setattr(operator, "OperatorControlFrontdoor", _Frontdoor)

    result = operator.tool_praxis_maintenance(
        {
            "action": "backfill_semantic_bridges",
            "include_object_relations": False,
            "include_operator_decisions": True,
            "as_of": "2026-04-16T21:00:00+00:00",
        }
    )

    assert result == {"backfill": "semantic"}
    assert captured["include_object_relations"] is False
    assert captured["include_operator_decisions"] is True
    assert captured["as_of"] == datetime(2026, 4, 16, 21, 0, tzinfo=timezone.utc)


def test_mcp_maintenance_refresh_semantic_projection_uses_cursor_consumer(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _consume(*, limit: int, as_of):
        captured["limit"] = limit
        captured["as_of"] = as_of
        return {"refreshed": True, "ending_cursor": 9}

    monkeypatch.setattr(operator, "consume_semantic_projection_events", _consume)

    result = operator.tool_praxis_maintenance(
        {
            "action": "refresh_semantic_projection",
            "limit": 25,
            "as_of": "2026-04-16T21:05:00+00:00",
        }
    )

    assert result == {
        "semantic_projection_refresh": {
            "refreshed": True,
            "ending_cursor": 9,
        }
    }
    assert captured["limit"] == 25
    assert captured["as_of"] == datetime(2026, 4, 16, 21, 5, tzinfo=timezone.utc)
