from __future__ import annotations

from datetime import datetime, timezone

from storage.postgres.validators import PostgresConfigurationError
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
            "source_idea_id": "operator_idea.bound.operator.write",
        }
    )

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.roadmap_write"
    assert captured["payload"]["action"] == "commit"
    assert captured["payload"]["title"] == "Bound operator write"
    assert captured["payload"]["source_idea_id"] == "operator_idea.bound.operator.write"
    assert captured["payload"]["depends_on"] == []
    assert captured["payload"]["registry_paths"] == []


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


def test_mcp_operator_closeout_normalizes_missing_sequences(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_closeout({"action": "preview"})

    assert result == {"ok": True}
    assert captured["operation_name"] == "operator.work_item_closeout"
    assert captured["payload"]["bug_ids"] == []
    assert captured["payload"]["roadmap_item_ids"] == []


def test_mcp_operator_catalog_tool_returns_structured_error_when_authority_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(operator, "_subs", object())

    def _raise_unavailable(*_args, **_kwargs):
        raise PostgresConfigurationError(
            "postgres.authority_unavailable",
            "WORKFLOW_DATABASE_URL authority unavailable",
            details={"operation": "operator.replay_ready_bugs"},
        )

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _raise_unavailable)

    result = operator.tool_praxis_replay_ready_bugs({"limit": 10})

    assert result == {
        "ok": False,
        "error": "WORKFLOW_DATABASE_URL authority unavailable",
        "error_code": "postgres.authority_unavailable",
        "operation_name": "operator.replay_ready_bugs",
        "details": {"operation": "operator.replay_ready_bugs"},
    }


def test_mcp_operator_ideas_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_ideas(
        {
            "action": "file",
            "title": "First-class ideas authority",
            "summary": "Pre-commitment intake for roadmap candidates.",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.ideas"
    assert captured["operation_name"] == "operator.ideas"
    assert captured["payload"]["action"] == "file"
    assert captured["payload"]["title"] == "First-class ideas authority"
    assert captured["payload"]["source_kind"] == "operator"
    assert captured["payload"]["idea_ids"] == []


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


def test_mcp_operator_relations_functional_area_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_relations(
        {
            "action": "record_functional_area",
            "area_slug": "checkout",
            "title": "Checkout",
            "summary": "Shared checkout semantics",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.functional_area_record"
    assert captured["operation_name"] == "operator.functional_area_record"
    assert captured["payload"]["area_slug"] == "checkout"


def test_mcp_operator_relations_record_relation_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_relations(
        {
            "action": "record_relation",
            "relation_kind": "grouped_in",
            "source_kind": "roadmap_item",
            "source_ref": "roadmap_item.checkout",
            "target_kind": "functional_area",
            "target_ref": "functional_area.checkout",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.object_relation_record"
    assert captured["operation_name"] == "operator.object_relation_record"
    assert captured["payload"]["target_ref"] == "functional_area.checkout"


def test_mcp_operator_architecture_policy_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_operator_architecture_policy(
        {
            "authority_domain": "decision_tables",
            "policy_slug": "db-native-authority",
            "title": "Decision tables are DB-native authority",
            "rationale": "Keep authority in Postgres.",
            "decided_by": "nate",
            "decision_source": "cto.guidance",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.architecture_policy_record"
    assert captured["operation_name"] == "operator.architecture_policy_record"
    assert captured["payload"]["authority_domain"] == "decision_tables"


def test_mcp_circuits_list_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_circuits({"action": "list", "provider_slug": "openai"})

    assert result["operation_receipt"]["operation_name"] == "operator.circuit_states"
    assert captured["operation_name"] == "operator.circuit_states"
    assert captured["payload"]["provider_slug"] == "openai"


def test_mcp_circuits_history_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_circuits({"action": "history", "provider_slug": "openai"})

    assert result["operation_receipt"]["operation_name"] == "operator.circuit_history"
    assert captured["operation_name"] == "operator.circuit_history"
    assert captured["payload"]["provider_slug"] == "openai"


def test_mcp_circuits_override_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_circuits(
        {
            "action": "open",
            "provider_slug": "openai",
            "reason_code": "provider_outage",
            "rationale": "Provider outage",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.circuit_override"
    assert captured["operation_name"] == "operator.circuit_override"
    assert captured["payload"]["override_state"] == "open"


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


def test_mcp_status_snapshot_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_status_snapshot({"since_hours": 24})

    assert result["operation_receipt"]["operation_name"] == "operator.status_snapshot"
    assert captured["operation_name"] == "operator.status_snapshot"
    assert captured["payload"] == {"since_hours": 24}


def test_mcp_metrics_reset_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_metrics_reset(
        {
            "confirm": True,
            "before_date": "2026-04-16",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.metrics_reset"
    assert captured["operation_name"] == "operator.metrics_reset"
    assert captured["payload"] == {"confirm": True, "before_date": "2026-04-16"}


def test_mcp_bug_replay_provenance_backfill_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_bug_replay_provenance_backfill(
        {
            "limit": 25,
            "open_only": False,
            "receipt_limit": 3,
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.bug_replay_provenance_backfill"
    assert captured["operation_name"] == "operator.bug_replay_provenance_backfill"
    assert captured["payload"]["limit"] == 25
    assert captured["payload"]["open_only"] is False
    assert captured["payload"]["receipt_limit"] == 3


def test_mcp_semantic_bridges_backfill_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_semantic_bridges_backfill(
        {
            "include_object_relations": False,
            "include_operator_decisions": True,
            "as_of": "2026-04-16T21:00:00+00:00",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.semantic_bridges_backfill"
    assert captured["operation_name"] == "operator.semantic_bridges_backfill"
    assert captured["payload"]["include_object_relations"] is False
    assert captured["payload"]["include_operator_decisions"] is True
    assert captured["payload"]["include_roadmap_items"] is True
    assert captured["payload"]["as_of"] == datetime(2026, 4, 16, 21, 0, tzinfo=timezone.utc)


def test_mcp_semantic_projection_refresh_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_semantic_projection_refresh(
        {
            "limit": 25,
            "as_of": "2026-04-16T21:05:00+00:00",
        }
    )

    assert result["operation_receipt"]["operation_name"] == "operator.semantic_projection_refresh"
    assert captured["operation_name"] == "operator.semantic_projection_refresh"
    assert captured["payload"]["limit"] == 25
    assert captured["payload"]["as_of"] == datetime(2026, 4, 16, 21, 5, tzinfo=timezone.utc)


def test_mcp_run_status_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_run_status({"run_id": "run_123"})

    assert result["operation_receipt"]["operation_name"] == "operator.run_status"
    assert captured["operation_name"] == "operator.run_status"
    assert captured["payload"] == {"run_id": "run_123"}


def test_mcp_issue_backlog_uses_operation_catalog_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(operator, "_subs", object())

    def _execute(subsystems, *, operation_name: str, payload):
        captured["subsystems"] = subsystems
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"operation_receipt": {"operation_name": operation_name}}

    monkeypatch.setattr(operator, "execute_operation_from_subsystems", _execute)

    result = operator.tool_praxis_issue_backlog(
        {"limit": 12, "open_only": False, "status": "open"}
    )

    assert result["operation_receipt"]["operation_name"] == "operator.issue_backlog"
    assert captured["operation_name"] == "operator.issue_backlog"
    assert captured["payload"] == {"limit": 12, "open_only": False, "status": "open"}


def test_mcp_praxis_orient_delegates_to_rest_authority(monkeypatch) -> None:
    """praxis_orient must delegate to the same _handle_orient that serves POST /orient.

    Single authority for the orient payload prevents HTTP/MCP drift. The test
    pins the delegation contract so a future refactor cannot silently re-inline
    a parallel implementation.
    """

    captured: dict[str, object] = {}

    sentinel_subs = object()
    monkeypatch.setattr(operator, "_subs", sentinel_subs)

    def _fake_handle_orient(subs, body):
        captured["subs"] = subs
        captured["body"] = body
        return {
            "platform": "praxis-workflow",
            "standing_orders": [],
            "tool_guidance": {},
        }

    from surfaces.api.handlers import workflow_admin

    monkeypatch.setattr(workflow_admin, "_handle_orient", _fake_handle_orient)

    result = operator.tool_praxis_orient({})

    assert result["platform"] == "praxis-workflow"
    assert captured["subs"] is sentinel_subs
    assert captured["body"] == {}


def test_mcp_praxis_orient_registered_in_catalog() -> None:
    """praxis_orient must appear in the MCP catalog with the orient CLI alias."""

    from surfaces.mcp.catalog import get_tool_catalog

    catalog = get_tool_catalog()
    assert "praxis_orient" in catalog
    definition = catalog["praxis_orient"]
    assert definition.cli_recommended_alias == "orient"
    assert definition.cli_tier == "curated"
    assert "read" in definition.risk_levels
