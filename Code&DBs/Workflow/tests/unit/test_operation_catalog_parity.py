from __future__ import annotations

from runtime.cqrs import bootstrap_registry, registry
import runtime.operation_catalog as operation_catalog


_SEEDED_OPERATION_ROWS = [
    {
        "operation_ref": "workflow-build-mutate",
        "operation_name": "workflow_build.mutate",
        "source_kind": "cqrs_command",
        "operation_kind": "command",
        "http_method": "POST",
        "http_path": "/api/workflows/{workflow_id}/build/{subpath:path}",
        "input_model_ref": "runtime.cqrs.commands.workflow_build.MutateWorkflowBuildCommand",
        "handler_ref": "runtime.cqrs.commands.workflow_build.handle_mutate_workflow_build",
        "authority_ref": "authority.workflow_build",
        "projection_ref": None,
        "posture": None,
        "idempotency_policy": None,
        "enabled": True,
        "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
    },
    {
        "operation_ref": "workflow-build-suggest-next",
        "operation_name": "workflow_build.suggest_next",
        "source_kind": "cqrs_query",
        "operation_kind": "query",
        "http_method": "POST",
        "http_path": "/api/workflows/{workflow_id}/build/suggest-next",
        "input_model_ref": "runtime.cqrs.commands.suggest_next.SuggestNextNodesCommand",
        "handler_ref": "runtime.cqrs.commands.suggest_next.handle_suggest_next_nodes",
        "authority_ref": "authority.capability_catalog",
        "projection_ref": "projection.capability_catalog",
        "posture": None,
        "idempotency_policy": None,
        "enabled": True,
        "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
    },
    {
        "operation_ref": "operator-roadmap-tree",
        "operation_name": "operator.roadmap_tree",
        "source_kind": "cqrs_query",
        "operation_kind": "query",
        "http_method": "GET",
        "http_path": "/api/operator/roadmap/tree/{root_roadmap_item_id}",
        "input_model_ref": "runtime.cqrs.queries.roadmap_tree.QueryRoadmapTree",
        "handler_ref": "runtime.cqrs.queries.roadmap_tree.handle_query_roadmap_tree",
        "authority_ref": "authority.roadmap_items",
        "projection_ref": "projection.roadmap_tree",
        "posture": None,
        "idempotency_policy": None,
        "enabled": True,
        "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
    },
    {
        "operation_ref": "operator-data-dictionary",
        "operation_name": "operator.data_dictionary",
        "source_kind": "cqrs_query",
        "operation_kind": "query",
        "http_method": "GET",
        "http_path": "/api/operator/data-dictionary",
        "input_model_ref": "runtime.cqrs.queries.data_dictionary.QueryDataDictionary",
        "handler_ref": "runtime.cqrs.queries.data_dictionary.handle_query_data_dictionary",
        "authority_ref": "authority.memory_entities",
        "projection_ref": "projection.memory_entities",
        "posture": None,
        "idempotency_policy": None,
        "enabled": True,
        "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
    },
]

_SEEDED_SOURCE_POLICIES = [
    {
        "policy_ref": "cqrs-command",
        "source_kind": "cqrs_command",
        "posture": "operate",
        "idempotency_policy": "non_idempotent",
        "enabled": True,
        "binding_revision": "binding.operation_catalog_source_policy_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_source_policy_registry.bootstrap.20260416",
    },
    {
        "policy_ref": "cqrs-query",
        "source_kind": "cqrs_query",
        "posture": "observe",
        "idempotency_policy": "read_only",
        "enabled": True,
        "binding_revision": "binding.operation_catalog_source_policy_registry.bootstrap.20260416",
        "decision_ref": "decision.operation_catalog_source_policy_registry.bootstrap.20260416",
    },
]


def test_seeded_operation_catalog_matches_live_cqrs_registry(monkeypatch) -> None:
    bootstrap_registry()
    monkeypatch.setattr(
        operation_catalog,
        "_list_operation_catalog_records",
        lambda conn, source_kind=None, include_disabled=False, limit=100: list(
            _SEEDED_OPERATION_ROWS
        ),
    )
    monkeypatch.setattr(
        operation_catalog,
        "_list_operation_source_policy_records",
        lambda conn, include_disabled=False, limit=100: list(_SEEDED_SOURCE_POLICIES),
    )

    seeded = {
        item.operation_name: item
        for item in operation_catalog.list_resolved_operation_definitions(
            object(),
            include_disabled=True,
        )
    }
    live = {
        binding["operation_name"]: binding
        for binding in registry.list_operation_bindings()
    }

    assert set(seeded) == set(live)

    for operation_name, resolved in seeded.items():
        binding = live[operation_name]
        assert binding["operation_kind"] == resolved.operation_kind
        assert binding["source_kind"] == resolved.source_kind
        assert binding["http_method"] == resolved.http_method
        assert binding["http_path"] == resolved.http_path
        assert binding["input_model_ref"] == resolved.input_model_ref
        assert binding["handler_ref"] == resolved.handler_ref
        assert binding["authority_ref"] == resolved.authority_ref
        assert binding["projection_ref"] == resolved.projection_ref
