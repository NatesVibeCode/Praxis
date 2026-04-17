from __future__ import annotations

import pytest

import runtime.operation_catalog as operation_catalog


_QUERY_ROW = {
    "operation_ref": "workflow-build-suggest-next",
    "operation_name": "workflow_build.suggest_next",
    "source_kind": "operation_query",
    "operation_kind": "query",
    "http_method": "POST",
    "http_path": "/api/workflows/{workflow_id}/build/suggest-next",
    "input_model_ref": "runtime.operations.commands.suggest_next.SuggestNextNodesCommand",
    "handler_ref": "runtime.operations.commands.suggest_next.handle_suggest_next_nodes",
    "authority_ref": "authority.capability_catalog",
    "projection_ref": "projection.capability_catalog",
    "posture": None,
    "idempotency_policy": None,
    "enabled": True,
    "binding_revision": "binding.operation_catalog_registry.bootstrap.20260416",
    "decision_ref": "decision.operation_catalog_registry.bootstrap.20260416",
}

_QUERY_POLICY = {
    "policy_ref": "operation-query",
    "source_kind": "operation_query",
    "posture": "observe",
    "idempotency_policy": "read_only",
    "enabled": True,
    "binding_revision": "binding.operation_catalog_source_policy_registry.bootstrap.20260416",
    "decision_ref": "decision.operation_catalog_source_policy_registry.bootstrap.20260416",
}


def test_get_resolved_operation_definition_applies_source_policy_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        operation_catalog,
        "_load_operation_catalog_record",
        lambda conn, operation_ref: None,
    )
    monkeypatch.setattr(
        operation_catalog,
        "_load_operation_catalog_record_by_name",
        lambda conn, operation_name: dict(_QUERY_ROW),
    )
    monkeypatch.setattr(
        operation_catalog,
        "_list_operation_source_policy_records",
        lambda conn, include_disabled=False, limit=100: [dict(_QUERY_POLICY)],
    )

    resolved = operation_catalog.get_resolved_operation_definition(
        object(),
        operation_name="workflow_build.suggest_next",
    )

    assert resolved.operation_name == "workflow_build.suggest_next"
    assert resolved.posture == "observe"
    assert resolved.idempotency_policy == "read_only"
    assert resolved.enabled is True
    assert resolved.source_policy_ref == "operation-query"


def test_list_resolved_operation_definitions_preserves_operation_overrides(monkeypatch) -> None:
    monkeypatch.setattr(
        operation_catalog,
        "_list_operation_catalog_records",
        lambda conn, source_kind=None, include_disabled=False, limit=100: [
            {
                **_QUERY_ROW,
                "operation_ref": "workflow-build-mutate",
                "operation_name": "workflow_build.mutate",
                "source_kind": "operation_command",
                "operation_kind": "command",
                "posture": "build",
                "idempotency_policy": "idempotent",
            }
        ],
    )
    monkeypatch.setattr(
        operation_catalog,
        "_list_operation_source_policy_records",
        lambda conn, include_disabled=False, limit=100: [
            {
                **_QUERY_POLICY,
                "policy_ref": "operation-command",
                "source_kind": "operation_command",
                "posture": "operate",
                "idempotency_policy": "non_idempotent",
                "enabled": False,
            }
        ],
    )

    resolved = operation_catalog.list_resolved_operation_definitions(
        object(),
        include_disabled=True,
    )

    assert len(resolved) == 1
    assert resolved[0].posture == "build"
    assert resolved[0].idempotency_policy == "idempotent"
    assert resolved[0].enabled is False
    assert resolved[0].operation_enabled is True
    assert resolved[0].source_policy_enabled is False


def test_get_operation_catalog_record_requires_exactly_one_lookup_key() -> None:
    with pytest.raises(operation_catalog.OperationCatalogBoundaryError) as exc_info:
        operation_catalog.get_operation_catalog_record(
            object(),
            operation_ref="workflow-build-mutate",
            operation_name="workflow_build.mutate",
        )

    assert exc_info.value.status_code == 400
