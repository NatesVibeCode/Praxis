from __future__ import annotations

import pytest

from runtime.operation_catalog import ResolvedOperationDefinition
from runtime.operation_catalog_bindings import (
    OperationBindingResolutionError,
    resolve_http_operation_binding,
)


def _definition(
    *,
    input_model_ref: str,
    handler_ref: str,
) -> ResolvedOperationDefinition:
    return ResolvedOperationDefinition(
        operation_ref="workflow-build-mutate",
        operation_name="workflow_build.mutate",
        source_kind="operation_command",
        operation_kind="command",
        http_method="POST",
        http_path="/api/workflows/{workflow_id}/build/{subpath:path}",
        input_model_ref=input_model_ref,
        handler_ref=handler_ref,
        authority_ref="authority.workflow_build",
        authority_domain_ref="authority.workflow_build",
        projection_ref=None,
        storage_target_ref="praxis.primary_postgres",
        input_schema_ref=input_model_ref,
        output_schema_ref="operation.output.default",
        idempotency_key_fields=[],
        required_capabilities={},
        allowed_callers=["cli", "mcp", "http", "workflow", "heartbeat"],
        timeout_ms=15000,
        receipt_required=True,
        event_required=True,
        event_type="workflow_build_mutate",
        projection_freshness_policy_ref=None,
        posture="operate",
        idempotency_policy="non_idempotent",
        enabled=True,
        operation_enabled=True,
        source_policy_ref="operation-command",
        source_policy_enabled=True,
        binding_revision="binding.operation_catalog_registry.bootstrap.20260416",
        decision_ref="decision.operation_catalog_registry.bootstrap.20260416",
    )


def test_resolve_http_operation_binding_loads_model_and_handler() -> None:
    binding = resolve_http_operation_binding(
        _definition(
            input_model_ref="runtime.operations.commands.workflow_build.MutateWorkflowBuildCommand",
            handler_ref="runtime.operations.commands.workflow_build.handle_mutate_workflow_build",
        )
    )

    assert binding.operation_name == "workflow_build.mutate"
    assert binding.command_class.__name__ == "MutateWorkflowBuildCommand"
    assert callable(binding.handler)
    assert binding.summary == "workflow_build.mutate"


def test_resolve_http_operation_binding_rejects_non_model_reference() -> None:
    with pytest.raises(OperationBindingResolutionError) as exc_info:
        resolve_http_operation_binding(
            _definition(
                input_model_ref="runtime.operations.commands.workflow_build.handle_mutate_workflow_build",
                handler_ref="runtime.operations.commands.workflow_build.handle_mutate_workflow_build",
            )
        )

    assert "Pydantic model class" in str(exc_info.value)


def test_resolve_http_operation_binding_rejects_missing_reference() -> None:
    with pytest.raises(OperationBindingResolutionError) as exc_info:
        resolve_http_operation_binding(
            _definition(
                input_model_ref="runtime.operations.commands.workflow_build.MutateWorkflowBuildCommand",
                handler_ref="runtime.operations.commands.workflow_build.missing_handler",
            )
        )

    assert "missing_handler" in str(exc_info.value)


@pytest.mark.parametrize(
    ("input_model_ref", "handler_ref", "expected_command_class"),
    [
        (
            "runtime.authority_objects.ListAuthorityDomainSummaryCommand",
            "runtime.authority_objects.handle_list_authority_domain_summary",
            "ListAuthorityDomainSummaryCommand",
        ),
        (
            "runtime.operations.commands.structured_documents.RecordStructuredDocumentContextSelectionCommand",
            "runtime.operations.commands.structured_documents.handle_record_context_selection",
            "RecordStructuredDocumentContextSelectionCommand",
        ),
        (
            "runtime.operations.queries.structured_documents.ListStructuredDocumentContextSelectionsQuery",
            "runtime.operations.queries.structured_documents.handle_list_context_selection_receipts",
            "ListStructuredDocumentContextSelectionsQuery",
        ),
        (
            "runtime.operations.commands.promote_experiment_winner_command.PromoteExperimentWinnerCommand",
            "runtime.operations.commands.promote_experiment_winner_command.handle_promote_experiment_winner",
            "PromoteExperimentWinnerCommand",
        ),
    ],
)
def test_resolve_http_operation_binding_loads_catalog_migrated_bindings(
    input_model_ref: str,
    handler_ref: str,
    expected_command_class: str,
) -> None:
    binding = resolve_http_operation_binding(
        _definition(
            input_model_ref=input_model_ref,
            handler_ref=handler_ref,
        )
    )

    assert binding.command_class.__name__ == expected_command_class
    assert callable(binding.handler)
