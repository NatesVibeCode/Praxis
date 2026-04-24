"""Regression tests for workflow-definition admission collisions."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from contracts.domain import WorkflowNodeContract, WorkflowRequest
from runtime.workflow._admission import (
    WorkflowSubmitConflict,
    _graph_request_envelope,
    _persist_graph_authority,
    _translate_definition_collision,
)
from runtime.workflow._shared import _definition_version_for_hash


class _DiagStub:
    def __init__(self, constraint_name: str) -> None:
        self.constraint_name = constraint_name


class _UniqueViolationStub(Exception):
    def __init__(self, message: str, *, constraint_name: str) -> None:
        super().__init__(message)
        self.diag = _DiagStub(constraint_name)


def test_translator_catches_constraint_name_from_diag() -> None:
    exc = _UniqueViolationStub(
        "duplicate key value violates unique constraint",
        constraint_name="workflow_definitions_workflow_id_definition_version_key",
    )

    translated = _translate_definition_collision(exc, workflow_id="e2e_exercise_20260417")

    assert isinstance(translated, WorkflowSubmitConflict)
    assert translated.workflow_id == "e2e_exercise_20260417"
    assert translated.reason_code == "workflow.submit.definition_collision"
    assert "e2e_exercise_20260417" in translated.remediation
    assert "bump the workflow_id" in translated.remediation
    assert "DELETE FROM workflow_definitions" in translated.remediation
    assert translated.underlying is exc


def test_translator_matches_message_when_diag_missing() -> None:
    # Older psycopg / raw cursor errors may not carry a diag attribute. We
    # fall back to substring matching on the stringified exception.
    exc = Exception(
        'ERROR: duplicate key value violates unique constraint '
        '"workflow_definitions_workflow_id_definition_version_key"'
    )

    translated = _translate_definition_collision(exc, workflow_id="abc")

    assert isinstance(translated, WorkflowSubmitConflict)
    assert translated.workflow_id == "abc"


def test_translator_ignores_unrelated_exceptions() -> None:
    translated = _translate_definition_collision(
        Exception("some other DB error"),
        workflow_id="abc",
    )
    assert translated is None


def test_conflict_str_is_the_remediation_text() -> None:
    # Surfaces that format exceptions via str(exc) must see actionable text,
    # not an empty RuntimeError message.
    conflict = WorkflowSubmitConflict(
        workflow_id="demo",
        remediation="do the thing to fix it",
    )

    assert str(conflict) == "do the thing to fix it"
    assert conflict.reason_code == "workflow.submit.definition_collision"


def test_graph_admission_uses_hash_derived_definition_version() -> None:
    digest = "9c9483619e16d5888db910ed032e3d895d54d7a66d812a13e5b9a2772a6edac9"
    definition_hash = f"sha256:{digest}"
    request = WorkflowRequest(
        schema_version=1,
        workflow_id="workflow.bootstrap.smoke",
        request_id="req.bootstrap.smoke",
        workflow_definition_id="workflow_def:bootstrap_smoke",
        definition_hash=definition_hash,
        workspace_ref="praxis",
        runtime_profile_ref="native",
        nodes=(
            WorkflowNodeContract(
                node_id="deterministic_worker_ready",
                node_type="deterministic_task",
                adapter_type="deterministic_task",
                display_name="Deterministic worker ready",
                inputs={"allow_passthrough_echo": True},
                expected_outputs={"ok": True},
                success_condition={},
                failure_behavior={},
                authority_requirements={},
                execution_boundary={},
                position_index=0,
            ),
        ),
        edges=(),
    )
    expected_version = _definition_version_for_hash(definition_hash)
    assert expected_version == _definition_version_for_hash(digest)

    envelope = _graph_request_envelope(request)

    assert envelope["definition_version"] == expected_version

    class _Conn:
        definition_insert_args = None

        def execute(self, query: str, *args):
            if "INSERT INTO workflow_definitions" in query:
                self.definition_insert_args = args
            return []

    now = datetime.now(timezone.utc)
    decision = SimpleNamespace(
        admission_decision_id="decision.bootstrap.smoke",
        decision=SimpleNamespace(value="admit"),
        reason_code="claim.validated",
        decided_at=now,
        decided_by="test",
        policy_snapshot_ref="policy",
        validation_result_ref="validation",
        authority_context_ref="authority",
    )
    outcome = SimpleNamespace(
        admitted_definition_hash=None,
        admitted_definition_ref=None,
        admission_decision=decision,
        current_state=SimpleNamespace(value="claim_accepted"),
        run_id="run.bootstrap.smoke",
    )
    conn = _Conn()

    _persist_graph_authority(conn, intake_outcome=outcome, request=request, requested_at=now)

    assert conn.definition_insert_args is not None
    assert conn.definition_insert_args[3] == expected_version
