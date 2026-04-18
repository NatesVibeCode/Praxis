"""Regression tests for the workflow-definition-collision error translator.

The admission layer's INSERT into `workflow_definitions` uses a unique
constraint on `(workflow_id, definition_version)` and, in the graph-submit
path, hardcodes `definition_version=1`. A re-submit with the same
workflow_id therefore raises a psycopg UniqueViolation that — without the
translator below — reaches operators as a raw SQL constraint name.

These tests pin the translator so that regression doesn't silently
re-introduce the opaque error.
"""

from __future__ import annotations

from runtime.workflow._admission import (
    WorkflowSubmitConflict,
    _translate_definition_collision,
)


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
