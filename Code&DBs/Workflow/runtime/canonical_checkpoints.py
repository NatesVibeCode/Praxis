"""Canonical runtime ownership for authority checkpoint mutation surfaces."""

from __future__ import annotations

from typing import Any

from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    create_authority_checkpoint,
    decide_authority_checkpoint,
)


class AuthorityCheckpointBoundaryError(RuntimeError):
    """Raised when canonical checkpoint ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise AuthorityCheckpointBoundaryError(str(exc), status_code=status_code) from exc


def request_authority_checkpoint(
    conn: Any,
    *,
    card_id: Any,
    model_id: Any,
    authority_level: Any,
    question: Any,
) -> dict[str, Any]:
    normalized_card_id = _text(card_id)
    normalized_model_id = _text(model_id)
    normalized_authority_level = _text(authority_level)
    normalized_question = _text(question)
    if not normalized_card_id:
        raise AuthorityCheckpointBoundaryError("card_id is required")
    if not normalized_model_id:
        raise AuthorityCheckpointBoundaryError("model_id is required")
    if not normalized_authority_level:
        raise AuthorityCheckpointBoundaryError("authority_level is required")
    if not normalized_question:
        raise AuthorityCheckpointBoundaryError("question is required")
    try:
        return create_authority_checkpoint(
            conn,
            card_id=normalized_card_id,
            model_id=normalized_model_id,
            authority_level=normalized_authority_level,
            question=normalized_question,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def resolve_authority_checkpoint(
    conn: Any,
    *,
    checkpoint_id: Any,
    decision: Any,
    notes: Any = None,
    decided_by: Any = None,
) -> dict[str, Any]:
    normalized_checkpoint_id = _text(checkpoint_id)
    normalized_decision = _text(decision)
    normalized_notes = None if notes is None else _text(notes)
    normalized_decided_by = None if decided_by is None else _text(decided_by)
    if not normalized_checkpoint_id:
        raise AuthorityCheckpointBoundaryError("checkpoint_id is required")
    if normalized_decision not in {"approved", "rejected", "escalated"}:
        raise AuthorityCheckpointBoundaryError("decision must be one of: approved, rejected, escalated")
    try:
        row = decide_authority_checkpoint(
            conn,
            checkpoint_id=normalized_checkpoint_id,
            decision=normalized_decision,
            decided_by=normalized_decided_by or None,
            notes=normalized_notes or None,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise AuthorityCheckpointBoundaryError(
            f"Checkpoint not found: {normalized_checkpoint_id}",
            status_code=404,
        )
    return row


__all__ = [
    "AuthorityCheckpointBoundaryError",
    "request_authority_checkpoint",
    "resolve_authority_checkpoint",
]
