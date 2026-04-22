"""CQRS feedback authority.

Feedback is durable evidence. This module records feedback intake and emits
authority events without directly mutating the target domain that feedback is
about. Domain authorities can consume these rows through their own commands.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class FeedbackAuthorityError(RuntimeError):
    """Raised when feedback authority rejects an operation."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code
        self.details = dict(details or {})


class RecordAuthorityFeedbackCommand(BaseModel):
    feedback_stream_ref: str = "feedback.operator_review"
    target_ref: str
    source_ref: str = "unknown"
    signal_kind: str = "observation"
    signal_payload: dict[str, Any] = Field(default_factory=dict)
    proposed_action: dict[str, Any] = Field(default_factory=dict)
    recorded_by: str = "feedback.authority"
    idempotency_key: str | None = None


class ListAuthorityFeedbackCommand(BaseModel):
    feedback_stream_ref: str | None = None
    target_ref: str | None = None
    signal_kind: str | None = None
    limit: int = 100


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FeedbackAuthorityError(
            "feedback.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise FeedbackAuthorityError(
                "feedback.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, dict):
        raise FeedbackAuthorityError(
            "feedback.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise FeedbackAuthorityError(
            "feedback.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise FeedbackAuthorityError(
            "feedback.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def _fetch(conn: Any, query: str, *args: Any) -> list[dict[str, Any]]:
    if hasattr(conn, "fetch") and callable(conn.fetch):
        rows = conn.fetch(query, *args)
    else:
        rows = conn.execute(query, *args)
    return [dict(row) for row in rows or []]


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _existing_idempotent_feedback(conn: Any, idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    row = conn.fetchrow(
        """
        SELECT feedback_event_id,
               feedback_stream_ref,
               target_ref,
               source_ref,
               signal_kind,
               signal_payload,
               proposed_action,
               recorded_by,
               idempotency_key,
               authority_event_id,
               recorded_at
          FROM authority_feedback_events
         WHERE idempotency_key = $1
        """,
        idempotency_key,
    )
    return None if row is None else _row(row)


def record_feedback_event(
    conn: Any,
    command: RecordAuthorityFeedbackCommand,
) -> dict[str, Any]:
    """Record immutable feedback and emit a feedback authority event."""

    stream_ref = _require_text(command.feedback_stream_ref, field_name="feedback_stream_ref")
    target_ref = _require_text(command.target_ref, field_name="target_ref")
    source_ref = _require_text(command.source_ref, field_name="source_ref")
    signal_kind = _require_text(command.signal_kind, field_name="signal_kind")
    recorded_by = _require_text(command.recorded_by, field_name="recorded_by")
    idempotency_key = _optional_text(command.idempotency_key, field_name="idempotency_key")
    signal_payload = _json_object(command.signal_payload, field_name="signal_payload")
    proposed_action = _json_object(command.proposed_action, field_name="proposed_action")

    existing = _existing_idempotent_feedback(conn, idempotency_key)
    if existing is not None:
        event_id = existing.get("authority_event_id")
        return {
            "status": "replayed",
            "feedback_event": existing,
            "authority_event_ids": [str(event_id)] if event_id else [],
        }

    stream = conn.fetchrow(
        """
        SELECT feedback_stream_ref, feedback_kind, target_authority_domain_ref
          FROM authority_feedback_streams
         WHERE feedback_stream_ref = $1
           AND enabled = TRUE
        """,
        stream_ref,
    )
    if stream is None:
        raise FeedbackAuthorityError(
            "feedback.stream_not_found",
            "feedback stream is not registered or enabled",
            status_code=404,
            details={"feedback_stream_ref": stream_ref},
        )

    feedback_event_id = str(uuid4())
    feedback_row = conn.fetchrow(
        """
        INSERT INTO authority_feedback_events (
            feedback_event_id,
            feedback_stream_ref,
            target_ref,
            source_ref,
            signal_kind,
            signal_payload,
            proposed_action,
            recorded_by,
            idempotency_key
        ) VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
        RETURNING feedback_event_id,
                  feedback_stream_ref,
                  target_ref,
                  source_ref,
                  signal_kind,
                  signal_payload,
                  proposed_action,
                  recorded_by,
                  idempotency_key,
                  authority_event_id,
                  recorded_at
        """,
        feedback_event_id,
        stream_ref,
        target_ref,
        source_ref,
        signal_kind,
        json.dumps(signal_payload, sort_keys=True, default=str),
        json.dumps(proposed_action, sort_keys=True, default=str),
        recorded_by,
        idempotency_key,
    )

    authority_event_id = str(uuid4())
    event_payload = {
        "feedback_event_id": feedback_event_id,
        "feedback_stream_ref": stream_ref,
        "target_ref": target_ref,
        "source_ref": source_ref,
        "signal_kind": signal_kind,
        "signal_payload": signal_payload,
        "proposed_action": proposed_action,
    }
    conn.execute(
        """
        INSERT INTO authority_events (
            event_id,
            authority_domain_ref,
            aggregate_ref,
            event_type,
            event_payload,
            idempotency_key,
            operation_ref,
            emitted_by
        ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, $7, $8)
        """,
        authority_event_id,
        "authority.feedback",
        f"{stream_ref}:{target_ref}",
        "feedback_recorded",
        json.dumps(event_payload, sort_keys=True, default=str),
        idempotency_key,
        "feedback.record",
        "authority.feedback",
    )
    row = conn.fetchrow(
        """
        UPDATE authority_feedback_events
           SET authority_event_id = $2::uuid
         WHERE feedback_event_id = $1::uuid
         RETURNING feedback_event_id,
                   feedback_stream_ref,
                   target_ref,
                   source_ref,
                   signal_kind,
                   signal_payload,
                   proposed_action,
                   recorded_by,
                   idempotency_key,
                   authority_event_id,
                   recorded_at
        """,
        feedback_event_id,
        authority_event_id,
    )
    return {
        "status": "recorded",
        "feedback_event": _row(row) or _row(feedback_row),
        "authority_event_ids": [authority_event_id],
    }


def list_feedback_events(
    conn: Any,
    *,
    feedback_stream_ref: str | None = None,
    target_ref: str | None = None,
    signal_kind: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses = ["SELECT * FROM authority_feedback_event_projection WHERE TRUE"]
    args: list[Any] = []
    normalized_stream = _optional_text(feedback_stream_ref, field_name="feedback_stream_ref")
    normalized_target = _optional_text(target_ref, field_name="target_ref")
    normalized_signal = _optional_text(signal_kind, field_name="signal_kind")
    if normalized_stream is not None:
        args.append(normalized_stream)
        clauses.append(f"AND feedback_stream_ref = ${len(args)}")
    if normalized_target is not None:
        args.append(normalized_target)
        clauses.append(f"AND target_ref = ${len(args)}")
    if normalized_signal is not None:
        args.append(normalized_signal)
        clauses.append(f"AND signal_kind = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(f"ORDER BY recorded_at DESC LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def handle_record_feedback(
    command: RecordAuthorityFeedbackCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return record_feedback_event(subsystems.get_pg_conn(), command)


def handle_list_feedback_events(
    command: ListAuthorityFeedbackCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_feedback_events(
        subsystems.get_pg_conn(),
        feedback_stream_ref=command.feedback_stream_ref,
        target_ref=command.target_ref,
        signal_kind=command.signal_kind,
        limit=command.limit,
    )
    return {"status": "listed", "feedback_events": rows, "count": len(rows)}


__all__ = [
    "FeedbackAuthorityError",
    "ListAuthorityFeedbackCommand",
    "RecordAuthorityFeedbackCommand",
    "handle_list_feedback_events",
    "handle_record_feedback",
    "list_feedback_events",
    "record_feedback_event",
]
