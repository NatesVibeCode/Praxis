"""Service-bus transport authority.

The service bus is not domain truth. This module records transport envelopes
against registered channels and message contracts so async coordination can be
inspected without becoming a second write model for workflow or service state.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from pydantic import BaseModel, Field


class ServiceBusAuthorityError(RuntimeError):
    """Raised when a service-bus authority write is rejected."""

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


class RecordServiceBusMessageCommand(BaseModel):
    channel_ref: str
    message_type_ref: str
    correlation_ref: str
    command_ref: str | None = None
    receipt_id: str | None = None
    authority_domain_ref: str | None = None
    message_status: str = "published"
    payload: dict[str, Any] = Field(default_factory=dict)
    recorded_by: str = "service_bus.authority"


class ListServiceBusMessagesCommand(BaseModel):
    channel_ref: str | None = None
    correlation_ref: str | None = None
    limit: int = 100


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ServiceBusAuthorityError(
            "service_bus.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise ServiceBusAuthorityError(
            "service_bus.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise ServiceBusAuthorityError(
            "service_bus.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ServiceBusAuthorityError(
                "service_bus.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, dict):
        raise ServiceBusAuthorityError(
            "service_bus.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _fetch(conn: Any, query: str, *args: Any) -> list[dict[str, Any]]:
    if hasattr(conn, "fetch") and callable(conn.fetch):
        rows = conn.fetch(query, *args)
    else:
        rows = conn.execute(query, *args)
    return [dict(row) for row in rows or []]


def record_service_bus_message(
    conn: Any,
    command: RecordServiceBusMessageCommand,
) -> dict[str, Any]:
    """Record one transport envelope against registered bus contracts."""

    channel_ref = _require_text(command.channel_ref, field_name="channel_ref")
    message_type_ref = _require_text(command.message_type_ref, field_name="message_type_ref")
    correlation_ref = _require_text(command.correlation_ref, field_name="correlation_ref")
    message_status = _require_text(command.message_status, field_name="message_status")
    command_ref = _optional_text(command.command_ref, field_name="command_ref")
    receipt_id = _optional_text(command.receipt_id, field_name="receipt_id")
    recorded_by = _require_text(command.recorded_by, field_name="recorded_by")
    payload = _json_object(command.payload, field_name="payload")

    contract = conn.fetchrow(
        """
        SELECT contracts.message_type_ref,
               contracts.channel_ref,
               contracts.authority_domain_ref
          FROM service_bus_message_contracts contracts
          JOIN service_bus_channel_registry channels
            ON channels.channel_ref = contracts.channel_ref
         WHERE contracts.message_type_ref = $1
           AND contracts.channel_ref = $2
           AND contracts.enabled = TRUE
           AND channels.enabled = TRUE
        """,
        message_type_ref,
        channel_ref,
    )
    if contract is None:
        raise ServiceBusAuthorityError(
            "service_bus.contract_not_found",
            "service bus message contract is not registered or enabled",
            status_code=404,
            details={"channel_ref": channel_ref, "message_type_ref": message_type_ref},
        )
    contract_row = dict(contract)
    authority_domain_ref = _optional_text(
        command.authority_domain_ref,
        field_name="authority_domain_ref",
    ) or str(contract_row["authority_domain_ref"])

    row = conn.fetchrow(
        """
        INSERT INTO service_bus_message_ledger (
            channel_ref,
            message_type_ref,
            correlation_ref,
            command_ref,
            receipt_id,
            authority_domain_ref,
            message_status,
            payload,
            recorded_by
        ) VALUES ($1, $2, $3, $4, $5::uuid, $6, $7, $8::jsonb, $9)
        RETURNING message_id,
                  channel_ref,
                  message_type_ref,
                  correlation_ref,
                  command_ref,
                  receipt_id,
                  authority_domain_ref,
                  message_status,
                  payload,
                  recorded_by,
                  recorded_at
        """,
        channel_ref,
        message_type_ref,
        correlation_ref,
        command_ref,
        receipt_id,
        authority_domain_ref,
        message_status,
        json.dumps(payload, sort_keys=True, default=str),
        recorded_by,
    )
    return {"status": "recorded", "message": dict(row)}


def list_service_bus_messages(
    conn: Any,
    *,
    channel_ref: str | None = None,
    correlation_ref: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses = ["SELECT * FROM service_bus_message_ledger WHERE TRUE"]
    args: list[Any] = []
    normalized_channel = _optional_text(channel_ref, field_name="channel_ref")
    normalized_correlation = _optional_text(correlation_ref, field_name="correlation_ref")
    if normalized_channel is not None:
        args.append(normalized_channel)
        clauses.append(f"AND channel_ref = ${len(args)}")
    if normalized_correlation is not None:
        args.append(normalized_correlation)
        clauses.append(f"AND correlation_ref = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(f"ORDER BY recorded_at DESC LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def handle_record_service_bus_message(
    command: RecordServiceBusMessageCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return record_service_bus_message(subsystems.get_pg_conn(), command)


def handle_list_service_bus_messages(
    command: ListServiceBusMessagesCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_service_bus_messages(
        subsystems.get_pg_conn(),
        channel_ref=command.channel_ref,
        correlation_ref=command.correlation_ref,
        limit=command.limit,
    )
    return {"status": "listed", "messages": rows, "count": len(rows)}


__all__ = [
    "ListServiceBusMessagesCommand",
    "RecordServiceBusMessageCommand",
    "ServiceBusAuthorityError",
    "handle_list_service_bus_messages",
    "handle_record_service_bus_message",
    "list_service_bus_messages",
    "record_service_bus_message",
]
