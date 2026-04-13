"""Typed protocol-event adapters around the shared workflow event envelope.

These helpers normalize transport traffic into the canonical WorkflowEventV1
shape and back out again without letting transport glue own runtime truth.
Route identity, request lineage, and evidence ordering stay authoritative in
the envelope. Protocol details stay explicit in the payload.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from typing import Any, Literal

from receipts.evidence import WorkflowEventV1
from runtime.domain import RouteIdentity

ProtocolKind = Literal["llm", "api", "a2a", "mcp"]
ProtocolDirection = Literal["ingress", "egress"]

PROTOCOL_INGRESS_EVENT_TYPE = "protocol_message_received"
PROTOCOL_EGRESS_EVENT_TYPE = "protocol_message_requested"
PROTOCOL_INGRESS_REASON_CODE = "protocol.ingress_normalized"
PROTOCOL_EGRESS_REASON_CODE = "protocol.egress_normalized"
PROTOCOL_ADAPTER_ACTOR_TYPE = "protocol_adapter"
_SUPPORTED_PROTOCOL_KINDS = frozenset({"llm", "api", "a2a", "mcp"})
_ENVELOPE_DIRECTION_BY_MARKERS = {
    (PROTOCOL_INGRESS_EVENT_TYPE, PROTOCOL_INGRESS_REASON_CODE): "ingress",
    (PROTOCOL_EGRESS_EVENT_TYPE, PROTOCOL_EGRESS_REASON_CODE): "egress",
}

JSONValue = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


class ProtocolEventAdapterError(RuntimeError):
    """Raised when protocol traffic cannot be represented safely."""


@dataclass(frozen=True, slots=True)
class ProtocolReplyTarget:
    """Explicit reply target for a protocol message."""

    target_kind: str
    transport_kind: str
    target_ref: str


@dataclass(frozen=True, slots=True)
class ProtocolMetadata:
    """Transport metadata that remains explicit but non-authoritative."""

    protocol_kind: ProtocolKind
    transport_kind: str
    correlation_ids: Mapping[str, str] = field(default_factory=dict)
    reply_target: ProtocolReplyTarget | None = None


@dataclass(frozen=True, slots=True)
class ProtocolMessage:
    """Typed protocol message carried in or out of the workflow bus."""

    direction: ProtocolDirection
    metadata: ProtocolMetadata
    body: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ProtocolEventRecord:
    """Decoded workflow event plus typed protocol message."""

    event: WorkflowEventV1
    message: ProtocolMessage


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProtocolEventAdapterError(f"{field_name} must be a non-empty string")
    return value


def _normalize_protocol_kind(value: object) -> ProtocolKind:
    protocol_kind = _require_text(value, field_name="protocol_kind")
    if protocol_kind not in _SUPPORTED_PROTOCOL_KINDS:
        raise ProtocolEventAdapterError(
            f"protocol_kind must be one of {sorted(_SUPPORTED_PROTOCOL_KINDS)!r}"
        )
    return protocol_kind


def _normalize_direction(value: object) -> ProtocolDirection:
    direction = _require_text(value, field_name="direction")
    if direction not in {"ingress", "egress"}:
        raise ProtocolEventAdapterError("direction must be 'ingress' or 'egress'")
    return direction


def _normalize_correlation_ids(value: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ProtocolEventAdapterError("correlation_ids must be a mapping")
    normalized: dict[str, str] = {}
    for key, item in dict(value).items():
        normalized_key = _require_text(key, field_name="correlation_ids.key")
        normalized[normalized_key] = _require_text(
            item,
            field_name=f"correlation_ids.{normalized_key}",
        )
    return normalized


def _reply_target_payload(reply_target: ProtocolReplyTarget | None) -> dict[str, str] | None:
    if reply_target is None:
        return None
    if not isinstance(reply_target, ProtocolReplyTarget):
        raise ProtocolEventAdapterError("reply_target must be a ProtocolReplyTarget")
    return {
        "target_kind": _require_text(reply_target.target_kind, field_name="reply_target.target_kind"),
        "transport_kind": _require_text(
            reply_target.transport_kind,
            field_name="reply_target.transport_kind",
        ),
        "target_ref": _require_text(reply_target.target_ref, field_name="reply_target.target_ref"),
    }


def _normalize_reply_target(value: Mapping[str, object] | None) -> ProtocolReplyTarget | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ProtocolEventAdapterError("reply_target must be a mapping")
    return ProtocolReplyTarget(
        target_kind=_require_text(value.get("target_kind"), field_name="reply_target.target_kind"),
        transport_kind=_require_text(
            value.get("transport_kind"),
            field_name="reply_target.transport_kind",
        ),
        target_ref=_require_text(value.get("target_ref"), field_name="reply_target.target_ref"),
    )


def _normalize_metadata(metadata: ProtocolMetadata) -> dict[str, Any]:
    return {
        "protocol_kind": _normalize_protocol_kind(metadata.protocol_kind),
        "transport_kind": _require_text(metadata.transport_kind, field_name="transport_kind"),
        "correlation_ids": _normalize_correlation_ids(metadata.correlation_ids),
        "reply_target": _reply_target_payload(metadata.reply_target),
    }


def _normalize_json_value(value: object, *, field_name: str) -> JSONValue:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ProtocolEventAdapterError(f"{field_name} must be finite to stay JSON-safe")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ProtocolEventAdapterError(f"{field_name} keys must be strings")
            normalized[key] = _normalize_json_value(
                item,
                field_name=f"{field_name}.{key}",
            )
        return normalized
    if isinstance(value, (list, tuple)):
        return [
            _normalize_json_value(item, field_name=f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ProtocolEventAdapterError(
        f"{field_name} must contain only canonical JSON-safe values; "
        f"got {type(value).__name__}"
    )


def _normalize_json_object(value: Mapping[str, object], *, field_name: str) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise ProtocolEventAdapterError(f"{field_name} must be a mapping")
    normalized = _normalize_json_value(value, field_name=field_name)
    if not isinstance(normalized, dict):
        raise ProtocolEventAdapterError(f"{field_name} must normalize to a mapping")
    return normalized


def _normalize_body(body: Mapping[str, Any]) -> dict[str, JSONValue]:
    if not isinstance(body, Mapping):
        raise ProtocolEventAdapterError("body must be a mapping")
    return _normalize_json_object(body, field_name="body")


def _validate_event_lineage(event: WorkflowEventV1) -> None:
    if event.workflow_id != event.route_identity.workflow_id:
        raise ProtocolEventAdapterError("event.workflow_id must match route_identity.workflow_id")
    if event.run_id != event.route_identity.run_id:
        raise ProtocolEventAdapterError("event.run_id must match route_identity.run_id")
    if event.request_id != event.route_identity.request_id:
        raise ProtocolEventAdapterError("event.request_id must match route_identity.request_id")
    if event.transition_seq != event.route_identity.transition_seq:
        raise ProtocolEventAdapterError(
            "event.transition_seq must match route_identity.transition_seq"
        )


def _classify_protocol_direction(event: WorkflowEventV1) -> ProtocolDirection:
    markers = (
        _require_text(event.event_type, field_name="event.event_type"),
        _require_text(event.reason_code, field_name="event.reason_code"),
    )
    direction = _ENVELOPE_DIRECTION_BY_MARKERS.get(markers)
    if direction is not None:
        return direction
    raise ProtocolEventAdapterError(
        "event.event_type and event.reason_code must identify a canonical protocol envelope"
    )


def _build_protocol_event(
    *,
    route_identity: RouteIdentity,
    event_id: str,
    evidence_seq: int,
    occurred_at: datetime,
    message: ProtocolMessage,
    event_type: str,
    reason_code: str,
    actor_type: str,
    causation_id: str | None = None,
    node_id: str | None = None,
) -> WorkflowEventV1:
    payload = {
        "protocol": {
            "direction": _normalize_direction(message.direction),
            **_normalize_metadata(message.metadata),
        },
        "message": _normalize_body(message.body),
    }
    return WorkflowEventV1(
        event_id=_require_text(event_id, field_name="event_id"),
        event_type=_require_text(event_type, field_name="event_type"),
        schema_version=1,
        workflow_id=route_identity.workflow_id,
        run_id=route_identity.run_id,
        request_id=route_identity.request_id,
        route_identity=route_identity,
        transition_seq=route_identity.transition_seq,
        evidence_seq=evidence_seq,
        occurred_at=occurred_at,
        actor_type=_require_text(actor_type, field_name="actor_type"),
        reason_code=_require_text(reason_code, field_name="reason_code"),
        payload=payload,
        causation_id=causation_id,
        node_id=node_id,
    )


def normalize_protocol_ingress(
    *,
    route_identity: RouteIdentity,
    event_id: str,
    evidence_seq: int,
    occurred_at: datetime,
    message: ProtocolMessage,
    event_type: str = PROTOCOL_INGRESS_EVENT_TYPE,
    reason_code: str = PROTOCOL_INGRESS_REASON_CODE,
    actor_type: str = PROTOCOL_ADAPTER_ACTOR_TYPE,
    causation_id: str | None = None,
    node_id: str | None = None,
) -> WorkflowEventV1:
    """Normalize inbound protocol traffic into the shared workflow event shape."""

    if message.direction != "ingress":
        raise ProtocolEventAdapterError("normalize_protocol_ingress requires direction='ingress'")
    return _build_protocol_event(
        route_identity=route_identity,
        event_id=event_id,
        evidence_seq=evidence_seq,
        occurred_at=occurred_at,
        message=message,
        event_type=event_type,
        reason_code=reason_code,
        actor_type=actor_type,
        causation_id=causation_id,
        node_id=node_id,
    )


def normalize_protocol_egress(
    *,
    route_identity: RouteIdentity,
    event_id: str,
    evidence_seq: int,
    occurred_at: datetime,
    message: ProtocolMessage,
    event_type: str = PROTOCOL_EGRESS_EVENT_TYPE,
    reason_code: str = PROTOCOL_EGRESS_REASON_CODE,
    actor_type: str = PROTOCOL_ADAPTER_ACTOR_TYPE,
    causation_id: str | None = None,
    node_id: str | None = None,
) -> WorkflowEventV1:
    """Normalize outbound protocol traffic into the shared workflow event shape."""

    if message.direction != "egress":
        raise ProtocolEventAdapterError("normalize_protocol_egress requires direction='egress'")
    return _build_protocol_event(
        route_identity=route_identity,
        event_id=event_id,
        evidence_seq=evidence_seq,
        occurred_at=occurred_at,
        message=message,
        event_type=event_type,
        reason_code=reason_code,
        actor_type=actor_type,
        causation_id=causation_id,
        node_id=node_id,
    )


def decode_protocol_event(event: WorkflowEventV1) -> ProtocolEventRecord:
    """Decode typed protocol metadata from a workflow event envelope."""

    _validate_event_lineage(event)
    direction = _classify_protocol_direction(event)
    if not isinstance(event.payload, Mapping):
        raise ProtocolEventAdapterError("event.payload must be a mapping")

    protocol_payload = event.payload.get("protocol")
    message_payload = event.payload.get("message")
    if not isinstance(protocol_payload, Mapping):
        raise ProtocolEventAdapterError("event.payload.protocol must be a mapping")
    if not isinstance(message_payload, Mapping):
        raise ProtocolEventAdapterError("event.payload.message must be a mapping")
    normalized_protocol_payload = _normalize_json_object(
        protocol_payload,
        field_name="event.payload.protocol",
    )
    normalized_message_payload = _normalize_json_object(
        message_payload,
        field_name="event.payload.message",
    )
    payload_direction = _normalize_direction(normalized_protocol_payload.get("direction"))
    if payload_direction != direction:
        raise ProtocolEventAdapterError(
            "event.payload.protocol.direction must match the protocol envelope markers"
        )

    metadata = ProtocolMetadata(
        protocol_kind=_normalize_protocol_kind(normalized_protocol_payload.get("protocol_kind")),
        transport_kind=_require_text(
            normalized_protocol_payload.get("transport_kind"),
            field_name="event.payload.protocol.transport_kind",
        ),
        correlation_ids=_normalize_correlation_ids(
            normalized_protocol_payload.get("correlation_ids", {})
        ),
        reply_target=_normalize_reply_target(normalized_protocol_payload.get("reply_target")),
    )
    message = ProtocolMessage(
        direction=direction,
        metadata=metadata,
        body=normalized_message_payload,
    )
    return ProtocolEventRecord(event=event, message=message)


__all__ = [
    "PROTOCOL_ADAPTER_ACTOR_TYPE",
    "PROTOCOL_EGRESS_EVENT_TYPE",
    "PROTOCOL_EGRESS_REASON_CODE",
    "PROTOCOL_INGRESS_EVENT_TYPE",
    "PROTOCOL_INGRESS_REASON_CODE",
    "ProtocolDirection",
    "ProtocolEventAdapterError",
    "ProtocolEventRecord",
    "ProtocolKind",
    "ProtocolMessage",
    "ProtocolMetadata",
    "ProtocolReplyTarget",
    "decode_protocol_event",
    "normalize_protocol_egress",
    "normalize_protocol_ingress",
]
