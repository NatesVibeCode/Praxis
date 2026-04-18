from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from adapters.protocol_events import (
    PROTOCOL_EGRESS_EVENT_TYPE,
    PROTOCOL_EGRESS_REASON_CODE,
    PROTOCOL_INGRESS_EVENT_TYPE,
    PROTOCOL_INGRESS_REASON_CODE,
    ProtocolEventAdapterError,
    ProtocolMessage,
    ProtocolMetadata,
    ProtocolReplyTarget,
    decode_protocol_event,
    normalize_protocol_egress,
    normalize_protocol_ingress,
)
from runtime import RouteIdentity
from receipts.evidence import WorkflowEventV1


@pytest.fixture
def route_identity() -> RouteIdentity:
    return RouteIdentity(
        workflow_id="workflow-1",
        run_id="run-1",
        request_id="request-1",
        authority_context_ref="authority-context-1",
        authority_context_digest="authority-digest-1",
        claim_id="claim-1",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=1,
    )


@pytest.fixture
def occurred_at() -> datetime:
    return datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)


def _protocol_cases():
    return (
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="llm",
                    transport_kind="responses_api",
                    correlation_ids={
                        "conversation_id": "conversation-1",
                        "response_id": "response-1",
                    },
                    reply_target=ProtocolReplyTarget(
                        target_kind="llm.response",
                        transport_kind="responses_api",
                        target_ref="response://conversation-1",
                    ),
                ),
                body={
                    "messages": (
                        {"role": "system", "content": "Answer tersely."},
                        {"role": "user", "content": "Summarize the workflow."},
                    ),
                },
            ),
            id="llm",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="api",
                    transport_kind="http",
                    correlation_ids={
                        "request_id": "http-request-1",
                        "trace_id": "trace-1",
                    },
                    reply_target=ProtocolReplyTarget(
                        target_kind="http.callback",
                        transport_kind="http",
                        target_ref="https://example.test/hooks/workflow",
                    ),
                ),
                body={
                    "method": "POST",
                    "path": "/v1/workflows/submit",
                    "json": {"workflow_id": "workflow-1"},
                },
            ),
            id="api",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="a2a",
                    transport_kind="queue",
                    correlation_ids={
                        "message_id": "message-1",
                        "thread_id": "thread-1",
                    },
                    reply_target=ProtocolReplyTarget(
                        target_kind="agent.inbox",
                        transport_kind="queue",
                        target_ref="agent://reviewer/inbox",
                    ),
                ),
                body={
                    "message_type": "proposal",
                    "payload": {"summary": "check promotion guardrails"},
                },
            ),
            id="a2a",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="mcp",
                    transport_kind="stdio",
                    correlation_ids={
                        "session_id": "session-1",
                        "tool_call_id": "call-1",
                    },
                    reply_target=ProtocolReplyTarget(
                        target_kind="mcp.session",
                        transport_kind="stdio",
                        target_ref="session://mcp-1",
                    ),
                ),
                body={
                    "method": "tools/call",
                    "params": {
                        "name": "inspect_run",
                        "arguments": {"run_id": "run-1"},
                    },
                },
            ),
            id="mcp",
        ),
    )


def _json_roundtrip(value: object) -> object:
    return json.loads(json.dumps(value))


def _canonical_message(message: ProtocolMessage) -> ProtocolMessage:
    return replace(message, body=_json_roundtrip(message.body))


@pytest.mark.parametrize("message", _protocol_cases())
def test_protocol_adapters_normalize_ingress_and_egress_across_supported_protocols(
    route_identity,
    occurred_at,
    message: ProtocolMessage,
) -> None:
    ingress_event = normalize_protocol_ingress(
        route_identity=route_identity,
        event_id=f"{message.metadata.protocol_kind}-ingress-event",
        evidence_seq=11,
        occurred_at=occurred_at,
        message=message,
    )
    persisted_ingress_event = replace(
        ingress_event,
        payload=_json_roundtrip(ingress_event.payload),
    )

    decoded_ingress = decode_protocol_event(persisted_ingress_event)

    assert ingress_event.event_type == PROTOCOL_INGRESS_EVENT_TYPE
    assert ingress_event.reason_code == PROTOCOL_INGRESS_REASON_CODE
    assert ingress_event.workflow_id == route_identity.workflow_id
    assert ingress_event.run_id == route_identity.run_id
    assert ingress_event.request_id == route_identity.request_id
    assert ingress_event.transition_seq == route_identity.transition_seq
    assert ingress_event.payload == persisted_ingress_event.payload
    assert decoded_ingress.message == _canonical_message(message)

    egress_message = ProtocolMessage(
        direction="egress",
        metadata=message.metadata,
        body={
            "status": "accepted",
            "protocol_kind": message.metadata.protocol_kind,
        },
    )
    egress_event = normalize_protocol_egress(
        route_identity=replace(route_identity, transition_seq=route_identity.transition_seq + 1),
        event_id=f"{message.metadata.protocol_kind}-egress-event",
        evidence_seq=12,
        occurred_at=occurred_at + timedelta(seconds=1),
        message=egress_message,
        causation_id=ingress_event.event_id,
    )
    persisted_egress_event = replace(
        egress_event,
        payload=_json_roundtrip(egress_event.payload),
    )

    decoded_egress = decode_protocol_event(persisted_egress_event)

    assert egress_event.event_type == PROTOCOL_EGRESS_EVENT_TYPE
    assert egress_event.reason_code == PROTOCOL_EGRESS_REASON_CODE
    assert egress_event.causation_id == ingress_event.event_id
    assert egress_event.payload == persisted_egress_event.payload
    assert decoded_egress.message == _canonical_message(egress_message)
    assert decoded_egress.event.route_identity.transition_seq == route_identity.transition_seq + 1


@pytest.mark.parametrize(
    ("message", "error_fragment"),
    (
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="smtp",
                    transport_kind="http",
                    correlation_ids={"request_id": "request-1"},
                ),
                body={"method": "GET", "path": "/health"},
            ),
            "protocol_kind",
            id="unsupported_protocol_kind",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="api",
                    transport_kind="http",
                    correlation_ids={"request_id": 7},
                ),
                body={"method": "GET", "path": "/health"},
            ),
            "correlation_ids.request_id",
            id="non_string_correlation_value",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="api",
                    transport_kind="http",
                    correlation_ids={"request_id": "request-1"},
                    reply_target="queue://reply",
                ),
                body={"method": "GET", "path": "/health"},
            ),
            "reply_target",
            id="non_mapping_reply_target",
        ),
        pytest.param(
            ProtocolMessage(
                direction="ingress",
                metadata=ProtocolMetadata(
                    protocol_kind="api",
                    transport_kind="http",
                    correlation_ids={"request_id": "request-1"},
                ),
                body={"method": "POST", "payload": {"tags": {"bad"}}},
            ),
            "canonical JSON-safe",
            id="non_json_safe_body_value",
        ),
    ),
)
def test_protocol_normalization_fails_closed_on_malformed_metadata_and_body(
    route_identity,
    occurred_at,
    message: ProtocolMessage,
    error_fragment: str,
) -> None:
    with pytest.raises(ProtocolEventAdapterError) as exc:
        normalize_protocol_ingress(
            route_identity=route_identity,
            event_id="event-1",
            evidence_seq=1,
            occurred_at=occurred_at,
            message=message,
        )

    assert error_fragment in str(exc.value)


@pytest.mark.parametrize(
    ("payload_mutator", "error_fragment"),
    (
        pytest.param(
            lambda payload: payload["protocol"].__setitem__("correlation_ids", ["request-1"]),
            "correlation_ids",
            id="correlation_ids_not_mapping",
        ),
        pytest.param(
            lambda payload: payload["protocol"].__setitem__(
                "reply_target",
                {"target_kind": "http.callback", "transport_kind": "http"},
            ),
            "reply_target.target_ref",
            id="reply_target_missing_target_ref",
        ),
        pytest.param(
            lambda payload: payload["message"].__setitem__("payload", object()),
            "canonical JSON-safe",
            id="non_json_safe_message_payload",
        ),
    ),
)
def test_protocol_event_decode_fails_closed_on_malformed_stored_payloads(
    route_identity,
    occurred_at,
    payload_mutator,
    error_fragment: str,
) -> None:
    event = normalize_protocol_ingress(
        route_identity=route_identity,
        event_id="event-1",
        evidence_seq=1,
        occurred_at=occurred_at,
        message=ProtocolMessage(
            direction="ingress",
            metadata=ProtocolMetadata(
                protocol_kind="api",
                transport_kind="http",
                correlation_ids={"request_id": "request-1"},
                reply_target=ProtocolReplyTarget(
                    target_kind="http.callback",
                    transport_kind="http",
                    target_ref="https://example.test/hooks/workflow",
                ),
            ),
            body={"method": "POST", "payload": {"workflow_id": "workflow-1"}},
        ),
    )
    payload = deepcopy(event.payload)
    payload_mutator(payload)

    with pytest.raises(ProtocolEventAdapterError) as exc:
        decode_protocol_event(replace(event, payload=payload))

    assert error_fragment in str(exc.value)


def test_protocol_event_decode_fails_closed_on_route_identity_mismatch(
    route_identity,
    occurred_at,
) -> None:
    message = ProtocolMessage(
        direction="ingress",
        metadata=ProtocolMetadata(
            protocol_kind="api",
            transport_kind="http",
            correlation_ids={"request_id": "request-1"},
        ),
        body={"method": "GET", "path": "/health"},
    )
    event = normalize_protocol_ingress(
        route_identity=route_identity,
        event_id="event-1",
        evidence_seq=1,
        occurred_at=occurred_at,
        message=message,
    )
    malformed_event = WorkflowEventV1(
        event_id=event.event_id,
        event_type=event.event_type,
        schema_version=event.schema_version,
        workflow_id="workflow-mismatch",
        run_id=event.run_id,
        request_id=event.request_id,
        route_identity=event.route_identity,
        transition_seq=event.transition_seq,
        evidence_seq=event.evidence_seq,
        occurred_at=event.occurred_at,
        actor_type=event.actor_type,
        reason_code=event.reason_code,
        payload=event.payload,
        causation_id=event.causation_id,
        node_id=event.node_id,
    )

    with pytest.raises(ProtocolEventAdapterError) as exc:
        decode_protocol_event(malformed_event)

    assert "workflow_id" in str(exc.value)


@pytest.mark.parametrize(
    ("event_type", "reason_code", "payload_direction", "error_fragment"),
    (
        pytest.param(
            PROTOCOL_EGRESS_EVENT_TYPE,
            PROTOCOL_INGRESS_REASON_CODE,
            "ingress",
            "canonical protocol envelope",
            id="event_type_reason_code_mismatch",
        ),
        pytest.param(
            PROTOCOL_EGRESS_EVENT_TYPE,
            PROTOCOL_EGRESS_REASON_CODE,
            "ingress",
            "protocol envelope markers",
            id="payload_direction_mismatch",
        ),
        pytest.param(
            "workflow_claim_received",
            "workflow.claim_received",
            "ingress",
            "canonical protocol envelope",
            id="non_protocol_event_markers",
        ),
    ),
)
def test_protocol_event_decode_uses_envelope_markers_as_authority(
    route_identity,
    occurred_at,
    event_type: str,
    reason_code: str,
    payload_direction: str,
    error_fragment: str,
) -> None:
    event = normalize_protocol_ingress(
        route_identity=route_identity,
        event_id="event-1",
        evidence_seq=1,
        occurred_at=occurred_at,
        message=ProtocolMessage(
            direction="ingress",
            metadata=ProtocolMetadata(
                protocol_kind="api",
                transport_kind="http",
                correlation_ids={"request_id": "request-1"},
            ),
            body={"method": "GET", "path": "/health"},
        ),
    )
    payload = deepcopy(event.payload)
    payload["protocol"]["direction"] = payload_direction

    with pytest.raises(ProtocolEventAdapterError) as exc:
        decode_protocol_event(
            replace(
                event,
                event_type=event_type,
                reason_code=reason_code,
                payload=payload,
            )
        )

    assert error_fragment in str(exc.value)
