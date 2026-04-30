"""CQRS commands for Virtual Lab state authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.virtual_lab.state import (
    EnvironmentRevision,
    EventEnvelope,
    ObjectStateRecord,
    CommandReceipt,
    command_receipt_from_dict,
    environment_revision_from_dict,
    event_chain_digest,
    event_envelope_from_dict,
    object_state_record_from_dict,
    object_states_from_seed_manifest,
    replay_environment_state,
)
from storage.postgres.virtual_lab_state_repository import persist_virtual_lab_state_packet


class RecordVirtualLabStateCommand(BaseModel):
    """Record a Virtual Lab environment revision state packet."""

    environment_revision: dict[str, Any]
    object_states: list[dict[str, Any]] = Field(default_factory=list)
    events: list[dict[str, Any]] = Field(default_factory=list)
    command_receipts: list[dict[str, Any]] = Field(default_factory=list)
    typed_gaps: list[dict[str, Any]] = Field(default_factory=list)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("object_states", "events", "command_receipts", "typed_gaps", mode="before")
    @classmethod
    def _normalize_record_list(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, dict):
            return [dict(value)]
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("Virtual Lab record groups must be JSON objects or lists of JSON objects")
        return [dict(item) for item in value]

    @field_validator("observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()


def handle_virtual_lab_state_record(
    command: RecordVirtualLabStateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    packet = _prepare_state_packet(command)
    persisted = persist_virtual_lab_state_packet(
        conn,
        environment_revision=packet["environment_revision"],
        object_states=packet["object_states"],
        events=packet["events"],
        command_receipts=packet["command_receipts"],
        typed_gaps=packet["typed_gaps"],
        event_chain_digest=packet["event_chain_digest"],
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    revision = packet["revision"]
    event_payload = {
        "environment_id": revision.environment_id,
        "revision_id": revision.revision_id,
        "revision_digest": revision.revision_digest,
        "seed_digest": revision.seed_digest,
        "object_state_count": len(packet["object_states"]),
        "event_count": len(packet["events"]),
        "receipt_count": len(packet["command_receipts"]),
        "typed_gap_count": len(packet["typed_gaps"]),
        "event_chain_digest": packet["event_chain_digest"],
    }
    return {
        "ok": True,
        "operation": "virtual_lab_state_record",
        "environment_revision": packet["environment_revision"],
        "object_states": packet["object_states"],
        "events": packet["events"],
        "command_receipts": packet["command_receipts"],
        "typed_gaps": packet["typed_gaps"],
        "validation": packet["validation"],
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _prepare_state_packet(command: RecordVirtualLabStateCommand) -> dict[str, Any]:
    revision = environment_revision_from_dict(dict(command.environment_revision))
    object_states = [object_state_record_from_dict(dict(item)) for item in command.object_states]
    events = [event_envelope_from_dict(dict(item)) for item in command.events]
    receipts = [command_receipt_from_dict(dict(item)) for item in command.command_receipts]
    _validate_revision_scope(revision, object_states, events, receipts, command.typed_gaps)
    _validate_state_projection(revision, object_states, events)
    chain_digest = event_chain_digest(tuple(events)) if events else None
    return {
        "revision": revision,
        "environment_revision": revision.to_json(),
        "object_states": [item.to_json() for item in object_states],
        "events": [item.to_json() for item in events],
        "command_receipts": [item.to_json() for item in receipts],
        "typed_gaps": [dict(item) for item in command.typed_gaps],
        "event_chain_digest": chain_digest,
        "validation": {
            "seed_object_count": len(revision.seed_manifest.entries),
            "object_state_count": len(object_states),
            "event_count": len(events),
            "receipt_count": len(receipts),
            "typed_gap_count": len(command.typed_gaps),
            "event_chain_digest": chain_digest,
        },
    }


def _validate_revision_scope(
    revision: EnvironmentRevision,
    object_states: list[ObjectStateRecord],
    events: list[EventEnvelope],
    receipts: list[CommandReceipt],
    typed_gaps: list[dict[str, Any]],
) -> None:
    for record in object_states:
        _assert_scope(revision, record.environment_id, record.revision_id, "object_state")
    for event in events:
        _assert_scope(revision, event.environment_id, event.revision_id, "event")
    for receipt in receipts:
        _assert_scope(revision, receipt.environment_id, receipt.revision_id, "command_receipt")
    for gap in typed_gaps:
        if gap.get("environment_id") and gap.get("environment_id") != revision.environment_id:
            raise ValueError("typed_gap environment_id must match environment_revision")
        if gap.get("revision_id") and gap.get("revision_id") != revision.revision_id:
            raise ValueError("typed_gap revision_id must match environment_revision")


def _validate_state_projection(
    revision: EnvironmentRevision,
    object_states: list[ObjectStateRecord],
    events: list[EventEnvelope],
) -> None:
    seed_states = object_states_from_seed_manifest(revision)
    if not object_states:
        if events:
            raise ValueError("object_states are required when events are recorded")
        return

    seed_keys = {_object_key(item) for item in seed_states}
    state_keys = {_object_key(item) for item in object_states}
    if seed_keys != state_keys:
        raise ValueError("object_states must cover exactly the revision seed manifest object instances")

    replayed = replay_environment_state(seed_states, tuple(events))
    for state in object_states:
        expected = replayed[_object_key(state)]
        if state.state_digest != expected.state_digest:
            raise ValueError(
                "object_state digest does not match deterministic replay "
                f"for {state.object_id}#{state.instance_id}"
            )


def _assert_scope(
    revision: EnvironmentRevision,
    environment_id: str,
    revision_id: str,
    record_kind: str,
) -> None:
    if environment_id != revision.environment_id or revision_id != revision.revision_id:
        raise ValueError(f"{record_kind} environment_id/revision_id must match environment_revision")


def _object_key(state: ObjectStateRecord) -> str:
    return f"{state.object_id}#{state.instance_id}"


__all__ = [
    "RecordVirtualLabStateCommand",
    "handle_virtual_lab_state_record",
]
