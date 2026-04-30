"""Deterministic Virtual Lab state primitives.

This module is pure domain code. It does not persist virtual lab state,
register CQRS operations, call Object Truth storage, or execute integrations.
Object Truth owns observed facts; these primitives own predicted state
transitions for one virtual environment revision.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from core.object_truth_ops import canonical_digest, canonical_value


VIRTUAL_LAB_STATE_SCHEMA_VERSION = 1
VIRTUAL_LAB_DIGEST_ALGORITHM = "sha256"
VIRTUAL_LAB_DIGEST_VERSION = "v1"

REVISION_STATUS_ACTIVE = "active"
REVISION_STATUS_CLOSED = "closed"

ReceiptStatus = Literal["accepted", "rejected", "conflict", "no_op"]
ActorType = Literal["user", "service_account", "agent", "system"]

REVISION_STATUSES = {REVISION_STATUS_ACTIVE, REVISION_STATUS_CLOSED}
RECEIPT_STATUSES = {"accepted", "rejected", "conflict", "no_op"}
ACTOR_TYPES = {"user", "service_account", "agent", "system"}

ENVIRONMENT_EVENT_TYPES = {
    "environment.created",
    "environment.forked",
    "environment.reseeded",
    "environment.closed",
}
OBJECT_EVENT_TYPES = {
    "object.seeded",
    "object.instantiated",
    "object.patched",
    "object.replaced",
    "object.tombstoned",
    "object.restored",
}
SYSTEM_EVENT_TYPES = {
    "command.rejected",
    "receipt.issued",
    "digest.snapshotted",
    "projection.rebuilt",
}
EVENT_TYPES = ENVIRONMENT_EVENT_TYPES | OBJECT_EVENT_TYPES | SYSTEM_EVENT_TYPES

EMPTY_STATE_DIGEST = f"{VIRTUAL_LAB_DIGEST_ALGORITHM}:{VIRTUAL_LAB_DIGEST_VERSION}:empty"


class VirtualLabStateError(RuntimeError):
    """Raised when virtual lab state cannot be represented safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


def virtual_lab_digest(value: Any, *, purpose: str) -> str:
    """Return an algorithm/version-qualified digest for virtual lab evidence."""

    scoped_purpose = f"{purpose}.{VIRTUAL_LAB_DIGEST_ALGORITHM}.{VIRTUAL_LAB_DIGEST_VERSION}"
    digest = canonical_digest(value, purpose=scoped_purpose)
    return f"{VIRTUAL_LAB_DIGEST_ALGORITHM}:{VIRTUAL_LAB_DIGEST_VERSION}:{digest}"


def object_stream_id(
    *,
    environment_id: str,
    revision_id: str,
    object_id: str,
    instance_id: str = "primary",
) -> str:
    return "/".join(
        [
            _required_text(environment_id, "environment_id"),
            _required_text(revision_id, "revision_id"),
            "objects",
            _required_text(object_id, "object_id"),
            _required_text(instance_id, "instance_id"),
        ]
    )


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    actor_id: str
    actor_type: ActorType

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_id", _required_text(self.actor_id, "actor_id"))
        object.__setattr__(self, "actor_type", _validate_member(self.actor_type, ACTOR_TYPES, "actor_type"))

    def to_json(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
        }


@dataclass(frozen=True, slots=True)
class SeedManifestEntry:
    object_id: str
    object_truth_ref: str
    object_truth_version: str
    projection_version: str
    seed_parameters: dict[str, Any] = field(default_factory=dict)
    base_state: dict[str, Any] = field(default_factory=dict)
    instance_id: str = "primary"

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_id", _required_text(self.object_id, "object_id"))
        object.__setattr__(
            self,
            "object_truth_ref",
            _required_text(self.object_truth_ref, "object_truth_ref"),
        )
        object.__setattr__(
            self,
            "object_truth_version",
            _required_text(self.object_truth_version, "object_truth_version"),
        )
        object.__setattr__(
            self,
            "projection_version",
            _required_text(self.projection_version, "projection_version"),
        )
        object.__setattr__(self, "instance_id", _required_text(self.instance_id, "instance_id"))
        object.__setattr__(self, "seed_parameters", _require_mapping(self.seed_parameters, "seed_parameters"))
        object.__setattr__(self, "base_state", _require_mapping(self.base_state, "base_state"))

    @property
    def source_ref(self) -> dict[str, Any]:
        return {
            "object_truth_ref": self.object_truth_ref,
            "object_truth_version": self.object_truth_version,
            "projection_version": self.projection_version,
        }

    @property
    def seed_digest(self) -> str:
        return virtual_lab_digest(self._digest_basis(), purpose="virtual_lab.seed_entry.v1")

    @property
    def base_state_digest(self) -> str:
        return virtual_lab_digest(self.base_state, purpose="virtual_lab.base_state.v1")

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.seed_entry.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "object_truth_ref": self.object_truth_ref,
            "object_truth_version": self.object_truth_version,
            "projection_version": self.projection_version,
            "seed_parameters": canonical_value(self.seed_parameters),
            "base_state": canonical_value(self.base_state),
            "base_state_digest": self.base_state_digest,
            "seed_digest": self.seed_digest,
        }

    def _digest_basis(self) -> dict[str, Any]:
        return {
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "object_truth_ref": self.object_truth_ref,
            "object_truth_version": self.object_truth_version,
            "projection_version": self.projection_version,
            "seed_parameters": canonical_value(self.seed_parameters),
            "base_state": canonical_value(self.base_state),
        }


@dataclass(frozen=True, slots=True)
class SeedManifest:
    entries: tuple[SeedManifestEntry, ...]

    def __post_init__(self) -> None:
        entries = tuple(self.entries or ())
        if not entries:
            raise VirtualLabStateError(
                "virtual_lab.seed_manifest_empty",
                "seed manifest requires at least one entry",
            )
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            key = (entry.object_id, entry.instance_id)
            if key in seen:
                raise VirtualLabStateError(
                    "virtual_lab.duplicate_seed_entry",
                    "seed manifest cannot contain duplicate object instance entries",
                    details={"object_id": entry.object_id, "instance_id": entry.instance_id},
                )
            seen.add(key)
        ordered = tuple(
            sorted(
                entries,
                key=lambda item: (
                    item.object_id,
                    item.instance_id,
                    item.object_truth_ref,
                    item.object_truth_version,
                    item.projection_version,
                ),
            )
        )
        object.__setattr__(self, "entries", ordered)

    @property
    def seed_digest(self) -> str:
        return virtual_lab_digest(
            [entry.to_json() for entry in self.entries],
            purpose="virtual_lab.seed_manifest.v1",
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.seed_manifest.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "entries": [entry.to_json() for entry in self.entries],
            "seed_digest": self.seed_digest,
        }


@dataclass(frozen=True, slots=True)
class EnvironmentRevision:
    environment_id: str
    revision_id: str
    parent_revision_id: str | None
    revision_reason: str
    seed_manifest: SeedManifest
    config_digest: str
    policy_digest: str
    created_at: str
    created_by: str
    status: str = REVISION_STATUS_ACTIVE
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "environment_id", _required_text(self.environment_id, "environment_id"))
        object.__setattr__(self, "revision_id", _required_text(self.revision_id, "revision_id"))
        object.__setattr__(
            self,
            "parent_revision_id",
            _optional_text(self.parent_revision_id),
        )
        object.__setattr__(self, "revision_reason", _required_text(self.revision_reason, "revision_reason"))
        object.__setattr__(self, "config_digest", _required_text(self.config_digest, "config_digest"))
        object.__setattr__(self, "policy_digest", _required_text(self.policy_digest, "policy_digest"))
        object.__setattr__(self, "created_at", _normalize_required_datetime(self.created_at, "created_at"))
        object.__setattr__(self, "created_by", _required_text(self.created_by, "created_by"))
        object.__setattr__(self, "status", _validate_member(self.status, REVISION_STATUSES, "status"))
        object.__setattr__(self, "metadata", _require_mapping(self.metadata, "metadata"))

    @property
    def seed_digest(self) -> str:
        return self.seed_manifest.seed_digest

    @property
    def revision_digest(self) -> str:
        return virtual_lab_digest(self._digest_basis(), purpose="virtual_lab.environment_revision.v1")

    @property
    def closed(self) -> bool:
        return self.status == REVISION_STATUS_CLOSED

    def close(self) -> "EnvironmentRevision":
        return EnvironmentRevision(
            environment_id=self.environment_id,
            revision_id=self.revision_id,
            parent_revision_id=self.parent_revision_id,
            revision_reason=self.revision_reason,
            seed_manifest=self.seed_manifest,
            config_digest=self.config_digest,
            policy_digest=self.policy_digest,
            created_at=self.created_at,
            created_by=self.created_by,
            status=REVISION_STATUS_CLOSED,
            metadata=self.metadata,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.environment_revision.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "parent_revision_id": self.parent_revision_id,
            "revision_reason": self.revision_reason,
            "seed_manifest": self.seed_manifest.to_json(),
            "seed_digest": self.seed_digest,
            "config_digest": self.config_digest,
            "policy_digest": self.policy_digest,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "status": self.status,
            "metadata": canonical_value(self.metadata),
            "revision_digest": self.revision_digest,
        }

    def _digest_basis(self) -> dict[str, Any]:
        return {
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "parent_revision_id": self.parent_revision_id,
            "revision_reason": self.revision_reason,
            "seed_digest": self.seed_digest,
            "config_digest": self.config_digest,
            "policy_digest": self.policy_digest,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "status": self.status,
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ObjectStateRecord:
    environment_id: str
    revision_id: str
    object_id: str
    instance_id: str
    source_ref: dict[str, Any]
    base_state: dict[str, Any]
    overlay_state: dict[str, Any] = field(default_factory=dict)
    last_event_id: str | None = None
    tombstone: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "environment_id", _required_text(self.environment_id, "environment_id"))
        object.__setattr__(self, "revision_id", _required_text(self.revision_id, "revision_id"))
        object.__setattr__(self, "object_id", _required_text(self.object_id, "object_id"))
        object.__setattr__(self, "instance_id", _required_text(self.instance_id, "instance_id"))
        object.__setattr__(self, "source_ref", _require_mapping(self.source_ref, "source_ref"))
        object.__setattr__(self, "base_state", _require_mapping(self.base_state, "base_state"))
        object.__setattr__(self, "overlay_state", _require_mapping(self.overlay_state, "overlay_state"))
        object.__setattr__(self, "last_event_id", _optional_text(self.last_event_id))
        object.__setattr__(self, "tombstone", bool(self.tombstone))

    @property
    def stream_id(self) -> str:
        return object_stream_id(
            environment_id=self.environment_id,
            revision_id=self.revision_id,
            object_id=self.object_id,
            instance_id=self.instance_id,
        )

    @property
    def base_state_digest(self) -> str:
        return virtual_lab_digest(self.base_state, purpose="virtual_lab.base_state.v1")

    @property
    def overlay_state_digest(self) -> str:
        return virtual_lab_digest(self.overlay_state, purpose="virtual_lab.overlay_state.v1")

    @property
    def effective_state(self) -> dict[str, Any]:
        return _deep_merge(self.base_state, self.overlay_state)

    @property
    def effective_state_digest(self) -> str:
        return virtual_lab_digest(self.effective_state, purpose="virtual_lab.effective_state.v1")

    @property
    def state_digest(self) -> str:
        return virtual_lab_digest(self._digest_basis(), purpose="virtual_lab.object_state_record.v1")

    def with_overlay_patch(
        self,
        patch: dict[str, Any],
        *,
        last_event_id: str | None,
    ) -> "ObjectStateRecord":
        return self.replace(
            overlay_state=_deep_merge(self.overlay_state, _require_mapping(patch, "patch")),
            last_event_id=last_event_id,
        )

    def with_overlay_replacement(
        self,
        overlay_state: dict[str, Any],
        *,
        last_event_id: str | None,
    ) -> "ObjectStateRecord":
        return self.replace(
            overlay_state=_require_mapping(overlay_state, "overlay_state"),
            last_event_id=last_event_id,
        )

    def with_tombstone(self, tombstone: bool, *, last_event_id: str | None) -> "ObjectStateRecord":
        return self.replace(tombstone=tombstone, last_event_id=last_event_id)

    def replace(self, **updates: Any) -> "ObjectStateRecord":
        payload = {
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "source_ref": self.source_ref,
            "base_state": self.base_state,
            "overlay_state": self.overlay_state,
            "last_event_id": self.last_event_id,
            "tombstone": self.tombstone,
        }
        payload.update(updates)
        return ObjectStateRecord(**payload)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.object_state.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "stream_id": self.stream_id,
            "source_ref": canonical_value(self.source_ref),
            "base_state": canonical_value(self.base_state),
            "overlay_state": canonical_value(self.overlay_state),
            "effective_state": canonical_value(self.effective_state),
            "base_state_digest": self.base_state_digest,
            "overlay_state_digest": self.overlay_state_digest,
            "effective_state_digest": self.effective_state_digest,
            "last_event_id": self.last_event_id,
            "tombstone": self.tombstone,
            "state_digest": self.state_digest,
        }

    def _digest_basis(self) -> dict[str, Any]:
        return {
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "source_ref": canonical_value(self.source_ref),
            "base_state_digest": self.base_state_digest,
            "overlay_state_digest": self.overlay_state_digest,
            "effective_state_digest": self.effective_state_digest,
            "tombstone": self.tombstone,
        }


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_id: str
    environment_id: str
    revision_id: str
    stream_id: str
    event_type: str
    event_version: int
    occurred_at: str
    recorded_at: str
    actor_id: str
    actor_type: ActorType
    command_id: str
    causation_id: str | None
    correlation_id: str | None
    parent_event_ids: tuple[str, ...]
    sequence_number: int
    pre_state_digest: str
    post_state_digest: str
    payload: dict[str, Any]
    payload_digest: str
    schema_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _required_text(self.event_id, "event_id"))
        object.__setattr__(self, "environment_id", _required_text(self.environment_id, "environment_id"))
        object.__setattr__(self, "revision_id", _required_text(self.revision_id, "revision_id"))
        object.__setattr__(self, "stream_id", _required_text(self.stream_id, "stream_id"))
        object.__setattr__(self, "event_type", _validate_member(self.event_type, EVENT_TYPES, "event_type"))
        if int(self.event_version) < 1:
            raise VirtualLabStateError(
                "virtual_lab.invalid_event_version",
                "event_version must be positive",
                details={"event_version": self.event_version},
            )
        object.__setattr__(self, "event_version", int(self.event_version))
        object.__setattr__(self, "occurred_at", _normalize_required_datetime(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "recorded_at", _normalize_required_datetime(self.recorded_at, "recorded_at"))
        object.__setattr__(self, "actor_id", _required_text(self.actor_id, "actor_id"))
        object.__setattr__(self, "actor_type", _validate_member(self.actor_type, ACTOR_TYPES, "actor_type"))
        object.__setattr__(self, "command_id", _required_text(self.command_id, "command_id"))
        object.__setattr__(self, "causation_id", _optional_text(self.causation_id))
        object.__setattr__(self, "correlation_id", _optional_text(self.correlation_id))
        object.__setattr__(self, "parent_event_ids", _clean_unique_tuple(self.parent_event_ids))
        if int(self.sequence_number) < 1:
            raise VirtualLabStateError(
                "virtual_lab.invalid_sequence_number",
                "sequence_number must be positive",
                details={"sequence_number": self.sequence_number},
            )
        object.__setattr__(self, "sequence_number", int(self.sequence_number))
        object.__setattr__(self, "pre_state_digest", _required_text(self.pre_state_digest, "pre_state_digest"))
        object.__setattr__(self, "post_state_digest", _required_text(self.post_state_digest, "post_state_digest"))
        object.__setattr__(self, "payload", _require_mapping(self.payload, "payload"))
        expected_payload_digest = virtual_lab_digest(self.payload, purpose="virtual_lab.event_payload.v1")
        if self.payload_digest != expected_payload_digest:
            raise VirtualLabStateError(
                "virtual_lab.payload_digest_mismatch",
                "payload_digest must match canonical payload",
                details={"expected": expected_payload_digest, "actual": self.payload_digest},
            )
        expected_schema_digest = event_schema_digest(self.event_type, self.event_version)
        if self.schema_digest != expected_schema_digest:
            raise VirtualLabStateError(
                "virtual_lab.schema_digest_mismatch",
                "schema_digest must match event type and version",
                details={"expected": expected_schema_digest, "actual": self.schema_digest},
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.event_envelope.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "event_id": self.event_id,
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "stream_id": self.stream_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at,
            "recorded_at": self.recorded_at,
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "command_id": self.command_id,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "parent_event_ids": list(self.parent_event_ids),
            "sequence_number": self.sequence_number,
            "pre_state_digest": self.pre_state_digest,
            "post_state_digest": self.post_state_digest,
            "payload": canonical_value(self.payload),
            "payload_digest": self.payload_digest,
            "schema_digest": self.schema_digest,
        }


@dataclass(frozen=True, slots=True)
class CommandReceipt:
    receipt_id: str
    command_id: str
    environment_id: str
    revision_id: str
    status: ReceiptStatus
    resulting_event_ids: tuple[str, ...]
    precondition_digest: str | None
    result_digest: str | None
    errors: tuple[dict[str, Any], ...]
    warnings: tuple[dict[str, Any], ...]
    issued_at: str
    issued_by: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "receipt_id", _required_text(self.receipt_id, "receipt_id"))
        object.__setattr__(self, "command_id", _required_text(self.command_id, "command_id"))
        object.__setattr__(self, "environment_id", _required_text(self.environment_id, "environment_id"))
        object.__setattr__(self, "revision_id", _required_text(self.revision_id, "revision_id"))
        object.__setattr__(self, "status", _validate_member(self.status, RECEIPT_STATUSES, "status"))
        object.__setattr__(self, "resulting_event_ids", _clean_unique_tuple(self.resulting_event_ids))
        object.__setattr__(self, "precondition_digest", _optional_text(self.precondition_digest))
        object.__setattr__(self, "result_digest", _optional_text(self.result_digest))
        object.__setattr__(self, "errors", tuple(_require_mapping(item, "error") for item in self.errors or ()))
        object.__setattr__(
            self,
            "warnings",
            tuple(_require_mapping(item, "warning") for item in self.warnings or ()),
        )
        object.__setattr__(self, "issued_at", _normalize_required_datetime(self.issued_at, "issued_at"))
        object.__setattr__(self, "issued_by", _required_text(self.issued_by, "issued_by"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.command_receipt.v1",
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
            "receipt_id": self.receipt_id,
            "command_id": self.command_id,
            "environment_id": self.environment_id,
            "revision_id": self.revision_id,
            "status": self.status,
            "resulting_event_ids": list(self.resulting_event_ids),
            "precondition_digest": self.precondition_digest,
            "result_digest": self.result_digest,
            "errors": [canonical_value(item) for item in self.errors],
            "warnings": [canonical_value(item) for item in self.warnings],
            "issued_at": self.issued_at,
            "issued_by": self.issued_by,
        }


@dataclass(frozen=True, slots=True)
class StateCommandResult:
    receipt: CommandReceipt
    state: ObjectStateRecord
    events: tuple[EventEnvelope, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.receipt.status == "accepted"

    def to_json(self) -> dict[str, Any]:
        return {
            "receipt": self.receipt.to_json(),
            "state": self.state.to_json(),
            "events": [event.to_json() for event in self.events],
        }


def build_seed_manifest(entries: list[SeedManifestEntry | dict[str, Any]] | tuple[SeedManifestEntry | dict[str, Any], ...]) -> SeedManifest:
    normalized = tuple(entry if isinstance(entry, SeedManifestEntry) else SeedManifestEntry(**entry) for entry in entries)
    return SeedManifest(entries=normalized)


def build_environment_revision(
    *,
    environment_id: str,
    revision_reason: str,
    seed_manifest: SeedManifest,
    config: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    created_at: Any,
    created_by: str,
    parent_revision_id: str | None = None,
    revision_id: str | None = None,
    status: str = REVISION_STATUS_ACTIVE,
    metadata: dict[str, Any] | None = None,
) -> EnvironmentRevision:
    config_digest = virtual_lab_digest(config or {}, purpose="virtual_lab.revision_config.v1")
    policy_digest = virtual_lab_digest(policy or {}, purpose="virtual_lab.revision_policy.v1")
    created_at_iso = _normalize_required_datetime(created_at, "created_at")
    revision_basis = {
        "environment_id": _required_text(environment_id, "environment_id"),
        "parent_revision_id": _optional_text(parent_revision_id),
        "revision_reason": _required_text(revision_reason, "revision_reason"),
        "seed_digest": seed_manifest.seed_digest,
        "config_digest": config_digest,
        "policy_digest": policy_digest,
        "created_at": created_at_iso,
        "created_by": _required_text(created_by, "created_by"),
        "metadata": canonical_value(metadata or {}),
    }
    resolved_revision_id = _optional_text(revision_id) or (
        "virtual_lab_revision."
        f"{canonical_digest(revision_basis, purpose='virtual_lab.revision_id.v1')[:20]}"
    )
    return EnvironmentRevision(
        environment_id=str(environment_id),
        revision_id=resolved_revision_id,
        parent_revision_id=parent_revision_id,
        revision_reason=revision_reason,
        seed_manifest=seed_manifest,
        config_digest=config_digest,
        policy_digest=policy_digest,
        created_at=created_at_iso,
        created_by=created_by,
        status=status,
        metadata=metadata or {},
    )


def object_state_from_seed(
    revision: EnvironmentRevision,
    entry: SeedManifestEntry,
    *,
    last_event_id: str | None = None,
) -> ObjectStateRecord:
    return ObjectStateRecord(
        environment_id=revision.environment_id,
        revision_id=revision.revision_id,
        object_id=entry.object_id,
        instance_id=entry.instance_id,
        source_ref=entry.source_ref,
        base_state=entry.base_state,
        overlay_state={},
        last_event_id=last_event_id,
        tombstone=False,
    )


def object_states_from_seed_manifest(revision: EnvironmentRevision) -> tuple[ObjectStateRecord, ...]:
    return tuple(object_state_from_seed(revision, entry) for entry in revision.seed_manifest.entries)


def build_event_envelope(
    *,
    environment_id: str,
    revision_id: str,
    stream_id: str,
    event_type: str,
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    pre_state_digest: str,
    post_state_digest: str,
    payload: dict[str, Any],
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope] = (),
    event_version: int = 1,
    causation_id: str | None = None,
    correlation_id: str | None = None,
    parent_event_ids: tuple[str, ...] | list[str] = (),
    sequence_number: int | None = None,
    event_id: str | None = None,
) -> EventEnvelope:
    existing_stream_events = tuple(event for event in stream_events if event.stream_id == stream_id)
    validate_event_stream(existing_stream_events)
    next_sequence = _next_sequence_number(existing_stream_events)
    resolved_sequence = int(sequence_number) if sequence_number is not None else next_sequence
    if resolved_sequence != next_sequence:
        raise VirtualLabStateError(
            "virtual_lab.sequence_conflict",
            "event sequence must be monotonic within stream",
            details={"expected": next_sequence, "actual": resolved_sequence, "stream_id": stream_id},
        )
    payload_digest = virtual_lab_digest(payload, purpose="virtual_lab.event_payload.v1")
    schema_digest = event_schema_digest(event_type, event_version)
    basis = {
        "environment_id": _required_text(environment_id, "environment_id"),
        "revision_id": _required_text(revision_id, "revision_id"),
        "stream_id": _required_text(stream_id, "stream_id"),
        "event_type": event_type,
        "event_version": int(event_version),
        "occurred_at": _normalize_required_datetime(occurred_at, "occurred_at"),
        "recorded_at": _normalize_required_datetime(recorded_at, "recorded_at"),
        "actor": actor.to_json(),
        "command_id": _required_text(command_id, "command_id"),
        "causation_id": _optional_text(causation_id),
        "correlation_id": _optional_text(correlation_id),
        "parent_event_ids": list(_clean_unique_tuple(parent_event_ids)),
        "sequence_number": resolved_sequence,
        "pre_state_digest": _required_text(pre_state_digest, "pre_state_digest"),
        "post_state_digest": _required_text(post_state_digest, "post_state_digest"),
        "payload_digest": payload_digest,
        "schema_digest": schema_digest,
    }
    resolved_event_id = _optional_text(event_id) or (
        "virtual_lab_event."
        f"{canonical_digest(basis, purpose='virtual_lab.event_id.v1')[:20]}"
    )
    event = EventEnvelope(
        event_id=resolved_event_id,
        environment_id=environment_id,
        revision_id=revision_id,
        stream_id=stream_id,
        event_type=event_type,
        event_version=event_version,
        occurred_at=basis["occurred_at"],
        recorded_at=basis["recorded_at"],
        actor_id=actor.actor_id,
        actor_type=actor.actor_type,
        command_id=command_id,
        causation_id=causation_id,
        correlation_id=correlation_id,
        parent_event_ids=tuple(parent_event_ids),
        sequence_number=resolved_sequence,
        pre_state_digest=pre_state_digest,
        post_state_digest=post_state_digest,
        payload=payload,
        payload_digest=payload_digest,
        schema_digest=schema_digest,
    )
    validate_event_append(stream_events, event)
    return event


def event_schema_digest(event_type: str, event_version: int = 1) -> str:
    return virtual_lab_digest(
        {
            "event_type": _validate_member(event_type, EVENT_TYPES, "event_type"),
            "event_version": int(event_version),
            "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
        },
        purpose="virtual_lab.event_schema.v1",
    )


def apply_overlay_patch_command(
    *,
    revision: EnvironmentRevision,
    state: ObjectStateRecord,
    patch: dict[str, Any],
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope] = (),
    expected_state_digest: str | None = None,
    causation_id: str | None = None,
    correlation_id: str | None = None,
    parent_event_ids: tuple[str, ...] | list[str] = (),
) -> StateCommandResult:
    return _mutate_object_state(
        revision=revision,
        state=state,
        event_type="object.patched",
        payload={"overlay_patch": _require_mapping(patch, "patch")},
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        stream_events=stream_events,
        expected_state_digest=expected_state_digest,
        causation_id=causation_id,
        correlation_id=correlation_id,
        parent_event_ids=parent_event_ids,
    )


def replace_overlay_command(
    *,
    revision: EnvironmentRevision,
    state: ObjectStateRecord,
    overlay_state: dict[str, Any],
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope] = (),
    expected_state_digest: str | None = None,
) -> StateCommandResult:
    return _mutate_object_state(
        revision=revision,
        state=state,
        event_type="object.replaced",
        payload={"overlay_state": _require_mapping(overlay_state, "overlay_state")},
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        stream_events=stream_events,
        expected_state_digest=expected_state_digest,
    )


def tombstone_object_command(
    *,
    revision: EnvironmentRevision,
    state: ObjectStateRecord,
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope] = (),
    expected_state_digest: str | None = None,
) -> StateCommandResult:
    return _mutate_object_state(
        revision=revision,
        state=state,
        event_type="object.tombstoned",
        payload={"tombstone": True},
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        stream_events=stream_events,
        expected_state_digest=expected_state_digest,
    )


def restore_object_command(
    *,
    revision: EnvironmentRevision,
    state: ObjectStateRecord,
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope] = (),
    expected_state_digest: str | None = None,
) -> StateCommandResult:
    return _mutate_object_state(
        revision=revision,
        state=state,
        event_type="object.restored",
        payload={"tombstone": False},
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        stream_events=stream_events,
        expected_state_digest=expected_state_digest,
    )


def validate_event_append(existing_events: tuple[EventEnvelope, ...] | list[EventEnvelope], event: EventEnvelope) -> None:
    stream_events = tuple(item for item in existing_events if item.stream_id == event.stream_id)
    validate_event_stream(stream_events)
    event_ids = {item.event_id for item in existing_events}
    if event.event_id in event_ids:
        raise VirtualLabStateError(
            "virtual_lab.duplicate_event_id",
            "event_id already exists",
            details={"event_id": event.event_id},
        )
    sequence_numbers = {item.sequence_number for item in stream_events}
    if event.sequence_number in sequence_numbers:
        raise VirtualLabStateError(
            "virtual_lab.duplicate_stream_sequence",
            "stream sequence_number already exists",
            details={"stream_id": event.stream_id, "sequence_number": event.sequence_number},
        )
    expected_sequence = _next_sequence_number(stream_events)
    if event.sequence_number != expected_sequence:
        raise VirtualLabStateError(
            "virtual_lab.sequence_gap",
            "stream sequence_number must not skip or go backward",
            details={
                "stream_id": event.stream_id,
                "expected": expected_sequence,
                "actual": event.sequence_number,
            },
        )
    ordered = sorted(stream_events, key=lambda item: item.sequence_number)
    if ordered and event.pre_state_digest != ordered[-1].post_state_digest:
        raise VirtualLabStateError(
            "virtual_lab.pre_state_digest_mismatch",
            "event pre_state_digest must match prior stream post_state_digest",
            details={
                "stream_id": event.stream_id,
                "expected": ordered[-1].post_state_digest,
                "actual": event.pre_state_digest,
            },
        )


def validate_event_stream(events: tuple[EventEnvelope, ...] | list[EventEnvelope]) -> None:
    by_stream: dict[str, list[EventEnvelope]] = {}
    seen_ids: set[str] = set()
    for event in events:
        if event.event_id in seen_ids:
            raise VirtualLabStateError(
                "virtual_lab.duplicate_event_id",
                "event_id already exists",
                details={"event_id": event.event_id},
            )
        seen_ids.add(event.event_id)
        by_stream.setdefault(event.stream_id, []).append(event)

    for stream_id, stream_events in by_stream.items():
        ordered = sorted(stream_events, key=lambda item: item.sequence_number)
        for index, event in enumerate(ordered, start=1):
            if event.sequence_number != index:
                raise VirtualLabStateError(
                    "virtual_lab.sequence_gap",
                    "stream sequence_number must be contiguous",
                    details={
                        "stream_id": stream_id,
                        "expected": index,
                        "actual": event.sequence_number,
                    },
                )
            if index > 1 and event.pre_state_digest != ordered[index - 2].post_state_digest:
                raise VirtualLabStateError(
                    "virtual_lab.pre_state_digest_mismatch",
                    "event pre_state_digest must match prior stream post_state_digest",
                    details={
                        "stream_id": stream_id,
                        "expected": ordered[index - 2].post_state_digest,
                        "actual": event.pre_state_digest,
                    },
                )


def event_chain_digest(events: tuple[EventEnvelope, ...] | list[EventEnvelope]) -> str:
    validate_event_stream(events)
    rolling: dict[str, Any] = {
        "schema_version": VIRTUAL_LAB_STATE_SCHEMA_VERSION,
        "events": [],
    }
    for event in sorted(events, key=lambda item: (item.stream_id, item.sequence_number, item.event_id)):
        rolling["events"].append(
            {
                "prior_chain_digest": virtual_lab_digest(rolling, purpose="virtual_lab.event_chain_step.v1"),
                "event": event.to_json(),
            }
        )
    return virtual_lab_digest(rolling, purpose="virtual_lab.event_chain.v1")


def replay_object_events(
    initial_state: ObjectStateRecord,
    events: tuple[EventEnvelope, ...] | list[EventEnvelope],
) -> ObjectStateRecord:
    stream_events = tuple(sorted((event for event in events if event.stream_id == initial_state.stream_id), key=lambda item: item.sequence_number))
    validate_event_stream(stream_events)
    state = initial_state
    for event in stream_events:
        state = apply_object_event(state, event)
    return state


def replay_environment_state(
    initial_states: tuple[ObjectStateRecord, ...] | list[ObjectStateRecord],
    events: tuple[EventEnvelope, ...] | list[EventEnvelope],
) -> dict[str, ObjectStateRecord]:
    validate_event_stream(events)
    states = {_object_instance_key(state.object_id, state.instance_id): state for state in initial_states}
    state_streams = {state.stream_id for state in initial_states}
    by_stream: dict[str, list[EventEnvelope]] = {}
    for event in events:
        if event.event_type not in OBJECT_EVENT_TYPES:
            continue
        if event.stream_id not in state_streams:
            raise VirtualLabStateError(
                "virtual_lab.orphan_object_event",
                "object event stream has no seeded object state",
                details={"stream_id": event.stream_id, "event_id": event.event_id},
            )
        by_stream.setdefault(event.stream_id, []).append(event)

    for key, state in tuple(states.items()):
        stream_events = by_stream.get(state.stream_id, [])
        states[key] = replay_object_events(state, stream_events)
    return states


def apply_object_event(state: ObjectStateRecord, event: EventEnvelope) -> ObjectStateRecord:
    if event.stream_id != state.stream_id:
        raise VirtualLabStateError(
            "virtual_lab.event_stream_mismatch",
            "object event stream_id does not match state stream",
            details={"expected": state.stream_id, "actual": event.stream_id},
        )
    if event.event_type == "object.seeded":
        if event.pre_state_digest != EMPTY_STATE_DIGEST:
            raise VirtualLabStateError(
                "virtual_lab.seed_event_pre_state_invalid",
                "object.seeded must start from empty state",
                details={"actual": event.pre_state_digest},
            )
        if event.post_state_digest != state.state_digest:
            raise VirtualLabStateError(
                "virtual_lab.post_state_digest_mismatch",
                "object.seeded post_state_digest must match seeded state",
                details={"expected": state.state_digest, "actual": event.post_state_digest},
            )
        return state.replace(last_event_id=event.event_id)
    if event.pre_state_digest != state.state_digest:
        raise VirtualLabStateError(
            "virtual_lab.pre_state_digest_mismatch",
            "event pre_state_digest must match current state",
            details={"expected": state.state_digest, "actual": event.pre_state_digest},
        )
    if event.event_type == "object.patched":
        next_state = state.with_overlay_patch(
            event.payload.get("overlay_patch") or {},
            last_event_id=event.event_id,
        )
    elif event.event_type == "object.replaced":
        next_state = state.with_overlay_replacement(
            event.payload.get("overlay_state") or {},
            last_event_id=event.event_id,
        )
    elif event.event_type == "object.tombstoned":
        next_state = state.with_tombstone(True, last_event_id=event.event_id)
    elif event.event_type == "object.restored":
        next_state = state.with_tombstone(False, last_event_id=event.event_id)
    else:
        raise VirtualLabStateError(
            "virtual_lab.unsupported_object_event",
            "event_type cannot be applied to object state",
            details={"event_type": event.event_type},
        )
    if next_state.state_digest != event.post_state_digest:
        raise VirtualLabStateError(
            "virtual_lab.post_state_digest_mismatch",
            "event post_state_digest must match computed next state",
            details={"expected": next_state.state_digest, "actual": event.post_state_digest},
        )
    return next_state


def build_receipt(
    *,
    command_id: str,
    environment_id: str,
    revision_id: str,
    status: ReceiptStatus,
    issued_at: Any,
    issued_by: str,
    resulting_event_ids: tuple[str, ...] | list[str] = (),
    precondition_digest: str | None = None,
    result_digest: str | None = None,
    errors: tuple[dict[str, Any], ...] | list[dict[str, Any]] = (),
    warnings: tuple[dict[str, Any], ...] | list[dict[str, Any]] = (),
    receipt_id: str | None = None,
) -> CommandReceipt:
    issued_at_iso = _normalize_required_datetime(issued_at, "issued_at")
    basis = {
        "command_id": _required_text(command_id, "command_id"),
        "environment_id": _required_text(environment_id, "environment_id"),
        "revision_id": _required_text(revision_id, "revision_id"),
        "status": _validate_member(status, RECEIPT_STATUSES, "status"),
        "resulting_event_ids": list(_clean_unique_tuple(resulting_event_ids)),
        "precondition_digest": _optional_text(precondition_digest),
        "result_digest": _optional_text(result_digest),
        "errors": [canonical_value(item) for item in errors],
        "warnings": [canonical_value(item) for item in warnings],
        "issued_at": issued_at_iso,
        "issued_by": _required_text(issued_by, "issued_by"),
    }
    resolved_receipt_id = _optional_text(receipt_id) or (
        "virtual_lab_receipt."
        f"{canonical_digest(basis, purpose='virtual_lab.receipt_id.v1')[:20]}"
    )
    return CommandReceipt(receipt_id=resolved_receipt_id, **basis)


def _mutate_object_state(
    *,
    revision: EnvironmentRevision,
    state: ObjectStateRecord,
    event_type: str,
    payload: dict[str, Any],
    actor: ActorIdentity,
    command_id: str,
    occurred_at: Any,
    recorded_at: Any,
    stream_events: tuple[EventEnvelope, ...] | list[EventEnvelope],
    expected_state_digest: str | None,
    causation_id: str | None = None,
    correlation_id: str | None = None,
    parent_event_ids: tuple[str, ...] | list[str] = (),
) -> StateCommandResult:
    if revision.environment_id != state.environment_id or revision.revision_id != state.revision_id:
        raise VirtualLabStateError(
            "virtual_lab.revision_state_mismatch",
            "object state must belong to the target environment revision",
            details={
                "revision_environment_id": revision.environment_id,
                "state_environment_id": state.environment_id,
                "revision_id": revision.revision_id,
                "state_revision_id": state.revision_id,
            },
        )
    duplicate = _event_for_command(stream_events, command_id, state.stream_id)
    if duplicate is not None:
        receipt = build_receipt(
            command_id=command_id,
            environment_id=revision.environment_id,
            revision_id=revision.revision_id,
            status="no_op",
            resulting_event_ids=(duplicate.event_id,),
            precondition_digest=state.state_digest,
            result_digest=duplicate.post_state_digest,
            warnings=(
                {
                    "reason_code": "virtual_lab.duplicate_command",
                    "message": "command_id already produced an event in this stream",
                    "event_id": duplicate.event_id,
                },
            ),
            issued_at=recorded_at,
            issued_by=actor.actor_id,
        )
        return StateCommandResult(receipt=receipt, state=state, events=())
    if revision.closed:
        receipt = build_receipt(
            command_id=command_id,
            environment_id=revision.environment_id,
            revision_id=revision.revision_id,
            status="rejected",
            precondition_digest=state.state_digest,
            result_digest=state.state_digest,
            errors=(
                {
                    "reason_code": "virtual_lab.revision_closed",
                    "message": "closed environment revisions cannot accept write commands",
                },
            ),
            issued_at=recorded_at,
            issued_by=actor.actor_id,
        )
        return StateCommandResult(receipt=receipt, state=state, events=())
    expected_digest = _optional_text(expected_state_digest)
    if expected_digest is not None and expected_digest != state.state_digest:
        receipt = build_receipt(
            command_id=command_id,
            environment_id=revision.environment_id,
            revision_id=revision.revision_id,
            status="conflict",
            precondition_digest=expected_digest,
            result_digest=state.state_digest,
            errors=(
                {
                    "reason_code": "virtual_lab.expected_state_digest_mismatch",
                    "message": "expected state digest does not match current state",
                    "expected": expected_digest,
                    "actual": state.state_digest,
                },
            ),
            issued_at=recorded_at,
            issued_by=actor.actor_id,
        )
        return StateCommandResult(receipt=receipt, state=state, events=())

    pre_state_digest = state.state_digest
    preview_event_id = "pending"
    if event_type == "object.patched":
        next_state = state.with_overlay_patch(payload["overlay_patch"], last_event_id=preview_event_id)
    elif event_type == "object.replaced":
        next_state = state.with_overlay_replacement(payload["overlay_state"], last_event_id=preview_event_id)
    elif event_type == "object.tombstoned":
        next_state = state.with_tombstone(True, last_event_id=preview_event_id)
    elif event_type == "object.restored":
        next_state = state.with_tombstone(False, last_event_id=preview_event_id)
    else:
        raise VirtualLabStateError(
            "virtual_lab.unsupported_mutation_event",
            "unsupported object mutation event",
            details={"event_type": event_type},
        )
    event = build_event_envelope(
        environment_id=revision.environment_id,
        revision_id=revision.revision_id,
        stream_id=state.stream_id,
        event_type=event_type,
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        pre_state_digest=pre_state_digest,
        post_state_digest=next_state.state_digest,
        payload=payload,
        stream_events=stream_events,
        causation_id=causation_id,
        correlation_id=correlation_id,
        parent_event_ids=parent_event_ids,
    )
    next_state = next_state.replace(last_event_id=event.event_id)
    event = build_event_envelope(
        environment_id=revision.environment_id,
        revision_id=revision.revision_id,
        stream_id=state.stream_id,
        event_type=event_type,
        actor=actor,
        command_id=command_id,
        occurred_at=occurred_at,
        recorded_at=recorded_at,
        pre_state_digest=pre_state_digest,
        post_state_digest=next_state.state_digest,
        payload=payload,
        stream_events=stream_events,
        causation_id=causation_id,
        correlation_id=correlation_id,
        parent_event_ids=parent_event_ids,
    )
    receipt = build_receipt(
        command_id=command_id,
        environment_id=revision.environment_id,
        revision_id=revision.revision_id,
        status="accepted",
        resulting_event_ids=(event.event_id,),
        precondition_digest=pre_state_digest,
        result_digest=next_state.state_digest,
        issued_at=recorded_at,
        issued_by=actor.actor_id,
    )
    return StateCommandResult(receipt=receipt, state=next_state, events=(event,))


def _event_for_command(
    events: tuple[EventEnvelope, ...] | list[EventEnvelope],
    command_id: str,
    stream_id: str,
) -> EventEnvelope | None:
    for event in events:
        if event.stream_id == stream_id and event.command_id == command_id:
            return event
    return None


def _next_sequence_number(events: tuple[EventEnvelope, ...] | list[EventEnvelope]) -> int:
    if not events:
        return 1
    ordered = sorted(events, key=lambda item: item.sequence_number)
    return ordered[-1].sequence_number + 1


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(canonical_value(base))
    for key, value in canonical_value(overlay).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _object_instance_key(object_id: str, instance_id: str) -> str:
    return f"{object_id}#{instance_id}"


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise VirtualLabStateError(
            f"virtual_lab.{field_name}_required",
            f"{field_name} is required",
        )
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VirtualLabStateError(
            f"virtual_lab.{field_name}_not_object",
            f"{field_name} must be a JSON object",
            details={"value_type": type(value).__name__},
        )
    return dict(canonical_value(value))


def _validate_member(value: Any, allowed: set[str], field_name: str) -> str:
    text = _required_text(value, field_name)
    if text not in allowed:
        raise VirtualLabStateError(
            f"virtual_lab.invalid_{field_name}",
            f"{field_name} is not supported",
            details={"value": text, "allowed": sorted(allowed)},
        )
    return text


def _clean_unique_tuple(values: tuple[Any, ...] | list[Any] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    cleaned = sorted({_required_text(value, "ref") for value in values})
    return tuple(cleaned)


def _normalize_required_datetime(value: Any, field_name: str) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        text = _required_text(value, field_name)
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise VirtualLabStateError(
                f"virtual_lab.invalid_{field_name}",
                f"{field_name} must be an ISO datetime",
                details={"value": text},
            ) from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
