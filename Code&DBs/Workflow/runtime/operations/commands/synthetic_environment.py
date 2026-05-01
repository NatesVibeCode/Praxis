"""CQRS commands for Synthetic Environment authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.synthetic_environment import (
    MAX_ENVIRONMENT_RECORDS,
    SyntheticEnvironmentError,
    advance_synthetic_environment_clock,
    clear_synthetic_environment,
    create_synthetic_environment_from_dataset,
    inject_synthetic_environment_event,
    reset_synthetic_environment,
)
from storage.postgres.synthetic_data_repository import load_synthetic_dataset
from storage.postgres.synthetic_environment_repository import (
    load_synthetic_environment,
    next_synthetic_environment_effect_sequence,
    persist_synthetic_environment,
)


class _SyntheticEnvironmentCommand(BaseModel):
    observed_by_ref: str | None = None
    source_ref: str | None = None
    actor_ref: str | None = None

    @field_validator("observed_by_ref", "source_ref", "actor_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()


class CreateSyntheticEnvironmentCommand(_SyntheticEnvironmentCommand):
    """Create a mutable Synthetic Environment from one Synthetic Data dataset."""

    dataset_ref: str
    namespace: str | None = None
    environment_ref: str | None = None
    seed: str | None = None
    clock_time: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    max_records: int = Field(default=MAX_ENVIRONMENT_RECORDS, ge=1, le=MAX_ENVIRONMENT_RECORDS)

    @field_validator("dataset_ref", mode="before")
    @classmethod
    def _normalize_dataset_ref(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("dataset_ref must be a non-empty string")
        return value.strip()

    @field_validator("namespace", "environment_ref", "seed", "clock_time", mode="before")
    @classmethod
    def _normalize_create_optional_text(cls, value: object) -> str | None:
        return _normalize_optional_text(value)

    @field_validator("metadata", mode="before")
    @classmethod
    def _normalize_metadata(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("metadata must be a JSON object")


class _ExistingEnvironmentCommand(_SyntheticEnvironmentCommand):
    environment_ref: str
    reason: str | None = None

    @field_validator("environment_ref", mode="before")
    @classmethod
    def _normalize_environment_ref(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("environment_ref must be a non-empty string")
        return value.strip()

    @field_validator("reason", mode="before")
    @classmethod
    def _normalize_reason(cls, value: object) -> str | None:
        return _normalize_optional_text(value)


class ClearSyntheticEnvironmentCommand(_ExistingEnvironmentCommand):
    """Clear current mutable records while preserving seed and effect history."""


class ResetSyntheticEnvironmentCommand(_ExistingEnvironmentCommand):
    """Reset current mutable records back to the seed state."""


class InjectSyntheticEnvironmentEventCommand(_ExistingEnvironmentCommand):
    """Inject one outside event into a Synthetic Environment."""

    event_type: str
    event_payload: dict[str, Any] = Field(default_factory=dict)
    target_refs: list[str] = Field(default_factory=list)
    occurred_at: str | None = None
    event_ref: str | None = None

    @field_validator("event_type", mode="before")
    @classmethod
    def _normalize_event_type(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("event_type must be a non-empty string")
        return value.strip()

    @field_validator("event_payload", mode="before")
    @classmethod
    def _normalize_payload(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("event_payload must be a JSON object")

    @field_validator("target_refs", mode="before")
    @classmethod
    def _normalize_targets(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raise ValueError("target_refs must be a list of strings")

    @field_validator("occurred_at", "event_ref", mode="before")
    @classmethod
    def _normalize_event_optional_text(cls, value: object) -> str | None:
        return _normalize_optional_text(value)


class AdvanceSyntheticEnvironmentClockCommand(_ExistingEnvironmentCommand):
    """Advance or set a Synthetic Environment clock."""

    seconds: int | None = Field(default=None, ge=0)
    set_time: str | None = None

    @field_validator("set_time", mode="before")
    @classmethod
    def _normalize_set_time(cls, value: object) -> str | None:
        return _normalize_optional_text(value)

    @model_validator(mode="after")
    def _validate_clock_change(self) -> "AdvanceSyntheticEnvironmentClockCommand":
        if self.seconds is None and self.set_time is None:
            raise ValueError("seconds or set_time is required")
        return self


def _normalize_optional_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("optional text fields must be non-empty strings when supplied")
    return value.strip()


def _not_found(operation: str, *, ref_name: str, ref: str, error_code: str) -> dict[str, Any]:
    return {
        "ok": False,
        "operation": operation,
        "error_code": error_code,
        "error": f"{ref_name} not found",
        "details": {ref_name: ref},
    }


def _event_payload(environment: dict[str, Any], effect: dict[str, Any]) -> dict[str, Any]:
    return {
        "environment_ref": environment["environment_ref"],
        "namespace": environment["namespace"],
        "source_dataset_ref": environment["source_dataset_ref"],
        "lifecycle_state": environment["lifecycle_state"],
        "clock_time": environment["clock_time"],
        "current_state_digest": environment["current_state_digest"],
        "record_count": environment["record_count"],
        "current_record_count": environment["current_record_count"],
        "dirty_record_count": environment["dirty_record_count"],
        "effect_ref": effect["effect_ref"],
        "effect_type": effect["effect_type"],
        "sequence_number": effect["sequence_number"],
        "changed_record_count": effect["changed_record_count"],
    }


def _domain_error(operation: str, exc: SyntheticEnvironmentError) -> dict[str, Any]:
    return {
        "ok": False,
        "operation": operation,
        "error_code": exc.reason_code,
        "error": str(exc),
        "details": exc.details,
    }


def handle_synthetic_environment_create(
    command: CreateSyntheticEnvironmentCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Create and persist one mutable Synthetic Environment."""

    operation = "synthetic_environment_create"
    conn = subsystems.get_pg_conn()
    dataset = load_synthetic_dataset(
        conn,
        dataset_ref=command.dataset_ref,
        include_records=True,
        limit=command.max_records,
    )
    if dataset is None:
        return _not_found(
            operation,
            ref_name="dataset_ref",
            ref=command.dataset_ref,
            error_code="synthetic_environment.dataset_not_found",
        )
    try:
        environment, effect = create_synthetic_environment_from_dataset(
            dataset=dataset,
            namespace=command.namespace,
            environment_ref=command.environment_ref,
            seed=command.seed,
            clock_time=command.clock_time,
            metadata=command.metadata,
            observed_by_ref=command.observed_by_ref,
            source_ref=command.source_ref,
            sequence_number=1,
            actor_ref=command.actor_ref,
        )
        persisted = persist_synthetic_environment(conn, environment=environment, effect=effect)
    except SyntheticEnvironmentError as exc:
        return _domain_error(operation, exc)
    return {
        "ok": True,
        "operation": operation,
        "environment_ref": persisted["environment_ref"],
        "environment": persisted,
        "effect": effect,
        "event_payload": _event_payload(persisted, effect),
    }


def _load_for_mutation(operation: str, conn: Any, environment_ref: str) -> dict[str, Any] | dict[str, Any]:
    environment = load_synthetic_environment(conn, environment_ref=environment_ref)
    if environment is None:
        return _not_found(
            operation,
            ref_name="environment_ref",
            ref=environment_ref,
            error_code="synthetic_environment.environment_not_found",
        )
    return environment


def handle_synthetic_environment_clear(
    command: ClearSyntheticEnvironmentCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Clear current mutable records and persist the effect."""

    operation = "synthetic_environment_clear"
    conn = subsystems.get_pg_conn()
    environment = _load_for_mutation(operation, conn, command.environment_ref)
    if environment.get("ok") is False:
        return environment
    try:
        updated, effect = clear_synthetic_environment(
            environment,
            reason=command.reason,
            sequence_number=next_synthetic_environment_effect_sequence(conn, environment_ref=command.environment_ref),
            actor_ref=command.actor_ref,
        )
        persisted = persist_synthetic_environment(conn, environment=updated, effect=effect)
    except SyntheticEnvironmentError as exc:
        return _domain_error(operation, exc)
    return {
        "ok": True,
        "operation": operation,
        "environment_ref": persisted["environment_ref"],
        "environment": persisted,
        "effect": effect,
        "event_payload": _event_payload(persisted, effect),
    }


def handle_synthetic_environment_reset(
    command: ResetSyntheticEnvironmentCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Reset current mutable records to seed and persist the effect."""

    operation = "synthetic_environment_reset"
    conn = subsystems.get_pg_conn()
    environment = _load_for_mutation(operation, conn, command.environment_ref)
    if environment.get("ok") is False:
        return environment
    try:
        updated, effect = reset_synthetic_environment(
            environment,
            reason=command.reason,
            sequence_number=next_synthetic_environment_effect_sequence(conn, environment_ref=command.environment_ref),
            actor_ref=command.actor_ref,
        )
        persisted = persist_synthetic_environment(conn, environment=updated, effect=effect)
    except SyntheticEnvironmentError as exc:
        return _domain_error(operation, exc)
    return {
        "ok": True,
        "operation": operation,
        "environment_ref": persisted["environment_ref"],
        "environment": persisted,
        "effect": effect,
        "event_payload": _event_payload(persisted, effect),
    }


def handle_synthetic_environment_event_inject(
    command: InjectSyntheticEnvironmentEventCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Inject one outside event and persist the resulting effect."""

    operation = "synthetic_environment_event_inject"
    conn = subsystems.get_pg_conn()
    environment = _load_for_mutation(operation, conn, command.environment_ref)
    if environment.get("ok") is False:
        return environment
    try:
        updated, effect = inject_synthetic_environment_event(
            environment,
            event_type=command.event_type,
            event_payload=command.event_payload,
            target_refs=command.target_refs,
            occurred_at=command.occurred_at,
            event_ref=command.event_ref,
            sequence_number=next_synthetic_environment_effect_sequence(conn, environment_ref=command.environment_ref),
            actor_ref=command.actor_ref,
        )
        persisted = persist_synthetic_environment(conn, environment=updated, effect=effect)
    except SyntheticEnvironmentError as exc:
        return _domain_error(operation, exc)
    return {
        "ok": True,
        "operation": operation,
        "environment_ref": persisted["environment_ref"],
        "environment": persisted,
        "effect": effect,
        "event_payload": _event_payload(persisted, effect),
    }


def handle_synthetic_environment_clock_advance(
    command: AdvanceSyntheticEnvironmentClockCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Advance or set the environment clock and persist the effect."""

    operation = "synthetic_environment_clock_advance"
    conn = subsystems.get_pg_conn()
    environment = _load_for_mutation(operation, conn, command.environment_ref)
    if environment.get("ok") is False:
        return environment
    try:
        updated, effect = advance_synthetic_environment_clock(
            environment,
            seconds=command.seconds,
            set_time=command.set_time,
            reason=command.reason,
            sequence_number=next_synthetic_environment_effect_sequence(conn, environment_ref=command.environment_ref),
            actor_ref=command.actor_ref,
        )
        persisted = persist_synthetic_environment(conn, environment=updated, effect=effect)
    except SyntheticEnvironmentError as exc:
        return _domain_error(operation, exc)
    return {
        "ok": True,
        "operation": operation,
        "environment_ref": persisted["environment_ref"],
        "environment": persisted,
        "effect": effect,
        "event_payload": _event_payload(persisted, effect),
    }


__all__ = [
    "AdvanceSyntheticEnvironmentClockCommand",
    "ClearSyntheticEnvironmentCommand",
    "CreateSyntheticEnvironmentCommand",
    "InjectSyntheticEnvironmentEventCommand",
    "ResetSyntheticEnvironmentCommand",
    "handle_synthetic_environment_clear",
    "handle_synthetic_environment_clock_advance",
    "handle_synthetic_environment_create",
    "handle_synthetic_environment_event_inject",
    "handle_synthetic_environment_reset",
]
