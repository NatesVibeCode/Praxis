"""Canonical operator-control authority.

This module defines validated operator decisions and cutover gates. It does not
own raw SQL, connection management, or infer control state from markdown, shell
history, or queue folklore.

Cross-cutting architecture policy guidance belongs in `operator_decisions`
under the typed `architecture_policy` decision kind. If the decision table
shape needs cleanup, simplify or improve `operator_decisions`; do not create a
parallel decision store that competes with it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from types import MappingProxyType
from typing import Any

from runtime.validation import (
    normalize_as_of as _shared_normalize_as_of,
    require_datetime as _shared_require_datetime,
    require_mapping as _shared_require_mapping,
    require_text as _shared_require_text,
)


class OperatorControlRepositoryError(RuntimeError):
    """Raised when operator-control authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _error(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> OperatorControlRepositoryError:
    return OperatorControlRepositoryError(reason_code, message, details=details)


def _normalize_as_of(value: datetime) -> datetime:
    return _shared_normalize_as_of(
        value,
        error_factory=_error,
        reason_code="operator_control.invalid_as_of",
    )


def _require_text(value: object, *, field_name: str) -> str:
    return _shared_require_text(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="operator_control.invalid_row",
        include_value_type=False,
    )


def _require_datetime(value: object, *, field_name: str) -> datetime:
    return _shared_require_datetime(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="operator_control.invalid_row",
        include_value_type=False,
    )


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    return _shared_require_mapping(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="operator_control.invalid_row",
        include_value_type=False,
        parse_json_strings=True,
    )


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


PENDING_REVIEW_SCOPE_CLAMP_TOKEN = "pending_review"
"""Marker string in scope_clamp.applies_to identifying a row that has not yet
been reviewed by the operator. Migration 264 backfills existing rows with this
token so the Moon Decisions panel can surface them for review."""


def _default_pending_review_scope_clamp() -> Mapping[str, Any]:
    """Return the placeholder clamp used for unreviewed rows.

    Returned as an immutable MappingProxyType so the dataclass default cannot
    be mutated in-place by accident.
    """

    return MappingProxyType(
        {
            "applies_to": (PENDING_REVIEW_SCOPE_CLAMP_TOKEN,),
            "does_not_apply_to": (),
        }
    )


def _normalize_scope_clamp(value: object, *, field_name: str) -> Mapping[str, Any]:
    """Validate and normalize a scope_clamp payload.

    Shape: {"applies_to": [string, ...], "does_not_apply_to": [string, ...]}.
    Empty arrays are allowed; non-string elements and unknown keys are rejected.
    """

    mapping = _shared_require_mapping(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="operator_control.invalid_scope_clamp",
        include_value_type=False,
        parse_json_strings=True,
    )
    allowed_keys = {"applies_to", "does_not_apply_to"}
    extra_keys = sorted(set(mapping.keys()) - allowed_keys)
    if extra_keys:
        raise _error(
            "operator_control.invalid_scope_clamp",
            f"{field_name} contains unsupported keys: {', '.join(extra_keys)}",
            details={"field": field_name, "extra_keys": ",".join(extra_keys)},
        )

    def _coerce_list(key: str) -> tuple[str, ...]:
        raw = mapping.get(key, ())
        if not isinstance(raw, (list, tuple)):
            raise _error(
                "operator_control.invalid_scope_clamp",
                f"{field_name}.{key} must be a list of strings",
                details={"field": f"{field_name}.{key}"},
            )
        normalized: list[str] = []
        for index, item in enumerate(raw):
            if not isinstance(item, str):
                raise _error(
                    "operator_control.invalid_scope_clamp",
                    f"{field_name}.{key}[{index}] must be a string",
                    details={"field": f"{field_name}.{key}", "index": str(index)},
                )
            stripped = item.strip()
            if not stripped:
                raise _error(
                    "operator_control.invalid_scope_clamp",
                    f"{field_name}.{key}[{index}] must not be blank",
                    details={"field": f"{field_name}.{key}", "index": str(index)},
                )
            normalized.append(stripped)
        return tuple(normalized)

    return MappingProxyType(
        {
            "applies_to": _coerce_list("applies_to"),
            "does_not_apply_to": _coerce_list("does_not_apply_to"),
        }
    )


def is_pending_review_scope_clamp(scope_clamp: Mapping[str, Any]) -> bool:
    """True when scope_clamp.applies_to contains the pending_review marker."""

    applies_to = scope_clamp.get("applies_to") if scope_clamp else None
    if not applies_to:
        return False
    return PENDING_REVIEW_SCOPE_CLAMP_TOKEN in tuple(applies_to)


@dataclass(frozen=True, slots=True)
class OperatorDecisionAuthorityRecord:
    """Canonical operator decision row."""

    operator_decision_id: str
    decision_key: str
    decision_kind: str
    decision_status: str
    title: str
    rationale: str
    decided_by: str
    decision_source: str
    effective_from: datetime
    effective_to: datetime | None
    decided_at: datetime
    created_at: datetime
    updated_at: datetime
    decision_scope_kind: str | None = None
    decision_scope_ref: str | None = None
    scope_clamp: Mapping[str, Any] = field(
        default_factory=_default_pending_review_scope_clamp,
    )


@dataclass(frozen=True, slots=True)
class OperatorDecisionScopePolicy:
    """Explicit scope contract for one decision kind."""

    scope_mode: str
    allowed_scope_kinds: tuple[str, ...] = ()
    infer_from_decision_key: bool = False
    infer_from_target: bool = False


_CUTOVER_TARGET_SCOPE_KINDS = (
    "roadmap_item",
    "workflow_class",
    "schedule_definition",
)

_OPERATOR_DECISION_SCOPE_POLICIES: dict[str, OperatorDecisionScopePolicy] = {
    "binding": OperatorDecisionScopePolicy(scope_mode="none"),
    "query": OperatorDecisionScopePolicy(scope_mode="none"),
    "operator_graph": OperatorDecisionScopePolicy(scope_mode="none"),
    "architecture_policy": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("authority_domain",),
    ),
    "circuit_breaker_force_open": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("provider",),
        infer_from_decision_key=True,
    ),
    "circuit_breaker_force_closed": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("provider",),
        infer_from_decision_key=True,
    ),
    "circuit_breaker_reset": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("provider",),
        infer_from_decision_key=True,
    ),
    "native_primary_cutover": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=_CUTOVER_TARGET_SCOPE_KINDS,
        infer_from_target=True,
    ),
    "cutover_gate": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=_CUTOVER_TARGET_SCOPE_KINDS,
        infer_from_target=True,
    ),
    "dataset_promotion": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("dataset_specialist",),
    ),
    "dataset_rejection": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("dataset_candidate",),
    ),
    "dataset_promotion_supersede": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("dataset_promotion",),
    ),
    "delivery_plan": OperatorDecisionScopePolicy(
        scope_mode="required",
        allowed_scope_kinds=("authority_domain",),
    ),
}


def operator_decision_scope_policy(*, decision_kind: str) -> OperatorDecisionScopePolicy:
    normalized_decision_kind = _require_text(
        decision_kind,
        field_name="decision_kind",
    )
    try:
        return _OPERATOR_DECISION_SCOPE_POLICIES[normalized_decision_kind]
    except KeyError as exc:
        raise OperatorControlRepositoryError(
            "operator_control.unknown_decision_kind",
            f"operator decision kind {normalized_decision_kind!r} has no registered scope policy",
            details={"decision_kind": normalized_decision_kind},
        ) from exc


def _infer_scope_from_decision_key(
    *,
    decision_kind: str,
    decision_key: str,
) -> tuple[str | None, str | None]:
    if decision_kind not in {
        "circuit_breaker_force_open",
        "circuit_breaker_force_closed",
        "circuit_breaker_reset",
    }:
        return None, None
    prefix = "circuit-breaker::"
    if not decision_key.startswith(prefix):
        return None, None
    suffix = decision_key[len(prefix):]
    provider_slug = suffix.split("::", 1)[0].strip().lower()
    if not provider_slug:
        return None, None
    return "provider", provider_slug


def normalize_operator_decision_record(
    operator_decision: OperatorDecisionAuthorityRecord,
    *,
    fallback_scope_kind: str | None = None,
    fallback_scope_ref: str | None = None,
) -> OperatorDecisionAuthorityRecord:
    """Normalize one operator decision against the registered scope policy."""

    normalized_scope_clamp = _normalize_scope_clamp(
        operator_decision.scope_clamp,
        field_name="operator_decision.scope_clamp",
    )
    operator_decision = replace(
        operator_decision,
        scope_clamp=normalized_scope_clamp,
    )

    normalized_decision_kind = _require_text(
        operator_decision.decision_kind,
        field_name="operator_decision.decision_kind",
    )
    policy = operator_decision_scope_policy(decision_kind=normalized_decision_kind)
    normalized_scope_kind = _optional_text(
        operator_decision.decision_scope_kind,
        field_name="operator_decision.decision_scope_kind",
    )
    normalized_scope_ref = _optional_text(
        operator_decision.decision_scope_ref,
        field_name="operator_decision.decision_scope_ref",
    )
    if (normalized_scope_kind is None) != (normalized_scope_ref is None):
        raise OperatorControlRepositoryError(
            "operator_control.invalid_row",
            "operator decision scope must provide both decision_scope_kind and decision_scope_ref or neither",
            details={"decision_kind": normalized_decision_kind},
        )

    if normalized_scope_kind is None and policy.infer_from_target:
        inferred_scope_kind = _optional_text(
            fallback_scope_kind,
            field_name="fallback_scope_kind",
        )
        inferred_scope_ref = _optional_text(
            fallback_scope_ref,
            field_name="fallback_scope_ref",
        )
        if inferred_scope_kind is not None and inferred_scope_ref is not None:
            normalized_scope_kind = inferred_scope_kind
            normalized_scope_ref = inferred_scope_ref

    if normalized_scope_kind is None and policy.infer_from_decision_key:
        normalized_scope_kind, normalized_scope_ref = _infer_scope_from_decision_key(
            decision_kind=normalized_decision_kind,
            decision_key=_require_text(
                operator_decision.decision_key,
                field_name="operator_decision.decision_key",
            ),
        )

    if policy.scope_mode == "none":
        if normalized_scope_kind is not None or normalized_scope_ref is not None:
            raise OperatorControlRepositoryError(
                "operator_control.invalid_scope",
                f"operator decision kind {normalized_decision_kind!r} must not carry typed scope",
                details={
                    "decision_kind": normalized_decision_kind,
                    "decision_scope_kind": normalized_scope_kind or "",
                    "decision_scope_ref": normalized_scope_ref or "",
                },
            )
        return replace(
            operator_decision,
            decision_kind=normalized_decision_kind,
            decision_scope_kind=None,
            decision_scope_ref=None,
        )

    if normalized_scope_kind is None or normalized_scope_ref is None:
        raise OperatorControlRepositoryError(
            "operator_control.scope_required",
            f"operator decision kind {normalized_decision_kind!r} requires typed scope",
            details={"decision_kind": normalized_decision_kind},
        )

    if (
        policy.allowed_scope_kinds
        and normalized_scope_kind not in policy.allowed_scope_kinds
    ):
        raise OperatorControlRepositoryError(
            "operator_control.invalid_scope",
            (
                f"operator decision kind {normalized_decision_kind!r} requires one of "
                f"{', '.join(policy.allowed_scope_kinds)}"
            ),
            details={
                "decision_kind": normalized_decision_kind,
                "decision_scope_kind": normalized_scope_kind,
                "allowed_scope_kinds": ",".join(policy.allowed_scope_kinds),
            },
        )

    return replace(
        operator_decision,
        decision_kind=normalized_decision_kind,
        decision_scope_kind=normalized_scope_kind,
        decision_scope_ref=normalized_scope_ref,
    )


@dataclass(frozen=True, slots=True)
class CutoverGateAuthorityRecord:
    """Canonical cutover gate row."""

    cutover_gate_id: str
    gate_key: str
    gate_name: str
    gate_kind: str
    gate_status: str
    target_kind: str
    target_ref: str
    gate_policy: Mapping[str, Any]
    required_evidence: Mapping[str, Any]
    opened_by_decision_id: str
    closed_by_decision_id: str | None
    opened_at: datetime
    closed_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OperatorDecisionResolution:
    """Resolved operator decision row for one decision-key lookup."""

    operator_decision: OperatorDecisionAuthorityRecord
    as_of: datetime

    @property
    def operator_decision_id(self) -> str:
        return self.operator_decision.operator_decision_id

    @property
    def decision_key(self) -> str:
        return self.operator_decision.decision_key

    @property
    def decision_kind(self) -> str:
        return self.operator_decision.decision_kind

    @property
    def decision_status(self) -> str:
        return self.operator_decision.decision_status

    @property
    def decided_by(self) -> str:
        return self.operator_decision.decided_by

    @property
    def decision_source(self) -> str:
        return self.operator_decision.decision_source

    @property
    def rationale(self) -> str:
        return self.operator_decision.rationale

    @property
    def decision_scope_kind(self) -> str | None:
        return self.operator_decision.decision_scope_kind

    @property
    def decision_scope_ref(self) -> str | None:
        return self.operator_decision.decision_scope_ref

    @property
    def scope_clamp(self) -> Mapping[str, Any]:
        return self.operator_decision.scope_clamp


@dataclass(frozen=True, slots=True)
class CutoverGateResolution:
    """Resolved cutover gate row for one gate-key lookup."""

    cutover_gate: CutoverGateAuthorityRecord
    as_of: datetime

    @property
    def cutover_gate_id(self) -> str:
        return self.cutover_gate.cutover_gate_id

    @property
    def gate_key(self) -> str:
        return self.cutover_gate.gate_key

    @property
    def gate_name(self) -> str:
        return self.cutover_gate.gate_name

    @property
    def gate_kind(self) -> str:
        return self.cutover_gate.gate_kind

    @property
    def gate_status(self) -> str:
        return self.cutover_gate.gate_status

    @property
    def target_kind(self) -> str:
        return self.cutover_gate.target_kind

    @property
    def target_ref(self) -> str:
        return self.cutover_gate.target_ref

    @property
    def gate_policy(self) -> Mapping[str, Any]:
        return self.cutover_gate.gate_policy

    @property
    def required_evidence(self) -> Mapping[str, Any]:
        return self.cutover_gate.required_evidence


@dataclass(frozen=True, slots=True)
class OperatorControlAuthority:
    """Canonical operator-control snapshot loaded from Postgres rows."""

    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...]
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...]
    as_of: datetime

    @property
    def decision_keys(self) -> tuple[str, ...]:
        return tuple(record.decision_key for record in self.operator_decisions)

    @property
    def gate_keys(self) -> tuple[str, ...]:
        return tuple(record.gate_key for record in self.cutover_gates)

    def resolve_decision(self, *, decision_key: str) -> OperatorDecisionResolution:
        normalized_decision_key = _require_text(
            decision_key,
            field_name="decision_key",
        )
        matching_decisions = [
            record
            for record in self.operator_decisions
            if record.decision_key == normalized_decision_key
        ]
        if not matching_decisions:
            raise OperatorControlRepositoryError(
                "operator_control.decision_missing",
                (
                    "missing authoritative operator decision for "
                    f"decision_key={normalized_decision_key!r}"
                ),
                details={"decision_key": normalized_decision_key},
            )
        if len(matching_decisions) > 1:
            raise OperatorControlRepositoryError(
                "operator_control.decision_ambiguous",
                (
                    "ambiguous authoritative operator decisions for "
                    f"decision_key={normalized_decision_key!r}"
                ),
                details={
                    "decision_key": normalized_decision_key,
                    "operator_decision_ids": ",".join(
                        record.operator_decision_id for record in matching_decisions
                    ),
                },
            )
        return OperatorDecisionResolution(
            operator_decision=matching_decisions[0],
            as_of=self.as_of,
        )

    def resolve_gate(self, *, gate_key: str) -> CutoverGateResolution:
        normalized_gate_key = _require_text(gate_key, field_name="gate_key")
        matching_gates = [
            record
            for record in self.cutover_gates
            if record.gate_key == normalized_gate_key
        ]
        if not matching_gates:
            raise OperatorControlRepositoryError(
                "operator_control.gate_missing",
                (
                    "missing authoritative cutover gate for "
                    f"gate_key={normalized_gate_key!r}"
                ),
                details={"gate_key": normalized_gate_key},
            )
        if len(matching_gates) > 1:
            raise OperatorControlRepositoryError(
                "operator_control.gate_ambiguous",
                (
                    "ambiguous authoritative cutover gates for "
                    f"gate_key={normalized_gate_key!r}"
                ),
                details={
                    "gate_key": normalized_gate_key,
                    "cutover_gate_ids": ",".join(
                        record.cutover_gate_id for record in matching_gates
                    ),
                },
            )
        return CutoverGateResolution(
            cutover_gate=matching_gates[0],
            as_of=self.as_of,
        )

    @classmethod
    def from_records(
        cls,
        *,
        operator_decision_records: Sequence[OperatorDecisionAuthorityRecord],
        cutover_gate_records: Sequence[CutoverGateAuthorityRecord],
        as_of: datetime,
    ) -> "OperatorControlAuthority":
        normalized_as_of = _normalize_as_of(as_of)
        ordered_decisions = tuple(
            sorted(
                operator_decision_records,
                key=lambda record: (
                    record.decision_key,
                    record.effective_from,
                    record.decided_at,
                    record.created_at,
                    record.operator_decision_id,
                ),
            )
        )
        ordered_gates = tuple(
            sorted(
                cutover_gate_records,
                key=lambda record: (
                    record.gate_key,
                    record.opened_at,
                    record.created_at,
                    record.cutover_gate_id,
                ),
            )
        )
        _validate_unique_records(
            ordered_decisions,
            key_name="decision_key",
            record_name="operator_decision",
            error_prefix="operator_control",
            as_of=normalized_as_of,
        )
        _validate_unique_records(
            ordered_gates,
            key_name="gate_key",
            record_name="cutover_gate",
            error_prefix="operator_control",
            as_of=normalized_as_of,
        )
        return cls(
            operator_decisions=ordered_decisions,
            cutover_gates=ordered_gates,
            as_of=normalized_as_of,
        )


def _validate_unique_records(
    records: Sequence[object],
    *,
    key_name: str,
    record_name: str,
    error_prefix: str,
    as_of: datetime,
) -> None:
    grouped: dict[str, list[object]] = {}
    for record in records:
        grouped.setdefault(_require_text(getattr(record, key_name), field_name=key_name), []).append(record)
    duplicates = {
        key: tuple(
            getattr(record, f"{record_name}_id")
            for record in items
        )
        for key, items in grouped.items()
        if len(items) > 1
    }
    if duplicates:
        key, ids = next(iter(duplicates.items()))
        reason_suffix = "decision" if key_name == "decision_key" else "gate"
        raise OperatorControlRepositoryError(
            f"{error_prefix}.{reason_suffix}_ambiguous",
            f"ambiguous active {record_name} rows for {key_name}={key!r}",
            details={
                "as_of": as_of.isoformat(),
                key_name: key,
                f"{record_name}_ids": ",".join(ids),
            },
        )


async def load_operator_control_authority(
    conn,
    *,
    as_of: datetime,
) -> OperatorControlAuthority:
    """Load the canonical operator-control snapshot using the Postgres repository."""

    from storage.postgres.operator_control_repository import PostgresOperatorControlRepository

    repository = PostgresOperatorControlRepository(conn)
    return await repository.load_operator_control_authority(as_of=as_of)


__all__ = [
    "CutoverGateAuthorityRecord",
    "CutoverGateResolution",
    "OperatorControlAuthority",
    "OperatorControlRepositoryError",
    "OperatorDecisionAuthorityRecord",
    "OperatorDecisionScopePolicy",
    "OperatorDecisionResolution",
    "load_operator_control_authority",
    "normalize_operator_decision_record",
    "operator_decision_scope_policy",
]
