"""Append-only workflow_event and receipt evidence writer.

This module owns the boring evidence path:

- typed workflow_event and receipt envelopes
- shared evidence_seq ordering
- request and causality lineage
- explicit route identity and transition_seq proof data
- atomic pair commits
- explicit failure behavior when evidence append cannot complete

The implementation stays in-memory for contract tests. Postgres-backed
storage owns the durable runtime path without changing the evidence shape.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from runtime.domain import (
    AtomicEvidenceWriter,
    DataQualityIssue,
    EvidenceCommitResult,
    LifecycleTransition,
    RouteIdentity,
    RunState,
)

V1_SCHEMA_VERSION = 1
_RESERVED_LINEAGE_KEYS = frozenset(
    {
        "route_identity",
        "event_id",
        "receipt_id",
        "evidence_seq",
        "transition_seq",
        "causation_id",
    }
)
_MISSING_LINEAGE_VALUE = object()


class EvidenceAppendError(RuntimeError):
    """Raised when a workflow_event or receipt append cannot complete safely."""

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


class _FrozenMapping(Mapping[str, Any]):
    """Read-only mapping that stays compatible with dataclasses.asdict()."""

    __slots__ = ("_data",)

    def __init__(self, value: Mapping[str, Any] | None = None) -> None:
        self._data = {key: _freeze_value(item) for key, item in dict(value or {}).items()}

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"_FrozenMapping({self._data!r})"

    def __deepcopy__(self, memo: dict[int, Any]) -> dict[str, Any]:
        return deepcopy(self._data, memo)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self._data)


def _freeze_value(value: Any) -> Any:
    if isinstance(value, _FrozenMapping):
        return value
    if isinstance(value, Mapping):
        return _FrozenMapping(value)
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_value(item) for item in value)
    return value


def _freeze_mapping(value: Mapping[str, Any]) -> _FrozenMapping:
    return _FrozenMapping(value)


def _require_text(value: str | None, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value


def _require_optional_text(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_positive_int(value: int | None, *, field_name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            f"{field_name} must be a positive integer",
            details={"field": field_name},
        )
    return value


def _require_utc(value: datetime | None, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise EvidenceAppendError(
            "evidence.invalid_time",
            f"{field_name} must be UTC-backed",
            details={"field": field_name},
        )
    return value


def _normalize_route_identity(route_identity: RouteIdentity) -> RouteIdentity:
    if not isinstance(route_identity, RouteIdentity):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "route_identity must be a RouteIdentity",
            details={"field": "route_identity"},
        )
    _require_text(route_identity.workflow_id, field_name="route_identity.workflow_id")
    _require_text(route_identity.run_id, field_name="route_identity.run_id")
    _require_text(route_identity.request_id, field_name="route_identity.request_id")
    _require_text(
        route_identity.authority_context_ref,
        field_name="route_identity.authority_context_ref",
    )
    _require_text(
        route_identity.authority_context_digest,
        field_name="route_identity.authority_context_digest",
    )
    _require_positive_int(route_identity.attempt_no, field_name="route_identity.attempt_no")
    _require_positive_int(route_identity.transition_seq, field_name="route_identity.transition_seq")
    _require_text(route_identity.claim_id, field_name="route_identity.claim_id")
    return route_identity


def _route_identity_snapshot(route_identity: RouteIdentity) -> dict[str, Any]:
    return {
        "workflow_id": route_identity.workflow_id,
        "run_id": route_identity.run_id,
        "request_id": route_identity.request_id,
        "authority_context_ref": route_identity.authority_context_ref,
        "authority_context_digest": route_identity.authority_context_digest,
        "claim_id": route_identity.claim_id,
        "lease_id": route_identity.lease_id,
        "proposal_id": route_identity.proposal_id,
        "promotion_decision_id": route_identity.promotion_decision_id,
        "attempt_no": route_identity.attempt_no,
        "transition_seq": route_identity.transition_seq,
    }


def _lineage_value(value: Any) -> Any:
    if isinstance(value, _FrozenMapping):
        return {key: _lineage_value(item) for key, item in value.items()}
    if isinstance(value, Mapping):
        return {str(key): _lineage_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return tuple(_lineage_value(item) for item in value)
    return value


def _format_lineage_path(path: Sequence[str]) -> str:
    formatted: list[str] = []
    for part in path:
        if part.isdigit() and formatted:
            formatted[-1] = f"{formatted[-1]}[{part}]"
            continue
        formatted.append(part)
    return ".".join(formatted)


def _iter_reserved_lineage(
    value: Any,
    *,
    path: tuple[str, ...],
):
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            current_path = (*path, key_text)
            if key_text in _RESERVED_LINEAGE_KEYS:
                yield current_path, key_text, item
                continue
            yield from _iter_reserved_lineage(item, path=current_path)
        return
    if isinstance(value, list | tuple):
        for index, item in enumerate(value):
            yield from _iter_reserved_lineage(item, path=(*path, str(index)))


def _validate_reserved_lineage(
    *,
    field_name: str,
    value: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    normalized_expected = {
        key: _lineage_value(item)
        for key, item in expected.items()
    }
    for path, key, received in _iter_reserved_lineage(value, path=(field_name,)):
        expected_value = normalized_expected.get(key, _MISSING_LINEAGE_VALUE)
        if expected_value is _MISSING_LINEAGE_VALUE:
            continue
        normalized_received = _lineage_value(received)
        if normalized_received == expected_value:
            continue
        raise EvidenceAppendError(
            "evidence.lineage_mismatch",
            f"{field_name} reserved lineage must match the authoritative envelope",
            details={
                "field": field_name,
                "path": _format_lineage_path(path),
                "lineage_key": key,
                "expected": expected_value,
                "received": normalized_received,
            },
        )


def _decision_ref_from_value(value: Any) -> DecisionRef:
    if isinstance(value, DecisionRef):
        return value
    if isinstance(value, Mapping):
        required = {"decision_type", "decision_id", "reason_code", "source_table"}
        missing = required - set(value)
        if missing:
            raise EvidenceAppendError(
                "evidence.invalid_shape",
                "decision_refs entry is missing required fields",
                details={"missing": sorted(missing)},
            )
        return DecisionRef(
            decision_type=_require_text(value.get("decision_type"), field_name="decision_type"),
            decision_id=_require_text(value.get("decision_id"), field_name="decision_id"),
            reason_code=_require_text(value.get("reason_code"), field_name="reason_code"),
            source_table=_require_text(value.get("source_table"), field_name="source_table"),
        )
    raise EvidenceAppendError(
        "evidence.invalid_shape",
        "decision_refs entries must be typed decision references",
        details={"entry_type": type(value).__name__},
    )


def _artifact_ref_from_value(value: Any) -> ArtifactRef:
    if isinstance(value, ArtifactRef):
        return value
    if isinstance(value, Mapping):
        required = {"artifact_id", "artifact_type", "content_hash", "storage_ref"}
        missing = required - set(value)
        if missing:
            raise EvidenceAppendError(
                "evidence.invalid_shape",
                "artifacts entry is missing required fields",
                details={"missing": sorted(missing)},
            )
        return ArtifactRef(
            artifact_id=_require_text(value.get("artifact_id"), field_name="artifact_id"),
            artifact_type=_require_text(value.get("artifact_type"), field_name="artifact_type"),
            content_hash=_require_text(value.get("content_hash"), field_name="content_hash"),
            storage_ref=_require_text(value.get("storage_ref"), field_name="storage_ref"),
        )
    raise EvidenceAppendError(
        "evidence.invalid_shape",
        "artifacts entries must be typed artifact references",
        details={"entry_type": type(value).__name__},
    )


def _coerce_decision_refs(values: Sequence[Any]) -> tuple[DecisionRef, ...]:
    return tuple(_decision_ref_from_value(value) for value in values)


def _coerce_artifacts(values: Sequence[Any]) -> tuple[ArtifactRef, ...]:
    return tuple(_artifact_ref_from_value(value) for value in values)


def _normalize_event(event: WorkflowEventV1) -> WorkflowEventV1:
    if not isinstance(event, WorkflowEventV1):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "workflow_event must be a WorkflowEventV1",
            details={"field": "workflow_event"},
        )
    route_identity = _normalize_route_identity(event.route_identity)
    _require_text(event.event_id, field_name="event_id")
    _require_text(event.event_type, field_name="event_type")
    _require_text(event.workflow_id, field_name="workflow_id")
    _require_text(event.run_id, field_name="run_id")
    _require_text(event.request_id, field_name="request_id")
    _require_positive_int(event.schema_version, field_name="schema_version")
    _require_positive_int(event.evidence_seq, field_name="evidence_seq")
    _require_positive_int(event.transition_seq, field_name="transition_seq")
    _require_utc(event.occurred_at, field_name="occurred_at")
    _require_text(event.actor_type, field_name="actor_type")
    _require_text(event.reason_code, field_name="reason_code")
    if not isinstance(event.payload, Mapping):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "workflow_event payload must be a mapping",
            details={"field": "payload"},
        )
    if event.workflow_id != route_identity.workflow_id:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "workflow_event workflow_id must match route_identity.workflow_id",
            details={"field": "workflow_id"},
        )
    if event.run_id != route_identity.run_id:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "workflow_event run_id must match route_identity.run_id",
            details={"field": "run_id"},
        )
    if event.request_id != route_identity.request_id:
        raise EvidenceAppendError(
            "evidence.request_id_mismatch",
            "workflow_event request_id must match route_identity.request_id",
            details={"field": "request_id"},
        )
    if event.transition_seq != route_identity.transition_seq:
        raise EvidenceAppendError(
            "evidence.transition_seq_mismatch",
            "workflow_event transition_seq must match route_identity.transition_seq",
            details={"field": "transition_seq"},
        )
    normalized_payload = _freeze_mapping(event.payload)
    _validate_reserved_lineage(
        field_name="payload",
        value=normalized_payload,
        expected={
            "route_identity": _route_identity_snapshot(route_identity),
            "event_id": event.event_id,
            "evidence_seq": event.evidence_seq,
            "transition_seq": event.transition_seq,
            "causation_id": event.causation_id,
        },
    )
    normalized = replace(
        event,
        route_identity=route_identity,
        payload=normalized_payload,
    )
    return normalized


def _normalize_receipt(receipt: ReceiptV1) -> ReceiptV1:
    if not isinstance(receipt, ReceiptV1):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "receipt must be a ReceiptV1",
            details={"field": "receipt"},
        )
    route_identity = _normalize_route_identity(receipt.route_identity)
    _require_text(receipt.receipt_id, field_name="receipt_id")
    _require_text(receipt.receipt_type, field_name="receipt_type")
    _require_text(receipt.workflow_id, field_name="workflow_id")
    _require_text(receipt.run_id, field_name="run_id")
    _require_text(receipt.request_id, field_name="request_id")
    _require_positive_int(receipt.schema_version, field_name="schema_version")
    _require_positive_int(receipt.evidence_seq, field_name="evidence_seq")
    _require_positive_int(receipt.transition_seq, field_name="transition_seq")
    _require_utc(receipt.started_at, field_name="started_at")
    _require_utc(receipt.finished_at, field_name="finished_at")
    _require_text(receipt.executor_type, field_name="executor_type")
    _require_text(receipt.status, field_name="status")
    if not isinstance(receipt.inputs, Mapping):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "receipt inputs must be a mapping",
            details={"field": "inputs"},
        )
    if not isinstance(receipt.outputs, Mapping):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "receipt outputs must be a mapping",
            details={"field": "outputs"},
        )
    if receipt.started_at > receipt.finished_at:
        raise EvidenceAppendError(
            "evidence.invalid_time",
            "receipt.started_at must not be after receipt.finished_at",
            details={"field": "started_at"},
        )
    if receipt.workflow_id != route_identity.workflow_id:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "receipt workflow_id must match route_identity.workflow_id",
            details={"field": "workflow_id"},
        )
    if receipt.run_id != route_identity.run_id:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "receipt run_id must match route_identity.run_id",
            details={"field": "run_id"},
        )
    if receipt.request_id != route_identity.request_id:
        raise EvidenceAppendError(
            "evidence.request_id_mismatch",
            "receipt request_id must match route_identity.request_id",
            details={"field": "request_id"},
        )
    if receipt.transition_seq != route_identity.transition_seq:
        raise EvidenceAppendError(
            "evidence.transition_seq_mismatch",
            "receipt transition_seq must match route_identity.transition_seq",
            details={"field": "transition_seq"},
        )
    if receipt.attempt_no is not None and receipt.attempt_no != route_identity.attempt_no:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "receipt attempt_no must match route_identity.attempt_no when set",
            details={"field": "attempt_no"},
        )
    normalized_inputs = _freeze_mapping(receipt.inputs)
    normalized_outputs = _freeze_mapping(receipt.outputs)
    route_snapshot = _route_identity_snapshot(route_identity)
    _validate_reserved_lineage(
        field_name="inputs",
        value=normalized_inputs,
        expected={
            "route_identity": route_snapshot,
            "receipt_id": receipt.receipt_id,
            "transition_seq": receipt.transition_seq,
        },
    )
    _validate_reserved_lineage(
        field_name="outputs",
        value=normalized_outputs,
        expected={
            "route_identity": route_snapshot,
            "receipt_id": receipt.receipt_id,
            "evidence_seq": receipt.evidence_seq,
            "transition_seq": receipt.transition_seq,
        },
    )
    normalized = replace(
        receipt,
        route_identity=route_identity,
        inputs=normalized_inputs,
        outputs=normalized_outputs,
        decision_refs=_coerce_decision_refs(receipt.decision_refs),
        artifacts=_coerce_artifacts(receipt.artifacts),
        attempt_no=route_identity.attempt_no if receipt.attempt_no is None else receipt.attempt_no,
    )
    if _requires_failure_code(receipt.status) and not normalized.failure_code:
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "receipt.failure_code is required for blocked, rejected, invalid, expired, cancelled, or failed outcomes",
            details={"field": "failure_code", "status": receipt.status},
        )
    return normalized


def _requires_failure_code(status: str) -> bool:
    return any(
        token in status
        for token in (
            "blocked",
            "failed",
            "rejected",
            "expired",
            "invalid",
            "cancelled",
        )
    )


def _transition_failure_code(transition: LifecycleTransition) -> str | None:
    if _requires_failure_code(transition.to_state.value):
        return transition.reason_code
    return None


def _stable_route_key(route_identity: RouteIdentity) -> tuple[Any, ...]:
    return (
        route_identity.workflow_id,
        route_identity.run_id,
        route_identity.request_id,
        route_identity.claim_id,
        route_identity.authority_context_ref,
        route_identity.authority_context_digest,
        route_identity.attempt_no,
    )


def _snapshot_run_evidence_state(state: _RunEvidenceState) -> dict[str, Any]:
    return {
        "last_route_identity": state.last_route_identity,
        "last_evidence_seq": state.last_evidence_seq,
        "last_transition_seq": state.last_transition_seq,
        "last_row_id": state.last_row_id,
        "workflow_events": list(state.workflow_events),
        "receipts": list(state.receipts),
        "timeline": list(state.timeline),
    }


def _restore_run_evidence_state(state: _RunEvidenceState, snapshot: Mapping[str, Any]) -> None:
    state.last_route_identity = snapshot["last_route_identity"]
    state.last_evidence_seq = snapshot["last_evidence_seq"]
    state.last_transition_seq = snapshot["last_transition_seq"]
    state.last_row_id = snapshot["last_row_id"]
    state.workflow_events = list(snapshot["workflow_events"])
    state.receipts = list(snapshot["receipts"])
    state.timeline = list(snapshot["timeline"])


def _make_event_id(run_id: str, evidence_seq: int) -> str:
    return f"workflow_event:{run_id}:{evidence_seq}"


def _make_receipt_id(run_id: str, evidence_seq: int) -> str:
    return f"receipt:{run_id}:{evidence_seq}"


def _validate_route_identity_lineage(
    *,
    previous: RouteIdentity | None,
    current: RouteIdentity,
) -> None:
    if previous is None:
        return

    stable_fields = (
        "workflow_id",
        "run_id",
        "request_id",
        "authority_context_ref",
        "authority_context_digest",
        "claim_id",
        "attempt_no",
    )
    for field_name in stable_fields:
        previous_value = getattr(previous, field_name)
        current_value = getattr(current, field_name)
        if previous_value != current_value:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "route_identity stable lineage changed mid-run",
                details={
                    "field": field_name,
                    "expected": previous_value,
                    "received": current_value,
                    "run_id": current.run_id,
                },
            )

    lineage_fields = ("lease_id", "proposal_id", "promotion_decision_id")
    for field_name in lineage_fields:
        previous_value = getattr(previous, field_name)
        current_value = getattr(current, field_name)
        if previous_value is None:
            _require_optional_text(
                current_value,
                field_name=f"route_identity.{field_name}",
            )
            continue
        if current_value != previous_value:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "route_identity stable lineage changed mid-run",
                details={
                    "field": field_name,
                    "expected": previous_value,
                    "received": current_value,
                    "run_id": current.run_id,
                },
            )


@dataclass(frozen=True, slots=True)
class DecisionRef:
    """Typed decision reference object."""

    decision_type: str
    decision_id: str
    reason_code: str
    source_table: str


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Typed artifact reference object."""

    artifact_id: str
    artifact_type: str
    content_hash: str
    storage_ref: str


@dataclass(frozen=True, slots=True)
class WorkflowEventV1:
    """Canonical workflow_event envelope."""

    event_id: str
    event_type: str
    schema_version: int
    workflow_id: str
    run_id: str
    request_id: str
    route_identity: RouteIdentity
    transition_seq: int
    evidence_seq: int
    occurred_at: datetime
    actor_type: str
    reason_code: str
    payload: Mapping[str, Any]
    causation_id: str | None = None
    node_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReceiptV1:
    """Canonical receipt envelope."""

    receipt_id: str
    receipt_type: str
    schema_version: int
    workflow_id: str
    run_id: str
    request_id: str
    route_identity: RouteIdentity
    transition_seq: int
    evidence_seq: int
    started_at: datetime
    finished_at: datetime
    executor_type: str
    status: str
    inputs: Mapping[str, Any]
    outputs: Mapping[str, Any]
    artifacts: tuple[ArtifactRef, ...] = ()
    decision_refs: tuple[DecisionRef, ...] = ()
    causation_id: str | None = None
    node_id: str | None = None
    attempt_no: int | None = None
    supersedes_receipt_id: str | None = None
    failure_code: str | None = None


@dataclass(frozen=True, slots=True)
class TransitionProofV1:
    """Typed event/receipt proof for one authoritative transition."""

    route_identity: RouteIdentity
    transition_seq: int
    event: WorkflowEventV1
    receipt: ReceiptV1


@dataclass(frozen=True, slots=True)
class EvidenceRow:
    """Mixed evidence timeline row for replay and inspection."""

    kind: Literal["workflow_event", "receipt"]
    evidence_seq: int
    row_id: str
    route_identity: RouteIdentity
    transition_seq: int
    record: WorkflowEventV1 | ReceiptV1
    data_quality_issues: tuple[DataQualityIssue, ...] = ()


@dataclass(slots=True)
class _RunEvidenceState:
    last_route_identity: RouteIdentity | None = None
    last_evidence_seq: int = 0
    last_transition_seq: int = 0
    last_row_id: str | None = None
    workflow_events: list[WorkflowEventV1] = field(default_factory=list)
    receipts: list[ReceiptV1] = field(default_factory=list)
    timeline: list[EvidenceRow] = field(default_factory=list)


class AppendOnlyWorkflowEvidenceWriter(AtomicEvidenceWriter):
    """Append-only workflow_event and receipt writer.

    The writer holds a tiny in-memory journal for contract tests. It does not
    stage any external side effects and never acts as a bridge to a second
    authority.
    """

    def __init__(self) -> None:
        self._runs: dict[str, _RunEvidenceState] = {}

    def commit_submission(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
    ) -> EvidenceCommitResult:
        """Persist the initial claim_received evidence bundle."""

        normalized_route_identity = _normalize_route_identity(route_identity)
        if not isinstance(request_payload, Mapping):
            raise EvidenceAppendError(
                "evidence.invalid_shape",
                "request_payload must be a mapping",
                details={"field": "request_payload"},
            )
        frozen_claim_envelope = _freeze_mapping(request_payload)
        submission_seq = self._next_transition_seq(normalized_route_identity.run_id)
        evidence_seq = self._next_evidence_seq(normalized_route_identity.run_id)
        occurred_at = datetime.now(timezone.utc)
        proof = TransitionProofV1(
            route_identity=normalized_route_identity,
            transition_seq=submission_seq,
            event=WorkflowEventV1(
                event_id=_make_event_id(normalized_route_identity.run_id, evidence_seq),
                event_type="claim_received",
                schema_version=V1_SCHEMA_VERSION,
                workflow_id=normalized_route_identity.workflow_id,
                run_id=normalized_route_identity.run_id,
                request_id=normalized_route_identity.request_id,
                route_identity=normalized_route_identity,
                transition_seq=submission_seq,
                evidence_seq=evidence_seq,
                occurred_at=occurred_at,
                actor_type="runtime",
                reason_code="claim.received",
                payload=_freeze_mapping(
                    {
                        "claim_envelope": frozen_claim_envelope,
                        "admitted_definition_ref": admitted_definition_ref,
                        "admitted_definition_hash": admitted_definition_hash,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                    }
                ),
                causation_id=None,
                node_id=None,
            ),
            receipt=ReceiptV1(
                receipt_id=_make_receipt_id(normalized_route_identity.run_id, evidence_seq + 1),
                receipt_type="claim_received_receipt",
                schema_version=V1_SCHEMA_VERSION,
                workflow_id=normalized_route_identity.workflow_id,
                run_id=normalized_route_identity.run_id,
                request_id=normalized_route_identity.request_id,
                route_identity=normalized_route_identity,
                transition_seq=submission_seq,
                evidence_seq=evidence_seq + 1,
                started_at=occurred_at,
                finished_at=occurred_at,
                executor_type="runtime.submit",
                status="claim_received",
                inputs=_freeze_mapping(
                    {
                        "claim_envelope": frozen_claim_envelope,
                        "admitted_definition_ref": admitted_definition_ref,
                        "admitted_definition_hash": admitted_definition_hash,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                    }
                ),
                outputs=_freeze_mapping(
                    {
                        "event_id": _make_event_id(normalized_route_identity.run_id, evidence_seq),
                        "receipt_id": _make_receipt_id(normalized_route_identity.run_id, evidence_seq + 1),
                        "run_id": normalized_route_identity.run_id,
                        "request_id": normalized_route_identity.request_id,
                        "claim_id": normalized_route_identity.claim_id,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                        "evidence_seq": evidence_seq + 1,
                        "transition_seq": submission_seq,
                    }
                ),
                artifacts=(),
                decision_refs=(),
                causation_id=_make_event_id(normalized_route_identity.run_id, evidence_seq),
                node_id=None,
                attempt_no=normalized_route_identity.attempt_no,
                supersedes_receipt_id=None,
                failure_code=None,
            ),
        )
        return self.append_transition_proof(proof)

    def commit_transition(
        self,
        *,
        transition: LifecycleTransition,
    ) -> EvidenceCommitResult:
        """Persist one runtime transition and matching evidence."""

        normalized_route_identity = _normalize_route_identity(transition.route_identity)
        expected_transition_seq = self._next_transition_seq(normalized_route_identity.run_id)
        if transition.route_identity.transition_seq != expected_transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_conflict",
                "transition_seq must advance one step at a time",
                details={
                    "run_id": normalized_route_identity.run_id,
                    "expected_transition_seq": expected_transition_seq,
                    "received_transition_seq": transition.route_identity.transition_seq,
                },
            )
        evidence_seq = self._next_evidence_seq(normalized_route_identity.run_id)
        occurred_at = transition.occurred_at
        if occurred_at.tzinfo is None or occurred_at.utcoffset() != timedelta(0):
            raise EvidenceAppendError(
                "evidence.invalid_time",
                "transition.occurred_at must be UTC-backed",
                details={"field": "occurred_at"},
            )
        previous_row_id = self._last_row_id(normalized_route_identity.run_id)
        proof = TransitionProofV1(
            route_identity=normalized_route_identity,
            transition_seq=transition.route_identity.transition_seq,
            event=WorkflowEventV1(
                event_id=_make_event_id(normalized_route_identity.run_id, evidence_seq),
                event_type=transition.event_type,
                schema_version=V1_SCHEMA_VERSION,
                workflow_id=normalized_route_identity.workflow_id,
                run_id=normalized_route_identity.run_id,
                request_id=normalized_route_identity.request_id,
                route_identity=normalized_route_identity,
                transition_seq=transition.route_identity.transition_seq,
                evidence_seq=evidence_seq,
                occurred_at=occurred_at,
                actor_type="runtime",
                reason_code=transition.reason_code,
                payload=_freeze_mapping(
                    {
                        "from_state": transition.from_state.value,
                        "to_state": transition.to_state.value,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                        "transition_seq": transition.route_identity.transition_seq,
                    }
                ),
                causation_id=previous_row_id,
                node_id=None,
            ),
            receipt=ReceiptV1(
                receipt_id=_make_receipt_id(normalized_route_identity.run_id, evidence_seq + 1),
                receipt_type=transition.receipt_type,
                schema_version=V1_SCHEMA_VERSION,
                workflow_id=normalized_route_identity.workflow_id,
                run_id=normalized_route_identity.run_id,
                request_id=normalized_route_identity.request_id,
                route_identity=normalized_route_identity,
                transition_seq=transition.route_identity.transition_seq,
                evidence_seq=evidence_seq + 1,
                started_at=occurred_at,
                finished_at=occurred_at,
                executor_type="runtime.transition",
                status=transition.to_state.value,
                inputs=_freeze_mapping(
                    {
                        "from_state": transition.from_state.value,
                        "to_state": transition.to_state.value,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                        "transition_seq": transition.route_identity.transition_seq,
                    }
                ),
                outputs=_freeze_mapping(
                    {
                        "event_id": _make_event_id(normalized_route_identity.run_id, evidence_seq),
                        "receipt_id": _make_receipt_id(normalized_route_identity.run_id, evidence_seq + 1),
                        "evidence_seq": evidence_seq + 1,
                        "transition_seq": transition.route_identity.transition_seq,
                        "to_state": transition.to_state.value,
                    }
                ),
                artifacts=(),
                decision_refs=(),
                causation_id=_make_event_id(normalized_route_identity.run_id, evidence_seq),
                node_id=None,
                attempt_no=normalized_route_identity.attempt_no,
                supersedes_receipt_id=None,
                failure_code=_transition_failure_code(transition),
            ),
        )
        return self.append_transition_proof(proof)

    def append_workflow_event(self, event: WorkflowEventV1) -> WorkflowEventV1:
        """Append one workflow_event row."""

        normalized = _normalize_event(event)
        state = self._state_for(normalized.route_identity)
        self._validate_route_lineage(state, normalized.route_identity)
        expected_seq = state.last_evidence_seq + 1
        if normalized.evidence_seq != expected_seq:
            raise EvidenceAppendError(
                "evidence_seq.conflict",
                "workflow_event evidence_seq must advance one step at a time",
                details={
                    "run_id": normalized.route_identity.run_id,
                    "expected_evidence_seq": expected_seq,
                    "received_evidence_seq": normalized.evidence_seq,
                },
            )
        if normalized.causation_id not in {None, state.last_row_id}:
            raise EvidenceAppendError(
                "evidence.causation_mismatch",
                "workflow_event causation_id must point at the previous evidence row",
                details={
                    "previous_row_id": state.last_row_id,
                    "event_causation_id": normalized.causation_id,
                },
            )
        if normalized.causation_id is None and state.last_row_id is not None:
            normalized = replace(normalized, causation_id=state.last_row_id)
        snapshot = _snapshot_run_evidence_state(state)
        try:
            self._append_event(state, normalized)
        except EvidenceAppendError:
            _restore_run_evidence_state(state, snapshot)
            raise
        except Exception as exc:  # pragma: no cover - defensive wrap
            _restore_run_evidence_state(state, snapshot)
            raise EvidenceAppendError(
                "evidence.append_failed",
                "workflow_event append failed after validation",
                details={"run_id": normalized.route_identity.run_id},
            ) from exc
        return normalized

    def append_receipt(self, receipt: ReceiptV1) -> ReceiptV1:
        """Append one receipt row."""

        normalized = _normalize_receipt(receipt)
        state = self._state_for(normalized.route_identity)
        self._validate_route_lineage(state, normalized.route_identity)
        expected_seq = state.last_evidence_seq + 1
        if normalized.evidence_seq != expected_seq:
            raise EvidenceAppendError(
                "evidence_seq.conflict",
                "receipt evidence_seq must advance one step at a time",
                details={
                    "run_id": normalized.route_identity.run_id,
                    "expected_evidence_seq": expected_seq,
                    "received_evidence_seq": normalized.evidence_seq,
                },
            )
        if normalized.causation_id not in {None, state.last_row_id}:
            raise EvidenceAppendError(
                "evidence.causation_mismatch",
                "receipt causation_id must point at the previous evidence row",
                details={
                    "previous_row_id": state.last_row_id,
                    "receipt_causation_id": normalized.causation_id,
                },
            )
        if normalized.causation_id is None and state.last_row_id is not None:
            normalized = replace(normalized, causation_id=state.last_row_id)
        snapshot = _snapshot_run_evidence_state(state)
        try:
            self._append_receipt(state, normalized)
        except EvidenceAppendError:
            _restore_run_evidence_state(state, snapshot)
            raise
        except Exception as exc:  # pragma: no cover - defensive wrap
            _restore_run_evidence_state(state, snapshot)
            raise EvidenceAppendError(
                "evidence.append_failed",
                "receipt append failed after validation",
                details={"run_id": normalized.route_identity.run_id},
            ) from exc
        return normalized

    def append_transition_proof(self, proof: TransitionProofV1) -> EvidenceCommitResult:
        """Append the event/receipt pair that proves one transition."""

        normalized_proof = self._normalize_proof(proof)
        state = self._state_for(normalized_proof.route_identity)
        self._validate_route_lineage(state, normalized_proof.route_identity)
        expected_transition_seq = state.last_transition_seq + 1
        if normalized_proof.transition_seq != expected_transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_conflict",
                "transition_seq must advance one step at a time",
                details={
                    "run_id": normalized_proof.route_identity.run_id,
                    "expected_transition_seq": expected_transition_seq,
                    "received_transition_seq": normalized_proof.transition_seq,
                },
            )
        expected_event_seq = state.last_evidence_seq + 1
        if normalized_proof.event.evidence_seq != expected_event_seq:
            raise EvidenceAppendError(
                "evidence_seq.conflict",
                "transition event evidence_seq must advance one step at a time",
                details={
                    "run_id": normalized_proof.route_identity.run_id,
                    "expected_evidence_seq": expected_event_seq,
                    "received_evidence_seq": normalized_proof.event.evidence_seq,
                },
            )
        if normalized_proof.receipt.evidence_seq != normalized_proof.event.evidence_seq + 1:
            raise EvidenceAppendError(
                "evidence_seq.conflict",
                "transition receipt evidence_seq must immediately follow the event",
                details={
                    "event_evidence_seq": normalized_proof.event.evidence_seq,
                    "receipt_evidence_seq": normalized_proof.receipt.evidence_seq,
                },
            )
        if normalized_proof.event.route_identity != normalized_proof.receipt.route_identity:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "transition event and receipt must share route_identity",
            )
        if normalized_proof.event.transition_seq != normalized_proof.receipt.transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_mismatch",
                "transition event and receipt must share transition_seq",
            )
        if normalized_proof.event.transition_seq != normalized_proof.transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_mismatch",
                "transition proof transition_seq must match the envelope transition_seq",
            )
        if normalized_proof.event.workflow_id != normalized_proof.receipt.workflow_id:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "transition event and receipt must share workflow_id",
            )
        if normalized_proof.event.run_id != normalized_proof.receipt.run_id:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "transition event and receipt must share run_id",
            )
        if normalized_proof.event.request_id != normalized_proof.receipt.request_id:
            raise EvidenceAppendError(
                "evidence.request_id_mismatch",
                "transition event and receipt must share request_id",
            )
        if normalized_proof.receipt.causation_id not in {None, normalized_proof.event.event_id}:
            raise EvidenceAppendError(
                "evidence.causation_mismatch",
                "transition receipt causation_id must point at the transition event",
                details={
                    "event_id": normalized_proof.event.event_id,
                    "receipt_causation_id": normalized_proof.receipt.causation_id,
                },
            )
        if normalized_proof.event.causation_id not in {None, state.last_row_id}:
            raise EvidenceAppendError(
                "evidence.causation_mismatch",
                "transition event causation_id must point at the previous evidence row",
                details={
                    "previous_row_id": state.last_row_id,
                    "event_causation_id": normalized_proof.event.causation_id,
                },
            )
        if normalized_proof.event.causation_id is None and state.last_row_id is not None:
            normalized_proof = replace(
                normalized_proof,
                event=replace(normalized_proof.event, causation_id=state.last_row_id),
            )
        if normalized_proof.receipt.causation_id is None:
            normalized_proof = replace(
                normalized_proof,
                receipt=replace(normalized_proof.receipt, causation_id=normalized_proof.event.event_id),
            )
        snapshot = _snapshot_run_evidence_state(state)
        try:
            self._append_event(state, normalized_proof.event)
            self._append_receipt(state, normalized_proof.receipt)
            state.last_transition_seq = normalized_proof.transition_seq
            state.last_row_id = normalized_proof.receipt.receipt_id
            state.last_route_identity = normalized_proof.route_identity
        except EvidenceAppendError:
            _restore_run_evidence_state(state, snapshot)
            raise
        except Exception as exc:  # pragma: no cover - defensive wrap
            _restore_run_evidence_state(state, snapshot)
            raise EvidenceAppendError(
                "evidence.append_failed",
                "transition proof append failed after validation",
                details={"run_id": normalized_proof.route_identity.run_id},
            ) from exc
        return EvidenceCommitResult(
            event_id=normalized_proof.event.event_id,
            receipt_id=normalized_proof.receipt.receipt_id,
            evidence_seq=normalized_proof.receipt.evidence_seq,
            committed_at=normalized_proof.receipt.finished_at,
        )

    def workflow_events(self, run_id: str) -> tuple[WorkflowEventV1, ...]:
        state = self._runs.get(run_id)
        if state is None:
            return ()
        return tuple(state.workflow_events)

    def receipts(self, run_id: str) -> tuple[ReceiptV1, ...]:
        state = self._runs.get(run_id)
        if state is None:
            return ()
        return tuple(state.receipts)

    def evidence_timeline(self, run_id: str) -> tuple[EvidenceRow, ...]:
        state = self._runs.get(run_id)
        if state is None:
            return ()
        return tuple(sorted(state.timeline, key=lambda row: row.evidence_seq))

    def last_evidence_seq(self, run_id: str) -> int | None:
        state = self._runs.get(run_id)
        if state is None or state.last_evidence_seq == 0:
            return None
        return state.last_evidence_seq

    def _state_for(self, route_identity: RouteIdentity) -> _RunEvidenceState:
        route_identity = _normalize_route_identity(route_identity)
        state = self._runs.get(route_identity.run_id)
        if state is None:
            state = _RunEvidenceState(last_route_identity=route_identity)
            self._runs[route_identity.run_id] = state
            return state
        return state

    def _validate_route_lineage(self, state: _RunEvidenceState, route_identity: RouteIdentity) -> None:
        _validate_route_identity_lineage(
            previous=state.last_route_identity,
            current=route_identity,
        )

    def _next_evidence_seq(self, run_id: str) -> int:
        state = self._runs.get(run_id)
        if state is None:
            return 1
        return state.last_evidence_seq + 1

    def _next_transition_seq(self, run_id: str) -> int:
        state = self._runs.get(run_id)
        if state is None:
            return 1
        return state.last_transition_seq + 1

    def _last_row_id(self, run_id: str) -> str | None:
        state = self._runs.get(run_id)
        if state is None:
            return None
        return state.last_row_id

    def _normalize_proof(self, proof: TransitionProofV1) -> TransitionProofV1:
        if not isinstance(proof, TransitionProofV1):
            raise EvidenceAppendError(
                "evidence.invalid_shape",
                "transition proof must be a TransitionProofV1",
            )
        route_identity = _normalize_route_identity(proof.route_identity)
        event = _normalize_event(proof.event)
        receipt = _normalize_receipt(proof.receipt)
        if event.route_identity != route_identity or receipt.route_identity != route_identity:
            raise EvidenceAppendError(
                "evidence.route_identity_mismatch",
                "transition proof route_identity must match both envelopes",
            )
        if proof.transition_seq != route_identity.transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_mismatch",
                "transition proof transition_seq must match route_identity.transition_seq",
            )
        if event.transition_seq != receipt.transition_seq:
            raise EvidenceAppendError(
                "evidence.transition_seq_mismatch",
                "transition event and receipt must agree on transition_seq",
            )
        route_snapshot = _route_identity_snapshot(route_identity)
        _validate_reserved_lineage(
            field_name="event.payload",
            value=event.payload,
            expected={
                "route_identity": route_snapshot,
                "event_id": event.event_id,
                "receipt_id": receipt.receipt_id,
                "evidence_seq": event.evidence_seq,
                "transition_seq": proof.transition_seq,
                "causation_id": event.causation_id,
            },
        )
        _validate_reserved_lineage(
            field_name="receipt.inputs",
            value=receipt.inputs,
            expected={
                "route_identity": route_snapshot,
                "event_id": event.event_id,
                "receipt_id": receipt.receipt_id,
                "evidence_seq": event.evidence_seq,
                "transition_seq": proof.transition_seq,
                "causation_id": event.causation_id,
            },
        )
        _validate_reserved_lineage(
            field_name="receipt.outputs",
            value=receipt.outputs,
            expected={
                "route_identity": route_snapshot,
                "event_id": event.event_id,
                "receipt_id": receipt.receipt_id,
                "evidence_seq": receipt.evidence_seq,
                "transition_seq": proof.transition_seq,
                "causation_id": event.causation_id,
            },
        )
        return TransitionProofV1(
            route_identity=route_identity,
            transition_seq=proof.transition_seq,
            event=event,
            receipt=receipt,
        )

    def _append_event(self, state: _RunEvidenceState, event: WorkflowEventV1) -> None:
        state.workflow_events.append(event)
        state.timeline.append(
            EvidenceRow(
                kind="workflow_event",
                evidence_seq=event.evidence_seq,
                row_id=event.event_id,
                route_identity=event.route_identity,
                transition_seq=event.transition_seq,
                record=event,
            )
        )
        state.last_evidence_seq = event.evidence_seq
        state.last_row_id = event.event_id
        state.last_route_identity = event.route_identity

    def _append_receipt(self, state: _RunEvidenceState, receipt: ReceiptV1) -> None:
        state.receipts.append(receipt)
        state.timeline.append(
            EvidenceRow(
                kind="receipt",
                evidence_seq=receipt.evidence_seq,
                row_id=receipt.receipt_id,
                route_identity=receipt.route_identity,
                transition_seq=receipt.transition_seq,
                record=receipt,
            )
        )
        state.last_evidence_seq = receipt.evidence_seq
        state.last_row_id = receipt.receipt_id
        state.last_route_identity = receipt.route_identity


WorkflowEvidenceWriter = AppendOnlyWorkflowEvidenceWriter


__all__ = [
    "AppendOnlyWorkflowEvidenceWriter",
    "ArtifactRef",
    "DecisionRef",
    "EvidenceAppendError",
    "EvidenceRow",
    "ReceiptV1",
    "TransitionProofV1",
    "V1_SCHEMA_VERSION",
    "WorkflowEvidenceWriter",
    "WorkflowEventV1",
]
