"""Canonical semantic assertion substrate for cross-domain semantics.

This module owns the typed write-model records and validation rules for
predicate registration and semantic assertion writes. The Postgres repository
is intentionally separate so the record contract stays reusable across command,
query, bridge, and projection paths.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, Protocol

from runtime._helpers import _fail as _shared_fail, _json_compatible

SUPPORTED_PREDICATE_STATUSES = ("active", "inactive")
SUPPORTED_ASSERTION_STATUSES = ("active", "superseded", "retracted")
SUPPORTED_CARDINALITY_MODES = (
    "many",
    "single_active_per_subject",
    "single_active_per_edge",
)
RESERVED_QUALIFIER_KEYS = frozenset(
    {
        "assertion_status",
        "bound_decision_id",
        "evidence_ref",
        "object",
        "object_kind",
        "object_ref",
        "predicate",
        "predicate_slug",
        "semantic_assertion_id",
        "source",
        "source_kind",
        "source_ref",
        "status",
        "subject",
        "subject_kind",
        "subject_ref",
        "valid_from",
        "valid_to",
    }
)


class SemanticAssertionError(RuntimeError):
    """Raised when semantic assertion authority cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=SemanticAssertionError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise _fail(
                "semantic_assertion.invalid_value",
                f"{field_name} must be a datetime",
                details={"field": field_name, "value_type": type(value).__name__},
            ) from exc
    else:
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return parsed.astimezone(timezone.utc)


def _slug_fragment(value: object, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name).lower()
    fragments = [char if char.isalnum() else "-" for char in normalized]
    collapsed = "".join(fragments).strip("-")
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    if not collapsed:
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must contain at least one alphanumeric character",
            details={"field": field_name},
        )
    return collapsed


def _normalize_predicate_slug(value: object, *, field_name: str) -> str:
    return _slug_fragment(value, field_name=field_name).replace("-", "_")


def _normalize_kind_token(value: object, *, field_name: str) -> str:
    return _slug_fragment(value, field_name=field_name).replace("-", "_")


def _require_status(
    value: object,
    *,
    field_name: str,
    allowed_statuses: Sequence[str],
) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in allowed_statuses:
        raise _fail(
            "semantic_assertion.invalid_status",
            f"{field_name} must be one of {', '.join(allowed_statuses)}",
            details={"field": field_name, "value": normalized},
        )
    return normalized


def _require_text_sequence(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a non-empty list of kind tokens",
            details={"field": field_name},
        )
    if isinstance(value, str):
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a list of kind tokens, not a single string",
            details={"field": field_name},
        )
    if not isinstance(value, Sequence):
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must be a list of kind tokens",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized = tuple(
        dict.fromkeys(
            _normalize_kind_token(item, field_name=f"{field_name}[{index}]")
            for index, item in enumerate(value)
        )
    )
    if not normalized:
        raise _fail(
            "semantic_assertion.invalid_value",
            f"{field_name} must contain at least one kind token",
            details={"field": field_name},
        )
    return normalized


def _normalize_qualifiers_json(value: object | None) -> Mapping[str, Any]:
    raw = {} if value is None else _require_mapping(_json_compatible(value), field_name="qualifiers_json")
    conflicting_keys = sorted(
        key for key in raw.keys() if str(key).strip().lower() in RESERVED_QUALIFIER_KEYS
    )
    if conflicting_keys:
        raise _fail(
            "semantic_assertion.hidden_authority",
            "qualifiers_json cannot hide authority fields that belong in explicit columns",
            details={"conflicting_keys": conflicting_keys},
        )
    return {str(key): _json_compatible(item) for key, item in raw.items()}


def semantic_assertion_id(
    *,
    predicate_slug: str,
    subject_kind: str,
    subject_ref: str,
    object_kind: str,
    object_ref: str,
    source_kind: str,
    source_ref: str,
) -> str:
    """Return a stable semantic assertion identity from the canonical edge and source."""

    normalized_predicate = _normalize_predicate_slug(
        predicate_slug,
        field_name="predicate_slug",
    )
    material = "\x1f".join(
        (
            normalized_predicate,
            _normalize_kind_token(subject_kind, field_name="subject_kind"),
            _require_text(subject_ref, field_name="subject_ref"),
            _normalize_kind_token(object_kind, field_name="object_kind"),
            _require_text(object_ref, field_name="object_ref"),
            _normalize_kind_token(source_kind, field_name="source_kind"),
            _require_text(source_ref, field_name="source_ref"),
        )
    )
    digest = hashlib.blake2s(material.encode("utf-8"), digest_size=12).hexdigest()
    return f"semantic_assertion.{normalized_predicate}.{digest}"


@dataclass(frozen=True, slots=True)
class SemanticPredicateRecord:
    """Vocabulary row for one semantic predicate."""

    predicate_slug: str
    predicate_status: str
    subject_kind_allowlist: tuple[str, ...]
    object_kind_allowlist: tuple[str, ...]
    cardinality_mode: str
    description: str
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "predicate_slug": self.predicate_slug,
            "predicate_status": self.predicate_status,
            "subject_kind_allowlist": list(self.subject_kind_allowlist),
            "object_kind_allowlist": list(self.object_kind_allowlist),
            "cardinality_mode": self.cardinality_mode,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class SemanticAssertionRecord:
    """Write-model row for one semantic assertion."""

    semantic_assertion_id: str
    predicate_slug: str
    assertion_status: str
    subject_kind: str
    subject_ref: str
    object_kind: str
    object_ref: str
    qualifiers_json: Mapping[str, Any]
    source_kind: str
    source_ref: str
    evidence_ref: str | None
    bound_decision_id: str | None
    valid_from: datetime
    valid_to: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "semantic_assertion_id": self.semantic_assertion_id,
            "predicate_slug": self.predicate_slug,
            "assertion_status": self.assertion_status,
            "subject": {
                "kind": self.subject_kind,
                "ref": self.subject_ref,
            },
            "object": {
                "kind": self.object_kind,
                "ref": self.object_ref,
            },
            "qualifiers_json": dict(self.qualifiers_json),
            "source": {
                "kind": self.source_kind,
                "ref": self.source_ref,
            },
            "evidence_ref": self.evidence_ref,
            "bound_decision_id": self.bound_decision_id,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": None if self.valid_to is None else self.valid_to.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def normalize_semantic_predicate_record(
    predicate: SemanticPredicateRecord,
) -> SemanticPredicateRecord:
    """Validate and normalize one semantic predicate record."""

    created_at = _require_datetime(predicate.created_at, field_name="created_at")
    updated_at = _require_datetime(predicate.updated_at, field_name="updated_at")
    return SemanticPredicateRecord(
        predicate_slug=_normalize_predicate_slug(
            predicate.predicate_slug,
            field_name="predicate_slug",
        ),
        predicate_status=_require_status(
            predicate.predicate_status,
            field_name="predicate_status",
            allowed_statuses=SUPPORTED_PREDICATE_STATUSES,
        ),
        subject_kind_allowlist=_require_text_sequence(
            predicate.subject_kind_allowlist,
            field_name="subject_kind_allowlist",
        ),
        object_kind_allowlist=_require_text_sequence(
            predicate.object_kind_allowlist,
            field_name="object_kind_allowlist",
        ),
        cardinality_mode=_require_status(
            predicate.cardinality_mode,
            field_name="cardinality_mode",
            allowed_statuses=SUPPORTED_CARDINALITY_MODES,
        ),
        description=_optional_text(predicate.description, field_name="description") or "",
        created_at=created_at,
        updated_at=updated_at,
    )


def normalize_semantic_assertion_record(
    assertion: SemanticAssertionRecord,
) -> SemanticAssertionRecord:
    """Validate and normalize one semantic assertion write-model row."""

    valid_from = _require_datetime(assertion.valid_from, field_name="valid_from")
    valid_to = (
        None
        if assertion.valid_to is None
        else _require_datetime(assertion.valid_to, field_name="valid_to")
    )
    if valid_to is not None and valid_to < valid_from:
        raise _fail(
            "semantic_assertion.invalid_validity_window",
            "valid_to must be later than or equal to valid_from",
            details={
                "valid_from": valid_from.isoformat(),
                "valid_to": valid_to.isoformat(),
            },
        )
    created_at = _require_datetime(assertion.created_at, field_name="created_at")
    updated_at = _require_datetime(assertion.updated_at, field_name="updated_at")
    normalized_predicate = _normalize_predicate_slug(
        assertion.predicate_slug,
        field_name="predicate_slug",
    )
    normalized_subject_kind = _normalize_kind_token(
        assertion.subject_kind,
        field_name="subject_kind",
    )
    normalized_object_kind = _normalize_kind_token(
        assertion.object_kind,
        field_name="object_kind",
    )
    normalized_source_kind = _normalize_kind_token(
        assertion.source_kind,
        field_name="source_kind",
    )
    normalized_subject_ref = _require_text(assertion.subject_ref, field_name="subject_ref")
    normalized_object_ref = _require_text(assertion.object_ref, field_name="object_ref")
    normalized_source_ref = _require_text(assertion.source_ref, field_name="source_ref")
    raw_assertion_id = (
        assertion.semantic_assertion_id
        if isinstance(assertion.semantic_assertion_id, str)
        and assertion.semantic_assertion_id.strip()
        else None
    )
    normalized_assertion_id = (
        _optional_text(raw_assertion_id, field_name="semantic_assertion_id")
        or semantic_assertion_id(
            predicate_slug=normalized_predicate,
            subject_kind=normalized_subject_kind,
            subject_ref=normalized_subject_ref,
            object_kind=normalized_object_kind,
            object_ref=normalized_object_ref,
            source_kind=normalized_source_kind,
            source_ref=normalized_source_ref,
        )
    )
    return SemanticAssertionRecord(
        semantic_assertion_id=normalized_assertion_id,
        predicate_slug=normalized_predicate,
        assertion_status=_require_status(
            assertion.assertion_status,
            field_name="assertion_status",
            allowed_statuses=SUPPORTED_ASSERTION_STATUSES,
        ),
        subject_kind=normalized_subject_kind,
        subject_ref=normalized_subject_ref,
        object_kind=normalized_object_kind,
        object_ref=normalized_object_ref,
        qualifiers_json=_normalize_qualifiers_json(assertion.qualifiers_json),
        source_kind=normalized_source_kind,
        source_ref=normalized_source_ref,
        evidence_ref=_optional_text(assertion.evidence_ref, field_name="evidence_ref"),
        bound_decision_id=_optional_text(
            assertion.bound_decision_id,
            field_name="bound_decision_id",
        ),
        valid_from=valid_from,
        valid_to=valid_to,
        created_at=created_at,
        updated_at=updated_at,
    )


def project_semantic_predicate(row: Mapping[str, Any]) -> SemanticPredicateRecord:
    """Project one payload or row into the canonical semantic predicate record."""

    raw_subject_allowlist = row.get("subject_kind_allowlist") or ()
    raw_object_allowlist = row.get("object_kind_allowlist") or ()
    if isinstance(raw_subject_allowlist, str):
        raw_subject_allowlist = json.loads(raw_subject_allowlist)
    if isinstance(raw_object_allowlist, str):
        raw_object_allowlist = json.loads(raw_object_allowlist)
    return normalize_semantic_predicate_record(
        SemanticPredicateRecord(
            predicate_slug=row.get("predicate_slug", ""),
            predicate_status=row.get("predicate_status", "active"),
            subject_kind_allowlist=tuple(raw_subject_allowlist),
            object_kind_allowlist=tuple(raw_object_allowlist),
            cardinality_mode=row.get("cardinality_mode", "many"),
            description=str(row.get("description") or ""),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )
    )


def project_semantic_assertion(row: Mapping[str, Any]) -> SemanticAssertionRecord:
    """Project one payload or row into the canonical semantic assertion record."""

    subject = row.get("subject")
    object_payload = row.get("object")
    source = row.get("source")
    if isinstance(subject, Mapping) and isinstance(object_payload, Mapping) and isinstance(source, Mapping):
        projected = {
            "semantic_assertion_id": row.get("semantic_assertion_id"),
            "predicate_slug": row.get("predicate_slug"),
            "assertion_status": row.get("assertion_status"),
            "subject_kind": subject.get("kind"),
            "subject_ref": subject.get("ref"),
            "object_kind": object_payload.get("kind"),
            "object_ref": object_payload.get("ref"),
            "qualifiers_json": row.get("qualifiers_json"),
            "source_kind": source.get("kind"),
            "source_ref": source.get("ref"),
            "evidence_ref": row.get("evidence_ref"),
            "bound_decision_id": row.get("bound_decision_id"),
            "valid_from": row.get("valid_from"),
            "valid_to": row.get("valid_to"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }
    else:
        projected = row
    return normalize_semantic_assertion_record(
        SemanticAssertionRecord(
            semantic_assertion_id=projected.get("semantic_assertion_id", ""),
            predicate_slug=projected.get("predicate_slug", ""),
            assertion_status=projected.get("assertion_status", "active"),
            subject_kind=projected.get("subject_kind", ""),
            subject_ref=projected.get("subject_ref", ""),
            object_kind=projected.get("object_kind", ""),
            object_ref=projected.get("object_ref", ""),
            qualifiers_json=projected.get("qualifiers_json") or {},
            source_kind=projected.get("source_kind", ""),
            source_ref=projected.get("source_ref", ""),
            evidence_ref=projected.get("evidence_ref"),
            bound_decision_id=projected.get("bound_decision_id"),
            valid_from=projected.get("valid_from"),
            valid_to=projected.get("valid_to"),
            created_at=projected.get("created_at"),
            updated_at=projected.get("updated_at"),
        )
    )


class SemanticAssertionRepository(Protocol):
    """Minimal repository contract for semantic predicate and assertion authority."""

    async def load_predicate(
        self,
        *,
        predicate_slug: str,
    ) -> SemanticPredicateRecord | None:
        ...

    async def upsert_predicate(
        self,
        *,
        predicate: SemanticPredicateRecord,
    ) -> SemanticPredicateRecord:
        ...

    async def load_assertion(
        self,
        *,
        semantic_assertion_id: str,
    ) -> SemanticAssertionRecord | None:
        ...

    async def record_assertion(
        self,
        *,
        assertion: SemanticAssertionRecord,
        cardinality_mode: str,
        as_of: datetime,
    ) -> tuple[SemanticAssertionRecord, tuple[SemanticAssertionRecord, ...]]:
        ...

    async def retract_assertion(
        self,
        *,
        semantic_assertion_id: str,
        retracted_at: datetime,
        updated_at: datetime,
    ) -> SemanticAssertionRecord:
        ...

    async def list_current_assertions(
        self,
        *,
        predicate_slug: str | None = None,
        subject_kind: str | None = None,
        subject_ref: str | None = None,
        object_kind: str | None = None,
        object_ref: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        limit: int = 100,
    ) -> tuple[SemanticAssertionRecord, ...]:
        ...

    async def list_assertions(
        self,
        *,
        predicate_slug: str | None = None,
        subject_kind: str | None = None,
        subject_ref: str | None = None,
        object_kind: str | None = None,
        object_ref: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        active_at: datetime | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> tuple[SemanticAssertionRecord, ...]:
        ...

    async def rebuild_current_assertions(
        self,
        *,
        as_of: datetime,
    ) -> int:
        ...


__all__ = [
    "RESERVED_QUALIFIER_KEYS",
    "SUPPORTED_ASSERTION_STATUSES",
    "SUPPORTED_CARDINALITY_MODES",
    "SUPPORTED_PREDICATE_STATUSES",
    "SemanticAssertionError",
    "SemanticAssertionRecord",
    "SemanticAssertionRepository",
    "SemanticPredicateRecord",
    "normalize_semantic_assertion_record",
    "normalize_semantic_predicate_record",
    "project_semantic_assertion",
    "project_semantic_predicate",
    "semantic_assertion_id",
]
