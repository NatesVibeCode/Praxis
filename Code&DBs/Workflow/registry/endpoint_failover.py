"""Postgres-backed provider failover and endpoint authority repository.

This module reads canonical provider failover bindings and provider endpoint
bindings from Postgres. It resolves explicit effective-dated authority slices
and fails closed when a selector is missing, under-specified, or ambiguous.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg


class ProviderFailoverAndEndpointAuthorityRepositoryError(RuntimeError):
    """Raised when provider failover or endpoint authority cannot be read safely."""

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


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_nullable_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_selector_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_selector",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_selector_nullable_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_selector_text(value, field_name=field_name)


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_row",
            f"{field_name} must be an integer",
            details={"field": field_name},
        )
    return value


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_selector",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_selector",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _require_optional_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_datetime(value, field_name=field_name)


@dataclass(frozen=True, slots=True)
class ProviderFailoverAuthoritySelector:
    """Explicit selector for one active provider failover binding slice."""

    model_profile_id: str
    provider_policy_id: str
    binding_scope: str
    as_of: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "model_profile_id",
            _require_selector_text(self.model_profile_id, field_name="model_profile_id"),
        )
        object.__setattr__(
            self,
            "provider_policy_id",
            _require_selector_text(self.provider_policy_id, field_name="provider_policy_id"),
        )
        object.__setattr__(
            self,
            "binding_scope",
            _require_selector_text(self.binding_scope, field_name="binding_scope"),
        )
        object.__setattr__(self, "as_of", _normalize_as_of(self.as_of))


@dataclass(frozen=True, slots=True)
class ProviderEndpointAuthoritySelector:
    """Explicit selector for one active provider endpoint binding."""

    provider_policy_id: str
    candidate_ref: str
    binding_scope: str
    as_of: datetime
    endpoint_ref: str | None = None
    endpoint_kind: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider_policy_id",
            _require_selector_text(self.provider_policy_id, field_name="provider_policy_id"),
        )
        object.__setattr__(
            self,
            "candidate_ref",
            _require_selector_text(self.candidate_ref, field_name="candidate_ref"),
        )
        object.__setattr__(
            self,
            "binding_scope",
            _require_selector_text(self.binding_scope, field_name="binding_scope"),
        )
        normalized_endpoint_ref = _require_selector_nullable_text(
            self.endpoint_ref,
            field_name="endpoint_ref",
        )
        normalized_endpoint_kind = _require_selector_nullable_text(
            self.endpoint_kind,
            field_name="endpoint_kind",
        )
        if (normalized_endpoint_ref is None) == (normalized_endpoint_kind is None):
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.invalid_selector",
                "endpoint selector must provide exactly one of endpoint_ref or endpoint_kind",
                details={
                    "endpoint_ref": normalized_endpoint_ref,
                    "endpoint_kind": normalized_endpoint_kind,
                },
            )
        object.__setattr__(self, "endpoint_ref", normalized_endpoint_ref)
        object.__setattr__(self, "endpoint_kind", normalized_endpoint_kind)
        object.__setattr__(self, "as_of", _normalize_as_of(self.as_of))


@dataclass(frozen=True, slots=True)
class ProviderFailoverBindingAuthorityRecord:
    """Canonical provider failover binding row."""

    provider_failover_binding_id: str
    model_profile_id: str
    provider_policy_id: str
    candidate_ref: str
    binding_scope: str
    failover_role: str
    trigger_rule: str
    position_index: int
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ProviderEndpointBindingAuthorityRecord:
    """Canonical provider endpoint binding row."""

    provider_endpoint_binding_id: str
    provider_policy_id: str
    candidate_ref: str
    binding_scope: str
    endpoint_ref: str
    endpoint_kind: str
    transport_kind: str
    endpoint_uri: str
    auth_ref: str
    binding_status: str
    request_policy: Any
    circuit_breaker_policy: Any
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


def _require_failover_selector(
    value: object,
    *,
    field_name: str,
) -> ProviderFailoverAuthoritySelector:
    if not isinstance(value, ProviderFailoverAuthoritySelector):
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_selector",
            f"{field_name} must be a ProviderFailoverAuthoritySelector",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_endpoint_selector(
    value: object,
    *,
    field_name: str,
) -> ProviderEndpointAuthoritySelector:
    if not isinstance(value, ProviderEndpointAuthoritySelector):
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.invalid_selector",
            f"{field_name} must be a ProviderEndpointAuthoritySelector",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_failover_selectors(
    selectors: Sequence[ProviderFailoverAuthoritySelector] | None,
    *,
    field_name: str,
) -> tuple[ProviderFailoverAuthoritySelector, ...]:
    if selectors is None:
        return ()
    normalized: list[ProviderFailoverAuthoritySelector] = []
    for index, selector in enumerate(selectors):
        normalized.append(
            _require_failover_selector(selector, field_name=f"{field_name}[{index}]")
        )
    return tuple(dict.fromkeys(normalized))


def _normalize_endpoint_selectors(
    selectors: Sequence[ProviderEndpointAuthoritySelector] | None,
    *,
    field_name: str,
) -> tuple[ProviderEndpointAuthoritySelector, ...]:
    if selectors is None:
        return ()
    normalized: list[ProviderEndpointAuthoritySelector] = []
    for index, selector in enumerate(selectors):
        normalized.append(
            _require_endpoint_selector(selector, field_name=f"{field_name}[{index}]")
        )
    return tuple(dict.fromkeys(normalized))


def _failover_selector_details(
    selector: ProviderFailoverAuthoritySelector,
) -> dict[str, str]:
    return {
        "model_profile_id": selector.model_profile_id,
        "provider_policy_id": selector.provider_policy_id,
        "binding_scope": selector.binding_scope,
        "as_of": selector.as_of.isoformat(),
    }


def _endpoint_selector_details(
    selector: ProviderEndpointAuthoritySelector,
) -> dict[str, str]:
    return {
        "provider_policy_id": selector.provider_policy_id,
        "candidate_ref": selector.candidate_ref,
        "binding_scope": selector.binding_scope,
        "endpoint_ref": selector.endpoint_ref or "",
        "endpoint_kind": selector.endpoint_kind or "",
        "as_of": selector.as_of.isoformat(),
    }


def _provider_failover_binding_from_row(
    row: asyncpg.Record,
) -> ProviderFailoverBindingAuthorityRecord:
    return ProviderFailoverBindingAuthorityRecord(
        provider_failover_binding_id=_require_text(
            row["provider_failover_binding_id"],
            field_name="provider_failover_binding_id",
        ),
        model_profile_id=_require_text(
            row["model_profile_id"],
            field_name="model_profile_id",
        ),
        provider_policy_id=_require_text(
            row["provider_policy_id"],
            field_name="provider_policy_id",
        ),
        candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
        binding_scope=_require_text(row["binding_scope"], field_name="binding_scope"),
        failover_role=_require_text(row["failover_role"], field_name="failover_role"),
        trigger_rule=_require_text(row["trigger_rule"], field_name="trigger_rule"),
        position_index=_require_int(row["position_index"], field_name="position_index"),
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=_require_optional_datetime(row["effective_to"], field_name="effective_to"),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _provider_endpoint_binding_from_row(
    row: asyncpg.Record,
) -> ProviderEndpointBindingAuthorityRecord:
    return ProviderEndpointBindingAuthorityRecord(
        provider_endpoint_binding_id=_require_text(
            row["provider_endpoint_binding_id"],
            field_name="provider_endpoint_binding_id",
        ),
        provider_policy_id=_require_text(
            row["provider_policy_id"],
            field_name="provider_policy_id",
        ),
        candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
        binding_scope=_require_text(row["binding_scope"], field_name="binding_scope"),
        endpoint_ref=_require_text(row["endpoint_ref"], field_name="endpoint_ref"),
        endpoint_kind=_require_text(row["endpoint_kind"], field_name="endpoint_kind"),
        transport_kind=_require_text(row["transport_kind"], field_name="transport_kind"),
        endpoint_uri=_require_text(row["endpoint_uri"], field_name="endpoint_uri"),
        auth_ref=_require_text(row["auth_ref"], field_name="auth_ref"),
        binding_status=_require_text(row["binding_status"], field_name="binding_status"),
        request_policy=row["request_policy"],
        circuit_breaker_policy=row["circuit_breaker_policy"],
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=_require_optional_datetime(row["effective_to"], field_name="effective_to"),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _active_slice_key(
    *,
    effective_from: datetime,
    effective_to: datetime | None,
    decision_ref: str,
) -> tuple[datetime, datetime | None, str]:
    return (effective_from, effective_to, decision_ref)


def _format_slice_key(slice_key: tuple[datetime, datetime | None, str]) -> str:
    effective_from, effective_to, decision_ref = slice_key
    return (
        f"effective_from={effective_from.isoformat()},"
        f"effective_to={'' if effective_to is None else effective_to.isoformat()},"
        f"decision_ref={decision_ref}"
    )


def _ensure_one_failover_slice(
    records: Sequence[ProviderFailoverBindingAuthorityRecord],
    *,
    selector: ProviderFailoverAuthoritySelector,
) -> tuple[ProviderFailoverBindingAuthorityRecord, ...]:
    if not records:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.failover_missing",
            "missing active provider failover bindings for the requested selector",
            details=_failover_selector_details(selector),
        )
    slice_keys = {
        _active_slice_key(
            effective_from=record.effective_from,
            effective_to=record.effective_to,
            decision_ref=record.decision_ref,
        )
        for record in records
    }
    if len(slice_keys) != 1:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.ambiguous_failover_slice",
                "multiple active provider failover slices matched the requested selector",
                details={
                    **_failover_selector_details(selector),
                    "slice_keys": tuple(
                        _format_slice_key(key)
                        for key in sorted(slice_keys, key=_format_slice_key)
                    ),
                },
            )
    return tuple(records)


def _ensure_one_endpoint_binding(
    records: Sequence[ProviderEndpointBindingAuthorityRecord],
    *,
    selector: ProviderEndpointAuthoritySelector,
) -> ProviderEndpointBindingAuthorityRecord:
    if not records:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.endpoint_missing",
            "missing active provider endpoint binding for the requested selector",
            details=_endpoint_selector_details(selector),
        )
    slice_keys = {
        _active_slice_key(
            effective_from=record.effective_from,
            effective_to=record.effective_to,
            decision_ref=record.decision_ref,
        )
        for record in records
    }
    if len(slice_keys) != 1 or len(records) != 1:
        raise ProviderFailoverAndEndpointAuthorityRepositoryError(
            "endpoint_failover.ambiguous_endpoint_slice",
            "multiple active provider endpoint bindings matched the requested selector",
                details={
                    **_endpoint_selector_details(selector),
                    "endpoint_refs": tuple(dict.fromkeys(record.endpoint_ref for record in records)),
                    "slice_keys": tuple(
                        _format_slice_key(key)
                        for key in sorted(slice_keys, key=_format_slice_key)
                    ),
                },
            )
    return records[0]


@dataclass(frozen=True, slots=True)
class ProviderFailoverAndEndpointAuthority:
    """Canonical provider failover and endpoint authority loaded from Postgres."""

    provider_failover_bindings: Mapping[
        ProviderFailoverAuthoritySelector,
        tuple[ProviderFailoverBindingAuthorityRecord, ...],
    ]
    provider_endpoint_bindings: Mapping[
        ProviderEndpointAuthoritySelector,
        ProviderEndpointBindingAuthorityRecord,
    ]

    @property
    def provider_policy_ids(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                tuple(selector.provider_policy_id for selector in self.provider_failover_bindings)
                + tuple(selector.provider_policy_id for selector in self.provider_endpoint_bindings)
            )
        )

    @property
    def endpoint_refs(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                binding.endpoint_ref for binding in self.provider_endpoint_bindings.values()
            )
        )

    def resolve_provider_failover_bindings(
        self,
        *,
        selector: ProviderFailoverAuthoritySelector,
    ) -> tuple[ProviderFailoverBindingAuthorityRecord, ...]:
        normalized_selector = _require_failover_selector(selector, field_name="selector")
        records = self.provider_failover_bindings.get(normalized_selector)
        if records is None:
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.failover_missing",
                "missing loaded provider failover bindings for the requested selector",
                details=_failover_selector_details(normalized_selector),
            )
        return records

    def resolve_endpoint_binding(
        self,
        *,
        selector: ProviderEndpointAuthoritySelector,
    ) -> ProviderEndpointBindingAuthorityRecord:
        normalized_selector = _require_endpoint_selector(selector, field_name="selector")
        record = self.provider_endpoint_bindings.get(normalized_selector)
        if record is None:
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.endpoint_missing",
                "missing loaded provider endpoint binding for the requested selector",
                details=_endpoint_selector_details(normalized_selector),
            )
        return record


class PostgresProviderFailoverAndEndpointAuthorityRepository:
    """Explicit Postgres repository for failover and endpoint authority bindings."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_provider_failover_bindings(
        self,
        *,
        selector: ProviderFailoverAuthoritySelector,
    ) -> tuple[ProviderFailoverBindingAuthorityRecord, ...]:
        normalized_selector = _require_failover_selector(selector, field_name="selector")
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    provider_failover_binding_id,
                    model_profile_id,
                    provider_policy_id,
                    candidate_ref,
                    binding_scope,
                    failover_role,
                    trigger_rule,
                    position_index,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM provider_failover_bindings
                WHERE model_profile_id = $1
                  AND provider_policy_id = $2
                  AND binding_scope = $3
                  AND effective_from <= $4
                  AND (effective_to IS NULL OR effective_to > $4)
                ORDER BY position_index, candidate_ref, provider_failover_binding_id
                """,
                normalized_selector.model_profile_id,
                normalized_selector.provider_policy_id,
                normalized_selector.binding_scope,
                normalized_selector.as_of,
            )
        except asyncpg.PostgresError as exc:
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.read_failed",
                "failed to read provider failover bindings",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        records = tuple(_provider_failover_binding_from_row(row) for row in rows)
        return _ensure_one_failover_slice(records, selector=normalized_selector)

    async def fetch_endpoint_binding(
        self,
        *,
        selector: ProviderEndpointAuthoritySelector,
    ) -> ProviderEndpointBindingAuthorityRecord:
        normalized_selector = _require_endpoint_selector(selector, field_name="selector")
        identity_field = "endpoint_ref" if normalized_selector.endpoint_ref is not None else "endpoint_kind"
        identity_value = (
            normalized_selector.endpoint_ref
            if normalized_selector.endpoint_ref is not None
            else normalized_selector.endpoint_kind
        )
        try:
            rows = await self._conn.fetch(
                f"""
                SELECT
                    provider_endpoint_binding_id,
                    provider_policy_id,
                    candidate_ref,
                    binding_scope,
                    endpoint_ref,
                    endpoint_kind,
                    transport_kind,
                    endpoint_uri,
                    auth_ref,
                    binding_status,
                    request_policy,
                    circuit_breaker_policy,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM provider_endpoint_bindings
                WHERE provider_policy_id = $1
                  AND candidate_ref = $2
                  AND binding_scope = $3
                  AND {identity_field} = $4
                  AND binding_status = 'active'
                  AND effective_from <= $5
                  AND (effective_to IS NULL OR effective_to > $5)
                ORDER BY endpoint_ref, provider_endpoint_binding_id
                """,
                normalized_selector.provider_policy_id,
                normalized_selector.candidate_ref,
                normalized_selector.binding_scope,
                identity_value,
                normalized_selector.as_of,
            )
        except asyncpg.PostgresError as exc:
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.read_failed",
                "failed to read provider endpoint bindings",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        records = tuple(_provider_endpoint_binding_from_row(row) for row in rows)
        return _ensure_one_endpoint_binding(records, selector=normalized_selector)

    async def load_provider_failover_and_endpoint_authority(
        self,
        *,
        failover_selectors: Sequence[ProviderFailoverAuthoritySelector] | None = None,
        endpoint_selectors: Sequence[ProviderEndpointAuthoritySelector] | None = None,
    ) -> ProviderFailoverAndEndpointAuthority:
        """Load canonical failover and endpoint authority for explicit selectors only."""

        normalized_failover_selectors = _normalize_failover_selectors(
            failover_selectors,
            field_name="failover_selectors",
        )
        normalized_endpoint_selectors = _normalize_endpoint_selectors(
            endpoint_selectors,
            field_name="endpoint_selectors",
        )
        if not normalized_failover_selectors and not normalized_endpoint_selectors:
            raise ProviderFailoverAndEndpointAuthorityRepositoryError(
                "endpoint_failover.invalid_selector",
                "at least one explicit failover or endpoint selector is required",
            )

        provider_failover_bindings: dict[
            ProviderFailoverAuthoritySelector,
            tuple[ProviderFailoverBindingAuthorityRecord, ...],
        ] = {}
        for selector in normalized_failover_selectors:
            provider_failover_bindings[selector] = await self.fetch_provider_failover_bindings(
                selector=selector
            )

        provider_endpoint_bindings: dict[
            ProviderEndpointAuthoritySelector,
            ProviderEndpointBindingAuthorityRecord,
        ] = {}
        for selector in normalized_endpoint_selectors:
            provider_endpoint_bindings[selector] = await self.fetch_endpoint_binding(
                selector=selector
            )

        return ProviderFailoverAndEndpointAuthority(
            provider_failover_bindings=provider_failover_bindings,
            provider_endpoint_bindings=provider_endpoint_bindings,
        )


async def load_provider_failover_and_endpoint_authority(
    conn: asyncpg.Connection,
    *,
    failover_selectors: Sequence[ProviderFailoverAuthoritySelector] | None = None,
    endpoint_selectors: Sequence[ProviderEndpointAuthoritySelector] | None = None,
) -> ProviderFailoverAndEndpointAuthority:
    """Load provider failover and endpoint authority using the Postgres repository."""

    repository = PostgresProviderFailoverAndEndpointAuthorityRepository(conn)
    return await repository.load_provider_failover_and_endpoint_authority(
        failover_selectors=failover_selectors,
        endpoint_selectors=endpoint_selectors,
    )


__all__ = [
    "PostgresProviderFailoverAndEndpointAuthorityRepository",
    "ProviderEndpointAuthoritySelector",
    "ProviderEndpointBindingAuthorityRecord",
    "ProviderFailoverAndEndpointAuthority",
    "ProviderFailoverAndEndpointAuthorityRepositoryError",
    "ProviderFailoverAuthoritySelector",
    "ProviderFailoverBindingAuthorityRecord",
    "load_provider_failover_and_endpoint_authority",
]
