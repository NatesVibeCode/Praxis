"""Postgres-backed provider route authority repository.

This module loads the canonical provider health windows, provider budget
windows, and route eligibility states that constrain routing decisions.
The authority is stored in Postgres and normalized here into explicit records
for ``registry.model_routing``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import asyncpg

from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_ROUTE_AUTHORITY_SCHEMA_FILENAME = "007_provider_route_health_budget.sql"


class ProviderRouteAuthorityRepositoryError(RuntimeError):
    """Raised when provider route authority rows cannot be read safely."""

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


@dataclass(frozen=True, slots=True)
class ProviderRouteHealthWindowAuthorityRecord:
    """Canonical provider-route health window behind a routing decision."""

    provider_route_health_window_id: str
    candidate_ref: str
    provider_ref: str
    health_status: str
    health_score: float
    sample_count: int
    failure_rate: float
    latency_p95_ms: int | None
    observed_window_started_at: Any
    observed_window_ended_at: Any
    observation_ref: str
    created_at: Any


@dataclass(frozen=True, slots=True)
class ProviderBudgetWindowAuthorityRecord:
    """Canonical provider budget window behind routing admission."""

    provider_budget_window_id: str
    provider_policy_id: str
    provider_ref: str
    budget_scope: str
    budget_status: str
    window_started_at: Any
    window_ended_at: Any
    request_limit: int | None
    requests_used: int
    token_limit: int | None
    tokens_used: int
    spend_limit_usd: object | None
    spend_used_usd: object
    decision_ref: str
    created_at: Any


@dataclass(frozen=True, slots=True)
class RouteEligibilityStateAuthorityRecord:
    """Canonical route eligibility state over a profile, policy, and candidate."""

    route_eligibility_state_id: str
    model_profile_id: str
    provider_policy_id: str
    candidate_ref: str
    eligibility_status: str
    reason_code: str
    source_window_refs: tuple[str, ...]
    evaluated_at: Any
    expires_at: Any | None
    decision_ref: str
    created_at: Any


@dataclass(frozen=True, slots=True)
class ProviderRouteAuthority:
    """Canonical provider route authority loaded from Postgres rows."""

    provider_route_health_windows: Mapping[
        str,
        tuple[ProviderRouteHealthWindowAuthorityRecord, ...],
    ]
    provider_budget_windows: Mapping[
        str,
        tuple[ProviderBudgetWindowAuthorityRecord, ...],
    ]
    route_eligibility_states: Mapping[
        str,
        tuple[RouteEligibilityStateAuthorityRecord, ...],
    ]


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


def _route_authority_schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_ROUTE_AUTHORITY_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "provider_routing.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "provider_routing.schema_missing"
        )
        message = (
            "provider route authority schema file did not contain executable statements"
            if reason_code == "provider_routing.schema_empty"
            else "provider route authority schema file could not be read"
        )
        raise ProviderRouteAuthorityRepositoryError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            f"{field_name} must be an integer",
            details={"field": field_name},
        )
    return value


def _require_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            f"{field_name} must be a number",
            details={"field": field_name},
        )
    return float(value)


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _json_array_items(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, list):
            return tuple(decoded)
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_row",
            "jsonb array value must decode to a list",
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    raise ProviderRouteAuthorityRepositoryError(
        "provider_routing.invalid_row",
        "jsonb array value must be a sequence or JSON text",
    )


def _normalize_refs(
    refs: Sequence[str] | None,
    *,
    field_name: str,
) -> tuple[str, ...] | None:
    if refs is None:
        return None
    normalized: list[str] = []
    for index, ref in enumerate(refs):
        normalized.append(_require_text(ref, field_name=f"{field_name}[{index}]"))
    return tuple(dict.fromkeys(normalized))


def _group_by_field(records: Sequence[object], *, field_name: str) -> dict[str, tuple[object, ...]]:
    grouped: dict[str, list[object]] = defaultdict(list)
    for record in records:
        grouped[_require_text(getattr(record, field_name), field_name=field_name)].append(record)
    return {key: tuple(value) for key, value in grouped.items()}


def _is_null_or_any_clause(field_name: str, parameter_index: int) -> str:
    placeholder = f"${parameter_index}::text[]"
    return f"({placeholder} IS NULL OR {field_name} = ANY({placeholder}))"


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _route_state_snapshot_key(
    record: RouteEligibilityStateAuthorityRecord,
) -> tuple[datetime, str, str]:
    return (
        _require_datetime(
            record.evaluated_at,
            field_name="route_eligibility_state.evaluated_at",
        ),
        _require_text(record.decision_ref, field_name="route_eligibility_state.decision_ref"),
        _require_text(
            record.route_eligibility_state_id,
            field_name="route_eligibility_state.route_eligibility_state_id",
        ),
    )


def snapshot_provider_route_authority(
    authority: ProviderRouteAuthority,
    *,
    as_of: datetime,
) -> ProviderRouteAuthority:
    """Snapshot route authority to the latest eligibility state at or before ``as_of``."""

    if not isinstance(authority, ProviderRouteAuthority):
        raise ProviderRouteAuthorityRepositoryError(
            "provider_routing.invalid_snapshot_source",
            "authority must be a ProviderRouteAuthority",
            details={"value_type": type(authority).__name__},
        )
    normalized_as_of = _normalize_as_of(as_of)

    selected_states: dict[
        tuple[str, str, str],
        RouteEligibilityStateAuthorityRecord,
    ] = {}
    for records in authority.route_eligibility_states.values():
        for record in records:
            if _require_datetime(
                record.evaluated_at,
                field_name="route_eligibility_state.evaluated_at",
            ) > normalized_as_of:
                continue
            snapshot_key = (
                _require_text(record.model_profile_id, field_name="model_profile_id"),
                _require_text(record.provider_policy_id, field_name="provider_policy_id"),
                _require_text(record.candidate_ref, field_name="candidate_ref"),
            )
            existing_record = selected_states.get(snapshot_key)
            if existing_record is None or _route_state_snapshot_key(record) > _route_state_snapshot_key(
                existing_record
            ):
                selected_states[snapshot_key] = record

    cited_window_refs: set[str] = set()
    grouped_states: dict[str, list[RouteEligibilityStateAuthorityRecord]] = defaultdict(list)
    for record in selected_states.values():
        grouped_states[record.candidate_ref].append(record)
        cited_window_refs.update(record.source_window_refs)

    route_eligibility_states = {
        candidate_ref: tuple(
            sorted(records, key=_route_state_snapshot_key, reverse=True)
        )
        for candidate_ref, records in grouped_states.items()
    }

    provider_route_health_windows = {
        candidate_ref: tuple(
            record
            for record in records
            if record.provider_route_health_window_id in cited_window_refs
        )
        for candidate_ref, records in authority.provider_route_health_windows.items()
    }
    provider_budget_windows = {
        provider_policy_id: tuple(
            record
            for record in records
            if record.provider_budget_window_id in cited_window_refs
        )
        for provider_policy_id, records in authority.provider_budget_windows.items()
    }

    return ProviderRouteAuthority(
        provider_route_health_windows={
            candidate_ref: records
            for candidate_ref, records in provider_route_health_windows.items()
            if records
        },
        provider_budget_windows={
            provider_policy_id: records
            for provider_policy_id, records in provider_budget_windows.items()
            if records
        },
        route_eligibility_states=route_eligibility_states,
    )


def bound_provider_route_authority(
    authority: ProviderRouteAuthority,
    *,
    model_profile_id: str,
    provider_policy_id: str,
    as_of: datetime,
) -> ProviderRouteAuthority:
    """Bound a route-authority snapshot to one model profile and provider policy."""

    normalized_model_profile_id = _require_text(
        model_profile_id,
        field_name="model_profile_id",
    )
    normalized_provider_policy_id = _require_text(
        provider_policy_id,
        field_name="provider_policy_id",
    )
    snapshot = snapshot_provider_route_authority(authority, as_of=as_of)

    filtered_eligibility_states: dict[str, tuple[RouteEligibilityStateAuthorityRecord, ...]] = {}
    cited_window_refs: set[str] = set()
    for candidate_ref, records in snapshot.route_eligibility_states.items():
        filtered_records = tuple(
            record
            for record in records
            if record.model_profile_id == normalized_model_profile_id
            and record.provider_policy_id == normalized_provider_policy_id
        )
        if not filtered_records:
            continue
        filtered_eligibility_states[candidate_ref] = filtered_records
        for record in filtered_records:
            cited_window_refs.update(record.source_window_refs)

    filtered_health_windows = {
        candidate_ref: tuple(
            record
            for record in records
            if record.provider_route_health_window_id in cited_window_refs
        )
        for candidate_ref, records in snapshot.provider_route_health_windows.items()
    }
    filtered_budget_windows = {
        provider_policy_key: tuple(
            record
            for record in records
            if record.provider_budget_window_id in cited_window_refs
        )
        for provider_policy_key, records in snapshot.provider_budget_windows.items()
    }

    return ProviderRouteAuthority(
        provider_route_health_windows={
            candidate_ref: records
            for candidate_ref, records in filtered_health_windows.items()
            if records
        },
        provider_budget_windows={
            provider_policy_key: records
            for provider_policy_key, records in filtered_budget_windows.items()
            if records
        },
        route_eligibility_states=filtered_eligibility_states,
    )


def select_route_eligibility_state(
    authority: ProviderRouteAuthority,
    *,
    model_profile_id: str,
    provider_policy_id: str,
    candidate_ref: str,
) -> RouteEligibilityStateAuthorityRecord | None:
    """Return the latest bounded route-eligibility state for one candidate."""

    normalized_model_profile_id = _require_text(
        model_profile_id,
        field_name="model_profile_id",
    )
    normalized_provider_policy_id = _require_text(
        provider_policy_id,
        field_name="provider_policy_id",
    )
    normalized_candidate_ref = _require_text(
        candidate_ref,
        field_name="candidate_ref",
    )
    candidate_records = authority.route_eligibility_states.get(normalized_candidate_ref, ())
    matching_records = tuple(
        record
        for record in candidate_records
        if record.model_profile_id == normalized_model_profile_id
        and record.provider_policy_id == normalized_provider_policy_id
    )
    if not matching_records:
        return None
    return max(
        matching_records,
        key=_route_state_snapshot_key,
    )


class PostgresProviderRouteAuthorityRepository:
    """Explicit Postgres repository for canonical provider route authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def bootstrap_provider_route_authority_schema(self) -> None:
        """Apply the route authority schema in an idempotent, fail-closed way."""

        async with self._conn.transaction():
            for statement in _route_authority_schema_statements():
                try:
                    async with self._conn.transaction():
                        await self._conn.execute(statement)
                except asyncpg.PostgresError as exc:
                    if _is_duplicate_object_error(exc):
                        continue
                    raise ProviderRouteAuthorityRepositoryError(
                        "provider_routing.schema_bootstrap_failed",
                        "failed to bootstrap the provider route authority schema",
                        details={
                            "sqlstate": getattr(exc, "sqlstate", None),
                            "statement": statement[:120],
                        },
                    ) from exc

    async def fetch_provider_route_health_windows(
        self,
        *,
        candidate_refs: Sequence[str] | None = None,
    ) -> tuple[ProviderRouteHealthWindowAuthorityRecord, ...]:
        normalized_candidate_refs = _normalize_refs(
            candidate_refs,
            field_name="candidate_refs",
        )
        try:
            rows = await self._conn.fetch(
                f"""
                SELECT
                    provider_route_health_window_id,
                    candidate_ref,
                    provider_ref,
                    health_status,
                    health_score,
                    sample_count,
                    failure_rate,
                    latency_p95_ms,
                    observed_window_started_at,
                    observed_window_ended_at,
                    observation_ref,
                    created_at
                FROM provider_route_health_windows
                WHERE {_is_null_or_any_clause("candidate_ref", 1)}
                ORDER BY candidate_ref, observed_window_ended_at DESC, provider_route_health_window_id DESC
                """,
                list(normalized_candidate_refs) if normalized_candidate_refs is not None else None,
            )
        except asyncpg.PostgresError as exc:
            raise ProviderRouteAuthorityRepositoryError(
                "provider_routing.read_failed",
                "failed to read provider route health windows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return tuple(
            ProviderRouteHealthWindowAuthorityRecord(
                provider_route_health_window_id=_require_text(
                    row["provider_route_health_window_id"],
                    field_name="provider_route_health_window_id",
                ),
                candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
                provider_ref=_require_text(row["provider_ref"], field_name="provider_ref"),
                health_status=_require_text(row["health_status"], field_name="health_status"),
                health_score=_require_float(row["health_score"], field_name="health_score"),
                sample_count=_require_int(row["sample_count"], field_name="sample_count"),
                failure_rate=_require_float(row["failure_rate"], field_name="failure_rate"),
                latency_p95_ms=(
                    _require_int(row["latency_p95_ms"], field_name="latency_p95_ms")
                    if row["latency_p95_ms"] is not None
                    else None
                ),
                observed_window_started_at=row["observed_window_started_at"],
                observed_window_ended_at=row["observed_window_ended_at"],
                observation_ref=_require_text(row["observation_ref"], field_name="observation_ref"),
                created_at=row["created_at"],
            )
            for row in rows
        )

    async def fetch_provider_budget_windows(
        self,
        *,
        provider_policy_ids: Sequence[str] | None = None,
    ) -> tuple[ProviderBudgetWindowAuthorityRecord, ...]:
        normalized_provider_policy_ids = _normalize_refs(
            provider_policy_ids,
            field_name="provider_policy_ids",
        )
        try:
            rows = await self._conn.fetch(
                f"""
                SELECT
                    provider_budget_window_id,
                    provider_policy_id,
                    provider_ref,
                    budget_scope,
                    budget_status,
                    window_started_at,
                    window_ended_at,
                    request_limit,
                    requests_used,
                    token_limit,
                    tokens_used,
                    spend_limit_usd,
                    spend_used_usd,
                    decision_ref,
                    created_at
                FROM provider_budget_windows
                WHERE {_is_null_or_any_clause("provider_policy_id", 1)}
                ORDER BY provider_policy_id, budget_scope, window_ended_at DESC, provider_budget_window_id DESC
                """,
                list(normalized_provider_policy_ids) if normalized_provider_policy_ids is not None else None,
            )
        except asyncpg.PostgresError as exc:
            raise ProviderRouteAuthorityRepositoryError(
                "provider_routing.read_failed",
                "failed to read provider budget windows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return tuple(
            ProviderBudgetWindowAuthorityRecord(
                provider_budget_window_id=_require_text(
                    row["provider_budget_window_id"],
                    field_name="provider_budget_window_id",
                ),
                provider_policy_id=_require_text(row["provider_policy_id"], field_name="provider_policy_id"),
                provider_ref=_require_text(row["provider_ref"], field_name="provider_ref"),
                budget_scope=_require_text(row["budget_scope"], field_name="budget_scope"),
                budget_status=_require_text(row["budget_status"], field_name="budget_status"),
                window_started_at=row["window_started_at"],
                window_ended_at=row["window_ended_at"],
                request_limit=(
                    _require_int(row["request_limit"], field_name="request_limit")
                    if row["request_limit"] is not None
                    else None
                ),
                requests_used=_require_int(row["requests_used"], field_name="requests_used"),
                token_limit=(
                    _require_int(row["token_limit"], field_name="token_limit")
                    if row["token_limit"] is not None
                    else None
                ),
                tokens_used=_require_int(row["tokens_used"], field_name="tokens_used"),
                spend_limit_usd=row["spend_limit_usd"],
                spend_used_usd=row["spend_used_usd"],
                decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
                created_at=row["created_at"],
            )
            for row in rows
        )

    async def fetch_route_eligibility_states(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> tuple[RouteEligibilityStateAuthorityRecord, ...]:
        normalized_model_profile_ids = _normalize_refs(
            model_profile_ids,
            field_name="model_profile_ids",
        )
        normalized_provider_policy_ids = _normalize_refs(
            provider_policy_ids,
            field_name="provider_policy_ids",
        )
        normalized_candidate_refs = _normalize_refs(
            candidate_refs,
            field_name="candidate_refs",
        )
        try:
            rows = await self._conn.fetch(
                f"""
                SELECT
                    route_eligibility_state_id,
                    model_profile_id,
                    provider_policy_id,
                    candidate_ref,
                    eligibility_status,
                    reason_code,
                    source_window_refs,
                    evaluated_at,
                    expires_at,
                    decision_ref,
                    created_at
                FROM route_eligibility_states
                WHERE {_is_null_or_any_clause("model_profile_id", 1)}
                  AND {_is_null_or_any_clause("provider_policy_id", 2)}
                  AND {_is_null_or_any_clause("candidate_ref", 3)}
                ORDER BY model_profile_id, provider_policy_id, candidate_ref, evaluated_at DESC, route_eligibility_state_id DESC
                """,
                list(normalized_model_profile_ids) if normalized_model_profile_ids is not None else None,
                list(normalized_provider_policy_ids) if normalized_provider_policy_ids is not None else None,
                list(normalized_candidate_refs) if normalized_candidate_refs is not None else None,
            )
        except asyncpg.PostgresError as exc:
            raise ProviderRouteAuthorityRepositoryError(
                "provider_routing.read_failed",
                "failed to read route eligibility states",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return tuple(
            RouteEligibilityStateAuthorityRecord(
                route_eligibility_state_id=_require_text(
                    row["route_eligibility_state_id"],
                    field_name="route_eligibility_state_id",
                ),
                model_profile_id=_require_text(row["model_profile_id"], field_name="model_profile_id"),
                provider_policy_id=_require_text(
                    row["provider_policy_id"],
                    field_name="provider_policy_id",
                ),
                candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
                eligibility_status=_require_text(
                    row["eligibility_status"],
                    field_name="eligibility_status",
                ),
                reason_code=_require_text(row["reason_code"], field_name="reason_code"),
                source_window_refs=tuple(
                    _require_text(ref, field_name=f"source_window_refs[{index}]")
                    for index, ref in enumerate(_json_array_items(row["source_window_refs"]))
                ),
                evaluated_at=row["evaluated_at"],
                expires_at=row["expires_at"],
                decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
                created_at=row["created_at"],
            )
            for row in rows
        )

    async def load_provider_route_authority(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> ProviderRouteAuthority:
        """Load canonical provider route authority from Postgres."""

        normalized_model_profile_ids = _normalize_refs(
            model_profile_ids,
            field_name="model_profile_ids",
        )
        normalized_provider_policy_ids = _normalize_refs(
            provider_policy_ids,
            field_name="provider_policy_ids",
        )
        normalized_candidate_refs = _normalize_refs(
            candidate_refs,
            field_name="candidate_refs",
        )

        health_windows = await self.fetch_provider_route_health_windows(
            candidate_refs=normalized_candidate_refs,
        )
        budget_windows = await self.fetch_provider_budget_windows(
            provider_policy_ids=normalized_provider_policy_ids,
        )
        eligibility_states = await self.fetch_route_eligibility_states(
            model_profile_ids=normalized_model_profile_ids,
            provider_policy_ids=normalized_provider_policy_ids,
            candidate_refs=normalized_candidate_refs,
        )

        return ProviderRouteAuthority(
            provider_route_health_windows=_group_by_field(
                health_windows,
                field_name="candidate_ref",
            ),
            provider_budget_windows=_group_by_field(
                budget_windows,
                field_name="provider_policy_id",
            ),
            route_eligibility_states=_group_by_field(
                eligibility_states,
                field_name="candidate_ref",
            ),
        )

    async def load_provider_route_authority_snapshot(
        self,
        *,
        as_of: datetime,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> ProviderRouteAuthority:
        """Load an explicit provider-route authority snapshot at or before ``as_of``."""

        authority = await self.load_provider_route_authority(
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )
        return snapshot_provider_route_authority(authority, as_of=as_of)


async def load_provider_route_authority(
    conn: asyncpg.Connection,
    *,
    model_profile_ids: Sequence[str] | None = None,
    provider_policy_ids: Sequence[str] | None = None,
    candidate_refs: Sequence[str] | None = None,
) -> ProviderRouteAuthority:
    """Load the canonical provider route authority using the Postgres repository."""

    repository = PostgresProviderRouteAuthorityRepository(conn)
    return await repository.load_provider_route_authority(
        model_profile_ids=model_profile_ids,
        provider_policy_ids=provider_policy_ids,
        candidate_refs=candidate_refs,
    )


async def load_provider_route_authority_snapshot(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
    model_profile_ids: Sequence[str] | None = None,
    provider_policy_ids: Sequence[str] | None = None,
    candidate_refs: Sequence[str] | None = None,
) -> ProviderRouteAuthority:
    """Load canonical provider route authority snapshot at or before ``as_of``."""

    repository = PostgresProviderRouteAuthorityRepository(conn)
    return await repository.load_provider_route_authority_snapshot(
        as_of=as_of,
        model_profile_ids=model_profile_ids,
        provider_policy_ids=provider_policy_ids,
        candidate_refs=candidate_refs,
    )


__all__ = [
    "bound_provider_route_authority",
    "PostgresProviderRouteAuthorityRepository",
    "ProviderBudgetWindowAuthorityRecord",
    "ProviderRouteAuthority",
    "ProviderRouteAuthorityRepositoryError",
    "ProviderRouteHealthWindowAuthorityRecord",
    "RouteEligibilityStateAuthorityRecord",
    "load_provider_route_authority",
    "load_provider_route_authority_snapshot",
    "select_route_eligibility_state",
    "snapshot_provider_route_authority",
]
