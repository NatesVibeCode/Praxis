"""Postgres-backed routing catalog repository.

This repository loads the canonical model-routing catalog from Postgres and
normalizes it into the authority records consumed by ``registry.model_routing``.
The repository does not own routing heuristics; it only reads the catalog and
preserves the stored authority order.
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

from .model_routing import (
    ModelProfileAuthorityRecord,
    ProviderModelCandidateAuthorityRecord,
    ProviderPolicyAuthorityRecord,
)

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_ROUTE_CATALOG_SCHEMA_FILENAMES = (
    "006_platform_authority_schema.sql",
    "046_provider_model_candidate_profiles.sql",
    "074_provider_policy_multi_provider_refs.sql",
)
_TRANSACTION_CONTROL_STATEMENTS = {"BEGIN", "BEGIN;", "COMMIT", "COMMIT;"}


class RouteCatalogRepositoryError(RuntimeError):
    """Raised when routing catalog rows cannot be read safely."""

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


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


def _route_catalog_schema_statements() -> tuple[str, ...]:
    statements: list[str] = []
    for filename in _ROUTE_CATALOG_SCHEMA_FILENAMES:
        try:
            for statement in workflow_migration_statements(filename):
                if statement.strip().upper() in _TRANSACTION_CONTROL_STATEMENTS:
                    continue
                statements.append(statement)
        except WorkflowMigrationError as exc:
            reason_code = (
                "route_catalog.schema_empty"
                if exc.reason_code == "workflow.migration_empty"
                else "route_catalog.schema_missing"
            )
            message = (
                f"route catalog schema file {filename!r} did not contain executable statements"
                if reason_code == "route_catalog.schema_empty"
                else f"route catalog schema file {filename!r} could not be read"
            )
            raise RouteCatalogRepositoryError(
                reason_code,
                message,
                details={"filename": filename, **exc.details},
            ) from exc
    return tuple(statements)


@dataclass(frozen=True, slots=True)
class ModelProfileCandidateBindingAuthorityRecord:
    """Canonical binding row that orders model-profile candidate admission."""

    model_profile_candidate_binding_id: str
    model_profile_id: str
    candidate_ref: str
    binding_role: str
    position_index: int


@dataclass(frozen=True, slots=True)
class RouteCatalogAuthority:
    """Canonical routing catalog as loaded from Postgres rows."""

    model_profiles: Mapping[str, tuple[ModelProfileAuthorityRecord, ...]]
    provider_policies: Mapping[str, tuple[ProviderPolicyAuthorityRecord, ...]]
    provider_model_candidates: Mapping[str, tuple[ProviderModelCandidateAuthorityRecord, ...]]
    model_profile_candidate_bindings: Mapping[
        str,
        tuple[ModelProfileCandidateBindingAuthorityRecord, ...],
    ]


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_row",
            f"{field_name} must be an integer",
            details={"field": field_name},
    )
    return value


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


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


def _json_text_array(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RouteCatalogRepositoryError(
                "route_catalog.invalid_row",
                f"{field_name} must decode to a JSON array",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, list):
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_row",
            f"{field_name} must be a JSON array or JSON text",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    values: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        text = _require_text(item, field_name=f"{field_name}[{index}]")
        if text in seen:
            continue
        seen.add(text)
        values.append(text)
    return tuple(values)


def _group_by_field(records: Sequence[object], *, field_name: str) -> dict[str, tuple[object, ...]]:
    grouped: dict[str, list[object]] = defaultdict(list)
    for record in records:
        grouped[_require_text(getattr(record, field_name), field_name=field_name)].append(record)
    return {key: tuple(value) for key, value in grouped.items()}


def _binding_candidate_refs(
    bindings: Sequence[ModelProfileCandidateBindingAuthorityRecord],
) -> tuple[str, ...]:
    ordered_bindings = sorted(
        bindings,
        key=lambda record: (
            record.position_index,
            record.model_profile_candidate_binding_id,
            record.candidate_ref,
        ),
    )
    candidate_refs: list[str] = []
    seen_position_indexes: set[int] = set()
    seen_candidate_refs: set[str] = set()
    for binding in ordered_bindings:
        if binding.position_index in seen_position_indexes:
            raise RouteCatalogRepositoryError(
                "route_catalog.binding_position_ambiguous",
                (
                    f"model profile {binding.model_profile_id!r} has duplicate "
                    f"binding position_index={binding.position_index}"
                ),
            )
        seen_position_indexes.add(binding.position_index)
        if binding.candidate_ref in seen_candidate_refs:
            raise RouteCatalogRepositoryError(
                "route_catalog.binding_candidate_duplicate",
                (
                    f"model profile {binding.model_profile_id!r} binds candidate "
                    f"{binding.candidate_ref!r} more than once"
                ),
            )
        seen_candidate_refs.add(binding.candidate_ref)
        candidate_refs.append(binding.candidate_ref)
    return tuple(candidate_refs)


def _json_array_items(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, list):
            return tuple(decoded)
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_row",
            "jsonb array value must decode to a list",
        )
    if isinstance(value, Sequence):
        return tuple(value)
    raise RouteCatalogRepositoryError(
        "route_catalog.invalid_row",
        "jsonb array value must be a sequence or JSON text",
    )


def _json_object_value(value: object, *, field_name: str) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
        raise RouteCatalogRepositoryError(
            "route_catalog.invalid_row",
            f"{field_name} must decode to a JSON object",
        )
    if isinstance(value, Mapping):
        return dict(value)
    raise RouteCatalogRepositoryError(
        "route_catalog.invalid_row",
        f"{field_name} must be a JSON object or JSON text",
    )


class PostgresRouteCatalogRepository:
    """Explicit Postgres repository for canonical routing catalog rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_model_profile_candidate_bindings(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        as_of: datetime | None = None,
    ) -> tuple[ModelProfileCandidateBindingAuthorityRecord, ...]:
        normalized_model_profile_ids = _normalize_refs(
            model_profile_ids,
            field_name="model_profile_ids",
        )
        normalized_as_of = _normalize_as_of(as_of) if as_of is not None else None
        try:
            if normalized_model_profile_ids is None and normalized_as_of is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_candidate_binding_id,
                        model_profile_id,
                        candidate_ref,
                        binding_role,
                        position_index
                    FROM model_profile_candidate_bindings
                    ORDER BY model_profile_id, position_index, model_profile_candidate_binding_id
                    """
                )
            elif normalized_model_profile_ids is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_candidate_binding_id,
                        model_profile_id,
                        candidate_ref,
                        binding_role,
                        position_index
                    FROM model_profile_candidate_bindings
                    WHERE effective_from <= $1
                      AND (effective_to IS NULL OR effective_to > $1)
                    ORDER BY model_profile_id, position_index, model_profile_candidate_binding_id
                    """,
                    normalized_as_of,
                )
            elif normalized_as_of is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_candidate_binding_id,
                        model_profile_id,
                        candidate_ref,
                        binding_role,
                        position_index
                    FROM model_profile_candidate_bindings
                    WHERE model_profile_id = ANY($1::text[])
                    ORDER BY model_profile_id, position_index, model_profile_candidate_binding_id
                    """,
                    list(normalized_model_profile_ids),
                )
            else:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_candidate_binding_id,
                        model_profile_id,
                        candidate_ref,
                        binding_role,
                        position_index
                    FROM model_profile_candidate_bindings
                    WHERE model_profile_id = ANY($1::text[])
                      AND effective_from <= $2
                      AND (effective_to IS NULL OR effective_to > $2)
                    ORDER BY model_profile_id, position_index, model_profile_candidate_binding_id
                    """,
                    list(normalized_model_profile_ids),
                    normalized_as_of,
                )
        except asyncpg.PostgresError as exc:
            raise RouteCatalogRepositoryError(
                "route_catalog.read_failed",
                "failed to read model-profile candidate bindings",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return tuple(
            ModelProfileCandidateBindingAuthorityRecord(
                model_profile_candidate_binding_id=_require_text(
                    row["model_profile_candidate_binding_id"],
                    field_name="model_profile_candidate_binding_id",
                ),
                model_profile_id=_require_text(row["model_profile_id"], field_name="model_profile_id"),
                candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
                binding_role=_require_text(row["binding_role"], field_name="binding_role"),
                position_index=_require_int(row["position_index"], field_name="position_index"),
            )
            for row in rows
        )

    async def fetch_provider_model_candidates(
        self,
        *,
        candidate_refs: Sequence[str] | None = None,
        as_of: datetime | None = None,
    ) -> tuple[ProviderModelCandidateAuthorityRecord, ...]:
        normalized_candidate_refs = _normalize_refs(
            candidate_refs,
            field_name="candidate_refs",
        )
        normalized_as_of = _normalize_as_of(as_of) if as_of is not None else None
        try:
            if normalized_candidate_refs is None and normalized_as_of is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        candidate_ref,
                        provider_ref,
                        provider_name,
                        provider_slug,
                        model_slug,
                        status,
                        priority,
                        balance_weight,
                        capability_tags,
                        route_tier,
                        route_tier_rank,
                        latency_class,
                        latency_rank,
                        reasoning_control,
                        task_affinities,
                        benchmark_profile,
                        default_parameters,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    FROM provider_model_candidates
                    ORDER BY candidate_ref
                    """
                )
            elif normalized_candidate_refs is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        candidate_ref,
                        provider_ref,
                        provider_name,
                        provider_slug,
                        model_slug,
                        status,
                        priority,
                        balance_weight,
                        capability_tags,
                        route_tier,
                        route_tier_rank,
                        latency_class,
                        latency_rank,
                        reasoning_control,
                        task_affinities,
                        benchmark_profile,
                        default_parameters,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    FROM provider_model_candidates
                    WHERE effective_from <= $1
                      AND (effective_to IS NULL OR effective_to > $1)
                    ORDER BY candidate_ref
                    """,
                    normalized_as_of,
                )
            elif normalized_as_of is None:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        candidate_ref,
                        provider_ref,
                        provider_name,
                        provider_slug,
                        model_slug,
                        status,
                        priority,
                        balance_weight,
                        capability_tags,
                        route_tier,
                        route_tier_rank,
                        latency_class,
                        latency_rank,
                        reasoning_control,
                        task_affinities,
                        benchmark_profile,
                        default_parameters,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    FROM provider_model_candidates
                    WHERE candidate_ref = ANY($1::text[])
                    ORDER BY candidate_ref
                    """,
                    list(normalized_candidate_refs),
                )
            else:
                rows = await self._conn.fetch(
                    """
                    SELECT
                        candidate_ref,
                        provider_ref,
                        provider_name,
                        provider_slug,
                        model_slug,
                        status,
                        priority,
                        balance_weight,
                        capability_tags,
                        route_tier,
                        route_tier_rank,
                        latency_class,
                        latency_rank,
                        reasoning_control,
                        task_affinities,
                        benchmark_profile,
                        default_parameters,
                        effective_from,
                        effective_to,
                        decision_ref,
                        created_at
                    FROM provider_model_candidates
                    WHERE candidate_ref = ANY($1::text[])
                      AND effective_from <= $2
                      AND (effective_to IS NULL OR effective_to > $2)
                    ORDER BY candidate_ref
                    """,
                    list(normalized_candidate_refs),
                    normalized_as_of,
                )
        except asyncpg.PostgresError as exc:
            raise RouteCatalogRepositoryError(
                "route_catalog.read_failed",
                "failed to read provider/model candidates",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        return tuple(
            ProviderModelCandidateAuthorityRecord(
                candidate_ref=_require_text(row["candidate_ref"], field_name="candidate_ref"),
                provider_ref=_require_text(row["provider_ref"], field_name="provider_ref"),
                provider_slug=_require_text(row["provider_slug"], field_name="provider_slug"),
                model_slug=_require_text(row["model_slug"], field_name="model_slug"),
                provider_name=_require_text(row["provider_name"], field_name="provider_name"),
                priority=_require_int(row["priority"], field_name="priority"),
                balance_weight=_require_int(row["balance_weight"], field_name="balance_weight"),
                capability_tags=tuple(
                    _require_text(tag, field_name=f"capability_tags[{index}]")
                    for index, tag in enumerate(_json_array_items(row["capability_tags"]))
                ),
                route_tier=(
                    _require_text(row["route_tier"], field_name="route_tier")
                    if row["route_tier"] is not None
                    else None
                ),
                route_tier_rank=(
                    _require_int(row["route_tier_rank"], field_name="route_tier_rank")
                    if row["route_tier_rank"] is not None
                    else None
                ),
                latency_class=(
                    _require_text(row["latency_class"], field_name="latency_class")
                    if row["latency_class"] is not None
                    else None
                ),
                latency_rank=(
                    _require_int(row["latency_rank"], field_name="latency_rank")
                    if row["latency_rank"] is not None
                    else None
                ),
                reasoning_control=_json_object_value(
                    row["reasoning_control"],
                    field_name="reasoning_control",
                ),
                task_affinities=_json_object_value(
                    row["task_affinities"],
                    field_name="task_affinities",
                ),
                benchmark_profile=_json_object_value(
                    row["benchmark_profile"],
                    field_name="benchmark_profile",
                ),
            )
            for row in rows
        )

    async def fetch_model_profiles(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        as_of: datetime | None = None,
    ) -> tuple[ModelProfileAuthorityRecord, ...]:
        normalized_model_profile_ids = _normalize_refs(
            model_profile_ids,
            field_name="model_profile_ids",
        )
        normalized_as_of = _normalize_as_of(as_of) if as_of is not None else None
        try:
            if normalized_model_profile_ids is None and normalized_as_of is None:
                profile_rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_id,
                        profile_name,
                        provider_name,
                        model_name,
                        schema_version,
                        status,
                        budget_policy,
                        routing_policy,
                        default_parameters,
                        effective_from,
                        effective_to,
                        supersedes_model_profile_id,
                        created_at
                    FROM model_profiles
                    ORDER BY model_profile_id
                    """
                )
            elif normalized_model_profile_ids is None:
                profile_rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_id,
                        profile_name,
                        provider_name,
                        model_name,
                        schema_version,
                        status,
                        budget_policy,
                        routing_policy,
                        default_parameters,
                        effective_from,
                        effective_to,
                        supersedes_model_profile_id,
                        created_at
                    FROM model_profiles
                    WHERE effective_from <= $1
                      AND (effective_to IS NULL OR effective_to > $1)
                    ORDER BY model_profile_id
                    """,
                    normalized_as_of,
                )
            elif normalized_as_of is None:
                profile_rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_id,
                        profile_name,
                        provider_name,
                        model_name,
                        schema_version,
                        status,
                        budget_policy,
                        routing_policy,
                        default_parameters,
                        effective_from,
                        effective_to,
                        supersedes_model_profile_id,
                        created_at
                    FROM model_profiles
                    WHERE model_profile_id = ANY($1::text[])
                    ORDER BY model_profile_id
                    """,
                    list(normalized_model_profile_ids),
                )
            else:
                profile_rows = await self._conn.fetch(
                    """
                    SELECT
                        model_profile_id,
                        profile_name,
                        provider_name,
                        model_name,
                        schema_version,
                        status,
                        budget_policy,
                        routing_policy,
                        default_parameters,
                        effective_from,
                        effective_to,
                        supersedes_model_profile_id,
                        created_at
                    FROM model_profiles
                    WHERE model_profile_id = ANY($1::text[])
                      AND effective_from <= $2
                      AND (effective_to IS NULL OR effective_to > $2)
                    ORDER BY model_profile_id
                    """,
                    list(normalized_model_profile_ids),
                    normalized_as_of,
                )
            binding_rows = await self.fetch_model_profile_candidate_bindings(
                model_profile_ids=normalized_model_profile_ids,
                as_of=normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise RouteCatalogRepositoryError(
                "route_catalog.read_failed",
                "failed to read model profiles",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        grouped_bindings = _group_by_field(binding_rows, field_name="model_profile_id")
        records: list[ModelProfileAuthorityRecord] = []
        for row in profile_rows:
            model_profile_id = _require_text(row["model_profile_id"], field_name="model_profile_id")
            bindings = grouped_bindings.get(model_profile_id, ())
            records.append(
                ModelProfileAuthorityRecord(
                    model_profile_id=model_profile_id,
                    candidate_refs=_binding_candidate_refs(bindings),
                    default_candidate_ref=None,
                )
            )
        return tuple(records)

    async def fetch_provider_policies(
        self,
        *,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
        as_of: datetime | None = None,
    ) -> tuple[ProviderPolicyAuthorityRecord, ...]:
        normalized_provider_policy_ids = _normalize_refs(
            provider_policy_ids,
            field_name="provider_policy_ids",
        )
        normalized_as_of = _normalize_as_of(as_of) if as_of is not None else None
        _ = candidate_refs
        try:
            if normalized_provider_policy_ids is None and normalized_as_of is None:
                policy_rows = await self._conn.fetch(
                    """
                    SELECT
                        provider_policy_id,
                        policy_name,
                        provider_name,
                        allowed_provider_refs,
                        preferred_provider_ref,
                        scope,
                        schema_version,
                        status,
                        allowed_models,
                        retry_policy,
                        budget_policy,
                        routing_rules,
                        effective_from,
                        effective_to,
                        decision_ref
                    FROM provider_policies
                    ORDER BY provider_policy_id
                    """
                )
            elif normalized_provider_policy_ids is None:
                policy_rows = await self._conn.fetch(
                    """
                    SELECT
                        provider_policy_id,
                        policy_name,
                        provider_name,
                        allowed_provider_refs,
                        preferred_provider_ref,
                        scope,
                        schema_version,
                        status,
                        allowed_models,
                        retry_policy,
                        budget_policy,
                        routing_rules,
                        effective_from,
                        effective_to,
                        decision_ref
                    FROM provider_policies
                    WHERE effective_from <= $1
                      AND (effective_to IS NULL OR effective_to > $1)
                    ORDER BY provider_policy_id
                    """,
                    normalized_as_of,
                )
            elif normalized_as_of is None:
                policy_rows = await self._conn.fetch(
                    """
                    SELECT
                        provider_policy_id,
                        policy_name,
                        provider_name,
                        allowed_provider_refs,
                        preferred_provider_ref,
                        scope,
                        schema_version,
                        status,
                        allowed_models,
                        retry_policy,
                        budget_policy,
                        routing_rules,
                        effective_from,
                    effective_to,
                    decision_ref
                FROM provider_policies
                    WHERE provider_policy_id = ANY($1::text[])
                    ORDER BY provider_policy_id
                    """,
                    list(normalized_provider_policy_ids),
                )
            else:
                policy_rows = await self._conn.fetch(
                    """
                    SELECT
                        provider_policy_id,
                        policy_name,
                        provider_name,
                        allowed_provider_refs,
                        preferred_provider_ref,
                        scope,
                        schema_version,
                        status,
                        allowed_models,
                        retry_policy,
                        budget_policy,
                        routing_rules,
                        effective_from,
                        effective_to,
                        decision_ref
                    FROM provider_policies
                    WHERE provider_policy_id = ANY($1::text[])
                      AND effective_from <= $2
                      AND (effective_to IS NULL OR effective_to > $2)
                    ORDER BY provider_policy_id
                    """,
                    list(normalized_provider_policy_ids),
                    normalized_as_of,
                )
        except asyncpg.PostgresError as exc:
            raise RouteCatalogRepositoryError(
                "route_catalog.read_failed",
                "failed to read provider policies",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc

        records: list[ProviderPolicyAuthorityRecord] = []
        for row in policy_rows:
            provider_policy_id = _require_text(row["provider_policy_id"], field_name="provider_policy_id")
            provider_name = _require_text(row["provider_name"], field_name="provider_name")
            allowed_provider_refs = _json_text_array(
                row.get("allowed_provider_refs"),
                field_name="allowed_provider_refs",
            )
            preferred_provider_ref = row.get("preferred_provider_ref")
            if preferred_provider_ref is not None:
                preferred_provider_ref = _require_text(
                    preferred_provider_ref,
                    field_name="preferred_provider_ref",
                )
            records.append(
                ProviderPolicyAuthorityRecord(
                    provider_policy_id=provider_policy_id,
                    allowed_provider_refs=allowed_provider_refs,
                    preferred_provider_ref=preferred_provider_ref,
                    provider_name=provider_name,
                )
            )
        return tuple(records)

    async def bootstrap_route_catalog_schema(self) -> None:
        """Apply the route catalog schema in an idempotent, fail-closed way."""

        async with self._conn.transaction():
            for statement in _route_catalog_schema_statements():
                try:
                    async with self._conn.transaction():
                        await self._conn.execute(statement)
                except asyncpg.PostgresError as exc:
                    if _is_duplicate_object_error(exc):
                        continue
                    raise RouteCatalogRepositoryError(
                        "route_catalog.schema_bootstrap_failed",
                        "failed to bootstrap the route catalog schema",
                        details={
                            "sqlstate": getattr(exc, "sqlstate", None),
                            "statement": statement[:120],
                        },
                    ) from exc

    async def load_route_catalog(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
        as_of: datetime | None = None,
    ) -> RouteCatalogAuthority:
        """Load canonical routing authority from Postgres."""

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
        normalized_as_of = _normalize_as_of(as_of) if as_of is not None else None

        model_profile_records = await self.fetch_model_profiles(
            model_profile_ids=normalized_model_profile_ids,
            as_of=normalized_as_of,
        )
        provider_policy_records = await self.fetch_provider_policies(
            provider_policy_ids=normalized_provider_policy_ids,
            candidate_refs=normalized_candidate_refs,
            as_of=normalized_as_of,
        )
        provider_model_candidate_records = await self.fetch_provider_model_candidates(
            candidate_refs=normalized_candidate_refs,
            as_of=normalized_as_of,
        )
        binding_records = await self.fetch_model_profile_candidate_bindings(
            model_profile_ids=normalized_model_profile_ids,
            as_of=normalized_as_of,
        )

        return RouteCatalogAuthority(
            model_profiles=_group_by_field(model_profile_records, field_name="model_profile_id"),
            provider_policies=_group_by_field(provider_policy_records, field_name="provider_policy_id"),
            provider_model_candidates=_group_by_field(
                provider_model_candidate_records,
                field_name="candidate_ref",
            ),
            model_profile_candidate_bindings=_group_by_field(
                binding_records,
                field_name="model_profile_id",
            ),
        )


async def load_route_catalog(
    conn: asyncpg.Connection,
    *,
    as_of: datetime | None = None,
) -> RouteCatalogAuthority:
    """Load the canonical routing catalog using the Postgres repository."""

    repository = PostgresRouteCatalogRepository(conn)
    return await repository.load_route_catalog(as_of=as_of)


__all__ = [
    "ModelProfileCandidateBindingAuthorityRecord",
    "PostgresRouteCatalogRepository",
    "RouteCatalogAuthority",
    "RouteCatalogRepositoryError",
    "load_route_catalog",
]
