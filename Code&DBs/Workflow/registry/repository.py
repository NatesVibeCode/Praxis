"""Postgres-backed registry authority repository.

This keeps durable workspace/runtime-profile authority rows in Postgres without
changing the runtime intake surface. Callers can bootstrap the authority schema,
persist canonical rows, and load a standard ``RegistryResolver`` from the
database instead of injecting records by hand.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from functools import lru_cache
from typing import Any

import asyncpg

from storage.migrations import WorkflowMigrationError, workflow_migration_statements

from .domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from .native_runtime_profile_sync import (
    NativeRuntimeProfileSyncError,
    is_native_runtime_profile_ref,
    sync_native_runtime_profile_authority_async,
)

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_REGISTRY_AUTHORITY_SCHEMA_FILENAME = "002_registry_authority.sql"


class RegistryRepositoryError(RuntimeError):
    """Raised when durable registry authority cannot be read or written safely."""

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


@lru_cache(maxsize=1)
def _registry_authority_schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_REGISTRY_AUTHORITY_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "registry.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "registry.schema_missing"
        )
        message = (
            "registry authority schema file did not contain executable statements"
            if reason_code == "registry.schema_empty"
            else "registry authority schema file could not be read"
        )
        raise RegistryRepositoryError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryRepositoryError(
            "registry.invalid_authority_record",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _normalize_refs(refs: Sequence[str] | None, *, field_name: str) -> tuple[str, ...] | None:
    if refs is None:
        return None
    normalized: list[str] = []
    for index, ref in enumerate(refs):
        normalized.append(_require_text(ref, field_name=f"{field_name}[{index}]"))
    # Preserve caller order while removing duplicates so query parameters stay stable.
    return tuple(dict.fromkeys(normalized))


def _workspace_record_from_row(row: asyncpg.Record) -> WorkspaceAuthorityRecord:
    return WorkspaceAuthorityRecord(
        workspace_ref=str(row["workspace_ref"]),
        repo_root=str(row["repo_root"]),
        workdir=str(row["workdir"]),
    )


def _runtime_profile_record_from_row(
    row: asyncpg.Record,
) -> RuntimeProfileAuthorityRecord:
    return RuntimeProfileAuthorityRecord(
        runtime_profile_ref=str(row["runtime_profile_ref"]),
        model_profile_id=str(row["model_profile_id"]),
        provider_policy_id=str(row["provider_policy_id"]),
    )


async def bootstrap_registry_authority_schema(conn: asyncpg.Connection) -> None:
    """Apply the registry authority schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        for statement in _registry_authority_schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise RegistryRepositoryError(
                    "registry.schema_bootstrap_failed",
                    "failed to bootstrap the registry authority schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


class PostgresRegistryAuthorityRepository:
    """Explicit Postgres repository for canonical registry authority rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def upsert_workspace_authority(
        self,
        record: WorkspaceAuthorityRecord,
    ) -> WorkspaceAuthorityRecord:
        workspace_ref = _require_text(record.workspace_ref, field_name="workspace_ref")
        repo_root = _require_text(record.repo_root, field_name="repo_root")
        workdir = _require_text(record.workdir, field_name="workdir")
        try:
            await self._conn.execute(
                """
                INSERT INTO registry_workspace_authority (
                    workspace_ref,
                    repo_root,
                    workdir
                ) VALUES ($1, $2, $3)
                ON CONFLICT (workspace_ref) DO UPDATE
                SET repo_root = EXCLUDED.repo_root,
                    workdir = EXCLUDED.workdir,
                    recorded_at = now()
                """,
                workspace_ref,
                repo_root,
                workdir,
            )
        except asyncpg.PostgresError as exc:
            raise RegistryRepositoryError(
                "registry.write_failed",
                "failed to persist workspace authority",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "workspace_ref": workspace_ref,
                },
            ) from exc
        return WorkspaceAuthorityRecord(
            workspace_ref=workspace_ref,
            repo_root=repo_root,
            workdir=workdir,
        )

    async def upsert_runtime_profile_authority(
        self,
        record: RuntimeProfileAuthorityRecord,
    ) -> RuntimeProfileAuthorityRecord:
        runtime_profile_ref = _require_text(
            record.runtime_profile_ref,
            field_name="runtime_profile_ref",
        )
        model_profile_id = _require_text(
            record.model_profile_id,
            field_name="model_profile_id",
        )
        provider_policy_id = _require_text(
            record.provider_policy_id,
            field_name="provider_policy_id",
        )
        try:
            await self._conn.execute(
                """
                INSERT INTO registry_runtime_profile_authority (
                    runtime_profile_ref,
                    model_profile_id,
                    provider_policy_id
                ) VALUES ($1, $2, $3)
                ON CONFLICT (runtime_profile_ref) DO UPDATE
                SET model_profile_id = EXCLUDED.model_profile_id,
                    provider_policy_id = EXCLUDED.provider_policy_id,
                    recorded_at = now()
                """,
                runtime_profile_ref,
                model_profile_id,
                provider_policy_id,
            )
        except asyncpg.PostgresError as exc:
            raise RegistryRepositoryError(
                "registry.write_failed",
                "failed to persist runtime-profile authority",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "runtime_profile_ref": runtime_profile_ref,
                },
            ) from exc
        return RuntimeProfileAuthorityRecord(
            runtime_profile_ref=runtime_profile_ref,
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
        )

    async def fetch_workspace_authority(
        self,
        *,
        workspace_refs: Sequence[str] | None = None,
    ) -> tuple[WorkspaceAuthorityRecord, ...]:
        normalized_refs = _normalize_refs(workspace_refs, field_name="workspace_refs")
        try:
            if normalized_refs is None:
                rows = await self._conn.fetch(
                    """
                    SELECT workspace_ref, repo_root, workdir
                    FROM registry_workspace_authority
                    ORDER BY workspace_ref
                    """
                )
            else:
                rows = await self._conn.fetch(
                    """
                    SELECT workspace_ref, repo_root, workdir
                    FROM registry_workspace_authority
                    WHERE workspace_ref = ANY($1::text[])
                    ORDER BY workspace_ref
                    """,
                    list(normalized_refs),
                )
        except asyncpg.PostgresError as exc:
            raise RegistryRepositoryError(
                "registry.read_failed",
                "failed to read workspace authority",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_workspace_record_from_row(row) for row in rows)

    async def fetch_runtime_profile_authority(
        self,
        *,
        runtime_profile_refs: Sequence[str] | None = None,
    ) -> tuple[RuntimeProfileAuthorityRecord, ...]:
        normalized_refs = _normalize_refs(
            runtime_profile_refs,
            field_name="runtime_profile_refs",
        )
        try:
            if normalized_refs is None:
                rows = await self._conn.fetch(
                    """
                    SELECT runtime_profile_ref, model_profile_id, provider_policy_id
                    FROM registry_runtime_profile_authority
                    ORDER BY runtime_profile_ref
                    """
                )
            else:
                rows = await self._conn.fetch(
                    """
                    SELECT runtime_profile_ref, model_profile_id, provider_policy_id
                    FROM registry_runtime_profile_authority
                    WHERE runtime_profile_ref = ANY($1::text[])
                    ORDER BY runtime_profile_ref
                    """,
                    list(normalized_refs),
                )
        except asyncpg.PostgresError as exc:
            raise RegistryRepositoryError(
                "registry.read_failed",
                "failed to read runtime-profile authority",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_runtime_profile_record_from_row(row) for row in rows)

    async def load_resolver(
        self,
        *,
        workspace_refs: Sequence[str] | None = None,
        runtime_profile_refs: Sequence[str] | None = None,
    ) -> RegistryResolver:
        should_sync_native_profiles = runtime_profile_refs is None or any(
            is_native_runtime_profile_ref(ref)
            for ref in runtime_profile_refs
        )
        if should_sync_native_profiles:
            try:
                await sync_native_runtime_profile_authority_async(
                    self._conn,
                    prune=False,
                )
            except NativeRuntimeProfileSyncError as exc:
                raise RegistryRepositoryError(
                    "registry.native_profile_sync_failed",
                    "failed to refresh native runtime-profile authority",
                    details={"message": str(exc)},
                ) from exc
        workspace_records = await self.fetch_workspace_authority(
            workspace_refs=workspace_refs,
        )
        runtime_profile_records = await self.fetch_runtime_profile_authority(
            runtime_profile_refs=runtime_profile_refs,
        )
        return _resolver_from_authority_records(
            workspace_records=workspace_records,
            runtime_profile_records=runtime_profile_records,
        )


def _resolver_from_authority_records(
    *,
    workspace_records: Iterable[WorkspaceAuthorityRecord],
    runtime_profile_records: Iterable[RuntimeProfileAuthorityRecord],
) -> RegistryResolver:
    grouped_workspaces: dict[str, list[WorkspaceAuthorityRecord]] = defaultdict(list)
    for record in workspace_records:
        grouped_workspaces[record.workspace_ref].append(record)

    grouped_runtime_profiles: dict[str, list[RuntimeProfileAuthorityRecord]] = defaultdict(list)
    for record in runtime_profile_records:
        grouped_runtime_profiles[record.runtime_profile_ref].append(record)

    return RegistryResolver(
        workspace_records={
            workspace_ref: tuple(records)
            for workspace_ref, records in grouped_workspaces.items()
        },
        runtime_profile_records={
            runtime_profile_ref: tuple(records)
            for runtime_profile_ref, records in grouped_runtime_profiles.items()
        },
    )


async def load_registry_resolver(
    conn: asyncpg.Connection,
    *,
    workspace_refs: Sequence[str] | None = None,
    runtime_profile_refs: Sequence[str] | None = None,
) -> RegistryResolver:
    """Load a standard RegistryResolver from canonical Postgres authority rows."""

    repository = PostgresRegistryAuthorityRepository(conn)
    return await repository.load_resolver(
        workspace_refs=workspace_refs,
        runtime_profile_refs=runtime_profile_refs,
    )


__all__ = [
    "PostgresRegistryAuthorityRepository",
    "RegistryRepositoryError",
    "bootstrap_registry_authority_schema",
    "load_registry_resolver",
]
