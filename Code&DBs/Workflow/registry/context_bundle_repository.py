"""Postgres-backed context bundle repository.

This module persists the canonical bundle snapshot for a run plus the anchor
rows that explain how the snapshot was derived. The authority is durable, not
injected.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any
import json

import asyncpg

from .domain import ContextBundle
from storage.migrations import WorkflowMigrationError, workflow_migration_statements


class ContextBundleRepositoryError(RuntimeError):
    """Raised when durable context bundle authority cannot be read or written."""

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
class ContextBundleAnchorRecord:
    """One durable anchor row that explains a stored context bundle."""

    anchor_ref: str
    anchor_kind: str
    content_hash: str
    payload: Mapping[str, Any]
    position_index: int


@dataclass(frozen=True, slots=True)
class ContextBundleSnapshot:
    """Durable bundle row plus its anchor rows."""

    bundle: ContextBundle
    anchors: tuple[ContextBundleAnchorRecord, ...]


_DUPLICATE_SQLSTATES = {"42P07", "42710", "23505"}
_PLATFORM_AUTHORITY_SCHEMA_FILENAME = "006_platform_authority_schema.sql"
_CONTEXT_BUNDLE_SCHEMA_MARKERS = ("context_bundles", "context_bundle_anchors")


def _is_duplicate_object_error(error: BaseException) -> bool:
    sqlstate = getattr(error, "sqlstate", None)
    if sqlstate in {"42P07", "42710"}:
        return True
    if sqlstate != "23505":
        return False
    detail = str(getattr(error, "detail", "") or "")
    message = str(error)
    return "pg_type_typname_nsp_index" in detail or "already exists" in message


@lru_cache(maxsize=1)
def _context_bundle_schema_statements() -> tuple[str, ...]:
    try:
        statements = workflow_migration_statements(_PLATFORM_AUTHORITY_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "context.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "context.schema_missing"
        )
        message = (
            "context bundle schema file did not contain executable statements"
            if reason_code == "context.schema_empty"
            else "context bundle schema file could not be read from the canonical workflow migration root"
        )
        raise ContextBundleRepositoryError(
            reason_code,
            message,
            details=exc.details,
        ) from exc

    statements = tuple(
        statement
        for statement in statements
        if any(marker in statement for marker in _CONTEXT_BUNDLE_SCHEMA_MARKERS)
    )
    if not statements:
        raise ContextBundleRepositoryError(
            "context.schema_missing",
            "canonical workflow migration packet does not define context bundle tables",
            details={"filename": _PLATFORM_AUTHORITY_SCHEMA_FILENAME},
        )
    return statements


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContextBundleRepositoryError(
            "context.invalid_bundle_record",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _canonical_json_text(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _bundle_identity(bundle: ContextBundle) -> dict[str, Any]:
    return {
        "bundle_hash": bundle.bundle_hash,
        "bundle_payload": _json_value(bundle.bundle_payload),
        "bundle_version": bundle.bundle_version,
        "context_bundle_id": bundle.context_bundle_id,
        "model_profile_id": bundle.model_profile_id,
        "provider_policy_id": bundle.provider_policy_id,
        "resolved_at": bundle.resolved_at.isoformat(),
        "run_id": bundle.run_id,
        "source_decision_refs": list(bundle.source_decision_refs),
        "workspace_ref": bundle.workspace_ref,
        "workflow_id": bundle.workflow_id,
    }


def _anchor_identity(anchor: ContextBundleAnchorRecord) -> dict[str, Any]:
    return {
        "anchor_kind": anchor.anchor_kind,
        "anchor_ref": anchor.anchor_ref,
        "content_hash": anchor.content_hash,
        "payload": _json_value(anchor.payload),
        "position_index": anchor.position_index,
    }


def _bundle_row_from_record(row: asyncpg.Record) -> ContextBundle:
    return ContextBundle(
        context_bundle_id=str(row["context_bundle_id"]),
        workflow_id=str(row["workflow_id"]),
        run_id=str(row["run_id"]),
        workspace_ref=str(row["workspace_ref"]),
        runtime_profile_ref=str(row["runtime_profile_ref"]),
        model_profile_id=str(row["model_profile_id"]),
        provider_policy_id=str(row["provider_policy_id"]),
        bundle_version=int(row["bundle_version"]),
        bundle_hash=str(row["bundle_hash"]),
        bundle_payload=_json_value(row["bundle_payload"]),
        source_decision_refs=tuple(str(ref) for ref in _json_value(row["source_decision_refs"])),
        resolved_at=row["resolved_at"],
    )


def _anchor_row_from_record(row: asyncpg.Record) -> ContextBundleAnchorRecord:
    return ContextBundleAnchorRecord(
        anchor_ref=str(row["anchor_ref"]),
        anchor_kind=str(row["anchor_kind"]),
        content_hash=str(row["content_hash"]),
        payload=_json_value(row["anchor_payload"]),
        position_index=int(row["position_index"]),
    )


def _bundle_anchor_id(*, context_bundle_id: str, anchor_kind: str, anchor_ref: str) -> str:
    return f"context_bundle_anchor:{context_bundle_id}:{anchor_kind}:{anchor_ref}"


def _bundle_payload(bundle: ContextBundle) -> dict[str, Any]:
    return {
        "bundle_version": bundle.bundle_version,
        "run_id": bundle.run_id,
        "runtime_profile": {
            "model_profile_id": bundle.model_profile_id,
            "provider_policy_id": bundle.provider_policy_id,
            "runtime_profile_ref": bundle.runtime_profile_ref,
        },
        "source_decision_refs": list(bundle.source_decision_refs),
        "workspace": {
            "repo_root": bundle.bundle_payload["workspace"]["repo_root"],
            "workdir": bundle.bundle_payload["workspace"]["workdir"],
            "workspace_ref": bundle.workspace_ref,
        },
        "workflow_id": bundle.workflow_id,
    }


async def bootstrap_context_bundle_schema(conn: asyncpg.Connection) -> None:
    """Create the canonical context bundle tables in an idempotent way."""

    async with conn.transaction():
        for statement in _context_bundle_schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise ContextBundleRepositoryError(
                    "context.schema_bootstrap_failed",
                    "failed to bootstrap the canonical context bundle schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


class PostgresContextBundleRepository:
    """Explicit Postgres repository for canonical context bundle facts."""

    def __init__(
        self,
        conn: asyncpg.Connection | None = None,
        *,
        database_url: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._conn = conn
        self._database_url = database_url
        self._env = env

    def _resolve_database_url(self) -> str:
        if self._database_url is not None:
            return _require_text(self._database_url, field_name="database_url")
        if self._env is None:
            raise ContextBundleRepositoryError(
                "context.config_missing",
                "database_url must be set when no explicit connection is provided",
            )
        database_url = self._env.get("WORKFLOW_DATABASE_URL")
        if database_url is None:
            raise ContextBundleRepositoryError(
                "context.config_missing",
                "WORKFLOW_DATABASE_URL must be set when no explicit connection is provided",
                details={"environment_variable": "WORKFLOW_DATABASE_URL"},
            )
        return _require_text(database_url, field_name="WORKFLOW_DATABASE_URL")

    @asynccontextmanager
    async def _connection(self):
        if self._conn is not None:
            yield self._conn
            return
        database_url = self._resolve_database_url()
        conn = await asyncpg.connect(database_url)
        try:
            yield conn
        finally:
            await conn.close()

    def load_context_bundle(self, *, context_bundle_id: str) -> ContextBundleSnapshot:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.load_context_bundle_async(context_bundle_id=context_bundle_id)
            )
        raise ContextBundleRepositoryError(
            "context.bundle_loop_active",
            "sync load_context_bundle() requires an explicit non-async call boundary",
            details={"context_bundle_id": context_bundle_id},
        )

    async def load_context_bundle_async(
        self,
        *,
        context_bundle_id: str,
    ) -> ContextBundleSnapshot:
        context_bundle_id = _require_text(
            context_bundle_id,
            field_name="context_bundle_id",
        )
        try:
            async with self._connection() as conn:
                async with conn.transaction():
                    bundle_row = await conn.fetchrow(
                        """
                        SELECT
                            context_bundle_id,
                            workflow_id,
                            run_id,
                            workspace_ref,
                            runtime_profile_ref,
                            model_profile_id,
                            provider_policy_id,
                            bundle_version,
                            bundle_hash,
                            bundle_payload,
                            source_decision_refs,
                            resolved_at
                        FROM context_bundles
                        WHERE context_bundle_id = $1
                        """,
                        context_bundle_id,
                    )
                    if bundle_row is None:
                        raise ContextBundleRepositoryError(
                            "context.bundle_unknown",
                            "missing authoritative context bundle",
                            details={"context_bundle_id": context_bundle_id},
                        )
                    anchor_rows = await conn.fetch(
                        """
                        SELECT
                            anchor_ref,
                            anchor_kind,
                            content_hash,
                            anchor_payload,
                            position_index
                        FROM context_bundle_anchors
                        WHERE context_bundle_id = $1
                        ORDER BY position_index
                        """,
                        context_bundle_id,
                    )
        except ContextBundleRepositoryError:
            raise
        except asyncpg.PostgresError as exc:
            raise ContextBundleRepositoryError(
                "context.bundle_read_failed",
                "failed to read context bundle authority",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "context_bundle_id": context_bundle_id,
                },
            ) from exc
        if not anchor_rows:
            raise ContextBundleRepositoryError(
                "context.bundle_anchor_missing",
                "stored context bundle is missing anchor rows",
                details={"context_bundle_id": context_bundle_id},
            )
        bundle = _bundle_row_from_record(bundle_row)
        anchors = tuple(_anchor_row_from_record(row) for row in anchor_rows)
        return ContextBundleSnapshot(bundle=bundle, anchors=anchors)

    def persist_context_bundle(
        self,
        *,
        bundle: ContextBundle,
        anchors: Sequence[ContextBundleAnchorRecord],
    ) -> ContextBundleSnapshot:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.persist_context_bundle_async(bundle=bundle, anchors=anchors)
            )
        raise ContextBundleRepositoryError(
            "context.bundle_loop_active",
            "sync persist_context_bundle() requires an explicit non-async call boundary",
            details={"context_bundle_id": bundle.context_bundle_id},
        )

    async def persist_context_bundle_async(
        self,
        *,
        bundle: ContextBundle,
        anchors: Sequence[ContextBundleAnchorRecord],
    ) -> ContextBundleSnapshot:
        context_bundle_id = _require_text(
            bundle.context_bundle_id,
            field_name="context_bundle_id",
        )
        workflow_id = _require_text(bundle.workflow_id, field_name="workflow_id")
        run_id = _require_text(bundle.run_id, field_name="run_id")
        workspace_ref = _require_text(bundle.workspace_ref, field_name="workspace_ref")
        runtime_profile_ref = _require_text(
            bundle.runtime_profile_ref,
            field_name="runtime_profile_ref",
        )
        model_profile_id = _require_text(
            bundle.model_profile_id,
            field_name="model_profile_id",
        )
        provider_policy_id = _require_text(
            bundle.provider_policy_id,
            field_name="provider_policy_id",
        )
        if bundle.bundle_version < 1:
            raise ContextBundleRepositoryError(
                "context.bundle_invalid",
                "bundle_version must be >= 1",
                details={"context_bundle_id": context_bundle_id},
            )
        if not anchors:
            raise ContextBundleRepositoryError(
                "context.bundle_anchor_missing",
                "cannot persist a bundle without anchor rows",
                details={"context_bundle_id": context_bundle_id},
            )

        normalized_anchors: list[ContextBundleAnchorRecord] = []
        seen_anchor_keys: set[tuple[str, str]] = set()
        seen_anchor_positions: set[int] = set()
        for index, anchor in enumerate(anchors):
            anchor_ref = _require_text(anchor.anchor_ref, field_name=f"anchors[{index}].anchor_ref")
            anchor_kind = _require_text(
                anchor.anchor_kind,
                field_name=f"anchors[{index}].anchor_kind",
            )
            content_hash = _require_text(
                anchor.content_hash,
                field_name=f"anchors[{index}].content_hash",
            )
            position_index = int(anchor.position_index)
            if position_index < 0:
                raise ContextBundleRepositoryError(
                    "context.anchor_invalid",
                    "anchor position_index must be non-negative",
                    details={
                        "context_bundle_id": context_bundle_id,
                        "position_index": position_index,
                    },
                )
            anchor_key = (anchor_kind, anchor_ref)
            if anchor_key in seen_anchor_keys:
                raise ContextBundleRepositoryError(
                    "context.anchor_duplicate",
                    "duplicate context bundle anchor rows are not allowed",
                    details={
                        "context_bundle_id": context_bundle_id,
                        "anchor_kind": anchor_kind,
                        "anchor_ref": anchor_ref,
                    },
                )
            seen_anchor_keys.add(anchor_key)
            if position_index in seen_anchor_positions:
                raise ContextBundleRepositoryError(
                    "context.anchor_duplicate",
                    "duplicate context bundle anchor positions are not allowed",
                    details={
                        "context_bundle_id": context_bundle_id,
                        "position_index": position_index,
                    },
                )
            seen_anchor_positions.add(position_index)
            normalized_anchors.append(
                ContextBundleAnchorRecord(
                    anchor_ref=anchor_ref,
                    anchor_kind=anchor_kind,
                    content_hash=content_hash,
                    payload=anchor.payload,
                    position_index=position_index,
                )
            )

        canonical_bundle_payload = _bundle_payload(bundle)
        try:
            bundle_payload_json = _canonical_json_text(canonical_bundle_payload)
            source_decision_refs_json = _canonical_json_text(
                list(bundle.source_decision_refs)
            )
        except (TypeError, ValueError) as exc:
            raise ContextBundleRepositoryError(
                "context.bundle_invalid",
                "context bundle payload must be JSON serializable",
                details={"context_bundle_id": context_bundle_id},
            ) from exc
        async with self._connection() as conn:
            async with conn.transaction():
                bundle_row = await conn.fetchrow(
                    """
                    INSERT INTO context_bundles (
                        context_bundle_id,
                        workflow_id,
                        run_id,
                        workspace_ref,
                        runtime_profile_ref,
                        model_profile_id,
                        provider_policy_id,
                        bundle_version,
                        bundle_hash,
                        bundle_payload,
                        source_decision_refs,
                        resolved_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12)
                    ON CONFLICT (context_bundle_id) DO NOTHING
                    RETURNING
                        context_bundle_id,
                        workflow_id,
                        run_id,
                        workspace_ref,
                        runtime_profile_ref,
                        model_profile_id,
                        provider_policy_id,
                        bundle_version,
                        bundle_hash,
                        bundle_payload,
                        source_decision_refs,
                        resolved_at
                    """,
                    context_bundle_id,
                    workflow_id,
                    run_id,
                    workspace_ref,
                    runtime_profile_ref,
                    model_profile_id,
                    provider_policy_id,
                    bundle.bundle_version,
                    bundle.bundle_hash,
                    bundle_payload_json,
                    source_decision_refs_json,
                    bundle.resolved_at,
                )
                if bundle_row is None:
                    existing_bundle_row = await conn.fetchrow(
                        """
                        SELECT
                            context_bundle_id,
                            workflow_id,
                            run_id,
                            workspace_ref,
                            runtime_profile_ref,
                            model_profile_id,
                            provider_policy_id,
                            bundle_version,
                            bundle_hash,
                            bundle_payload,
                            source_decision_refs,
                            resolved_at
                        FROM context_bundles
                        WHERE context_bundle_id = $1
                        """,
                        context_bundle_id,
                    )
                    if existing_bundle_row is None:
                        raise ContextBundleRepositoryError(
                            "context.bundle_write_failed",
                            "failed to persist context bundle",
                            details={"context_bundle_id": context_bundle_id},
                        )
                    existing_bundle = _bundle_row_from_record(existing_bundle_row)
                    if _bundle_identity(existing_bundle) != _bundle_identity(bundle):
                        raise ContextBundleRepositoryError(
                            "context.bundle_conflict",
                            "context bundle already exists with different canonical content",
                            details={"context_bundle_id": context_bundle_id},
                        )
                    bundle = existing_bundle
                else:
                    bundle = _bundle_row_from_record(bundle_row)

                for anchor in normalized_anchors:
                    try:
                        anchor_payload_json = _canonical_json_text(
                            _json_value(anchor.payload)
                        )
                    except (TypeError, ValueError) as exc:
                        raise ContextBundleRepositoryError(
                            "context.anchor_invalid",
                            "context bundle anchor payload must be JSON serializable",
                            details={
                                "context_bundle_id": context_bundle_id,
                                "anchor_kind": anchor.anchor_kind,
                                "anchor_ref": anchor.anchor_ref,
                            },
                        ) from exc
                    anchor_row = await conn.fetchrow(
                        """
                        INSERT INTO context_bundle_anchors (
                            context_bundle_anchor_id,
                            context_bundle_id,
                            anchor_ref,
                            anchor_kind,
                            content_hash,
                            anchor_payload,
                            position_index,
                            anchored_at
                        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
                        ON CONFLICT (context_bundle_id, anchor_kind, anchor_ref) DO NOTHING
                        RETURNING
                        anchor_ref,
                        anchor_kind,
                        content_hash,
                        anchor_payload,
                        position_index
                        """,
                        _bundle_anchor_id(
                            context_bundle_id=context_bundle_id,
                            anchor_kind=anchor.anchor_kind,
                            anchor_ref=anchor.anchor_ref,
                        ),
                        context_bundle_id,
                        anchor.anchor_ref,
                        anchor.anchor_kind,
                        anchor.content_hash,
                        anchor_payload_json,
                        anchor.position_index,
                        bundle.resolved_at,
                    )
                    if anchor_row is None:
                        existing_anchor_row = await conn.fetchrow(
                            """
                            SELECT
                                anchor_ref,
                                anchor_kind,
                                content_hash,
                                anchor_payload,
                                position_index
                            FROM context_bundle_anchors
                            WHERE context_bundle_id = $1
                              AND anchor_kind = $2
                              AND anchor_ref = $3
                            """,
                            context_bundle_id,
                            anchor.anchor_kind,
                            anchor.anchor_ref,
                        )
                        if existing_anchor_row is None:
                            raise ContextBundleRepositoryError(
                                "context.bundle_write_failed",
                                "failed to persist context bundle anchor",
                                details={
                                    "context_bundle_id": context_bundle_id,
                                    "anchor_kind": anchor.anchor_kind,
                                    "anchor_ref": anchor.anchor_ref,
                                },
                            )
                        existing_anchor = _anchor_row_from_record(existing_anchor_row)
                        if _anchor_identity(existing_anchor) != _anchor_identity(anchor):
                            raise ContextBundleRepositoryError(
                                "context.anchor_conflict",
                                "context bundle anchor already exists with different canonical content",
                                details={
                                    "context_bundle_id": context_bundle_id,
                                    "anchor_kind": anchor.anchor_kind,
                                    "anchor_ref": anchor.anchor_ref,
                                },
                            )

        return ContextBundleSnapshot(
            bundle=bundle,
            anchors=tuple(sorted(normalized_anchors, key=lambda anchor: anchor.position_index)),
        )


__all__ = [
    "ContextBundleAnchorRecord",
    "ContextBundleRepositoryError",
    "ContextBundleSnapshot",
    "PostgresContextBundleRepository",
    "bootstrap_context_bundle_schema",
]
