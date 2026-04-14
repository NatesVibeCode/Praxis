"""Schema bootstrap and inspection for the Postgres control plane."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
import json
import logging
import time

import asyncpg

from storage.migrations import (
    WorkflowMigrationError,
    WorkflowMigrationExpectedObject,
    workflow_migration_manifest,
    workflow_migration_expected_objects,
    workflow_migrations_root,
    workflow_migration_path,
    workflow_migration_sql_text,
    workflow_migration_statements,
)
from .validators import PostgresSchemaError

_CONTROL_PLANE_SCHEMA_FILENAME = "001_v1_control_plane.sql"
_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_SCHEMA_BOOTSTRAP_LOCK_POLL_INTERVAL_S = 0.25
_SCHEMA_BOOTSTRAP_WAIT_WARNING_THRESHOLD_S = 2.0
_SCHEMA_BOOTSTRAP_WAIT_LOG_INTERVAL_S = 10.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ControlPlaneSchemaReadiness:
    """Canonical expected-object readiness for the v1 control-plane schema."""

    expected_objects: tuple[WorkflowMigrationExpectedObject, ...]
    missing_objects: tuple[WorkflowMigrationExpectedObject, ...]

    @property
    def is_bootstrapped(self) -> bool:
        return not self.missing_objects

    @property
    def relation_names(self) -> tuple[str, ...]:
        return tuple(item.object_name for item in self.expected_objects)

    @property
    def missing_relations(self) -> tuple[str, ...]:
        return tuple(item.object_name for item in self.missing_objects)


@dataclass(frozen=True, slots=True)
class WorkflowSchemaReadiness:
    """Canonical expected-object readiness for the full workflow schema."""

    expected_objects: tuple[WorkflowMigrationExpectedObject, ...]
    missing_objects: tuple[WorkflowMigrationExpectedObject, ...]
    missing_by_migration: dict[str, tuple[WorkflowMigrationExpectedObject, ...]]

    @property
    def is_bootstrapped(self) -> bool:
        return not self.missing_objects

    @property
    def missing_relations(self) -> tuple[str, ...]:
        return tuple(item.object_name for item in self.missing_objects)


def _control_plane_schema_path():
    try:
        return workflow_migration_path(_CONTROL_PLANE_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        raise PostgresSchemaError(
            "postgres.schema_missing",
            "control-plane schema file could not be resolved from the canonical workflow migration root",
            details=exc.details,
        ) from exc


@lru_cache(maxsize=1)
def _control_plane_schema_sql_text() -> str:
    try:
        return workflow_migration_sql_text(_CONTROL_PLANE_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        raise PostgresSchemaError(
            "postgres.schema_missing",
            "control-plane schema file could not be read from the canonical workflow migration root",
            details=exc.details,
        ) from exc


@lru_cache(maxsize=1)
def _control_plane_schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_CONTROL_PLANE_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "postgres.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "postgres.schema_missing"
        )
        message = (
            "control-plane schema file did not contain executable statements"
            if reason_code == "postgres.schema_empty"
            else "control-plane schema file could not be resolved from the canonical workflow migration root"
        )
        raise PostgresSchemaError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


@lru_cache(maxsize=1)
def _control_plane_schema_expected_objects() -> tuple[WorkflowMigrationExpectedObject, ...]:
    try:
        return workflow_migration_expected_objects(_CONTROL_PLANE_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "postgres.schema_empty"
            if exc.reason_code == "workflow.migration_expected_objects_empty"
            else "postgres.schema_missing"
        )
        message = (
            "control-plane schema expected-object contract is empty"
            if reason_code == "postgres.schema_empty"
            else "control-plane schema expected-object contract could not be resolved from the canonical workflow migration root"
        )
        raise PostgresSchemaError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


def _strip_leading_sql_comments(statement: str) -> str:
    text = statement.lstrip()
    while text:
        if text.startswith("--"):
            newline_index = text.find("\n")
            if newline_index == -1:
                return ""
            text = text[newline_index + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            comment_end = text.find("*/", 2)
            if comment_end == -1:
                return ""
            text = text[comment_end + 2 :].lstrip()
            continue
        break
    return text


def _is_transaction_wrapper_statement(statement: str) -> bool:
    normalized = _strip_leading_sql_comments(statement).strip().rstrip(";").strip().lower()
    return normalized in {"begin", "begin transaction", "commit"}


def _schema_bootstrap_monotonic() -> float:
    return time.monotonic()


@lru_cache(maxsize=1)
def _full_workflow_migration_filenames() -> tuple[str, ...]:
    root = workflow_migrations_root()
    return tuple(sorted(path.name for path in root.glob("*.sql")))


async def _schema_bootstrap_lock_holder_details(
    conn: asyncpg.Connection,
) -> str:
    row = await conn.fetchrow(
        """
        SELECT
            a.pid,
            NULLIF(a.application_name, '') AS application_name,
            a.state,
            a.wait_event_type,
            a.wait_event,
            EXTRACT(EPOCH FROM (clock_timestamp() - a.xact_start)) AS xact_age_s,
            LEFT(COALESCE(a.query, ''), 240) AS query_text
        FROM pg_locks AS l
        JOIN pg_stat_activity AS a
            ON a.pid = l.pid
        WHERE l.locktype = 'advisory'
          AND l.classid = 0
          AND l.objid = $1
          AND l.granted
        ORDER BY a.xact_start NULLS LAST, a.backend_start NULLS LAST, a.pid
        LIMIT 1
        """,
        _SCHEMA_BOOTSTRAP_LOCK_ID,
    )
    if row is None:
        return "holder=unknown"

    application_name = str(row["application_name"] or "unknown")
    state = str(row["state"] or "unknown")
    wait_event_type = str(row["wait_event_type"] or "-")
    wait_event = str(row["wait_event"] or "-")
    xact_age_raw = row["xact_age_s"]
    try:
        xact_age_s = 0.0 if xact_age_raw is None else float(xact_age_raw)
    except (TypeError, ValueError):
        xact_age_s = 0.0
    query_text = " ".join(str(row["query_text"] or "").split())
    if not query_text:
        query_text = "<empty>"
    return (
        f"holder_pid={row['pid']} application_name={application_name} "
        f"state={state} wait_event_type={wait_event_type} wait_event={wait_event} "
        f"xact_age_s={xact_age_s:.2f} query={query_text}"
    )


async def _acquire_schema_bootstrap_lock(conn: asyncpg.Connection) -> float:
    wait_started_at = _schema_bootstrap_monotonic()
    warned = False
    next_log_at = _SCHEMA_BOOTSTRAP_WAIT_WARNING_THRESHOLD_S

    while True:
        acquired = bool(
            await conn.fetchval(
                "SELECT pg_try_advisory_xact_lock($1::bigint)",
                _SCHEMA_BOOTSTRAP_LOCK_ID,
            )
        )
        elapsed_s = _schema_bootstrap_monotonic() - wait_started_at
        if acquired:
            if warned:
                logger.warning(
                    "schema bootstrap advisory lock %s acquired after %.2fs wait",
                    _SCHEMA_BOOTSTRAP_LOCK_ID,
                    elapsed_s,
                )
            return elapsed_s

        if elapsed_s >= next_log_at:
            holder_details = await _schema_bootstrap_lock_holder_details(conn)
            logger.warning(
                "waiting %.2fs for schema bootstrap advisory lock %s; %s",
                elapsed_s,
                _SCHEMA_BOOTSTRAP_LOCK_ID,
                holder_details,
            )
            warned = True
            next_log_at = elapsed_s + _SCHEMA_BOOTSTRAP_WAIT_LOG_INTERVAL_S

        await asyncio.sleep(_SCHEMA_BOOTSTRAP_LOCK_POLL_INTERVAL_S)


async def _bootstrap_migration(conn: asyncpg.Connection, filename: str) -> None:
    for statement in workflow_migration_statements(filename):
        if _is_transaction_wrapper_statement(statement):
            continue
        try:
            async with conn.transaction():
                await conn.execute(statement)
        except asyncpg.PostgresError as exc:
            if _is_duplicate_object_error(exc):
                continue
            raise PostgresSchemaError(
                "postgres.schema_bootstrap_failed",
                f"failed to bootstrap workflow migration {filename}",
                details={
                    "filename": filename,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "statement": statement[:120],
                },
            ) from exc


async def bootstrap_control_plane_schema(conn: asyncpg.Connection) -> None:
    """Apply the v1 control-plane schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        await _bootstrap_migration(conn, _CONTROL_PLANE_SCHEMA_FILENAME)


async def bootstrap_workflow_schema(conn: asyncpg.Connection) -> None:
    """Apply the full canonical workflow schema in manifest order."""

    # Fast path: if the schema is already complete, do not queue on the bootstrap
    # advisory lock. Surface startup hits this path frequently, and blocking on a
    # stale bootstrap holder when no schema work is needed makes healthy servers
    # look broken.
    readiness = await inspect_workflow_schema(conn)
    if readiness.is_bootstrapped:
        return

    async with conn.transaction():
        await _acquire_schema_bootstrap_lock(conn)
        # Re-check after taking the lock in case another process finished the
        # bootstrap while we were waiting.
        readiness = await inspect_workflow_schema(conn)
        if readiness.is_bootstrapped:
            return
        control_readiness = await inspect_control_plane_schema(conn)
        if not control_readiness.is_bootstrapped:
            for filename in _full_workflow_migration_filenames():
                await _bootstrap_migration(conn, filename)
            return
        for entry in workflow_migration_manifest():
            missing_objects = readiness.missing_by_migration.get(entry.filename, ())
            if not missing_objects:
                continue
            await _bootstrap_migration(conn, entry.filename)


async def inspect_control_plane_schema(
    conn: asyncpg.Connection,
) -> ControlPlaneSchemaReadiness:
    """Inspect whether every canonical control-plane object exists."""

    expected_objects = _control_plane_schema_expected_objects()
    expected_payload = json.dumps(
        [
            {"object_type": item.object_type, "object_name": item.object_name}
            for item in expected_objects
        ]
    )
    rows = await conn.fetch(
        """
        WITH expected AS (
            SELECT
                item->>'object_type' AS object_type,
                item->>'object_name' AS object_name
            FROM jsonb_array_elements($1::jsonb) AS item
        )
        SELECT expected.object_type, expected.object_name
        FROM expected
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS cls
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = expected.object_name
              AND (
                  (expected.object_type = 'table' AND cls.relkind IN ('r', 'p'))
                  OR (expected.object_type = 'index' AND cls.relkind = 'i')
              )
        )
        AND NOT (
            expected.object_type = 'column'
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns AS cols
                WHERE cols.table_schema = 'public'
                  AND cols.table_name = split_part(expected.object_name, '.', 1)
                  AND cols.column_name = split_part(expected.object_name, '.', 2)
            )
        )
        AND NOT (
            expected.object_type = 'constraint'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint AS con
                JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = con.conrelid
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND cls.relname = split_part(expected.object_name, '.', 1)
                  AND con.conname = split_part(expected.object_name, '.', 2)
            )
        )
        ORDER BY expected.object_type, expected.object_name
        """,
        expected_payload,
    )
    missing_objects = tuple(
        WorkflowMigrationExpectedObject(
            object_type=str(row["object_type"]),
            object_name=str(row["object_name"]),
        )
        for row in rows
    )
    return ControlPlaneSchemaReadiness(
        expected_objects=expected_objects,
        missing_objects=missing_objects,
    )


async def inspect_workflow_schema(
    conn: asyncpg.Connection,
) -> WorkflowSchemaReadiness:
    """Inspect whether every canonical manifest object exists."""

    manifest = workflow_migration_manifest()
    expected_by_migration: dict[str, tuple[WorkflowMigrationExpectedObject, ...]] = {}
    for entry in manifest:
        try:
            expected_objects = workflow_migration_expected_objects(entry.filename)
        except WorkflowMigrationError as exc:
            if exc.reason_code in {
                "workflow.migration_expected_objects_missing",
                "workflow.migration_expected_objects_empty",
            }:
                continue
            raise PostgresSchemaError(
                "postgres.schema_missing",
                "workflow schema expected-object contract could not be resolved",
                details={
                    "filename": entry.filename,
                    **(exc.details or {}),
                },
            ) from exc
        if expected_objects:
            expected_by_migration[entry.filename] = expected_objects
    expected_objects = tuple(
        obj
        for entry in manifest
        for obj in expected_by_migration.get(entry.filename, ())
    )
    if not expected_objects:
        return WorkflowSchemaReadiness(
            expected_objects=(),
            missing_objects=(),
            missing_by_migration={},
        )
    expected_payload = json.dumps(
        [
            {
                "object_type": item.object_type,
                "object_name": item.object_name,
            }
            for item in expected_objects
        ]
    )
    rows = await conn.fetch(
        """
        WITH expected AS (
            SELECT
                item->>'object_type' AS object_type,
                item->>'object_name' AS object_name
            FROM jsonb_array_elements($1::jsonb) AS item
        )
        SELECT expected.object_type, expected.object_name
        FROM expected
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS cls
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = expected.object_name
              AND (
                  (expected.object_type = 'table' AND cls.relkind IN ('r', 'p'))
                  OR (expected.object_type = 'index' AND cls.relkind = 'i')
              )
        )
        AND NOT (
            expected.object_type = 'column'
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns AS cols
                WHERE cols.table_schema = 'public'
                  AND cols.table_name = split_part(expected.object_name, '.', 1)
                  AND cols.column_name = split_part(expected.object_name, '.', 2)
            )
        )
        AND NOT (
            expected.object_type = 'constraint'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint AS con
                JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = con.conrelid
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND cls.relname = split_part(expected.object_name, '.', 1)
                  AND con.conname = split_part(expected.object_name, '.', 2)
            )
        )
        AND NOT (
            expected.object_type = 'function'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_proc AS proc
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = proc.pronamespace
                WHERE ns.nspname = 'public'
                  AND proc.proname = expected.object_name
            )
        )
        ORDER BY expected.object_type, expected.object_name
        """,
        expected_payload,
    )
    missing_objects = tuple(
        WorkflowMigrationExpectedObject(
            object_type=str(row["object_type"]),
            object_name=str(row["object_name"]),
        )
        for row in rows
    )
    missing_names = {(obj.object_type, obj.object_name) for obj in missing_objects}
    missing_by_migration = {
        filename: tuple(
            obj
            for obj in objects
            if (obj.object_type, obj.object_name) in missing_names
        )
        for filename, objects in expected_by_migration.items()
    }
    missing_by_migration = {
        filename: objects
        for filename, objects in missing_by_migration.items()
        if objects
    }
    return WorkflowSchemaReadiness(
        expected_objects=expected_objects,
        missing_objects=missing_objects,
        missing_by_migration=missing_by_migration,
    )
