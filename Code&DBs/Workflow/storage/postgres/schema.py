"""Schema bootstrap and inspection for the Postgres control plane."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
import logging
import time
from typing import Any

import asyncpg

from storage._generated_workflow_migration_authority import (
    WORKFLOW_FULL_BOOTSTRAP_SEQUENCE as _GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE,
    WORKFLOW_MIGRATION_POLICIES as _GENERATED_WORKFLOW_MIGRATION_POLICIES,
    WORKFLOW_SCHEMA_READINESS_SEQUENCE as _GENERATED_WORKFLOW_SCHEMA_READINESS_SEQUENCE,
)
from storage.migrations import (
    WorkflowMigrationError,
    WorkflowMigrationExpectedObject,
    workflow_bootstrap_migration_sql_text,
    workflow_migration_expected_objects,
    workflow_migrations_root,
    workflow_migration_path,
    workflow_migration_sql_text,
    workflow_bootstrap_migration_statements,
    workflow_migration_statements,
)
from .validators import PostgresSchemaError

_CONTROL_PLANE_SCHEMA_FILENAME = "001_v1_control_plane.sql"
_SCHEMA_MIGRATIONS_FILENAME = "173_schema_migrations.sql"
_SCHEMA_MIGRATIONS_APPLIED_BY_BOOTSTRAP = "schema_bootstrap"
_SCHEMA_MIGRATIONS_ENSURE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename         text        NOT NULL PRIMARY KEY,
    content_sha256   text        NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now(),
    applied_by       text        NOT NULL,
    bootstrap_role   text        NOT NULL,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT schema_migrations_filename_nonblank
        CHECK (btrim(filename) <> ''),
    CONSTRAINT schema_migrations_sha256_shape
        CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT schema_migrations_bootstrap_role_check
        CHECK (bootstrap_role IN ('canonical', 'bootstrap_only'))
)
""".strip()
_DUPLICATE_SQLSTATES = {"42P07", "42701", "42710"}
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_SCHEMA_BOOTSTRAP_LOCK_POLL_INTERVAL_S = 0.25
_SCHEMA_BOOTSTRAP_WAIT_WARNING_THRESHOLD_S = 2.0
_SCHEMA_BOOTSTRAP_WAIT_LOG_INTERVAL_S = 10.0
_ROW_EXPECTATION_KEY_COLUMNS = {
    "authority_event_contracts": "event_contract_ref",
    "authority_object_registry": "object_ref",
    "data_dictionary_objects": "object_kind",
    "model_profile_candidate_bindings": "model_profile_candidate_binding_id",
    "model_profiles": "model_profile_id",
    "model_sync_config": "provider_slug",
    "operation_catalog_registry": "operation_name",
    "operation_catalog_source_policy_registry": "source_kind",
    "operator_decisions": "operator_decision_id",
    "platform_config": "config_key",
    "provider_concurrency": "provider_slug",
    "provider_cli_profiles": "provider_slug",
    "provider_lane_policy": "provider_slug",
    "provider_model_candidates": "candidate_ref",
    "provider_transport_admissions": "provider_transport_admission_id",
    "registry_native_runtime_profile_authority": "runtime_profile_ref",
    "registry_runtime_profile_authority": "runtime_profile_ref",
    "registry_sandbox_profile_authority": "sandbox_profile_ref",
    "registry_workspace_authority": "workspace_ref",
    "semantic_predicates": "predicate_slug",
    "surface_catalog_registry": "catalog_item_id",
    "ui_feature_flow_registry": "feature_id",
    "ui_surface_action_registry": "action_id",
    "verification_registry": "verification_ref",
    "verifier_registry": "verifier_ref",
    "workflow_definitions": "workflow_definition_id",
    "workflow_definition_nodes": "workflow_definition_node_id",
    "workflow_definition_edges": "workflow_definition_edge_id",
}
_STRUCTURAL_EXPECTED_OBJECT_TYPES = frozenset(
    {"table", "index", "column", "constraint", "function", "view", "trigger"}
)
_ABSENCE_EXPECTED_OBJECT_TYPE_PREFIX = "absent_"
_BOOTSTRAP_BASELINE_ANCHOR_OBJECTS = (
    WorkflowMigrationExpectedObject(object_type="table", object_name="workflows"),
    WorkflowMigrationExpectedObject(object_type="table", object_name="system_events"),
    WorkflowMigrationExpectedObject(object_type="table", object_name="maintenance_policies"),
)

logger = logging.getLogger(__name__)


def _postgres_error_annotation(exc: asyncpg.PostgresError) -> dict[str, Any]:
    """Attach Postgres error fields so CHECK / FK violations stay visible."""

    annotation: dict[str, Any] = {}
    for attr in (
        "message",
        "detail",
        "hint",
        "constraint_name",
        "schema_name",
        "table_name",
        "column_name",
        "datatype_name",
    ):
        val = getattr(exc, attr, None)
        if val not in (None, ""):
            annotation[f"postgres_{attr}"] = val
    return annotation


def _postgres_error_tail(exc: asyncpg.PostgresError) -> str:
    detail = getattr(exc, "detail", None)
    message = getattr(exc, "message", None) or str(exc)
    return (str(detail).strip() if detail else str(message).strip()) or str(exc)


def _postgres_bootstrap_failure_summary(filename: str, exc: asyncpg.PostgresError) -> str:
    return f"failed to bootstrap workflow migration {filename}: {_postgres_error_tail(exc)}"


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


@dataclass(frozen=True, slots=True)
class WorkflowMigrationAppliedRow:
    """One apply-tracking row recorded in ``schema_migrations``."""

    filename: str
    content_sha256: str
    applied_at: object  # datetime-like (asyncpg hands back a datetime)
    applied_by: str
    bootstrap_role: str


@dataclass(frozen=True, slots=True)
class WorkflowMigrationAudit:
    """Declared-vs-applied diff against ``schema_migrations``.

    ``declared`` is the canonical full-bootstrap sequence in manifest order.
    ``applied`` is the set of filenames with an apply-tracking row. Derived
    views:

    * ``missing``  — declared filenames with no apply-tracking row; the
      bootstrap may never have run against this database.
    * ``drifted`` — applied filenames whose on-disk sha256 no longer matches
      the recorded sha256; the file was edited after it was first applied.
    * ``extra``   — filenames in ``applied`` that are not in the current
      declared manifest; usually a rename/retire in the authority file.
    """

    declared: tuple[str, ...]
    applied: tuple[WorkflowMigrationAppliedRow, ...]
    missing: tuple[str, ...]
    drifted: tuple[WorkflowMigrationAppliedRow, ...]
    extra: tuple[WorkflowMigrationAppliedRow, ...]

    @property
    def is_clean(self) -> bool:
        return not (self.missing or self.drifted or self.extra)


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


def _is_comment_only_statement(statement: str) -> bool:
    return not _strip_leading_sql_comments(statement).strip()


def _schema_bootstrap_monotonic() -> float:
    return time.monotonic()


@lru_cache(maxsize=1)
def _full_workflow_migration_filenames() -> tuple[str, ...]:
    return _GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE


@lru_cache(maxsize=1)
def _workflow_schema_readiness_by_migration() -> tuple[
    tuple[str, tuple[WorkflowMigrationExpectedObject, ...]],
    ...,
]:
    workflow_migrations_root()
    return tuple(
        (
            filename,
            tuple(
                WorkflowMigrationExpectedObject(
                    object_type=object_type,
                    object_name=object_name,
                )
                for object_type, object_name in objects
            ),
        )
        for filename, objects in _GENERATED_WORKFLOW_SCHEMA_READINESS_SEQUENCE
    )


@lru_cache(maxsize=1)
def _workflow_schema_manifest_filenames() -> tuple[str, ...]:
    return tuple(filename for filename, _objects in _workflow_schema_readiness_by_migration())


def _absence_base_object_type(object_type: str) -> str | None:
    if not object_type.startswith(_ABSENCE_EXPECTED_OBJECT_TYPE_PREFIX):
        return None
    base_type = object_type[len(_ABSENCE_EXPECTED_OBJECT_TYPE_PREFIX) :]
    if base_type in _STRUCTURAL_EXPECTED_OBJECT_TYPES or base_type == "row":
        return base_type
    return None


async def _workflow_expected_object_exists(
    conn: asyncpg.Connection,
    expected: WorkflowMigrationExpectedObject,
    *,
    object_type: str | None = None,
) -> bool:
    effective_type = object_type or expected.object_type
    object_name = expected.object_name

    if effective_type == "table":
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_class AS cls
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = $1
              AND cls.relkind IN ('r', 'p')
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "view":
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_class AS cls
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = $1
              AND cls.relkind IN ('v', 'm')
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "index":
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_class AS cls
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = $1
              AND cls.relkind = 'i'
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "column":
        table_name, _, column_name = object_name.partition(".")
        if not table_name or not column_name:
            return False
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM information_schema.columns AS cols
            WHERE cols.table_schema = 'public'
              AND cols.table_name = $1
              AND cols.column_name = $2
            LIMIT 1
            """,
            table_name,
            column_name,
        )
        return row is not None
    if effective_type == "constraint":
        relation_name, _, constraint_name = object_name.partition(".")
        if relation_name and constraint_name:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM pg_catalog.pg_constraint AS con
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = con.connamespace
                LEFT JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = con.conrelid
                WHERE ns.nspname = 'public'
                  AND cls.relname = $1
                  AND con.conname = $2
                LIMIT 1
                """,
                relation_name,
                constraint_name,
            )
            return row is not None
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_constraint AS con
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = con.connamespace
            WHERE ns.nspname = 'public'
              AND con.conname = $1
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "function":
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_proc AS proc
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = proc.pronamespace
            WHERE ns.nspname = 'public'
              AND proc.proname = $1
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "trigger":
        relation_name, _, trigger_name = object_name.partition(".")
        if relation_name and trigger_name:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM pg_catalog.pg_trigger AS trg
                JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = trg.tgrelid
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND cls.relname = $1
                  AND trg.tgname = $2
                  AND NOT trg.tgisinternal
                LIMIT 1
                """,
                relation_name,
                trigger_name,
            )
            return row is not None
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM pg_catalog.pg_trigger AS trg
            JOIN pg_catalog.pg_class AS cls
                ON cls.oid = trg.tgrelid
            JOIN pg_catalog.pg_namespace AS ns
                ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND trg.tgname = $1
              AND NOT trg.tgisinternal
            LIMIT 1
            """,
            object_name,
        )
        return row is not None
    if effective_type == "row":
        table_name, _, row_key = object_name.partition(".")
        if not table_name or not row_key:
            return False
        key_column = _ROW_EXPECTATION_KEY_COLUMNS.get(table_name)
        if key_column is None:
            return False
        table_exists = await conn.fetchval(
            "SELECT to_regclass($1::text) IS NOT NULL",
            f"public.{table_name}",
        )
        if not table_exists:
            return False
        row = await conn.fetchrow(
            f"SELECT 1 FROM {table_name} WHERE {key_column} = $1 LIMIT 1",
            row_key,
        )
        return row is not None
    return False




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
    
    # Use session-level or transaction-level advisory lock to coordinate.
    # pg_advisory_xact_lock(bigint) waits until the lock is available
    # and releases it at the end of the transaction.
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1::bigint)",
        _SCHEMA_BOOTSTRAP_LOCK_ID,
    )
    
    elapsed_s = _schema_bootstrap_monotonic() - wait_started_at
    if elapsed_s > 1.0:
        logger.warning(
            "schema bootstrap advisory lock %s acquired after %.2fs wait",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
            elapsed_s,
        )
    return elapsed_s


def _workflow_migration_policy_for(filename: str) -> str:
    """Resolve the policy bucket for a migration, defaulting to canonical.

    Used to populate ``schema_migrations.bootstrap_role`` when recording an
    apply. Must match the policy bucket at apply time; fall back to
    ``canonical`` when unclassified (only reachable for files outside the
    generated authority, which should already have been rejected upstream).
    """

    policy = _GENERATED_WORKFLOW_MIGRATION_POLICIES.get(filename)
    if policy in {"canonical", "bootstrap_only"}:
        return policy
    return "canonical"


async def _ensure_schema_migrations_table(conn: asyncpg.Connection) -> None:
    """Eagerly materialize the apply-tracking table before any apply-tracking INSERT.

    The 173_schema_migrations.sql migration creates this table as part of the
    canonical manifest. But migrations that run earlier in the same bootstrap
    pass also want to record their apply, so we create it inline first and
    trust CREATE TABLE IF NOT EXISTS to be idempotent. 173 itself is a no-op
    once this has run.
    """

    try:
        async with conn.transaction():
            await conn.execute(_SCHEMA_MIGRATIONS_ENSURE_DDL)
    except asyncpg.PostgresError as exc:
        if _is_duplicate_object_error(exc):
            return
        raise PostgresSchemaError(
            "postgres.schema_bootstrap_failed",
            f"failed to ensure schema_migrations apply-tracking table exists: {_postgres_error_tail(exc)}",
            details={
                "sqlstate": getattr(exc, "sqlstate", None),
                **_postgres_error_annotation(exc),
            },
        ) from exc


async def record_migration_apply(
    conn: asyncpg.Connection,
    filename: str,
    *,
    applied_by: str = _SCHEMA_MIGRATIONS_APPLIED_BY_BOOTSTRAP,
) -> None:
    """Record a successful migration apply.

    Public manual-apply API. When a migration runs out-of-band (e.g. an
    operator applies a single file via psql or a one-off script), the
    schema_migrations row must still be written so future drift checks
    succeed and ``inspect_workflow_schema`` reports the right state. The
    schema_migrations table requires both ``content_sha256`` and
    ``bootstrap_role``; manual INSERT attempts hit two NOT NULL violations
    in sequence (BUG-431B3436). This helper fills both correctly.

    Computes sha256 of the migration's SQL text at apply time so later drift
    between disk content and recorded hash is detectable. Uses ON CONFLICT DO
    UPDATE because re-applies (idempotent DDL) should refresh the recorded
    sha and applied_at instead of producing duplicate rows.
    """

    try:
        sql_text = workflow_bootstrap_migration_sql_text(filename)
    except WorkflowMigrationError as exc:
        # The statements for this file already applied. If we cannot re-read
        # the text for sha hashing, fall back to logging and continue — the
        # apply completed and failing here would leave the system stuck.
        logger.warning(
            "schema_migrations apply-tracking sha read failed for %s (%s); skipping row",
            filename,
            exc.reason_code,
        )
        return
    sha256 = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
    policy = _workflow_migration_policy_for(filename)
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO schema_migrations (
                    filename, content_sha256, applied_by, bootstrap_role
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (filename) DO UPDATE
                SET content_sha256 = EXCLUDED.content_sha256,
                    applied_at     = now(),
                    applied_by     = EXCLUDED.applied_by,
                    bootstrap_role = EXCLUDED.bootstrap_role
                """,
                filename,
                sha256,
                applied_by,
                policy,
            )
    except asyncpg.PostgresError as exc:
        # Apply-tracking must not block schema bootstrap on first-install paths
        # where schema_migrations itself has yet to exist. Log and continue; a
        # subsequent bootstrap pass will land the row once the table exists.
        logger.warning(
            "schema_migrations apply-tracking insert failed for %s (sqlstate=%s); continuing",
            filename,
            getattr(exc, "sqlstate", None),
        )


async def _bootstrap_migration(conn: asyncpg.Connection, filename: str) -> None:
    """Apply one migration file all-or-nothing.

    The whole migration runs in a single outer transaction so a failure on
    statement N rolls back statements 1..N-1. Each statement runs inside a
    savepoint (asyncpg nests transactions as savepoints) so duplicate-object
    errors can be skipped without poisoning the outer transaction
    (BUG-25C5319C). The schema_migrations row is recorded as the last
    in-transaction step — if it lands, every statement landed too.
    """
    await _ensure_schema_migrations_table(conn)
    async with conn.transaction():
        for statement in workflow_bootstrap_migration_statements(filename):
            if _is_comment_only_statement(statement):
                continue
            if _is_transaction_wrapper_statement(statement):
                continue
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                details = {
                    "filename": filename,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "statement": statement[:120],
                }
                details.update(_postgres_error_annotation(exc))
                raise PostgresSchemaError(
                    "postgres.schema_bootstrap_failed",
                    _postgres_bootstrap_failure_summary(filename, exc),
                    details=details,
                ) from exc
        await _record_migration_apply(conn, filename)


# Backwards-compat alias — historic callers (tests, scripts) reference the
# private name. Public API is ``record_migration_apply``. Internal callers
# go through the underscore alias so monkeypatch-based tests can intercept
# the apply-tracking step without also intercepting external surfaces.
_record_migration_apply = record_migration_apply


async def _record_bootstrapped_schema_migration_rows(
    conn: asyncpg.Connection,
    filenames: tuple[str, ...],
) -> None:
    """Backfill apply-tracking rows when expected schema authority is already present."""

    for filename in filenames:
        await _record_migration_apply(
            conn,
            filename,
            applied_by="schema_ledger_backfill",
        )


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
        migration_audit = await workflow_migration_audit(conn)
        if not migration_audit.missing:
            return

    async with conn.transaction():
        await _acquire_schema_bootstrap_lock(conn)
        # Re-check after taking the lock in case another process finished the
        # bootstrap while we were waiting.
        readiness = await inspect_workflow_schema(conn)
        if readiness.is_bootstrapped:
            migration_audit = await workflow_migration_audit(conn)
            if migration_audit.missing:
                await _record_bootstrapped_schema_migration_rows(
                    conn,
                    migration_audit.missing,
                )
            return
        control_readiness = await inspect_control_plane_schema(conn)
        if not control_readiness.is_bootstrapped:
            for filename in _full_workflow_migration_filenames():
                await _bootstrap_migration(conn, filename)
            return
        if await _bootstrap_baseline_anchor_is_missing(conn):
            for filename in _full_workflow_migration_filenames():
                await _bootstrap_migration(conn, filename)
            return
        for filename in _workflow_schema_manifest_filenames():
            missing_objects = readiness.missing_by_migration.get(filename, ())
            if not missing_objects:
                continue
            await _bootstrap_migration(conn, filename)


async def _bootstrap_baseline_anchor_is_missing(conn: asyncpg.Connection) -> bool:
    for expected in _BOOTSTRAP_BASELINE_ANCHOR_OBJECTS:
        if not await _workflow_expected_object_exists(conn, expected):
            return True
    return False


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
                  OR (expected.object_type = 'view' AND cls.relkind IN ('v', 'm'))
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
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = con.connamespace
                LEFT JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = con.conrelid
                WHERE ns.nspname = 'public'
                  AND (
                      (
                          position('.' in expected.object_name) > 0
                          AND cls.relname = split_part(expected.object_name, '.', 1)
                          AND con.conname = split_part(expected.object_name, '.', 2)
                      )
                      OR (
                          position('.' in expected.object_name) = 0
                          AND con.conname = expected.object_name
                      )
                  )
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
        AND NOT (
            expected.object_type = 'trigger'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_trigger AS trg
                JOIN pg_catalog.pg_class AS cls
                    ON cls.oid = trg.tgrelid
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND NOT trg.tgisinternal
                  AND (
                      (
                          position('.' in expected.object_name) > 0
                          AND cls.relname = split_part(expected.object_name, '.', 1)
                          AND trg.tgname = split_part(expected.object_name, '.', 2)
                      )
                      OR (
                          position('.' in expected.object_name) = 0
                          AND trg.tgname = expected.object_name
                      )
                  )
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

    expected_by_migration = dict(_workflow_schema_readiness_by_migration())
    expected_objects = tuple(
        obj
        for _filename, objects in _workflow_schema_readiness_by_migration()
        for obj in objects
    )
    if not expected_objects:
        return WorkflowSchemaReadiness(
            expected_objects=(),
            missing_objects=(),
            missing_by_migration={},
        )
    structural_expected_objects = tuple(
        obj for obj in expected_objects if obj.object_type in _STRUCTURAL_EXPECTED_OBJECT_TYPES
    )
    row_expected_objects = tuple(obj for obj in expected_objects if obj.object_type == "row")
    absent_expected_objects = tuple(
        obj for obj in expected_objects if _absence_base_object_type(obj.object_type) is not None
    )
    unsupported_expected_objects = tuple(
        obj
        for obj in expected_objects
        if obj.object_type not in _STRUCTURAL_EXPECTED_OBJECT_TYPES
        and obj.object_type != "row"
        and _absence_base_object_type(obj.object_type) is None
    )
    expected_payload = json.dumps(
        [
            {
                "object_type": item.object_type,
                "object_name": item.object_name,
            }
            for item in structural_expected_objects
        ]
    )
    structural_missing_objects: tuple[WorkflowMigrationExpectedObject, ...] = ()
    if structural_expected_objects:
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
                      OR (expected.object_type = 'view' AND cls.relkind IN ('v', 'm'))
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
                    JOIN pg_catalog.pg_namespace AS ns
                        ON ns.oid = con.connamespace
                    LEFT JOIN pg_catalog.pg_class AS cls
                        ON cls.oid = con.conrelid
                    WHERE ns.nspname = 'public'
                      AND (
                          (
                              position('.' in expected.object_name) > 0
                              AND cls.relname = split_part(expected.object_name, '.', 1)
                              AND con.conname = split_part(expected.object_name, '.', 2)
                          )
                          OR (
                              position('.' in expected.object_name) = 0
                              AND con.conname = expected.object_name
                          )
                      )
                )
            )
            AND NOT (
                expected.object_type = 'trigger'
                AND EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_trigger AS trg
                    JOIN pg_catalog.pg_class AS cls
                        ON cls.oid = trg.tgrelid
                    JOIN pg_catalog.pg_namespace AS ns
                        ON ns.oid = cls.relnamespace
                    WHERE ns.nspname = 'public'
                      AND NOT trg.tgisinternal
                      AND (
                          (
                              position('.' in expected.object_name) > 0
                              AND cls.relname = split_part(expected.object_name, '.', 1)
                              AND trg.tgname = split_part(expected.object_name, '.', 2)
                          )
                          OR (
                              position('.' in expected.object_name) = 0
                              AND trg.tgname = expected.object_name
                          )
                      )
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
        structural_missing_objects = tuple(
            WorkflowMigrationExpectedObject(
                object_type=str(row["object_type"]),
                object_name=str(row["object_name"]),
            )
            for row in rows
        )

    row_missing_objects: list[WorkflowMigrationExpectedObject] = []
    row_expectations_by_table: dict[str, list[tuple[str, WorkflowMigrationExpectedObject]]] = {}
    for item in row_expected_objects:
        table_name, _, row_key = item.object_name.partition(".")
        if not table_name or not row_key:
            row_missing_objects.append(item)
            continue
        row_expectations_by_table.setdefault(table_name, []).append((row_key, item))
    for table_name, entries in row_expectations_by_table.items():
        key_column = _ROW_EXPECTATION_KEY_COLUMNS.get(table_name)
        if key_column is None:
            row_missing_objects.extend(item for _row_key, item in entries)
            continue
        table_exists = await conn.fetchval(
            "SELECT to_regclass($1::text) IS NOT NULL",
            f"public.{table_name}",
        )
        if not table_exists:
            row_missing_objects.extend(item for _row_key, item in entries)
            continue
        expected_row_keys = [row_key for row_key, _item in entries]
        rows = await conn.fetch(
            f"SELECT {key_column} AS row_key FROM {table_name} WHERE {key_column} = ANY($1::text[])",
            expected_row_keys,
        )
        existing_row_keys = {str(row["row_key"]) for row in rows}
        row_missing_objects.extend(
            item for row_key, item in entries if row_key not in existing_row_keys
        )

    absent_missing_objects: list[WorkflowMigrationExpectedObject] = []
    for item in absent_expected_objects:
        base_type = _absence_base_object_type(item.object_type)
        if base_type is None:
            absent_missing_objects.append(item)
            continue
        if await _workflow_expected_object_exists(conn, item, object_type=base_type):
            absent_missing_objects.append(item)

    missing_name_pairs = {
        (obj.object_type, obj.object_name)
        for obj in (
            *structural_missing_objects,
            *row_missing_objects,
            *absent_missing_objects,
            *unsupported_expected_objects,
        )
    }
    missing_objects = tuple(
        obj
        for obj in expected_objects
        if (obj.object_type, obj.object_name) in missing_name_pairs
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


async def workflow_migration_audit(conn: asyncpg.Connection) -> WorkflowMigrationAudit:
    """Diff the declared full-bootstrap manifest against ``schema_migrations``.

    Returns a structured audit report. If the apply-tracking table does not
    exist yet (pre-173 databases), the audit reports every declared migration
    as ``missing`` with an empty ``applied`` list rather than erroring; that
    is the signal to operators that apply-tracking has never been wired.
    """

    declared = tuple(_GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE)
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class cls
            JOIN pg_catalog.pg_namespace ns
              ON ns.oid = cls.relnamespace
            WHERE ns.nspname = 'public'
              AND cls.relname = 'schema_migrations'
              AND cls.relkind IN ('r', 'p')
        )
        """
    )
    if not table_exists:
        return WorkflowMigrationAudit(
            declared=declared,
            applied=(),
            missing=declared,
            drifted=(),
            extra=(),
        )

    rows = await conn.fetch(
        """
        SELECT filename, content_sha256, applied_at, applied_by, bootstrap_role
        FROM schema_migrations
        ORDER BY filename
        """
    )
    applied_rows = tuple(
        WorkflowMigrationAppliedRow(
            filename=str(row["filename"]),
            content_sha256=str(row["content_sha256"]),
            applied_at=row["applied_at"],
            applied_by=str(row["applied_by"]),
            bootstrap_role=str(row["bootstrap_role"]),
        )
        for row in rows
    )
    applied_by_name = {row.filename: row for row in applied_rows}
    declared_set = set(declared)

    missing = tuple(
        filename for filename in declared if filename not in applied_by_name
    )
    extra = tuple(
        row for row in applied_rows if row.filename not in declared_set
    )

    drifted_list: list[WorkflowMigrationAppliedRow] = []
    for filename in declared:
        row = applied_by_name.get(filename)
        if row is None:
            continue
        try:
            sql_text = workflow_bootstrap_migration_sql_text(filename)
        except WorkflowMigrationError:
            # Declared but missing on disk — reported via the manifest check
            # elsewhere; do not double-report here.
            continue
        expected_sha = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
        if expected_sha != row.content_sha256:
            drifted_list.append(row)
    drifted = tuple(drifted_list)

    return WorkflowMigrationAudit(
        declared=declared,
        applied=applied_rows,
        missing=missing,
        drifted=drifted,
        extra=extra,
    )
