"""Raw Postgres repository for semantic predicate and assertion authority."""

from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from typing import Any, cast

import asyncpg

from runtime.semantic_assertions import (
    SUPPORTED_CARDINALITY_MODES,
    SemanticAssertionError,
    SemanticAssertionRecord,
    SemanticPredicateRecord,
    normalize_semantic_assertion_record,
    normalize_semantic_predicate_record,
    project_semantic_assertion,
    project_semantic_predicate,
)
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_DUPLICATE_SQLSTATES = {"42P07", "42701", "42710"}
_SCHEMA_FILENAME = "146_semantic_assertion_substrate.sql"


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "semantic_assertion.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "semantic_assertion.schema_missing"
        )
        message = (
            "semantic assertion schema file did not contain executable statements"
            if reason_code == "semantic_assertion.schema_empty"
            else "semantic assertion schema file could not be resolved from the canonical workflow migration root"
        )
        raise SemanticAssertionError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


async def bootstrap_semantic_assertion_repository_schema(conn: asyncpg.Connection) -> None:
    """Apply the semantic assertion schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        for statement in _schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise SemanticAssertionError(
                    "semantic_assertion.schema_bootstrap_failed",
                    "failed to bootstrap the semantic assertion schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


class PostgresSemanticAssertionRepository:
    """Explicit Postgres-backed repository for semantic authority rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def load_predicate(
        self,
        *,
        predicate_slug: str,
    ) -> SemanticPredicateRecord | None:
        row = await self._conn.fetchrow(
            """
            SELECT
                predicate_slug,
                predicate_status,
                subject_kind_allowlist,
                object_kind_allowlist,
                cardinality_mode,
                description,
                created_at,
                updated_at
            FROM semantic_predicates
            WHERE predicate_slug = $1
            LIMIT 1
            """,
            predicate_slug,
        )
        if row is None:
            return None
        return project_semantic_predicate(cast(dict[str, Any], dict(row)))

    async def upsert_predicate(
        self,
        *,
        predicate: SemanticPredicateRecord,
    ) -> SemanticPredicateRecord:
        normalized = normalize_semantic_predicate_record(predicate)
        row = await self._conn.fetchrow(
            """
            INSERT INTO semantic_predicates (
                predicate_slug,
                predicate_status,
                subject_kind_allowlist,
                object_kind_allowlist,
                cardinality_mode,
                description,
                created_at,
                updated_at
            ) VALUES (
                $1,
                $2,
                $3::jsonb,
                $4::jsonb,
                $5,
                $6,
                $7,
                $8
            )
            ON CONFLICT (predicate_slug) DO UPDATE SET
                predicate_status = EXCLUDED.predicate_status,
                subject_kind_allowlist = EXCLUDED.subject_kind_allowlist,
                object_kind_allowlist = EXCLUDED.object_kind_allowlist,
                cardinality_mode = EXCLUDED.cardinality_mode,
                description = EXCLUDED.description,
                updated_at = EXCLUDED.updated_at
            RETURNING
                predicate_slug,
                predicate_status,
                subject_kind_allowlist,
                object_kind_allowlist,
                cardinality_mode,
                description,
                created_at,
                updated_at
            """,
            normalized.predicate_slug,
            normalized.predicate_status,
            json.dumps(list(normalized.subject_kind_allowlist)),
            json.dumps(list(normalized.object_kind_allowlist)),
            normalized.cardinality_mode,
            normalized.description,
            normalized.created_at,
            normalized.updated_at,
        )
        assert row is not None
        return project_semantic_predicate(cast(dict[str, Any], dict(row)))

    async def load_assertion(
        self,
        *,
        semantic_assertion_id: str,
    ) -> SemanticAssertionRecord | None:
        row = await self._conn.fetchrow(
            """
            SELECT
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            FROM semantic_assertions
            WHERE semantic_assertion_id = $1
            LIMIT 1
            """,
            semantic_assertion_id,
        )
        if row is None:
            return None
        return project_semantic_assertion(cast(dict[str, Any], dict(row)))

    async def _supersede_conflicts(
        self,
        *,
        assertion: SemanticAssertionRecord,
        cardinality_mode: str,
        as_of: datetime,
    ) -> tuple[SemanticAssertionRecord, ...]:
        if cardinality_mode not in SUPPORTED_CARDINALITY_MODES:
            raise SemanticAssertionError(
                "semantic_assertion.invalid_cardinality_mode",
                f"unsupported cardinality mode: {cardinality_mode}",
                details={"cardinality_mode": cardinality_mode},
            )
        if cardinality_mode == "many":
            return ()
        object_kind = (
            assertion.object_kind
            if cardinality_mode == "single_active_per_edge"
            else None
        )
        object_ref = (
            assertion.object_ref
            if cardinality_mode == "single_active_per_edge"
            else None
        )
        rows = await self._conn.fetch(
            """
            UPDATE semantic_assertions
               SET assertion_status = 'superseded',
                   valid_to = CASE
                       WHEN valid_from < $6 THEN $6
                       ELSE valid_from
                   END,
                   updated_at = $7
             WHERE predicate_slug = $1
               AND subject_kind = $2
               AND subject_ref = $3
               AND ($4::text IS NULL OR object_kind = $4)
               AND ($5::text IS NULL OR object_ref = $5)
               AND semantic_assertion_id <> $8
               AND assertion_status <> 'retracted'
               AND (valid_to IS NULL OR valid_to > $6)
            RETURNING
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            """,
            assertion.predicate_slug,
            assertion.subject_kind,
            assertion.subject_ref,
            object_kind,
            object_ref,
            assertion.valid_from,
            as_of,
            assertion.semantic_assertion_id,
        )
        return tuple(
            project_semantic_assertion(cast(dict[str, Any], dict(row)))
            for row in rows
        )

    async def record_assertion(
        self,
        *,
        assertion: SemanticAssertionRecord,
        cardinality_mode: str,
        as_of: datetime,
    ) -> tuple[SemanticAssertionRecord, tuple[SemanticAssertionRecord, ...]]:
        normalized = normalize_semantic_assertion_record(assertion)
        superseded = await self._supersede_conflicts(
            assertion=normalized,
            cardinality_mode=cardinality_mode,
            as_of=as_of,
        )
        row = await self._conn.fetchrow(
            """
            INSERT INTO semantic_assertions (
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8::jsonb,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                $15,
                $16
            )
            ON CONFLICT (semantic_assertion_id) DO UPDATE SET
                predicate_slug = EXCLUDED.predicate_slug,
                assertion_status = EXCLUDED.assertion_status,
                subject_kind = EXCLUDED.subject_kind,
                subject_ref = EXCLUDED.subject_ref,
                object_kind = EXCLUDED.object_kind,
                object_ref = EXCLUDED.object_ref,
                qualifiers_json = EXCLUDED.qualifiers_json,
                source_kind = EXCLUDED.source_kind,
                source_ref = EXCLUDED.source_ref,
                evidence_ref = EXCLUDED.evidence_ref,
                bound_decision_id = EXCLUDED.bound_decision_id,
                valid_from = EXCLUDED.valid_from,
                valid_to = EXCLUDED.valid_to,
                updated_at = EXCLUDED.updated_at
            RETURNING
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            """,
            normalized.semantic_assertion_id,
            normalized.predicate_slug,
            normalized.assertion_status,
            normalized.subject_kind,
            normalized.subject_ref,
            normalized.object_kind,
            normalized.object_ref,
            json.dumps(dict(normalized.qualifiers_json)),
            normalized.source_kind,
            normalized.source_ref,
            normalized.evidence_ref,
            normalized.bound_decision_id,
            normalized.valid_from,
            normalized.valid_to,
            normalized.created_at,
            normalized.updated_at,
        )
        assert row is not None
        persisted = project_semantic_assertion(cast(dict[str, Any], dict(row)))
        await self.rebuild_current_assertions(as_of=as_of)
        return persisted, superseded

    async def retract_assertion(
        self,
        *,
        semantic_assertion_id: str,
        retracted_at: datetime,
        updated_at: datetime,
    ) -> SemanticAssertionRecord:
        row = await self._conn.fetchrow(
            """
            UPDATE semantic_assertions
               SET assertion_status = 'retracted',
                   valid_to = CASE
                       WHEN valid_to IS NULL OR valid_to > $2 THEN
                           CASE
                               WHEN valid_from < $2 THEN $2
                               ELSE valid_from
                           END
                       ELSE valid_to
                   END,
                   updated_at = $3
             WHERE semantic_assertion_id = $1
            RETURNING
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            """,
            semantic_assertion_id,
            retracted_at,
            updated_at,
        )
        if row is None:
            raise SemanticAssertionError(
                "semantic_assertion.not_found",
                "semantic assertion does not exist",
                details={"semantic_assertion_id": semantic_assertion_id},
            )
        persisted = project_semantic_assertion(cast(dict[str, Any], dict(row)))
        await self.rebuild_current_assertions(as_of=updated_at)
        return persisted

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
        rows = await self._conn.fetch(
            """
            SELECT
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            FROM semantic_current_assertions
            WHERE ($1::text IS NULL OR predicate_slug = $1)
              AND ($2::text IS NULL OR subject_kind = $2)
              AND ($3::text IS NULL OR subject_ref = $3)
              AND ($4::text IS NULL OR object_kind = $4)
              AND ($5::text IS NULL OR object_ref = $5)
              AND ($6::text IS NULL OR source_kind = $6)
              AND ($7::text IS NULL OR source_ref = $7)
            ORDER BY valid_from DESC, created_at DESC, semantic_assertion_id
            LIMIT $8
            """,
            predicate_slug,
            subject_kind,
            subject_ref,
            object_kind,
            object_ref,
            source_kind,
            source_ref,
            max(1, int(limit or 100)),
        )
        return tuple(
            project_semantic_assertion(cast(dict[str, Any], dict(row)))
            for row in rows
        )

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
        rows = await self._conn.fetch(
            """
            SELECT
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            FROM semantic_assertions
            WHERE ($1::text IS NULL OR predicate_slug = $1)
              AND ($2::text IS NULL OR subject_kind = $2)
              AND ($3::text IS NULL OR subject_ref = $3)
              AND ($4::text IS NULL OR object_kind = $4)
              AND ($5::text IS NULL OR object_ref = $5)
              AND ($6::text IS NULL OR source_kind = $6)
              AND ($7::text IS NULL OR source_ref = $7)
              AND (
                    NOT $8::boolean
                    OR (
                        valid_from <= $9
                        AND (valid_to IS NULL OR valid_to > $9)
                    )
                  )
            ORDER BY valid_from DESC, created_at DESC, semantic_assertion_id
            LIMIT $10
            """,
            predicate_slug,
            subject_kind,
            subject_ref,
            object_kind,
            object_ref,
            source_kind,
            source_ref,
            active_only,
            active_at,
            max(1, int(limit or 100)),
        )
        return tuple(
            project_semantic_assertion(cast(dict[str, Any], dict(row)))
            for row in rows
        )

    async def rebuild_current_assertions(
        self,
        *,
        as_of: datetime,
    ) -> int:
        row = await self._conn.fetchrow(
            """
            WITH current_assertions AS (
                SELECT DISTINCT ON (semantic_assertion_id)
                    semantic_assertion_id,
                    predicate_slug,
                    assertion_status,
                    subject_kind,
                    subject_ref,
                    object_kind,
                    object_ref,
                    qualifiers_json,
                    source_kind,
                    source_ref,
                    evidence_ref,
                    bound_decision_id,
                    valid_from,
                    valid_to,
                    created_at,
                    updated_at
                FROM semantic_assertions
                WHERE valid_from <= $1
                  AND (valid_to IS NULL OR valid_to > $1)
                ORDER BY
                    semantic_assertion_id,
                    updated_at DESC,
                    valid_from DESC,
                    created_at DESC
            ),
            upserted AS (
            INSERT INTO semantic_current_assertions (
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            )
            SELECT
                semantic_assertion_id,
                predicate_slug,
                assertion_status,
                subject_kind,
                subject_ref,
                object_kind,
                object_ref,
                qualifiers_json,
                source_kind,
                source_ref,
                evidence_ref,
                bound_decision_id,
                valid_from,
                valid_to,
                created_at,
                updated_at
            FROM current_assertions
            ON CONFLICT (semantic_assertion_id) DO UPDATE SET
                predicate_slug = EXCLUDED.predicate_slug,
                assertion_status = EXCLUDED.assertion_status,
                subject_kind = EXCLUDED.subject_kind,
                subject_ref = EXCLUDED.subject_ref,
                object_kind = EXCLUDED.object_kind,
                object_ref = EXCLUDED.object_ref,
                qualifiers_json = EXCLUDED.qualifiers_json,
                source_kind = EXCLUDED.source_kind,
                source_ref = EXCLUDED.source_ref,
                evidence_ref = EXCLUDED.evidence_ref,
                bound_decision_id = EXCLUDED.bound_decision_id,
                valid_from = EXCLUDED.valid_from,
                valid_to = EXCLUDED.valid_to,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            RETURNING semantic_assertion_id
            ),
            pruned AS (
                DELETE FROM semantic_current_assertions current_assertion
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM current_assertions
                    WHERE current_assertions.semantic_assertion_id =
                        current_assertion.semantic_assertion_id
                )
                RETURNING semantic_assertion_id
            )
            SELECT COUNT(*)::integer AS row_count
            FROM upserted
            """,
            as_of,
        )
        assert row is not None
        return int(row["row_count"])


__all__ = [
    "PostgresSemanticAssertionRepository",
    "bootstrap_semantic_assertion_repository_schema",
]
