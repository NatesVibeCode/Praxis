"""Raw Postgres repository for operator-control authority."""

from __future__ import annotations

from functools import lru_cache

import asyncpg

from authority.operator_control import (
    CutoverGateAuthorityRecord,
    OperatorControlAuthority,
    OperatorControlRepositoryError,
    OperatorDecisionAuthorityRecord,
    _normalize_as_of,
    _require_datetime,
    _require_mapping,
    _require_text,
)
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_SCHEMA_FILENAME = "010_operator_control_authority.sql"
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "operator_control.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "operator_control.schema_missing"
        )
        message = (
            "operator-control schema file did not contain executable statements"
            if reason_code == "operator_control.schema_empty"
            else "operator-control schema file could not be resolved from the canonical workflow migration root"
        )
        raise OperatorControlRepositoryError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _target_from_row(row: asyncpg.Record) -> tuple[str, str]:
    target_columns = (
        ("roadmap_item_id", "roadmap_item", row["roadmap_item_id"]),
        ("workflow_class_id", "workflow_class", row["workflow_class_id"]),
        ("schedule_definition_id", "schedule_definition", row["schedule_definition_id"]),
    )
    populated_targets: list[tuple[str, str]] = []
    for field_name, target_kind, value in target_columns:
        if value is None:
            continue
        populated_targets.append(
            (
                target_kind,
                _require_text(value, field_name=field_name),
            )
        )
    if len(populated_targets) != 1:
        raise OperatorControlRepositoryError(
            "operator_control.invalid_row",
            "cutover gate must target exactly one authority row",
            details={
                "cutover_gate_id": _require_text(
                    row["cutover_gate_id"],
                    field_name="cutover_gate_id",
                ),
                "target_columns": ",".join(
                    field_name for field_name, _, value in target_columns if value is not None
                ),
            },
        )
    return populated_targets[0]


def _decision_record_from_row(row: asyncpg.Record) -> OperatorDecisionAuthorityRecord:
    return OperatorDecisionAuthorityRecord(
        operator_decision_id=_require_text(
            row["operator_decision_id"],
            field_name="operator_decision_id",
        ),
        decision_key=_require_text(row["decision_key"], field_name="decision_key"),
        decision_kind=_require_text(row["decision_kind"], field_name="decision_kind"),
        decision_status=_require_text(row["decision_status"], field_name="decision_status"),
        title=_require_text(row["title"], field_name="title"),
        rationale=_require_text(row["rationale"], field_name="rationale"),
        decided_by=_require_text(row["decided_by"], field_name="decided_by"),
        decision_source=_require_text(row["decision_source"], field_name="decision_source"),
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=(
            _require_datetime(row["effective_to"], field_name="effective_to")
            if row["effective_to"] is not None
            else None
        ),
        decided_at=_require_datetime(row["decided_at"], field_name="decided_at"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
        updated_at=_require_datetime(row["updated_at"], field_name="updated_at"),
    )


def _gate_record_from_row(row: asyncpg.Record) -> CutoverGateAuthorityRecord:
    target_kind, target_ref = _target_from_row(row)
    gate_policy = _require_mapping(row["gate_policy"], field_name="gate_policy")
    required_evidence = _require_mapping(
        row["required_evidence"],
        field_name="required_evidence",
    )
    return CutoverGateAuthorityRecord(
        cutover_gate_id=_require_text(row["cutover_gate_id"], field_name="cutover_gate_id"),
        gate_key=_require_text(row["gate_key"], field_name="gate_key"),
        gate_name=_require_text(row["gate_name"], field_name="gate_name"),
        gate_kind=_require_text(row["gate_kind"], field_name="gate_kind"),
        gate_status=_require_text(row["gate_status"], field_name="gate_status"),
        target_kind=target_kind,
        target_ref=target_ref,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        opened_by_decision_id=_require_text(
            row["opened_by_decision_id"],
            field_name="opened_by_decision_id",
        ),
        closed_by_decision_id=(
            _require_text(row["closed_by_decision_id"], field_name="closed_by_decision_id")
            if row["closed_by_decision_id"] is not None
            else None
        ),
        opened_at=_require_datetime(row["opened_at"], field_name="opened_at"),
        closed_at=(
            _require_datetime(row["closed_at"], field_name="closed_at")
            if row["closed_at"] is not None
            else None
        ),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
        updated_at=_require_datetime(row["updated_at"], field_name="updated_at"),
    )


class PostgresOperatorControlRepository:
    """Explicit Postgres repository for canonical operator-control rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def record_operator_decision(
        self,
        *,
        operator_decision: OperatorDecisionAuthorityRecord,
    ) -> OperatorDecisionAuthorityRecord:
        """Persist one canonical operator decision row."""

        try:
            row = await self._conn.fetchrow(
                """
                INSERT INTO operator_decisions (
                    operator_decision_id,
                    decision_key,
                    decision_kind,
                    decision_status,
                    title,
                    rationale,
                    decided_by,
                    decision_source,
                    effective_from,
                    effective_to,
                    decided_at,
                    created_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                )
                ON CONFLICT (operator_decision_id) DO UPDATE SET
                    decision_key = EXCLUDED.decision_key,
                    decision_kind = EXCLUDED.decision_kind,
                    decision_status = EXCLUDED.decision_status,
                    title = EXCLUDED.title,
                    rationale = EXCLUDED.rationale,
                    decided_by = EXCLUDED.decided_by,
                    decision_source = EXCLUDED.decision_source,
                    effective_from = EXCLUDED.effective_from,
                    effective_to = EXCLUDED.effective_to,
                    decided_at = EXCLUDED.decided_at,
                    updated_at = EXCLUDED.updated_at
                RETURNING
                    operator_decision_id,
                    decision_key,
                    decision_kind,
                    decision_status,
                    title,
                    rationale,
                    decided_by,
                    decision_source,
                    effective_from,
                    effective_to,
                    decided_at,
                    created_at,
                    updated_at
                """,
                operator_decision.operator_decision_id,
                operator_decision.decision_key,
                operator_decision.decision_kind,
                operator_decision.decision_status,
                operator_decision.title,
                operator_decision.rationale,
                operator_decision.decided_by,
                operator_decision.decision_source,
                operator_decision.effective_from,
                operator_decision.effective_to,
                operator_decision.decided_at,
                operator_decision.created_at,
                operator_decision.updated_at,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "failed to record operator decision row",
                details={
                    "operator_decision_id": operator_decision.operator_decision_id,
                    "decision_key": operator_decision.decision_key,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc
        if row is None:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "recording operator decision row returned no row",
                details={
                    "operator_decision_id": operator_decision.operator_decision_id,
                    "decision_key": operator_decision.decision_key,
                },
            )
        return _decision_record_from_row(row)

    async def fetch_operator_decision_records(
        self,
        *,
        as_of,
    ) -> tuple[OperatorDecisionAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    operator_decision_id,
                    decision_key,
                    decision_kind,
                    decision_status,
                    title,
                    rationale,
                    decided_by,
                    decision_source,
                    effective_from,
                    effective_to,
                    decided_at,
                    created_at,
                    updated_at
                FROM operator_decisions
                WHERE effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY decision_key, effective_from DESC, decided_at DESC, created_at DESC, operator_decision_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.read_failed",
                "failed to read operator decision rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_decision_record_from_row(row) for row in rows)

    async def fetch_cutover_gate_records(
        self,
        *,
        as_of,
    ) -> tuple[CutoverGateAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    cutover_gate_id,
                    gate_key,
                    gate_name,
                    gate_kind,
                    gate_status,
                    roadmap_item_id,
                    workflow_class_id,
                    schedule_definition_id,
                    gate_policy,
                    required_evidence,
                    opened_by_decision_id,
                    closed_by_decision_id,
                    opened_at,
                    closed_at,
                    created_at,
                    updated_at
                FROM cutover_gates
                WHERE opened_at <= $1
                  AND (closed_at IS NULL OR closed_at > $1)
                ORDER BY gate_key, opened_at DESC, created_at DESC, cutover_gate_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.read_failed",
                "failed to read cutover gate rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_gate_record_from_row(row) for row in rows)

    async def load_operator_control_authority(
        self,
        *,
        as_of,
    ) -> OperatorControlAuthority:
        """Load canonical operator-control authority from Postgres."""

        async with self._conn.transaction():
            decision_records = await self.fetch_operator_decision_records(as_of=as_of)
            gate_records = await self.fetch_cutover_gate_records(as_of=as_of)
            return OperatorControlAuthority.from_records(
                operator_decision_records=decision_records,
                cutover_gate_records=gate_records,
                as_of=as_of,
            )


async def bootstrap_operator_control_repository_schema(conn: asyncpg.Connection) -> None:
    """Apply the operator-control schema in one explicit transaction."""

    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in _schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise OperatorControlRepositoryError(
                    "operator_control.schema_bootstrap_failed",
                    "failed to bootstrap the operator-control schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


__all__ = [
    "PostgresOperatorControlRepository",
    "bootstrap_operator_control_repository_schema",
]
