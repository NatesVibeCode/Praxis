"""Raw Postgres repository for operator-control authority."""

from __future__ import annotations

from functools import lru_cache

import asyncpg

from authority.operator_control import (
    CutoverGateAuthorityRecord,
    OperatorControlAuthority,
    OperatorControlRepositoryError,
    OperatorDecisionAuthorityRecord,
    PENDING_REVIEW_SCOPE_CLAMP_TOKEN,
    _normalize_as_of,
    normalize_operator_decision_record,
    _require_datetime,
    _require_mapping,
    _require_text,
)
from runtime.embedding_service import embed_text_literal
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_DUPLICATE_SQLSTATES = {"42P07", "42701", "42710"}
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
        decision_scope_kind=(
            _require_text(row["decision_scope_kind"], field_name="decision_scope_kind")
            if row["decision_scope_kind"] is not None
            else None
        ),
        decision_scope_ref=(
            _require_text(row["decision_scope_ref"], field_name="decision_scope_ref")
            if row["decision_scope_ref"] is not None
            else None
        ),
        scope_clamp=_require_mapping(
            row["scope_clamp"]
            if "scope_clamp" in row.keys() and row["scope_clamp"] is not None
            else {
                "applies_to": (PENDING_REVIEW_SCOPE_CLAMP_TOKEN,),
                "does_not_apply_to": (),
            },
            field_name="scope_clamp",
        ),
        decision_provenance=(
            # Migration 302 column. Backward compatible: if the row was
            # selected by an older query that didn't include this column,
            # fall back to the schema default.
            str(row["decision_provenance"]).strip()
            if "decision_provenance" in row.keys() and row["decision_provenance"] is not None
            else "inferred"
        ),
        decision_why=(
            str(row["decision_why"])
            if "decision_why" in row.keys() and row["decision_why"] is not None
            else None
        ),
    )


def _scope_clamp_for_storage(scope_clamp: object) -> dict[str, list[str]]:
    """Convert the in-memory scope_clamp Mapping into a JSONB-friendly dict."""

    if not isinstance(scope_clamp, dict) and hasattr(scope_clamp, "get"):
        applies_to = scope_clamp.get("applies_to") or ()
        does_not_apply_to = scope_clamp.get("does_not_apply_to") or ()
    else:
        applies_to = (scope_clamp or {}).get("applies_to") or ()
        does_not_apply_to = (scope_clamp or {}).get("does_not_apply_to") or ()
    return {
        "applies_to": [str(item) for item in applies_to],
        "does_not_apply_to": [str(item) for item in does_not_apply_to],
    }


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

        normalized_operator_decision = normalize_operator_decision_record(
            operator_decision,
        )
        embedding_literal = embed_text_literal(
            f"{normalized_operator_decision.title} "
            f"{normalized_operator_decision.rationale}".strip(),
        )

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
                    updated_at,
                    decision_scope_kind,
                    decision_scope_ref,
                    scope_clamp,
                    embedding,
                    decision_provenance,
                    decision_why
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17::vector, $18, $19
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
                    updated_at = EXCLUDED.updated_at,
                    decision_scope_kind = EXCLUDED.decision_scope_kind,
                    decision_scope_ref = EXCLUDED.decision_scope_ref,
                    scope_clamp = EXCLUDED.scope_clamp,
                    embedding = COALESCE(EXCLUDED.embedding, operator_decisions.embedding),
                    decision_provenance = EXCLUDED.decision_provenance,
                    decision_why = COALESCE(EXCLUDED.decision_why, operator_decisions.decision_why)
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
                    updated_at,
                    decision_scope_kind,
                    decision_scope_ref,
                    scope_clamp,
                    decision_provenance,
                    decision_why
                """,
                normalized_operator_decision.operator_decision_id,
                normalized_operator_decision.decision_key,
                normalized_operator_decision.decision_kind,
                normalized_operator_decision.decision_status,
                normalized_operator_decision.title,
                normalized_operator_decision.rationale,
                normalized_operator_decision.decided_by,
                normalized_operator_decision.decision_source,
                normalized_operator_decision.effective_from,
                normalized_operator_decision.effective_to,
                normalized_operator_decision.decided_at,
                normalized_operator_decision.created_at,
                normalized_operator_decision.updated_at,
                normalized_operator_decision.decision_scope_kind,
                normalized_operator_decision.decision_scope_ref,
                _scope_clamp_for_storage(normalized_operator_decision.scope_clamp),
                embedding_literal,
                normalized_operator_decision.decision_provenance,
                normalized_operator_decision.decision_why,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "failed to record operator decision row",
                details={
                    "operator_decision_id": normalized_operator_decision.operator_decision_id,
                    "decision_key": normalized_operator_decision.decision_key,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc
        if row is None:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "recording operator decision row returned no row",
                details={
                    "operator_decision_id": normalized_operator_decision.operator_decision_id,
                    "decision_key": normalized_operator_decision.decision_key,
                },
        )
        return _decision_record_from_row(row)

    async def record_cutover_gate(
        self,
        *,
        operator_decision: OperatorDecisionAuthorityRecord,
        cutover_gate: CutoverGateAuthorityRecord,
    ) -> tuple[OperatorDecisionAuthorityRecord, CutoverGateAuthorityRecord]:
        normalized_operator_decision = normalize_operator_decision_record(
            operator_decision,
            fallback_scope_kind=cutover_gate.target_kind,
            fallback_scope_ref=cutover_gate.target_ref,
        )
        normalized_operator_decision = OperatorDecisionAuthorityRecord(
            operator_decision_id=_require_text(
                normalized_operator_decision.operator_decision_id,
                field_name="operator_decision.operator_decision_id",
            ),
            decision_key=_require_text(
                normalized_operator_decision.decision_key,
                field_name="operator_decision.decision_key",
            ),
            decision_kind=_require_text(
                normalized_operator_decision.decision_kind,
                field_name="operator_decision.decision_kind",
            ),
            decision_status=_require_text(
                normalized_operator_decision.decision_status,
                field_name="operator_decision.decision_status",
            ),
            title=_require_text(
                normalized_operator_decision.title,
                field_name="operator_decision.title",
            ),
            rationale=_require_text(
                normalized_operator_decision.rationale,
                field_name="operator_decision.rationale",
            ),
            decided_by=_require_text(
                normalized_operator_decision.decided_by,
                field_name="operator_decision.decided_by",
            ),
            decision_source=_require_text(
                normalized_operator_decision.decision_source,
                field_name="operator_decision.decision_source",
            ),
            effective_from=_require_datetime(
                normalized_operator_decision.effective_from,
                field_name="operator_decision.effective_from",
            ),
            effective_to=_require_datetime(
                normalized_operator_decision.effective_to,
                field_name="operator_decision.effective_to",
            )
            if normalized_operator_decision.effective_to is not None
            else None,
            decided_at=_require_datetime(
                normalized_operator_decision.decided_at,
                field_name="operator_decision.decided_at",
            ),
            created_at=_require_datetime(
                normalized_operator_decision.created_at,
                field_name="operator_decision.created_at",
            ),
            updated_at=_require_datetime(
                normalized_operator_decision.updated_at,
                field_name="operator_decision.updated_at",
            ),
            decision_scope_kind=normalized_operator_decision.decision_scope_kind,
            decision_scope_ref=normalized_operator_decision.decision_scope_ref,
            scope_clamp=normalized_operator_decision.scope_clamp,
        )
        normalized_cutover_gate = CutoverGateAuthorityRecord(
            cutover_gate_id=_require_text(
                cutover_gate.cutover_gate_id,
                field_name="cutover_gate.cutover_gate_id",
            ),
            gate_key=_require_text(cutover_gate.gate_key, field_name="cutover_gate.gate_key"),
            gate_name=_require_text(
                cutover_gate.gate_name,
                field_name="cutover_gate.gate_name",
            ),
            gate_kind=_require_text(
                cutover_gate.gate_kind,
                field_name="cutover_gate.gate_kind",
            ),
            gate_status=_require_text(
                cutover_gate.gate_status,
                field_name="cutover_gate.gate_status",
            ),
            target_kind=_require_text(
                cutover_gate.target_kind,
                field_name="cutover_gate.target_kind",
            ),
            target_ref=_require_text(
                cutover_gate.target_ref,
                field_name="cutover_gate.target_ref",
            ),
            gate_policy=_require_mapping(
                cutover_gate.gate_policy,
                field_name="cutover_gate.gate_policy",
            ),
            required_evidence=_require_mapping(
                cutover_gate.required_evidence,
                field_name="cutover_gate.required_evidence",
            ),
            opened_by_decision_id=_require_text(
                cutover_gate.opened_by_decision_id,
                field_name="cutover_gate.opened_by_decision_id",
            ),
            closed_by_decision_id=(
                _require_text(
                    cutover_gate.closed_by_decision_id,
                    field_name="cutover_gate.closed_by_decision_id",
                )
                if cutover_gate.closed_by_decision_id is not None
                else None
            ),
            opened_at=_require_datetime(
                cutover_gate.opened_at,
                field_name="cutover_gate.opened_at",
            ),
            closed_at=_require_datetime(
                cutover_gate.closed_at,
                field_name="cutover_gate.closed_at",
            )
            if cutover_gate.closed_at is not None
            else None,
            created_at=_require_datetime(
                cutover_gate.created_at,
                field_name="cutover_gate.created_at",
            ),
            updated_at=_require_datetime(
                cutover_gate.updated_at,
                field_name="cutover_gate.updated_at",
            ),
        )

        if normalized_cutover_gate.target_kind == "roadmap_item":
            roadmap_item_id = normalized_cutover_gate.target_ref
            workflow_class_id = None
            schedule_definition_id = None
        elif normalized_cutover_gate.target_kind == "workflow_class":
            roadmap_item_id = None
            workflow_class_id = normalized_cutover_gate.target_ref
            schedule_definition_id = None
        elif normalized_cutover_gate.target_kind == "schedule_definition":
            roadmap_item_id = None
            workflow_class_id = None
            schedule_definition_id = normalized_cutover_gate.target_ref
        else:
            raise OperatorControlRepositoryError(
                "operator_control.invalid_row",
                f"unsupported cutover target_kind {normalized_cutover_gate.target_kind!r}",
                details={
                    "cutover_gate_id": normalized_cutover_gate.cutover_gate_id,
                    "target_kind": normalized_cutover_gate.target_kind,
                },
            )

        gate_decision_embedding_literal = embed_text_literal(
            f"{normalized_operator_decision.title} "
            f"{normalized_operator_decision.rationale}".strip(),
        )

        try:
            async with self._conn.transaction():
                decision_row = await self._conn.fetchrow(
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
                        updated_at,
                        decision_scope_kind,
                        decision_scope_ref,
                        scope_clamp,
                        embedding,
                        decision_provenance,
                        decision_why
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17::vector, $18, $19
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
                        updated_at = EXCLUDED.updated_at,
                        decision_scope_kind = EXCLUDED.decision_scope_kind,
                        decision_scope_ref = EXCLUDED.decision_scope_ref,
                        scope_clamp = EXCLUDED.scope_clamp,
                        embedding = COALESCE(EXCLUDED.embedding, operator_decisions.embedding),
                        decision_provenance = EXCLUDED.decision_provenance,
                        decision_why = COALESCE(EXCLUDED.decision_why, operator_decisions.decision_why)
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
                        updated_at,
                        decision_scope_kind,
                        decision_scope_ref,
                        scope_clamp,
                        decision_provenance,
                        decision_why
                    """,
                    normalized_operator_decision.operator_decision_id,
                    normalized_operator_decision.decision_key,
                    normalized_operator_decision.decision_kind,
                    normalized_operator_decision.decision_status,
                    normalized_operator_decision.title,
                    normalized_operator_decision.rationale,
                    normalized_operator_decision.decided_by,
                    normalized_operator_decision.decision_source,
                    normalized_operator_decision.effective_from,
                    normalized_operator_decision.effective_to,
                    normalized_operator_decision.decided_at,
                    normalized_operator_decision.created_at,
                    normalized_operator_decision.updated_at,
                    normalized_operator_decision.decision_scope_kind,
                    normalized_operator_decision.decision_scope_ref,
                    _scope_clamp_for_storage(normalized_operator_decision.scope_clamp),
                    gate_decision_embedding_literal,
                    normalized_operator_decision.decision_provenance,
                    normalized_operator_decision.decision_why,
                )
                gate_row = await self._conn.fetchrow(
                    """
                    INSERT INTO cutover_gates (
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
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT (cutover_gate_id) DO UPDATE SET
                        gate_key = EXCLUDED.gate_key,
                        gate_name = EXCLUDED.gate_name,
                        gate_kind = EXCLUDED.gate_kind,
                        gate_status = EXCLUDED.gate_status,
                        roadmap_item_id = EXCLUDED.roadmap_item_id,
                        workflow_class_id = EXCLUDED.workflow_class_id,
                        schedule_definition_id = EXCLUDED.schedule_definition_id,
                        gate_policy = EXCLUDED.gate_policy,
                        required_evidence = EXCLUDED.required_evidence,
                        opened_by_decision_id = EXCLUDED.opened_by_decision_id,
                        closed_by_decision_id = EXCLUDED.closed_by_decision_id,
                        opened_at = EXCLUDED.opened_at,
                        closed_at = EXCLUDED.closed_at,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    RETURNING
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
                    """,
                    normalized_cutover_gate.cutover_gate_id,
                    normalized_cutover_gate.gate_key,
                    normalized_cutover_gate.gate_name,
                    normalized_cutover_gate.gate_kind,
                    normalized_cutover_gate.gate_status,
                    roadmap_item_id,
                    workflow_class_id,
                    schedule_definition_id,
                    normalized_cutover_gate.gate_policy,
                    normalized_cutover_gate.required_evidence,
                    normalized_cutover_gate.opened_by_decision_id,
                    normalized_cutover_gate.closed_by_decision_id,
                    normalized_cutover_gate.opened_at,
                    normalized_cutover_gate.closed_at,
                    normalized_cutover_gate.created_at,
                    normalized_cutover_gate.updated_at,
                )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "failed to record cutover gate rows",
                details={
                    "operator_decision_id": normalized_operator_decision.operator_decision_id,
                    "cutover_gate_id": normalized_cutover_gate.cutover_gate_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc
        if decision_row is None:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "recording operator decision row returned no row",
                details={"operator_decision_id": normalized_operator_decision.operator_decision_id},
            )
        if gate_row is None:
            raise OperatorControlRepositoryError(
                "operator_control.write_failed",
                "recording cutover gate row returned no row",
                details={"cutover_gate_id": normalized_cutover_gate.cutover_gate_id},
            )
        return (
            _decision_record_from_row(decision_row),
            _gate_record_from_row(gate_row),
        )

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
                    updated_at,
                    decision_scope_kind,
                    decision_scope_ref,
                    scope_clamp
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

    async def list_operator_decisions(
        self,
        *,
        decision_kind: str | None = None,
        decision_source: str | None = None,
        decision_scope_kind: str | None = None,
        decision_scope_ref: str | None = None,
        active_only: bool = True,
        as_of=None,
        limit: int = 100,
    ) -> tuple[OperatorDecisionAuthorityRecord, ...]:
        normalized_decision_kind = (
            _require_text(decision_kind, field_name="decision_kind")
            if decision_kind is not None
            else ""
        )
        normalized_decision_source = (
            _require_text(decision_source, field_name="decision_source")
            if decision_source is not None
            else ""
        )
        normalized_decision_scope_kind = (
            _require_text(decision_scope_kind, field_name="decision_scope_kind")
            if decision_scope_kind is not None
            else ""
        )
        normalized_decision_scope_ref = (
            _require_text(decision_scope_ref, field_name="decision_scope_ref")
            if decision_scope_ref is not None
            else ""
        )
        normalized_limit = max(1, min(int(limit), 500))
        normalized_as_of = _normalize_as_of(as_of) if active_only else None
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
                    updated_at,
                    decision_scope_kind,
                    decision_scope_ref,
                    scope_clamp
                FROM operator_decisions
                WHERE ($1::text = '' OR decision_kind = $1)
                  AND ($2::text = '' OR decision_source = $2)
                  AND ($3::text = '' OR decision_scope_kind = $3)
                  AND ($4::text = '' OR decision_scope_ref = $4)
                  AND (
                        NOT $5::boolean
                        OR (
                            effective_from <= $6
                            AND (effective_to IS NULL OR effective_to > $6)
                        )
                  )
                ORDER BY effective_from DESC, decided_at DESC, created_at DESC, operator_decision_id DESC
                LIMIT $7
                """,
                normalized_decision_kind,
                normalized_decision_source,
                normalized_decision_scope_kind,
                normalized_decision_scope_ref,
                active_only,
                normalized_as_of,
                normalized_limit,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.read_failed",
                "failed to list operator decision rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_decision_record_from_row(row) for row in rows)

    async def fetch_operator_decisions_for_semantic_bridge(
        self,
        *,
        as_of=None,
    ) -> tuple[OperatorDecisionAuthorityRecord, ...]:
        """Return canonical operator decisions for deterministic semantic bridge replay."""

        normalized_as_of = None if as_of is None else _normalize_as_of(as_of)
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
                    updated_at,
                    decision_scope_kind,
                    decision_scope_ref,
                    scope_clamp
                FROM operator_decisions
                WHERE ($1::timestamptz IS NULL OR created_at <= $1)
                ORDER BY effective_from ASC, decided_at ASC, created_at ASC, operator_decision_id ASC
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise OperatorControlRepositoryError(
                "operator_control.read_failed",
                "failed to fetch operator decision rows for semantic bridge replay",
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
