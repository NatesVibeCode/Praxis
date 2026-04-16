"""Explicit async Postgres repository for canonical workflow evidence writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any

from .validators import (
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_nonnegative_int,
    _require_positive_int,
    _require_text,
    _require_utc,
)


def _json_compatible_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "to_json"):
        return _json_compatible_value(value.to_json())
    if hasattr(value, "to_contract"):
        return _json_compatible_value(value.to_contract())
    if is_dataclass(value):
        return {
            field.name: _json_compatible_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _json_compatible_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_compatible_value(item) for item in value]
    if hasattr(value, "value") and type(getattr(value, "value")).__name__ == "str":
        return getattr(value, "value")
    return value


class PostgresEvidenceRepository:
    """Owns canonical durable workflow evidence and lifecycle writes."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def insert_workflow_definition_if_absent(
        self,
        *,
        workflow_definition_id: str,
        workflow_id: str,
        definition_hash: str,
        request_envelope: Mapping[str, Any],
        created_at: datetime,
    ) -> None:
        normalized_request_envelope = _json_compatible_value(
            dict(_require_mapping(request_envelope, field_name="request_envelope"))
        )
        await self._conn.execute(
            """
            INSERT INTO workflow_definitions (
                workflow_definition_id,
                workflow_id,
                schema_version,
                definition_version,
                definition_hash,
                status,
                request_envelope,
                normalized_definition,
                created_at,
                supersedes_workflow_definition_id
            ) VALUES ($1, $2, 1, 1, $3, 'active', $4::jsonb, $4::jsonb, $5, NULL)
            ON CONFLICT DO NOTHING
            """,
            _require_text(workflow_definition_id, field_name="workflow_definition_id"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(definition_hash, field_name="definition_hash"),
            _encode_jsonb(normalized_request_envelope, field_name="request_envelope"),
            _require_utc(created_at, field_name="created_at"),
        )

    async def insert_admission_decision_if_absent(
        self,
        *,
        admission_decision_id: str,
        workflow_id: str,
        request_id: str,
        decided_at: datetime,
        authority_context_ref: str,
    ) -> None:
        await self._conn.execute(
            """
            INSERT INTO admission_decisions (
                admission_decision_id,
                workflow_id,
                request_id,
                decision,
                reason_code,
                decided_at,
                decided_by,
                policy_snapshot_ref,
                validation_result_ref,
                authority_context_ref
            ) VALUES ($1, $2, $3, 'admit', 'auto_admit', $4, 'runtime', 'none', 'none', $5)
            ON CONFLICT DO NOTHING
            """,
            _require_text(admission_decision_id, field_name="admission_decision_id"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(request_id, field_name="request_id"),
            _require_utc(decided_at, field_name="decided_at"),
            _require_text(authority_context_ref, field_name="authority_context_ref"),
        )

    async def insert_workflow_run_if_absent(
        self,
        *,
        run_id: str,
        workflow_id: str,
        request_id: str,
        request_digest: str,
        authority_context_digest: str,
        workflow_definition_id: str,
        admitted_definition_hash: str,
        run_idempotency_key: str,
        request_envelope: Mapping[str, Any],
        context_bundle_id: str,
        admission_decision_id: str,
        current_state: str,
        requested_at: datetime,
        admitted_at: datetime | None = None,
    ) -> None:
        normalized_request_envelope = _json_compatible_value(
            dict(_require_mapping(request_envelope, field_name="request_envelope"))
        )
        normalized_requested_at = _require_utc(requested_at, field_name="requested_at")
        normalized_admitted_at = (
            _require_utc(admitted_at, field_name="admitted_at")
            if admitted_at
            else normalized_requested_at
        )
        await self._conn.execute(
            """
            INSERT INTO workflow_runs (
                run_id,
                workflow_id,
                request_id,
                request_digest,
                authority_context_digest,
                workflow_definition_id,
                admitted_definition_hash,
                run_idempotency_key,
                schema_version,
                request_envelope,
                context_bundle_id,
                admission_decision_id,
                current_state,
                terminal_reason_code,
                requested_at,
                admitted_at,
                started_at,
                finished_at,
                last_event_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, 1,
                $9::jsonb, $10, $11, $12, NULL, $13, $14, NULL, NULL, NULL
            )
            ON CONFLICT DO NOTHING
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(request_id, field_name="request_id"),
            _require_text(request_digest, field_name="request_digest"),
            _require_text(
                authority_context_digest,
                field_name="authority_context_digest",
            ),
            _require_text(workflow_definition_id, field_name="workflow_definition_id"),
            _require_text(admitted_definition_hash, field_name="admitted_definition_hash"),
            _require_text(run_idempotency_key, field_name="run_idempotency_key"),
            _encode_jsonb(
                normalized_request_envelope,
                field_name="request_envelope",
            ),
            _require_text(context_bundle_id, field_name="context_bundle_id"),
            _require_text(admission_decision_id, field_name="admission_decision_id"),
            _require_text(current_state, field_name="current_state"),
            normalized_requested_at,
            normalized_admitted_at,
        )

    async def insert_workflow_event_if_absent(
        self,
        *,
        event_id: str,
        event_type: str,
        schema_version: int,
        workflow_id: str,
        run_id: str,
        request_id: str,
        causation_id: str | None,
        node_id: str | None,
        occurred_at: datetime,
        evidence_seq: int,
        actor_type: str,
        reason_code: str | None,
        payload: Mapping[str, Any],
    ) -> None:
        await self._conn.execute(
            """
            INSERT INTO workflow_events (
                event_id,
                event_type,
                schema_version,
                workflow_id,
                run_id,
                request_id,
                causation_id,
                node_id,
                occurred_at,
                evidence_seq,
                actor_type,
                reason_code,
                payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
            ON CONFLICT DO NOTHING
            """,
            _require_text(event_id, field_name="event_id"),
            _require_text(event_type, field_name="event_type"),
            _require_positive_int(schema_version, field_name="schema_version"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(run_id, field_name="run_id"),
            _require_text(request_id, field_name="request_id"),
            _optional_text(causation_id, field_name="causation_id"),
            _optional_text(node_id, field_name="node_id"),
            _require_utc(occurred_at, field_name="occurred_at"),
            _require_positive_int(evidence_seq, field_name="evidence_seq"),
            _require_text(actor_type, field_name="actor_type"),
            _optional_text(reason_code, field_name="reason_code"),
            _encode_jsonb(
                _json_compatible_value(
                    dict(_require_mapping(payload, field_name="payload"))
                ),
                field_name="payload",
            ),
        )

    async def insert_receipt_if_absent(
        self,
        *,
        receipt_id: str,
        receipt_type: str,
        schema_version: int,
        workflow_id: str,
        run_id: str,
        request_id: str,
        causation_id: str | None,
        node_id: str | None,
        attempt_no: int,
        supersedes_receipt_id: str | None,
        started_at: datetime,
        finished_at: datetime,
        evidence_seq: int,
        executor_type: str,
        status: str,
        inputs: Mapping[str, Any],
        outputs: Mapping[str, Any],
        artifacts: Sequence[Mapping[str, Any]],
        failure_code: str | None,
        decision_refs: Sequence[Mapping[str, Any]],
    ) -> None:
        await self._conn.execute(
            """
            INSERT INTO receipts (
                receipt_id,
                receipt_type,
                schema_version,
                workflow_id,
                run_id,
                request_id,
                causation_id,
                node_id,
                attempt_no,
                supersedes_receipt_id,
                started_at,
                finished_at,
                evidence_seq,
                executor_type,
                status,
                inputs,
                outputs,
                artifacts,
                failure_code,
                decision_refs
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16::jsonb, $17::jsonb,
                $18::jsonb, $19, $20::jsonb
            )
            ON CONFLICT DO NOTHING
            """,
            _require_text(receipt_id, field_name="receipt_id"),
            _require_text(receipt_type, field_name="receipt_type"),
            _require_positive_int(schema_version, field_name="schema_version"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(run_id, field_name="run_id"),
            _require_text(request_id, field_name="request_id"),
            _optional_text(causation_id, field_name="causation_id"),
            _optional_text(node_id, field_name="node_id"),
            _require_positive_int(attempt_no, field_name="attempt_no"),
            _optional_text(supersedes_receipt_id, field_name="supersedes_receipt_id"),
            _require_utc(started_at, field_name="started_at"),
            _require_utc(finished_at, field_name="finished_at"),
            _require_nonnegative_int(evidence_seq, field_name="evidence_seq"),
            _require_text(executor_type, field_name="executor_type"),
            _require_text(status, field_name="status"),
            _encode_jsonb(
                _json_compatible_value(dict(_require_mapping(inputs, field_name="inputs"))),
                field_name="inputs",
            ),
            _encode_jsonb(
                _json_compatible_value(dict(_require_mapping(outputs, field_name="outputs"))),
                field_name="outputs",
            ),
            _encode_jsonb(
                _json_compatible_value([
                    dict(_require_mapping(item, field_name=f"artifacts[{index}]"))
                    for index, item in enumerate(artifacts)
                ]),
                field_name="artifacts",
            ),
            _optional_text(failure_code, field_name="failure_code"),
            _encode_jsonb(
                _json_compatible_value([
                    dict(_require_mapping(item, field_name=f"decision_refs[{index}]"))
                    for index, item in enumerate(decision_refs)
                ]),
                field_name="decision_refs",
            ),
        )

    async def update_workflow_run_state(
        self,
        *,
        run_id: str,
        new_state: str,
        terminal_reason_code: str | None,
        finished_at: datetime | None,
        last_event_id: str,
        occurred_at: datetime,
        expected_current_state: str | None = None,
    ) -> bool:
        terminal_states = ("succeeded", "failed", "cancelled", "dead_letter")
        base_query = """
            UPDATE workflow_runs
            SET current_state = $2,
                admitted_at = CASE
                    WHEN $2 IN ('claim_accepted', 'claim_rejected', 'claim_blocked')
                    THEN COALESCE(admitted_at, $6)
                    ELSE admitted_at
                END,
                started_at = CASE
                    WHEN $2 IN ('lease_requested', 'lease_active', 'proposal_submitted', 'gate_evaluating', 'running', 'succeeded', 'failed', 'cancelled')
                    THEN COALESCE(started_at, admitted_at, requested_at, $6)
                    ELSE started_at
                END,
                terminal_reason_code = CASE
                    WHEN $2 IN %(terminal_states)s THEN
                        CASE
                            WHEN current_state IN %(terminal_states)s THEN terminal_reason_code
                            ELSE COALESCE($3, terminal_reason_code)
                        END
                    ELSE NULL
                END,
                finished_at = CASE
                    WHEN $2 IN %(terminal_states)s THEN COALESCE($4, finished_at)
                    ELSE finished_at
                END,
                last_event_id = $5
            WHERE run_id = $1
              AND (current_state NOT IN %(terminal_states)s OR current_state = $2)
        """ % {"terminal_states": tuple(terminal_states)}
        args: list[object] = [
            _require_text(run_id, field_name="run_id"),
            _require_text(new_state, field_name="new_state"),
            _optional_text(terminal_reason_code, field_name="terminal_reason_code"),
            _require_utc(finished_at, field_name="finished_at") if finished_at else None,
            _require_text(last_event_id, field_name="last_event_id"),
            _require_utc(occurred_at, field_name="occurred_at"),
        ]
        if expected_current_state is not None:
            base_query += " AND current_state = $7"
            args.append(_require_text(expected_current_state, field_name="expected_current_state"))
        result = await self._conn.execute(base_query, *args)

        return result == "UPDATE 1"


    async def notify_run_cancelled(self, *, channel: str, run_id: str) -> None:
        await self._conn.execute(
            "SELECT pg_notify($1, $2)",
            _require_text(channel, field_name="channel"),
            _require_text(run_id, field_name="run_id"),
        )

    async def lock_workflow_run(self, *, run_lock_key: int) -> None:
        await self._conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            int(run_lock_key),
        )

    async def load_current_state(self, *, run_id: str) -> str | None:
        state = await self._conn.fetchval(
            "SELECT current_state FROM workflow_runs WHERE run_id = $1",
            _require_text(run_id, field_name="run_id"),
        )
        return None if state is None else str(state)

    async def workflow_run_exists(self, *, run_id: str) -> bool:
        value = await self._conn.fetchval(
            "SELECT run_id FROM workflow_runs WHERE run_id = $1",
            _require_text(run_id, field_name="run_id"),
        )
        return value is not None

    async def list_recent_runs(self, *, limit: int) -> list[dict[str, Any]]:
        rows = await self._conn.fetch(
            """
            SELECT
                run_id,
                workflow_id,
                current_state,
                terminal_reason_code,
                requested_at,
                started_at,
                finished_at,
                last_event_id
            FROM workflow_runs
            ORDER BY requested_at DESC
            LIMIT $1
            """,
            _require_positive_int(limit, field_name="limit"),
        )
        return [dict(row) for row in rows]

    async def load_run_detail(self, *, run_id: str) -> dict[str, Any] | None:
        row = await self._conn.fetchrow(
            """
            SELECT
                run_id,
                workflow_id,
                request_id,
                current_state,
                terminal_reason_code,
                requested_at,
                admitted_at,
                started_at,
                finished_at,
                last_event_id,
                schema_version,
                workflow_definition_id,
                admitted_definition_hash,
                admission_decision_id,
                context_bundle_id
            FROM workflow_runs
            WHERE run_id = $1
            """,
            _require_text(run_id, field_name="run_id"),
        )
        return None if row is None else dict(row)

    async def count_events_for_run(self, *, run_id: str) -> int:
        count = await self._conn.fetchval(
            "SELECT count(*) FROM workflow_events WHERE run_id = $1",
            _require_text(run_id, field_name="run_id"),
        )
        return int(count or 0)

    async def count_receipts_for_run(self, *, run_id: str) -> int:
        count = await self._conn.fetchval(
            "SELECT count(*) FROM receipts WHERE run_id = $1",
            _require_text(run_id, field_name="run_id"),
        )
        return int(count or 0)
