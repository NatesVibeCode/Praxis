"""Explicit sync Postgres repository for receipt persistence and job context."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
from datetime import datetime, timedelta, timezone
import json
from typing import Any

from storage.migrations import workflow_compile_authority_readiness_tables
from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_nonnegative_int,
    _require_positive_int,
    _require_text,
    _require_utc,
)


_COMPILE_AUTHORITY_READINESS_OBJECTS = workflow_compile_authority_readiness_tables()

_COMPILE_AUTHORITY_READINESS_SQL = ",\n                ".join(
    f"to_regclass('public.{object_name}') IS NOT NULL AS {object_name}_ready"
    for object_name in _COMPILE_AUTHORITY_READINESS_OBJECTS
)


def _normalize_duration_seconds(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a non-negative number",
            details={"field": field_name},
        )
    duration = float(value)
    if duration < 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a non-negative number",
            details={"field": field_name},
        )
    return duration


def _normalize_optional_number(value: object, *, field_name: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be numeric when provided",
            details={"field": field_name},
        )
    return value


class PostgresReceiptRepository:
    """Owns canonical receipt, workflow-notification, and runtime-context writes."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def _fetchrow_compat(self, query: str, *args: Any) -> Any:
        """Support lightweight execute-only test doubles as well as real DB connections."""
        if hasattr(self._conn, "fetchrow"):
            return self._conn.fetchrow(query, *args)
        rows = self._conn.execute(query, *args)
        if not rows:
            return None
        return rows[0]

    def update_receipt_payloads(
        self,
        *,
        receipt_id: str,
        inputs: Mapping[str, Any],
        outputs: Mapping[str, Any],
    ) -> bool:
        rows = self._conn.execute(
            """
            UPDATE receipts
            SET inputs = $2::jsonb,
                outputs = $3::jsonb
            WHERE receipt_id = $1
            RETURNING receipt_id
            """,
            _require_text(receipt_id, field_name="receipt_id"),
            _encode_jsonb(dict(_require_mapping(inputs, field_name="inputs")), field_name="inputs"),
            _encode_jsonb(
                dict(_require_mapping(outputs, field_name="outputs")),
                field_name="outputs",
            ),
        )
        return bool(rows)

    def upsert_receipt(
        self,
        *,
        receipt_id: str,
        receipt_type: str,
        schema_version: int,
        workflow_id: str,
        run_id: str,
        request_id: str,
        node_id: str | None,
        attempt_no: int,
        started_at: datetime,
        finished_at: datetime,
        evidence_seq: int,
        executor_type: str,
        status: str,
        inputs: Mapping[str, Any],
        outputs: Mapping[str, Any],
        artifacts: Mapping[str, Any],
        failure_code: str | None,
        decision_refs: Sequence[Mapping[str, Any]],
    ) -> str:
        normalized_receipt_id = _require_text(receipt_id, field_name="receipt_id")
        self._conn.execute(
            """
            INSERT INTO receipts (
                receipt_id, receipt_type, schema_version,
                workflow_id, run_id, request_id,
                causation_id, node_id, attempt_no, supersedes_receipt_id,
                started_at, finished_at, evidence_seq,
                executor_type, status, inputs, outputs, artifacts,
                failure_code, decision_refs
            ) VALUES (
                $1, $2, $3,
                $4, $5, $6,
                NULL, $7, $8, NULL,
                $9, $10, $11,
                $12, $13, $14::jsonb, $15::jsonb, $16::jsonb,
                $17, $18::jsonb
            )
            ON CONFLICT (receipt_id) DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                request_id = EXCLUDED.request_id,
                node_id = EXCLUDED.node_id,
                attempt_no = EXCLUDED.attempt_no,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                evidence_seq = EXCLUDED.evidence_seq,
                executor_type = EXCLUDED.executor_type,
                status = EXCLUDED.status,
                inputs = EXCLUDED.inputs,
                outputs = EXCLUDED.outputs,
                artifacts = EXCLUDED.artifacts,
                failure_code = EXCLUDED.failure_code,
                decision_refs = EXCLUDED.decision_refs
            """,
            normalized_receipt_id,
            _require_text(receipt_type, field_name="receipt_type"),
            _require_positive_int(schema_version, field_name="schema_version"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(run_id, field_name="run_id"),
            _require_text(request_id, field_name="request_id"),
            _optional_text(node_id, field_name="node_id"),
            _require_positive_int(attempt_no, field_name="attempt_no"),
            _require_utc(started_at, field_name="started_at"),
            _require_utc(finished_at, field_name="finished_at"),
            _require_nonnegative_int(evidence_seq, field_name="evidence_seq"),
            _require_text(executor_type, field_name="executor_type"),
            _require_text(status, field_name="status"),
            _encode_jsonb(dict(_require_mapping(inputs, field_name="inputs")), field_name="inputs"),
            _encode_jsonb(
                dict(_require_mapping(outputs, field_name="outputs")),
                field_name="outputs",
            ),
            _encode_jsonb(
                dict(_require_mapping(artifacts, field_name="artifacts")),
                field_name="artifacts",
            ),
            _optional_text(failure_code, field_name="failure_code"),
            _encode_jsonb(
                [
                    dict(_require_mapping(item, field_name=f"decision_refs[{index}]"))
                    for index, item in enumerate(decision_refs)
                ],
                field_name="decision_refs",
            ),
        )
        return normalized_receipt_id

    def insert_receipt_if_absent(
        self,
        *,
        receipt_id: str,
        workflow_id: str,
        run_id: str,
        request_id: str,
        node_id: str,
        attempt_no: int,
        started_at: datetime,
        finished_at: datetime,
        evidence_seq: int,
        status: str,
        inputs: Mapping[str, Any],
        outputs: Mapping[str, Any],
        artifacts: Mapping[str, Any],
        failure_code: str | None,
    ) -> str:
        normalized_receipt_id = _require_text(receipt_id, field_name="receipt_id")
        self._conn.execute(
            """
            INSERT INTO receipts (
                receipt_id, receipt_type, schema_version,
                workflow_id, run_id, request_id,
                causation_id, node_id, attempt_no, supersedes_receipt_id,
                started_at, finished_at, evidence_seq,
                executor_type, status, inputs, outputs, artifacts,
                failure_code, decision_refs
            ) VALUES (
                $1, 'workflow_job', 1,
                $2, $3, $4,
                NULL, $5, $6, NULL,
                $7, $8, $9,
                'workflow_unified', $10, $11::jsonb, $12::jsonb, $13::jsonb,
                $14, $15::jsonb
            )
            ON CONFLICT (receipt_id) DO NOTHING
            """,
            normalized_receipt_id,
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(run_id, field_name="run_id"),
            _require_text(request_id, field_name="request_id"),
            _require_text(node_id, field_name="node_id"),
            _require_positive_int(attempt_no, field_name="attempt_no"),
            _require_utc(started_at, field_name="started_at"),
            _require_utc(finished_at, field_name="finished_at"),
            _require_nonnegative_int(evidence_seq, field_name="evidence_seq"),
            _require_text(status, field_name="status"),
            _encode_jsonb(dict(_require_mapping(inputs, field_name="inputs")), field_name="inputs"),
            _encode_jsonb(
                dict(_require_mapping(outputs, field_name="outputs")),
                field_name="outputs",
            ),
            _encode_jsonb(
                dict(_require_mapping(artifacts, field_name="artifacts")),
                field_name="artifacts",
            ),
            _optional_text(failure_code, field_name="failure_code"),
            _encode_jsonb([], field_name="decision_refs"),
        )
        return normalized_receipt_id

    def insert_receipt_if_absent_with_deterministic_seq(
        self,
        *,
        receipt_id: str,
        receipt_type: str = "workflow_job",
        schema_version: int = 1,
        workflow_id: str,
        run_id: str,
        request_id: str,
        node_id: str,
        attempt_no: int,
        started_at: datetime,
        finished_at: datetime,
        causation_id: str | None = None,
        supersedes_receipt_id: str | None = None,
        status: str,
        inputs: Mapping[str, Any],
        outputs: Mapping[str, Any],
        artifacts: Mapping[str, Any],
        failure_code: str | None,
        executor_type: str = "workflow_unified",
        decision_refs: Sequence[Mapping[str, Any]] | None = None,
    ) -> int:
        normalized_receipt_id = _require_text(receipt_id, field_name="receipt_id")
        normalized_run_id = _require_text(run_id, field_name="run_id")
        _require_text(workflow_id, field_name="workflow_id")
        _require_text(request_id, field_name="request_id")
        _require_text(node_id, field_name="node_id")
        _require_text(receipt_type, field_name="receipt_type")
        _require_text(executor_type, field_name="executor_type")

        lock_key = int.from_bytes(
            hashlib.blake2b(normalized_run_id.encode("utf-8"), digest_size=8).digest(),
            "big",
            signed=True,
        )

        row = self._fetchrow_compat(
            """
            WITH lock_token AS (
                SELECT pg_advisory_xact_lock($2::bigint)
            ),
            existing AS (
                SELECT evidence_seq
                FROM receipts
                WHERE receipt_id = $1
            ),
            next_seq AS (
                SELECT CASE
                    WHEN EXISTS (SELECT 1 FROM existing) THEN
                        (SELECT evidence_seq FROM existing LIMIT 1)
                    ELSE 1 + GREATEST(
                        COALESCE((SELECT MAX(evidence_seq) FROM workflow_events WHERE run_id = $3), 0),
                        COALESCE((SELECT MAX(evidence_seq) FROM receipts WHERE run_id = $3), 0)
                    )
                END AS evidence_seq
                FROM lock_token
            ),
            inserted AS (
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
                )
                SELECT
                    $1,
                    $4,
                    $5,
                    $6,
                    $3,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    $13,
                    next_seq.evidence_seq,
                    $14,
                    $15,
                    jsonb_set(
                        $16::jsonb,
                        '{transition_seq}',
                        to_jsonb(next_seq.evidence_seq::bigint),
                        true
                    ),
                    $17::jsonb,
                    $18::jsonb,
                    $19,
                    $20::jsonb
                FROM next_seq
                WHERE NOT EXISTS (SELECT 1 FROM existing)
                ON CONFLICT (receipt_id) DO NOTHING
                RETURNING evidence_seq
            )
            SELECT evidence_seq FROM inserted
            UNION ALL
            SELECT evidence_seq FROM existing
            LIMIT 1;
            """,
            normalized_receipt_id,
            lock_key,
            normalized_run_id,
            _require_text(receipt_type, field_name="receipt_type"),
            _require_positive_int(schema_version, field_name="schema_version"),
            _require_text(workflow_id, field_name="workflow_id"),
            _require_text(request_id, field_name="request_id"),
            _optional_text(causation_id, field_name="causation_id"),
            _require_text(node_id, field_name="node_id"),
            _require_positive_int(attempt_no, field_name="attempt_no"),
            _optional_text(supersedes_receipt_id, field_name="supersedes_receipt_id"),
            _require_utc(started_at, field_name="started_at"),
            _require_utc(finished_at, field_name="finished_at"),
            _require_text(executor_type, field_name="executor_type"),
            _require_text(status, field_name="status"),
            json.dumps(dict(_require_mapping(inputs, field_name="inputs")), sort_keys=True, default=str),
            json.dumps(dict(_require_mapping(outputs, field_name="outputs")), sort_keys=True, default=str),
            json.dumps(dict(_require_mapping(artifacts, field_name="artifacts")), sort_keys=True, default=str),
            _optional_text(failure_code, field_name="failure_code"),
            json.dumps(
                [
                    dict(_require_mapping(item, field_name=f"decision_refs[{index}]"))
                    for index, item in enumerate(decision_refs or ())
                ],
                sort_keys=True,
                default=str,
            ),
        )

        if row is None:
            raise PostgresWriteError(
                "receipt_repository.allocation_failed",
                "failed to allocate evidence sequence",
                details={"receipt_id": normalized_receipt_id},
            )

        if isinstance(row, Mapping):
            evidence_seq = row.get("evidence_seq")
        elif isinstance(row, (list, tuple)):
            evidence_seq = row[0] if row else None
        else:
            evidence_seq = row["evidence_seq"]

        if evidence_seq is None:
            raise PostgresWriteError(
                "receipt_repository.invalid_evidence_seq",
                "evidence sequence allocation returned empty result",
                details={"receipt_id": normalized_receipt_id},
            )

        return int(evidence_seq)

    def insert_workflow_notification_if_absent(
        self,
        *,
        run_id: str,
        job_label: str,
        spec_name: str,
        agent_slug: str,
        status: str,
        failure_code: str | None,
        duration_seconds: float,
        cpu_percent: int | float | None = None,
        mem_bytes: int | float | None = None,
        created_at: datetime | None = None,
    ) -> None:
        normalized_failure_code = str(failure_code or "").strip() or None
        self._conn.execute(
            """
            INSERT INTO workflow_notifications
                   (run_id, job_label, spec_name, agent_slug, status, failure_code,
                    duration_seconds, cpu_percent, mem_bytes, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT DO NOTHING
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(job_label, field_name="job_label"),
            str(spec_name or ""),
            _require_text(agent_slug, field_name="agent_slug"),
            _require_text(status, field_name="status"),
            _optional_text(normalized_failure_code, field_name="failure_code"),
            _normalize_duration_seconds(duration_seconds, field_name="duration_seconds"),
            _normalize_optional_number(cpu_percent, field_name="cpu_percent"),
            _normalize_optional_number(mem_bytes, field_name="mem_bytes"),
            _require_utc(created_at, field_name="created_at")
            if created_at is not None
            else None,
        )

    def notify_job_completed(self, *, run_id: str) -> None:
        self._conn.execute(
            "SELECT pg_notify('job_completed', $1)",
            _require_text(run_id, field_name="run_id"),
        )

    def upsert_workflow_job_runtime_context(
        self,
        *,
        run_id: str,
        job_label: str,
        workflow_id: str | None,
        execution_context_shard: Mapping[str, Any],
        execution_bundle: Mapping[str, Any],
    ) -> str:
        normalized_run_id = _require_text(run_id, field_name="run_id")
        normalized_job_label = _require_text(job_label, field_name="job_label")
        self._conn.execute(
            """
            INSERT INTO workflow_job_runtime_context
               (run_id, job_label, workflow_id, execution_context_shard, execution_bundle)
               VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
               ON CONFLICT (run_id, job_label) DO UPDATE SET
                   workflow_id = EXCLUDED.workflow_id,
                   execution_context_shard = EXCLUDED.execution_context_shard,
                   execution_bundle = EXCLUDED.execution_bundle,
                   updated_at = now()
            """,
            normalized_run_id,
            normalized_job_label,
            _optional_text(workflow_id, field_name="workflow_id"),
            _encode_jsonb(
                dict(
                    _require_mapping(
                        execution_context_shard,
                        field_name="execution_context_shard",
                    )
                ),
                field_name="execution_context_shard",
            ),
            _encode_jsonb(
                dict(_require_mapping(execution_bundle, field_name="execution_bundle")),
                field_name="execution_bundle",
            ),
        )
        return normalized_job_label

    def load_workflow_job_runtime_context(
        self,
        *,
        run_id: str,
        job_label: str,
    ) -> dict[str, Any] | None:
        row = self._fetchrow_compat(
            """
            SELECT run_id, job_label, workflow_id, execution_context_shard, execution_bundle,
                   created_at, updated_at
            FROM workflow_job_runtime_context
            WHERE run_id = $1 AND job_label = $2
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(job_label, field_name="job_label"),
        )
        return None if row is None else dict(row)

    def load_workflow_job_receipt_context(
        self,
        *,
        job_id: int,
        run_id: str,
    ) -> dict[str, Any] | None:
        row = self._fetchrow_compat(
            """
            SELECT wr.workflow_id,
                   wr.request_id,
                   wr.request_envelope,
                   j.attempt,
                   j.started_at,
                   j.finished_at,
                   j.touch_keys
            FROM workflow_jobs j
            JOIN workflow_runs wr ON wr.run_id = j.run_id
            WHERE j.id = $1 AND j.run_id = $2
            LIMIT 1
            """,
            _require_positive_int(job_id, field_name="job_id"),
            _require_text(run_id, field_name="run_id"),
        )
        return None if row is None else dict(row)

    def list_receipts(
        self,
        *,
        limit: int,
        since_hours: int = 0,
        status: str | None = None,
        agent: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        idx = 1
        if since_hours > 0:
            clauses.append(f"COALESCE(finished_at, started_at) >= ${idx}")
            params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
            idx += 1
        if status:
            clauses.append(f"status = ${idx}")
            params.append(_require_text(status, field_name="status"))
            idx += 1
        if agent:
            clauses.append(
                f"COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') = ${idx}"
            )
            params.append(_require_text(agent, field_name="agent"))
            idx += 1
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(_require_positive_int(limit, field_name="limit"))
        rows = self._conn.execute(
            "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
            "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
            f"FROM receipts {where} "
            f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST, receipt_id DESC LIMIT ${idx}",
            *params,
        )
        return [dict(row) for row in rows or ()]

    def load_receipt(
        self,
        *,
        receipt_id: str,
    ) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
            "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
            "FROM receipts WHERE receipt_id = $1 LIMIT 1",
            _require_text(receipt_id, field_name="receipt_id"),
        )
        return None if row is None else dict(row)

    def load_latest_receipt_for_run(
        self,
        *,
        run_id: str,
    ) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
            "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
            "FROM receipts WHERE run_id = $1 ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT 1",
            _require_text(run_id, field_name="run_id"),
        )
        return None if row is None else dict(row)

    def search_receipts(
        self,
        *,
        query: str,
        limit: int,
        status: str | None = None,
        agent: str | None = None,
        workflow_id: str | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [_require_text(query, field_name="query")]
        idx = 2
        clauses = [
            "(to_tsvector('english', COALESCE(node_id, '') || ' ' || COALESCE(status, '') || ' ' || COALESCE(failure_code, '') || ' ' || COALESCE(inputs::text, '') || ' ' || COALESCE(outputs::text, '')) @@ plainto_tsquery('english', $1) "
            "OR COALESCE(node_id, '') ILIKE '%' || $1 || '%' "
            "OR COALESCE(inputs::text, '') ILIKE '%' || $1 || '%' "
            "OR COALESCE(outputs::text, '') ILIKE '%' || $1 || '%')"
        ]
        if status:
            clauses.append(f"status = ${idx}")
            params.append(_require_text(status, field_name="status"))
            idx += 1
        if agent:
            clauses.append(
                f"COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') = ${idx}"
            )
            params.append(_require_text(agent, field_name="agent"))
            idx += 1
        if workflow_id:
            clauses.append(f"workflow_id = ${idx}")
            params.append(_require_text(workflow_id, field_name="workflow_id"))
            idx += 1
        params.append(_require_positive_int(limit, field_name="limit"))
        rows = self._conn.execute(
            "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
            "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
            f"FROM receipts WHERE {' AND '.join(clauses)} "
            f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT ${idx}",
            *params,
        )
        return [dict(row) for row in rows or ()]

    def receipt_stats(
        self,
        *,
        since_hours: int,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, 'unknown') AS agent,
                   COALESCE(SUM(COALESCE(NULLIF(outputs->>'token_input', '')::bigint, 0)), 0) AS total_input,
                   COALESCE(SUM(COALESCE(NULLIF(outputs->>'token_output', '')::bigint, 0)), 0) AS total_output,
                   COALESCE(SUM(COALESCE(NULLIF(outputs->>'cost_usd', '')::double precision, 0)), 0) AS total_cost,
                   COUNT(*) AS receipt_count
              FROM receipts
             WHERE COALESCE(finished_at, started_at) >= $1
             GROUP BY 1
            """,
            datetime.now(timezone.utc) - timedelta(hours=since_hours),
        )
        return [dict(row) for row in rows or ()]

    def proof_metrics_snapshot(
        self,
        *,
        since_hours: int = 0,
    ) -> dict[str, dict[str, Any]]:
        params: list[Any] = []
        where_clauses: list[str] = []
        if since_hours > 0:
            params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
            where_clauses.append("COALESCE(finished_at, started_at) >= $1")
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        receipt_row = self._conn.fetchrow(
            f"""
            SELECT
                COUNT(*) AS receipts_total,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') <> ''
                ) AS receipts_with_verification_status,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                ) AS receipts_with_attempted_verification,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') = 'configured'
                ) AS receipts_with_configured_verification,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') = 'skipped'
                ) AS receipts_with_skipped_verification,
                COUNT(*) FILTER (
                    WHERE jsonb_typeof(outputs->'verification') = 'object'
                      AND outputs->'verification' <> '{{}}'::jsonb
                ) AS receipts_with_verification,
                COUNT(*) FILTER (
                    WHERE jsonb_typeof(outputs->'verified_paths') = 'array'
                      AND jsonb_array_length(outputs->'verified_paths') > 0
                ) AS receipts_with_verified_paths,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                      AND NOT COALESCE((
                          jsonb_typeof(outputs->'verification') = 'object'
                          AND outputs->'verification' <> '{{}}'::jsonb
                      ), FALSE)
                      AND NOT COALESCE((
                          jsonb_typeof(outputs->'verified_paths') = 'array'
                          AND jsonb_array_length(outputs->'verified_paths') > 0
                      ), FALSE)
                ) AS receipts_with_status_only_verification,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                      AND jsonb_typeof(outputs->'verified_paths') = 'array'
                      AND jsonb_array_length(outputs->'verified_paths') > 0
                ) AS receipts_with_path_backed_verification,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                      AND jsonb_typeof(outputs->'verification') = 'object'
                      AND outputs->'verification' <> '{{}}'::jsonb
                      AND jsonb_typeof(outputs->'verified_paths') = 'array'
                      AND jsonb_array_length(outputs->'verified_paths') > 0
                ) AS receipts_with_fully_proved_verification,
                COUNT(*) FILTER (WHERE outputs ? 'write_manifest') AS receipts_with_write_manifest,
                COUNT(*) FILTER (WHERE outputs ? 'mutation_provenance') AS receipts_with_mutation_provenance,
                COUNT(*) FILTER (WHERE outputs ? 'git_provenance') AS receipts_with_git_provenance,
                COUNT(*) FILTER (
                    WHERE COALESCE(outputs->'git_provenance'->>'repo_snapshot_ref', '') <> ''
                ) AS receipts_with_repo_snapshot_ref
            FROM receipts
            {where}
            """,
            *params,
        ) or {}
        memory_row = self._conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE entity_type = 'code_unit') AS code_units,
                COUNT(*) FILTER (WHERE entity_type = 'table') AS tables,
                COUNT(*) FILTER (
                    WHERE entity_type = 'fact' AND COALESCE(metadata->>'entity_subtype', '') = 'verification_result'
                ) AS verification_results,
                COUNT(*) FILTER (
                    WHERE entity_type = 'fact' AND COALESCE(metadata->>'entity_subtype', '') = 'failure_result'
                ) AS failure_results
            FROM memory_entities
            WHERE archived = false
            """
        ) or {}
        edge_row = self._conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE relation_type = 'verified_by' AND active = true) AS verified_by_edges,
                COUNT(*) FILTER (WHERE relation_type = 'recorded_in' AND active = true) AS recorded_in_edges,
                COUNT(*) FILTER (WHERE relation_type = 'produced' AND active = true) AS produced_edges,
                COUNT(*) FILTER (WHERE relation_type = 'related_to' AND active = true) AS related_edges
            FROM memory_edges
            """
        ) or {}
        compile_row = self._conn.fetchrow(
            f"""
            SELECT
                {_COMPILE_AUTHORITY_READINESS_SQL}
            """
        ) or {}
        repo_snapshot_row = (
            self._conn.fetchrow("SELECT COUNT(*) AS repo_snapshots FROM repo_snapshots") or {}
        ) if bool(compile_row.get("repo_snapshots_ready")) else {"repo_snapshots": 0}
        verifier_healer_row = (
            self._conn.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM verifier_registry) AS verifiers,
                    (SELECT COUNT(*) FROM healer_registry) AS healers,
                    (SELECT COUNT(*) FROM verifier_healer_bindings WHERE enabled = TRUE) AS verifier_healer_bindings,
                    (SELECT COUNT(*) FROM verification_runs) AS verification_runs,
                    (SELECT COUNT(*) FROM healing_runs) AS healing_runs
                """
            ) or {}
        ) if all(
            bool(compile_row.get(key))
            for key in (
                "verifier_registry_ready",
                "healer_registry_ready",
                "verifier_healer_bindings_ready",
                "verification_runs_ready",
                "healing_runs_ready",
            )
        ) else {
            "verifiers": 0,
            "healers": 0,
            "verifier_healer_bindings": 0,
            "verification_runs": 0,
            "healing_runs": 0,
        }
        return {
            "receipts": dict(receipt_row),
            "memory_graph": dict(memory_row),
            "edges": dict(edge_row),
            "compile_authority": dict(compile_row),
            "repo_snapshots": dict(repo_snapshot_row),
            "recovery_authority": dict(verifier_healer_row),
        }

    def list_receipts_for_provenance_backfill(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where_clauses: list[str] = []
        if run_id:
            params.append(_require_text(run_id, field_name="run_id"))
            where_clauses.append(f"r.run_id = ${len(params)}")
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = ""
        if limit is not None:
            params.append(max(limit, 0))
            limit_sql = f" LIMIT ${len(params)}"
        rows = self._conn.execute(
            f"""
            SELECT
                r.receipt_id,
                r.inputs,
                r.outputs,
                j.touch_keys,
                wr.request_envelope
            FROM receipts AS r
            LEFT JOIN workflow_jobs AS j
                ON j.receipt_id = r.receipt_id
            LEFT JOIN workflow_runs AS wr
                ON wr.run_id = r.run_id
            {where}
            ORDER BY r.evidence_seq ASC
            {limit_sql}
            """,
            *params,
        )
        return [dict(row) for row in rows or ()]
