"""Postgres-backed evidence writer for durable workflow state.

Implements the same interface as AppendOnlyWorkflowEvidenceWriter but
writes directly to Postgres so runs survive process crashes and are
queryable via the workflow_runs / workflow_events / receipts tables.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

import asyncpg

from adapters.evidence import build_claim_received_proof, build_transition_proof
from receipts.evidence import (
    EvidenceAppendError,
    EvidenceRow,
    ReceiptV1,
    TransitionProofV1,
    WorkflowEventV1,
    _make_event_id,
    _make_receipt_id,
    _normalize_event,
    _normalize_receipt,
    _normalize_route_identity,
    _route_identity_snapshot,
    _transition_failure_code,
    _validate_reserved_lineage,
    _validate_route_identity_lineage,
)
from runtime.domain import (
    EvidenceCommitResult,
    LifecycleTransition,
    RouteIdentity,
    RuntimeBoundaryError,
)
from storage.postgres import (
    connect_workflow_database,
    fetch_workflow_evidence_timeline,
)


def _encode_jsonb(value: object) -> str:
    """JSON-encode a value for Postgres jsonb columns."""

    def _json_ready(raw: object) -> object:
        if hasattr(raw, "to_dict") and callable(getattr(raw, "to_dict")):
            return _json_ready(raw.to_dict())
        if isinstance(raw, Mapping):
            return {str(key): _json_ready(item) for key, item in raw.items()}
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            return [_json_ready(item) for item in raw]
        if isinstance(raw, datetime):
            return raw.isoformat()
        return raw

    return json.dumps(_json_ready(value), sort_keys=True, default=str)


class _SyncAsyncBridge:
    """Run async coroutines from sync code on a dedicated event loop.

    Keeps a single event loop alive so asyncpg connections created on it
    remain valid across multiple calls. Avoids the "attached to a different
    loop" error from calling asyncio.run() repeatedly.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        return self._loop

    def run(self, coro):
        loop = self._get_loop()
        return loop.run_until_complete(coro)

    def close(self) -> None:
        if self._loop is not None and not self._loop.is_closed():
            self._loop.close()
            self._loop = None


def _run_async(coro):
    """Run an async coroutine from sync code, matching the project pattern."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


@dataclass(frozen=True, slots=True)
class _PersistedEvidenceState:
    timeline: tuple[EvidenceRow, ...]
    last_route_identity: RouteIdentity | None
    last_evidence_seq: int
    last_transition_seq: int
    last_row_id: str | None


def _run_lock_key(run_id: str) -> int:
    digest = hashlib.blake2b(run_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def _state_from_timeline(timeline: Sequence[EvidenceRow]) -> _PersistedEvidenceState:
    rows = tuple(sorted(timeline, key=lambda row: (row.evidence_seq, row.row_id)))
    if not rows:
        return _PersistedEvidenceState(
            timeline=(),
            last_route_identity=None,
            last_evidence_seq=0,
            last_transition_seq=0,
            last_row_id=None,
        )
    last = rows[-1]
    return _PersistedEvidenceState(
        timeline=rows,
        last_route_identity=last.route_identity,
        last_evidence_seq=last.evidence_seq,
        last_transition_seq=last.transition_seq,
        last_row_id=last.row_id,
    )


def _normalize_proof(proof: TransitionProofV1) -> TransitionProofV1:
    if not isinstance(proof, TransitionProofV1):
        raise EvidenceAppendError(
            "evidence.invalid_shape",
            "transition proof must be a TransitionProofV1",
        )
    route_identity = _normalize_route_identity(proof.route_identity)
    event = _normalize_event(proof.event)
    receipt = _normalize_receipt(proof.receipt)
    if event.route_identity != route_identity or receipt.route_identity != route_identity:
        raise EvidenceAppendError(
            "evidence.route_identity_mismatch",
            "transition proof route_identity must match both envelopes",
        )
    if proof.transition_seq != route_identity.transition_seq:
        raise EvidenceAppendError(
            "evidence.transition_seq_mismatch",
            "transition proof transition_seq must match route_identity.transition_seq",
        )
    if event.transition_seq != receipt.transition_seq:
        raise EvidenceAppendError(
            "evidence.transition_seq_mismatch",
            "transition event and receipt must agree on transition_seq",
        )
    route_snapshot = _route_identity_snapshot(route_identity)
    _validate_reserved_lineage(
        field_name="event.payload",
        value=event.payload,
        expected={
            "route_identity": route_snapshot,
            "event_id": event.event_id,
            "receipt_id": receipt.receipt_id,
            "evidence_seq": event.evidence_seq,
            "transition_seq": proof.transition_seq,
            "causation_id": event.causation_id,
        },
    )
    _validate_reserved_lineage(
        field_name="receipt.inputs",
        value=receipt.inputs,
        expected={
            "route_identity": route_snapshot,
            "event_id": event.event_id,
            "receipt_id": receipt.receipt_id,
            "evidence_seq": event.evidence_seq,
            "transition_seq": proof.transition_seq,
            "causation_id": event.causation_id,
        },
    )
    _validate_reserved_lineage(
        field_name="receipt.outputs",
        value=receipt.outputs,
        expected={
            "route_identity": route_snapshot,
            "event_id": event.event_id,
            "receipt_id": receipt.receipt_id,
            "evidence_seq": receipt.evidence_seq,
            "transition_seq": proof.transition_seq,
            "causation_id": event.causation_id,
        },
    )
    return TransitionProofV1(
        route_identity=route_identity,
        transition_seq=proof.transition_seq,
        event=event,
        receipt=receipt,
    )


class PostgresEvidenceWriter:
    """Evidence writer that persists to Postgres.

    Uses a dedicated event loop so the asyncpg connection stays valid
    across multiple sync calls from the dispatch path.
    """

    def __init__(
        self,
        *,
        conn: asyncpg.Connection | None = None,
        database_url: str | None = None,
    ) -> None:
        self._conn = conn
        self._database_url = database_url
        self._owns_conn = conn is None
        self._bridge = _SyncAsyncBridge()

    def _run(self, coro):
        """Run an async coroutine on this writer's dedicated loop."""
        return self._bridge.run(coro)

    def close_blocking(self) -> None:
        """Close connection and dedicated loop on the writer's own bridge."""
        try:
            self._run(self.close())
        finally:
            self._bridge.close()

    def _get_conn(self) -> asyncpg.Connection:
        if self._conn is not None:
            return self._conn
        raise RuntimeError(
            "PostgresEvidenceWriter: no connection available. "
            "Call _ensure_conn() first."
        )

    def operator_frame_repository(self):
        """Return the canonical Postgres operator-frame authority for this writer."""

        from storage.postgres import PostgresOperatorFrameRepository, ensure_postgres_available

        env = None
        if isinstance(self._database_url, str) and self._database_url.strip():
            env = {"WORKFLOW_DATABASE_URL": self._database_url}
        return PostgresOperatorFrameRepository(ensure_postgres_available(env=env))

    def current_state_for_run(self, run_id: str) -> str | None:
        """Return the canonical persisted workflow_runs state for one run."""
        return self._run(self._load_current_state(run_id=run_id))

    async def _ensure_conn(self) -> asyncpg.Connection:
        if self._conn is not None:
            return self._conn
        if self._database_url:
            self._conn = await asyncpg.connect(self._database_url)
        else:
            self._conn = await connect_workflow_database()
        self._owns_conn = True
        return self._conn

    async def _lock_run(self, conn: asyncpg.Connection, run_id: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _run_lock_key(run_id),
        )

    async def _load_persisted_evidence_state(
        self,
        run_id: str,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> _PersistedEvidenceState:
        active_conn = conn if conn is not None else await self._ensure_conn()
        timeline = await fetch_workflow_evidence_timeline(active_conn, run_id=run_id)
        return _state_from_timeline(timeline)

    async def _load_current_state(self, *, run_id: str) -> str | None:
        conn = await self._ensure_conn()
        state = await conn.fetchval(
            "SELECT current_state FROM workflow_runs WHERE run_id = $1",
            run_id,
        )
        if state is None:
            return None
        return str(state)

    def commit_submission(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
    ) -> EvidenceCommitResult:
        """Persist the initial claim_received evidence bundle to Postgres."""
        normalized_route_identity = _normalize_route_identity(route_identity)
        if not isinstance(request_payload, Mapping):
            raise EvidenceAppendError(
                "evidence.invalid_shape",
                "request_payload must be a mapping",
                details={"field": "request_payload"},
            )

        # Persist the run + evidence to Postgres
        run_id = normalized_route_identity.run_id

        try:
            result = self._run(
                self._persist_submission(
                    route_identity=normalized_route_identity,
                    admitted_definition_ref=admitted_definition_ref,
                    admitted_definition_hash=admitted_definition_hash,
                    request_payload=request_payload,
                )
            )
        except (EvidenceAppendError, RuntimeBoundaryError):
            raise
        except Exception as exc:
            raise RuntimeBoundaryError(
                f"persistent evidence submission failed for run {run_id}"
            ) from exc

        return result

    async def _persist_submission(
        self,
        *,
        route_identity: RouteIdentity,
        admitted_definition_ref: str,
        admitted_definition_hash: str,
        request_payload: Mapping[str, Any],
    ) -> EvidenceCommitResult:
        conn = await self._ensure_conn()
        normalized_route_identity = _normalize_route_identity(route_identity)
        run_id = normalized_route_identity.run_id
        workflow_id = normalized_route_identity.workflow_id
        request_id = normalized_route_identity.request_id
        occurred_at = datetime.now(timezone.utc)
        proof = _normalize_proof(
            build_claim_received_proof(
                route_identity=replace(normalized_route_identity, transition_seq=1),
                event_id=_make_event_id(run_id, 1),
                receipt_id=_make_receipt_id(run_id, 2),
                evidence_seq=1,
                transition_seq=1,
                request_payload=request_payload,
                admitted_definition_ref=admitted_definition_ref,
                admitted_definition_hash=admitted_definition_hash,
                occurred_at=occurred_at,
            )
        )
        request_envelope_json = _encode_jsonb(request_payload)

        async with conn.transaction():
            await self._lock_run(conn, run_id)
            state = await self._load_persisted_evidence_state(run_id, conn=conn)
            if state.last_evidence_seq != 0:
                raise RuntimeBoundaryError(
                    f"persistent evidence submission already exists for run {run_id}"
                )
            # Insert workflow_definitions (idempotent)
            await conn.execute(
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
                admitted_definition_ref,
                workflow_id,
                admitted_definition_hash,
                request_envelope_json,
                occurred_at,
            )

            # Insert admission_decisions
            admission_decision_id = f"admission:{run_id}"
            await conn.execute(
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
                admission_decision_id,
                workflow_id,
                request_id,
                occurred_at,
                normalized_route_identity.authority_context_ref,
            )

            # Insert workflow_runs
            await conn.execute(
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
                    $9::jsonb, $10, $11, 'claim_received', NULL, $12, NULL, NULL, NULL, NULL
                )
                ON CONFLICT DO NOTHING
                """,
                run_id,
                workflow_id,
                request_id,
                f"sha256:{run_id}",
                normalized_route_identity.authority_context_digest,
                admitted_definition_ref,
                admitted_definition_hash,
                f"idempotency:{run_id}",
                request_envelope_json,
                f"context:{run_id}",
                admission_decision_id,
                occurred_at,
            )

            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="workflow_event",
                    evidence_seq=proof.event.evidence_seq,
                    row_id=proof.event.event_id,
                    route_identity=proof.route_identity,
                    transition_seq=proof.transition_seq,
                    record=proof.event,
                ),
            )
            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="receipt",
                    evidence_seq=proof.receipt.evidence_seq,
                    row_id=proof.receipt.receipt_id,
                    route_identity=proof.route_identity,
                    transition_seq=proof.transition_seq,
                    record=proof.receipt,
                ),
            )

            await self._update_workflow_run_state(
                conn,
                run_id=run_id,
                new_state=proof.receipt.status,
                reason_code=proof.event.reason_code,
                occurred_at=proof.event.occurred_at,
                last_event_id=proof.event.event_id,
            )

        return EvidenceCommitResult(
            event_id=proof.event.event_id,
            receipt_id=proof.receipt.receipt_id,
            evidence_seq=proof.receipt.evidence_seq,
            committed_at=proof.receipt.finished_at,
        )

    def commit_transition(
        self,
        *,
        transition: LifecycleTransition,
    ) -> EvidenceCommitResult:
        """Persist one runtime transition and matching evidence to Postgres."""
        run_id = transition.route_identity.run_id

        try:
            result = self._run(self._persist_transition(transition=transition))
        except (EvidenceAppendError, RuntimeBoundaryError):
            raise
        except Exception as exc:
            raise RuntimeBoundaryError(
                f"persistent evidence transition failed for run {run_id}"
            ) from exc

        return result

    async def _persist_transition(
        self,
        *,
        transition: LifecycleTransition,
    ) -> EvidenceCommitResult:
        conn = await self._ensure_conn()
        normalized_route_identity = _normalize_route_identity(transition.route_identity)
        run_id = normalized_route_identity.run_id

        async with conn.transaction():
            await self._lock_run(conn, run_id)
            state = await self._load_persisted_evidence_state(run_id, conn=conn)
            if state.last_route_identity is None:
                raise RuntimeBoundaryError(
                    f"persistent evidence transition missing durable submission for run {run_id}"
                )
            _validate_route_identity_lineage(
                previous=state.last_route_identity,
                current=normalized_route_identity,
            )
            expected_transition_seq = state.last_transition_seq + 1
            if normalized_route_identity.transition_seq != expected_transition_seq:
                raise EvidenceAppendError(
                    "evidence.transition_seq_conflict",
                    "transition_seq must advance one step at a time",
                    details={
                        "run_id": run_id,
                        "expected_transition_seq": expected_transition_seq,
                        "received_transition_seq": normalized_route_identity.transition_seq,
                    },
                )

            event_evidence_seq = state.last_evidence_seq + 1
            proof = _normalize_proof(
                build_transition_proof(
                    route_identity=normalized_route_identity,
                    transition_seq=normalized_route_identity.transition_seq,
                    event_id=_make_event_id(run_id, event_evidence_seq),
                    receipt_id=_make_receipt_id(run_id, event_evidence_seq + 1),
                    event_type=transition.event_type,
                    receipt_type=transition.receipt_type,
                    reason_code=transition.reason_code,
                    evidence_seq=event_evidence_seq,
                    occurred_at=transition.occurred_at,
                    payload={
                        "from_state": transition.from_state.value,
                        "to_state": transition.to_state.value,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                        "transition_seq": normalized_route_identity.transition_seq,
                    },
                    inputs={
                        "from_state": transition.from_state.value,
                        "to_state": transition.to_state.value,
                        "route_identity": _route_identity_snapshot(normalized_route_identity),
                        "transition_seq": normalized_route_identity.transition_seq,
                    },
                    outputs={
                        "event_id": _make_event_id(run_id, event_evidence_seq),
                        "receipt_id": _make_receipt_id(run_id, event_evidence_seq + 1),
                        "evidence_seq": event_evidence_seq + 1,
                        "transition_seq": normalized_route_identity.transition_seq,
                        "to_state": transition.to_state.value,
                    },
                    causation_id=state.last_row_id,
                    status=transition.to_state.value,
                    failure_code=_transition_failure_code(transition),
                )
            )

            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="workflow_event",
                    evidence_seq=proof.event.evidence_seq,
                    row_id=proof.event.event_id,
                    route_identity=proof.route_identity,
                    transition_seq=proof.transition_seq,
                    record=proof.event,
                ),
            )
            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="receipt",
                    evidence_seq=proof.receipt.evidence_seq,
                    row_id=proof.receipt.receipt_id,
                    route_identity=proof.route_identity,
                    transition_seq=proof.transition_seq,
                    record=proof.receipt,
                ),
            )

            await self._update_workflow_run_state(
                conn,
                run_id,
                new_state=transition.to_state.value,
                reason_code=transition.reason_code,
                occurred_at=transition.occurred_at,
                last_event_id=proof.event.event_id,
            )

        return EvidenceCommitResult(
            event_id=proof.event.event_id,
            receipt_id=proof.receipt.receipt_id,
            evidence_seq=proof.receipt.evidence_seq,
            committed_at=proof.receipt.finished_at,
        )

    async def _insert_evidence_row(
        self,
        conn: asyncpg.Connection,
        row: EvidenceRow,
    ) -> None:
        """Insert a single evidence row (event or receipt) into Postgres."""

        if row.kind == "workflow_event":
            event: WorkflowEventV1 = row.record
            await conn.execute(
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
                event.event_id,
                event.event_type,
                event.schema_version,
                event.workflow_id,
                event.run_id,
                event.request_id,
                event.causation_id,
                event.node_id,
                event.occurred_at,
                event.evidence_seq,
                event.actor_type,
                event.reason_code,
                _encode_jsonb(event.payload),
            )

        elif row.kind == "receipt":
            receipt: ReceiptV1 = row.record
            await conn.execute(
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
                receipt.receipt_id,
                receipt.receipt_type,
                receipt.schema_version,
                receipt.workflow_id,
                receipt.run_id,
                receipt.request_id,
                receipt.causation_id,
                receipt.node_id,
                receipt.attempt_no,
                receipt.supersedes_receipt_id,
                receipt.started_at,
                receipt.finished_at,
                receipt.evidence_seq,
                receipt.executor_type,
                receipt.status,
                _encode_jsonb(receipt.inputs),
                _encode_jsonb(receipt.outputs),
                _encode_jsonb(
                    [
                        {
                            "artifact_id": a.artifact_id,
                            "artifact_type": a.artifact_type,
                            "content_hash": a.content_hash,
                            "storage_ref": a.storage_ref,
                        }
                        for a in receipt.artifacts
                    ]
                ),
                receipt.failure_code,
                _encode_jsonb(
                    [
                        {
                            "decision_type": d.decision_type,
                            "decision_id": d.decision_id,
                            "reason_code": d.reason_code,
                            "source_table": d.source_table,
                        }
                        for d in receipt.decision_refs
                    ]
                ),
            )

    async def _update_workflow_run_state(
        self,
        conn: asyncpg.Connection,
        run_id: str,
        *,
        new_state: str,
        reason_code: str | None,
        occurred_at: datetime,
        last_event_id: str,
    ) -> None:
        terminal_states = {"succeeded", "failed", "cancelled"}
        terminal_reason = None
        finished_at = None
        if new_state in terminal_states:
            terminal_reason = reason_code
            finished_at = occurred_at

        result = await conn.execute(
            """
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
                terminal_reason_code = COALESCE($3, terminal_reason_code),
                finished_at = COALESCE($4, finished_at),
                last_event_id = $5
            WHERE run_id = $1
            """,
            run_id,
            new_state,
            terminal_reason,
            finished_at,
            last_event_id,
            occurred_at,
        )
        if result != "UPDATE 1":
            raise RuntimeBoundaryError(
                f"persistent evidence missing workflow_runs row for {run_id}"
            )

    def append_transition_proof(
        self,
        proof: TransitionProofV1,
    ) -> EvidenceCommitResult:
        """Append one event/receipt transition proof to Postgres."""
        normalized_proof = _normalize_proof(proof)
        run_id = normalized_proof.route_identity.run_id

        try:
            return self._run(self._persist_proof(proof=normalized_proof))
        except (EvidenceAppendError, RuntimeBoundaryError):
            raise
        except Exception as exc:
            raise RuntimeBoundaryError(
                f"persistent evidence proof append failed for run {run_id}"
            ) from exc

    async def _persist_proof(
        self,
        *,
        proof: TransitionProofV1,
    ) -> EvidenceCommitResult:
        conn = await self._ensure_conn()
        normalized_proof = _normalize_proof(proof)
        route_identity = normalized_proof.route_identity
        run_id = route_identity.run_id

        async with conn.transaction():
            await self._lock_run(conn, run_id)
            state = await self._load_persisted_evidence_state(run_id, conn=conn)
            _validate_route_identity_lineage(
                previous=state.last_route_identity,
                current=route_identity,
            )
            expected_transition_seq = state.last_transition_seq + 1
            if normalized_proof.transition_seq != expected_transition_seq:
                raise EvidenceAppendError(
                    "evidence.transition_seq_conflict",
                    "transition_seq must advance one step at a time",
                    details={
                        "run_id": run_id,
                        "expected_transition_seq": expected_transition_seq,
                        "received_transition_seq": normalized_proof.transition_seq,
                    },
                )
            expected_event_seq = state.last_evidence_seq + 1
            if normalized_proof.event.evidence_seq != expected_event_seq:
                raise EvidenceAppendError(
                    "evidence_seq.conflict",
                    "transition event evidence_seq must advance one step at a time",
                    details={
                        "run_id": run_id,
                        "expected_evidence_seq": expected_event_seq,
                        "received_evidence_seq": normalized_proof.event.evidence_seq,
                    },
                )
            if normalized_proof.receipt.evidence_seq != normalized_proof.event.evidence_seq + 1:
                raise EvidenceAppendError(
                    "evidence_seq.conflict",
                    "transition receipt evidence_seq must immediately follow the event",
                    details={
                        "event_evidence_seq": normalized_proof.event.evidence_seq,
                        "receipt_evidence_seq": normalized_proof.receipt.evidence_seq,
                    },
                )
            if normalized_proof.event.route_identity != normalized_proof.receipt.route_identity:
                raise EvidenceAppendError(
                    "evidence.route_identity_mismatch",
                    "transition event and receipt must share route_identity",
                )
            if normalized_proof.event.transition_seq != normalized_proof.receipt.transition_seq:
                raise EvidenceAppendError(
                    "evidence.transition_seq_mismatch",
                    "transition event and receipt must share transition_seq",
                )
            if normalized_proof.event.transition_seq != normalized_proof.transition_seq:
                raise EvidenceAppendError(
                    "evidence.transition_seq_mismatch",
                    "transition proof transition_seq must match the envelope transition_seq",
                )
            if normalized_proof.event.workflow_id != normalized_proof.receipt.workflow_id:
                raise EvidenceAppendError(
                    "evidence.route_identity_mismatch",
                    "transition event and receipt must share workflow_id",
                )
            if normalized_proof.event.run_id != normalized_proof.receipt.run_id:
                raise EvidenceAppendError(
                    "evidence.route_identity_mismatch",
                    "transition event and receipt must share run_id",
                )
            if normalized_proof.event.request_id != normalized_proof.receipt.request_id:
                raise EvidenceAppendError(
                    "evidence.request_id_mismatch",
                    "transition event and receipt must share request_id",
                )
            if normalized_proof.receipt.causation_id not in {None, normalized_proof.event.event_id}:
                raise EvidenceAppendError(
                    "evidence.causation_mismatch",
                    "transition receipt causation_id must point at the transition event",
                    details={
                        "event_id": normalized_proof.event.event_id,
                        "receipt_causation_id": normalized_proof.receipt.causation_id,
                    },
                )
            if normalized_proof.event.causation_id not in {None, state.last_row_id}:
                raise EvidenceAppendError(
                    "evidence.causation_mismatch",
                    "transition event causation_id must point at the previous evidence row",
                    details={
                        "previous_row_id": state.last_row_id,
                        "event_causation_id": normalized_proof.event.causation_id,
                    },
                )
            if normalized_proof.event.causation_id is None and state.last_row_id is not None:
                normalized_proof = replace(
                    normalized_proof,
                    event=replace(normalized_proof.event, causation_id=state.last_row_id),
                )
            if normalized_proof.receipt.causation_id is None:
                normalized_proof = replace(
                    normalized_proof,
                    receipt=replace(
                        normalized_proof.receipt,
                        causation_id=normalized_proof.event.event_id,
                    ),
                )

            if state.last_route_identity is None:
                existing_run_id = await conn.fetchval(
                    "SELECT run_id FROM workflow_runs WHERE run_id = $1",
                    run_id,
                )
                if existing_run_id is not None:
                    existing_run_id = str(existing_run_id)
            else:
                existing_run_id = route_identity.run_id

            if state.last_route_identity is None and existing_run_id is None:
                req_json = _encode_jsonb(normalized_proof.receipt.inputs)
                def_id = f"workflow_definition.{route_identity.workflow_id}:v1"
                def_hash = f"sha256:{route_identity.workflow_id}"
                adm_id = f"admission:{run_id}"

                await conn.execute(
                    """
                    INSERT INTO workflow_definitions (
                        workflow_definition_id, workflow_id, schema_version,
                        definition_version, definition_hash, status,
                        request_envelope, normalized_definition, created_at,
                        supersedes_workflow_definition_id
                    ) VALUES ($1, $2, 1, 1, $3, 'active', $4::jsonb, $4::jsonb, $5, NULL)
                    ON CONFLICT DO NOTHING
                    """,
                    def_id,
                    route_identity.workflow_id,
                    def_hash,
                    req_json,
                    normalized_proof.event.occurred_at,
                )

                await conn.execute(
                    """
                    INSERT INTO admission_decisions (
                        admission_decision_id, workflow_id, request_id,
                        decision, reason_code, decided_at, decided_by,
                        policy_snapshot_ref, validation_result_ref, authority_context_ref
                    ) VALUES ($1, $2, $3, 'admit', 'auto_admit', $4, 'runtime', 'none', 'none', $5)
                    ON CONFLICT DO NOTHING
                    """,
                    adm_id,
                    route_identity.workflow_id,
                    route_identity.request_id,
                    normalized_proof.event.occurred_at,
                    route_identity.authority_context_ref,
                )

                await conn.execute(
                    """
                    INSERT INTO workflow_runs (
                        run_id, workflow_id, request_id, request_digest,
                        authority_context_digest, workflow_definition_id,
                        admitted_definition_hash, run_idempotency_key,
                        schema_version, request_envelope, context_bundle_id,
                    admission_decision_id, current_state, terminal_reason_code,
                    requested_at, admitted_at, started_at, finished_at, last_event_id
                ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, 1,
                        $9::jsonb, $10, $11, $13, NULL,
                        $12, $12, NULL, NULL, NULL
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    run_id,
                    route_identity.workflow_id,
                    route_identity.request_id,
                    f"sha256:{run_id}",
                    route_identity.authority_context_digest,
                    def_id,
                    def_hash,
                    f"idem:{run_id}",
                    req_json,
                    f"ctx:{run_id}",
                    adm_id,
                    normalized_proof.event.occurred_at,
                    normalized_proof.receipt.status,
                )

            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="workflow_event",
                    evidence_seq=normalized_proof.event.evidence_seq,
                    row_id=normalized_proof.event.event_id,
                    route_identity=normalized_proof.route_identity,
                    transition_seq=normalized_proof.transition_seq,
                    record=normalized_proof.event,
                ),
            )
            await self._insert_evidence_row(
                conn,
                EvidenceRow(
                    kind="receipt",
                    evidence_seq=normalized_proof.receipt.evidence_seq,
                    row_id=normalized_proof.receipt.receipt_id,
                    route_identity=normalized_proof.route_identity,
                    transition_seq=normalized_proof.transition_seq,
                    record=normalized_proof.receipt,
                ),
            )
            await self._update_workflow_run_state(
                conn,
                run_id,
                new_state=normalized_proof.receipt.status,
                reason_code=normalized_proof.receipt.failure_code or normalized_proof.event.reason_code,
                occurred_at=normalized_proof.receipt.finished_at,
                last_event_id=normalized_proof.event.event_id,
            )

        return EvidenceCommitResult(
            event_id=normalized_proof.event.event_id,
            receipt_id=normalized_proof.receipt.receipt_id,
            evidence_seq=normalized_proof.receipt.evidence_seq,
            committed_at=normalized_proof.receipt.finished_at,
        )

    def evidence_timeline(self, run_id: str) -> Sequence[EvidenceRow]:
        """Return evidence timeline from durable Postgres state."""
        return self._run(self._load_evidence_timeline(run_id))

    async def _load_evidence_timeline(self, run_id: str) -> tuple[EvidenceRow, ...]:
        conn = await self._ensure_conn()
        return await fetch_workflow_evidence_timeline(conn, run_id=run_id)

    def workflow_events(self, run_id: str) -> tuple[WorkflowEventV1, ...]:
        timeline = self.evidence_timeline(run_id)
        return tuple(
            row.record
            for row in timeline
            if row.kind == "workflow_event"
        )

    def receipts(self, run_id: str) -> tuple[ReceiptV1, ...]:
        timeline = self.evidence_timeline(run_id)
        return tuple(
            row.record
            for row in timeline
            if row.kind == "receipt"
        )

    def last_evidence_seq(self, run_id: str) -> int | None:
        timeline = self.evidence_timeline(run_id)
        if not timeline:
            return None
        return timeline[-1].evidence_seq

    async def close(self) -> None:
        """Close the owned connection if we created it."""
        if self._owns_conn and self._conn is not None:
            await self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Query helpers for CLI
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkflowRunSummary:
    """Lightweight summary of one workflow run for CLI display."""

    run_id: str
    workflow_id: str
    current_state: str
    terminal_reason_code: str | None
    requested_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    evidence_count: int = 0


async def _fetch_recent_runs(
    conn: asyncpg.Connection,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
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
        limit,
    )
    results = []
    for row in rows:
        results.append({
            "run_id": row["run_id"],
            "workflow_id": row["workflow_id"],
            "current_state": row["current_state"],
            "terminal_reason_code": row["terminal_reason_code"],
            "requested_at": row["requested_at"].isoformat() if row["requested_at"] else None,
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        })
    return results


async def _fetch_run_detail(
    conn: asyncpg.Connection,
    *,
    run_id: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
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
        run_id,
    )
    if row is None:
        return None

    # Count evidence rows
    event_count = await conn.fetchval(
        "SELECT count(*) FROM workflow_events WHERE run_id = $1",
        run_id,
    )
    receipt_count = await conn.fetchval(
        "SELECT count(*) FROM receipts WHERE run_id = $1",
        run_id,
    )

    return {
        "run_id": row["run_id"],
        "workflow_id": row["workflow_id"],
        "request_id": row["request_id"],
        "current_state": row["current_state"],
        "terminal_reason_code": row["terminal_reason_code"],
        "requested_at": row["requested_at"].isoformat() if row["requested_at"] else None,
        "admitted_at": row["admitted_at"].isoformat() if row["admitted_at"] else None,
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "last_event_id": row["last_event_id"],
        "schema_version": row["schema_version"],
        "workflow_definition_id": row["workflow_definition_id"],
        "admitted_definition_hash": row["admitted_definition_hash"],
        "admission_decision_id": row["admission_decision_id"],
        "context_bundle_id": row["context_bundle_id"],
        "evidence": {
            "workflow_events": event_count,
            "receipts": receipt_count,
            "total": event_count + receipt_count,
        },
    }


def query_recent_runs(*, limit: int = 20) -> list[dict[str, Any]]:
    """Query recent workflow runs from Postgres (sync wrapper)."""
    async def _query():
        conn = await connect_workflow_database()
        try:
            return await _fetch_recent_runs(conn, limit=limit)
        finally:
            await conn.close()

    return _run_async(_query())


def query_run_detail(run_id: str) -> dict[str, Any] | None:
    """Query full detail for one workflow run from Postgres (sync wrapper)."""
    async def _query():
        conn = await connect_workflow_database()
        try:
            return await _fetch_run_detail(conn, run_id=run_id)
        finally:
            await conn.close()

    return _run_async(_query())
