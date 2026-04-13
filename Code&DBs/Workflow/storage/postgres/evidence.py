"""Evidence reading and timeline queries for the Postgres control plane."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json

import asyncpg
from receipts import (
    ArtifactRef,
    DecisionRef,
    EvidenceRow,
    ReceiptV1,
    RouteIdentity,
    WorkflowEventV1,
)

from .validators import PostgresStorageError

WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


@dataclass(frozen=True, slots=True)
class PostgresEvidenceReader:
    """Read canonical evidence rows from explicit Postgres storage only."""

    database_url: str | None = None
    env: Mapping[str, str] | None = None

    def operator_frame_repository(self):
        """Return the canonical Postgres operator-frame authority for this reader."""

        from .connection import ensure_postgres_available
        from .operator_frame_repository import PostgresOperatorFrameRepository

        env = (
            {WORKFLOW_DATABASE_URL_ENV: self.database_url}
            if isinstance(self.database_url, str) and self.database_url.strip()
            else self.env
        )
        return PostgresOperatorFrameRepository(ensure_postgres_available(env=env))

    def current_state_for_run(self, run_id: str) -> str | None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.load_current_state(run_id=run_id))
        raise PostgresStorageError(
            "postgres.read_loop_active",
            "sync current_state_for_run() requires an explicit non-async call boundary",
            details={"run_id": run_id},
        )

    def evidence_timeline(self, run_id: str) -> tuple[EvidenceRow, ...]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.load_evidence_timeline(run_id=run_id))
        raise PostgresStorageError(
            "postgres.read_loop_active",
            "sync evidence_timeline() requires an explicit non-async call boundary",
            details={"run_id": run_id},
        )

    async def load_evidence_timeline(self, *, run_id: str) -> tuple[EvidenceRow, ...]:
        # Import here to avoid circular dependency
        from .connection import resolve_workflow_database_url

        database_url = (
            resolve_workflow_database_url(
                env={WORKFLOW_DATABASE_URL_ENV: self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await fetch_workflow_evidence_timeline(conn, run_id=run_id)
        finally:
            await conn.close()

    async def load_current_state(self, *, run_id: str) -> str | None:
        from .connection import resolve_workflow_database_url

        database_url = (
            resolve_workflow_database_url(
                env={WORKFLOW_DATABASE_URL_ENV: self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            state = await conn.fetchval(
                "SELECT current_state FROM workflow_runs WHERE run_id = $1",
                run_id,
            )
            if state is None:
                return None
            return str(state)
        finally:
            await conn.close()


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _route_identity_from_lineage(value: Mapping[str, object]) -> RouteIdentity:
    lineage = _json_value(value["route_identity"])
    if not isinstance(lineage, Mapping):
        raise PostgresStorageError(
            "postgres.read_invalid_evidence",
            "persisted evidence row is missing route_identity lineage",
        )
    return RouteIdentity(
        workflow_id=str(lineage["workflow_id"]),
        run_id=str(lineage["run_id"]),
        request_id=str(lineage["request_id"]),
        authority_context_ref=str(lineage["authority_context_ref"]),
        authority_context_digest=str(lineage["authority_context_digest"]),
        claim_id=str(lineage["claim_id"]),
        lease_id=lineage.get("lease_id"),
        proposal_id=lineage.get("proposal_id"),
        promotion_decision_id=lineage.get("promotion_decision_id"),
        attempt_no=int(lineage["attempt_no"]),
        transition_seq=int(lineage["transition_seq"]),
    )


def _artifact_refs(value: object) -> tuple[ArtifactRef, ...]:
    raw_value = _json_value(value)
    if not isinstance(raw_value, Sequence) or isinstance(
        raw_value,
        (str, bytes, bytearray),
    ):
        return ()
    artifacts: list[ArtifactRef] = []
    for item in raw_value:
        if not isinstance(item, Mapping):
            raise PostgresStorageError(
                "postgres.read_invalid_evidence",
                "persisted receipt artifacts must be mappings",
            )
        artifacts.append(
            ArtifactRef(
                artifact_id=str(item["artifact_id"]),
                artifact_type=str(item["artifact_type"]),
                content_hash=str(item["content_hash"]),
                storage_ref=str(item["storage_ref"]),
            )
        )
    return tuple(artifacts)


def _decision_refs(value: object) -> tuple[DecisionRef, ...]:
    raw_value = _json_value(value)
    if not isinstance(raw_value, Sequence) or isinstance(
        raw_value,
        (str, bytes, bytearray),
    ):
        return ()
    refs: list[DecisionRef] = []
    for item in raw_value:
        if not isinstance(item, Mapping):
            raise PostgresStorageError(
                "postgres.read_invalid_evidence",
                "persisted receipt decision_refs must be mappings",
            )
        refs.append(
            DecisionRef(
                decision_type=str(item["decision_type"]),
                decision_id=str(item["decision_id"]),
                reason_code=str(item["reason_code"]),
                source_table=str(item["source_table"]),
            )
        )
    return tuple(refs)


def _event_from_row(row: asyncpg.Record) -> EvidenceRow:
    payload = _json_value(row["payload"])
    if not isinstance(payload, Mapping):
        raise PostgresStorageError(
            "postgres.read_invalid_evidence",
            "persisted workflow_event payload must be a mapping",
        )
    route_identity = _route_identity_from_lineage(payload)
    transition_seq = int(payload["transition_seq"])
    event = WorkflowEventV1(
        event_id=row["event_id"],
        event_type=row["event_type"],
        schema_version=row["schema_version"],
        workflow_id=row["workflow_id"],
        run_id=row["run_id"],
        request_id=row["request_id"],
        route_identity=route_identity,
        transition_seq=transition_seq,
        evidence_seq=row["evidence_seq"],
        occurred_at=row["occurred_at"],
        actor_type=row["actor_type"],
        reason_code=row["reason_code"],
        payload=payload,
        causation_id=row["causation_id"],
        node_id=row["node_id"],
    )
    return EvidenceRow(
        kind="workflow_event",
        evidence_seq=event.evidence_seq,
        row_id=event.event_id,
        route_identity=route_identity,
        transition_seq=transition_seq,
        record=event,
    )


def _receipt_from_row(row: asyncpg.Record) -> EvidenceRow:
    inputs = _json_value(row["inputs"])
    outputs = _json_value(row["outputs"])
    if not isinstance(inputs, Mapping) or not isinstance(outputs, Mapping):
        raise PostgresStorageError(
            "postgres.read_invalid_evidence",
            "persisted receipt inputs and outputs must be mappings",
        )
    route_identity = _route_identity_from_lineage(inputs)
    transition_seq = int(inputs["transition_seq"])
    receipt = ReceiptV1(
        receipt_id=row["receipt_id"],
        receipt_type=row["receipt_type"],
        schema_version=row["schema_version"],
        workflow_id=row["workflow_id"],
        run_id=row["run_id"],
        request_id=row["request_id"],
        route_identity=route_identity,
        transition_seq=transition_seq,
        evidence_seq=row["evidence_seq"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        executor_type=row["executor_type"],
        status=row["status"],
        inputs=inputs,
        outputs=outputs,
        artifacts=_artifact_refs(row["artifacts"]),
        decision_refs=_decision_refs(row["decision_refs"]),
        causation_id=row["causation_id"],
        node_id=row["node_id"],
        attempt_no=row["attempt_no"],
        supersedes_receipt_id=row["supersedes_receipt_id"],
        failure_code=row["failure_code"],
    )
    return EvidenceRow(
        kind="receipt",
        evidence_seq=receipt.evidence_seq,
        row_id=receipt.receipt_id,
        route_identity=route_identity,
        transition_seq=transition_seq,
        record=receipt,
    )


async def fetch_workflow_evidence_timeline(
    conn: asyncpg.Connection,
    *,
    run_id: str,
) -> tuple[EvidenceRow, ...]:
    """Return one run's canonical persisted evidence rows in shared order."""

    event_rows = await conn.fetch(
        """
        SELECT
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
        FROM workflow_events
        WHERE run_id = $1
        ORDER BY evidence_seq
        """,
        run_id,
    )
    receipt_rows = await conn.fetch(
        """
        SELECT
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
        FROM receipts
        WHERE run_id = $1
        ORDER BY evidence_seq
        """,
        run_id,
    )
    combined = [_event_from_row(row) for row in event_rows]
    combined.extend(_receipt_from_row(row) for row in receipt_rows)
    return tuple(sorted(combined, key=lambda item: (item.evidence_seq, item.row_id)))
