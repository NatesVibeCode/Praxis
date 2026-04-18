"""Evidence reading and timeline queries for the Postgres control plane."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json

import asyncpg
from receipts import (
    ArtifactRef,
    DataQualityIssue,
    DecisionRef,
    EvidenceRow,
    ReceiptV1,
    RouteIdentity,
    WorkflowEventV1,
)

from .validators import PostgresStorageError

_MISSING_LINEAGE_HINT = (
    "row predates the route_identity contract — backfill required to restore "
    "full lineage; inspect surface continues to render with a sentinel"
)
_MISSING_LINEAGE_SENTINEL = "missing"

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


def _route_identity_from_lineage(
    payload: Mapping[str, object],
    *,
    kind: str,
    row_id: str,
    evidence_seq: int,
    fallback_workflow_id: str,
    fallback_run_id: str,
    fallback_request_id: str,
) -> tuple[RouteIdentity, tuple[DataQualityIssue, ...]]:
    issues: list[DataQualityIssue] = []

    raw_lineage = payload.get("route_identity") if "route_identity" in payload else None
    lineage = _json_value(raw_lineage) if raw_lineage is not None else None

    if not isinstance(lineage, Mapping):
        issues.append(
            DataQualityIssue(
                reason_code="workflow.inspect.missing_route_identity",
                kind=kind,
                row_id=row_id,
                evidence_seq=evidence_seq,
                hint=_MISSING_LINEAGE_HINT,
            )
        )
        sentinel = RouteIdentity(
            workflow_id=fallback_workflow_id,
            run_id=fallback_run_id,
            request_id=fallback_request_id,
            authority_context_ref=_MISSING_LINEAGE_SENTINEL,
            authority_context_digest=_MISSING_LINEAGE_SENTINEL,
            claim_id=_MISSING_LINEAGE_SENTINEL,
            attempt_no=1,
            transition_seq=int(payload.get("transition_seq") or 0),
        )
        return sentinel, tuple(issues)

    def _required_str(field: str, fallback: str) -> str:
        value = lineage.get(field)
        if value is None:
            issues.append(
                DataQualityIssue(
                    reason_code="workflow.inspect.missing_lineage_field",
                    kind=kind,
                    row_id=row_id,
                    evidence_seq=evidence_seq,
                    hint=f"route_identity.{field} missing — using fallback value",
                )
            )
            return fallback
        return str(value)

    def _required_int(field: str, fallback: int) -> int:
        value = lineage.get(field)
        if value is None:
            issues.append(
                DataQualityIssue(
                    reason_code="workflow.inspect.missing_lineage_field",
                    kind=kind,
                    row_id=row_id,
                    evidence_seq=evidence_seq,
                    hint=f"route_identity.{field} missing — using fallback value",
                )
            )
            return fallback
        return int(value)

    route_identity = RouteIdentity(
        workflow_id=_required_str("workflow_id", fallback_workflow_id),
        run_id=_required_str("run_id", fallback_run_id),
        request_id=_required_str("request_id", fallback_request_id),
        authority_context_ref=_required_str(
            "authority_context_ref", _MISSING_LINEAGE_SENTINEL
        ),
        authority_context_digest=_required_str(
            "authority_context_digest", _MISSING_LINEAGE_SENTINEL
        ),
        claim_id=_required_str("claim_id", _MISSING_LINEAGE_SENTINEL),
        lease_id=lineage.get("lease_id"),
        proposal_id=lineage.get("proposal_id"),
        promotion_decision_id=lineage.get("promotion_decision_id"),
        attempt_no=_required_int("attempt_no", 1),
        transition_seq=_required_int("transition_seq", 0),
    )
    return route_identity, tuple(issues)


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
    route_identity, issues = _route_identity_from_lineage(
        payload,
        kind="workflow_event",
        row_id=str(row["event_id"]),
        evidence_seq=int(row["evidence_seq"]),
        fallback_workflow_id=str(row["workflow_id"]),
        fallback_run_id=str(row["run_id"]),
        fallback_request_id=str(row["request_id"]),
    )
    transition_seq = int(payload.get("transition_seq") or route_identity.transition_seq)
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
        data_quality_issues=issues,
    )


def _receipt_from_row(row: asyncpg.Record) -> EvidenceRow:
    inputs = _json_value(row["inputs"])
    outputs = _json_value(row["outputs"])
    if not isinstance(inputs, Mapping) or not isinstance(outputs, Mapping):
        raise PostgresStorageError(
            "postgres.read_invalid_evidence",
            "persisted receipt inputs and outputs must be mappings",
        )
    route_identity, issues = _route_identity_from_lineage(
        inputs,
        kind="receipt",
        row_id=str(row["receipt_id"]),
        evidence_seq=int(row["evidence_seq"]),
        fallback_workflow_id=str(row["workflow_id"]),
        fallback_run_id=str(row["run_id"]),
        fallback_request_id=str(row["request_id"]),
    )
    transition_seq = int(inputs.get("transition_seq") or route_identity.transition_seq)
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
        data_quality_issues=issues,
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
