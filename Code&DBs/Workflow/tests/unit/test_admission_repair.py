from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from storage.postgres import (
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
)
from runtime import admission_repair


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, *, row):
        self.row = row
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.fetches: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object):
        self.fetches.append((query, args))
        return self.row

    async def execute(self, query: str, *args: object):
        self.executed.append((query, args))
        return "UPDATE 1"

    def transaction(self):
        return _FakeTransaction()


def _submission(current_state: str = "claim_accepted") -> WorkflowAdmissionSubmission:
    decided_at = datetime(2026, 4, 17, 8, 0, tzinfo=timezone.utc)
    return WorkflowAdmissionSubmission(
        decision=WorkflowAdmissionDecisionWrite(
            admission_decision_id="admission.alpha",
            workflow_id="workflow.alpha",
            request_id="request.alpha",
            decision="admit",
            reason_code="policy.admission_allowed",
            decided_at=decided_at,
            decided_by="policy.intake",
            policy_snapshot_ref="policy.snapshot.alpha",
            validation_result_ref="validation.alpha",
            authority_context_ref="context.alpha",
        ),
        run=WorkflowRunWrite(
            run_id="run:alpha",
            workflow_id="workflow.alpha",
            request_id="request.alpha",
            request_digest="digest.alpha",
            authority_context_digest="authority.digest.alpha",
            workflow_definition_id="workflow_definition.alpha:v1",
            admitted_definition_hash="sha256:alpha",
            run_idempotency_key="request.alpha",
            schema_version=1,
            request_envelope={"workflow_id": "workflow.alpha"},
            context_bundle_id="context.alpha",
            admission_decision_id="admission.alpha",
            current_state=current_state,
            requested_at=decided_at,
            admitted_at=decided_at,
            terminal_reason_code=None,
            started_at=None,
            finished_at=None,
            last_event_id=None,
        ),
    )


def _timeline(status: str, *, evidence_seq: int = 4):
    return (SimpleNamespace(record=SimpleNamespace(status=status), evidence_seq=evidence_seq),)


def test_repair_authority_returns_existing_fully_seeded_submission(monkeypatch):
    submission = _submission()
    conn = _FakeConn(
        row={
            "run_id": submission.run.run_id,
            "workflow_id": submission.run.workflow_id,
            "request_id": submission.run.request_id,
            "request_digest": submission.run.request_digest,
            "workflow_definition_id": submission.run.workflow_definition_id,
            "admitted_definition_hash": submission.run.admitted_definition_hash,
            "current_state": submission.run.current_state,
            "run_idempotency_key": submission.run.run_idempotency_key,
            "context_bundle_id": submission.run.context_bundle_id,
            "authority_context_digest": submission.run.authority_context_digest,
            "admission_decision_id": submission.run.admission_decision_id,
            "request_envelope": submission.run.request_envelope,
        }
    )

    class _FakeWriter:
        def __init__(self, *, conn):
            self._conn = conn

        async def evidence_timeline_async(self, _run_id: str):
            return _timeline("claim_accepted")

    monkeypatch.setattr(admission_repair, "PostgresEvidenceWriter", _FakeWriter)

    async def _unexpected(*_args, **_kwargs):
        raise AssertionError("should not attempt repair for fully seeded submission")

    monkeypatch.setattr(admission_repair, "persist_workflow_admission", _unexpected)
    monkeypatch.setattr(admission_repair, "persist_submission_evidence_async", _unexpected)
    monkeypatch.setattr(admission_repair, "append_admission_transition_async", _unexpected)

    result = asyncio.run(
        admission_repair.repair_or_seed_submission_evidence(conn, submission=submission)
    )

    assert result.run_id == submission.run.run_id
    assert conn.executed == []


def test_repair_authority_reuses_claim_received_timeline_and_appends_admission(monkeypatch):
    submission = _submission()
    conn = _FakeConn(
        row={
            "run_id": submission.run.run_id,
            "workflow_id": submission.run.workflow_id,
            "request_id": submission.run.request_id,
            "request_digest": submission.run.request_digest,
            "workflow_definition_id": submission.run.workflow_definition_id,
            "admitted_definition_hash": submission.run.admitted_definition_hash,
            "current_state": "claim_received",
            "run_idempotency_key": submission.run.run_idempotency_key,
            "context_bundle_id": submission.run.context_bundle_id,
            "authority_context_digest": submission.run.authority_context_digest,
            "admission_decision_id": submission.run.admission_decision_id,
            "request_envelope": submission.run.request_envelope,
        }
    )
    captured: dict[str, object] = {}

    class _FakeWriter:
        def __init__(self, *, conn):
            self._conn = conn

        async def evidence_timeline_async(self, _run_id: str):
            return _timeline("claim_received", evidence_seq=2)

    async def _append(writer, *, admission, submission_evidence_seq):
        captured["admission"] = admission
        captured["submission_evidence_seq"] = submission_evidence_seq
        return SimpleNamespace(evidence_seq=4)

    async def _unexpected(*_args, **_kwargs):
        raise AssertionError("bootstrap should not run when claim_received evidence exists")

    monkeypatch.setattr(admission_repair, "PostgresEvidenceWriter", _FakeWriter)
    monkeypatch.setattr(admission_repair, "append_admission_transition_async", _append)
    monkeypatch.setattr(admission_repair, "persist_workflow_admission", _unexpected)
    monkeypatch.setattr(admission_repair, "persist_submission_evidence_async", _unexpected)

    result = asyncio.run(
        admission_repair.repair_or_seed_submission_evidence(conn, submission=submission)
    )

    assert result.run_id == submission.run.run_id
    assert captured["submission_evidence_seq"] == 2
    assert captured["admission"].route_identity.run_id == submission.run.run_id
    assert any("SET current_state = $2" in query for query, _args in conn.executed)
