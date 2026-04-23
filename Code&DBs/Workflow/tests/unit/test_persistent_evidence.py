from __future__ import annotations

import asyncio
import sys
import threading
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from adapters.evidence import build_claim_received_proof, build_transition_proof
from runtime.domain import (
    LifecycleTransition,
    RouteIdentity,
    RunState,
    RuntimeBoundaryError,
)
from runtime.persistent_evidence import PostgresEvidenceWriter, _proof_run_state_update


def _route_identity(run_id: str) -> RouteIdentity:
    return RouteIdentity(
        workflow_id="workflow.persistent-evidence",
        run_id=run_id,
        request_id=f"request:{run_id}",
        authority_context_ref=f"authority:{run_id}",
        authority_context_digest=f"sha256:{run_id}",
        claim_id=f"claim:{run_id}",
        transition_seq=1,
    )


def _claim_received_proof(run_id: str):
    return build_claim_received_proof(
        route_identity=_route_identity(run_id),
        event_id=f"workflow_event:{run_id}:1",
        receipt_id=f"receipt:{run_id}:2",
        evidence_seq=1,
        transition_seq=1,
        request_payload={"payload": "value"},
        admitted_definition_ref=f"workflow_definition.{run_id}",
        admitted_definition_hash=f"sha256:{run_id}",
        occurred_at=datetime(2026, 4, 9, 15, 0, tzinfo=timezone.utc),
    )


def _node_started_proof(run_id: str):
    return build_transition_proof(
        route_identity=replace(_route_identity(run_id), transition_seq=2),
        transition_seq=2,
        event_id=f"workflow_event:{run_id}:3",
        receipt_id=f"receipt:{run_id}:4",
        event_type="node_started",
        receipt_type="node_start_receipt",
        reason_code="runtime.node_started",
        evidence_seq=3,
        occurred_at=datetime(2026, 4, 9, 15, 1, tzinfo=timezone.utc),
        status="running",
        payload={"node_id": "node-1"},
        inputs={"node_id": "node-1"},
        outputs={"node_id": "node-1", "status": "running"},
        node_id="node-1",
        causation_id=f"receipt:{run_id}:2",
    )


def test_proof_run_state_update_accepts_claim_received_bootstrap() -> None:
    state_update = _proof_run_state_update(_claim_received_proof("run-bootstrap"))

    assert state_update is not None
    assert state_update.from_state is None
    assert state_update.to_state is RunState.CLAIM_RECEIVED
    assert state_update.expected_current_state == RunState.CLAIM_RECEIVED.value
    assert state_update.is_submission_bootstrap is True


def test_proof_run_state_update_parses_explicit_workflow_transition() -> None:
    proof = build_transition_proof(
        route_identity=replace(_route_identity("run-transition"), transition_seq=2),
        transition_seq=2,
        event_id="workflow_event:run-transition:3",
        receipt_id="receipt:run-transition:4",
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        reason_code="claim.validated",
        evidence_seq=3,
        occurred_at=datetime(2026, 4, 9, 15, 1, tzinfo=timezone.utc),
        status=RunState.CLAIM_VALIDATING.value,
        payload={
            "from_state": RunState.CLAIM_RECEIVED.value,
            "to_state": RunState.CLAIM_VALIDATING.value,
        },
    )

    state_update = _proof_run_state_update(proof)

    assert state_update is not None
    assert state_update.from_state is RunState.CLAIM_RECEIVED
    assert state_update.to_state is RunState.CLAIM_VALIDATING
    assert state_update.is_submission_bootstrap is False


def test_proof_run_state_update_leaves_node_evidence_alone() -> None:
    assert _proof_run_state_update(_node_started_proof("run-node")) is None


def test_commit_submission_raises_when_persistence_fails() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")

    async def _boom(**_kwargs):
        raise RuntimeError("db unavailable")

    writer._persist_submission = _boom

    try:
        with pytest.raises(RuntimeBoundaryError, match="persistent evidence submission failed"):
            writer.commit_submission(
                route_identity=_route_identity("run-persist-fail"),
                admitted_definition_ref="workflow_definition.run-persist-fail",
                admitted_definition_hash="sha256:run-persist-fail",
                request_payload={"payload": "value"},
            )
    finally:
        writer._bridge.close()


def test_commit_submission_raises_when_run_row_is_missing_after_persist() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")

    async def _persist(**_kwargs):
        raise RuntimeBoundaryError(
            "persistent evidence missing workflow_runs row for run-missing-row"
        )

    writer._persist_submission = _persist

    try:
        with pytest.raises(RuntimeBoundaryError, match="missing workflow_runs row"):
            writer.commit_submission(
                route_identity=_route_identity("run-missing-row"),
                admitted_definition_ref="workflow_definition.run-missing-row",
                admitted_definition_hash="sha256:run-missing-row",
                request_payload={"payload": "value"},
            )
    finally:
        writer._bridge.close()


def test_persist_submission_async_uses_public_async_contract() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    expected = object()

    async def _persist(**_kwargs):
        return expected

    try:
        writer._persist_submission = _persist
        result = asyncio.run(
            writer.persist_submission_async(
                route_identity=_route_identity("run-persist-async"),
                admitted_definition_ref="workflow_definition.run-persist-async",
                admitted_definition_hash="sha256:run-persist-async",
                request_payload={"payload": "value"},
            )
        )
        assert result is expected
    finally:
        writer._bridge.close()


def test_commit_transition_raises_when_persistence_fails() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    route_identity = _route_identity("run-transition-fail")

    async def _boom(**_kwargs):
        raise RuntimeError("db unavailable")

    writer._persist_transition = _boom

    transition = LifecycleTransition(
        route_identity=replace(route_identity, lease_id="lease-1", transition_seq=2),
        from_state=RunState.CLAIM_RECEIVED,
        to_state=RunState.CLAIM_VALIDATING,
        reason_code="claim.validated",
        evidence_seq=3,
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        occurred_at=datetime(2026, 4, 9, 15, 1, tzinfo=timezone.utc),
    )

    try:
        with pytest.raises(RuntimeBoundaryError, match="persistent evidence transition failed"):
            writer.commit_transition(transition=transition)
    finally:
        writer._bridge.close()


def test_append_transition_proof_raises_when_persistence_fails() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    proof = _claim_received_proof("run-proof-fail")

    async def _boom(**_kwargs):
        raise RuntimeError("db unavailable")

    writer._persist_proof = _boom

    try:
        with pytest.raises(RuntimeBoundaryError, match="persistent evidence proof append failed"):
            writer.append_transition_proof(proof)
    finally:
        writer._bridge.close()


def test_append_transition_proof_async_uses_public_async_contract() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    proof = _claim_received_proof("run-proof-async")
    expected = object()

    async def _persist(**_kwargs):
        return expected

    try:
        writer._persist_proof = _persist
        result = asyncio.run(writer.append_transition_proof_async(proof))
        assert result is expected
    finally:
        writer._bridge.close()


def test_evidence_timeline_does_not_fallback_to_in_memory() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")

    async def _boom(_run_id: str):
        raise RuntimeError("db unavailable")

    writer._load_evidence_timeline = _boom

    try:
        with pytest.raises(RuntimeError, match="db unavailable"):
            writer.evidence_timeline("run-no-fallback")
    finally:
        writer._bridge.close()


def test_close_blocking_uses_writer_bridge_for_loop_safe_shutdown() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    events: list[str] = []

    class _FakeConn:
        async def close(self) -> None:
            events.append("conn.close")

    class _FakeBridge:
        def run(self, coro):
            events.append("bridge.run")
            return asyncio.run(coro)

        def close(self) -> None:
            events.append("bridge.close")

    writer._conn = _FakeConn()
    writer._owns_conn = True
    writer._bridge = _FakeBridge()

    writer.close_blocking()

    assert events == ["bridge.run", "conn.close", "bridge.close"]
    assert writer._conn is None


def test_commit_submission_from_running_loop_uses_dedicated_bridge_thread() -> None:
    writer = PostgresEvidenceWriter(database_url="postgresql://unused")
    expected = object()
    events: list[tuple[str, int]] = []

    class _FakeConn:
        async def close(self) -> None:
            events.append(("close", threading.get_ident()))

    async def _persist(**_kwargs):
        events.append(("persist", threading.get_ident()))
        return expected

    writer._conn = _FakeConn()
    writer._owns_conn = True
    writer._persist_submission = _persist

    async def _invoke_from_running_loop():
        result = writer.commit_submission(
            route_identity=_route_identity("run-loop-safe"),
            admitted_definition_ref="workflow_definition.run-loop-safe",
            admitted_definition_hash="sha256:run-loop-safe",
            request_payload={"payload": "value"},
        )
        writer.close_blocking()
        return result

    result = asyncio.run(_invoke_from_running_loop())

    assert result is expected
    assert [event for event, _thread_id in events] == ["persist", "close"]
    assert len({thread_id for _event, thread_id in events}) == 1
