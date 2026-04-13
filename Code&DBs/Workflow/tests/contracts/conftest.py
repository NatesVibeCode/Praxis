from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from adapters.evidence import build_claim_received_proof
from runtime import RouteIdentity


@pytest.fixture
def route_identity() -> RouteIdentity:
    return RouteIdentity(
        workflow_id="workflow-1",
        run_id="run-1",
        request_id="request-1",
        authority_context_ref="authority-context-1",
        authority_context_digest="authority-digest-1",
        claim_id="claim-1",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=1,
    )


@pytest.fixture
def occurred_at() -> datetime:
    return datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def request_payload() -> dict[str, object]:
    return {
        "node": "deterministic_task",
        "payload": {"answer": 42},
    }


@pytest.fixture
def admitted_definition_ref() -> str:
    return "workflow_definition:workflow-1:1"


@pytest.fixture
def admitted_definition_hash() -> str:
    return "sha256:deadbeef"


@pytest.fixture
def claim_received_proof(
    route_identity: RouteIdentity,
    request_payload: dict[str, object],
    admitted_definition_ref: str,
    admitted_definition_hash: str,
    occurred_at: datetime,
):
    return build_claim_received_proof(
        route_identity=route_identity,
        event_id="workflow_event:run-1:1",
        receipt_id="receipt:run-1:2",
        evidence_seq=1,
        transition_seq=1,
        request_payload=request_payload,
        admitted_definition_ref=admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        occurred_at=occurred_at,
    )
