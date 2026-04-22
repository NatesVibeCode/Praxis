from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

import _pg_test_conn

# Set WORKFLOW_DATABASE_URL for tests that need Postgres using the repo-local
# authority path, and ensure the default test database actually exists.
if "WORKFLOW_DATABASE_URL" not in os.environ:
    os.environ["WORKFLOW_DATABASE_URL"] = _pg_test_conn.ensure_test_database_ready()

# Ensure Postgres tools (pg_ctl, psql) are discoverable without baking in a
# package-manager path. CI/dev harnesses can set PRAXIS_POSTGRES_BIN_DIR when
# PATH does not already expose the tools.
_pg_bin_dir = os.environ.get("PRAXIS_POSTGRES_BIN_DIR", "").strip()
if _pg_bin_dir and os.path.isdir(_pg_bin_dir) and _pg_bin_dir not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _pg_bin_dir + os.pathsep + os.environ.get("PATH", "")

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
