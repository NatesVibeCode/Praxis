"""Tests for defensive route_identity reads in the postgres evidence reader.

Inspect must keep rendering even when persisted evidence rows lack the
``route_identity`` lineage block — a sentinel is returned and a structured
``DataQualityIssue`` is attached so operators can see what's wrong.
"""

from __future__ import annotations

import pytest

from receipts import DataQualityIssue
from storage.postgres.evidence import _route_identity_from_lineage


def _call(payload: dict) -> tuple:
    return _route_identity_from_lineage(
        payload,
        kind="workflow_event",
        row_id="row-123",
        evidence_seq=42,
        fallback_workflow_id="wf-A",
        fallback_run_id="run-A",
        fallback_request_id="req-A",
    )


def test_well_formed_payload_returns_no_issues() -> None:
    payload = {
        "route_identity": {
            "workflow_id": "wf-X",
            "run_id": "run-X",
            "request_id": "req-X",
            "authority_context_ref": "ctx-1",
            "authority_context_digest": "digest-1",
            "claim_id": "claim-1",
            "attempt_no": 2,
            "transition_seq": 7,
        },
        "transition_seq": 7,
    }
    route_identity, issues = _call(payload)
    assert issues == ()
    assert route_identity.workflow_id == "wf-X"
    assert route_identity.attempt_no == 2
    assert route_identity.transition_seq == 7


def test_missing_route_identity_returns_sentinel_and_issue() -> None:
    payload = {"transition_seq": 3}
    route_identity, issues = _call(payload)
    assert len(issues) == 1
    issue = issues[0]
    assert isinstance(issue, DataQualityIssue)
    assert issue.reason_code == "workflow.inspect.missing_route_identity"
    assert issue.row_id == "row-123"
    assert issue.evidence_seq == 42
    assert route_identity.workflow_id == "wf-A"
    assert route_identity.run_id == "run-A"
    assert route_identity.request_id == "req-A"
    assert route_identity.authority_context_ref == "missing"
    assert route_identity.transition_seq == 3


def test_route_identity_present_but_missing_subfield_emits_issue() -> None:
    payload = {
        "route_identity": {
            "workflow_id": "wf-X",
            "run_id": "run-X",
            "request_id": "req-X",
            "authority_context_ref": "ctx-1",
            "authority_context_digest": "digest-1",
        },
        "transition_seq": 1,
    }
    route_identity, issues = _call(payload)
    assert any(
        issue.reason_code == "workflow.inspect.missing_lineage_field"
        for issue in issues
    )
    assert route_identity.claim_id == "missing"
    assert route_identity.attempt_no == 1
    assert route_identity.transition_seq == 0


def test_string_encoded_lineage_is_accepted() -> None:
    import json

    payload = {
        "route_identity": json.dumps(
            {
                "workflow_id": "wf-X",
                "run_id": "run-X",
                "request_id": "req-X",
                "authority_context_ref": "ctx-1",
                "authority_context_digest": "digest-1",
                "claim_id": "claim-1",
                "attempt_no": 1,
                "transition_seq": 0,
            }
        ),
        "transition_seq": 0,
    }
    route_identity, issues = _call(payload)
    assert issues == ()
    assert route_identity.claim_id == "claim-1"
