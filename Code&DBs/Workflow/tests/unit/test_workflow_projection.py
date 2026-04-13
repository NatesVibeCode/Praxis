from __future__ import annotations

from datetime import datetime, timezone

from receipts.evidence import EvidenceRow, ReceiptV1, WorkflowEventV1
from runtime.domain import RouteIdentity
from runtime.workflow_projection import project_workflow_result


def test_project_workflow_result_uses_failure_reason_when_terminal_event_claims_success():
    route_identity = RouteIdentity(
        workflow_id="workflow.test",
        run_id="run_fail",
        request_id="req_fail",
        authority_context_ref="ctx.ref",
        authority_context_digest="ctx.digest",
        claim_id="claim_fail",
        attempt_no=1,
        transition_seq=1,
    )
    started_at = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 8, 12, 0, 5, tzinfo=timezone.utc)
    timeline = (
        EvidenceRow(
            kind="workflow_event",
            evidence_seq=1,
            row_id="event_1",
            route_identity=route_identity,
            transition_seq=1,
            record=WorkflowEventV1(
                event_id="event_1",
                event_type="runtime.workflow_succeeded",
                schema_version=1,
                workflow_id="workflow.test",
                run_id="run_fail",
                request_id="req_fail",
                route_identity=route_identity,
                transition_seq=1,
                evidence_seq=1,
                occurred_at=started_at,
                actor_type="runtime",
                reason_code="runtime.workflow_succeeded",
                payload={},
            ),
        ),
        EvidenceRow(
            kind="receipt",
            evidence_seq=2,
            row_id="receipt_1",
            route_identity=route_identity,
            transition_seq=1,
            record=ReceiptV1(
                receipt_id="receipt_1",
                receipt_type="node_execution",
                schema_version=1,
                workflow_id="workflow.test",
                run_id="run_fail",
                request_id="req_fail",
                route_identity=route_identity,
                transition_seq=1,
                evidence_seq=2,
                started_at=started_at,
                finished_at=finished_at,
                executor_type="cli_llm",
                status="failed",
                inputs={},
                outputs={"completion": None},
                node_id="llm",
                failure_code="cli_adapter.nonzero_exit",
            ),
        ),
    )

    projected = project_workflow_result(
        run_id="run_fail",
        timeline=timeline,
        spec_provider_slug="openai",
        spec_model_slug="gpt-5.4",
        spec_adapter_type="cli_llm",
    )

    assert projected["status"] == "failed"
    assert projected["reason_code"] == "cli_adapter.nonzero_exit"
    assert projected["failure_code"] == "cli_adapter.nonzero_exit"
