from __future__ import annotations

from observability.read_models import InspectionReadModel, ProjectionCompleteness, ProjectionWatermark
from observability.status_observability import build_frontdoor_observability


def test_frontdoor_observability_snapshot_marks_missing_inspection_as_degraded() -> None:
    snapshot = build_frontdoor_observability(
        run_id="run.alpha",
        run_row={
            "workflow_id": "workflow.alpha",
            "request_id": "request.alpha",
            "request_digest": "digest.alpha",
            "workflow_definition_id": "workflow_definition.alpha.v1",
            "admitted_definition_hash": "sha256:alpha",
            "current_state": "claim_accepted",
            "terminal_reason_code": None,
            "run_idempotency_key": "request.alpha",
            "request_envelope": {
                "name": "alpha-plan",
                "definition_hash": "sha256:alpha",
            },
        },
        inspection=None,
        jobs=[
            {
                "label": "build.codegen",
                "status": "running",
            }
        ],
        packet_inspection=None,
        packet_inspection_source="missing",
        contract_drift_refs=("workflow_runs.packet_inspection_column_missing",),
    )

    payload = snapshot.to_json()
    assert payload["kind"] == "frontdoor_observability"
    assert payload["health_state"] == "degraded"
    assert payload["job_count"] == 1
    assert payload["running_job_count"] == 1
    assert payload["job_status_counts"] == {"running": 1}
    assert payload["contract_drift"]["status"] == "drifted"
    assert payload["contract_drift"]["issues"][0]["issue_code"] == "workflow_runs.packet_inspection_column_missing"
    assert payload["provenance_coverage"]["coverage_rate"] == 0.5
    assert payload["run_identity"]["dedupe_decision"] == "stable_idempotency_key"
    assert payload["failure_taxonomy"]["dominant_category"] == "in_progress"
    assert payload["anomaly_digest"]["headline"] == "Status path detected 1 contract drift issue(s)"
    assert payload["anomaly_digest"]["focus_refs"] == [
        "inspection:missing",
        "workflow_runs.packet_inspection_column_missing",
    ]


def test_frontdoor_observability_snapshot_reports_healthy_when_all_surfaces_align() -> None:
    snapshot = build_frontdoor_observability(
        run_id="run.beta",
        run_row={
            "workflow_id": "workflow.beta",
            "request_id": "request.beta",
            "request_digest": "digest.beta",
            "workflow_definition_id": "workflow_definition.beta.v1",
            "admitted_definition_hash": "sha256:beta",
            "current_state": "running",
            "terminal_reason_code": None,
            "run_idempotency_key": "request.beta",
            "request_envelope": {
                "name": "beta-plan",
                "definition_hash": "sha256:beta",
            },
        },
        inspection=InspectionReadModel(
            run_id="run.beta",
            request_id="request.beta",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=2, source="canonical_evidence"),
            evidence_refs=("event.1", "receipt.2"),
            current_state="running",
            node_timeline=("node_a:running",),
            terminal_reason=None,
        ),
        jobs=[
            {
                "label": "build.codegen",
                "status": "succeeded",
            },
            {
                "label": "review.pack",
                "status": "succeeded",
            },
        ],
        packet_inspection={
            "current_packet": {
                "packet_revision": "packet.beta.1",
                "packet_hash": "packet-hash-beta",
            },
            "drift": {"status": "aligned"},
        },
        packet_inspection_source="materialized",
    )

    payload = snapshot.to_json()
    assert payload["health_state"] == "healthy"
    assert payload["job_completion_rate"] == 1.0
    assert payload["failure_rate"] == 0.0
    assert payload["inspection_completeness"] == {
        "is_complete": True,
        "missing_evidence_refs": [],
    }
    assert payload["contract_drift"]["status"] == "aligned"
    assert payload["provenance_coverage"]["authoritative_count"] == 4
    assert payload["run_identity"]["packet_revision"] == "packet.beta.1"
    assert payload["failure_taxonomy"]["dominant_category"] == "success"
    assert payload["anomaly_digest"]["headline"] == "Run observations are internally consistent"
    assert payload["anomaly_digest"]["packet_drift_status"] == "aligned"
