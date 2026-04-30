"""Tools: praxis_virtual_lab_sandbox_promotion_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_virtual_lab_sandbox_promotion_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record sandbox promotion and drift proof through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording Virtual Lab sandbox promotion",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_sandbox_promotion_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - sandbox promotion record {status}",
        )
    return result


def tool_praxis_virtual_lab_sandbox_promotion_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read sandbox promotion and drift proof through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading Virtual Lab sandbox promotions",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_sandbox_promotion_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - sandbox promotion read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_virtual_lab_sandbox_promotion_record": (
        tool_praxis_virtual_lab_sandbox_promotion_record,
        {
            "kind": "write",
            "operation_names": ["virtual_lab_sandbox_promotion_record"],
            "description": (
                "Persist live sandbox promotion manifests, required simulation "
                "verifier proof refs, execution/readback evidence, "
                "predicted-vs-actual comparison reports, drift ledgers, "
                "handoff refs, and stop/continue summaries through the CQRS "
                "gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "manifest": {
                        "type": "object",
                        "description": "PromotionManifest JSON packet from runtime.virtual_lab.sandbox_drift.",
                    },
                    "candidate_records": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": (
                            "One record per manifest candidate with candidate_id, "
                            "simulation_run_id, execution, evidence_package, "
                            "checks, and classifications."
                        ),
                    },
                    "promotion_record_id": {"type": "string"},
                    "summary_id": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "require_simulation_verifier_proof": {"type": "boolean"},
                },
                "required": ["manifest", "candidate_records"],
            },
            "type_contract": {
                "record_virtual_lab_sandbox_promotion": {
                    "consumes": [
                        "virtual_lab.sandbox_promotion_manifest",
                        "virtual_lab.simulation_run",
                        "virtual_lab.sandbox_execution_record",
                        "virtual_lab.sandbox_readback_evidence",
                        "virtual_lab.predicted_actual_check",
                        "virtual_lab.drift_classification",
                    ],
                    "produces": [
                        "virtual_lab.sandbox_promotion_record",
                        "virtual_lab.sandbox_comparison_report",
                        "virtual_lab.sandbox_drift_ledger",
                        "virtual_lab.sandbox_stop_continue_summary",
                        "authority_operation_receipt",
                        "authority_event.virtual_lab_sandbox_promotion.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_virtual_lab_sandbox_promotion_read": (
        tool_praxis_virtual_lab_sandbox_promotion_read,
        {
            "kind": "analytics",
            "operation_names": ["virtual_lab_sandbox_promotion_read"],
            "description": (
                "Read persisted live sandbox promotion records, readback "
                "evidence, predicted-vs-actual reports, drift classifications, "
                "handoffs, and stop/continue recommendations through the CQRS "
                "gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_records",
                            "describe_record",
                            "list_drift",
                            "list_handoffs",
                            "list_readback_evidence",
                        ],
                    },
                    "promotion_record_id": {"type": "string"},
                    "manifest_id": {"type": "string"},
                    "candidate_id": {"type": "string"},
                    "simulation_run_id": {"type": "string"},
                    "recommendation": {"type": "string"},
                    "comparison_status": {"type": "string"},
                    "reason_code": {"type": "string"},
                    "severity": {"type": "string"},
                    "layer": {"type": "string"},
                    "disposition": {"type": "string"},
                    "handoff_kind": {"type": "string"},
                    "handoff_status": {"type": "string"},
                    "available": {"type": "boolean"},
                    "trusted": {"type": "boolean"},
                    "include_candidates": {"type": "boolean"},
                    "include_executions": {"type": "boolean"},
                    "include_readback": {"type": "boolean"},
                    "include_reports": {"type": "boolean"},
                    "include_drift": {"type": "boolean"},
                    "include_handoffs": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
            },
            "type_contract": {
                "read_virtual_lab_sandbox_promotion": {
                    "consumes": [
                        "virtual_lab.sandbox_promotion_record_id",
                        "virtual_lab.sandbox_candidate_id",
                        "virtual_lab.drift_reason_code",
                    ],
                    "produces": [
                        "virtual_lab.sandbox_promotion_records",
                        "virtual_lab.sandbox_readback_evidence",
                        "virtual_lab.sandbox_drift_classifications",
                        "virtual_lab.sandbox_handoffs",
                    ],
                }
            },
        },
    ),
}
