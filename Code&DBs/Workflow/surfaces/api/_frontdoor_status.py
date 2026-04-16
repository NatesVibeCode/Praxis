"""Status helpers for the native workflow frontdoor."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


def build_status_payload(
    frontdoor: Any,
    *,
    run_id: str,
    env: Mapping[str, str] | None,
    runtime_orchestrator_cls: type[Any],
    serialize_inspection: Callable[[Any], dict[str, Any]],
    build_frontdoor_observability: Callable[..., Any],
    json_compatible: Callable[[Any], Any],
    load_sync_status: Callable[[str], Mapping[str, Any]],
) -> dict[str, Any]:
    source, instance = frontdoor._resolve_instance(env=env)
    row, packet_inspection, jobs, observability_hints = frontdoor._run_sync_status_row(
        source,
        run_id=run_id,
    )
    inspection = None
    inspection_payload = None
    last_event_id = row.get("last_event_id")
    if isinstance(last_event_id, str) and last_event_id:
        inspection = runtime_orchestrator_cls(
            evidence_reader=frontdoor.evidence_reader_factory(source),
        ).inspect_run(run_id=run_id)
        inspection_payload = serialize_inspection(inspection)
    observability_payload = build_frontdoor_observability(
        run_id=run_id,
        run_row=row,
        inspection=inspection,
        jobs=jobs,
        packet_inspection=packet_inspection,
        packet_inspection_source=observability_hints.packet_inspection_source,
        contract_drift_refs=observability_hints.contract_drift_refs,
    ).to_json()
    sync_payload = load_sync_status(run_id)
    payload = {
        "native_instance": instance.to_contract(),
        "run": {
            "run_id": row["run_id"],
            "workflow_id": row["workflow_id"],
            "request_id": row["request_id"],
            "request_digest": row["request_digest"],
            "workflow_definition_id": row["workflow_definition_id"],
            "admitted_definition_hash": row["admitted_definition_hash"],
                "current_state": row["current_state"],
                "terminal_reason_code": row["terminal_reason_code"],
                "run_idempotency_key": row["run_idempotency_key"],
                "context_bundle_id": row["context_bundle_id"],
                "authority_context_digest": row["authority_context_digest"],
                "admission_decision_id": row["admission_decision_id"],
                "requested_at": json_compatible(row["requested_at"]),
                "admitted_at": json_compatible(row["admitted_at"]),
                "started_at": json_compatible(row["started_at"]),
                "finished_at": json_compatible(row["finished_at"]),
            "last_event_id": last_event_id,
            "persisted": True,
            "sync_status": sync_payload["sync_status"],
            "sync_cycle_id": sync_payload["sync_cycle_id"],
            "sync_error_count": sync_payload["sync_error_count"],
            "jobs": jobs,
        },
        "inspection": inspection_payload,
        "observability": observability_payload,
    }
    if packet_inspection is not None:
        payload["packet_inspection"] = packet_inspection
    return payload


__all__ = ["build_status_payload"]
