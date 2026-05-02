"""Shared execution-packet lineage, inspection, and materialized run views."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


class PacketInspectionUnavailable(RuntimeError):
    """Raised when packet inspection cannot be resolved from authority inputs."""

    def __init__(
        self,
        stage: str,
        message: str,
        *,
        error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.error = error


def _json_clone(value: object) -> object:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _json_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
        if isinstance(parsed, list):
            return list(parsed)
    return []


def _mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return []
    values: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            values.append(text)
    return values


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def resolve_execution_packet_revisions(
    *,
    raw_snapshot: dict[str, Any],
    spec: Any,
    workflow_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Resolve packet revisions once and stamp their provenance.

    Queue and chain runs may need deterministic synthetic revisions for
    diagnostic packet rows, but that provenance must remain explicit so they
    are never mistaken for compile-index-backed revisions.
    """

    definition_revision = str(raw_snapshot.get("definition_revision") or "").strip()
    plan_revision = str(raw_snapshot.get("plan_revision") or "").strip()
    had_definition_revision = bool(definition_revision)
    had_plan_revision = bool(plan_revision)

    spec_jobs = raw_snapshot.get("jobs")
    if not isinstance(spec_jobs, list):
        spec_jobs = [dict(j) for j in getattr(spec, "jobs", ()) or ()]
    canonicalized_jobs = []
    for job in spec_jobs:
        if not isinstance(job, Mapping):
            continue
        canonicalized_jobs.append(
            {k: _json_clone(v) for k, v in job.items() if k not in {"_route_plan"}}
        )
    spec_fingerprint = {
        "workflow_id": workflow_id,
        "name": raw_snapshot.get("name") or getattr(spec, "name", ""),
        "task_type": raw_snapshot.get("task_type") or getattr(spec, "task_type", ""),
        "jobs": canonicalized_jobs,
    }

    from runtime.materialize_reuse import stable_hash

    synthetic_fields: list[str] = []
    if not definition_revision:
        definition_revision = f"def_{stable_hash(spec_fingerprint)[:16]}"
        synthetic_fields.append("definition_revision")
    if not plan_revision:
        plan_revision = f"plan_{stable_hash({'definition_revision': definition_revision, 'fingerprint': spec_fingerprint})[:16]}"
        synthetic_fields.append("plan_revision")

    if had_definition_revision and had_plan_revision:
        provenance_kind = "compiled"
    elif synthetic_fields and (had_definition_revision or had_plan_revision):
        provenance_kind = "partial_synthetic"
    else:
        provenance_kind = "synthetic"

    authority = {
        "kind": "execution_packet_revision_authority",
        "provenance_kind": provenance_kind,
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "synthetic_fields": synthetic_fields,
        "reason_code": (
            "packet.revision.compiled"
            if provenance_kind == "compiled"
            else "packet.revision.synthetic_fallback"
        ),
        "workflow_id": str(workflow_id or "").strip(),
        "run_id": str(run_id or "").strip(),
    }
    raw_snapshot["definition_revision"] = definition_revision
    raw_snapshot["plan_revision"] = plan_revision
    raw_snapshot["packet_revision_authority"] = authority
    return authority


def build_execution_packet_lineage_payload(
    packet: Mapping[str, Any],
    *,
    parent_artifact_ref: str,
) -> dict[str, Any]:
    materialize_provenance = _json_mapping(packet.get("materialize_provenance"))
    lineage_payload: dict[str, Any] = {
        "definition_revision": str(packet.get("definition_revision") or "").strip(),
        "plan_revision": str(packet.get("plan_revision") or "").strip(),
        "packet_version": int(packet.get("packet_version") or 0),
        "workflow_id": str(packet.get("workflow_id") or "").strip(),
        "spec_name": str(packet.get("spec_name") or "").strip(),
        "source_kind": str(packet.get("source_kind") or "").strip(),
        "packet_revision_authority": _json_mapping(
            _json_clone(packet.get("packet_revision_authority"))
        ),
        "authority_refs": list(_json_sequence(packet.get("authority_refs"))),
        "model_messages": _mapping_list(_json_clone(packet.get("model_messages"))),
        "reference_bindings": _mapping_list(_json_clone(packet.get("reference_bindings"))),
        "capability_bindings": _mapping_list(_json_clone(packet.get("capability_bindings"))),
        "verify_refs": _string_list(_json_clone(packet.get("verify_refs"))),
        "authority_inputs": _json_mapping(_json_clone(materialize_provenance.get("authority_inputs"))),
        "file_inputs": _json_mapping(_json_clone(materialize_provenance.get("file_inputs"))),
        "materialize_provenance": materialize_provenance,
    }
    lineage_hash = hashlib.sha256(
        json.dumps(lineage_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    lineage_payload["packet_hash"] = lineage_hash
    lineage_payload["packet_revision"] = f"packet_{lineage_hash[:16]}:{lineage_payload['packet_version']}"
    lineage_payload["decision_ref"] = f"decision.compile.packet.{lineage_hash[:16]}"
    lineage_payload["parent_artifact_ref"] = parent_artifact_ref
    return lineage_payload


def finalize_execution_packet(
    packet: Mapping[str, Any],
    *,
    lineage_payload: Mapping[str, Any],
    reuse_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    finalized = dict(packet)
    materialize_provenance = _json_mapping(finalized.get("materialize_provenance"))
    materialize_provenance["packet_lineage_revision"] = lineage_payload["packet_revision"]
    materialize_provenance["packet_lineage_hash"] = lineage_payload["packet_hash"]
    materialize_provenance["reuse"] = dict(reuse_metadata)
    finalized["materialize_provenance"] = materialize_provenance
    finalized["parent_artifact_ref"] = lineage_payload["packet_revision"]
    packet_hash = hashlib.sha256(
        json.dumps(finalized, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    finalized["packet_hash"] = packet_hash
    finalized["packet_revision"] = f"packet_{packet_hash[:16]}:{finalized['packet_version']}"
    finalized["decision_ref"] = f"decision.compile.packet.{packet_hash[:16]}"
    return finalized


def packet_inspection_from_row(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    inspection = _json_mapping(row.get("packet_inspection"))
    return inspection or None


def _packet_payloads(value: object) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    packets: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        payload = item.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError, ValueError):
                payload = None
        if isinstance(payload, Mapping):
            packets.append(dict(payload))
        else:
            packets.append(dict(item))
    return packets


def resolve_packet_inspection(
    *,
    run_row: Mapping[str, Any] | None,
    packets: object,
) -> tuple[dict[str, Any] | None, str]:
    """Resolve packet inspection through one authority-owned path."""

    materialized = packet_inspection_from_row(run_row)
    if materialized is not None:
        return materialized, "materialized"

    packet_payloads = _packet_payloads(packets)
    if not packet_payloads:
        return None, "missing"
    try:
        return inspect_execution_packets(packet_payloads, run_row=run_row), "derived"
    except Exception as exc:  # noqa: BLE001 - preserve stage for callers.
        raise PacketInspectionUnavailable(
            "derive",
            "workflow run packet inspection derivation failed",
            error=exc,
        ) from exc


def load_workflow_run_packet_inspection(
    conn: Any,
    *,
    run_id: str,
    run_row: Mapping[str, Any] | None = None,
    persist_if_derived: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    """Load packet inspection for one run, optionally repairing the projection."""

    effective_run_row: Mapping[str, Any] | None = run_row
    if effective_run_row is None:
        run_rows = conn.execute(
            """
            SELECT run_id,
                   workflow_id,
                   request_id,
                   workflow_definition_id,
                   current_state,
                   request_envelope,
                   requested_at,
                   admitted_at,
                   started_at,
                   finished_at,
                   last_event_id,
                   packet_inspection
              FROM workflow_runs
             WHERE run_id = $1
             LIMIT 1
            """,
            run_id,
        )
        if not run_rows:
            return None, "missing"
        effective_run_row = dict(run_rows[0])

    try:
        packet_rows = conn.execute(
            """
            SELECT COALESCE(
                       jsonb_agg(payload ORDER BY created_at, execution_packet_id),
                       '[]'::jsonb
                   ) AS packets
              FROM execution_packets
             WHERE run_id = $1
            """,
            run_id,
        )
    except Exception as exc:  # noqa: BLE001 - status surfaces report this as data quality.
        raise PacketInspectionUnavailable(
            "query",
            "workflow run packet inspection query failed",
            error=exc,
        ) from exc
    packet_rows = [dict(row) for row in packet_rows or [] if isinstance(row, Mapping)]
    if len(packet_rows) == 1 and "packets" in packet_rows[0]:
        packets: object = packet_rows[0].get("packets")
    else:
        packets = packet_rows
    inspection, source = resolve_packet_inspection(
        run_row=effective_run_row,
        packets=packets,
    )
    if persist_if_derived and source == "derived":
        try:
            conn.execute(
                "UPDATE workflow_runs SET packet_inspection = $2::jsonb WHERE run_id = $1",
                run_id,
                json.dumps(inspection, sort_keys=True, default=str),
            )
            source = "materialized_after_repair"
        except Exception:
            source = "derived_persist_failed"
    return inspection, source


def inspect_execution_packets(
    packets: Sequence[Mapping[str, Any]] | None,
    *,
    run_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize execution-packet truth for read surfaces."""

    normalized_packets = [
        dict(_json_mapping(packet.get("payload"))) | dict(packet)
        if _json_mapping(packet.get("payload"))
        else dict(packet)
        for packet in (packets or ())
        if isinstance(packet, Mapping)
    ]
    execution = _execution_snapshot(run_row)
    if not normalized_packets:
        return {
            "kind": "shadow_execution_packet_inspection",
            "packet_count": 0,
            "packet_revision": None,
            "packet_history": [],
            "current_packet": None,
            "execution": execution,
            "drift": {
                "kind": "shadow_execution_packet_drift",
                "status": "missing",
                "is_drifted": True,
                "differences": [
                    {
                        "field": "packet",
                        "expected": "present",
                        "actual": "missing",
                    }
                ],
            },
        }

    current_packet = dict(normalized_packets[-1])
    current_packet_provenance = _packet_provenance(current_packet)
    if current_packet_provenance:
        current_packet["packet_provenance"] = current_packet_provenance
    current_packet["authority_inputs"] = _json_mapping(current_packet.get("authority_inputs"))
    current_packet["file_inputs"] = _json_mapping(current_packet.get("file_inputs"))
    packet_history = [
        {
            "packet_revision": str(packet.get("packet_revision") or "").strip() or None,
            "packet_hash": str(packet.get("packet_hash") or "").strip() or None,
        }
        for packet in normalized_packets
    ]

    if execution is None:
        expected = {
            "run_id": str(current_packet.get("run_id") or "").strip(),
            "workflow_id": str(current_packet.get("workflow_id") or "").strip(),
            "spec_name": str(current_packet.get("spec_name") or "").strip(),
            "source_kind": str(current_packet.get("source_kind") or "").strip(),
            "definition_revision": str(current_packet.get("definition_revision") or "").strip(),
            "plan_revision": str(current_packet.get("plan_revision") or "").strip(),
            "authority_refs": list(_json_sequence(current_packet.get("authority_refs"))),
            "verify_refs": _dedupe_strings(_string_list(current_packet.get("verify_refs"))),
            "packet_provenance": _json_clone(current_packet_provenance),
            "packet_revision_authority": _json_mapping(
                current_packet.get("packet_revision_authority")
            ),
        }
    else:
        spec_snapshot = _json_mapping(execution.get("spec_snapshot"))
        request_envelope_provenance = _json_mapping(spec_snapshot.get("packet_provenance"))
        definition_revision = str(
            spec_snapshot.get("definition_revision")
            or current_packet.get("definition_revision")
            or ""
        ).strip()
        plan_revision = str(
            spec_snapshot.get("plan_revision")
            or current_packet.get("plan_revision")
            or ""
        ).strip()
        expected = {
            "run_id": execution.get("run_id") or str(current_packet.get("run_id") or "").strip(),
            "workflow_id": execution.get("workflow_id") or str(current_packet.get("workflow_id") or "").strip(),
            "spec_name": execution.get("spec_name") or str(current_packet.get("spec_name") or "").strip(),
            "source_kind": str(
                request_envelope_provenance.get("source_kind")
                or current_packet.get("source_kind")
                or ""
            ).strip(),
            "definition_revision": definition_revision,
            "plan_revision": plan_revision,
            "authority_refs": [ref for ref in (definition_revision, plan_revision) if ref],
            "verify_refs": _dedupe_strings(_string_list(spec_snapshot.get("verify_refs"))),
            "packet_provenance": request_envelope_provenance,
            "packet_revision_authority": _json_mapping(
                spec_snapshot.get("packet_revision_authority")
            ),
        }

    actual = {
        "run_id": str(current_packet.get("run_id") or "").strip(),
        "workflow_id": str(current_packet.get("workflow_id") or "").strip(),
        "spec_name": str(current_packet.get("spec_name") or "").strip(),
        "source_kind": str(current_packet.get("source_kind") or "").strip(),
        "definition_revision": str(current_packet.get("definition_revision") or "").strip(),
        "plan_revision": str(current_packet.get("plan_revision") or "").strip(),
        "authority_refs": list(_json_sequence(current_packet.get("authority_refs"))),
        "verify_refs": _dedupe_strings(_string_list(current_packet.get("verify_refs"))),
        "packet_provenance": _json_clone(current_packet_provenance),
        "packet_revision_authority": _json_mapping(
            current_packet.get("packet_revision_authority")
        ),
    }
    differences = _packet_differences(expected=expected, actual=actual)
    drift_status = "aligned" if not differences else "drifted"

    return {
        "kind": "shadow_execution_packet_inspection",
        "packet_count": len(normalized_packets),
        "packet_revision": current_packet.get("packet_revision"),
        "packet_history": packet_history,
        "current_packet": current_packet,
        "execution": execution,
        "drift": {
            "kind": "shadow_execution_packet_drift",
            "status": drift_status,
            "is_drifted": bool(differences),
            "differences": differences,
        },
    }


def rebuild_workflow_run_packet_inspection(
    conn: Any,
    *,
    run_id: str,
) -> dict[str, Any] | None:
    inspection, _source = load_workflow_run_packet_inspection(
        conn,
        run_id=run_id,
        persist_if_derived=False,
    )
    conn.execute(
        "UPDATE workflow_runs SET packet_inspection = $2::jsonb WHERE run_id = $1",
        run_id,
        json.dumps(inspection, sort_keys=True, default=str) if inspection is not None else None,
    )
    return inspection


def _execution_snapshot(run_row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if run_row is None:
        return None
    row = _json_mapping(run_row)
    request_envelope = _json_mapping(row.get("request_envelope"))
    spec_snapshot = _json_mapping(request_envelope.get("spec_snapshot"))
    return {
        "run_id": str(row.get("run_id") or "").strip(),
        "workflow_id": str(row.get("workflow_id") or "").strip(),
        "request_id": str(row.get("request_id") or "").strip(),
        "workflow_definition_id": str(row.get("workflow_definition_id") or "").strip(),
        "current_state": str(row.get("current_state") or row.get("status") or "").strip(),
        "requested_at": _json_clone(row.get("requested_at")) if row.get("requested_at") is not None else None,
        "admitted_at": _json_clone(row.get("admitted_at")) if row.get("admitted_at") is not None else None,
        "started_at": _json_clone(row.get("started_at")) if row.get("started_at") is not None else None,
        "finished_at": _json_clone(row.get("finished_at")) if row.get("finished_at") is not None else None,
        "spec_name": str(
            request_envelope.get("name")
            or request_envelope.get("spec_name")
            or row.get("spec_name")
            or row.get("workflow_id")
            or ""
        ).strip(),
        "request_envelope": request_envelope,
        "spec_snapshot": spec_snapshot,
    }


def _packet_provenance(packet: Mapping[str, Any]) -> dict[str, Any]:
    packet_provenance = _json_mapping(packet.get("packet_provenance"))
    if packet_provenance:
        return packet_provenance
    authority_inputs = _json_mapping(packet.get("authority_inputs"))
    return _json_mapping(authority_inputs.get("packet_provenance"))


def _packet_differences(
    *,
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    for field_name in (
        "run_id",
        "workflow_id",
        "spec_name",
        "source_kind",
        "definition_revision",
        "plan_revision",
        "authority_refs",
        "verify_refs",
        "packet_provenance",
        "packet_revision_authority",
    ):
        expected_value = _json_clone(expected.get(field_name))
        actual_value = _json_clone(actual.get(field_name))
        if expected_value != actual_value:
            differences.append(
                {
                    "field": field_name,
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )
    return differences


__all__ = [
    "build_execution_packet_lineage_payload",
    "finalize_execution_packet",
    "inspect_execution_packets",
    "load_workflow_run_packet_inspection",
    "PacketInspectionUnavailable",
    "packet_inspection_from_row",
    "rebuild_workflow_run_packet_inspection",
    "resolve_packet_inspection",
    "resolve_execution_packet_revisions",
]
