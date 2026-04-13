"""Shared execution-packet lineage, inspection, and materialized run views."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


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


def build_execution_packet_lineage_payload(
    packet: Mapping[str, Any],
    *,
    parent_artifact_ref: str,
) -> dict[str, Any]:
    compile_provenance = _json_mapping(packet.get("compile_provenance"))
    lineage_payload: dict[str, Any] = {
        "definition_revision": str(packet.get("definition_revision") or "").strip(),
        "plan_revision": str(packet.get("plan_revision") or "").strip(),
        "packet_version": int(packet.get("packet_version") or 0),
        "workflow_id": str(packet.get("workflow_id") or "").strip(),
        "spec_name": str(packet.get("spec_name") or "").strip(),
        "source_kind": str(packet.get("source_kind") or "").strip(),
        "authority_refs": list(_json_sequence(packet.get("authority_refs"))),
        "model_messages": _mapping_list(_json_clone(packet.get("model_messages"))),
        "reference_bindings": _mapping_list(_json_clone(packet.get("reference_bindings"))),
        "capability_bindings": _mapping_list(_json_clone(packet.get("capability_bindings"))),
        "verify_refs": _string_list(_json_clone(packet.get("verify_refs"))),
        "authority_inputs": _json_mapping(_json_clone(compile_provenance.get("authority_inputs"))),
        "file_inputs": _json_mapping(_json_clone(compile_provenance.get("file_inputs"))),
        "compile_provenance": compile_provenance,
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
    compile_provenance = _json_mapping(finalized.get("compile_provenance"))
    compile_provenance["packet_lineage_revision"] = lineage_payload["packet_revision"]
    compile_provenance["packet_lineage_hash"] = lineage_payload["packet_hash"]
    compile_provenance["reuse"] = dict(reuse_metadata)
    finalized["compile_provenance"] = compile_provenance
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
        return None
    run_row = dict(run_rows[0])
    packet_rows = conn.execute(
        """
        SELECT payload
          FROM execution_packets
         WHERE run_id = $1
         ORDER BY created_at ASC, execution_packet_id ASC
        """,
        run_id,
    )
    packets = [dict(row.get("payload")) for row in packet_rows or [] if isinstance(row.get("payload"), Mapping)]
    inspection = inspect_execution_packets(packets, run_row=run_row) if packets else None
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
    "packet_inspection_from_row",
    "rebuild_workflow_run_packet_inspection",
]
