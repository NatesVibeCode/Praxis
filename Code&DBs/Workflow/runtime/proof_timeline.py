"""DB-backed proof timeline projection.

This module is the read authority for stitching existing proof records into a
single timeline. It does not write receipts, verification rows, or bug evidence
links; it makes their relationship queryable from one place.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from runtime.bug_evidence import (
    EVIDENCE_KIND_HEALING_RUN,
    EVIDENCE_KIND_RECEIPT,
    EVIDENCE_KIND_RUN,
    EVIDENCE_KIND_VERIFICATION_RUN,
    EVIDENCE_ROLE_ATTEMPTED_FIX,
    EVIDENCE_ROLE_OBSERVED_IN,
    EVIDENCE_ROLE_VALIDATES_FIX,
    verification_passed,
)
from runtime.payload_coercion import (
    coerce_datetime as _coerce_datetime,
    json_list as _json_list,
    json_object as _json_object,
)

QueryRowsFn = Callable[..., tuple[list[Any], str | None]]

PROOF_TIMELINE_AUTHORITY = "runtime.proof_timeline"


def _row_value(row: Any, key: str) -> Any:
    try:
        return row.get(key)
    except AttributeError:
        return row[key]
    except (KeyError, TypeError):
        return None


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _entry_from_row(row: Any) -> dict[str, Any]:
    proof_kind = _clean_text(_row_value(row, "proof_kind"))
    proof_ref = _clean_text(_row_value(row, "proof_ref"))
    proof_status = _clean_text(_row_value(row, "proof_status")) or None
    evidence_role = _clean_text(_row_value(row, "evidence_role"))
    metadata = {
        "inputs": _json_object(_row_value(row, "inputs")),
        "outputs": _json_object(_row_value(row, "outputs")),
        "artifacts": _json_object(_row_value(row, "artifacts")),
        "decision_refs": _json_list(_row_value(row, "decision_refs")),
    }
    metadata = {key: value for key, value in metadata.items() if value}
    entry: dict[str, Any] = {
        "authority": PROOF_TIMELINE_AUTHORITY,
        "subject_kind": _clean_text(_row_value(row, "subject_kind")),
        "subject_ref": _clean_text(_row_value(row, "subject_ref")),
        "proof_kind": proof_kind,
        "proof_ref": proof_ref,
        "proof_status": proof_status,
        "proof_passed": verification_passed(proof_status),
        "evidence_role": evidence_role,
        "source_table": _clean_text(_row_value(row, "source_table")),
        "source_ref": _clean_text(_row_value(row, "source_ref")),
        "source_created_by": _clean_text(_row_value(row, "source_created_by")) or None,
        "notes": _clean_text(_row_value(row, "notes")) or None,
        "recorded_at": _coerce_datetime(_row_value(row, "recorded_at")),
        "proof_recorded_at": _coerce_datetime(_row_value(row, "proof_recorded_at")),
        "run_id": _clean_text(_row_value(row, "run_id")) or None,
        "receipt_id": _clean_text(_row_value(row, "receipt_id")) or None,
        "verification_run_id": _clean_text(_row_value(row, "verification_run_id")) or None,
        "healing_run_id": _clean_text(_row_value(row, "healing_run_id")) or None,
        "workflow_id": _clean_text(_row_value(row, "workflow_id")) or None,
        "request_id": _clean_text(_row_value(row, "request_id")) or None,
        "verifier_ref": _clean_text(_row_value(row, "verifier_ref")) or None,
        "healer_ref": _clean_text(_row_value(row, "healer_ref")) or None,
        "target_kind": _clean_text(_row_value(row, "target_kind")) or None,
        "target_ref": _clean_text(_row_value(row, "target_ref")) or None,
        "decision_ref": _clean_text(_row_value(row, "decision_ref")) or None,
        "duration_ms": int(_row_value(row, "duration_ms") or 0),
        "metadata": metadata,
    }
    return {key: value for key, value in entry.items() if value not in ("", None, {})}


def _sort_key(entry: dict[str, Any]) -> tuple[Any, str, str, str]:
    timestamp = entry.get("recorded_at") or entry.get("proof_recorded_at")
    return (
        timestamp is None,
        timestamp,
        str(entry.get("proof_kind") or ""),
        str(entry.get("proof_ref") or ""),
    )


def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    result: list[dict[str, Any]] = []
    for entry in sorted(entries, key=_sort_key):
        key = (
            str(entry.get("subject_ref") or ""),
            str(entry.get("proof_kind") or ""),
            str(entry.get("proof_ref") or ""),
            str(entry.get("evidence_role") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(entry)
    return result


def bug_proof_timeline(
    *,
    bug_id: str,
    query_rows_fn: QueryRowsFn,
) -> tuple[list[dict[str, Any]], str | None]:
    """Return the canonical proof timeline for one bug.

    The timeline merges explicit bug_evidence_links with direct discovery
    provenance on the bug row. Callers get one source for proof refs instead
    of separately joining bug links, receipts, runs, and verification rows.
    """

    normalized_bug_id = _clean_text(bug_id)
    if not normalized_bug_id:
        return [], "bug_id_required"
    rows, error = query_rows_fn(
        f"""
        WITH linked AS (
            SELECT
                'bug'::text AS subject_kind,
                bel.bug_id AS subject_ref,
                bel.evidence_kind AS proof_kind,
                bel.evidence_ref AS proof_ref,
                bel.evidence_role,
                'bug_evidence_links'::text AS source_table,
                bel.bug_evidence_link_id AS source_ref,
                bel.created_by AS source_created_by,
                bel.notes,
                bel.created_at AS recorded_at,
                CASE
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_VERIFICATION_RUN}' THEN vr.status
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_HEALING_RUN}' THEN hr.status
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_RECEIPT}' THEN r.status
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_RUN}' THEN wr.current_state
                    ELSE NULL
                END AS proof_status,
                CASE
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_VERIFICATION_RUN}' THEN vr.attempted_at
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_HEALING_RUN}' THEN hr.attempted_at
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_RECEIPT}' THEN COALESCE(r.finished_at, r.started_at)
                    WHEN bel.evidence_kind = '{EVIDENCE_KIND_RUN}' THEN COALESCE(wr.finished_at, wr.started_at, wr.admitted_at, wr.requested_at)
                    ELSE bel.created_at
                END AS proof_recorded_at,
                r.run_id,
                r.receipt_id,
                vr.verification_run_id,
                hr.healing_run_id,
                COALESCE(r.workflow_id, wr.workflow_id) AS workflow_id,
                COALESCE(r.request_id, wr.request_id) AS request_id,
                COALESCE(vr.verifier_ref, hr.verifier_ref) AS verifier_ref,
                hr.healer_ref,
                COALESCE(vr.target_kind, hr.target_kind) AS target_kind,
                COALESCE(vr.target_ref, hr.target_ref) AS target_ref,
                COALESCE(vr.decision_ref, hr.decision_ref) AS decision_ref,
                COALESCE(vr.duration_ms, hr.duration_ms, 0) AS duration_ms,
                COALESCE(vr.inputs, hr.inputs, r.inputs, '{{}}'::jsonb) AS inputs,
                COALESCE(vr.outputs, hr.outputs, r.outputs, '{{}}'::jsonb) AS outputs,
                COALESCE(r.artifacts, '{{}}'::jsonb) AS artifacts,
                COALESCE(r.decision_refs, '[]'::jsonb) AS decision_refs
            FROM bug_evidence_links AS bel
            LEFT JOIN verification_runs AS vr
              ON bel.evidence_kind = '{EVIDENCE_KIND_VERIFICATION_RUN}'
             AND vr.verification_run_id = bel.evidence_ref
            LEFT JOIN healing_runs AS hr
              ON bel.evidence_kind = '{EVIDENCE_KIND_HEALING_RUN}'
             AND hr.healing_run_id = bel.evidence_ref
            LEFT JOIN receipts AS r
              ON bel.evidence_kind = '{EVIDENCE_KIND_RECEIPT}'
             AND r.receipt_id = bel.evidence_ref
            LEFT JOIN workflow_runs AS wr
              ON bel.evidence_kind = '{EVIDENCE_KIND_RUN}'
             AND wr.run_id = bel.evidence_ref
            WHERE bel.bug_id = $1
        ),
        discovered_receipt AS (
            SELECT
                'bug'::text AS subject_kind,
                b.bug_id AS subject_ref,
                '{EVIDENCE_KIND_RECEIPT}'::text AS proof_kind,
                b.discovered_in_receipt_id AS proof_ref,
                '{EVIDENCE_ROLE_OBSERVED_IN}'::text AS evidence_role,
                'bugs'::text AS source_table,
                b.bug_id AS source_ref,
                b.filed_by AS source_created_by,
                NULL::text AS notes,
                b.created_at AS recorded_at,
                r.status AS proof_status,
                COALESCE(r.finished_at, r.started_at) AS proof_recorded_at,
                r.run_id,
                r.receipt_id,
                NULL::text AS verification_run_id,
                NULL::text AS healing_run_id,
                r.workflow_id,
                r.request_id,
                NULL::text AS verifier_ref,
                NULL::text AS healer_ref,
                NULL::text AS target_kind,
                NULL::text AS target_ref,
                NULL::text AS decision_ref,
                0::integer AS duration_ms,
                COALESCE(r.inputs, '{{}}'::jsonb) AS inputs,
                COALESCE(r.outputs, '{{}}'::jsonb) AS outputs,
                COALESCE(r.artifacts, '{{}}'::jsonb) AS artifacts,
                COALESCE(r.decision_refs, '[]'::jsonb) AS decision_refs
            FROM bugs AS b
            LEFT JOIN receipts AS r
              ON r.receipt_id = b.discovered_in_receipt_id
            WHERE b.bug_id = $1
              AND NULLIF(BTRIM(b.discovered_in_receipt_id), '') IS NOT NULL
        ),
        discovered_run AS (
            SELECT
                'bug'::text AS subject_kind,
                b.bug_id AS subject_ref,
                '{EVIDENCE_KIND_RUN}'::text AS proof_kind,
                b.discovered_in_run_id AS proof_ref,
                '{EVIDENCE_ROLE_OBSERVED_IN}'::text AS evidence_role,
                'bugs'::text AS source_table,
                b.bug_id AS source_ref,
                b.filed_by AS source_created_by,
                NULL::text AS notes,
                b.created_at AS recorded_at,
                wr.current_state AS proof_status,
                COALESCE(wr.finished_at, wr.started_at, wr.admitted_at, wr.requested_at) AS proof_recorded_at,
                wr.run_id,
                NULL::text AS receipt_id,
                NULL::text AS verification_run_id,
                NULL::text AS healing_run_id,
                wr.workflow_id,
                wr.request_id,
                NULL::text AS verifier_ref,
                NULL::text AS healer_ref,
                NULL::text AS target_kind,
                NULL::text AS target_ref,
                NULL::text AS decision_ref,
                0::integer AS duration_ms,
                COALESCE(wr.request_envelope, '{{}}'::jsonb) AS inputs,
                '{{}}'::jsonb AS outputs,
                '{{}}'::jsonb AS artifacts,
                '[]'::jsonb AS decision_refs
            FROM bugs AS b
            LEFT JOIN workflow_runs AS wr
              ON wr.run_id = b.discovered_in_run_id
            WHERE b.bug_id = $1
              AND NULLIF(BTRIM(b.discovered_in_run_id), '') IS NOT NULL
        )
        SELECT *
          FROM linked
        UNION ALL
        SELECT *
          FROM discovered_receipt
        UNION ALL
        SELECT *
          FROM discovered_run
        ORDER BY recorded_at ASC NULLS LAST, proof_kind, proof_ref
        """,
        normalized_bug_id,
    )
    if error:
        return [], f"proof_timeline.query_failed:{error}"
    return _dedupe_entries([_entry_from_row(row) for row in rows]), None


def passed_validates_fix_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return passed validates_fix verification entries from a proof timeline."""

    return [
        entry
        for entry in entries
        if entry.get("evidence_role") == EVIDENCE_ROLE_VALIDATES_FIX
        and entry.get("proof_kind") == EVIDENCE_KIND_VERIFICATION_RUN
        and bool(entry.get("proof_passed"))
    ]


def _verification_row_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "verification_run_id": entry.get("verification_run_id") or entry.get("proof_ref"),
        "verifier_ref": entry.get("verifier_ref") or "",
        "target_kind": entry.get("target_kind") or "",
        "target_ref": entry.get("target_ref") or "",
        "status": entry.get("proof_status") or "",
        "inputs": dict(entry.get("metadata", {}).get("inputs") or {}),
        "outputs": dict(entry.get("metadata", {}).get("outputs") or {}),
        "decision_ref": entry.get("decision_ref") or "",
        "attempted_at": entry.get("proof_recorded_at") or entry.get("recorded_at"),
        "duration_ms": int(entry.get("duration_ms") or 0),
    }


def _healing_row_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "healing_run_id": entry.get("healing_run_id") or entry.get("proof_ref"),
        "healer_ref": entry.get("healer_ref") or "",
        "verifier_ref": entry.get("verifier_ref") or "",
        "target_kind": entry.get("target_kind") or "",
        "target_ref": entry.get("target_ref") or "",
        "status": entry.get("proof_status") or "",
        "inputs": dict(entry.get("metadata", {}).get("inputs") or {}),
        "outputs": dict(entry.get("metadata", {}).get("outputs") or {}),
        "decision_ref": entry.get("decision_ref") or "",
        "attempted_at": entry.get("proof_recorded_at") or entry.get("recorded_at"),
        "duration_ms": int(entry.get("duration_ms") or 0),
    }


def historical_fix_evidence(
    *,
    bug_id: str,
    query_rows_fn: QueryRowsFn,
) -> dict[str, Any]:
    """Return fix proof summary using the shared proof timeline projection."""

    timeline, error = bug_proof_timeline(bug_id=bug_id, query_rows_fn=query_rows_fn)
    if error:
        return {
            "fix_verified": False,
            "linked_validation_count": 0,
            "verified_validation_count": 0,
            "last_validation": None,
            "attempted_fix_count": 0,
            "last_attempted_fix": None,
            "errors": (error,),
        }
    validation_entries = [
        entry
        for entry in timeline
        if entry.get("evidence_role") == EVIDENCE_ROLE_VALIDATES_FIX
        and entry.get("proof_kind") == EVIDENCE_KIND_VERIFICATION_RUN
    ]
    verified_rows = [_verification_row_from_entry(entry) for entry in validation_entries if entry.get("proof_passed")]
    attempted_entries = [
        entry
        for entry in timeline
        if entry.get("evidence_role") == EVIDENCE_ROLE_ATTEMPTED_FIX
        and entry.get("proof_kind") == EVIDENCE_KIND_HEALING_RUN
    ]
    attempted_rows = [_healing_row_from_entry(entry) for entry in attempted_entries]
    latest_validation = max(
        verified_rows,
        key=lambda row: row.get("attempted_at") or "",
        default=None,
    )
    latest_attempted_fix = max(
        attempted_rows,
        key=lambda row: row.get("attempted_at") or "",
        default=None,
    )
    return {
        "fix_verified": bool(verified_rows),
        "linked_validation_count": len(validation_entries),
        "verified_validation_count": len(verified_rows),
        "last_validation": latest_validation,
        "attempted_fix_count": len(attempted_entries),
        "last_attempted_fix": latest_attempted_fix,
        "errors": (),
    }
