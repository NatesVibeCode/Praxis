"""Evidence aggregation, failure signatures, and receipt helpers for the bug tracker."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from runtime.bug_tagging import stable_fingerprint
from runtime.primitive_contracts import failure_identity_fields


def _json_object(value: Any) -> dict[str, Any]:
    """Coerce a value to a dict, parsing JSON strings if needed."""
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


def build_query_error(
    *,
    scope: str,
    reason_code: str,
    error: BaseException | str,
    component: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "scope": str(scope or "").strip(),
        "reason_code": str(reason_code or "").strip(),
        "error_message": str(error or "").strip(),
    }
    if isinstance(error, BaseException):
        payload["error_type"] = type(error).__name__
    if component:
        payload["component"] = str(component).strip()
    return payload


_BUG_BLAST_RADIUS_WINDOW_SQL = "7 days"

# Canonical evidence role / kind names. Consumers MUST import the named
# constants (or the allowed-set) rather than hand-roll the literal strings so
# this module stays the single naming source for bug-evidence proof refs.
EVIDENCE_ROLE_OBSERVED_IN = "observed_in"
EVIDENCE_ROLE_ATTEMPTED_FIX = "attempted_fix"
EVIDENCE_ROLE_VALIDATES_FIX = "validates_fix"
EVIDENCE_ROLE_DISCOVERED_BY = "discovered_by"
ALLOWED_EVIDENCE_ROLES: frozenset[str] = frozenset(
    {
        EVIDENCE_ROLE_OBSERVED_IN,
        EVIDENCE_ROLE_ATTEMPTED_FIX,
        EVIDENCE_ROLE_VALIDATES_FIX,
        EVIDENCE_ROLE_DISCOVERED_BY,
    }
)

EVIDENCE_KIND_RECEIPT = "receipt"
EVIDENCE_KIND_RUN = "run"
EVIDENCE_KIND_VERIFICATION_RUN = "verification_run"
EVIDENCE_KIND_HEALING_RUN = "healing_run"
EVIDENCE_KIND_GOVERNANCE_SCAN = "governance_scan"
ALLOWED_EVIDENCE_KINDS: frozenset[str] = frozenset(
    {
        EVIDENCE_KIND_RECEIPT,
        EVIDENCE_KIND_RUN,
        EVIDENCE_KIND_VERIFICATION_RUN,
        EVIDENCE_KIND_HEALING_RUN,
        EVIDENCE_KIND_GOVERNANCE_SCAN,
    }
)

# Legacy private aliases kept so existing internal references continue to
# resolve; prefer the public names above for new code.
_ALLOWED_EVIDENCE_KINDS = ALLOWED_EVIDENCE_KINDS
_ALLOWED_EVIDENCE_ROLES = ALLOWED_EVIDENCE_ROLES
_VERIFICATION_SUCCESS_STATUSES = frozenset({"passed", "succeeded", "success", "ok"})
_SIGNATURE_ANCHOR_FIELDS = failure_identity_fields()


def build_failure_signature(
    *,
    failure_code: str | None,
    job_label: str | None = None,
    node_id: str | None = None,
    failure_category: str | None = None,
    agent: str | None = None,
    provider_slug: str | None = None,
    model_slug: str | None = None,
    source_kind: str | None = None,
) -> dict[str, Any]:
    payload = {
        "failure_code": str(failure_code or "").strip() or None,
        "job_label": str(job_label or "").strip() or None,
        "node_id": str(node_id or "").strip() or None,
        "failure_category": str(failure_category or "").strip() or None,
        "agent": str(agent or "").strip() or None,
        "provider_slug": str(provider_slug or "").strip() or None,
        "model_slug": str(model_slug or "").strip() or None,
        "source_kind": str(source_kind or "").strip() or None,
    }
    payload["fingerprint"] = stable_fingerprint(
        {key: value for key, value in payload.items() if key != "fingerprint"}
    )
    return payload


def signature_anchor_fields(signature: dict[str, Any]) -> tuple[str, ...]:
    anchors: list[str] = []
    for field in _SIGNATURE_ANCHOR_FIELDS:
        if str(signature.get(field) or "").strip():
            anchors.append(field)
    return tuple(anchors)


def materialize_packet_signature(
    signature: dict[str, Any],
    *,
    bug_id: str,
    source_kind: str | None,
) -> dict[str, Any]:
    payload = dict(signature)
    anchor_fields = signature_anchor_fields(payload)
    if anchor_fields:
        payload["authority"] = "evidence_or_tags"
        payload["fingerprint_scope"] = "cross_bug"
        payload["anchor_fields"] = anchor_fields
        return payload

    payload["fingerprint"] = stable_fingerprint(
        {
            "bug_id": str(bug_id or "").strip(),
            "source_kind": str(source_kind or payload.get("source_kind") or "").strip() or None,
            "fingerprint_scope": "bug_only",
        }
    )
    payload["authority"] = "bug_record_fallback"
    payload["fingerprint_scope"] = "bug_only"
    payload["anchor_fields"] = ()
    return payload


def extract_receipt_paths(payload: dict[str, Any], *, key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


def extract_write_paths(inputs: dict[str, Any], outputs: dict[str, Any]) -> tuple[str, ...]:
    paths: list[str] = []
    seen: set[str] = set()
    write_manifest = _json_object(outputs.get("write_manifest"))
    manifest_results = write_manifest.get("results")
    if isinstance(manifest_results, list):
        for row in manifest_results:
            if not isinstance(row, dict):
                continue
            path = str(row.get("file_path") or "").strip()
            if path and path not in seen:
                seen.add(path)
                paths.append(path)
    mutation_provenance = _json_object(outputs.get("mutation_provenance"))
    for source in (
        mutation_provenance.get("write_paths"),
        outputs.get("verified_paths"),
        inputs.get("write_scope"),
        inputs.get("file_paths"),
    ):
        if isinstance(source, list):
            for raw_path in source:
                path = str(raw_path or "").strip()
                if path and path not in seen:
                    seen.add(path)
                    paths.append(path)
    return tuple(paths)


def verification_passed(status: object) -> bool:
    return str(status or "").strip().lower() in _VERIFICATION_SUCCESS_STATUSES


def packet_summary(packet: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "signature": packet.get("signature"),
        "observability_state": packet.get("observability_state"),
        "observability_gaps": packet.get("observability_gaps"),
    }
    rc = packet.get("resume_context")
    if isinstance(rc, dict) and rc:
        summary["resume_context"] = rc
    sn = packet.get("semantic_neighbors")
    if isinstance(sn, dict):
        items = sn.get("items") or ()
        if items:
            summary["semantic_neighbor_count"] = len(items)
            note = sn.get("note")
            if isinstance(note, str) and note.strip():
                summary["semantic_neighbors_note"] = note.strip()
    return summary


def replay_state_from_hint(hint: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(hint or {})
    return {
        "replay_ready": bool(payload.get("available")),
        "replay_reason_code": str(payload.get("reason_code") or "bug.replay_not_ready"),
        "replay_run_id": payload.get("run_id"),
        "replay_receipt_id": payload.get("receipt_id"),
    }


def history_summary(*, bug_id: str, packet: dict[str, Any]) -> dict[str, Any]:
    agent_actions = _json_object(packet.get("agent_actions"))
    replay = _json_object(agent_actions.get("replay"))
    return {
        "bug_id": bug_id,
        "signature": packet.get("signature"),
        "observability_state": packet.get("observability_state"),
        "observability_gaps": packet.get("observability_gaps"),
        "errors": packet.get("errors"),
        "trace": packet.get("trace"),
        "latest_receipt": packet.get("latest_receipt"),
        "fallback_receipts": packet.get("fallback_receipts"),
        "blast_radius": packet.get("blast_radius"),
        "historical_fixes": packet.get("historical_fixes"),
        "fix_verification": packet.get("fix_verification"),
        "replay_context": packet.get("replay_context"),
        "provenance_backfill": packet.get("provenance_backfill"),
        "resume_context": packet.get("resume_context"),
        "semantic_neighbors": packet.get("semantic_neighbors"),
        "agent_actions": {
            "replay": replay or None,
        },
    }


def attempted_at_sort_key(item: Any) -> datetime:
    if isinstance(item, dict):
        attempted_at = item.get("attempted_at")
        if isinstance(attempted_at, datetime):
            return attempted_at
    return datetime.min.replace(tzinfo=timezone.utc)


def public_receipt_summary(receipt: dict[str, Any] | None) -> dict[str, Any] | None:
    from runtime.bug_tagging import payload_keys

    if receipt is None:
        return None
    inputs = _json_object(receipt.get("inputs"))
    outputs = _json_object(receipt.get("outputs"))
    artifacts = _json_object(receipt.get("artifacts"))
    git_provenance = _json_object(receipt.get("git_provenance"))
    return {
        "receipt_id": receipt.get("receipt_id"),
        "workflow_id": receipt.get("workflow_id"),
        "run_id": receipt.get("run_id"),
        "request_id": receipt.get("request_id"),
        "node_id": receipt.get("node_id"),
        "status": receipt.get("status"),
        "failure_code": receipt.get("failure_code"),
        "timestamp": receipt.get("timestamp"),
        "started_at": receipt.get("started_at"),
        "finished_at": receipt.get("finished_at"),
        "executor_type": receipt.get("executor_type"),
        "agent": receipt.get("agent"),
        "provider_slug": receipt.get("provider_slug"),
        "model_slug": receipt.get("model_slug"),
        "latency_ms": receipt.get("latency_ms"),
        "verification_status": receipt.get("verification_status"),
        "failure_category": receipt.get("failure_category"),
        "decision_refs": tuple(receipt.get("decision_refs") or ()),
        "repo_snapshot_ref": git_provenance.get("repo_snapshot_ref"),
        "workspace_ref": inputs.get("workspace_ref"),
        "runtime_profile_ref": inputs.get("runtime_profile_ref"),
        "write_paths": tuple(receipt.get("write_paths") or ()),
        "verified_paths": tuple(receipt.get("verified_paths") or ()),
        "payload_redacted": True,
        "input_keys": payload_keys(inputs),
        "output_keys": payload_keys(outputs),
        "artifact_keys": payload_keys(artifacts),
    }


def replay_action(
    *,
    bug_id: str,
    replay_context: dict[str, Any],
) -> dict[str, Any]:
    ready = bool(replay_context.get("ready") and replay_context.get("run_id"))
    source = str(replay_context.get("source") or "")
    if ready:
        reason_code = "bug.replay_ready"
    elif source == "fallback":
        reason_code = "bug.replay_inferred_only"
    elif replay_context.get("run_id"):
        reason_code = "bug.replay_missing_receipt_context"
    else:
        reason_code = "bug.replay_missing_run_context"
    return {
        "available": ready,
        "automatic": ready,
        "reason_code": reason_code,
        "run_id": replay_context.get("run_id"),
        "receipt_id": replay_context.get("receipt_id"),
        "tool": "praxis_bugs",
        "arguments": {
            "action": "replay",
            "bug_id": bug_id,
        },
        "http_request": {
            "method": "POST",
            "path": "/bugs",
            "body": {
                "action": "replay",
                "bug_id": bug_id,
            },
        },
    }


def build_observability_gaps(
    *,
    bug: Any,
    bug_status_fixed: Any,
    evidence_links: list[dict[str, Any]],
    latest_receipt: dict[str, Any] | None,
    fix_validation_count: int,
) -> tuple[str, ...]:
    gaps: list[str] = []
    if not evidence_links:
        gaps.append("bug.evidence_links.missing")
    if latest_receipt is None:
        gaps.append("receipt.missing")
    else:
        if not latest_receipt.get("run_id"):
            gaps.append("receipt.run_id.missing")
        if not latest_receipt.get("receipt_id"):
            gaps.append("receipt.receipt_id.missing")
        if not latest_receipt.get("failure_code"):
            gaps.append("receipt.failure_code.missing")
        git_provenance = _json_object(latest_receipt.get("git_provenance"))
        if not git_provenance:
            gaps.append("receipt.git_provenance.missing")
        elif not git_provenance.get("repo_snapshot_ref") and not git_provenance.get("available", False):
            gaps.append("receipt.git_provenance.unavailable")
        if not latest_receipt.get("write_paths"):
            gaps.append("receipt.write_paths.missing")
        if not latest_receipt.get("verification_status"):
            gaps.append("receipt.verification_status.missing")
        if not latest_receipt.get("decision_refs") and not bug.decision_ref:
            gaps.append("decision_ref.missing")
    if bug.status == bug_status_fixed and fix_validation_count <= 0:
        gaps.append("fix_validation.missing")
    return tuple(gaps)


def build_counterfactual_axes(latest_receipt: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
    if latest_receipt is None:
        return ()
    git_provenance = _json_object(latest_receipt.get("git_provenance"))
    baseline_model = "/".join(
        part
        for part in (
            latest_receipt.get("provider_slug"),
            latest_receipt.get("model_slug"),
        )
        if part
    ) or latest_receipt.get("agent")
    return (
        {
            "axis": "repo_snapshot",
            "baseline": git_provenance.get("repo_snapshot_ref") or None,
            "ready": bool(latest_receipt.get("run_id") and latest_receipt.get("receipt_id")),
            "description": "Replay the same evidence against a different repo snapshot.",
        },
        {
            "axis": "provider_model",
            "baseline": baseline_model or None,
            "ready": bool(baseline_model),
            "description": "Compare the same workload against a different route or model.",
        },
        {
            "axis": "verification",
            "baseline": latest_receipt.get("verification_status"),
            "ready": True,
            "description": "Re-run verification after a proposed fix and diff the result.",
        },
    )


def bug_signature_from_tags(bug: Any) -> dict[str, Any]:
    from runtime.bug_tagging import extract_tag_value

    return build_failure_signature(
        failure_code=extract_tag_value(bug.tags, "failure_code"),
        job_label=extract_tag_value(bug.tags, "job_label"),
        node_id=extract_tag_value(bug.tags, "node_id"),
        failure_category=extract_tag_value(bug.tags, "failure_category"),
        agent=extract_tag_value(bug.tags, "agent"),
        provider_slug=extract_tag_value(bug.tags, "provider"),
        model_slug=extract_tag_value(bug.tags, "model"),
        source_kind=bug.source_kind,
    )


def shared_signature_fields(
    current_signature: dict[str, Any],
    candidate_signature: dict[str, Any],
) -> tuple[str, ...]:
    shared: list[str] = []
    for field in (
        "failure_code",
        "failure_category",
        "agent",
        "provider_slug",
        "model_slug",
    ):
        current_value = str(current_signature.get(field) or "").strip()
        candidate_value = str(candidate_signature.get(field) or "").strip()
        if current_value and current_value == candidate_value:
            shared.append(field)
    current_node = str(
        current_signature.get("node_id") or current_signature.get("job_label") or ""
    ).strip()
    candidate_node = str(
        candidate_signature.get("node_id") or candidate_signature.get("job_label") or ""
    ).strip()
    if current_node and current_node == candidate_node:
        shared.append("node_id")
    return tuple(shared)


def signature_expectation_from_bug(bug: Any) -> dict[str, str | None]:
    from runtime.bug_tagging import extract_tag_value

    return {
        "failure_code": extract_tag_value(bug.tags, "failure_code"),
        "job_label": extract_tag_value(bug.tags, "job_label"),
        "node_id": extract_tag_value(bug.tags, "node_id"),
        "failure_category": extract_tag_value(bug.tags, "failure_category"),
        "agent": extract_tag_value(bug.tags, "agent"),
        "provider_slug": extract_tag_value(bug.tags, "provider"),
        "model_slug": extract_tag_value(bug.tags, "model"),
    }


def receipt_matches_backfill_signature(
    *,
    receipt: dict[str, Any],
    expected: dict[str, str | None],
) -> bool:
    candidate = build_failure_signature(
        failure_code=str(receipt.get("failure_code") or "").strip() or None,
        job_label=str(receipt.get("node_id") or "").strip() or None,
        node_id=str(receipt.get("node_id") or "").strip() or None,
        failure_category=str(receipt.get("failure_category") or "").strip() or None,
        agent=str(receipt.get("agent") or "").strip() or None,
        provider_slug=str(receipt.get("provider_slug") or "").strip() or None,
        model_slug=str(receipt.get("model_slug") or "").strip() or None,
    )
    for field in (
        "failure_code",
        "failure_category",
        "agent",
        "provider_slug",
        "model_slug",
    ):
        expected_value = str(expected.get(field) or "").strip()
        if expected_value and str(candidate.get(field) or "").strip() != expected_value:
            return False
    expected_job = str(expected.get("job_label") or "").strip()
    expected_node = str(expected.get("node_id") or "").strip()
    candidate_node = str(candidate.get("node_id") or candidate.get("job_label") or "").strip()
    if expected_job and candidate_node != expected_job:
        return False
    if expected_node and candidate_node != expected_node:
        return False
    return True


# -- conn-dependent evidence queries ----------------------------------------


def _query_rows_with_error(conn: Any, query: str, *params: object) -> tuple[list[Any], str | None]:
    try:
        return list(conn.execute(query, *params) or []), None
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


def find_signature_receipts(
    conn: Any,
    *,
    failure_code: str | None,
    node_id: str | None,
    limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    clauses: list[str] = []
    params: list[object] = []
    idx = 1
    if failure_code:
        clauses.append(f"failure_code = ${idx}")
        params.append(failure_code)
        idx += 1
    if node_id:
        clauses.append(f"node_id = ${idx}")
        params.append(node_id)
        idx += 1
    if not clauses:
        return [], None
    params.append(limit)
    rows, error = _query_rows_with_error(
        conn,
        (
            "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
            "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
            f"FROM receipts WHERE {' AND '.join(clauses)} "
            f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT ${idx}"
        ),
        *params,
    )
    return [row_to_receipt_summary(row) for row in rows], error


def compare_write_sets(conn: Any, latest_receipt: dict[str, Any] | None) -> dict[str, Any]:
    if latest_receipt is None:
        return {
            "baseline_receipt_id": None,
            "added_paths": (),
            "removed_paths": (),
            "unchanged_paths": (),
            "current_write_count": 0,
            "baseline_write_count": 0,
            "note": "no receipt evidence available",
            "error": None,
        }
    node_id = str(latest_receipt.get("node_id") or "").strip()
    workflow_id = str(latest_receipt.get("workflow_id") or "").strip()
    receipt_id = str(latest_receipt.get("receipt_id") or "").strip()
    if not node_id or not workflow_id or not receipt_id:
        return {
            "baseline_receipt_id": None,
            "added_paths": (),
            "removed_paths": (),
            "unchanged_paths": (),
            "current_write_count": len(latest_receipt.get("write_paths") or ()),
            "baseline_write_count": 0,
            "note": "missing workflow or node identity for comparison",
            "error": None,
        }
    row = None
    query_error = None
    try:
        row = conn.fetchrow(
            """
            SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at,
                   executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs
              FROM receipts
             WHERE workflow_id = $1
               AND node_id = $2
               AND status = 'succeeded'
               AND receipt_id <> $3
             ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST
             LIMIT 1
            """,
            workflow_id,
            node_id,
            receipt_id,
        )
    except Exception as exc:
        row = None
        query_error = build_query_error(
            scope="write_set_diff",
            reason_code="write_set_diff.query_failed",
            error=exc,
        )
    baseline_receipt = row_to_receipt_summary(row) if row else None
    current_paths = set(latest_receipt.get("write_paths") or ())
    baseline_paths = set(
        baseline_receipt.get("write_paths") or ()
        if baseline_receipt is not None
        else ()
    )
    return {
        "baseline_receipt_id": baseline_receipt.get("receipt_id") if baseline_receipt else None,
        "added_paths": tuple(sorted(current_paths - baseline_paths)),
        "removed_paths": tuple(sorted(baseline_paths - current_paths)),
        "unchanged_paths": tuple(sorted(current_paths & baseline_paths)),
        "current_write_count": len(current_paths),
        "baseline_write_count": len(baseline_paths),
        "note": (
            None
            if baseline_receipt
            else ("baseline receipt lookup failed" if query_error else "no comparable successful receipt")
        ),
        "error": query_error,
    }


def load_verification_rows(
    conn: Any,
    table_name: str,
    id_field: str,
    refs: tuple[str, ...],
) -> tuple[dict[str, dict[str, Any]], str | None]:
    if not refs:
        return {}, None
    rows, error = _query_rows_with_error(
        conn,
        f"""
        SELECT {id_field},
               verifier_ref,
               target_kind,
               target_ref,
               status,
               inputs,
               outputs,
               decision_ref,
               attempted_at,
               duration_ms
          FROM {table_name}
         WHERE {id_field} = ANY($1::text[])
        """,
        list(refs),
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        ref = str(row.get(id_field) or "").strip()
        if not ref:
            continue
        result[ref] = {
            id_field: ref,
            "verifier_ref": str(row.get("verifier_ref") or ""),
            "target_kind": str(row.get("target_kind") or ""),
            "target_ref": str(row.get("target_ref") or ""),
            "status": str(row.get("status") or ""),
            "inputs": _json_object(row.get("inputs")),
            "outputs": _json_object(row.get("outputs")),
            "decision_ref": str(row.get("decision_ref") or ""),
            "attempted_at": _coerce_datetime(row.get("attempted_at")),
            "duration_ms": int(row.get("duration_ms") or 0),
        }
    return result, error


def historical_fix_evidence(
    conn: Any,
    bug_id: str,
    evidence_links: list[dict[str, Any]],
    *,
    load_verification_rows_fn: Any | None = None,
) -> dict[str, Any]:
    def _load_rows(
        table_name: str,
        id_field: str,
        refs: tuple[str, ...],
    ) -> tuple[dict[str, dict[str, Any]], str | None]:
        if load_verification_rows_fn is not None:
            return load_verification_rows_fn(table_name, id_field, refs)
        return load_verification_rows(conn, table_name, id_field, refs)

    validation_links = [
        evidence
        for evidence in evidence_links
        if evidence.get("evidence_role") == "validates_fix"
        and evidence.get("evidence_kind") == "verification_run"
    ]
    attempted_fix_links = [
        evidence
        for evidence in evidence_links
        if evidence.get("evidence_role") == "attempted_fix"
        and evidence.get("evidence_kind") == "healing_run"
    ]
    verification_rows, verification_error = _load_rows(
        "verification_runs",
        "verification_run_id",
        tuple(
            str(link.get("evidence_ref") or "").strip()
            for link in validation_links
            if str(link.get("evidence_ref") or "").strip()
        ),
    )
    healing_rows, healing_error = _load_rows(
        "healing_runs",
        "healing_run_id",
        tuple(
            str(link.get("evidence_ref") or "").strip()
            for link in attempted_fix_links
            if str(link.get("evidence_ref") or "").strip()
        ),
    )
    verified_rows = [
        verification_rows.get(str(link.get("evidence_ref") or ""))
        for link in validation_links
        if verification_passed(
            verification_rows.get(str(link.get("evidence_ref") or ""), {}).get("status")
        )
    ]
    latest_validation = max(
        (row for row in verified_rows if isinstance(row, dict)),
        key=attempted_at_sort_key,
        default=None,
    )
    latest_attempted_fix = max(
        (
            healing_rows.get(str(link.get("evidence_ref") or ""))
            for link in attempted_fix_links
            if isinstance(healing_rows.get(str(link.get("evidence_ref") or "")), dict)
        ),
        key=attempted_at_sort_key,
        default=None,
    )
    errors: list[str] = []
    if verification_error:
        errors.append(f"verification_runs.query_failed:{verification_error}")
    if healing_error:
        errors.append(f"healing_runs.query_failed:{healing_error}")
    return {
        "fix_verified": bool(verified_rows),
        "linked_validation_count": len(validation_links),
        "verified_validation_count": len(verified_rows),
        "last_validation": latest_validation,
        "attempted_fix_count": len(attempted_fix_links),
        "last_attempted_fix": latest_attempted_fix,
        "errors": tuple(errors),
    }


def build_blast_radius(
    conn: Any,
    *,
    failure_code: str | None,
    node_id: str | None,
) -> dict[str, Any]:
    if not failure_code and not node_id:
        return {
            "window": _BUG_BLAST_RADIUS_WINDOW_SQL,
            "occurrence_count": 0,
            "distinct_runs": 0,
            "distinct_workflows": 0,
            "distinct_nodes": 0,
            "distinct_requests": 0,
            "distinct_agents": 0,
            "error": None,
        }
    clauses: list[str] = []
    params: list[object] = []
    idx = 1
    if failure_code:
        clauses.append(f"failure_code = ${idx}")
        params.append(failure_code)
        idx += 1
    if node_id:
        clauses.append(f"node_id = ${idx}")
        params.append(node_id)
        idx += 1
    row = {}
    query_error = None
    try:
        row = conn.fetchrow(
            f"""
            SELECT COUNT(*) AS occurrence_count,
                   COUNT(DISTINCT run_id) AS distinct_runs,
                   COUNT(DISTINCT workflow_id) AS distinct_workflows,
                   COUNT(DISTINCT node_id) AS distinct_nodes,
                   COUNT(DISTINCT request_id) AS distinct_requests,
                   COUNT(DISTINCT COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, 'unknown')) AS distinct_agents
              FROM receipts
             WHERE {' AND '.join(clauses)}
               AND COALESCE(finished_at, started_at) >= NOW() - INTERVAL '{_BUG_BLAST_RADIUS_WINDOW_SQL}'
            """,
            *params,
        ) or {}
    except Exception as exc:
        row = {}
        query_error = build_query_error(
            scope="blast_radius",
            reason_code="blast_radius.query_failed",
            error=exc,
        )
    return {
        "window": _BUG_BLAST_RADIUS_WINDOW_SQL,
        "occurrence_count": int(row.get("occurrence_count") or 0),
        "distinct_runs": int(row.get("distinct_runs") or 0),
        "distinct_workflows": int(row.get("distinct_workflows") or 0),
        "distinct_nodes": int(row.get("distinct_nodes") or 0),
        "distinct_requests": int(row.get("distinct_requests") or 0),
        "distinct_agents": int(row.get("distinct_agents") or 0),
        "error": query_error,
    }


def assemble_failure_packet(
    *,
    bug: Any,
    bug_status_fixed: Any,
    evidence_links: list[dict[str, Any]],
    explicit_receipts: list[dict[str, Any]],
    fallback_receipts: list[dict[str, Any]],
    verification_rows: dict[str, dict[str, Any]],
    healing_rows: dict[str, dict[str, Any]],
    verification_run_refs: set[str],
    healing_run_refs: set[str],
    query_errors: list[dict[str, Any]],
    signature: dict[str, Any],
    failure_code: str | None,
    node_id: str | None,
    replay_action_result: dict[str, Any],
    write_set_diff: dict[str, Any],
    blast_radius: dict[str, Any],
    historical_fixes: dict[str, Any],
    backfill: dict[str, Any] | None,
    semantic_neighbors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from runtime.bug_tagging import ordered_unique, payload_keys

    latest_receipt = explicit_receipts[0] if explicit_receipts else None
    inferred_receipt = fallback_receipts[0] if fallback_receipts else None
    validate_fix_links = [
        e for e in evidence_links if e.get("evidence_role") == "validates_fix"
    ]
    attempted_fix_links = [
        e for e in evidence_links if e.get("evidence_role") == "attempted_fix"
    ]
    observed_links = [
        e for e in evidence_links if e.get("evidence_role") == "observed_in"
    ]
    verified_validation_rows = [
        verification_rows.get(str(item.get("evidence_ref") or ""))
        for item in validate_fix_links
        if verification_passed(
            verification_rows.get(str(item.get("evidence_ref") or ""), {}).get("status")
        )
    ]
    validation_times = [
        row.get("attempted_at")
        for row in verified_validation_rows
        if isinstance(row, dict) and isinstance(row.get("attempted_at"), datetime)
    ]
    observed_receipt_ids = {
        str(item.get("evidence_ref") or "")
        for item in observed_links
        if str(item.get("evidence_kind") or "") == "receipt"
    }
    observed_run_ids = {
        str(item.get("evidence_ref") or "")
        for item in observed_links
        if str(item.get("evidence_kind") or "") == "run"
    }
    if bug.discovered_in_receipt_id:
        observed_receipt_ids.add(bug.discovered_in_receipt_id)
    if bug.discovered_in_run_id:
        observed_run_ids.add(bug.discovered_in_run_id)
    first_seen_candidates = [bug.filed_at]
    last_seen_candidates = [bug.updated_at]
    for evidence in evidence_links:
        created_at = evidence.get("created_at")
        if isinstance(created_at, datetime):
            first_seen_candidates.append(created_at)
            last_seen_candidates.append(created_at)
    for receipt in explicit_receipts or fallback_receipts:
        timestamp = receipt.get("timestamp")
        if isinstance(timestamp, datetime):
            first_seen_candidates.append(timestamp)
            last_seen_candidates.append(timestamp)
    fix_verified_at = max(validation_times) if validation_times else None
    last_seen_at = max(last_seen_candidates)
    lifecycle = {
        "first_seen_at": min(first_seen_candidates),
        "last_seen_at": last_seen_at,
        "recurrence_count": max(len(observed_links), len(observed_receipt_ids), 1),
        "impacted_run_count": len(observed_run_ids),
        "impacted_receipt_count": len(observed_receipt_ids),
        "attempted_fix_count": len(attempted_fix_links),
        "fix_validation_count": len(validate_fix_links),
        "verified_validation_count": len(verified_validation_rows),
        "fix_verified_at": fix_verified_at,
        "has_regression_after_fix": bool(
            fix_verified_at is not None
            and isinstance(last_seen_at, datetime)
            and last_seen_at > fix_verified_at
            and len(observed_links) > len(verified_validation_rows)
        ),
    }
    observability_gaps = list(
        build_observability_gaps(
            bug=bug,
            bug_status_fixed=bug_status_fixed,
            evidence_links=evidence_links,
            latest_receipt=latest_receipt,
            fix_validation_count=len(verified_validation_rows),
        )
    )
    if fallback_receipts and not explicit_receipts:
        observability_gaps.append("receipt.inferred_only")
    observability_gaps = list(dict.fromkeys(observability_gaps))
    initial_decision_refs: list[Any] = [bug.decision_ref]
    if latest_receipt:
        initial_decision_refs.extend(latest_receipt.get("decision_refs") or ())
    decision_refs = ordered_unique(initial_decision_refs)
    latest_validation = None
    if validate_fix_links:
        latest_validation = max(
            (
                verification_rows.get(str(link.get("evidence_ref") or ""))
                for link in validate_fix_links
            ),
            key=attempted_at_sort_key,
            default=None,
        )
    latest_healing = None
    if attempted_fix_links:
        latest_healing = max(
            (
                healing_rows.get(str(link.get("evidence_ref") or ""))
                for link in attempted_fix_links
            ),
            key=attempted_at_sort_key,
            default=None,
        )
    decision_refs = ordered_unique(
        [
            *decision_refs,
            latest_validation.get("decision_ref") if isinstance(latest_validation, dict) else None,
            latest_healing.get("decision_ref") if isinstance(latest_healing, dict) else None,
        ]
    )
    replay_context = {
        "ready": bool(
            latest_receipt
            and latest_receipt.get("run_id")
            and latest_receipt.get("receipt_id")
        ),
        "source": "evidence" if latest_receipt else ("fallback" if fallback_receipts else "missing"),
        "workflow_id": latest_receipt.get("workflow_id") if latest_receipt else None,
        "run_id": latest_receipt.get("run_id") if latest_receipt else bug.discovered_in_run_id,
        "receipt_id": latest_receipt.get("receipt_id") if latest_receipt else bug.discovered_in_receipt_id,
        "request_id": latest_receipt.get("request_id") if latest_receipt else None,
        "node_id": latest_receipt.get("node_id") if latest_receipt else None,
        "failure_code": failure_code,
        "repo_snapshot_ref": (
            _json_object(latest_receipt.get("git_provenance")).get("repo_snapshot_ref")
            if latest_receipt is not None
            else None
        ),
        "workspace_ref": (
            latest_receipt.get("inputs", {}).get("workspace_ref")
            if latest_receipt is not None
            else None
        ),
        "runtime_profile_ref": (
            latest_receipt.get("inputs", {}).get("runtime_profile_ref")
            if latest_receipt is not None
            else None
        ),
        "decision_refs": decision_refs,
        "verification_status": latest_receipt.get("verification_status") if latest_receipt else None,
    }
    observability_state = "degraded" if (query_errors or observability_gaps) else "complete"
    return {
        "bug": bug,
        "signature": signature,
        "lifecycle": lifecycle,
        "evidence_links": tuple(evidence_links),
        "recent_receipts": tuple(public_receipt_summary(r) for r in explicit_receipts),
        "latest_receipt": public_receipt_summary(latest_receipt),
        "fallback_receipts": tuple(public_receipt_summary(r) for r in fallback_receipts),
        "trace": {
            "run_ids": tuple(sorted(observed_run_ids)),
            "receipt_ids": tuple(sorted(observed_receipt_ids)),
            "verification_run_ids": tuple(sorted(verification_run_refs)),
            "healing_run_ids": tuple(sorted(healing_run_refs)),
            "decision_refs": decision_refs,
        },
        "replay_context": replay_context,
        "minimal_repro": {
            "ready": replay_context["ready"],
            "run_id": replay_context["run_id"],
            "receipt_id": replay_context["receipt_id"],
            "node_id": latest_receipt.get("node_id") if latest_receipt else None,
            "failure_code": failure_code,
            "workspace_ref": replay_context["workspace_ref"],
            "runtime_profile_ref": replay_context["runtime_profile_ref"],
            "write_paths": tuple(latest_receipt.get("write_paths") or ()) if latest_receipt else (),
            "verified_paths": tuple(latest_receipt.get("verified_paths") or ()) if latest_receipt else (),
            "input_keys": payload_keys(latest_receipt.get("inputs")) if latest_receipt else (),
            "payload_redacted": True,
        },
        "write_set_diff": write_set_diff,
        "observability_state": observability_state,
        "observability_gaps": tuple(observability_gaps),
        "errors": tuple(dict(item) for item in query_errors),
        "fix_verification": {
            "fix_verified": bool(verified_validation_rows),
            "linked_validation_count": len(validate_fix_links),
            "verified_validation_count": len(verified_validation_rows),
            "last_validation": latest_validation,
            "attempted_fix_count": len(attempted_fix_links),
            "last_attempted_fix": latest_healing,
        },
        "blast_radius": blast_radius,
        "historical_fixes": historical_fixes,
        "counterfactual_axes": build_counterfactual_axes(latest_receipt or inferred_receipt),
        "resume_context": dict(getattr(bug, "resume_context", None) or {}),
        "semantic_neighbors": semantic_neighbors
        if isinstance(semantic_neighbors, dict)
        else {
            "reason_code": "bug.semantic_neighbors.unavailable",
            "items": (),
            "note": None,
            "sources_tried": (),
        },
        "agent_actions": {
            "replay": replay_action_result,
        },
        "provenance_backfill": backfill,
    }


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def row_to_evidence_link(row: Any) -> dict[str, Any]:
    return {
        "bug_evidence_link_id": str(row.get("bug_evidence_link_id") or ""),
        "bug_id": str(row.get("bug_id") or ""),
        "evidence_kind": str(row.get("evidence_kind") or ""),
        "evidence_ref": str(row.get("evidence_ref") or ""),
        "evidence_role": str(row.get("evidence_role") or ""),
        "created_at": _coerce_datetime(row.get("created_at")),
        "created_by": str(row.get("created_by") or ""),
        "notes": str(row.get("notes") or "").strip() or None,
    }


def row_to_receipt_summary(row: Any) -> dict[str, Any]:
    inputs = _json_object(row.get("inputs"))
    outputs = _json_object(row.get("outputs"))
    artifacts = _json_object(row.get("artifacts"))
    decision_refs = _json_list(row.get("decision_refs"))
    agent = (
        str(
            inputs.get("agent_slug")
            or inputs.get("agent")
            or outputs.get("author_model")
            or row.get("executor_type")
            or ""
        ).strip()
        or "unknown"
    )
    provider_slug = str(
        outputs.get("provider_slug") or inputs.get("provider_slug") or ""
    ).strip()
    model_slug = str(outputs.get("model_slug") or inputs.get("model_slug") or "").strip()
    if not provider_slug and "/" in agent:
        provider_slug, _, model_slug = agent.partition("/")
    git_provenance = _json_object(outputs.get("git_provenance"))
    workspace_provenance = _json_object(outputs.get("workspace_provenance"))
    failure_classification = _json_object(outputs.get("failure_classification"))
    write_paths = extract_write_paths(inputs, outputs)
    verified_paths = extract_receipt_paths(outputs, key="verified_paths")
    timestamp = _coerce_datetime(row.get("finished_at")) or _coerce_datetime(row.get("started_at"))
    return {
        "receipt_id": str(row.get("receipt_id") or ""),
        "workflow_id": str(row.get("workflow_id") or ""),
        "run_id": str(row.get("run_id") or ""),
        "request_id": str(row.get("request_id") or ""),
        "node_id": str(row.get("node_id") or ""),
        "status": str(row.get("status") or ""),
        "failure_code": str(row.get("failure_code") or ""),
        "timestamp": timestamp,
        "started_at": _coerce_datetime(row.get("started_at")),
        "finished_at": _coerce_datetime(row.get("finished_at")),
        "executor_type": str(row.get("executor_type") or ""),
        "agent": agent,
        "provider_slug": provider_slug or None,
        "model_slug": model_slug or None,
        "latency_ms": int(outputs.get("duration_ms") or 0),
        "verification_status": str(outputs.get("verification_status") or "").strip() or None,
        "failure_category": str(failure_classification.get("category") or "").strip() or None,
        "failure_classification": failure_classification or None,
        "inputs": inputs,
        "outputs": outputs,
        "artifacts": artifacts,
        "decision_refs": decision_refs,
        "git_provenance": git_provenance,
        "workspace_provenance": workspace_provenance,
        "write_paths": write_paths,
        "verified_paths": verified_paths,
    }
