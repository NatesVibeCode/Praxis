"""Builtin verifier and healer implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


class VerifierBuiltinsError(RuntimeError):
    """Raised when a builtin verifier or healer ref is unknown."""


def _workflow_database_status_payload(*, bootstrap: bool) -> dict[str, Any]:
    from surfaces._boot import workflow_database_status_payload

    return workflow_database_status_payload(bootstrap=bootstrap)


def builtin_verify_schema_authority(*, inputs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    health = _workflow_database_status_payload(bootstrap=False)
    passed = bool(health.get("schema_bootstrapped")) and not health.get("missing_schema_objects")
    return (
        "passed" if passed else "failed",
        {
            "health": health,
            "summary": {
                "schema_bootstrapped": bool(health.get("schema_bootstrapped")),
                "missing_schema_object_count": len(health.get("missing_schema_objects") or []),
            },
        },
    )


def builtin_verify_receipt_provenance(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    from runtime.receipt_store import proof_metrics

    db = connection_fn(conn)
    metrics = proof_metrics(conn=db)
    summary_row = db.fetchrow(
        """
        SELECT
            COUNT(*) AS receipts_total,
            COUNT(*) FILTER (
                WHERE outputs ? 'git_provenance'
            ) AS receipts_with_git_provenance,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->'git_provenance'->>'repo_snapshot_ref', '') <> ''
            ) AS receipts_with_repo_snapshot_ref,
            COUNT(*) FILTER (
                WHERE outputs ? 'git_provenance'
                  AND (
                    outputs->'git_provenance' ? 'workspace_root'
                    OR outputs->'git_provenance' ? 'workspace_ref'
                    OR outputs->'git_provenance' ? 'runtime_profile_ref'
                  )
            ) AS duplicated_git_fields,
            COUNT(*) FILTER (
                WHERE outputs ? 'git_provenance'
                  AND COALESCE(outputs->'git_provenance'->>'reason_code', '') = 'git_provenance_unavailable'
            ) AS unavailable_git_provenance
        FROM receipts
        """
    ) or {}
    receipts_total = int(summary_row.get("receipts_total") or 0)
    with_git = int(summary_row.get("receipts_with_git_provenance") or 0)
    with_repo_snapshot_ref = int(summary_row.get("receipts_with_repo_snapshot_ref") or 0)
    unavailable_git = int(summary_row.get("unavailable_git_provenance") or 0)
    duplicated_git_fields = int(summary_row.get("duplicated_git_fields") or 0)
    eligible_git = max(receipts_total - unavailable_git, 0)
    missing_git = max(receipts_total - with_git, 0)
    missing_compacted = max(eligible_git - with_repo_snapshot_ref, 0)
    passed = duplicated_git_fields == 0 and missing_git == 0 and missing_compacted == 0
    return (
        "passed" if passed else "failed",
        {
            "proof_metrics": metrics["receipts"],
            "summary": {
                "total_receipts": receipts_total,
                "eligible_git_receipts": eligible_git,
                "with_git_provenance": with_git,
                "with_repo_snapshot_ref": with_repo_snapshot_ref,
                "missing_git_receipts": missing_git,
                "missing_compacted_git_receipts": missing_compacted,
                "duplicated_git_field_receipts": duplicated_git_fields,
                "unavailable_git_provenance_receipts": unavailable_git,
            },
            "inputs": dict(inputs),
        },
    )


def builtin_verify_memory_proof_links(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    db = connection_fn(conn)
    row = db.fetchrow(
        """
        WITH verification_receipts AS (
            SELECT receipt_id
              FROM receipts
             WHERE COALESCE(outputs->>'verification_status', '') <> ''
        ),
        verification_entities AS (
            SELECT id,
                   COALESCE(metadata->>'receipt_id', '') AS receipt_id
              FROM memory_entities
             WHERE archived = false
               AND entity_type = 'fact'
               AND COALESCE(metadata->>'entity_subtype', '') = 'verification_result'
        ),
        recorded_entities AS (
            SELECT DISTINCT source_id
              FROM memory_edges
             WHERE active = true
               AND relation_type = 'recorded_in'
        )
        SELECT
            (SELECT COUNT(*) FROM verification_receipts) AS receipt_count,
            (SELECT COUNT(*) FROM verification_entities) AS entity_count,
            (
                SELECT COUNT(*)
                  FROM verification_receipts vr
                  LEFT JOIN verification_entities ve
                    ON ve.receipt_id = vr.receipt_id
                 WHERE ve.id IS NULL
            ) AS receipts_missing_verification_entity,
            (
                SELECT COUNT(*)
                  FROM verification_entities ve
                  LEFT JOIN recorded_entities re
                    ON re.source_id = ve.id
                 WHERE re.source_id IS NULL
            ) AS entities_missing_recorded_in
        """
    ) or {}
    receipt_count = int(row.get("receipt_count") or 0)
    entity_count = int(row.get("entity_count") or 0)
    receipts_missing = int(row.get("receipts_missing_verification_entity") or 0)
    entities_missing = int(row.get("entities_missing_recorded_in") or 0)
    passed = receipts_missing == 0 and entities_missing == 0 and entity_count >= receipt_count
    return (
        "passed" if passed else "failed",
        {
            "summary": {
                "receipts_with_verification_status": receipt_count,
                "verification_entities": entity_count,
                "receipts_missing_verification_entity": receipts_missing,
                "entities_missing_recorded_in": entities_missing,
            },
            "inputs": dict(inputs),
        },
    )


def run_builtin_verifier(
    builtin_ref: str,
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    if builtin_ref == "schema_authority":
        return builtin_verify_schema_authority(inputs=inputs)
    if builtin_ref == "receipt_provenance":
        return builtin_verify_receipt_provenance(inputs=inputs, conn=conn, connection_fn=connection_fn)
    if builtin_ref == "memory_proof_links":
        return builtin_verify_memory_proof_links(inputs=inputs, conn=conn, connection_fn=connection_fn)
    if builtin_ref == "connector_capability":
        return builtin_verify_connector_capability(inputs=inputs, conn=conn, connection_fn=connection_fn)
    if builtin_ref == "authority_impact_contract":
        return builtin_verify_authority_impact_contract(
            inputs=inputs, conn=conn, connection_fn=connection_fn
        )
    raise VerifierBuiltinsError(f"unknown builtin verifier: {builtin_ref}")


def builtin_verify_authority_impact_contract(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    """Defense-in-depth verifier for the authority impact contract.

    Inputs require `candidate_id`. The verifier:

    1. Loads the candidate row (intended_files + base_head_ref).
    2. Decides whether the candidate is authority-bearing via
       runtime.workflow.authority_overlap.is_authority_bearing.
    3. If authority-bearing, asserts the impact contract is complete:
       - declared `agent_declared` impact rows exist,
       - the latest preflight record is `passed`,
       - preflight `base_head_ref_at_preflight` matches the candidate base,
       - preflight `impact_contract_complete=True` and `contested_count=0`.
    4. Returns `passed` only when all gates are green; otherwise `failed`
       with a structured findings dict naming the specific reason.

    This verifier may be added to a candidate's verifier_inputs as a
    second check alongside the test verifier, so materialize refuses the
    candidate even if preflight or review somehow bypassed the gate.
    """

    from runtime.workflow.authority_overlap import is_authority_bearing

    candidate_id = str(inputs.get("candidate_id") or "").strip()
    if not candidate_id:
        return "error", {
            "reason_code": "verifier.authority_impact_contract.candidate_id_required",
            "error": "candidate_id is required in verifier inputs",
        }

    db = connection_fn(conn)
    candidate_row = db.fetchrow(
        """
        SELECT candidate_id::text       AS candidate_id,
               base_head_ref,
               intended_files
          FROM code_change_candidate_payloads
         WHERE candidate_id = $1::uuid
        """,
        candidate_id,
    )
    if candidate_row is None:
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.candidate_not_found",
            "candidate_id": candidate_id,
        }
    candidate = dict(candidate_row)
    intended_files = list(candidate.get("intended_files") or [])
    requires_contract = is_authority_bearing(intended_files)

    if not requires_contract:
        return "passed", {
            "reason_code": "verifier.authority_impact_contract.not_authority_bearing",
            "candidate_id": candidate_id,
            "intended_files": intended_files,
            "verdict": "no contract required for this candidate",
        }

    declared_count_row = db.fetchrow(
        """
        SELECT COUNT(*) AS declared_count
          FROM candidate_authority_impacts
         WHERE candidate_id = $1::uuid
           AND discovery_source = 'agent_declared'
        """,
        candidate_id,
    )
    declared_count = int((dict(declared_count_row) if declared_count_row else {}).get("declared_count") or 0)
    if declared_count == 0:
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.declared_impacts_missing",
            "candidate_id": candidate_id,
            "intended_files": intended_files,
            "declared_impact_count": 0,
        }

    preflight_row = db.fetchrow(
        """
        SELECT preflight_id::text       AS preflight_id,
               preflight_status::text   AS preflight_status,
               base_head_ref_at_preflight,
               impact_contract_complete,
               contested_impact_count,
               temp_verifier_passed,
               runtime_addition_impact_count
          FROM candidate_latest_preflight
         WHERE candidate_id = $1::uuid
        """,
        candidate_id,
    )
    if preflight_row is None:
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.preflight_required",
            "candidate_id": candidate_id,
            "declared_impact_count": declared_count,
        }
    preflight = dict(preflight_row)

    candidate_base = str(candidate.get("base_head_ref") or "")
    preflight_base = str(preflight.get("base_head_ref_at_preflight") or "")
    if candidate_base and preflight_base and candidate_base != preflight_base:
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.preflight_stale",
            "candidate_id": candidate_id,
            "candidate_base_head_ref": candidate_base,
            "preflight_base_head_ref": preflight_base,
            "preflight_id": preflight.get("preflight_id"),
        }

    status = str(preflight.get("preflight_status") or "")
    if status != "passed":
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.preflight_not_passed",
            "candidate_id": candidate_id,
            "preflight_id": preflight.get("preflight_id"),
            "preflight_status": status,
            "contested_impact_count": preflight.get("contested_impact_count"),
        }

    if not bool(preflight.get("impact_contract_complete")):
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.contract_incomplete",
            "candidate_id": candidate_id,
            "preflight_id": preflight.get("preflight_id"),
            "contested_impact_count": preflight.get("contested_impact_count"),
        }

    contested = int(preflight.get("contested_impact_count") or 0)
    if contested > 0:
        return "failed", {
            "reason_code": "verifier.authority_impact_contract.contested_impacts_present",
            "candidate_id": candidate_id,
            "preflight_id": preflight.get("preflight_id"),
            "contested_impact_count": contested,
        }

    return "passed", {
        "reason_code": "verifier.authority_impact_contract.green",
        "candidate_id": candidate_id,
        "preflight_id": preflight.get("preflight_id"),
        "declared_impact_count": declared_count,
        "runtime_addition_impact_count": preflight.get("runtime_addition_impact_count"),
        "temp_verifier_passed": preflight.get("temp_verifier_passed"),
    }


def builtin_verify_connector_capability(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    from runtime.integrations.connector_verifier import verify_connector

    db = connection_fn(conn)
    slug = str(inputs.get("connector_slug") or "").strip()
    if not slug:
        return "error", {"error": "connector_slug is required in inputs"}
    result = verify_connector(slug, db)
    if result.get("error"):
        return "error", result
    status = "passed" if result.get("verification_status") == "verified" else "failed"
    return status, result


def builtin_heal_schema_bootstrap(*, inputs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    health = _workflow_database_status_payload(bootstrap=True)
    status = "succeeded" if bool(health.get("schema_bootstrapped")) else "failed"
    return status, {"health": health, "inputs": dict(inputs)}


def builtin_heal_receipt_provenance_backfill(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    from runtime.receipt_store import backfill_receipt_provenance

    db = connection_fn(conn)
    run_id = str(inputs.get("run_id") or "").strip() or None
    limit = inputs.get("limit")
    if limit is not None:
        limit = int(limit)
    repo_root = str(inputs.get("repo_root") or "").strip() or None
    result = backfill_receipt_provenance(
        run_id=run_id,
        limit=limit,
        repo_root=repo_root,
        conn=db,
    )
    return "succeeded", result


def builtin_heal_proof_backfill(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    from runtime.post_workflow_sync import backfill_workflow_proof

    db = connection_fn(conn)
    run_id = str(inputs.get("run_id") or "").strip() or None
    limit = inputs.get("limit")
    if limit is not None:
        limit = int(limit)
    repo_root = str(inputs.get("repo_root") or "").strip() or None
    result = backfill_workflow_proof(
        run_id=run_id,
        limit=limit,
        repo_root=repo_root,
        conn=db,
    )
    return "succeeded", result


def run_builtin_healer(
    action_ref: str,
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
    connection_fn,
) -> tuple[str, dict[str, Any]]:
    if action_ref == "schema_bootstrap":
        return builtin_heal_schema_bootstrap(inputs=inputs)
    if action_ref == "receipt_provenance_backfill":
        return builtin_heal_receipt_provenance_backfill(inputs=inputs, conn=conn, connection_fn=connection_fn)
    if action_ref == "proof_backfill":
        return builtin_heal_proof_backfill(inputs=inputs, conn=conn, connection_fn=connection_fn)
    raise VerifierBuiltinsError(f"unknown builtin healer action: {action_ref}")
