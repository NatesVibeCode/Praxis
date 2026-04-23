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
    raise VerifierBuiltinsError(f"unknown builtin verifier: {builtin_ref}")


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
