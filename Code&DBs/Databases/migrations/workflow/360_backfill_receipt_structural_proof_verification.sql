-- Migration 360: Backfill structural-proof verification blocks for existing receipts.
--
-- Migration 359 registered verifier.receipt.structural_proof and the runtime
-- now attaches a verification block to every NEW workflow receipt. This
-- migration runs the same structural check as a pure SQL UPDATE over the
-- 17K+ historical receipts so the verification_coverage metric reflects
-- the full corpus, not just receipts written after the runtime change.
--
-- The check mirrors verifier_builtins.builtin_verify_receipt_structural_proof:
--   * has git_provenance
--   * if git_provenance is available, has repo_snapshot_ref
--   * git_provenance does not duplicate workspace_root / workspace_ref /
--     runtime_profile_ref (those belong in workspace_provenance)
--
-- Receipts that already carry a verification block are skipped — explicit
-- verifier runs (run_registered_verifier output) are authoritative.

BEGIN;

WITH structural_check AS (
    SELECT
        receipt_id,
        outputs,
        (outputs ? 'git_provenance') AS has_git_provenance,
        (COALESCE(outputs->'git_provenance'->>'reason_code', '') = 'git_provenance_unavailable') AS git_unavailable,
        (COALESCE(outputs->'git_provenance'->>'repo_snapshot_ref', '') <> '') AS has_repo_snapshot_ref,
        (
            (outputs->'git_provenance' ? 'workspace_root')
            OR (outputs->'git_provenance' ? 'workspace_ref')
            OR (outputs->'git_provenance' ? 'runtime_profile_ref')
        ) AS duplicated_git_fields,
        (outputs ? 'workspace_provenance') AS has_workspace_provenance,
        (outputs ? 'route_identity') AS has_route_identity
    FROM receipts
    WHERE NOT (outputs ? 'verification')
),
classified AS (
    SELECT
        receipt_id,
        outputs,
        has_git_provenance,
        git_unavailable,
        has_repo_snapshot_ref,
        duplicated_git_fields,
        has_workspace_provenance,
        has_route_identity,
        (
            has_git_provenance
            AND (git_unavailable OR has_repo_snapshot_ref)
            AND NOT duplicated_git_fields
        ) AS passed,
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN NOT has_git_provenance THEN 'git_provenance' END,
                CASE
                    WHEN has_git_provenance
                         AND NOT git_unavailable
                         AND NOT has_repo_snapshot_ref
                    THEN 'git_provenance.repo_snapshot_ref'
                END,
                CASE WHEN duplicated_git_fields THEN 'git_provenance.no_duplicated_workspace_fields' END
            ],
            NULL
        ) AS missing_fields
    FROM structural_check
)
UPDATE receipts AS r
SET outputs = r.outputs || jsonb_build_object(
    'verification', jsonb_build_object(
        'verifier_ref', 'verifier.receipt.structural_proof',
        'kind', 'structural_proof',
        'status', CASE WHEN c.passed THEN 'passed' ELSE 'failed' END,
        'checks', jsonb_build_object(
            'has_git_provenance', c.has_git_provenance,
            'git_unavailable_marker', c.git_unavailable,
            'has_repo_snapshot_ref', c.has_repo_snapshot_ref,
            'duplicated_git_fields', c.duplicated_git_fields,
            'has_workspace_provenance', c.has_workspace_provenance,
            'has_route_identity', c.has_route_identity
        ),
        'missing', to_jsonb(c.missing_fields),
        'backfilled', true,
        'backfill_decision_ref', 'decision.verifier_registry.receipt_structural_proof.20260430'
    ),
    'verification_status', COALESCE(
        r.outputs->>'verification_status',
        CASE WHEN c.passed THEN 'passed' ELSE 'failed' END
    )
)
FROM classified AS c
WHERE r.receipt_id = c.receipt_id;

INSERT INTO platform_config (
    config_key,
    config_value,
    value_type,
    category,
    description,
    min_value,
    max_value
) VALUES (
    'receipts.structural_proof_backfill.20260430',
    'applied',
    'string',
    'verification',
    'Marker proving Migration 360 applied the historical receipt structural-proof verification backfill.',
    NULL,
    NULL
)
ON CONFLICT (config_key) DO UPDATE SET
    config_value = EXCLUDED.config_value,
    value_type = EXCLUDED.value_type,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    updated_at = now();

COMMIT;
