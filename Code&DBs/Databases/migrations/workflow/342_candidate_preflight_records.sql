-- Migration 342: candidate_preflight_records + register
-- code_change_candidate.preflight as a catalog operation.
--
-- The preflight record is the trusted view of a candidate that reviewers
-- (human or LLM) must read instead of the agent-shaped submission payload.
-- It captures the runtime-derived patch (recomputed from the real base head),
-- the temp verifier outcome, and the runtime-derived authority impact set
-- compared against the agent-declared impact set.
--
-- code_change_candidate.review approve is gated on the existence of a
-- passed preflight whose base_head_ref still matches the candidates
-- base_head_ref. Without that, approve refuses with reason_code
-- code_change_candidate.preflight_required (or preflight_stale).
--
-- Operation catalog registration uses register_operation_atomic (added in
-- migration 239) so the data_dictionary_objects + authority_object_registry
-- + operation_catalog_registry chain stays in one canonical helper.

BEGIN;

CREATE TYPE candidate_preflight_status AS ENUM (
    'pending',                    -- preflight in progress
    'passed',                     -- all checks green
    'failed_patch_divergence',    -- runtime-derived patch differs from agent-declared patch beyond noise
    'failed_temp_verifier',       -- runtime temp verifier did not pass
    'failed_impact_contract',     -- impact contract incomplete or contested
    'superseded_by_revision',     -- newer preflight exists for this candidate
    'superseded_by_head_change'   -- base_head_ref moved; preflight is stale
);

CREATE TABLE IF NOT EXISTS candidate_preflight_records (
    preflight_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL,
    preflight_status candidate_preflight_status NOT NULL DEFAULT 'pending',
    base_head_ref_at_preflight text NOT NULL,
    runtime_derived_patch_sha256 text,
    runtime_derived_patch_artifact_ref text,
    agent_declared_patch_sha256 text,
    patch_divergence jsonb NOT NULL DEFAULT '{}'::jsonb,
    temp_verifier_run_id text,
    temp_verifier_passed boolean,
    impact_contract_complete boolean NOT NULL DEFAULT FALSE,
    impact_contract_findings jsonb NOT NULL DEFAULT '[]'::jsonb,
    runtime_derived_impact_count integer NOT NULL DEFAULT 0,
    agent_declared_impact_count integer NOT NULL DEFAULT 0,
    contested_impact_count integer NOT NULL DEFAULT 0,
    runtime_addition_impact_count integer NOT NULL DEFAULT 0,
    gate_findings jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT candidate_preflight_records_candidate_fkey
        FOREIGN KEY (candidate_id)
        REFERENCES code_change_candidate_payloads (candidate_id)
        ON DELETE CASCADE,
    CONSTRAINT candidate_preflight_records_terminal_completed_at CHECK (
        (preflight_status = 'pending') = (completed_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS candidate_preflight_records_candidate_idx
    ON candidate_preflight_records (candidate_id, created_at DESC);

CREATE INDEX IF NOT EXISTS candidate_preflight_records_status_idx
    ON candidate_preflight_records (preflight_status, created_at DESC);

CREATE OR REPLACE VIEW candidate_latest_preflight AS
SELECT DISTINCT ON (candidate_id)
       candidate_id,
       preflight_id,
       preflight_status,
       base_head_ref_at_preflight,
       runtime_derived_patch_sha256,
       runtime_derived_patch_artifact_ref,
       agent_declared_patch_sha256,
       patch_divergence,
       temp_verifier_run_id,
       temp_verifier_passed,
       impact_contract_complete,
       impact_contract_findings,
       runtime_derived_impact_count,
       agent_declared_impact_count,
       contested_impact_count,
       runtime_addition_impact_count,
       gate_findings,
       created_by,
       created_at,
       completed_at
  FROM candidate_preflight_records
 ORDER BY candidate_id, created_at DESC, preflight_id DESC;

COMMENT ON TABLE candidate_preflight_records IS
    'Trusted, runtime-derived view of a code-change candidate. Reviewers (human or LLM) read this, not the agent-shaped submission payload. Approve refuses without a passed preflight whose base matches.';
COMMENT ON COLUMN candidate_preflight_records.runtime_derived_patch_sha256 IS
    'Sha of the patch the runtime recomputed from the real base head. If this differs from the agent-declared patch, preflight fails with patch_divergence findings.';
COMMENT ON COLUMN candidate_preflight_records.impact_contract_complete IS
    'True only when every runtime-derived impact has either an agent-declared match (validated) or has been recorded as a runtime_addition. False blocks approve.';

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'event_contract.code_change_candidate.preflight_completed',
    'code_change_candidate.preflight_completed',
    'authority.workflow_runs',
    'data_dictionary.object.code_change_candidate_preflight_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'decision.architecture_policy.platform_architecture.candidate_authority_impact_contract',
    jsonb_build_object(
        'note', 'Emitted when code_change_candidate.preflight finishes. Carries preflight_status, runtime_derived_patch_sha256, impact contract counts, and verifier outcome. The review.approve gate keys off the latest preflight per candidate.',
        'expected_payload_fields', jsonb_build_array(
            'candidate_id',
            'preflight_id',
            'preflight_status',
            'runtime_derived_patch_sha256',
            'temp_verifier_passed',
            'impact_contract_complete',
            'runtime_derived_impact_count',
            'agent_declared_impact_count',
            'contested_impact_count',
            'runtime_addition_impact_count'
        )
    )
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'code-change-candidate-preflight',
    p_operation_name        := 'code_change_candidate.preflight',
    p_handler_ref           := 'runtime.operations.commands.candidate_preflight.handle_preflight_candidate',
    p_input_model_ref       := 'runtime.operations.commands.candidate_preflight.PreflightCodeChangeCandidate',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/code_change_candidate/preflight',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_type            := 'code_change_candidate.preflight_completed',
    p_event_required        := TRUE,
    p_receipt_required      := TRUE,
    p_output_schema_ref     := 'data_dictionary.object.code_change_candidate_preflight_completed_event',
    p_decision_ref          := 'decision.architecture_policy.platform_architecture.candidate_authority_impact_contract',
    p_summary               := 'Trusted preflight pass for a code-change candidate: recomputes the patch from the real base head, runs the temp verifier, scans for runtime-derived authority impacts, validates them against the agent-declared impact contract, and writes a candidate_preflight_records row. Approve is gated on a passed preflight.'
);

COMMIT;
