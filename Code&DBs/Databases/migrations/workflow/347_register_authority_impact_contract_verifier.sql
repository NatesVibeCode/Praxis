-- Migration 347: register verifier.authority.impact_contract_complete.
--
-- Defense-in-depth verifier for the authority impact contract. Even though
-- preflight + review.approve already gate the contract on the candidate
-- review path, this verifier can be added to a candidates verifier_inputs
-- alongside the test verifier. Materialize then refuses the candidate even
-- if preflight or approve was somehow bypassed.
--
-- Implementation lives in
-- runtime.verifier_builtins.builtin_verify_authority_impact_contract;
-- dispatched via the existing builtin verifier registry by builtin_ref.
--
-- Inputs: {"candidate_id": "<uuid>"}
-- Pass:   intended_files non-authority-bearing OR contract_complete
-- Fail:   declared impacts missing, preflight required/stale/not_passed,
--         contract incomplete, or contested impacts present.

BEGIN;

INSERT INTO verifier_registry (
    verifier_ref,
    display_name,
    description,
    verifier_kind,
    builtin_ref,
    default_inputs,
    enabled,
    decision_ref
) VALUES (
    'verifier.authority.impact_contract_complete',
    'Authority Impact Contract Verifier',
    'Confirms that an authority-bearing code-change candidate carries a complete impact contract: agent-declared impact rows present, preflight passed against the candidate base, impact_contract_complete=true, and contested_impact_count=0. Defense-in-depth alongside the candidate.review preflight gate.',
    'builtin',
    'authority_impact_contract',
    '{}'::jsonb,
    TRUE,
    'decision.architecture_policy.platform_architecture.authority_impact_contract_verifier_defense_in_depth'
)
ON CONFLICT (verifier_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description  = EXCLUDED.description,
    builtin_ref  = EXCLUDED.builtin_ref,
    enabled      = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = now();

COMMIT;
