-- Dataset refinery indexes for the common access patterns:
--   1. List candidates by kind/route, newest first.
--   2. Resolve duplicates by signature.
--   3. Find stale candidates fast.
--   4. List eligible candidates per policy.
--   5. List active promotions per (specialist, family, split).
--   6. Walk evidence backwards from a known authority row to all candidates that referenced it.

CREATE INDEX IF NOT EXISTS dataset_raw_candidates_kind_route_ingested_idx
    ON dataset_raw_candidates (candidate_kind, route_slug, ingested_at DESC);

CREATE INDEX IF NOT EXISTS dataset_raw_candidates_dedupe_signature_idx
    ON dataset_raw_candidates (dedupe_signature);

CREATE INDEX IF NOT EXISTS dataset_raw_candidates_staleness_partial_idx
    ON dataset_raw_candidates (staleness_status)
    WHERE staleness_status <> 'fresh';

CREATE INDEX IF NOT EXISTS dataset_raw_candidates_redaction_partial_idx
    ON dataset_raw_candidates (redaction_status)
    WHERE redaction_status <> 'clean';

CREATE INDEX IF NOT EXISTS dataset_raw_candidates_definition_hash_idx
    ON dataset_raw_candidates (workflow_definition_id, admitted_definition_hash);

CREATE INDEX IF NOT EXISTS dataset_candidate_scores_eligibility_idx
    ON dataset_candidate_scores (policy_id, eligibility, confidence DESC);

CREATE INDEX IF NOT EXISTS dataset_promotions_active_lookup_idx
    ON dataset_promotions (specialist_target, dataset_family, split_tag)
    WHERE superseded_by IS NULL;

CREATE INDEX IF NOT EXISTS dataset_promotions_promoted_at_idx
    ON dataset_promotions (promoted_at DESC);

CREATE INDEX IF NOT EXISTS dataset_candidate_evidence_links_by_evidence_idx
    ON dataset_candidate_evidence_links (evidence_kind, evidence_ref);

CREATE INDEX IF NOT EXISTS dataset_curated_examples_active_lookup_idx
    ON dataset_curated_examples (specialist_target, split_tag)
    WHERE is_active;

CREATE INDEX IF NOT EXISTS dataset_curated_eval_cases_active_lookup_idx
    ON dataset_curated_eval_cases (specialist_target)
    WHERE is_active;

CREATE INDEX IF NOT EXISTS dataset_export_manifests_lookup_idx
    ON dataset_export_manifests (specialist_target, dataset_family, exported_at DESC);
