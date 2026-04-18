-- Dataset refinery projection tables (rebuildable from dataset_promotions).
--
-- These tables are read models maintained by DatasetCurationProjectionSubscriber
-- consuming CHANNEL_DATASET events. They can be dropped and rebuilt from the
-- authority tables in 155_dataset_refinery_authority.sql at any time.

CREATE TABLE IF NOT EXISTS dataset_curated_examples (
    promotion_id      text PRIMARY KEY
        REFERENCES dataset_promotions (promotion_id) ON DELETE CASCADE,
    specialist_target text NOT NULL,
    split_tag         text,
    prompt            jsonb NOT NULL,
    target_output     jsonb NOT NULL,
    candidate_id      text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE RESTRICT,
    is_active         boolean NOT NULL DEFAULT true,
    refreshed_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_curated_examples_prompt_object
        CHECK (jsonb_typeof(prompt) = 'object'),
    CONSTRAINT dataset_curated_examples_target_object
        CHECK (jsonb_typeof(target_output) = 'object'),
    CONSTRAINT dataset_curated_examples_split_check
        CHECK (split_tag IS NULL OR split_tag IN ('train', 'eval', 'holdout'))
);

COMMENT ON TABLE dataset_curated_examples IS 'SFT-family read model. One row per promotion of dataset_family=sft.';

CREATE TABLE IF NOT EXISTS dataset_curated_preference_pairs (
    promotion_id           text PRIMARY KEY
        REFERENCES dataset_promotions (promotion_id) ON DELETE CASCADE,
    specialist_target      text NOT NULL,
    split_tag              text,
    prompt                 jsonb NOT NULL,
    chosen_output          jsonb NOT NULL,
    rejected_output        jsonb NOT NULL,
    chosen_candidate_id    text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE RESTRICT,
    rejected_candidate_id  text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE RESTRICT,
    pair_evidence          jsonb NOT NULL,
    is_active              boolean NOT NULL DEFAULT true,
    refreshed_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_curated_preference_pairs_distinct
        CHECK (chosen_candidate_id <> rejected_candidate_id),
    CONSTRAINT dataset_curated_preference_pairs_evidence_object
        CHECK (jsonb_typeof(pair_evidence) = 'object'),
    CONSTRAINT dataset_curated_preference_pairs_split_check
        CHECK (split_tag IS NULL OR split_tag IN ('train', 'eval', 'holdout'))
);

COMMENT ON TABLE dataset_curated_preference_pairs IS 'Preference-pair read model. pair_evidence records why these two candidates are comparable (same task, divergent outcome).';

CREATE TABLE IF NOT EXISTS dataset_curated_eval_cases (
    promotion_id            text PRIMARY KEY
        REFERENCES dataset_promotions (promotion_id) ON DELETE CASCADE,
    specialist_target       text NOT NULL,
    case_input              jsonb NOT NULL,
    expected_output         jsonb,
    rubric                  jsonb,
    difficulty_tags         text[] NOT NULL DEFAULT ARRAY[]::text[],
    domain_tags             text[] NOT NULL DEFAULT ARRAY[]::text[],
    revision_scope          jsonb NOT NULL,
    excluded_from_training  boolean NOT NULL DEFAULT true,
    is_active               boolean NOT NULL DEFAULT true,
    refreshed_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_curated_eval_cases_input_object
        CHECK (jsonb_typeof(case_input) = 'object'),
    CONSTRAINT dataset_curated_eval_cases_revision_object
        CHECK (jsonb_typeof(revision_scope) = 'object')
);

COMMENT ON TABLE dataset_curated_eval_cases IS 'Eval-set read model. excluded_from_training defaults true and gates training exports.';

CREATE TABLE IF NOT EXISTS dataset_export_manifests (
    manifest_id        text PRIMARY KEY,
    dataset_family     text NOT NULL,
    specialist_target  text NOT NULL,
    split_tag          text NOT NULL,
    promotion_ids      text[] NOT NULL,
    output_path        text NOT NULL,
    output_sha256      text NOT NULL,
    row_count          integer NOT NULL CHECK (row_count >= 0),
    exported_by        text NOT NULL,
    exported_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_export_manifests_family_check
        CHECK (dataset_family IN ('sft', 'preference', 'eval', 'routing')),
    CONSTRAINT dataset_export_manifests_split_check
        CHECK (split_tag IN ('train', 'eval', 'holdout')),
    CONSTRAINT dataset_export_manifests_path_nonblank
        CHECK (btrim(output_path) <> '')
);

COMMENT ON TABLE dataset_export_manifests IS 'Durable record of every dataset export (path, content hash, row count, who).';

-- Lineage view: flatten promotion -> candidate -> evidence links so operators
-- can audit "why does this promoted row exist" with a single SELECT.
CREATE OR REPLACE VIEW dataset_lineage_v AS
SELECT
    p.promotion_id,
    p.dataset_family,
    p.specialist_target,
    p.split_tag,
    p.promotion_kind,
    p.policy_id,
    p.decision_ref,
    p.superseded_by,
    p.promoted_at,
    c.candidate_id,
    c.candidate_kind,
    c.source_receipt_id,
    c.source_run_id,
    c.workflow_definition_id,
    c.admitted_definition_hash,
    c.staleness_status,
    c.redaction_status,
    l.evidence_kind,
    l.evidence_ref,
    l.evidence_role
FROM dataset_promotions p
JOIN LATERAL unnest(p.candidate_ids) AS cand(candidate_id) ON TRUE
JOIN dataset_raw_candidates c ON c.candidate_id = cand.candidate_id
LEFT JOIN dataset_candidate_evidence_links l ON l.candidate_id = c.candidate_id;

COMMENT ON VIEW dataset_lineage_v IS 'Flat audit view: every promoted candidate joined to its evidence links. Filter by superseded_by IS NULL to see active promotions only.';
