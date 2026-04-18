-- Dataset refinery authority tables.
--
-- Phase 1 of the dataset refinery: derive evidence-linked training candidates
-- from existing Praxis authority (receipts, verification_runs, semantic_assertions,
-- operator_decisions, bugs, dispatch_runs) and record per-policy scores plus
-- explicit promotion decisions. All five tables defined here are authority:
-- raw_candidates and candidate_scores are written by subscribers from durable
-- evidence; scoring_policies and promotions are written by operator_write helpers.
-- Projection (curated_*) tables live in 156_dataset_refinery_projections.sql.

CREATE TABLE IF NOT EXISTS dataset_raw_candidates (
    candidate_id              text PRIMARY KEY,
    candidate_kind            text NOT NULL,
    source_receipt_id         text NOT NULL
        REFERENCES receipts (receipt_id) ON DELETE RESTRICT,
    source_run_id             text NOT NULL
        REFERENCES workflow_runs (run_id) ON DELETE RESTRICT,
    source_node_id            text NOT NULL,
    source_workflow_id        text,
    task_type                 text,
    route_slug                text,
    persona                   text,
    provider_ref              text,
    model_ref                 text,
    workflow_definition_id    text,
    admitted_definition_hash  text,
    repo_snapshot_ref         text,
    raw_input_ref             jsonb NOT NULL,
    raw_output_ref            jsonb NOT NULL,
    parsed_output_ref         jsonb,
    verifier_summary          jsonb,
    review_summary            jsonb,
    operator_decision_summary jsonb,
    downstream_summary        jsonb,
    linked_bug_ids            text[] NOT NULL DEFAULT ARRAY[]::text[],
    linked_roadmap_ids        text[] NOT NULL DEFAULT ARRAY[]::text[],
    redaction_status          text NOT NULL DEFAULT 'unverified',
    staleness_status          text NOT NULL DEFAULT 'fresh',
    dedupe_signature          text NOT NULL,
    ingested_at               timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_raw_candidates_kind_receipt_unique
        UNIQUE (source_receipt_id, candidate_kind),
    CONSTRAINT dataset_raw_candidates_kind_check
        CHECK (candidate_kind IN ('review', 'triage', 'operator_explain', 'route_choice', 'repair')),
    CONSTRAINT dataset_raw_candidates_redaction_check
        CHECK (redaction_status IN ('clean', 'unverified', 'redaction_required', 'sensitive_blocked')),
    CONSTRAINT dataset_raw_candidates_staleness_check
        CHECK (staleness_status IN ('fresh', 'definition_stale', 'evidence_stale')),
    CONSTRAINT dataset_raw_candidates_dedupe_nonblank
        CHECK (btrim(dedupe_signature) <> '')
);

COMMENT ON TABLE dataset_raw_candidates IS 'Evidence-linked training-data candidates derived from receipts. Append-only; subscribers must be idempotent on (source_receipt_id, candidate_kind).';
COMMENT ON COLUMN dataset_raw_candidates.raw_input_ref IS 'JSONB pointer into receipts.inputs (e.g. {"receipt_path": "$.inputs.review_prompt"}); never the materialized payload.';
COMMENT ON COLUMN dataset_raw_candidates.raw_output_ref IS 'JSONB pointer into receipts.outputs; never the materialized payload.';
COMMENT ON COLUMN dataset_raw_candidates.dedupe_signature IS 'sha256 of normalized (input || candidate_kind || route_slug). Strips ULIDs, timestamps, absolute paths.';

CREATE TABLE IF NOT EXISTS dataset_candidate_evidence_links (
    candidate_id    text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE CASCADE,
    evidence_kind   text NOT NULL,
    evidence_ref    text NOT NULL,
    evidence_role   text NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_candidate_evidence_links_pk
        PRIMARY KEY (candidate_id, evidence_kind, evidence_ref, evidence_role),
    CONSTRAINT dataset_candidate_evidence_links_kind_check
        CHECK (evidence_kind IN (
            'receipt', 'verification_run', 'semantic_assertion',
            'operator_decision', 'bug', 'route_eligibility', 'dispatch_run', 'workflow_run'
        )),
    CONSTRAINT dataset_candidate_evidence_links_role_check
        CHECK (evidence_role IN (
            'source_input', 'verifier_signal', 'reviewer_signal',
            'operator_signal', 'failure_signature', 'downstream_outcome'
        ))
);

COMMENT ON TABLE dataset_candidate_evidence_links IS 'Append-only lineage edges from a candidate to authoritative evidence rows. Cascades on candidate delete (replays rebuild from event log).';

CREATE TABLE IF NOT EXISTS dataset_scoring_policies (
    policy_id          text PRIMARY KEY,
    policy_slug        text NOT NULL UNIQUE,
    specialist_target  text NOT NULL,
    rubric             jsonb NOT NULL,
    auto_promote       boolean NOT NULL DEFAULT false,
    decided_by         text NOT NULL,
    rationale          text NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    superseded_by      text REFERENCES dataset_scoring_policies (policy_id) ON DELETE SET NULL,
    CONSTRAINT dataset_scoring_policies_rubric_object
        CHECK (jsonb_typeof(rubric) = 'object'),
    CONSTRAINT dataset_scoring_policies_rationale_nonblank
        CHECK (btrim(rationale) <> ''),
    CONSTRAINT dataset_scoring_policies_decided_by_nonblank
        CHECK (btrim(decided_by) <> '')
);

COMMENT ON TABLE dataset_scoring_policies IS 'Per-specialist scoring rubrics. auto_promote defaults to false; flipping it requires an explicit operator decision recorded in rationale.';

CREATE TABLE IF NOT EXISTS dataset_candidate_scores (
    candidate_id                    text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE CASCADE,
    policy_id                       text NOT NULL
        REFERENCES dataset_scoring_policies (policy_id) ON DELETE RESTRICT,
    eligibility                     text NOT NULL,
    confidence                      numeric(4,3) NOT NULL,
    factors                         jsonb NOT NULL,
    rationale                       text NOT NULL,
    scored_at                       timestamptz NOT NULL DEFAULT now(),
    scored_against_definition_hash  text,
    CONSTRAINT dataset_candidate_scores_pk
        PRIMARY KEY (candidate_id, policy_id),
    CONSTRAINT dataset_candidate_scores_eligibility_check
        CHECK (eligibility IN (
            'rejected', 'manual_review', 'sft_eligible',
            'preference_eligible', 'eval_eligible', 'routing_eligible'
        )),
    CONSTRAINT dataset_candidate_scores_confidence_range
        CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT dataset_candidate_scores_factors_object
        CHECK (jsonb_typeof(factors) = 'object')
);

COMMENT ON TABLE dataset_candidate_scores IS 'Per-(candidate, policy) score. Re-scoring overwrites; promotions stay append-only.';

CREATE TABLE IF NOT EXISTS dataset_promotions (
    promotion_id      text PRIMARY KEY,
    candidate_ids     text[] NOT NULL,
    dataset_family    text NOT NULL,
    specialist_target text NOT NULL,
    policy_id         text NOT NULL
        REFERENCES dataset_scoring_policies (policy_id) ON DELETE RESTRICT,
    payload           jsonb NOT NULL,
    split_tag         text,
    promoted_by       text NOT NULL,
    promotion_kind    text NOT NULL,
    rationale         text NOT NULL,
    decision_ref      text REFERENCES operator_decisions (operator_decision_id) ON DELETE SET NULL,
    superseded_by     text REFERENCES dataset_promotions (promotion_id) ON DELETE SET NULL,
    superseded_reason text,
    promoted_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_promotions_family_check
        CHECK (dataset_family IN ('sft', 'preference', 'eval', 'routing')),
    CONSTRAINT dataset_promotions_kind_check
        CHECK (promotion_kind IN ('manual', 'auto')),
    CONSTRAINT dataset_promotions_split_check
        CHECK (split_tag IS NULL OR split_tag IN ('train', 'eval', 'holdout')),
    CONSTRAINT dataset_promotions_payload_object
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT dataset_promotions_rationale_nonblank
        CHECK (btrim(rationale) <> ''),
    CONSTRAINT dataset_promotions_promoted_by_nonblank
        CHECK (btrim(promoted_by) <> ''),
    CONSTRAINT dataset_promotions_candidate_ids_nonempty
        CHECK (array_length(candidate_ids, 1) >= 1),
    CONSTRAINT dataset_promotions_preference_pair_arity
        CHECK (
            dataset_family <> 'preference'
            OR array_length(candidate_ids, 1) = 2
        ),
    CONSTRAINT dataset_promotions_decision_required_when_manual
        CHECK (promotion_kind <> 'manual' OR decision_ref IS NOT NULL),
    CONSTRAINT dataset_promotions_supersede_reason_paired
        CHECK ((superseded_by IS NULL) = (superseded_reason IS NULL))
);

COMMENT ON TABLE dataset_promotions IS 'Append-only promotion authority. Edits happen by inserting a new promotion and setting superseded_by + superseded_reason on the prior row.';
