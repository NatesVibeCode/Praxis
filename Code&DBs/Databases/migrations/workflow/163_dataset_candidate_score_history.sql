-- Append-only journal of dataset_candidate_scores writes.
--
-- dataset_candidate_scores uses a (candidate_id, policy_id) primary key and
-- overwrites on re-score, which means the answer to "when did this become
-- sft_eligible, and why did that change?" is unrecoverable. This migration
-- adds a history table and a trigger that captures every INSERT and UPDATE
-- so the eligibility journal survives re-scoring.
--
-- The trigger fires automatically on any write to dataset_candidate_scores,
-- so subscribers and backfill paths do not need to change. Existing score
-- rows are seeded as 'initial' entries so the history covers the full set.

CREATE TABLE IF NOT EXISTS dataset_candidate_score_history (
    history_id                      bigserial PRIMARY KEY,
    candidate_id                    text NOT NULL
        REFERENCES dataset_raw_candidates (candidate_id) ON DELETE CASCADE,
    policy_id                       text NOT NULL
        REFERENCES dataset_scoring_policies (policy_id) ON DELETE RESTRICT,
    eligibility                     text NOT NULL,
    confidence                      numeric(4,3) NOT NULL,
    factors                         jsonb NOT NULL,
    rationale                       text NOT NULL,
    scored_at                       timestamptz NOT NULL,
    scored_against_definition_hash  text,
    previous_eligibility            text,
    previous_confidence             numeric(4,3),
    change_reason                   text NOT NULL,
    recorded_at                     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dataset_candidate_score_history_change_check
        CHECK (change_reason IN ('initial', 'rescore')),
    CONSTRAINT dataset_candidate_score_history_eligibility_check
        CHECK (eligibility IN (
            'rejected', 'manual_review', 'sft_eligible',
            'preference_eligible', 'eval_eligible', 'routing_eligible'
        )),
    CONSTRAINT dataset_candidate_score_history_confidence_range
        CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT dataset_candidate_score_history_factors_object
        CHECK (jsonb_typeof(factors) = 'object'),
    CONSTRAINT dataset_candidate_score_history_prev_consistency
        CHECK (
            (change_reason = 'initial' AND previous_eligibility IS NULL AND previous_confidence IS NULL)
            OR (change_reason = 'rescore' AND previous_eligibility IS NOT NULL)
        )
);

COMMENT ON TABLE dataset_candidate_score_history IS 'Append-only journal of every dataset_candidate_scores write. Captured automatically by dataset_candidate_scores_history_ai trigger. Filter by previous_eligibility IS DISTINCT FROM eligibility to see eligibility transitions.';

CREATE INDEX IF NOT EXISTS dataset_candidate_score_history_by_candidate_idx
    ON dataset_candidate_score_history (candidate_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS dataset_candidate_score_history_by_policy_idx
    ON dataset_candidate_score_history (policy_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS dataset_candidate_score_history_transitions_idx
    ON dataset_candidate_score_history (eligibility, previous_eligibility, recorded_at DESC)
    WHERE previous_eligibility IS DISTINCT FROM eligibility;


CREATE OR REPLACE FUNCTION dataset_candidate_scores_append_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO dataset_candidate_score_history (
            candidate_id, policy_id, eligibility, confidence, factors,
            rationale, scored_at, scored_against_definition_hash,
            previous_eligibility, previous_confidence, change_reason
        ) VALUES (
            NEW.candidate_id, NEW.policy_id, NEW.eligibility, NEW.confidence, NEW.factors,
            NEW.rationale, NEW.scored_at, NEW.scored_against_definition_hash,
            NULL, NULL, 'initial'
        );
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO dataset_candidate_score_history (
            candidate_id, policy_id, eligibility, confidence, factors,
            rationale, scored_at, scored_against_definition_hash,
            previous_eligibility, previous_confidence, change_reason
        ) VALUES (
            NEW.candidate_id, NEW.policy_id, NEW.eligibility, NEW.confidence, NEW.factors,
            NEW.rationale, NEW.scored_at, NEW.scored_against_definition_hash,
            OLD.eligibility, OLD.confidence, 'rescore'
        );
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS dataset_candidate_scores_history_ai ON dataset_candidate_scores;
CREATE TRIGGER dataset_candidate_scores_history_ai
    AFTER INSERT OR UPDATE ON dataset_candidate_scores
    FOR EACH ROW
    EXECUTE FUNCTION dataset_candidate_scores_append_history();

-- Seed: existing scores become 'initial' history rows so the journal covers
-- pre-migration data. Guarded by NOT EXISTS so the migration is re-runnable.
INSERT INTO dataset_candidate_score_history (
    candidate_id, policy_id, eligibility, confidence, factors,
    rationale, scored_at, scored_against_definition_hash,
    previous_eligibility, previous_confidence, change_reason
)
SELECT
    s.candidate_id, s.policy_id, s.eligibility, s.confidence, s.factors,
    s.rationale, s.scored_at, s.scored_against_definition_hash,
    NULL, NULL, 'initial'
FROM dataset_candidate_scores s
WHERE NOT EXISTS (
    SELECT 1 FROM dataset_candidate_score_history h
     WHERE h.candidate_id = s.candidate_id AND h.policy_id = s.policy_id
);
