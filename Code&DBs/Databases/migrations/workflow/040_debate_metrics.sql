BEGIN;

CREATE TABLE IF NOT EXISTS debate_round_metrics (
    id BIGSERIAL PRIMARY KEY,
    debate_run_id TEXT NOT NULL,
    debate_id TEXT NOT NULL,
    round_number INTEGER NOT NULL CHECK (round_number > 0),
    persona_position INTEGER NOT NULL DEFAULT 0 CHECK (persona_position >= 0),
    round_id TEXT NOT NULL,
    persona TEXT NOT NULL,
    word_count INTEGER NOT NULL CHECK (word_count >= 0),
    claim_count INTEGER NOT NULL CHECK (claim_count >= 0),
    evidence_citations INTEGER NOT NULL CHECK (evidence_citations >= 0),
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score >= 0.0 AND quality_score <= 1.0),
    duration_seconds DOUBLE PRECISION NOT NULL CHECK (duration_seconds >= 0.0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT debate_round_metrics_run_round_position_uniq
        UNIQUE (debate_run_id, round_number, persona_position)
);

CREATE INDEX IF NOT EXISTS debate_round_metrics_debate_idx
    ON debate_round_metrics (debate_id, created_at DESC);
CREATE INDEX IF NOT EXISTS debate_round_metrics_run_idx
    ON debate_round_metrics (debate_run_id, round_number, persona_position);

CREATE TABLE IF NOT EXISTS debate_consensus (
    id BIGSERIAL PRIMARY KEY,
    debate_run_id TEXT NOT NULL UNIQUE,
    debate_id TEXT NOT NULL,
    total_rounds INTEGER NOT NULL CHECK (total_rounds >= 0),
    consensus_points JSONB NOT NULL DEFAULT '[]'::jsonb,
    disagreements JSONB NOT NULL DEFAULT '[]'::jsonb,
    avg_quality DOUBLE PRECISION NOT NULL CHECK (avg_quality >= 0.0 AND avg_quality <= 1.0),
    synthesis_quality DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS debate_consensus_debate_idx
    ON debate_consensus (debate_id, updated_at DESC);

COMMIT;
