-- Migration 245: compile_runs trace + compile_health observability.
--
-- The Moon "Describe it" / compile_prose path silently degrades when the LLM
-- doesn't fire, when no persona is registered, or when the artifact cache
-- replays a stale result. Failure mode observed today: composer emitted
-- prose-shaped nodes with 0 bindings / 0 pills / 0 release gates and no
-- caller could tell whether (a) the LLM fired and produced empty structure,
-- (b) the LLM was skipped because of a routing miss, (c) the result was a
-- cache replay, or (d) no persona was registered to teach the LLM the
-- expected response shape.
--
-- This migration adds:
--   1. compile_runs table — one row per compile_prose invocation with full
--      provenance: which provider/model/persona, whether LLM fired, why if
--      not, cache state, output structural counts, duration, error.
--   2. compile_health view — degraded-state surface for praxis workflow
--      health: counts of LLM-skipped runs, persona-missing runs, recent
--      empty-output runs. Surfaces silent degradation loudly.

BEGIN;

CREATE TABLE IF NOT EXISTS compile_runs (
    compile_run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id             TEXT,
    title                   TEXT,
    prose_sha256            TEXT NOT NULL CHECK (prose_sha256 ~ '^[0-9a-f]{64}$'),
    prose_preview           TEXT NOT NULL DEFAULT '',
    task_type_requested     TEXT NOT NULL DEFAULT 'build',

    -- LLM lane provenance
    llm_requested           BOOLEAN NOT NULL DEFAULT FALSE,
    llm_fired               BOOLEAN NOT NULL DEFAULT FALSE,
    llm_skip_reason         TEXT,
    provider_slug           TEXT,
    model_slug              TEXT,
    persona_profile_id      TEXT,
    persona_resolved        BOOLEAN NOT NULL DEFAULT FALSE,

    -- Cache state
    cache_hit               BOOLEAN NOT NULL DEFAULT FALSE,
    cache_reason            TEXT,

    -- Output structural counts — the empty-bindings / empty-gates problem
    -- becomes detectable here. Zero counts on a fresh LLM-fired run with a
    -- persona is a quality red flag.
    node_count              INTEGER NOT NULL DEFAULT 0,
    edge_count              INTEGER NOT NULL DEFAULT 0,
    pill_count              INTEGER NOT NULL DEFAULT 0,
    gate_count              INTEGER NOT NULL DEFAULT 0,
    binding_count           INTEGER NOT NULL DEFAULT 0,
    deterministic_fallback  BOOLEAN NOT NULL DEFAULT FALSE,

    -- Outcome
    duration_ms             INTEGER NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    status                  TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'partial')),
    error_code              TEXT,
    error_detail            TEXT,

    -- Timestamps
    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS compile_runs_started_at_idx
    ON compile_runs (started_at DESC);

-- Silent-degradation index: completed runs where the LLM was requested but
-- did not fire are exactly the case that bit us today. One query finds them.
CREATE INDEX IF NOT EXISTS compile_runs_silent_degradation_idx
    ON compile_runs (started_at DESC)
    WHERE llm_requested = TRUE AND llm_fired = FALSE;

-- Empty-output index: completed LLM runs that produced no structural output
-- (no nodes / no pills / no gates) — composer-quality red flag.
CREATE INDEX IF NOT EXISTS compile_runs_empty_output_idx
    ON compile_runs (started_at DESC)
    WHERE status = 'completed' AND llm_fired = TRUE
      AND (node_count = 0 OR (pill_count = 0 AND gate_count = 0 AND binding_count = 0));

CREATE INDEX IF NOT EXISTS compile_runs_workflow_idx
    ON compile_runs (workflow_id, started_at DESC)
    WHERE workflow_id IS NOT NULL;


-- compile_health view — single read for observability surfaces
CREATE OR REPLACE VIEW compile_health AS
WITH recent AS (
    SELECT *
    FROM compile_runs
    WHERE started_at > now() - INTERVAL '24 hours'
),
counts AS (
    SELECT
        COUNT(*)                                           AS recent_runs,
        COUNT(*) FILTER (WHERE llm_requested AND NOT llm_fired) AS llm_skipped_runs,
        COUNT(*) FILTER (WHERE llm_fired)                  AS llm_fired_runs,
        COUNT(*) FILTER (WHERE cache_hit)                  AS cache_replays,
        COUNT(*) FILTER (WHERE deterministic_fallback)     AS deterministic_fallback_runs,
        COUNT(*) FILTER (WHERE NOT persona_resolved AND llm_requested) AS persona_missing_runs,
        COUNT(*) FILTER (WHERE status = 'completed' AND llm_fired
                          AND (pill_count = 0 AND gate_count = 0 AND binding_count = 0))
                                                            AS empty_output_runs,
        COUNT(*) FILTER (WHERE status = 'failed')           AS failed_runs
    FROM recent
),
persona_count AS (
    SELECT COUNT(*) AS registered_personas FROM persona_profiles
)
SELECT
    counts.recent_runs,
    counts.llm_skipped_runs,
    counts.llm_fired_runs,
    counts.cache_replays,
    counts.deterministic_fallback_runs,
    counts.persona_missing_runs,
    counts.empty_output_runs,
    counts.failed_runs,
    persona_count.registered_personas,
    CASE
        WHEN persona_count.registered_personas = 0 THEN 'critical'
        WHEN counts.recent_runs = 0 THEN 'idle'
        WHEN counts.llm_skipped_runs::FLOAT / NULLIF(counts.recent_runs, 0) > 0.5 THEN 'degraded'
        WHEN counts.empty_output_runs::FLOAT / NULLIF(counts.llm_fired_runs, 0) > 0.5 THEN 'degraded'
        WHEN counts.failed_runs::FLOAT / NULLIF(counts.recent_runs, 0) > 0.25 THEN 'degraded'
        ELSE 'ok'
    END AS health_state,
    CASE
        WHEN persona_count.registered_personas = 0
            THEN 'No personas registered in persona_profiles. compile_prose has no contract to teach the LLM the response shape, so output will be empty / prose-shaped. Seed at least one persona for task_type=build before relying on Moon Describe-it.'
        WHEN counts.recent_runs = 0
            THEN 'No compile_runs in the last 24h. Cannot assess health.'
        WHEN counts.llm_skipped_runs::FLOAT / NULLIF(counts.recent_runs, 0) > 0.5
            THEN 'More than half of recent compile runs requested an LLM but did not fire. Check task_type_routing rows + provider_transport_admissions + key in keychain.'
        WHEN counts.empty_output_runs::FLOAT / NULLIF(counts.llm_fired_runs, 0) > 0.5
            THEN 'More than half of LLM-fired compile runs produced empty pills/gates/bindings. Persona response_contract is missing or wrong.'
        WHEN counts.failed_runs::FLOAT / NULLIF(counts.recent_runs, 0) > 0.25
            THEN 'More than a quarter of recent compile runs failed outright.'
        ELSE 'compile_runs lane healthy.'
    END AS health_reason
FROM counts CROSS JOIN persona_count;


COMMENT ON TABLE compile_runs IS
    'One row per compile_prose invocation. Records LLM provenance (whether the LLM fired and why), persona resolution, cache state, structural output counts, and duration. Lets praxis_compile_trace + compile_health surface silent degradation that previously hid behind ok=true.';

COMMENT ON VIEW compile_health IS
    'Single-read health surface for the compile path. Reports critical when no personas registered, degraded when too many LLM-skipped or empty-output runs in the last 24h. Visible via praxis workflow tools call praxis_health (compile_health probe) once wired.';

COMMIT;
