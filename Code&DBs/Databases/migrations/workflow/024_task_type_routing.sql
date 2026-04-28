-- Migration 024: Task-type routing with per-model permissions
--
-- Maps task types to allowed models with benchmark-backed rankings.
-- When a dispatch spec says agent: "auto/build", the router picks the
-- best model from this table based on task_type, benchmark score, and cost.
--
-- Columns per task type control which models are permitted for which work.
-- This prevents using expensive reasoning models for mechanical wiring,
-- or cheap models for architecture decisions.
--
-- Schema shape note (fresh-bootstrap fix, 2026-04-27):
--   The original 024 declared PK = (task_type, provider_slug, model_slug).
--   Migration 286 later widened the PK to (task_type, sub_task_type,
--   provider_slug, model_slug, transport_type) and added the two new
--   columns with defaults '*' / 'CLI'. Migrations 091/093/101/214/249-...
--   all use the post-286 ON CONFLICT shape — which means on a fresh
--   bootstrap (where 024 ran before 286), every one of those INSERT
--   statements would fail with "column does not exist".
--
--   Resolution: pre-apply 286's column adds + PK shape here. The original
--   PK initialization block below now no-ops (PK already set by CREATE
--   TABLE). Migration 286 becomes idempotent on fresh installs (its
--   ADD COLUMN IF NOT EXISTS / DROP-AND-READD-PK guards already handle
--   the no-op case).

CREATE TABLE IF NOT EXISTS task_type_routing (
    task_type       TEXT NOT NULL,
    sub_task_type   TEXT NOT NULL DEFAULT '*',
    transport_type  TEXT NOT NULL DEFAULT 'CLI',
    model_slug      TEXT NOT NULL,
    provider_slug   TEXT NOT NULL,
    -- Permission gate: is this model allowed for this task type?
    permitted       BOOLEAN NOT NULL DEFAULT true,
    -- Ranking within task type (1 = best, used for auto-selection)
    rank            INTEGER NOT NULL DEFAULT 99,
    -- Benchmark score that justifies the ranking (for auditability)
    benchmark_score FLOAT DEFAULT 0,
    benchmark_name  TEXT DEFAULT '',
    -- Cost per million tokens (input + output avg, for cost-aware routing)
    cost_per_m_tokens FLOAT DEFAULT 0,
    -- Max concurrent jobs of this type for this model (rate limit gate)
    max_concurrent  INTEGER DEFAULT 5,
    -- Notes for humans
    rationale       TEXT DEFAULT '',
    route_tier      TEXT NOT NULL DEFAULT 'medium'
        CHECK (route_tier IN ('high', 'medium', 'low')),
    route_tier_rank INTEGER NOT NULL DEFAULT 99
        CHECK (route_tier_rank >= 1),
    latency_class   TEXT NOT NULL DEFAULT 'reasoning'
        CHECK (latency_class IN ('reasoning', 'instant')),
    latency_rank    INTEGER NOT NULL DEFAULT 99
        CHECK (latency_rank >= 1),
    reasoning_control JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_health_score DOUBLE PRECISION NOT NULL DEFAULT 0.65
        CHECK (route_health_score >= 0.0 AND route_health_score <= 1.0),
    observed_completed_count INTEGER NOT NULL DEFAULT 0,
    observed_execution_failure_count INTEGER NOT NULL DEFAULT 0,
    observed_external_failure_count INTEGER NOT NULL DEFAULT 0,
    observed_config_failure_count INTEGER NOT NULL DEFAULT 0,
    observed_downstream_failure_count INTEGER NOT NULL DEFAULT 0,
    observed_downstream_bug_count INTEGER NOT NULL DEFAULT 0,
    consecutive_internal_failures INTEGER NOT NULL DEFAULT 0,
    last_failure_category TEXT,
    last_failure_zone TEXT,
    last_outcome_at TIMESTAMPTZ,
    last_reviewed_at TIMESTAMPTZ,
    recent_successes INTEGER NOT NULL DEFAULT 0,
    recent_failures INTEGER NOT NULL DEFAULT 0,
    last_failure_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    route_source    TEXT NOT NULL DEFAULT 'explicit',
    temperature     NUMERIC(4,3),
    max_tokens      INTEGER,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type)
);

-- Defensive: if the table already exists from an older 024 apply that
-- pre-dated migration 286's column-adds, the CREATE TABLE IF NOT EXISTS
-- above is a no-op and `sub_task_type` / `transport_type` are absent.
-- The ON CONFLICT clause at the bottom of this file references both
-- columns and fails ("column does not exist") on bootstrap. ADD COLUMN
-- IF NOT EXISTS guards make the migration idempotent across the full
-- history of installs. See BUG-2BBCC370.
ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS sub_task_type TEXT NOT NULL DEFAULT '*';
ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS transport_type TEXT NOT NULL DEFAULT 'CLI';
ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS route_tier TEXT NOT NULL DEFAULT 'medium'
        CHECK (route_tier IN ('high', 'medium', 'low')),
    ADD COLUMN IF NOT EXISTS route_tier_rank INTEGER NOT NULL DEFAULT 99
        CHECK (route_tier_rank >= 1),
    ADD COLUMN IF NOT EXISTS latency_class TEXT NOT NULL DEFAULT 'reasoning'
        CHECK (latency_class IN ('reasoning', 'instant')),
    ADD COLUMN IF NOT EXISTS latency_rank INTEGER NOT NULL DEFAULT 99
        CHECK (latency_rank >= 1),
    ADD COLUMN IF NOT EXISTS reasoning_control JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS route_health_score DOUBLE PRECISION NOT NULL DEFAULT 0.65
        CHECK (route_health_score >= 0.0 AND route_health_score <= 1.0),
    ADD COLUMN IF NOT EXISTS observed_completed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_execution_failure_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_external_failure_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_config_failure_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_downstream_failure_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_downstream_bug_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS consecutive_internal_failures INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_failure_category TEXT,
    ADD COLUMN IF NOT EXISTS last_failure_zone TEXT,
    ADD COLUMN IF NOT EXISTS last_outcome_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_reviewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recent_successes INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recent_failures INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS route_source TEXT NOT NULL DEFAULT 'explicit',
    ADD COLUMN IF NOT EXISTS temperature NUMERIC(4,3),
    ADD COLUMN IF NOT EXISTS max_tokens INTEGER;

-- Widen PK to the 5-column shape if it isn't already. Older 024 applies
-- created the table with a 4-column PK (no sub_task_type / transport_type);
-- migration 286 was supposed to widen later, but later ON CONFLICT clauses
-- in 091+ reference the 5-column shape and fail before 286 runs. Fix it
-- here in 024 so the bootstrap chain can continue. Idempotent: skipped
-- when the PK already matches the target shape.
DO $$
DECLARE
    v_pk_cols text;
    v_target_cols text := 'task_type,sub_task_type,provider_slug,model_slug,transport_type';
BEGIN
    SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
      INTO v_pk_cols
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
     WHERE n.nspname = 'public' AND t.relname = 'task_type_routing' AND c.contype = 'p';

    IF v_pk_cols IS DISTINCT FROM v_target_cols THEN
        IF v_pk_cols IS NOT NULL THEN
            EXECUTE 'ALTER TABLE task_type_routing DROP CONSTRAINT task_type_routing_pkey';
        END IF;
        EXECUTE 'ALTER TABLE task_type_routing ADD CONSTRAINT task_type_routing_pkey '
             || 'PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type)';
    END IF;
END $$;

DO $$
BEGIN
    -- Defensive: only initialize a PK if none exists. With the CREATE
    -- TABLE above setting the wide PK, this branch never fires on fresh
    -- installs. Kept for re-runs against historical DBs that might have
    -- the table without a PK (legacy edge case).
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'task_type_routing'
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'task_type_routing'
          AND c.contype = 'p'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_pkey
            PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS task_type_routing_type_rank_idx
    ON task_type_routing (task_type, rank)
    WHERE permitted = true;

-- Seed: benchmark-backed routing table (April 2026 data)
--
-- Task types:
--   build       = write code, implement features, fix bugs
--   architecture = design decisions, migration planning, API design
--   wiring      = mechanical plumbing, import fixes, config changes
--   test        = write and run tests
--   refactor    = restructure existing code
--   debate      = adversarial analysis, tradeoff evaluation
--   review      = code review, quality assessment
--
-- The INSERT omits sub_task_type and transport_type so they take the
-- column defaults ('*' / 'CLI') — matching what migration 286's
-- column-adds would have produced on a historical bootstrap.

INSERT INTO task_type_routing (task_type, model_slug, provider_slug, permitted, rank, benchmark_score, benchmark_name, cost_per_m_tokens, rationale) VALUES
-- BUILD: GPT-5.4 leads SWE-Bench Pro (57.7%), Terminal-Bench (75.1%), Aider (88%)
('build', 'gpt-5.4',                'openai',    true,  1, 57.7, 'SWE-Bench Pro',  8.75, 'Best coder across all benchmarks'),
('build', 'gemini-3.1-pro-preview', 'google',    true,  2, 54.2, 'SWE-Bench Pro',  7.00, 'Strong #2, best price/performance'),
('build', 'claude-sonnet-4-6',      'anthropic',  true,  3, 79.6, 'SWE-Bench Verified', 9.00, 'Solid but not top on coding benchmarks'),
('build', 'gpt-5.4-mini',           'openai',    true,  4, 0,    '',               2.63, 'Good for simpler build tasks'),
('build', 'claude-opus-4-7',        'anthropic',  false, 99, 80.8, 'SWE-Bench Verified', 15.0, 'BLOCKED: too expensive for build — use for architecture only'),

-- ARCHITECTURE: Opus and GPT-5.4 for deep reasoning
('architecture', 'claude-opus-4-7',        'anthropic',  true,  1, 80.8, 'SWE-Bench Verified', 15.0, 'Best reasoning depth and long-context coherence'),
('architecture', 'gpt-5.4',                'openai',    true,  2, 57.7, 'SWE-Bench Pro',  8.75, 'Strong reasoning, cheaper than Opus'),
('architecture', 'gemini-3.1-pro-preview', 'google',    true,  3, 80.6, 'SWE-Bench Verified', 7.00, 'Good architecture at lowest cost'),
('architecture', 'claude-sonnet-4-6',      'anthropic',  false, 99, 0,    '',               9.00, 'BLOCKED: use Opus or GPT-5.4 for architecture'),
('architecture', 'gpt-5.4-mini',           'openai',    false, 99, 0,    '',               2.63, 'BLOCKED: too lightweight for architecture'),

-- WIRING: mechanical tasks, GPT-5.4-mini and Gemini Flash
('wiring', 'gpt-5.4-mini',           'openai',    true,  1, 0, '', 2.63, 'Cheapest, fast, good for mechanical work'),
('wiring', 'gemini-3.1-pro-preview', 'google',    true,  2, 0, '', 7.00, 'Good fallback for wiring'),
('wiring', 'gpt-5.4',                'openai',    true,  3, 0, '', 8.75, 'Overkill but works'),
('wiring', 'claude-sonnet-4-6',      'anthropic',  true,  4, 0, '', 9.00, 'Acceptable for wiring'),
('wiring', 'claude-opus-4-7',        'anthropic',  false, 99, 0, '', 15.0, 'BLOCKED: never use Opus for wiring'),

-- TEST: writing and running tests
('test', 'gpt-5.4',                'openai',    true,  1, 75.1, 'Terminal-Bench', 8.75, 'Best at CLI/terminal execution'),
('test', 'gemini-3.1-pro-preview', 'google',    true,  2, 68.5, 'Terminal-Bench', 7.00, 'Good test writer'),
('test', 'claude-sonnet-4-6',      'anthropic',  true,  3, 0, '', 9.00, 'Solid test coverage'),
('test', 'gpt-5.4-mini',           'openai',    true,  4, 0, '', 2.63, 'Quick unit tests'),
('test', 'claude-opus-4-7',        'anthropic',  false, 99, 0, '', 15.0, 'BLOCKED: too expensive for testing'),

-- REFACTOR: restructuring existing code
('refactor', 'gpt-5.4',                'openai',    true,  1, 88.0, 'Aider Polyglot', 8.75, 'Best at multi-language code editing'),
('refactor', 'gemini-3.1-pro-preview', 'google',    true,  2, 83.1, 'Aider Polyglot', 7.00, 'Strong refactoring'),
('refactor', 'claude-sonnet-4-6',      'anthropic',  true,  3, 0, '', 9.00, 'Decent refactoring'),
('refactor', 'gpt-5.4-mini',           'openai',    true,  4, 0, '', 2.63, 'Simple renames/moves'),
('refactor', 'claude-opus-4-7',        'anthropic',  false, 99, 0, '', 15.0, 'BLOCKED: overkill for refactoring'),

-- DEBATE: adversarial analysis
('debate', 'claude-opus-4-7',        'anthropic',  true,  1, 0, '', 15.0, 'Best reasoning for adversarial debate'),
('debate', 'gpt-5.4',                'openai',    true,  2, 0, '', 8.75, 'Strong debater'),
('debate', 'gemini-3.1-pro-preview', 'google',    true,  3, 0, '', 7.00, 'Good analysis'),
('debate', 'claude-sonnet-4-6',      'anthropic',  true,  4, 0, '', 9.00, 'Adequate for debate'),

-- REVIEW: code review and quality assessment
('review', 'gpt-5.4',                'openai',    true,  1, 0, '', 8.75, 'Best code understanding'),
('review', 'claude-sonnet-4-6',      'anthropic',  true,  2, 0, '', 9.00, 'Good reviewer'),
('review', 'gemini-3.1-pro-preview', 'google',    true,  3, 0, '', 7.00, 'Adequate for review'),
('review', 'gpt-5.4-mini',           'openai',    true,  4, 0, '', 2.63, 'Quick reviews')

ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
    permitted = EXCLUDED.permitted,
    rank = EXCLUDED.rank,
    benchmark_score = EXCLUDED.benchmark_score,
    benchmark_name = EXCLUDED.benchmark_name,
    cost_per_m_tokens = EXCLUDED.cost_per_m_tokens,
    rationale = EXCLUDED.rationale,
    updated_at = NOW();
