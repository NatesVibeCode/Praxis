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
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type)
);

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
