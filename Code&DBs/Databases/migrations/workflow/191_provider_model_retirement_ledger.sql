-- Migration 191: Provider model retirement ledger.
--
-- Curated retirement schedule for providers whose model catalogs can't be
-- probed directly from this harness. Anthropic is the primary motivating case:
-- per decision.2026-04-20.anthropic-cli-only-restored, this harness cannot
-- call api.anthropic.com to discover models (the CLI profile is the only
-- allowed surface, and `claude models` does not list deprecation dates).
--
-- The automatic retirement detector consults this ledger for any provider
-- whose live discovery is unavailable. For providers that DO expose a live
-- /models endpoint (openai, google, deepseek, cursor, openrouter), discovery
-- is authoritative and this ledger stays advisory (we still record retirement
-- rows for audit, but they duplicate what discovery already detected).

BEGIN;

CREATE TABLE IF NOT EXISTS provider_model_retirement_ledger (
    provider_slug text NOT NULL,
    model_slug text NOT NULL,
    retirement_effective_date date NOT NULL,
    retirement_kind text NOT NULL DEFAULT 'retired'
        CHECK (retirement_kind IN ('retired', 'deprecating', 'sunset_warning')),
    source text NOT NULL,
    source_url text,
    notes text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (provider_slug, model_slug, retirement_effective_date)
);

CREATE INDEX IF NOT EXISTS provider_model_retirement_ledger_provider_slug_idx
    ON provider_model_retirement_ledger (provider_slug, retirement_effective_date DESC);

CREATE INDEX IF NOT EXISTS provider_model_retirement_ledger_effective_date_idx
    ON provider_model_retirement_ledger (retirement_effective_date DESC)
    WHERE retirement_kind IN ('retired', 'deprecating');

-- Seed: Anthropic's known retirement schedule as of 2026-04-20.
-- Source: Anthropic's published model lifecycle page.
--   https://docs.anthropic.com/en/docs/about-claude/model-deprecations

INSERT INTO provider_model_retirement_ledger
    (provider_slug, model_slug, retirement_effective_date, retirement_kind, source, source_url, notes)
VALUES
    ('anthropic', 'claude-3-haiku-20240307', '2026-04-19', 'retired',
     'anthropic_model_deprecations_page_2026_04_20',
     'https://docs.anthropic.com/en/docs/about-claude/model-deprecations',
     'Haiku 3 retired 2026-04-19. Reach current-generation haiku via claude-haiku-4-5 (Haiku 4.5).'),
    ('anthropic', 'claude-3-5-haiku-20241022', '2026-04-19', 'retired',
     'anthropic_model_deprecations_page_2026_04_20',
     'https://docs.anthropic.com/en/docs/about-claude/model-deprecations',
     'Haiku 3.5 retired in same window as Haiku 3.'),
    ('anthropic', 'claude-sonnet-4-20250514', '2026-06-15', 'deprecating',
     'anthropic_model_deprecations_page_2026_04_20',
     'https://docs.anthropic.com/en/docs/about-claude/model-deprecations',
     'Sonnet 4 retires 2026-06-15. Migrate to Sonnet 4.6 (claude-sonnet-4-6).'),
    ('anthropic', 'claude-opus-4-20250514', '2026-06-15', 'deprecating',
     'anthropic_model_deprecations_page_2026_04_20',
     'https://docs.anthropic.com/en/docs/about-claude/model-deprecations',
     'Opus 4 retires 2026-06-15. Migrate to Opus 4.6 (claude-opus-4-6) or Opus 4.7 (claude-opus-4-5).')
ON CONFLICT (provider_slug, model_slug, retirement_effective_date) DO NOTHING;

COMMIT;
