-- Migration 014: Add FTS columns and missing bug_tracker columns

-- Add columns the SQLite bug_tracker uses but Postgres schema lacks
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS category text NOT NULL DEFAULT 'other';
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS description text NOT NULL DEFAULT '';
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS filed_by text NOT NULL DEFAULT 'system';
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS assigned_to text;
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS tags text NOT NULL DEFAULT '';

-- Add tsvector for full-text search on bugs
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description, '') || ' ' || coalesce(summary, ''))
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_bugs_fts ON bugs USING GIN (search_vector);

-- Add tsvector for full-text search on roadmap_items
ALTER TABLE roadmap_items ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title, '') || ' ' || coalesce(summary, ''))
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_roadmap_items_fts ON roadmap_items USING GIN (search_vector);
