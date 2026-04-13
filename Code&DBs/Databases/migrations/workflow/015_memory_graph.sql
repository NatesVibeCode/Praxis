-- Migration 015: Memory graph (formerly 14 SQLite entity tables + FTS5)
-- Consolidates all entity types into a single memory_entities table
-- with Postgres tsvector for full-text search.

-- Unified entity table
CREATE TABLE IF NOT EXISTS memory_entities (
    id            text PRIMARY KEY,
    entity_type   text NOT NULL,
    name          text,
    content       text,
    metadata      jsonb DEFAULT '{}',
    source        text,
    confidence    real,
    archived      boolean DEFAULT false,
    created_at    timestamptz,
    updated_at    timestamptz,
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(content, ''))
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_memory_entities_type
    ON memory_entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_memory_entities_type_archived
    ON memory_entities (entity_type, archived);
CREATE INDEX IF NOT EXISTS idx_memory_entities_source
    ON memory_entities (source);
CREATE INDEX IF NOT EXISTS idx_memory_entities_fts
    ON memory_entities USING GIN (search_vector);

-- Edges table (entity relationships)
CREATE TABLE IF NOT EXISTS memory_edges (
    source_id     text NOT NULL,
    target_id     text NOT NULL,
    relation_type text NOT NULL,
    weight        real DEFAULT 1.0,
    metadata      jsonb DEFAULT '{}',
    created_at    timestamptz DEFAULT now(),
    UNIQUE (source_id, target_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_memory_edges_source
    ON memory_edges (source_id);
CREATE INDEX IF NOT EXISTS idx_memory_edges_target
    ON memory_edges (target_id);

-- Evidence table (provenance records)
CREATE TABLE IF NOT EXISTS memory_evidence (
    entity_id     text NOT NULL,
    evidence_type text NOT NULL,
    evidence_data jsonb DEFAULT '{}',
    recorded_at   timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_memory_evidence_entity
    ON memory_evidence (entity_id);
