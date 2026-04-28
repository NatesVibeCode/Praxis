-- Migration 018: Module embeddings for functional synonym detection
--
-- Enables vector similarity search over codebase modules so agents can
-- discover functionally equivalent implementations before building new ones.
-- Requires pgvector to be enabled by the bootstrap/onboarding platform gate.
-- Migrations consume the extension; they do not own privileged extension setup.

-- Core table: one row per indexable code unit (module, class, function)
CREATE TABLE IF NOT EXISTS module_embeddings (
    module_id       TEXT PRIMARY KEY,
    module_path     TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('module', 'class', 'function', 'subsystem')),
    name            TEXT NOT NULL,
    docstring       TEXT DEFAULT '',
    signature       TEXT DEFAULT '',
    behavior        JSONB DEFAULT '{}'::jsonb,
    summary         TEXT NOT NULL,
    embedding       vector(384) NOT NULL,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash     TEXT NOT NULL DEFAULT ''
);

-- HNSW index for fast approximate nearest neighbor on cosine distance
CREATE INDEX IF NOT EXISTS module_embeddings_hnsw_idx
    ON module_embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- B-tree indexes for filtered queries
CREATE INDEX IF NOT EXISTS module_embeddings_kind_idx ON module_embeddings (kind);
CREATE INDEX IF NOT EXISTS module_embeddings_path_idx ON module_embeddings (module_path);

-- Full-text search on summary for hybrid retrieval
ALTER TABLE module_embeddings ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', summary)) STORED;
CREATE INDEX IF NOT EXISTS module_embeddings_fts_idx ON module_embeddings USING gin (search_vector);
