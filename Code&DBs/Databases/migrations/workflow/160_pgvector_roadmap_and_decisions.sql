-- Migration 160: Add pgvector embedding columns to roadmap_items and operator_decisions
--
-- Closes the semantic-search gap identified during the post-restore audit:
-- every other long-form authority table (bugs, constraints, friction, memory,
-- registry_*) already carries a vector(384) column, but these two only had
-- tsvector FTS. Columns are NULLABLE so existing rows can be backfilled
-- asynchronously via the standard embedder path.

-- roadmap_items — title + summary form the embed text
ALTER TABLE roadmap_items ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS roadmap_items_hnsw_idx
    ON roadmap_items
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- operator_decisions — title + rationale form the embed text
ALTER TABLE operator_decisions ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS operator_decisions_hnsw_idx
    ON operator_decisions
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
