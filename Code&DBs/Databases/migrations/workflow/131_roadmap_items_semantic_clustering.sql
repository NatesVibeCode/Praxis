-- Roadmap semantic topical clustering lane.
-- Mirrors bugs table vector support so roadmap items can be grouped by embedding proximity.

ALTER TABLE roadmap_items ADD COLUMN IF NOT EXISTS embedding vector(384);

CREATE INDEX IF NOT EXISTS roadmap_items_hnsw_idx
    ON roadmap_items
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
