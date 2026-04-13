-- Migration 019: Expand pgvector semantic search
--
-- Adds vector(384) embedding columns and HNSW indexes to existing tables,
-- extending semantic search from module similarity (018) to the knowledge
-- graph, dispatch receipts, bugs, constraints, friction events, and the
-- intent-matching registries. All columns are NULLABLE — existing rows
-- will be backfilled asynchronously via separate tooling.

-- 1. memory_entities — knowledge graph nodes
ALTER TABLE memory_entities ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS memory_entities_hnsw_idx
    ON memory_entities
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 2. receipt_search — dispatch receipts full-text + vector
ALTER TABLE receipt_search ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS receipt_search_hnsw_idx
    ON receipt_search
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 3. bugs — bug tracker entries
ALTER TABLE bugs ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS bugs_hnsw_idx
    ON bugs
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 4. dispatch_constraints — mined failure constraints
ALTER TABLE dispatch_constraints ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS dispatch_constraints_hnsw_idx
    ON dispatch_constraints
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 5. friction_events — guardrail bounces and warnings
ALTER TABLE friction_events ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS friction_events_hnsw_idx
    ON friction_events
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 6. registry_ui_components — intent matching for UI primitives
ALTER TABLE registry_ui_components ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS registry_ui_components_hnsw_idx
    ON registry_ui_components
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 7. registry_calculations — intent matching for calculation primitives
ALTER TABLE registry_calculations ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS registry_calculations_hnsw_idx
    ON registry_calculations
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 8. registry_workflows — intent matching for workflow primitives
ALTER TABLE registry_workflows ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS registry_workflows_hnsw_idx
    ON registry_workflows
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
