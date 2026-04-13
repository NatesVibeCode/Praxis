-- Enable pg_trgm for trigram similarity (used by heartbeat duplicate_scanner)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Index for fast similarity lookups on entity names
CREATE INDEX IF NOT EXISTS idx_memory_entities_name_trgm
    ON memory_entities USING gin (lower(name) gin_trgm_ops);
