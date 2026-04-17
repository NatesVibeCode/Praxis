BEGIN;

ALTER TABLE memory_edges
    ADD COLUMN IF NOT EXISTS authority_class text,
    ADD COLUMN IF NOT EXISTS provenance_kind text,
    ADD COLUMN IF NOT EXISTS provenance_ref text;

UPDATE memory_edges
SET provenance_kind = CASE
    WHEN COALESCE(NULLIF(provenance_kind, ''), '') <> '' THEN provenance_kind
    WHEN COALESCE(metadata->>'edge_kind', '') = 'foreign_key' THEN 'schema_projection'
    WHEN COALESCE(metadata->>'edge_kind', '') IN ('trigger', 'table_to_catalog') THEN 'schema_projection'
    WHEN COALESCE(metadata->>'edge_kind', '') = 'receipt_mutation' THEN 'receipt_projection'
    WHEN COALESCE(metadata->>'edge_kind', '') IN ('verification_receipt', 'verification_coverage') THEN 'verification_projection'
    WHEN COALESCE(metadata->>'edge_kind', '') IN ('failure_receipt', 'failure_impact') THEN 'failure_projection'
    WHEN source_id LIKE 'constraint:%' AND target_id LIKE 'receipt:%' THEN 'constraint_projection'
    WHEN source_id LIKE 'friction:%' AND target_id LIKE 'receipt:%' THEN 'friction_projection'
    WHEN source_id LIKE 'table:%' OR target_id LIKE 'table:%' THEN 'schema_projection'
    WHEN COALESCE(metadata->>'source_file', '') <> '' THEN 'import_graph_projection'
    WHEN source_id LIKE 'mod:%' AND target_id LIKE 'mod:%' AND relation_type = 'depends_on' THEN 'import_graph_projection'
    WHEN COALESCE(metadata->>'extraction', '') <> '' THEN 'relationship_mining'
    WHEN relation_type = 'regressed_from' THEN 'rollup_inference'
    WHEN relation_type IN ('correlates_with', 'covers', 'triggered') THEN 'relationship_mining'
    WHEN relation_type = 'related_to' THEN 'heuristic_extraction'
    ELSE 'legacy_unspecified'
END
WHERE provenance_kind IS NULL
   OR provenance_kind = '';

UPDATE memory_edges
SET authority_class = CASE
    WHEN authority_class IN ('canonical', 'enrichment') THEN authority_class
    WHEN provenance_kind IN (
        'receipt_projection',
        'verification_projection',
        'failure_projection',
        'constraint_projection',
        'friction_projection',
        'schema_projection',
        'import_graph_projection'
    ) THEN 'canonical'
    ELSE 'enrichment'
END
WHERE authority_class IS NULL
   OR authority_class = '';

UPDATE memory_edges
SET provenance_ref = CASE
    WHEN provenance_ref IS NOT NULL AND btrim(provenance_ref) <> '' THEN provenance_ref
    WHEN COALESCE(metadata->>'receipt_id', '') <> '' THEN metadata->>'receipt_id'
    WHEN COALESCE(metadata->>'policy_key', '') <> '' THEN metadata->>'policy_key'
    WHEN COALESCE(metadata->>'path', '') <> '' THEN metadata->>'path'
    WHEN COALESCE(metadata->>'trigger_name', '') <> '' THEN metadata->>'trigger_name'
    WHEN COALESCE(metadata->>'source_file', '') <> '' THEN metadata->>'source_file'
    WHEN COALESCE(metadata->>'extraction', '') <> '' THEN metadata->>'extraction'
    ELSE NULL
END;

UPDATE memory_edges
SET edge_origin = provenance_kind
WHERE edge_origin IS DISTINCT FROM provenance_kind;

ALTER TABLE memory_edges
    ALTER COLUMN authority_class SET DEFAULT 'enrichment',
    ALTER COLUMN provenance_kind SET DEFAULT 'legacy_unspecified';

UPDATE memory_edges
SET authority_class = COALESCE(NULLIF(authority_class, ''), 'enrichment'),
    provenance_kind = COALESCE(NULLIF(provenance_kind, ''), 'legacy_unspecified');

ALTER TABLE memory_edges
    ALTER COLUMN authority_class SET NOT NULL,
    ALTER COLUMN provenance_kind SET NOT NULL;

ALTER TABLE memory_edges
    DROP CONSTRAINT IF EXISTS ck_memory_edges_authority_class;

ALTER TABLE memory_edges
    ADD CONSTRAINT ck_memory_edges_authority_class
    CHECK (authority_class IN ('canonical', 'enrichment'));

ALTER TABLE memory_edges
    DROP CONSTRAINT IF EXISTS ck_memory_edges_provenance_kind;

ALTER TABLE memory_edges
    ADD CONSTRAINT ck_memory_edges_provenance_kind
    CHECK (
        provenance_kind IN (
            'legacy_unspecified',
            'structured_ingest',
            'conversation_extraction',
            'receipt_projection',
            'verification_projection',
            'failure_projection',
            'constraint_projection',
            'friction_projection',
            'schema_projection',
            'import_graph_projection',
            'heuristic_extraction',
            'relationship_mining',
            'rollup_inference'
        )
    );

CREATE INDEX IF NOT EXISTS idx_memory_edges_active_authority_source
    ON memory_edges (active, authority_class, source_id, relation_type);

CREATE INDEX IF NOT EXISTS idx_memory_edges_active_authority_target
    ON memory_edges (active, authority_class, target_id, relation_type);

CREATE OR REPLACE VIEW memory_relationship_authority AS
SELECT
    e.source_id,
    e.target_id,
    e.relation_type,
    e.authority_class AS relationship_class,
    e.weight::double precision AS score,
    jsonb_strip_nulls(
        COALESCE(e.metadata, '{}'::jsonb)
        || jsonb_build_object(
            'authority_class', e.authority_class,
            'provenance_kind', e.provenance_kind,
            'provenance_ref', e.provenance_ref
        )
    ) AS provenance,
    e.active,
    e.last_validated_at AS refreshed_at,
    NULL::timestamptz AS expires_at
FROM memory_edges e
UNION ALL
SELECT
    ie.source_id,
    ie.target_id,
    ie.relation_type,
    'enrichment'::text AS relationship_class,
    ie.confidence::double precision AS score,
    jsonb_strip_nulls(
        COALESCE(ie.metadata, '{}'::jsonb)
        || jsonb_build_object(
            'authority_class', 'enrichment',
            'provenance_kind', ie.inference_kind,
            'provenance_ref', NULL
        )
    ) AS provenance,
    ie.active,
    ie.refreshed_at,
    ie.expires_at
FROM memory_inferred_edges ie;

COMMIT;
