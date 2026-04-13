BEGIN;

ALTER TABLE memory_entities
    DROP CONSTRAINT IF EXISTS ck_memory_entities_type;

ALTER TABLE memory_entities
    ADD CONSTRAINT ck_memory_entities_type
    CHECK (
        entity_type IN (
            'person', 'topic', 'decision', 'preference', 'constraint',
            'fact', 'task', 'document', 'workstream', 'action',
            'lesson', 'pattern', 'module', 'table', 'code_unit',
            'tool', 'metric'
        )
    );

ALTER TABLE memory_edges
    DROP CONSTRAINT IF EXISTS ck_memory_edges_relation_type;

ALTER TABLE memory_edges
    ADD CONSTRAINT ck_memory_edges_relation_type
    CHECK (
        relation_type IN (
            'depends_on', 'constrains', 'implements', 'supersedes',
            'related_to', 'derived_from', 'caused_by', 'correlates_with',
            'produced', 'triggered', 'covers', 'verified_by',
            'recorded_in', 'regressed_from', 'semantic_neighbor'
        )
    );

UPDATE memory_entities
   SET entity_type = 'table',
       metadata = COALESCE(metadata, '{}'::jsonb) || '{"kind":"table","entity_subtype":"schema_table"}'::jsonb,
       updated_at = NOW()
 WHERE entity_type IN ('module', 'document')
   AND COALESCE(metadata->>'kind', '') = 'table'
   AND NOT archived;

UPDATE memory_entities
   SET entity_type = 'code_unit',
       metadata = COALESCE(metadata, '{}'::jsonb) || '{"entity_subtype":"code_path"}'::jsonb,
       updated_at = NOW()
 WHERE entity_type = 'document'
   AND COALESCE(metadata->>'entity_subtype', '') = 'code_path'
   AND NOT archived;

INSERT INTO memory_edges (
    source_id,
    target_id,
    relation_type,
    weight,
    metadata,
    created_at,
    edge_origin,
    evidence_count,
    policy_version,
    last_validated_at,
    active
)
SELECT
    target_id,
    source_id,
    'recorded_in',
    weight,
    COALESCE(metadata, '{}'::jsonb) || '{"migrated_from":"produced","edge_kind":"verification_receipt"}'::jsonb,
    created_at,
    edge_origin,
    evidence_count,
    policy_version,
    last_validated_at,
    active
FROM memory_edges
WHERE relation_type = 'produced'
  AND source_id LIKE 'receipt:%'
  AND target_id LIKE 'verification:%'
ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
    weight = EXCLUDED.weight,
    metadata = EXCLUDED.metadata,
    active = EXCLUDED.active;

INSERT INTO memory_edges (
    source_id,
    target_id,
    relation_type,
    weight,
    metadata,
    created_at,
    edge_origin,
    evidence_count,
    policy_version,
    last_validated_at,
    active
)
SELECT
    target_id,
    source_id,
    'verified_by',
    weight,
    COALESCE(metadata, '{}'::jsonb) || '{"migrated_from":"covers","edge_kind":"verification_coverage"}'::jsonb,
    created_at,
    edge_origin,
    evidence_count,
    policy_version,
    last_validated_at,
    active
FROM memory_edges
WHERE relation_type = 'covers'
  AND source_id LIKE 'verification:%'
  AND target_id LIKE 'codepath:%'
ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
    weight = EXCLUDED.weight,
    metadata = EXCLUDED.metadata,
    active = EXCLUDED.active;

DELETE FROM memory_edges
 WHERE relation_type = 'produced'
   AND source_id LIKE 'receipt:%'
   AND target_id LIKE 'verification:%';

DELETE FROM memory_edges
 WHERE relation_type = 'covers'
   AND source_id LIKE 'verification:%'
   AND target_id LIKE 'codepath:%';

COMMIT;
