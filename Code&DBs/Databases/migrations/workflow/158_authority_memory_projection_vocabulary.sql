-- Extend memory_entities.entity_type and memory_edges.relation_type to admit
-- the vocabulary needed by the authority-to-memory projection. Without this,
-- FK-derived edges from roadmap_items, operator_object_relations,
-- workflow_build_intents, etc. cannot land in the knowledge graph because the
-- existing check constraints exclude both the endpoint kinds and the
-- relation kinds.
--
-- See runtime/authority_memory_projection.py for the projection that
-- populates edges under these new kinds with authority_class='canonical' and
-- provenance_kind='schema_projection'.

BEGIN;

ALTER TABLE memory_entities
    DROP CONSTRAINT IF EXISTS ck_memory_entities_type;

ALTER TABLE memory_entities
    ADD CONSTRAINT ck_memory_entities_type CHECK (entity_type = ANY (ARRAY[
        'person',
        'topic',
        'decision',
        'preference',
        'constraint',
        'fact',
        'task',
        'document',
        'workstream',
        'action',
        'lesson',
        'pattern',
        'module',
        'table',
        'code_unit',
        'tool',
        'metric',
        -- Authority-projected kinds (migration 158):
        'roadmap_item',
        'bug',
        'workflow',
        'workflow_build_intent',
        'functional_area',
        'repo_path',
        'operator_decision',
        'issue',
        'workflow_chain',
        'workflow_chain_wave',
        'workflow_chain_wave_run',
        'workflow_job_submission',
        'workflow_run',
        'verification_run',
        'healing_run',
        'receipt',
        'provider',
        'authority_domain',
        'cutover_gate',
        'workflow_class',
        'schedule_definition'
    ]));

ALTER TABLE memory_edges
    DROP CONSTRAINT IF EXISTS ck_memory_edges_relation_type;

ALTER TABLE memory_edges
    ADD CONSTRAINT ck_memory_edges_relation_type CHECK (relation_type = ANY (ARRAY[
        'depends_on',
        'constrains',
        'implements',
        'supersedes',
        'related_to',
        'derived_from',
        'caused_by',
        'correlates_with',
        'produced',
        'triggered',
        'covers',
        'verified_by',
        'recorded_in',
        'regressed_from',
        'semantic_neighbor',
        -- Authority-projected kinds (migration 158):
        'parent_of',
        'resolves_bug',
        'implements_build',
        'belongs_to_area'
    ]));

COMMIT;
