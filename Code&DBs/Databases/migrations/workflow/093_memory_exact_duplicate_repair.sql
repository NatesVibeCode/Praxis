BEGIN;

CREATE INDEX IF NOT EXISTS idx_memory_entities_active_exact_duplicate_lookup
    ON memory_entities (entity_type, source_hash, created_at, id)
    WHERE archived = false
      AND COALESCE(source_hash, '') <> '';

INSERT INTO maintenance_policies (
    policy_key,
    subject_kind,
    intent_kind,
    enabled,
    priority,
    cadence_seconds,
    max_attempts,
    config,
    created_at,
    updated_at
)
VALUES (
    'memory_entity.archive_exact_duplicates',
    'memory_entity',
    'archive_exact_duplicate_entities',
    true,
    40,
    900,
    5,
    '{"group_limit":25}'::jsonb,
    now(),
    now()
)
ON CONFLICT (policy_key) DO UPDATE
SET subject_kind = EXCLUDED.subject_kind,
    intent_kind = EXCLUDED.intent_kind,
    enabled = EXCLUDED.enabled,
    priority = EXCLUDED.priority,
    cadence_seconds = EXCLUDED.cadence_seconds,
    max_attempts = EXCLUDED.max_attempts,
    config = EXCLUDED.config,
    updated_at = now();

CREATE OR REPLACE FUNCTION queue_memory_entity_exact_duplicate_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.archived OR COALESCE(NEW.source_hash, '') = '' THEN
        RETURN NEW;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM memory_entities
        WHERE archived = false
          AND entity_type = NEW.entity_type
          AND source_hash = NEW.source_hash
          AND id <> NEW.id
    ) THEN
        PERFORM enqueue_maintenance_intent(
            'archive_exact_duplicate_entities',
            'memory_entity_duplicate_group',
            'exact_duplicate:' || NEW.entity_type || ':' || NEW.source_hash,
            'archive_exact_duplicate_entities:' || NEW.entity_type || ':' || NEW.source_hash,
            95,
            jsonb_build_object(
                'entity_type', NEW.entity_type,
                'source_hash', NEW.source_hash,
                'group_limit', 1
            ),
            now(),
            5,
            'memory_entity.archive_exact_duplicates'
        );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_queue_memory_entity_exact_duplicate_intent ON memory_entities;
CREATE TRIGGER trg_queue_memory_entity_exact_duplicate_intent
    AFTER INSERT OR UPDATE ON memory_entities
    FOR EACH ROW
    EXECUTE FUNCTION queue_memory_entity_exact_duplicate_intent();

CREATE OR REPLACE FUNCTION absorb_exact_duplicate_memory_entities(
    p_canonical_id text,
    p_duplicate_ids text[]
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    v_duplicate_ids text[];
    v_group_ids text[];
    v_archived_ids text[] := ARRAY[]::text[];
    v_archived_count integer := 0;
    v_rehomed_edge_rows integer := 0;
    v_deleted_edge_rows integer := 0;
    v_deleted_inferred_rows integer := 0;
    v_deleted_neighbor_rows integer := 0;
    v_deleted_pending_intents integer := 0;
BEGIN
    SELECT ARRAY(
        SELECT DISTINCT candidate_id
        FROM unnest(COALESCE(p_duplicate_ids, ARRAY[]::text[])) AS candidate_id
        WHERE candidate_id IS NOT NULL
          AND btrim(candidate_id) <> ''
          AND candidate_id <> p_canonical_id
    )
    INTO v_duplicate_ids;

    IF COALESCE(array_length(v_duplicate_ids, 1), 0) = 0 THEN
        RETURN jsonb_build_object(
            'canonical_entity_id', p_canonical_id,
            'archived_ids', ARRAY[]::text[],
            'archived_count', 0,
            'rehomed_edge_rows', 0,
            'deleted_edge_rows', 0,
            'deleted_inferred_rows', 0,
            'deleted_neighbor_rows', 0,
            'deleted_pending_intents', 0
        );
    END IF;

    v_group_ids := array_append(v_duplicate_ids, p_canonical_id);

    WITH remapped AS (
        SELECT
            CASE
                WHEN edge.source_id = ANY(v_group_ids) THEN p_canonical_id
                ELSE edge.source_id
            END AS source_id,
            CASE
                WHEN edge.target_id = ANY(v_group_ids) THEN p_canonical_id
                ELSE edge.target_id
            END AS target_id,
            edge.relation_type,
            MAX(edge.weight) AS weight,
            (
                array_agg(COALESCE(edge.metadata, '{}'::jsonb) ORDER BY edge.created_at ASC, edge.source_id ASC, edge.target_id ASC)
            )[1] AS metadata,
            MIN(COALESCE(edge.created_at, now())) AS created_at
        FROM memory_edges AS edge
        WHERE edge.source_id = ANY(v_group_ids)
           OR edge.target_id = ANY(v_group_ids)
        GROUP BY 1, 2, 3
    )
    INSERT INTO memory_edges (
        source_id,
        target_id,
        relation_type,
        weight,
        metadata,
        created_at
    )
    SELECT source_id, target_id, relation_type, weight, metadata, created_at
    FROM remapped
    WHERE source_id <> target_id
    ON CONFLICT (source_id, target_id, relation_type) DO UPDATE
    SET weight = GREATEST(memory_edges.weight, EXCLUDED.weight),
        metadata = CASE
            WHEN memory_edges.metadata = '{}'::jsonb THEN EXCLUDED.metadata
            ELSE memory_edges.metadata
        END,
        last_validated_at = now(),
        active = true;
    GET DIAGNOSTICS v_rehomed_edge_rows = ROW_COUNT;

    DELETE FROM memory_edges
    WHERE source_id = ANY(v_duplicate_ids)
       OR target_id = ANY(v_duplicate_ids);
    GET DIAGNOSTICS v_deleted_edge_rows = ROW_COUNT;

    DELETE FROM memory_inferred_edges
    WHERE source_id = ANY(v_duplicate_ids)
       OR target_id = ANY(v_duplicate_ids);
    GET DIAGNOSTICS v_deleted_inferred_rows = ROW_COUNT;

    DELETE FROM memory_vector_neighbors
    WHERE source_entity_id = ANY(v_duplicate_ids)
       OR target_entity_id = ANY(v_duplicate_ids);
    GET DIAGNOSTICS v_deleted_neighbor_rows = ROW_COUNT;

    DELETE FROM maintenance_intents
    WHERE subject_kind = 'memory_entity'
      AND subject_id = ANY(v_duplicate_ids)
      AND status = 'pending';
    GET DIAGNOSTICS v_deleted_pending_intents = ROW_COUNT;

    WITH archived AS (
        UPDATE memory_entities
        SET archived = true,
            needs_reembed = false,
            embedding_status = 'archived',
            last_maintained_at = now()
        WHERE id = ANY(v_duplicate_ids)
          AND archived = false
        RETURNING id
    )
    SELECT COALESCE(array_agg(id ORDER BY id), ARRAY[]::text[]), COUNT(*)
    INTO v_archived_ids, v_archived_count
    FROM archived;

    UPDATE memory_entities
    SET last_maintained_at = now()
    WHERE id = p_canonical_id;

    RETURN jsonb_build_object(
        'canonical_entity_id', p_canonical_id,
        'archived_ids', v_archived_ids,
        'archived_count', v_archived_count,
        'rehomed_edge_rows', v_rehomed_edge_rows,
        'deleted_edge_rows', v_deleted_edge_rows,
        'deleted_inferred_rows', v_deleted_inferred_rows,
        'deleted_neighbor_rows', v_deleted_neighbor_rows,
        'deleted_pending_intents', v_deleted_pending_intents
    );
END;
$$;

COMMIT;
