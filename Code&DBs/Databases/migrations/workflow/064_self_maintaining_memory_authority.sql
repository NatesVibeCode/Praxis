BEGIN;

ALTER TABLE memory_entities
    ADD COLUMN IF NOT EXISTS source_hash text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS needs_reembed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS embedding_status text NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS embedding_model text,
    ADD COLUMN IF NOT EXISTS embedding_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS embedded_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_maintained_at timestamptz NOT NULL DEFAULT now();

UPDATE memory_entities
SET confidence = LEAST(GREATEST(COALESCE(confidence, 0.5), 0), 1),
    created_at = COALESCE(created_at, now()),
    updated_at = GREATEST(COALESCE(updated_at, created_at, now()), COALESCE(created_at, now())),
    source_hash = md5(
        COALESCE(entity_type, '') || '|' ||
        COALESCE(name, '') || '|' ||
        COALESCE(content, '') || '|' ||
        COALESCE(metadata, '{}'::jsonb)::text
    ),
    needs_reembed = CASE
        WHEN archived THEN false
        WHEN embedding IS NULL THEN true
        ELSE COALESCE(needs_reembed, false)
    END,
    embedding_status = CASE
        WHEN archived THEN 'archived'
        WHEN embedding IS NULL THEN 'pending'
        ELSE 'ready'
    END,
    embedded_at = CASE
        WHEN embedding IS NULL THEN NULL
        ELSE COALESCE(embedded_at, now())
    END,
    last_maintained_at = COALESCE(last_maintained_at, now());

ALTER TABLE memory_entities
    ALTER COLUMN confidence SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_memory_entities_needs_reembed
    ON memory_entities (needs_reembed, archived, updated_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_entities_confidence'
          AND conrelid = 'memory_entities'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_entities
                 ADD CONSTRAINT ck_memory_entities_confidence
                 CHECK (confidence BETWEEN 0 AND 1)';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_entities_temporal'
          AND conrelid = 'memory_entities'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_entities
                 ADD CONSTRAINT ck_memory_entities_temporal
                 CHECK (updated_at >= created_at)';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_entities_embedding_status'
          AND conrelid = 'memory_entities'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_entities
                 ADD CONSTRAINT ck_memory_entities_embedding_status
                 CHECK (embedding_status IN (''pending'', ''ready'', ''archived'', ''failed''))';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_entities_type'
          AND conrelid = 'memory_entities'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_entities
                 ADD CONSTRAINT ck_memory_entities_type
                 CHECK (
                     entity_type IN (
                         ''person'', ''topic'', ''decision'', ''preference'', ''constraint'',
                         ''fact'', ''task'', ''document'', ''workstream'', ''action'',
                         ''lesson'', ''pattern'', ''module'', ''tool'', ''metric''
                     )
                 )';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION prepare_memory_entity_maintenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_source_hash text;
BEGIN
    NEW.metadata := COALESCE(NEW.metadata, '{}'::jsonb);
    NEW.created_at := COALESCE(NEW.created_at, now());
    NEW.updated_at := GREATEST(COALESCE(NEW.updated_at, NEW.created_at), NEW.created_at);
    NEW.confidence := LEAST(GREATEST(COALESCE(NEW.confidence, 0.5), 0), 1);
    NEW.embedding_version := COALESCE(NEW.embedding_version, 1);
    NEW.last_maintained_at := COALESCE(NEW.last_maintained_at, now());

    v_source_hash := md5(
        COALESCE(NEW.entity_type, '') || '|' ||
        COALESCE(NEW.name, '') || '|' ||
        COALESCE(NEW.content, '') || '|' ||
        COALESCE(NEW.metadata, '{}'::jsonb)::text
    );

    IF TG_OP = 'INSERT' THEN
        NEW.source_hash := v_source_hash;
        IF COALESCE(NEW.archived, false) THEN
            NEW.needs_reembed := false;
            NEW.embedding_status := 'archived';
        ELSIF NEW.embedding IS NULL THEN
            NEW.needs_reembed := true;
            NEW.embedding_status := 'pending';
            NEW.embedded_at := NULL;
        ELSE
            NEW.needs_reembed := false;
            NEW.embedding_status := 'ready';
            NEW.embedded_at := COALESCE(NEW.embedded_at, now());
        END IF;
        RETURN NEW;
    END IF;

    IF COALESCE(NEW.archived, false) THEN
        NEW.source_hash := COALESCE(OLD.source_hash, v_source_hash);
        NEW.needs_reembed := false;
        NEW.embedding_status := 'archived';
        RETURN NEW;
    END IF;

    IF v_source_hash IS DISTINCT FROM COALESCE(OLD.source_hash, '') THEN
        NEW.source_hash := v_source_hash;
        NEW.needs_reembed := true;
        NEW.embedding_status := 'pending';
        NEW.embedding := NULL;
        NEW.embedded_at := NULL;
        NEW.embedding_version := COALESCE(OLD.embedding_version, 1) + 1;
        RETURN NEW;
    END IF;

    NEW.source_hash := COALESCE(OLD.source_hash, v_source_hash);
    IF NEW.embedding IS DISTINCT FROM OLD.embedding AND NEW.embedding IS NOT NULL THEN
        NEW.needs_reembed := false;
        NEW.embedding_status := 'ready';
        NEW.embedded_at := COALESCE(NEW.embedded_at, now());
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prepare_memory_entity_maintenance ON memory_entities;
CREATE TRIGGER trg_prepare_memory_entity_maintenance
    BEFORE INSERT OR UPDATE ON memory_entities
    FOR EACH ROW
    EXECUTE FUNCTION prepare_memory_entity_maintenance();

DELETE FROM memory_edges e
WHERE e.source_id = e.target_id
   OR COALESCE(e.weight, 1.0) < 0
   OR COALESCE(e.weight, 1.0) > 1
   OR e.relation_type NOT IN (
       'depends_on', 'constrains', 'implements', 'supersedes', 'related_to',
       'derived_from', 'caused_by', 'correlates_with', 'produced',
       'triggered', 'covers', 'regressed_from', 'semantic_neighbor'
   )
   OR NOT EXISTS (
       SELECT 1
       FROM memory_entities src
       WHERE src.id = e.source_id
         AND src.archived = false
   )
   OR NOT EXISTS (
       SELECT 1
       FROM memory_entities tgt
       WHERE tgt.id = e.target_id
         AND tgt.archived = false
   );

ALTER TABLE memory_edges
    ADD COLUMN IF NOT EXISTS edge_origin text NOT NULL DEFAULT 'asserted',
    ADD COLUMN IF NOT EXISTS evidence_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS policy_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_validated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;

UPDATE memory_edges
SET weight = COALESCE(weight, 1.0),
    metadata = COALESCE(metadata, '{}'::jsonb),
    created_at = COALESCE(created_at, now()),
    last_validated_at = COALESCE(last_validated_at, now()),
    edge_origin = COALESCE(edge_origin, 'asserted'),
    evidence_count = COALESCE(evidence_count, 0),
    policy_version = COALESCE(policy_version, 1),
    active = COALESCE(active, true);

ALTER TABLE memory_edges
    ALTER COLUMN weight SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_memory_edges_active_source
    ON memory_edges (active, source_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_memory_edges_active_target
    ON memory_edges (active, target_id, relation_type);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_edges_weight'
          AND conrelid = 'memory_edges'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_edges
                 ADD CONSTRAINT ck_memory_edges_weight
                 CHECK (weight BETWEEN 0 AND 1)';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_edges_not_self'
          AND conrelid = 'memory_edges'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_edges
                 ADD CONSTRAINT ck_memory_edges_not_self
                 CHECK (source_id <> target_id)';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_memory_edges_relation_type'
          AND conrelid = 'memory_edges'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_edges
                 ADD CONSTRAINT ck_memory_edges_relation_type
                 CHECK (
                     relation_type IN (
                         ''depends_on'', ''constrains'', ''implements'', ''supersedes'',
                         ''related_to'', ''derived_from'', ''caused_by'', ''correlates_with'',
                         ''produced'', ''triggered'', ''covers'', ''regressed_from'',
                         ''semantic_neighbor''
                     )
                 )';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_memory_edges_source'
          AND conrelid = 'memory_edges'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_edges
                 ADD CONSTRAINT fk_memory_edges_source
                 FOREIGN KEY (source_id) REFERENCES memory_entities(id) ON DELETE CASCADE';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_memory_edges_target'
          AND conrelid = 'memory_edges'::regclass
    ) THEN
        EXECUTE 'ALTER TABLE memory_edges
                 ADD CONSTRAINT fk_memory_edges_target
                 FOREIGN KEY (target_id) REFERENCES memory_entities(id) ON DELETE CASCADE';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS memory_inferred_edges (
    source_id text NOT NULL,
    target_id text NOT NULL,
    relation_type text NOT NULL,
    inference_kind text NOT NULL,
    confidence real NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence_count integer NOT NULL DEFAULT 0,
    embedding_version integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (source_id, target_id, relation_type, inference_kind),
    CONSTRAINT ck_memory_inferred_edges_not_self CHECK (source_id <> target_id),
    CONSTRAINT ck_memory_inferred_edges_confidence CHECK (confidence BETWEEN 0 AND 1),
    CONSTRAINT ck_memory_inferred_edges_relation_type CHECK (
        relation_type IN (
            'depends_on', 'constrains', 'implements', 'supersedes',
            'related_to', 'derived_from', 'caused_by', 'correlates_with',
            'produced', 'triggered', 'covers', 'regressed_from',
            'semantic_neighbor'
        )
    ),
    CONSTRAINT fk_memory_inferred_edges_source
        FOREIGN KEY (source_id) REFERENCES memory_entities(id) ON DELETE CASCADE,
    CONSTRAINT fk_memory_inferred_edges_target
        FOREIGN KEY (target_id) REFERENCES memory_entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_memory_inferred_edges_source
    ON memory_inferred_edges (active, source_id, inference_kind);
CREATE INDEX IF NOT EXISTS idx_memory_inferred_edges_target
    ON memory_inferred_edges (active, target_id, inference_kind);

CREATE TABLE IF NOT EXISTS memory_vector_neighbors (
    source_entity_id text NOT NULL,
    target_entity_id text NOT NULL,
    policy_key text NOT NULL,
    similarity real NOT NULL,
    rank integer NOT NULL,
    embedding_version integer NOT NULL DEFAULT 1,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (source_entity_id, target_entity_id, policy_key),
    CONSTRAINT uq_memory_vector_neighbors_rank UNIQUE (source_entity_id, policy_key, rank),
    CONSTRAINT ck_memory_vector_neighbors_not_self CHECK (source_entity_id <> target_entity_id),
    CONSTRAINT ck_memory_vector_neighbors_similarity CHECK (similarity BETWEEN 0 AND 1),
    CONSTRAINT ck_memory_vector_neighbors_rank CHECK (rank > 0),
    CONSTRAINT fk_memory_vector_neighbors_source
        FOREIGN KEY (source_entity_id) REFERENCES memory_entities(id) ON DELETE CASCADE,
    CONSTRAINT fk_memory_vector_neighbors_target
        FOREIGN KEY (target_entity_id) REFERENCES memory_entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_memory_vector_neighbors_target
    ON memory_vector_neighbors (target_entity_id, policy_key, rank);

CREATE OR REPLACE VIEW memory_relationship_authority AS
SELECT
    e.source_id,
    e.target_id,
    e.relation_type,
    'asserted'::text AS relationship_class,
    e.weight::double precision AS score,
    e.metadata AS provenance,
    e.active,
    e.last_validated_at AS refreshed_at,
    NULL::timestamptz AS expires_at
FROM memory_edges e
UNION ALL
SELECT
    ie.source_id,
    ie.target_id,
    ie.relation_type,
    ie.inference_kind AS relationship_class,
    ie.confidence::double precision AS score,
    ie.metadata AS provenance,
    ie.active,
    ie.refreshed_at,
    ie.expires_at
FROM memory_inferred_edges ie;

CREATE TABLE IF NOT EXISTS maintenance_policies (
    policy_key text PRIMARY KEY,
    subject_kind text NOT NULL,
    intent_kind text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    priority integer NOT NULL DEFAULT 100,
    cadence_seconds integer,
    max_attempts integer NOT NULL DEFAULT 5,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_enqueued_at timestamptz,
    last_run_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_maintenance_policies_positive_priority CHECK (priority > 0),
    CONSTRAINT ck_maintenance_policies_positive_attempts CHECK (max_attempts > 0),
    CONSTRAINT ck_maintenance_policies_nonnegative_cadence CHECK (cadence_seconds IS NULL OR cadence_seconds > 0)
);

CREATE TABLE IF NOT EXISTS maintenance_intents (
    intent_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_kind text NOT NULL,
    subject_kind text NOT NULL,
    subject_id text,
    policy_key text,
    fingerprint text NOT NULL,
    priority integer NOT NULL DEFAULT 100,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending',
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    last_error text,
    outcome jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_maintenance_intents_fingerprint UNIQUE (fingerprint),
    CONSTRAINT ck_maintenance_intents_status CHECK (
        status IN ('pending', 'claimed', 'completed', 'failed', 'skipped')
    ),
    CONSTRAINT ck_maintenance_intents_positive_priority CHECK (priority > 0),
    CONSTRAINT ck_maintenance_intents_positive_attempts CHECK (max_attempts > 0),
    CONSTRAINT fk_maintenance_intents_policy
        FOREIGN KEY (policy_key) REFERENCES maintenance_policies(policy_key) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_maintenance_intents_claim
    ON maintenance_intents (status, available_at, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_maintenance_intents_subject
    ON maintenance_intents (subject_kind, subject_id, status);

CREATE OR REPLACE FUNCTION enqueue_maintenance_intent(
    p_intent_kind text,
    p_subject_kind text,
    p_subject_id text,
    p_fingerprint text,
    p_priority integer DEFAULT 100,
    p_payload jsonb DEFAULT '{}'::jsonb,
    p_available_at timestamptz DEFAULT now(),
    p_max_attempts integer DEFAULT 5,
    p_policy_key text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_intent_id bigint;
BEGIN
    INSERT INTO maintenance_intents (
        intent_kind,
        subject_kind,
        subject_id,
        policy_key,
        fingerprint,
        priority,
        payload,
        status,
        available_at,
        max_attempts,
        created_at,
        updated_at
    )
    VALUES (
        p_intent_kind,
        p_subject_kind,
        p_subject_id,
        p_policy_key,
        p_fingerprint,
        p_priority,
        COALESCE(p_payload, '{}'::jsonb),
        'pending',
        COALESCE(p_available_at, now()),
        COALESCE(p_max_attempts, 5),
        now(),
        now()
    )
    ON CONFLICT (fingerprint) DO UPDATE
    SET priority = GREATEST(maintenance_intents.priority, EXCLUDED.priority),
        payload = EXCLUDED.payload,
        available_at = LEAST(maintenance_intents.available_at, EXCLUDED.available_at),
        updated_at = now()
    RETURNING intent_id INTO v_intent_id;

    RETURN v_intent_id;
END;
$$;

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
VALUES
    (
        'memory_entity.embed',
        'memory_entity',
        'embed_entity',
        true,
        100,
        NULL,
        5,
        '{"embedding_model":"all-MiniLM-L6-v2"}'::jsonb,
        now(),
        now()
    ),
    (
        'memory_entity.vector_neighbors',
        'memory_entity',
        'refresh_vector_neighbors',
        true,
        80,
        NULL,
        5,
        '{"top_k":8,"min_similarity":0.8}'::jsonb,
        now(),
        now()
    ),
    (
        'memory_entity.archive_stale',
        'memory_entity',
        'archive_stale_entities',
        true,
        30,
        21600,
        3,
        '{"max_age_days":90}'::jsonb,
        now(),
        now()
    ),
    (
        'workflow_constraint.embed',
        'workflow_constraint',
        'embed_constraint',
        true,
        70,
        NULL,
        5,
        '{}'::jsonb,
        now(),
        now()
    ),
    (
        'friction_event.embed',
        'friction_event',
        'embed_friction_event',
        true,
        60,
        NULL,
        5,
        '{}'::jsonb,
        now(),
        now()
    )
ON CONFLICT (policy_key) DO UPDATE
SET enabled = EXCLUDED.enabled,
    priority = EXCLUDED.priority,
    cadence_seconds = EXCLUDED.cadence_seconds,
    max_attempts = EXCLUDED.max_attempts,
    config = EXCLUDED.config,
    updated_at = now();

CREATE OR REPLACE FUNCTION queue_memory_entity_embedding_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.archived OR NOT COALESCE(NEW.needs_reembed, false) THEN
        RETURN NEW;
    END IF;

    PERFORM enqueue_maintenance_intent(
        'embed_entity',
        'memory_entity',
        NEW.id,
        'embed_entity:' || NEW.id || ':' || COALESCE(NEW.source_hash, ''),
        100,
        jsonb_build_object(
            'source_hash', NEW.source_hash,
            'embedding_version', NEW.embedding_version,
            'entity_type', NEW.entity_type
        ),
        now(),
        5,
        'memory_entity.embed'
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_queue_memory_entity_embedding_intent ON memory_entities;
CREATE TRIGGER trg_queue_memory_entity_embedding_intent
    AFTER INSERT OR UPDATE ON memory_entities
    FOR EACH ROW
    EXECUTE FUNCTION queue_memory_entity_embedding_intent();

CREATE OR REPLACE FUNCTION queue_constraint_embedding_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.embedding IS NOT NULL THEN
        RETURN NEW;
    END IF;

    PERFORM enqueue_maintenance_intent(
        'embed_constraint',
        'workflow_constraint',
        NEW.constraint_id,
        'embed_constraint:' || NEW.constraint_id || ':' ||
            md5(COALESCE(NEW.pattern, '') || '|' || COALESCE(NEW.constraint_text, '')),
        70,
        jsonb_build_object('pattern', NEW.pattern),
        now(),
        5,
        'workflow_constraint.embed'
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_queue_constraint_embedding_intent ON workflow_constraints;
CREATE TRIGGER trg_queue_constraint_embedding_intent
    AFTER INSERT OR UPDATE ON workflow_constraints
    FOR EACH ROW
    EXECUTE FUNCTION queue_constraint_embedding_intent();

CREATE OR REPLACE FUNCTION queue_friction_embedding_intent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.embedding IS NOT NULL THEN
        RETURN NEW;
    END IF;

    PERFORM enqueue_maintenance_intent(
        'embed_friction_event',
        'friction_event',
        NEW.event_id,
        'embed_friction_event:' || NEW.event_id,
        60,
        jsonb_build_object('friction_type', NEW.friction_type, 'source', NEW.source),
        now(),
        5,
        'friction_event.embed'
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_queue_friction_embedding_intent ON friction_events;
CREATE TRIGGER trg_queue_friction_embedding_intent
    AFTER INSERT OR UPDATE ON friction_events
    FOR EACH ROW
    EXECUTE FUNCTION queue_friction_embedding_intent();

CREATE OR REPLACE FUNCTION cleanup_archived_memory_entity_dependencies()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.archived AND NOT COALESCE(OLD.archived, false) THEN
        DELETE FROM memory_edges
        WHERE source_id = NEW.id OR target_id = NEW.id;
        DELETE FROM memory_inferred_edges
        WHERE source_id = NEW.id OR target_id = NEW.id;
        DELETE FROM memory_vector_neighbors
        WHERE source_entity_id = NEW.id OR target_entity_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_cleanup_archived_memory_entity_dependencies ON memory_entities;
CREATE TRIGGER trg_cleanup_archived_memory_entity_dependencies
    AFTER UPDATE OF archived ON memory_entities
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_archived_memory_entity_dependencies();

SELECT enqueue_maintenance_intent(
    'embed_entity',
    'memory_entity',
    id,
    'embed_entity:' || id || ':' || COALESCE(source_hash, ''),
    100,
    jsonb_build_object(
        'source_hash', source_hash,
        'embedding_version', embedding_version,
        'entity_type', entity_type
    ),
    now(),
    5,
    'memory_entity.embed'
)
FROM memory_entities
WHERE archived = false
  AND (embedding IS NULL OR needs_reembed = true);

SELECT enqueue_maintenance_intent(
    'embed_constraint',
    'workflow_constraint',
    constraint_id,
    'embed_constraint:' || constraint_id || ':' ||
        md5(COALESCE(pattern, '') || '|' || COALESCE(constraint_text, '')),
    70,
    jsonb_build_object('pattern', pattern),
    now(),
    5,
    'workflow_constraint.embed'
)
FROM workflow_constraints
WHERE embedding IS NULL;

SELECT enqueue_maintenance_intent(
    'embed_friction_event',
    'friction_event',
    event_id,
    'embed_friction_event:' || event_id,
    60,
    jsonb_build_object('friction_type', friction_type, 'source', source),
    now(),
    5,
    'friction_event.embed'
)
FROM friction_events
WHERE embedding IS NULL;

COMMIT;
