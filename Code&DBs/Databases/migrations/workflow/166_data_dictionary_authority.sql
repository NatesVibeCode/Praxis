-- Migration 165: Unified data dictionary authority.
--
-- One authority table (data_dictionary_entries) keyed by (object_kind, field_path, source)
-- so auto-projected descriptors and operator overrides coexist per field. The merged view
-- `data_dictionary_effective` picks the highest-precedence source per field, letting
-- read paths treat the dictionary as a single flat surface while writes stay layered.
--
-- Decision:  operator_decision.architecture_policy.data_dictionary.unified_auto_projected_registry
-- Scope:     authority_domain=data_dictionary

CREATE TABLE IF NOT EXISTS data_dictionary_objects (
    object_kind       text PRIMARY KEY,
    label             text NOT NULL DEFAULT '',
    category          text NOT NULL DEFAULT 'object',
    summary           text NOT NULL DEFAULT '',
    origin_ref        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT data_dictionary_objects_kind_nonblank
        CHECK (btrim(object_kind) <> ''),
    CONSTRAINT data_dictionary_objects_category_check
        CHECK (category IN (
            'table',             -- postgres table
            'object_type',       -- object_types registry entry
            'integration',       -- integration manifest
            'dataset',           -- dataset refinery family/specialist
            'ingest',            -- memory IngestPayload kind
            'decision',          -- operator_decisions kind
            'receipt',           -- receipt kind
            'tool',              -- MCP tool input schema
            'object'             -- catchall
        ))
);

COMMENT ON TABLE data_dictionary_objects IS
    'One row per known object kind in Praxis (tables, object_types, integrations, datasets, etc.). Paired with data_dictionary_entries rows for field-level descriptors.';
COMMENT ON COLUMN data_dictionary_objects.origin_ref IS
    'Where the projector discovered this kind, e.g. {"source":"schema_projector","table":"workflow_runs"} or {"source":"integration_manifest","path":"Integrations/manifests/slack.toml"}.';


CREATE TABLE IF NOT EXISTS data_dictionary_entries (
    object_kind        text NOT NULL,
    field_path         text NOT NULL,
    source             text NOT NULL,
    field_kind         text NOT NULL DEFAULT 'text',
    label              text NOT NULL DEFAULT '',
    description        text NOT NULL DEFAULT '',
    required           boolean NOT NULL DEFAULT false,
    default_value      jsonb,
    valid_values       jsonb NOT NULL DEFAULT '[]'::jsonb,
    examples           jsonb NOT NULL DEFAULT '[]'::jsonb,
    deprecation_notes  text NOT NULL DEFAULT '',
    display_order      integer NOT NULL DEFAULT 100,
    origin_ref         jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_kind, field_path, source),
    CONSTRAINT data_dictionary_entries_object_fk
        FOREIGN KEY (object_kind) REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_entries_field_nonblank
        CHECK (btrim(field_path) <> ''),
    CONSTRAINT data_dictionary_entries_source_check
        CHECK (source IN ('auto', 'inferred', 'operator')),
    CONSTRAINT data_dictionary_entries_kind_check
        CHECK (field_kind IN (
            'text', 'number', 'boolean', 'enum', 'json', 'date', 'datetime',
            'reference', 'array', 'object'
        )),
    CONSTRAINT data_dictionary_entries_valid_values_shape
        CHECK (jsonb_typeof(valid_values) IN ('array', 'object')),
    CONSTRAINT data_dictionary_entries_examples_shape
        CHECK (jsonb_typeof(examples) IN ('array', 'object'))
);

COMMENT ON TABLE data_dictionary_entries IS
    'Field descriptors for any injected object kind. Keyed by (object_kind, field_path, source) so auto/inferred/operator layers coexist; data_dictionary_effective picks the winner per field.';
COMMENT ON COLUMN data_dictionary_entries.source IS
    'Precedence: operator > inferred > auto. Projectors write auto/inferred; operator writes go through runtime/data_dictionary.py.';
COMMENT ON COLUMN data_dictionary_entries.origin_ref IS
    'Where this descriptor was derived: {"projector":"schema_projector","location":"pg_catalog:workflow_runs"} or {"projector":"manifest_projector","path":"Integrations/manifests/slack.toml"}.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_entries_kind_order
    ON data_dictionary_entries (object_kind, display_order, field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_entries_source
    ON data_dictionary_entries (source);


-- Trigger: bump updated_at on write so freshness panels can surface staleness.
CREATE OR REPLACE FUNCTION touch_data_dictionary_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_objects_touch ON data_dictionary_objects;
CREATE TRIGGER trg_data_dictionary_objects_touch
    BEFORE UPDATE ON data_dictionary_objects
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();

DROP TRIGGER IF EXISTS trg_data_dictionary_entries_touch ON data_dictionary_entries;
CREATE TRIGGER trg_data_dictionary_entries_touch
    BEFORE UPDATE ON data_dictionary_entries
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();


-- Effective view: operator > inferred > auto, one row per (object_kind, field_path).
CREATE OR REPLACE VIEW data_dictionary_effective AS
SELECT DISTINCT ON (object_kind, field_path)
       object_kind,
       field_path,
       source AS effective_source,
       field_kind,
       label,
       description,
       required,
       default_value,
       valid_values,
       examples,
       deprecation_notes,
       display_order,
       origin_ref,
       metadata,
       created_at,
       updated_at
  FROM data_dictionary_entries
 ORDER BY object_kind,
          field_path,
          CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 WHEN 'auto' THEN 2 ELSE 3 END;

COMMENT ON VIEW data_dictionary_effective IS
    'Merged data dictionary view: for each (object_kind, field_path), the highest-precedence source wins (operator > inferred > auto).';


-- pg_notify channel so Moon / subscribers can react to dictionary mutations.
CREATE OR REPLACE FUNCTION notify_data_dictionary_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload := jsonb_build_object(
            'op', 'delete',
            'object_kind', OLD.object_kind,
            'field_path', OLD.field_path,
            'source', OLD.source
        );
    ELSE
        payload := jsonb_build_object(
            'op', lower(TG_OP),
            'object_kind', NEW.object_kind,
            'field_path', NEW.field_path,
            'source', NEW.source
        );
    END IF;
    PERFORM pg_notify('data_dictionary_changed', payload::text);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_entries_notify ON data_dictionary_entries;
CREATE TRIGGER trg_data_dictionary_entries_notify
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_entries
    FOR EACH ROW EXECUTE FUNCTION notify_data_dictionary_change();
