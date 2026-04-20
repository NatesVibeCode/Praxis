-- Migration 170: Data dictionary classifications / tags authority.
--
-- Key-valued labels attached to (object_kind, field_path). Layered sources
-- mirror data_dictionary_entries and data_dictionary_lineage:
--   auto     — projected from name heuristics (PII detectors, owner cols)
--              and type hints (json columns marked "structured", etc.)
--   inferred — sampler-derived (e.g. value matches email regex)
--   operator — hand-curated, highest precedence
--
-- Reserved (but not enforced) tag_keys:
--   pii               — value = "email" | "phone" | "ssn" | "credit_card" ...
--   sensitive         — value = "high" | "medium" | "low"
--   retention         — value = "30d" | "1y" | "permanent"
--   owner_domain      — value = free-form domain label (e.g. "finance")
--   structured_shape  — value = short JSON-shape hint for jsonb cols
--
-- Decision: operator_decision.architecture_policy.data_dictionary.classifications_mdm_lite
-- Scope:    authority_domain=data_dictionary.classifications

CREATE TABLE IF NOT EXISTS data_dictionary_classifications (
    object_kind       text NOT NULL,
    field_path        text NOT NULL DEFAULT '',  -- '' = object-level tag
    tag_key           text NOT NULL,
    tag_value         text NOT NULL DEFAULT '',
    source            text NOT NULL,
    confidence        real NOT NULL DEFAULT 1.0,
    origin_ref        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_kind, field_path, tag_key, source),
    CONSTRAINT data_dictionary_classifications_object_fk
        FOREIGN KEY (object_kind) REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_classifications_key_nonblank
        CHECK (btrim(tag_key) <> ''),
    CONSTRAINT data_dictionary_classifications_source_check
        CHECK (source IN ('auto', 'inferred', 'operator')),
    CONSTRAINT data_dictionary_classifications_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

COMMENT ON TABLE data_dictionary_classifications IS
    'Key/value tags on data dictionary fields. Layered (auto/inferred/operator) — heuristic projectors cannot clobber operator-curated labels. tag_key defines the dimension (pii, sensitive, retention, owner_domain); tag_value is the specific label within that dimension.';
COMMENT ON COLUMN data_dictionary_classifications.field_path IS
    'Field-path within object_kind; empty string = object-level tag.';
COMMENT ON COLUMN data_dictionary_classifications.origin_ref IS
    'Where this tag was derived, e.g. {"projector":"classification_pii_heuristics","rule":"email_colname"}.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_classifications_object
    ON data_dictionary_classifications (object_kind, field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_classifications_tag
    ON data_dictionary_classifications (tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_classifications_source
    ON data_dictionary_classifications (source);


-- Bump updated_at (shared trigger function from 166).
DROP TRIGGER IF EXISTS trg_data_dictionary_classifications_touch
    ON data_dictionary_classifications;
CREATE TRIGGER trg_data_dictionary_classifications_touch
    BEFORE UPDATE ON data_dictionary_classifications
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();


-- Effective view: operator > inferred > auto per (object, field, tag_key).
CREATE OR REPLACE VIEW data_dictionary_classifications_effective AS
SELECT DISTINCT ON (object_kind, field_path, tag_key)
       object_kind,
       field_path,
       tag_key,
       tag_value,
       source AS effective_source,
       confidence,
       origin_ref,
       metadata,
       created_at,
       updated_at
  FROM data_dictionary_classifications
 ORDER BY object_kind,
          field_path,
          tag_key,
          CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 WHEN 'auto' THEN 2 ELSE 3 END;

COMMENT ON VIEW data_dictionary_classifications_effective IS
    'Merged classification view: operator > inferred > auto per (object_kind, field_path, tag_key).';


-- pg_notify channel for Moon / subscribers.
CREATE OR REPLACE FUNCTION notify_data_dictionary_classification_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload := jsonb_build_object(
            'op', 'delete',
            'object_kind', OLD.object_kind,
            'field_path', OLD.field_path,
            'tag_key', OLD.tag_key,
            'source', OLD.source
        );
    ELSE
        payload := jsonb_build_object(
            'op', lower(TG_OP),
            'object_kind', NEW.object_kind,
            'field_path', NEW.field_path,
            'tag_key', NEW.tag_key,
            'source', NEW.source
        );
    END IF;
    PERFORM pg_notify('data_dictionary_classification_changed', payload::text);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_classifications_notify
    ON data_dictionary_classifications;
CREATE TRIGGER trg_data_dictionary_classifications_notify
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_classifications
    FOR EACH ROW EXECUTE FUNCTION notify_data_dictionary_classification_change();
