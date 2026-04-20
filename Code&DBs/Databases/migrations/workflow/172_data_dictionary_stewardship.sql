-- Migration 172: Data dictionary stewardship authority.
--
-- Who is responsible for a data asset? Layered (auto/inferred/operator)
-- per (object_kind, field_path, steward_kind, steward_id). Multiple
-- stewards per kind are allowed (two owners, three contacts) — the
-- unique edge is (..., steward_id) so distinct ids coexist.
--
-- Reserved steward_kind values:
--   owner      — primary accountable party
--   approver   — must sign off on schema / semantic changes
--   contact    — ask-here for questions
--   publisher  — runs the producing pipeline
--   consumer   — downstream user that must be notified on breaking change
--
-- Reserved steward_type values:
--   person | team | agent | role | service
--
-- Decision: operator_decision.architecture_policy.data_dictionary.stewardship_mdm_lite
-- Scope:    authority_domain=data_dictionary.stewardship

CREATE TABLE IF NOT EXISTS data_dictionary_stewardship (
    object_kind       text NOT NULL,
    field_path        text NOT NULL DEFAULT '',  -- '' = object-level stewardship
    steward_kind      text NOT NULL,
    steward_id        text NOT NULL,             -- opaque handle: email, slug, agent_tag
    steward_type      text NOT NULL DEFAULT 'person',
    source            text NOT NULL,
    confidence        real NOT NULL DEFAULT 1.0,
    origin_ref        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_kind, field_path, steward_kind, steward_id, source),
    CONSTRAINT data_dictionary_stewardship_object_fk
        FOREIGN KEY (object_kind) REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_stewardship_kind_nonblank
        CHECK (btrim(steward_kind) <> ''),
    CONSTRAINT data_dictionary_stewardship_id_nonblank
        CHECK (btrim(steward_id) <> ''),
    CONSTRAINT data_dictionary_stewardship_source_check
        CHECK (source IN ('auto', 'inferred', 'operator')),
    CONSTRAINT data_dictionary_stewardship_type_check
        CHECK (steward_type IN ('person', 'team', 'agent', 'role', 'service')),
    CONSTRAINT data_dictionary_stewardship_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

COMMENT ON TABLE data_dictionary_stewardship IS
    'Stewards for data dictionary objects and fields. Layered (auto/inferred/operator) — heuristic projectors cannot clobber operator-curated stewards. (steward_kind, steward_id) is multi-valued; multiple owners or contacts coexist.';
COMMENT ON COLUMN data_dictionary_stewardship.field_path IS
    'Field path within object_kind; empty string = object-level steward.';
COMMENT ON COLUMN data_dictionary_stewardship.steward_kind IS
    'Role dimension: owner, approver, contact, publisher, consumer.';
COMMENT ON COLUMN data_dictionary_stewardship.steward_id IS
    'Opaque handle — email, github login, team slug, agent_tag, service name.';
COMMENT ON COLUMN data_dictionary_stewardship.steward_type IS
    'Type of principal: person, team, agent, role, service.';
COMMENT ON COLUMN data_dictionary_stewardship.origin_ref IS
    'Where this steward was derived, e.g. {"projector":"stewardship_created_by_heuristics","principal_column":"created_by"}.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_stewardship_object
    ON data_dictionary_stewardship (object_kind, field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_stewardship_id
    ON data_dictionary_stewardship (steward_id);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_stewardship_kind
    ON data_dictionary_stewardship (steward_kind);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_stewardship_source
    ON data_dictionary_stewardship (source);


-- Bump updated_at (shared trigger function from 166).
DROP TRIGGER IF EXISTS trg_data_dictionary_stewardship_touch
    ON data_dictionary_stewardship;
CREATE TRIGGER trg_data_dictionary_stewardship_touch
    BEFORE UPDATE ON data_dictionary_stewardship
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();


-- Effective view: operator > inferred > auto per (object, field, kind, id).
CREATE OR REPLACE VIEW data_dictionary_stewardship_effective AS
SELECT DISTINCT ON (object_kind, field_path, steward_kind, steward_id)
       object_kind,
       field_path,
       steward_kind,
       steward_id,
       steward_type,
       source AS effective_source,
       confidence,
       origin_ref,
       metadata,
       created_at,
       updated_at
  FROM data_dictionary_stewardship
 ORDER BY object_kind,
          field_path,
          steward_kind,
          steward_id,
          CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 WHEN 'auto' THEN 2 ELSE 3 END;

COMMENT ON VIEW data_dictionary_stewardship_effective IS
    'Merged stewardship: operator > inferred > auto per (object_kind, field_path, steward_kind, steward_id).';


-- pg_notify channel for Moon / subscribers.
CREATE OR REPLACE FUNCTION notify_data_dictionary_stewardship_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload := jsonb_build_object(
            'op', 'delete',
            'object_kind', OLD.object_kind,
            'field_path', OLD.field_path,
            'steward_kind', OLD.steward_kind,
            'steward_id', OLD.steward_id,
            'source', OLD.source
        );
    ELSE
        payload := jsonb_build_object(
            'op', lower(TG_OP),
            'object_kind', NEW.object_kind,
            'field_path', NEW.field_path,
            'steward_kind', NEW.steward_kind,
            'steward_id', NEW.steward_id,
            'source', NEW.source
        );
    END IF;
    PERFORM pg_notify('data_dictionary_stewardship_changed', payload::text);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_stewardship_notify
    ON data_dictionary_stewardship;
CREATE TRIGGER trg_data_dictionary_stewardship_notify
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_stewardship
    FOR EACH ROW EXECUTE FUNCTION notify_data_dictionary_stewardship_change();
