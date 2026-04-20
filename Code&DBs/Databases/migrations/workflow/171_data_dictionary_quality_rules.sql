-- Migration 171: Data dictionary quality rules + rule runs authority.
--
-- Rules are declarative data-quality checks bound to (object_kind, field_path,
-- rule_kind). Layered sources mirror data_dictionary_entries:
--   auto     — projected from schema heuristics (NOT NULL columns in pg_attribute,
--              unique indexes, FKs → implicit not_null + referential rules).
--   inferred — sampler-derived (field distribution suggests an enum, range, etc.)
--   operator — hand-curated, highest precedence.
--
-- Rule runs are observations: each row records the outcome of evaluating one
-- effective rule at a point in time. They live in a separate append-mostly
-- table so we can build quality-over-time dashboards.
--
-- Separate from `semantic_assertions`: that is a pure edge store (subject, predicate,
-- object) with no evaluation engine. Quality rules DO have evaluators (runtime
-- walks Postgres and produces a pass/fail) and their runs capture observed
-- counts + sample failing values for operator review.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.quality_rules_mdm_lite
-- Scope:    authority_domain=data_dictionary.quality

CREATE TABLE IF NOT EXISTS data_dictionary_quality_rules (
    object_kind       text NOT NULL,
    field_path        text NOT NULL DEFAULT '',
    rule_kind         text NOT NULL,
    source            text NOT NULL,
    expression        jsonb NOT NULL DEFAULT '{}'::jsonb,
    severity          text NOT NULL DEFAULT 'warning',
    description       text NOT NULL DEFAULT '',
    enabled           boolean NOT NULL DEFAULT true,
    confidence        real NOT NULL DEFAULT 1.0,
    origin_ref        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_kind, field_path, rule_kind, source),
    CONSTRAINT data_dictionary_quality_rules_object_fk
        FOREIGN KEY (object_kind) REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_quality_rules_source_check
        CHECK (source IN ('auto', 'inferred', 'operator')),
    CONSTRAINT data_dictionary_quality_rules_severity_check
        CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    CONSTRAINT data_dictionary_quality_rules_rule_kind_nonblank
        CHECK (btrim(rule_kind) <> ''),
    CONSTRAINT data_dictionary_quality_rules_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

COMMENT ON TABLE data_dictionary_quality_rules IS
    'Declarative data-quality checks on data dictionary fields. Layered (auto/inferred/operator) — heuristic projectors cannot clobber operator-curated rules. rule_kind names the check family (not_null, unique, regex, range, enum, row_count_min, row_count_max, referential, custom_sql); expression holds its structured payload.';
COMMENT ON COLUMN data_dictionary_quality_rules.expression IS
    'Structured check payload; shape depends on rule_kind. Examples: {"regex":"^\\S+@\\S+$"}, {"min":0,"max":100}, {"sql":"SELECT count(*) FROM t WHERE x IS NULL"}.';
COMMENT ON COLUMN data_dictionary_quality_rules.origin_ref IS
    'Provenance, e.g. {"projector":"quality_not_null_from_pg_attribute","pg_attnum":3}.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_rules_object
    ON data_dictionary_quality_rules (object_kind, field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_rules_kind
    ON data_dictionary_quality_rules (rule_kind);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_rules_source
    ON data_dictionary_quality_rules (source);


-- Bump updated_at (shared trigger function from migration 166).
DROP TRIGGER IF EXISTS trg_data_dictionary_quality_rules_touch
    ON data_dictionary_quality_rules;
CREATE TRIGGER trg_data_dictionary_quality_rules_touch
    BEFORE UPDATE ON data_dictionary_quality_rules
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();


-- Effective view: operator > inferred > auto per (object, field, rule_kind).
CREATE OR REPLACE VIEW data_dictionary_quality_rules_effective AS
SELECT DISTINCT ON (object_kind, field_path, rule_kind)
       object_kind,
       field_path,
       rule_kind,
       source AS effective_source,
       expression,
       severity,
       description,
       enabled,
       confidence,
       origin_ref,
       metadata,
       created_at,
       updated_at
  FROM data_dictionary_quality_rules
 ORDER BY object_kind,
          field_path,
          rule_kind,
          CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 WHEN 'auto' THEN 2 ELSE 3 END;

COMMENT ON VIEW data_dictionary_quality_rules_effective IS
    'Merged quality-rules view: operator > inferred > auto per (object_kind, field_path, rule_kind).';


-- Runs table: one row per evaluation of one effective rule.
CREATE TABLE IF NOT EXISTS data_dictionary_quality_runs (
    run_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    object_kind       text NOT NULL,
    field_path        text NOT NULL DEFAULT '',
    rule_kind         text NOT NULL,
    effective_source  text NOT NULL,
    status            text NOT NULL,
    observed          jsonb NOT NULL DEFAULT '{}'::jsonb,
    duration_ms       real NOT NULL DEFAULT 0.0,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    error_message     text NOT NULL DEFAULT '',
    CONSTRAINT data_dictionary_quality_runs_status_check
        CHECK (status IN ('pass', 'fail', 'error'))
);

COMMENT ON TABLE data_dictionary_quality_runs IS
    'Rule-run observations. Append-mostly. Each row = one evaluation of the effective rule at (object_kind, field_path, rule_kind), capturing status, observed counts / samples, and timing. Older runs are retained for quality-over-time analysis; prune is scheduler-driven, not constraint-driven.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_runs_rule
    ON data_dictionary_quality_runs (object_kind, field_path, rule_kind);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_runs_started
    ON data_dictionary_quality_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_quality_runs_status
    ON data_dictionary_quality_runs (status)
    WHERE status IN ('fail', 'error');


-- Latest-run view: the most recent run per rule, for dashboards.
CREATE OR REPLACE VIEW data_dictionary_quality_latest_runs AS
SELECT DISTINCT ON (object_kind, field_path, rule_kind)
       run_id,
       object_kind,
       field_path,
       rule_kind,
       effective_source,
       status,
       observed,
       duration_ms,
       started_at,
       finished_at,
       error_message
  FROM data_dictionary_quality_runs
 ORDER BY object_kind, field_path, rule_kind, started_at DESC;

COMMENT ON VIEW data_dictionary_quality_latest_runs IS
    'Most recent run per (object_kind, field_path, rule_kind) — feeds dashboards without needing to scan history.';


-- pg_notify channel for Moon / subscribers.
CREATE OR REPLACE FUNCTION notify_data_dictionary_quality_rule_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload := jsonb_build_object(
            'op', 'delete',
            'object_kind', OLD.object_kind,
            'field_path', OLD.field_path,
            'rule_kind', OLD.rule_kind,
            'source', OLD.source
        );
    ELSE
        payload := jsonb_build_object(
            'op', lower(TG_OP),
            'object_kind', NEW.object_kind,
            'field_path', NEW.field_path,
            'rule_kind', NEW.rule_kind,
            'source', NEW.source
        );
    END IF;
    PERFORM pg_notify('data_dictionary_quality_rule_changed', payload::text);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_quality_rules_notify
    ON data_dictionary_quality_rules;
CREATE TRIGGER trg_data_dictionary_quality_rules_notify
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_quality_rules
    FOR EACH ROW EXECUTE FUNCTION notify_data_dictionary_quality_rule_change();
