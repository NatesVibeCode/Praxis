-- Migration 169: Data dictionary lineage authority.
--
-- Directed edges between known object kinds (and, optionally, specific fields
-- within them). Layered sources mirror data_dictionary_entries:
--   auto     — projected from FK constraints, pg_depend view rels,
--              dataset_promotions, ingest manifests, workflow step I/O
--   inferred — sampler-derived (e.g. value overlap across tables)
--   operator — hand-authored relationships
-- The merged view `data_dictionary_lineage_effective` picks the highest-
-- precedence source per (src, dst, edge_kind) tuple.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.lineage_projector_mdm_lite
-- Scope:    authority_domain=data_dictionary.lineage

CREATE TABLE IF NOT EXISTS data_dictionary_lineage (
    src_object_kind    text NOT NULL,
    src_field_path     text NOT NULL DEFAULT '',     -- '' = object-level edge
    dst_object_kind    text NOT NULL,
    dst_field_path     text NOT NULL DEFAULT '',     -- '' = object-level edge
    edge_kind          text NOT NULL,
    source             text NOT NULL,
    confidence         real NOT NULL DEFAULT 1.0,
    origin_ref         jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (src_object_kind, src_field_path,
                 dst_object_kind, dst_field_path,
                 edge_kind, source),
    CONSTRAINT data_dictionary_lineage_src_fk
        FOREIGN KEY (src_object_kind)
        REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_lineage_dst_fk
        FOREIGN KEY (dst_object_kind)
        REFERENCES data_dictionary_objects (object_kind)
        ON DELETE CASCADE,
    CONSTRAINT data_dictionary_lineage_source_check
        CHECK (source IN ('auto', 'inferred', 'operator')),
    CONSTRAINT data_dictionary_lineage_kind_check
        CHECK (edge_kind IN (
            'references',     -- FK / manifest reference / tool-call target
            'derives_from',   -- value derived from src (ETL, projection)
            'projects_to',    -- projector writes auto rows into dst
            'ingests_from',   -- dst ingests payloads from src
            'produces',       -- workflow step produces dst
            'consumes',       -- workflow step consumes src
            'promotes_to',    -- dataset_promotion publishes into dst
            'same_as'         -- dedup / rename linkage
        )),
    CONSTRAINT data_dictionary_lineage_no_self_loop
        CHECK (
            src_object_kind <> dst_object_kind
            OR src_field_path <> dst_field_path
        ),
    CONSTRAINT data_dictionary_lineage_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

COMMENT ON TABLE data_dictionary_lineage IS
    'Directed lineage edges between data-dictionary objects. Layered (auto/inferred/operator) so projectors and operators can coexist without clobbering each other. edge_kind captures the relationship semantics.';
COMMENT ON COLUMN data_dictionary_lineage.src_field_path IS
    'Field within src_object_kind; empty string = object-level edge.';
COMMENT ON COLUMN data_dictionary_lineage.edge_kind IS
    'references=FK/manifest ref, derives_from=ETL, projects_to=projector, ingests_from=ingest, produces/consumes=workflow step I/O, promotes_to=dataset_promotion, same_as=rename/dedup.';
COMMENT ON COLUMN data_dictionary_lineage.origin_ref IS
    'Where this edge was discovered, e.g. {"projector":"lineage_projector","fk":"workflow_runs.spec_id -> workflow_specs.spec_id"}.';

CREATE INDEX IF NOT EXISTS idx_data_dictionary_lineage_src
    ON data_dictionary_lineage (src_object_kind, src_field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_lineage_dst
    ON data_dictionary_lineage (dst_object_kind, dst_field_path);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_lineage_kind
    ON data_dictionary_lineage (edge_kind);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_lineage_source
    ON data_dictionary_lineage (source);


-- Bump updated_at on write (same trigger function as entries).
DROP TRIGGER IF EXISTS trg_data_dictionary_lineage_touch ON data_dictionary_lineage;
CREATE TRIGGER trg_data_dictionary_lineage_touch
    BEFORE UPDATE ON data_dictionary_lineage
    FOR EACH ROW EXECUTE FUNCTION touch_data_dictionary_updated_at();


-- Effective view: operator > inferred > auto per (src, dst, edge_kind) tuple.
CREATE OR REPLACE VIEW data_dictionary_lineage_effective AS
SELECT DISTINCT ON (
           src_object_kind, src_field_path,
           dst_object_kind, dst_field_path,
           edge_kind
       )
       src_object_kind,
       src_field_path,
       dst_object_kind,
       dst_field_path,
       edge_kind,
       source AS effective_source,
       confidence,
       origin_ref,
       metadata,
       created_at,
       updated_at
  FROM data_dictionary_lineage
 ORDER BY src_object_kind,
          src_field_path,
          dst_object_kind,
          dst_field_path,
          edge_kind,
          CASE source WHEN 'operator' THEN 0 WHEN 'inferred' THEN 1 WHEN 'auto' THEN 2 ELSE 3 END;

COMMENT ON VIEW data_dictionary_lineage_effective IS
    'Merged lineage view: operator > inferred > auto per (src, dst, edge_kind).';


-- pg_notify channel so Moon / subscribers can react to lineage mutations.
CREATE OR REPLACE FUNCTION notify_data_dictionary_lineage_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload := jsonb_build_object(
            'op', 'delete',
            'src_object_kind', OLD.src_object_kind,
            'src_field_path', OLD.src_field_path,
            'dst_object_kind', OLD.dst_object_kind,
            'dst_field_path', OLD.dst_field_path,
            'edge_kind', OLD.edge_kind,
            'source', OLD.source
        );
    ELSE
        payload := jsonb_build_object(
            'op', lower(TG_OP),
            'src_object_kind', NEW.src_object_kind,
            'src_field_path', NEW.src_field_path,
            'dst_object_kind', NEW.dst_object_kind,
            'dst_field_path', NEW.dst_field_path,
            'edge_kind', NEW.edge_kind,
            'source', NEW.source
        );
    END IF;
    PERFORM pg_notify('data_dictionary_lineage_changed', payload::text);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_data_dictionary_lineage_notify ON data_dictionary_lineage;
CREATE TRIGGER trg_data_dictionary_lineage_notify
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_lineage
    FOR EACH ROW EXECUTE FUNCTION notify_data_dictionary_lineage_change();
