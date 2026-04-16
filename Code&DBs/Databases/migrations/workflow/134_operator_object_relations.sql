-- Canonical cross-object semantic authority for operator-visible work.
--
-- Existing issue/bug/roadmap lineage stays authoritative in the native tables
-- that own those transitions. This migration adds the missing explicit graph
-- layer for cross-object semantics that do not fit one specialized column:
-- functional areas, code paths, documents, workflow targets, and decisions.

CREATE TABLE IF NOT EXISTS functional_areas (
    functional_area_id text PRIMARY KEY,
    area_slug text NOT NULL UNIQUE,
    title text NOT NULL,
    area_status text NOT NULL DEFAULT 'active',
    summary text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT functional_areas_nonblank
        CHECK (
            btrim(functional_area_id) <> ''
            AND btrim(area_slug) <> ''
            AND btrim(title) <> ''
            AND btrim(area_status) <> ''
            AND btrim(summary) <> ''
        )
);

CREATE INDEX IF NOT EXISTS functional_areas_status_updated_idx
    ON functional_areas (area_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS operator_object_relations (
    operator_object_relation_id text PRIMARY KEY,
    relation_kind text NOT NULL,
    relation_status text NOT NULL DEFAULT 'active',
    source_kind text NOT NULL,
    source_ref text NOT NULL,
    target_kind text NOT NULL,
    target_ref text NOT NULL,
    relation_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    bound_by_decision_id text REFERENCES operator_decisions (operator_decision_id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operator_object_relations_nonblank
        CHECK (
            btrim(operator_object_relation_id) <> ''
            AND btrim(relation_kind) <> ''
            AND btrim(relation_status) <> ''
            AND btrim(source_kind) <> ''
            AND btrim(source_ref) <> ''
            AND btrim(target_kind) <> ''
            AND btrim(target_ref) <> ''
        ),
    CONSTRAINT operator_object_relations_distinct_endpoints
        CHECK (
            NOT (
                source_kind = target_kind
                AND source_ref = target_ref
            )
        ),
    CONSTRAINT operator_object_relations_source_kind_check
        CHECK (
            source_kind IN (
                'issue',
                'bug',
                'roadmap_item',
                'operator_decision',
                'cutover_gate',
                'workflow_class',
                'schedule_definition',
                'workflow_run',
                'document',
                'repo_path',
                'functional_area'
            )
        ),
    CONSTRAINT operator_object_relations_target_kind_check
        CHECK (
            target_kind IN (
                'issue',
                'bug',
                'roadmap_item',
                'operator_decision',
                'cutover_gate',
                'workflow_class',
                'schedule_definition',
                'workflow_run',
                'document',
                'repo_path',
                'functional_area'
            )
        ),
    CONSTRAINT operator_object_relations_unique_edge
        UNIQUE (relation_kind, source_kind, source_ref, target_kind, target_ref)
);

CREATE INDEX IF NOT EXISTS operator_object_relations_source_idx
    ON operator_object_relations (
        source_kind,
        source_ref,
        relation_status,
        relation_kind,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS operator_object_relations_target_idx
    ON operator_object_relations (
        target_kind,
        target_ref,
        relation_status,
        relation_kind,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS operator_object_relations_decision_idx
    ON operator_object_relations (bound_by_decision_id, created_at DESC)
    WHERE bound_by_decision_id IS NOT NULL;

COMMENT ON TABLE functional_areas IS
    'Canonical operator-visible functional areas. Use these rows instead of ad hoc text tags when bugs, roadmap items, code paths, or documents must share one durable semantic grouping.';
COMMENT ON TABLE operator_object_relations IS
    'Canonical cross-object relation authority. Use these rows for explicit semantics between work items, workflow targets, decisions, documents, repo paths, and functional areas.';
COMMENT ON COLUMN operator_object_relations.relation_kind IS
    'Typed semantic edge label such as grouped_in, described_by, implemented_by, governed_by, or evidenced_by. Keep relation meaning explicit and queryable.';
COMMENT ON COLUMN operator_object_relations.relation_metadata IS
    'Optional structured evidence or qualifiers for the relation. This is context, not hidden authority.';
