-- Canonical semantic assertion substrate for cross-domain semantics.
--
-- This is the authority layer for typed semantic assertions that must be
-- durable, queryable, and replayable across runtime domains. JSON qualifiers
-- remain contextual evidence only; subject, predicate, object, provenance, and
-- validity stay first-class columns.

CREATE TABLE IF NOT EXISTS semantic_predicates (
    predicate_slug text PRIMARY KEY,
    predicate_status text NOT NULL DEFAULT 'active',
    subject_kind_allowlist jsonb NOT NULL DEFAULT '[]'::jsonb,
    object_kind_allowlist jsonb NOT NULL DEFAULT '[]'::jsonb,
    cardinality_mode text NOT NULL DEFAULT 'many',
    description text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT semantic_predicates_nonblank
        CHECK (
            btrim(predicate_slug) <> ''
            AND btrim(predicate_status) <> ''
            AND btrim(cardinality_mode) <> ''
        ),
    CONSTRAINT semantic_predicates_status_check
        CHECK (predicate_status IN ('active', 'inactive')),
    CONSTRAINT semantic_predicates_cardinality_check
        CHECK (
            cardinality_mode IN (
                'many',
                'single_active_per_subject',
                'single_active_per_edge'
            )
        ),
    CONSTRAINT semantic_predicates_subject_allowlist_is_array
        CHECK (jsonb_typeof(subject_kind_allowlist) = 'array'),
    CONSTRAINT semantic_predicates_object_allowlist_is_array
        CHECK (jsonb_typeof(object_kind_allowlist) = 'array')
);

CREATE INDEX IF NOT EXISTS semantic_predicates_status_updated_idx
    ON semantic_predicates (predicate_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS semantic_assertions (
    semantic_assertion_id text PRIMARY KEY,
    predicate_slug text NOT NULL REFERENCES semantic_predicates (predicate_slug) ON DELETE RESTRICT,
    assertion_status text NOT NULL DEFAULT 'active',
    subject_kind text NOT NULL,
    subject_ref text NOT NULL,
    object_kind text NOT NULL,
    object_ref text NOT NULL,
    qualifiers_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_kind text NOT NULL,
    source_ref text NOT NULL,
    evidence_ref text,
    bound_decision_id text REFERENCES operator_decisions (operator_decision_id) ON DELETE SET NULL,
    valid_from timestamptz NOT NULL DEFAULT now(),
    valid_to timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT semantic_assertions_nonblank
        CHECK (
            btrim(semantic_assertion_id) <> ''
            AND btrim(predicate_slug) <> ''
            AND btrim(assertion_status) <> ''
            AND btrim(subject_kind) <> ''
            AND btrim(subject_ref) <> ''
            AND btrim(object_kind) <> ''
            AND btrim(object_ref) <> ''
            AND btrim(source_kind) <> ''
            AND btrim(source_ref) <> ''
        ),
    CONSTRAINT semantic_assertions_status_check
        CHECK (assertion_status IN ('active', 'superseded', 'retracted')),
    CONSTRAINT semantic_assertions_qualifiers_object_check
        CHECK (jsonb_typeof(qualifiers_json) = 'object'),
    CONSTRAINT semantic_assertions_valid_window_check
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS semantic_assertions_predicate_subject_idx
    ON semantic_assertions (
        predicate_slug,
        subject_kind,
        subject_ref,
        valid_from DESC,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS semantic_assertions_predicate_edge_idx
    ON semantic_assertions (
        predicate_slug,
        subject_kind,
        subject_ref,
        object_kind,
        object_ref,
        valid_from DESC
    );

CREATE INDEX IF NOT EXISTS semantic_assertions_object_idx
    ON semantic_assertions (
        object_kind,
        object_ref,
        valid_from DESC,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS semantic_assertions_source_idx
    ON semantic_assertions (source_kind, source_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS semantic_assertions_bound_decision_idx
    ON semantic_assertions (bound_decision_id, created_at DESC)
    WHERE bound_decision_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS semantic_current_assertions (
    semantic_assertion_id text PRIMARY KEY,
    predicate_slug text NOT NULL,
    assertion_status text NOT NULL,
    subject_kind text NOT NULL,
    subject_ref text NOT NULL,
    object_kind text NOT NULL,
    object_ref text NOT NULL,
    qualifiers_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_kind text NOT NULL,
    source_ref text NOT NULL,
    evidence_ref text,
    bound_decision_id text,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT semantic_current_assertions_nonblank
        CHECK (
            btrim(semantic_assertion_id) <> ''
            AND btrim(predicate_slug) <> ''
            AND btrim(assertion_status) <> ''
            AND btrim(subject_kind) <> ''
            AND btrim(subject_ref) <> ''
            AND btrim(object_kind) <> ''
            AND btrim(object_ref) <> ''
            AND btrim(source_kind) <> ''
            AND btrim(source_ref) <> ''
        ),
    CONSTRAINT semantic_current_assertions_qualifiers_object_check
        CHECK (jsonb_typeof(qualifiers_json) = 'object')
);

CREATE INDEX IF NOT EXISTS semantic_current_assertions_predicate_subject_idx
    ON semantic_current_assertions (
        predicate_slug,
        subject_kind,
        subject_ref,
        valid_from DESC
    );

CREATE INDEX IF NOT EXISTS semantic_current_assertions_object_idx
    ON semantic_current_assertions (
        object_kind,
        object_ref,
        valid_from DESC
    );

CREATE INDEX IF NOT EXISTS semantic_current_assertions_source_idx
    ON semantic_current_assertions (source_kind, source_ref, created_at DESC);

COMMENT ON TABLE semantic_predicates IS
    'Vocabulary registry for allowed semantic predicates. No free-form predicate meaning should bypass this table.';
COMMENT ON TABLE semantic_assertions IS
    'Canonical write-model authority for cross-domain semantic assertions. This table owns truth; projections and bus consumers interpret it.';
COMMENT ON TABLE semantic_current_assertions IS
    'Current-time CQRS projection rebuilt from semantic_assertions. Read optimizations live here; authority does not.';
COMMENT ON COLUMN semantic_assertions.qualifiers_json IS
    'Optional contextual evidence for one assertion. This field is not allowed to hide subject, predicate, object, or provenance authority.';
