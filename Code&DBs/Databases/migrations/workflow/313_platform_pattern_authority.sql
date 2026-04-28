-- Migration 313: Platform pattern authority.
--
-- Patterns sit between raw evidence and bugs:
--   evidence event -> recurring pattern -> intervention / bug / policy.
--
-- Identity is crypto-ready: every pattern stores an explicit digest purpose,
-- algorithm, and canonicalization version so the future crypto registry can
-- adopt this without guessing what a bare hash means.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.patterns',
    'praxis.engine',
    'stream.authority.patterns',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'operator_decision.architecture_policy.pattern_authority.failure_patterns_between_evidence_and_bugs'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS platform_patterns (
    pattern_ref text PRIMARY KEY,
    pattern_key text NOT NULL,
    identity_digest text NOT NULL,
    identity_digest_purpose text NOT NULL DEFAULT 'platform_pattern.identity',
    identity_digest_algorithm text NOT NULL DEFAULT 'sha256',
    identity_digest_canonicalization text NOT NULL DEFAULT 'platform_pattern_identity_v1',
    pattern_kind text NOT NULL,
    title text NOT NULL,
    failure_mode text NOT NULL,
    status text NOT NULL DEFAULT 'observing',
    severity text NOT NULL DEFAULT 'P2',
    promotion_rule jsonb NOT NULL DEFAULT '{}'::jsonb,
    owner_surface text NOT NULL DEFAULT 'praxis_patterns',
    verifier_ref text,
    decision_ref text NOT NULL DEFAULT 'operator_decision.architecture_policy.pattern_authority.failure_patterns_between_evidence_and_bugs',
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    evidence_count integer NOT NULL DEFAULT 0,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT platform_patterns_pattern_key_nonblank CHECK (btrim(pattern_key) <> ''),
    CONSTRAINT platform_patterns_identity_digest_nonblank CHECK (btrim(identity_digest) <> ''),
    CONSTRAINT platform_patterns_kind_check CHECK (
        pattern_kind IN (
            'architecture_smell',
            'runtime_failure_pattern',
            'operator_friction',
            'missing_authority',
            'weak_observability'
        )
    ),
    CONSTRAINT platform_patterns_status_check CHECK (
        status IN (
            'observing',
            'confirmed',
            'intervention_planned',
            'mitigated',
            'rejected'
        )
    ),
    CONSTRAINT platform_patterns_severity_check CHECK (severity IN ('P0', 'P1', 'P2', 'P3')),
    CONSTRAINT platform_patterns_promotion_rule_object CHECK (jsonb_typeof(promotion_rule) = 'object'),
    CONSTRAINT platform_patterns_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT platform_patterns_unique_key UNIQUE (pattern_key),
    CONSTRAINT platform_patterns_unique_digest UNIQUE (
        identity_digest_purpose,
        identity_digest_algorithm,
        identity_digest_canonicalization,
        identity_digest
    )
);

CREATE INDEX IF NOT EXISTS platform_patterns_kind_status_idx
    ON platform_patterns (pattern_kind, status, last_seen_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS platform_patterns_status_seen_idx
    ON platform_patterns (status, last_seen_at DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS platform_pattern_evidence_links (
    pattern_evidence_link_id text PRIMARY KEY,
    pattern_ref text NOT NULL,
    evidence_kind text NOT NULL,
    evidence_ref text NOT NULL,
    evidence_role text NOT NULL DEFAULT 'observed_in',
    observed_at timestamptz,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL DEFAULT 'praxis_patterns',
    CONSTRAINT platform_pattern_evidence_pattern_fkey
        FOREIGN KEY (pattern_ref)
        REFERENCES platform_patterns (pattern_ref)
        ON DELETE CASCADE,
    CONSTRAINT platform_pattern_evidence_kind_check CHECK (
        evidence_kind IN (
            'bug',
            'receipt',
            'operation_receipt',
            'run',
            'friction_event',
            'typed_gap',
            'authority_event',
            'roadmap_item',
            'decision',
            'file'
        )
    ),
    CONSTRAINT platform_pattern_evidence_ref_nonblank CHECK (btrim(evidence_ref) <> ''),
    CONSTRAINT platform_pattern_evidence_role_nonblank CHECK (btrim(evidence_role) <> ''),
    CONSTRAINT platform_pattern_evidence_details_object CHECK (jsonb_typeof(details) = 'object'),
    CONSTRAINT platform_pattern_evidence_unique_link
        UNIQUE (pattern_ref, evidence_kind, evidence_ref, evidence_role)
);

CREATE INDEX IF NOT EXISTS platform_pattern_evidence_pattern_created_idx
    ON platform_pattern_evidence_links (pattern_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS platform_pattern_evidence_kind_ref_idx
    ON platform_pattern_evidence_links (evidence_kind, evidence_ref);

CREATE OR REPLACE FUNCTION touch_platform_patterns_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_platform_patterns_touch ON platform_patterns;
CREATE TRIGGER trg_platform_patterns_touch
    BEFORE UPDATE ON platform_patterns
    FOR EACH ROW EXECUTE FUNCTION touch_platform_patterns_updated_at();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'platform_patterns',
        'Platform patterns',
        'table',
        'Durable authority for recurring platform failure shapes between raw evidence and bugs.',
        '{"migration":"313_platform_pattern_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.patterns"}'::jsonb
    ),
    (
        'platform_pattern_evidence_links',
        'Platform pattern evidence links',
        'table',
        'Explicit evidence links from platform patterns to bugs, receipts, friction events, typed gaps, decisions, and files.',
        '{"migration":"313_platform_pattern_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.patterns"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES
    (
        'table.public.platform_patterns',
        'table',
        'platform_patterns',
        'public',
        'authority.patterns',
        'platform_patterns',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.pattern_authority.failure_patterns_between_evidence_and_bugs',
        '{"purpose":"recurring pattern authority"}'::jsonb
    ),
    (
        'table.public.platform_pattern_evidence_links',
        'table',
        'platform_pattern_evidence_links',
        'public',
        'authority.patterns',
        'platform_pattern_evidence_links',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.pattern_authority.failure_patterns_between_evidence_and_bugs',
        '{"purpose":"pattern evidence authority"}'::jsonb
    )
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'event_contract.pattern.candidates_materialized',
    'pattern.candidates_materialized',
    'authority.patterns',
    'data_dictionary.object.pattern_candidates_materialized_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'operator_decision.architecture_policy.pattern_authority.failure_patterns_between_evidence_and_bugs',
    '{"migration":"313_platform_pattern_authority.sql","description":"Emitted when recurring evidence candidates are materialized into durable platform pattern authority."}'::jsonb
)
ON CONFLICT (event_contract_ref) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs = EXCLUDED.reducer_refs,
    projection_refs = EXCLUDED.projection_refs,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMENT ON TABLE platform_patterns IS
    'Durable pattern authority between raw evidence and bug tickets. Patterns own recurring system shapes, evidence links, promotion rules, and intervention state.';

COMMENT ON COLUMN platform_patterns.identity_digest IS
    'Digest over canonical pattern identity. Pair with purpose/algorithm/canonicalization columns; never treat a bare digest as self-describing authority.';

COMMIT;
