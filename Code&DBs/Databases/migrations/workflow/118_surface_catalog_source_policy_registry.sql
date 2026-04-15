BEGIN;

CREATE TABLE IF NOT EXISTS surface_catalog_source_policy_registry (
    policy_ref TEXT PRIMARY KEY,
    surface_name TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('capability', 'integration', 'connector')),
    truth_category TEXT NOT NULL CHECK (truth_category IN ('runtime', 'persisted', 'alias', 'partial', 'coming_soon')),
    truth_badge TEXT NOT NULL,
    truth_detail TEXT NOT NULL,
    surface_tier TEXT NOT NULL CHECK (surface_tier IN ('primary', 'advanced', 'hidden')),
    surface_badge TEXT NOT NULL,
    surface_detail TEXT NOT NULL,
    hard_choice TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT surface_catalog_source_policy_registry_surface_source_kind_key
        UNIQUE (surface_name, source_kind)
);

CREATE INDEX IF NOT EXISTS surface_catalog_source_policy_registry_surface_enabled_idx
    ON surface_catalog_source_policy_registry (surface_name, enabled, source_kind);

COMMENT ON TABLE surface_catalog_source_policy_registry IS 'Canonical source-kind policy registry for dynamic builder catalog rows. Dynamic capability, integration, and connector rows inherit truth and surface policy from here instead of API heuristics.';
COMMENT ON COLUMN surface_catalog_source_policy_registry.binding_revision IS 'Revision stamp for the source-policy binding. Policy changes must publish a new revision.';
COMMENT ON COLUMN surface_catalog_source_policy_registry.decision_ref IS 'Decision authority that justified the current dynamic catalog policy.';

INSERT INTO surface_catalog_source_policy_registry (
    policy_ref,
    surface_name,
    source_kind,
    truth_category,
    truth_badge,
    truth_detail,
    surface_tier,
    surface_badge,
    surface_detail,
    hard_choice,
    enabled,
    binding_revision,
    decision_ref
) VALUES
    (
        'moon-capability',
        'moon',
        'capability',
        'runtime',
        'Runs on release',
        'Capability routes persist into the build graph and become planned runtime routes at release.',
        'hidden',
        'Hidden',
        'Capability catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.',
        NULL,
        TRUE,
        'binding.surface_catalog_source_policy_registry.moon.bootstrap.20260415',
        'decision.surface_catalog_source_policy_registry.moon.bootstrap.20260415'
    ),
    (
        'moon-integration',
        'moon',
        'integration',
        'runtime',
        'Runs on release',
        'Integration actions persist into the build graph and become planned runtime routes at release.',
        'hidden',
        'Hidden',
        'Live integration catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.',
        NULL,
        TRUE,
        'binding.surface_catalog_source_policy_registry.moon.bootstrap.20260415',
        'decision.surface_catalog_source_policy_registry.moon.bootstrap.20260415'
    ),
    (
        'moon-connector',
        'moon',
        'connector',
        'runtime',
        'Runs on release',
        'Connector actions persist into the build graph and become planned runtime routes at release.',
        'hidden',
        'Hidden',
        'Connector catalog rows stay out of the main Moon builder until they are promoted into explicit surface primitives.',
        NULL,
        TRUE,
        'binding.surface_catalog_source_policy_registry.moon.bootstrap.20260415',
        'decision.surface_catalog_source_policy_registry.moon.bootstrap.20260415'
    )
ON CONFLICT (policy_ref) DO UPDATE SET
    surface_name = EXCLUDED.surface_name,
    source_kind = EXCLUDED.source_kind,
    truth_category = EXCLUDED.truth_category,
    truth_badge = EXCLUDED.truth_badge,
    truth_detail = EXCLUDED.truth_detail,
    surface_tier = EXCLUDED.surface_tier,
    surface_badge = EXCLUDED.surface_badge,
    surface_detail = EXCLUDED.surface_detail,
    hard_choice = EXCLUDED.hard_choice,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
