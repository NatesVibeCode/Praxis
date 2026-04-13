BEGIN;

CREATE TABLE IF NOT EXISTS compile_artifacts (
    compile_artifact_id TEXT PRIMARY KEY,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('definition', 'plan', 'packet_lineage')),
    artifact_ref TEXT NOT NULL,
    revision_ref TEXT NOT NULL,
    parent_artifact_ref TEXT,
    content_hash TEXT NOT NULL,
    authority_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload JSONB NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT compile_artifacts_authority_refs_array_check
        CHECK (jsonb_typeof(authority_refs) = 'array'),
    CONSTRAINT compile_artifacts_payload_object_check
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT compile_artifacts_unique_kind_revision
        UNIQUE (artifact_kind, revision_ref)
);

CREATE INDEX IF NOT EXISTS compile_artifacts_kind_revision_idx
    ON compile_artifacts (artifact_kind, revision_ref DESC);

CREATE INDEX IF NOT EXISTS compile_artifacts_content_hash_idx
    ON compile_artifacts (content_hash);

COMMENT ON TABLE compile_artifacts IS 'Canonical compile-spine artifact store for definition, plan, and packet-lineage authority. Owned by runtime/.';
COMMENT ON COLUMN compile_artifacts.artifact_kind IS 'Compile artifact class. definition, plan, and packet_lineage are the canonical v1 kinds.';
COMMENT ON COLUMN compile_artifacts.revision_ref IS 'Revision-aware authority ref for this artifact row. Rewrites must produce a new revision_ref.';
COMMENT ON COLUMN compile_artifacts.authority_refs IS 'Explicit authority refs that produced or justified this artifact snapshot.';

CREATE TABLE IF NOT EXISTS capability_catalog (
    capability_ref TEXT PRIMARY KEY,
    capability_slug TEXT NOT NULL UNIQUE,
    capability_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    description TEXT NOT NULL,
    route TEXT NOT NULL,
    engines JSONB NOT NULL DEFAULT '[]'::jsonb,
    signals JSONB NOT NULL DEFAULT '[]'::jsonb,
    reference_slugs JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT capability_catalog_engines_array_check
        CHECK (jsonb_typeof(engines) = 'array'),
    CONSTRAINT capability_catalog_signals_array_check
        CHECK (jsonb_typeof(signals) = 'array'),
    CONSTRAINT capability_catalog_reference_slugs_array_check
        CHECK (jsonb_typeof(reference_slugs) = 'array')
);

CREATE INDEX IF NOT EXISTS capability_catalog_kind_enabled_idx
    ON capability_catalog (capability_kind, enabled, title);

CREATE INDEX IF NOT EXISTS capability_catalog_route_idx
    ON capability_catalog (route);

COMMENT ON TABLE capability_catalog IS 'Canonical compile capability authority. Owned by runtime/compiler sync and DB rows, not hardcoded lists.';
COMMENT ON COLUMN capability_catalog.binding_revision IS 'Revision stamp for the authority row. Any catalog edit must publish a new revision.';

INSERT INTO capability_catalog (
    capability_ref,
    capability_slug,
    capability_kind,
    title,
    summary,
    description,
    route,
    engines,
    signals,
    reference_slugs,
    enabled,
    binding_revision,
    decision_ref
) VALUES
    (
        'cap-research-local-knowledge',
        'research/local-knowledge',
        'memory',
        'Local knowledge recall',
        'Search prior findings and saved research before going outbound.',
        'Uses dag_research and the local research runtime to search existing findings and compile briefs before new work starts.',
        'dag_research',
        '["dag_research", "memory.research_runtime"]'::jsonb,
        '["research", "findings", "knowledge", "brief", "prior", "existing", "history", "recall"]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'binding.capability_catalog.bootstrap.20260408',
        'decision.capability_catalog.bootstrap.20260408'
    ),
    (
        'cap-research-fan-out',
        'research/fan-out',
        'fanout',
        'Parallel research fan-out',
        'Split research into parallel sub-queries and aggregate the result.',
        'Uses runtime fan_out dispatch and fast Haiku-backed fan-out work when a question benefits from parallel angles or source sweeps.',
        'workflow.fanout',
        '["fan_out_dispatch", "claude-haiku-4-5-20251001"]'::jsonb,
        '["parallel", "fan out", "compare", "multiple", "angles", "sources", "sweep", "broad", "research"]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'binding.capability_catalog.bootstrap.20260408',
        'decision.capability_catalog.bootstrap.20260408'
    ),
    (
        'cap-research-gemini-cli',
        'research/gemini-cli',
        'cli',
        'Gemini CLI scan',
        'Use the Gemini CLI lane for broad external scanning when local context is thin.',
        'Uses the Gemini CLI provider lane exposed by the runtime planner and executor for broad CLI-driven scans.',
        'google/gemini-cli',
        '["gemini-cli"]'::jsonb,
        '["gemini", "cli", "scan", "external", "web", "search", "browse", "online"]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'binding.capability_catalog.bootstrap.20260408',
        'decision.capability_catalog.bootstrap.20260408'
    )
ON CONFLICT (capability_slug) DO UPDATE SET
    capability_kind = EXCLUDED.capability_kind,
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    description = EXCLUDED.description,
    route = EXCLUDED.route,
    engines = EXCLUDED.engines,
    signals = EXCLUDED.signals,
    reference_slugs = EXCLUDED.reference_slugs,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS verify_refs (
    verify_ref TEXT PRIMARY KEY,
    verification_ref TEXT NOT NULL,
    label TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT verify_refs_inputs_object_check
        CHECK (jsonb_typeof(inputs) = 'object'),
    CONSTRAINT verify_refs_verification_ref_fkey
        FOREIGN KEY (verification_ref)
        REFERENCES verification_registry (verification_ref)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS verify_refs_verification_enabled_idx
    ON verify_refs (verification_ref, enabled, updated_at DESC);

COMMENT ON TABLE verify_refs IS 'Canonical verification binding authority. Workflow specs emit these refs and runtime resolves them to typed argv rows.';
COMMENT ON COLUMN verify_refs.binding_revision IS 'Revision stamp for the binding row. Rebinding a verifier requires a new verify_ref.';
COMMENT ON COLUMN verify_refs.inputs IS 'Concrete inputs for the verification binding. Legacy raw command shapes are read-only compatibility only.';

COMMIT;
