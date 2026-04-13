BEGIN;

CREATE TABLE IF NOT EXISTS verifier_registry (
    verifier_ref TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    verifier_kind TEXT NOT NULL
        CHECK (verifier_kind IN ('verification_ref', 'builtin')),
    verification_ref TEXT
        REFERENCES verification_registry (verification_ref)
        ON DELETE CASCADE,
    builtin_ref TEXT,
    default_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT verifier_registry_target_check
        CHECK (
            (verifier_kind = 'verification_ref' AND verification_ref IS NOT NULL AND COALESCE(builtin_ref, '') = '')
            OR
            (verifier_kind = 'builtin' AND verification_ref IS NULL AND COALESCE(builtin_ref, '') <> '')
        ),
    CONSTRAINT verifier_registry_default_inputs_object_check
        CHECK (jsonb_typeof(default_inputs) = 'object')
);

CREATE INDEX IF NOT EXISTS verifier_registry_kind_enabled_idx
    ON verifier_registry (verifier_kind, enabled);

CREATE INDEX IF NOT EXISTS verifier_registry_verification_ref_idx
    ON verifier_registry (verification_ref);

CREATE TABLE IF NOT EXISTS healer_registry (
    healer_ref TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    executor_kind TEXT NOT NULL
        CHECK (executor_kind IN ('builtin')),
    action_ref TEXT NOT NULL,
    auto_mode TEXT NOT NULL DEFAULT 'manual'
        CHECK (auto_mode IN ('manual', 'assisted', 'automatic')),
    safety_mode TEXT NOT NULL DEFAULT 'guarded'
        CHECK (safety_mode IN ('guarded', 'unsafe')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS healer_registry_kind_enabled_idx
    ON healer_registry (executor_kind, enabled);

CREATE INDEX IF NOT EXISTS healer_registry_auto_mode_idx
    ON healer_registry (auto_mode, safety_mode);

CREATE TABLE IF NOT EXISTS verifier_healer_bindings (
    binding_ref TEXT PRIMARY KEY,
    verifier_ref TEXT NOT NULL
        REFERENCES verifier_registry (verifier_ref)
        ON DELETE CASCADE,
    healer_ref TEXT NOT NULL
        REFERENCES healer_registry (healer_ref)
        ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT verifier_healer_bindings_pair_unique UNIQUE (verifier_ref, healer_ref)
);

CREATE INDEX IF NOT EXISTS verifier_healer_bindings_verifier_enabled_idx
    ON verifier_healer_bindings (verifier_ref, enabled);

CREATE INDEX IF NOT EXISTS verifier_healer_bindings_healer_enabled_idx
    ON verifier_healer_bindings (healer_ref, enabled);

CREATE TABLE IF NOT EXISTS verification_runs (
    verification_run_id TEXT PRIMARY KEY,
    verifier_ref TEXT NOT NULL
        REFERENCES verifier_registry (verifier_ref)
        ON DELETE RESTRICT,
    target_kind TEXT NOT NULL
        CHECK (target_kind IN ('platform', 'receipt', 'run', 'path')),
    target_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL
        CHECK (status IN ('passed', 'failed', 'error')),
    inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    outputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    suggested_healer_ref TEXT
        REFERENCES healer_registry (healer_ref)
        ON DELETE SET NULL,
    healing_candidate BOOLEAN NOT NULL DEFAULT FALSE,
    decision_ref TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INTEGER NOT NULL DEFAULT 0
        CHECK (duration_ms >= 0),
    CONSTRAINT verification_runs_inputs_object_check
        CHECK (jsonb_typeof(inputs) = 'object'),
    CONSTRAINT verification_runs_outputs_object_check
        CHECK (jsonb_typeof(outputs) = 'object')
);

CREATE INDEX IF NOT EXISTS verification_runs_verifier_attempted_idx
    ON verification_runs (verifier_ref, attempted_at DESC);

CREATE INDEX IF NOT EXISTS verification_runs_target_status_idx
    ON verification_runs (target_kind, target_ref, status);

CREATE TABLE IF NOT EXISTS healing_runs (
    healing_run_id TEXT PRIMARY KEY,
    healer_ref TEXT NOT NULL
        REFERENCES healer_registry (healer_ref)
        ON DELETE RESTRICT,
    verifier_ref TEXT NOT NULL
        REFERENCES verifier_registry (verifier_ref)
        ON DELETE RESTRICT,
    target_kind TEXT NOT NULL
        CHECK (target_kind IN ('platform', 'receipt', 'run', 'path')),
    target_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL
        CHECK (status IN ('succeeded', 'failed', 'skipped', 'error')),
    inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    outputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision_ref TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INTEGER NOT NULL DEFAULT 0
        CHECK (duration_ms >= 0),
    CONSTRAINT healing_runs_inputs_object_check
        CHECK (jsonb_typeof(inputs) = 'object'),
    CONSTRAINT healing_runs_outputs_object_check
        CHECK (jsonb_typeof(outputs) = 'object')
);

CREATE INDEX IF NOT EXISTS healing_runs_healer_attempted_idx
    ON healing_runs (healer_ref, attempted_at DESC);

CREATE INDEX IF NOT EXISTS healing_runs_verifier_status_idx
    ON healing_runs (verifier_ref, status);

INSERT INTO verifier_registry (
    verifier_ref,
    display_name,
    description,
    verifier_kind,
    verification_ref,
    builtin_ref,
    default_inputs,
    enabled,
    decision_ref
) VALUES
    (
        'verifier.job.python.py_compile',
        'Job Python Bytecode Compile',
        'Run the canonical py_compile verifier through verification_registry authority.',
        'verification_ref',
        'verification.python.py_compile',
        NULL,
        '{}'::jsonb,
        TRUE,
        'decision.verifier_registry.bootstrap.20260409'
    ),
    (
        'verifier.job.python.pytest_file',
        'Job Pytest File',
        'Run the canonical pytest-file verifier through verification_registry authority.',
        'verification_ref',
        'verification.python.pytest_file',
        NULL,
        '{}'::jsonb,
        TRUE,
        'decision.verifier_registry.bootstrap.20260409'
    ),
    (
        'verifier.platform.schema_authority',
        'Platform Schema Authority',
        'Verify that the local workflow Postgres schema is bootstrapped and authoritative.',
        'builtin',
        NULL,
        'schema_authority',
        '{}'::jsonb,
        TRUE,
        'decision.verifier_registry.bootstrap.20260409'
    ),
    (
        'verifier.platform.receipt_provenance',
        'Receipt Provenance Compaction',
        'Verify that eligible receipts are compacted onto repo snapshots without duplicated git fields.',
        'builtin',
        NULL,
        'receipt_provenance',
        '{}'::jsonb,
        TRUE,
        'decision.verifier_registry.bootstrap.20260409'
    ),
    (
        'verifier.platform.memory_proof_links',
        'Memory Proof Links',
        'Verify that receipts with verification status have verification entities recorded into the graph.',
        'builtin',
        NULL,
        'memory_proof_links',
        '{}'::jsonb,
        TRUE,
        'decision.verifier_registry.bootstrap.20260409'
    )
ON CONFLICT (verifier_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    verifier_kind = EXCLUDED.verifier_kind,
    verification_ref = EXCLUDED.verification_ref,
    builtin_ref = EXCLUDED.builtin_ref,
    default_inputs = EXCLUDED.default_inputs,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO healer_registry (
    healer_ref,
    display_name,
    description,
    executor_kind,
    action_ref,
    auto_mode,
    safety_mode,
    enabled,
    decision_ref
) VALUES
    (
        'healer.platform.schema_bootstrap',
        'Schema Bootstrap',
        'Start local Postgres if needed and apply the canonical workflow schema.',
        'builtin',
        'schema_bootstrap',
        'assisted',
        'guarded',
        TRUE,
        'decision.healer_registry.bootstrap.20260409'
    ),
    (
        'healer.platform.receipt_provenance_backfill',
        'Receipt Provenance Backfill',
        'Backfill compact repo snapshot provenance and write/mutation provenance on historical receipts.',
        'builtin',
        'receipt_provenance_backfill',
        'assisted',
        'guarded',
        TRUE,
        'decision.healer_registry.bootstrap.20260409'
    ),
    (
        'healer.platform.proof_backfill',
        'Proof Backfill',
        'Rebuild receipt provenance and memory proof links for historical workflow runs.',
        'builtin',
        'proof_backfill',
        'assisted',
        'guarded',
        TRUE,
        'decision.healer_registry.bootstrap.20260409'
    )
ON CONFLICT (healer_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    executor_kind = EXCLUDED.executor_kind,
    action_ref = EXCLUDED.action_ref,
    auto_mode = EXCLUDED.auto_mode,
    safety_mode = EXCLUDED.safety_mode,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO verifier_healer_bindings (
    binding_ref,
    verifier_ref,
    healer_ref,
    enabled,
    binding_revision,
    decision_ref
) VALUES
    (
        'binding.verifier_healer.schema_authority',
        'verifier.platform.schema_authority',
        'healer.platform.schema_bootstrap',
        TRUE,
        'binding.verifier_healer.20260409',
        'decision.verifier_healer.bootstrap.20260409'
    ),
    (
        'binding.verifier_healer.receipt_provenance',
        'verifier.platform.receipt_provenance',
        'healer.platform.receipt_provenance_backfill',
        TRUE,
        'binding.verifier_healer.20260409',
        'decision.verifier_healer.bootstrap.20260409'
    ),
    (
        'binding.verifier_healer.memory_proof_links',
        'verifier.platform.memory_proof_links',
        'healer.platform.proof_backfill',
        TRUE,
        'binding.verifier_healer.20260409',
        'decision.verifier_healer.bootstrap.20260409'
    )
ON CONFLICT (binding_ref) DO UPDATE SET
    verifier_ref = EXCLUDED.verifier_ref,
    healer_ref = EXCLUDED.healer_ref,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
