BEGIN;

INSERT INTO verification_registry (
    verification_ref,
    display_name,
    description,
    executor_kind,
    argv_template,
    template_inputs,
    default_timeout_seconds,
    workdir_policy,
    enabled,
    decision_ref
) VALUES (
    'verification.javascript.vitest_file',
    'Vitest File',
    'Run the app npm/Vitest file target through verification_registry authority.',
    'argv',
    '["npm", "test", "--", "{path}"]'::jsonb,
    '["path"]'::jsonb,
    180,
    'job',
    TRUE,
    'decision.verification_registry.javascript_vitest.20260423'
)
ON CONFLICT (verification_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    executor_kind = EXCLUDED.executor_kind,
    argv_template = EXCLUDED.argv_template,
    template_inputs = EXCLUDED.template_inputs,
    default_timeout_seconds = EXCLUDED.default_timeout_seconds,
    workdir_policy = EXCLUDED.workdir_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

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
) VALUES (
    'verifier.job.javascript.vitest_file',
    'Job Vitest File',
    'Run a focused app Vitest test file through verification_registry authority.',
    'verification_ref',
    'verification.javascript.vitest_file',
    NULL,
    '{}'::jsonb,
    TRUE,
    'decision.verifier_registry.javascript_vitest.20260423'
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

COMMIT;
