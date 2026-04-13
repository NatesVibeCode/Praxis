BEGIN;

CREATE TABLE IF NOT EXISTS verification_registry (
    verification_ref TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    executor_kind TEXT NOT NULL CHECK (executor_kind IN ('argv')),
    argv_template JSONB NOT NULL,
    template_inputs JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_timeout_seconds INTEGER NOT NULL DEFAULT 60 CHECK (default_timeout_seconds > 0),
    workdir_policy TEXT NOT NULL DEFAULT 'job' CHECK (workdir_policy IN ('job')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT verification_registry_argv_template_array_check
        CHECK (jsonb_typeof(argv_template) = 'array'),
    CONSTRAINT verification_registry_template_inputs_array_check
        CHECK (jsonb_typeof(template_inputs) = 'array')
);

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
) VALUES
    (
        'verification.python.py_compile',
        'Python Bytecode Compile',
        'Compile a Python file with py_compile to catch syntax errors.',
        'argv',
        '["python3", "-m", "py_compile", "{path}"]'::jsonb,
        '["path"]'::jsonb,
        60,
        'job',
        TRUE,
        'decision.verification_registry.bootstrap.20260408'
    ),
    (
        'verification.python.pytest_file',
        'Pytest File',
        'Run pytest against a specific Python test file.',
        'argv',
        '["python3", "-m", "pytest", "-xvs", "{path}"]'::jsonb,
        '["path"]'::jsonb,
        120,
        'job',
        TRUE,
        'decision.verification_registry.bootstrap.20260408'
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

COMMIT;
