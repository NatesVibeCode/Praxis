-- Add scope and contract defaults to task_type_profiles for the simplified
-- authoring spec format.  These columns let the engine infer scope and
-- fallback contracts from the task_type when the spec author omits them.

ALTER TABLE task_type_profiles
    ADD COLUMN IF NOT EXISTS default_scope_read     JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS default_scope_write    JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS default_authoring_contract  JSONB,
    ADD COLUMN IF NOT EXISTS default_acceptance_contract JSONB;

-- Seed scope defaults for each task type.

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","lib/","tests/"]'::jsonb,
    default_scope_write = '["src/","tests/"]'::jsonb
WHERE task_type = 'code_generation';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","lib/"]'::jsonb,
    default_scope_write = '["src/"]'::jsonb
WHERE task_type = 'code_edit';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","lib/","tests/"]'::jsonb,
    default_scope_write = '[]'::jsonb
WHERE task_type = 'code_review';

UPDATE task_type_profiles SET
    default_scope_read  = '[]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'research';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","artifacts/"]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'analysis';

UPDATE task_type_profiles SET
    default_scope_read  = '[]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'creative';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","lib/","tests/","logs/"]'::jsonb,
    default_scope_write = '["src/"]'::jsonb
WHERE task_type = 'debug';

UPDATE task_type_profiles SET
    default_scope_read  = '[]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'extraction';

UPDATE task_type_profiles SET
    default_scope_read  = '[]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'ocr';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","docs/"]'::jsonb,
    default_scope_write = '["docs/","artifacts/"]'::jsonb
WHERE task_type = 'architecture';

UPDATE task_type_profiles SET
    default_scope_read  = '["src/","lib/","tests/"]'::jsonb,
    default_scope_write = '[]'::jsonb
WHERE task_type = 'review';

UPDATE task_type_profiles SET
    default_scope_read  = '[]'::jsonb,
    default_scope_write = '["artifacts/"]'::jsonb
WHERE task_type = 'brainstorm';
