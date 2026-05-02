BEGIN;

ALTER TABLE agent_registry
    ADD COLUMN IF NOT EXISTS system_prompt_template text,
    ADD COLUMN IF NOT EXISTS description text,
    ADD COLUMN IF NOT EXISTS icon_hint text,
    ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'visible' CHECK (visibility IN ('visible', 'hidden', 'archived')),
    ADD COLUMN IF NOT EXISTS builder_category text NOT NULL DEFAULT 'custom' CHECK (builder_category IN ('builtin', 'custom')),
    ADD COLUMN IF NOT EXISTS model_preference text,
    ADD COLUMN IF NOT EXISTS reasoning_effort text CHECK (reasoning_effort IN ('low', 'medium', 'high'));

INSERT INTO agent_registry (
    agent_principal_ref, title, description, icon_hint, visibility, builder_category, system_prompt_template
) VALUES
    ('agent.builtin.build', 'Build Agent', 'General-purpose builder and software engineer', 'build', 'visible', 'builtin', 'You are the primary build engineer...'),
    ('agent.builtin.review', 'Review Agent', 'Validation, audit, and structural review', 'review', 'visible', 'builtin', 'You are a code reviewer and validator...'),
    ('agent.builtin.research', 'Research Agent', 'Investigation, evidence gathering, codebase traversal', 'research', 'visible', 'builtin', 'You are an investigator tasked with finding evidence...'),
    ('agent.builtin.reasoning', 'Reasoning Agent', 'Synthesis, reconciliation, and deep thinking', 'reasoning', 'visible', 'builtin', 'You are tasked with deep logical reasoning and synthesis...')
ON CONFLICT (agent_principal_ref) DO UPDATE SET
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    icon_hint = EXCLUDED.icon_hint,
    visibility = EXCLUDED.visibility,
    builder_category = EXCLUDED.builder_category,
    system_prompt_template = COALESCE(agent_registry.system_prompt_template, EXCLUDED.system_prompt_template);

COMMIT;
