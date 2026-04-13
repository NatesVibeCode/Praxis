-- Migration 016: App builder registries
-- Creates UI component, calculation, workflow, and app manifest registries
-- for the app builder system.

-- 1. UI components registry
CREATE TABLE IF NOT EXISTS registry_ui_components (
    id              text PRIMARY KEY,
    name            text UNIQUE NOT NULL,
    description     text NOT NULL,
    category        text NOT NULL,
    props_schema    jsonb NOT NULL DEFAULT '{}',
    emits_events    jsonb NOT NULL DEFAULT '[]',
    accepts_slots   boolean DEFAULT false,
    default_size    jsonb DEFAULT '{}',
    search_vector   tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED,
    created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registry_ui_components_search
    ON registry_ui_components USING gin(search_vector);

-- 2. Calculations registry
CREATE TABLE IF NOT EXISTS registry_calculations (
    id              text PRIMARY KEY,
    name            text UNIQUE NOT NULL,
    description     text NOT NULL,
    category        text NOT NULL,
    input_schema    jsonb NOT NULL DEFAULT '{}',
    output_schema   jsonb NOT NULL DEFAULT '{}',
    execution_type  text NOT NULL,
    resource_ref    text,
    search_vector   tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED,
    created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registry_calculations_search
    ON registry_calculations USING gin(search_vector);

-- 3. Workflows registry
CREATE TABLE IF NOT EXISTS registry_workflows (
    id              text PRIMARY KEY,
    name            text UNIQUE NOT NULL,
    description     text NOT NULL,
    category        text NOT NULL,
    trigger_type    text NOT NULL,
    input_schema    jsonb NOT NULL DEFAULT '{}',
    output_schema   jsonb NOT NULL DEFAULT '{}',
    steps           jsonb NOT NULL DEFAULT '[]',
    mcp_tool_refs   jsonb DEFAULT '[]',
    search_vector   tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED,
    created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registry_workflows_search
    ON registry_workflows USING gin(search_vector);

-- 4. App manifests
CREATE TABLE IF NOT EXISTS app_manifests (
    id                  text PRIMARY KEY,
    name                text NOT NULL,
    description         text DEFAULT '',
    created_by          text NOT NULL DEFAULT 'system',
    intent_history      jsonb DEFAULT '[]',
    manifest            jsonb NOT NULL DEFAULT '{}',
    version             integer NOT NULL DEFAULT 1,
    parent_manifest_id  text,
    status              text NOT NULL DEFAULT 'draft',
    search_vector       tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, ''))
    ) STORED,
    created_at          timestamptz DEFAULT now(),
    updated_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_app_manifests_search
    ON app_manifests USING gin(search_vector);

-- 5. App manifest history (append-only version history)
CREATE TABLE IF NOT EXISTS app_manifest_history (
    id                  text PRIMARY KEY,
    manifest_id         text NOT NULL REFERENCES app_manifests(id),
    version             integer NOT NULL,
    manifest_snapshot   jsonb NOT NULL,
    change_description  text DEFAULT '',
    changed_by          text NOT NULL DEFAULT 'system',
    created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_app_manifest_history_manifest_version
    ON app_manifest_history(manifest_id, version);

-- Seed: UI Components
INSERT INTO registry_ui_components (id, name, description, category, accepts_slots) VALUES
    ('drag_and_drop_canvas', 'Drag and Drop Canvas', 'Drag and drop interface with snapping', 'layout', true),
    ('text_box', 'Text Box', 'Text input field', 'input', false),
    ('table', 'Table', 'Data table with sorting and filtering', 'display', false),
    ('icon', 'Icon', 'Icon selector and display', 'display', false),
    ('box', 'Box', 'Container box with configurable borders and padding', 'layout', true),
    ('color_scheme', 'Color Scheme', 'Color scheme selector and applicator', 'display', false),
    ('chart', 'Chart', 'Chart component - bar, line, pie, area', 'display', false),
    ('text_format', 'Text Format', 'Rich text with formatting options', 'display', false),
    ('button', 'Button', 'Clickable action button with variants', 'action', false),
    ('dropdown', 'Dropdown', 'Dropdown selector with search and multi-select', 'input', false)
ON CONFLICT (id) DO NOTHING;

-- Seed: Calculations
INSERT INTO registry_calculations (id, name, description, category, execution_type) VALUES
    ('schema_charts_rollups', 'Schema Charts & Rollups', 'Pre-built schema charts and data rollups', 'aggregation', 'sql'),
    ('platform_datasets', 'Platform Datasets', 'Links to platform datasets and tables', 'dataset', 'platform_algo'),
    ('platform_algos', 'Platform Algorithms', 'Platform algorithms - scoring, ranking, classification', 'algorithm', 'platform_algo'),
    ('common_calcs', 'Common Calculations', 'Common calculations - averages, sums, percentages, growth rates', 'formula', 'sql')
ON CONFLICT (id) DO NOTHING;

-- Seed: Workflows
INSERT INTO registry_workflows (id, name, description, category, trigger_type) VALUES
    ('brainstorming', 'Brainstorming', 'Collaborative brainstorming with AI assistance', 'personal', 'manual'),
    ('research', 'Research', 'Research workflow - gather, analyze, synthesize', 'research', 'manual'),
    ('coding_workflow', 'Coding Workflow', 'Code generation, review, and testing pipeline', 'coding', 'manual'),
    ('personal_workflow', 'Personal Workflow', 'Personal task and habit workflows', 'personal', 'manual'),
    ('collateral_creation', 'Collateral Creation', 'Document and content creation pipeline', 'collaboration', 'manual'),
    ('hitl', 'Human in the Loop', 'Human-in-the-loop approval and review', 'collaboration', 'event'),
    ('schedule_based', 'Schedule Based', 'Recurring scheduled task execution', 'automation', 'scheduled'),
    ('triggers_and_actions', 'Triggers and Actions', 'Event-driven trigger-action automations', 'automation', 'webhook'),
    ('workflow_composition', 'Workflow Composition', 'Workflows that call and orchestrate other workflows', 'automation', 'manual'),
    ('data_splitting_logging', 'Data Splitting & Logging', 'Data partitioning, routing, and audit logging', 'automation', 'event'),
    ('handoff_mechanisms', 'Handoff Mechanisms', 'Task handoff between humans and AI agents', 'collaboration', 'event'),
    ('instructions_by_tool', 'Instructions by Tool', 'Tool-specific instruction sets and configurations', 'coding', 'manual')
ON CONFLICT (id) DO NOTHING;
