-- Migration 218: DB authority for composable UI feature flows.
--
-- Surface actions and shell routes explain buttons and navigation. This layer
-- explains product features that compose UI, actions, tools/functions,
-- workflows, primitives, and visible states into one queryable contract for
-- future agents.

BEGIN;

CREATE TABLE IF NOT EXISTS ui_feature_flow_registry (
    feature_id TEXT PRIMARY KEY CHECK (btrim(feature_id) <> ''),
    label TEXT NOT NULL CHECK (btrim(label) <> ''),
    summary TEXT NOT NULL CHECK (btrim(summary) <> ''),
    primary_surface_name TEXT NOT NULL CHECK (btrim(primary_surface_name) <> ''),
    exposure_status TEXT NOT NULL CHECK (
        exposure_status IN ('ready', 'planned', 'hidden', 'deprecated')
    ),
    launch_action_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(launch_action_ids) = 'array'),
    backing_tool_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(backing_tool_refs) = 'array'),
    workflow_ref TEXT CHECK (workflow_ref IS NULL OR btrim(workflow_ref) <> ''),
    workflow_shape JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(workflow_shape) = 'object'),
    primitive_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(primitive_refs) = 'array'),
    stage_contracts JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(stage_contracts) = 'array'),
    required_inputs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(required_inputs) = 'array'),
    visible_states JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(visible_states) = 'array'),
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_refs) = 'array'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    display_order INTEGER NOT NULL DEFAULT 0,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ui_feature_flow_registry_surface_order_idx
    ON ui_feature_flow_registry (primary_surface_name, enabled, display_order, feature_id);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'ui_feature_flow_registry',
    'UI feature flow registry',
    'table',
    'DB-backed registry linking product features to UI surfaces, actions, tools, workflows, primitives, stages, and visible states.',
    '{"migration":"218_ui_feature_flow_registry.sql"}'::jsonb,
    '{"authority_domain_ref":"authority.surface_catalog"}'::jsonb
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
) VALUES (
    'table.public.ui_feature_flow_registry',
    'table',
    'ui_feature_flow_registry',
    'public',
    'authority.surface_catalog',
    'ui_feature_flow_registry',
    'active',
    'registry',
    'praxis.engine',
    'decision.ui_feature_flow_registry.composability.20260424',
    '{"migration":"218_ui_feature_flow_registry.sql"}'::jsonb
)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    schema_name = EXCLUDED.schema_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO ui_feature_flow_registry (
    feature_id,
    label,
    summary,
    primary_surface_name,
    exposure_status,
    launch_action_ids,
    backing_tool_refs,
    workflow_ref,
    workflow_shape,
    primitive_refs,
    stage_contracts,
    required_inputs,
    visible_states,
    source_refs,
    enabled,
    display_order,
    binding_revision,
    decision_ref
) VALUES (
    'custom-integration-builder',
    'Custom Integration Builder',
    'Visible workflow for taking an app name or domain, investigating API docs, evaluating integration feasibility, building a connector, reviewing it, registering it, and verifying it.',
    'build',
    'planned',
    '[]'::jsonb,
    '["tool:praxis_connector"]'::jsonb,
    'workflow.visual.moon.build_graph',
    '{
      "visual_authority": "Moon build_graph",
      "release_projection": "definition.execution_setup.phases",
      "runtime_tool": "praxis_connector",
      "current_backend_pipeline": ["discover API", "map objects", "build client", "review"],
      "ui_contract": "The user should see the workflow shape first; LLM/tool work is represented as visible stages, not hidden pre-work."
    }'::jsonb,
    '[
      "surface_catalog_registry.gather-research",
      "surface_catalog_registry.think-classify",
      "surface_catalog_registry.think-draft",
      "surface_catalog_registry.ctrl-validation",
      "surface_catalog_registry.act-invoke"
    ]'::jsonb,
    '[
      {
        "stage_id": "define",
        "label": "Define",
        "purpose": "Capture app name or app domain, optional docs URL, desired integration scope, and secret hint.",
        "llm_assisted": false,
        "expected_output": "connector_build_intent"
      },
      {
        "stage_id": "investigate",
        "label": "Investigate",
        "purpose": "Plan search, retrieve official API/auth documentation, and collect evidence.",
        "llm_assisted": true,
        "expected_output": "docs_evidence_bundle"
      },
      {
        "stage_id": "evaluate",
        "label": "Evaluate",
        "purpose": "Assess auth model, core objects, endpoint coverage, risks, and confidence.",
        "llm_assisted": true,
        "expected_output": "integration_feasibility_report"
      },
      {
        "stage_id": "build",
        "label": "Build",
        "purpose": "Generate connector client, manifest, and tests from the evaluated plan.",
        "llm_assisted": true,
        "expected_output": "connector_artifacts"
      },
      {
        "stage_id": "review",
        "label": "Review",
        "purpose": "Inspect generated artifacts and decide whether the connector can be registered.",
        "llm_assisted": true,
        "expected_output": "review_decision"
      },
      {
        "stage_id": "register_verify",
        "label": "Register + Verify",
        "purpose": "Promote the connector to a callable integration and run verification against the declared auth setup.",
        "llm_assisted": false,
        "expected_output": "registered_verified_connector"
      }
    ]'::jsonb,
    '[
      {"field":"app_name_or_domain","required":true,"kind":"text"},
      {"field":"auth_docs_url","required":false,"kind":"url"},
      {"field":"secret_env_var","required":false,"kind":"text"},
      {"field":"desired_capabilities","required":false,"kind":"text"}
    ]'::jsonb,
    '[
      "idle",
      "triaging",
      "searching_docs",
      "retrieving_docs",
      "evaluating_api",
      "building_connector",
      "reviewing",
      "needs_secret",
      "ready_to_register",
      "registered",
      "verified",
      "blocked"
    ]'::jsonb,
    '[
      "tool:praxis_connector",
      "Code&DBs/Workflow/surfaces/app/src/moon/MoonIntegrationsPanel.tsx",
      "Code&DBs/Workflow/surfaces/app/src/shared/buildGraphDefinition.ts",
      "Code&DBs/Workflow/surfaces/app/src/shared/types.ts"
    ]'::jsonb,
    TRUE,
    10,
    'binding.ui_feature_flow_registry.custom_integration_builder.20260424',
    'decision.ui_feature_flow_registry.custom_integration_builder.20260424'
)
ON CONFLICT (feature_id) DO UPDATE SET
    label = EXCLUDED.label,
    summary = EXCLUDED.summary,
    primary_surface_name = EXCLUDED.primary_surface_name,
    exposure_status = EXCLUDED.exposure_status,
    launch_action_ids = EXCLUDED.launch_action_ids,
    backing_tool_refs = EXCLUDED.backing_tool_refs,
    workflow_ref = EXCLUDED.workflow_ref,
    workflow_shape = EXCLUDED.workflow_shape,
    primitive_refs = EXCLUDED.primitive_refs,
    stage_contracts = EXCLUDED.stage_contracts,
    required_inputs = EXCLUDED.required_inputs,
    visible_states = EXCLUDED.visible_states,
    source_refs = EXCLUDED.source_refs,
    enabled = EXCLUDED.enabled,
    display_order = EXCLUDED.display_order,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
