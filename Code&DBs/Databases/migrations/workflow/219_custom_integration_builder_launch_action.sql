-- Migration 219: bind the custom integration builder feature flow to a UI action.
--
-- The feature-flow registry explains the workflow shape. This migration adds
-- the launch action contract so agents can see where the human-facing entry
-- point belongs, even before the renderer exposes the button.

BEGIN;

INSERT INTO ui_surface_action_registry (
    action_id,
    surface_name,
    label,
    action_kind,
    effect,
    target_surface_name,
    http_method,
    endpoint_template,
    state_effect,
    confidence,
    source_refs,
    status,
    enabled,
    display_order,
    binding_revision,
    decision_ref
) VALUES (
    'build.custom-integration-builder',
    'build',
    'Build custom integration',
    'builder',
    'Launches the Custom Integration Builder feature flow: app/domain intake, docs investigation, evaluation, connector build, review, registration, and verification.',
    'build',
    NULL,
    NULL,
    'activeTabId=build, buildFeatureFlowId=custom-integration-builder, openDock=action',
    'db_backed_catalog',
    '[
      "ui_feature_flow_registry:custom-integration-builder",
      "tool:praxis_connector",
      "Code&DBs/Workflow/surfaces/app/src/moon/MoonIntegrationsPanel.tsx",
      "Code&DBs/Workflow/surfaces/app/src/shared/buildGraphDefinition.ts"
    ]'::jsonb,
    'hidden',
    TRUE,
    25,
    'binding.ui_surface_action_registry.custom_integration_builder.20260424',
    'decision.ui_surface_action_registry.custom_integration_builder.20260424'
)
ON CONFLICT (action_id) DO UPDATE SET
    surface_name = EXCLUDED.surface_name,
    label = EXCLUDED.label,
    action_kind = EXCLUDED.action_kind,
    effect = EXCLUDED.effect,
    target_surface_name = EXCLUDED.target_surface_name,
    http_method = EXCLUDED.http_method,
    endpoint_template = EXCLUDED.endpoint_template,
    state_effect = EXCLUDED.state_effect,
    confidence = EXCLUDED.confidence,
    source_refs = EXCLUDED.source_refs,
    status = EXCLUDED.status,
    enabled = EXCLUDED.enabled,
    display_order = EXCLUDED.display_order,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

UPDATE ui_feature_flow_registry
   SET launch_action_ids = '["build.custom-integration-builder"]'::jsonb,
       updated_at = now()
 WHERE feature_id = 'custom-integration-builder';

COMMIT;
