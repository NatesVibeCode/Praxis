BEGIN;

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
) VALUES (
    'operator-provider-onboarding',
    'operator.provider_onboarding',
    'operation_command',
    'command',
    'POST',
    '/api/operator/provider-onboarding',
    'runtime.operations.commands.provider_onboarding.ProviderOnboardingCommand',
    'runtime.operations.commands.provider_onboarding.handle_provider_onboarding',
    'authority.provider_onboarding',
    NULL,
    NULL,
    NULL,
    TRUE,
    'binding.operation_catalog_registry.provider_onboarding.20260416',
    'decision.operation_catalog_registry.provider_onboarding.20260416'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
