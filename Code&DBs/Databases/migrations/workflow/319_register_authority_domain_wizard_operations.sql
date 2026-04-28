-- Migration 319: Register authority-domain wizard operations.
--
-- These operations make authority-domain creation a first-class CQRS surface:
--   1. authority_domain_forge    -> preview / diagnose / produce payload
--   2. authority_domain_register -> create or update authority_domains
--
-- This prevents the common failure mode where agents attach new operations,
-- tables, or tools to a convenient-but-wrong authority domain just to satisfy
-- a foreign key.

BEGIN;

SELECT register_operation_atomic(
    p_operation_ref         := 'authority_domain.query.forge',
    p_operation_name        := 'authority_domain_forge',
    p_handler_ref           := 'runtime.operations.queries.authority_domain_forge.handle_query_authority_domain_forge',
    p_input_model_ref       := 'runtime.operations.queries.authority_domain_forge.QueryAuthorityDomainForge',
    p_authority_domain_ref  := 'authority.cqrs',
    p_authority_ref         := 'authority.cqrs',
    p_operation_kind        := 'query',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority-domain/forge',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_decision_ref          := 'architecture-policy::platform-architecture::cqrs_gateway_robust_determinism',
    p_binding_revision      := 'binding.operation_catalog_registry.authority_domain_forge.20260428',
    p_label                 := 'Authority Domain Forge',
    p_summary               := 'Preview authority domain ownership, attached operations, registry state, and the safe register payload before creating new authority.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'authority_domain.command.register',
    p_operation_name        := 'authority_domain_register',
    p_handler_ref           := 'runtime.operations.commands.authority_domain_register.handle_register_authority_domain',
    p_input_model_ref       := 'runtime.operations.commands.authority_domain_register.RegisterAuthorityDomainCommand',
    p_authority_domain_ref  := 'authority.cqrs',
    p_authority_ref         := 'authority.cqrs',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/authority-domain/register',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_type            := 'authority.domain.registered',
    p_event_required        := TRUE,
    p_decision_ref          := 'architecture-policy::platform-architecture::cqrs_gateway_robust_determinism',
    p_binding_revision      := 'binding.operation_catalog_registry.authority_domain_register.20260428',
    p_label                 := 'Authority Domain Register',
    p_summary               := 'Register or update an authority domain through a receipt-backed CQRS command before operations or tables are attached to it.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, operation_kind, authority_domain_ref
--     FROM operation_catalog_registry
--    WHERE operation_ref IN (
--      'authority_domain.query.forge',
--      'authority_domain.command.register'
--    );
