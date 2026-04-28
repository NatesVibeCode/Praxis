-- Migration 297: Register Policy Authority CQRS operations (P4.2.c).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- Migrations 295/296 shipped the schema + triggers. This migration
-- registers the two read paths through the CQRS catalog so agents and
-- operators can list active policies and (eventually) compliance
-- receipts via the gateway. Both are queries with idempotency_policy =
-- 'read_only' so identical inputs replay from receipt cache.

BEGIN;

-- Register the authority domain that owns the new operations. The FK
-- on operation_catalog_registry.authority_domain_ref → authority_domains
-- means this row must exist before register_operation_atomic runs.
INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.policy_definitions',
    'praxis.engine',
    'stream.policy_authority',
    'projection.policy_definitions',
    'praxis.primary_postgres',
    TRUE,
    'operator_decision.architecture_policy.policy_authority.operator_decisions_not_deletable'
)
ON CONFLICT (authority_domain_ref) DO NOTHING;

SELECT register_operation_atomic(
    p_operation_ref         := 'policy-list',
    p_operation_name        := 'policy.list',
    p_handler_ref           := 'runtime.operations.queries.policy_authority.handle_query_policy_list',
    p_input_model_ref       := 'runtime.operations.queries.policy_authority.QueryPolicyList',
    p_authority_domain_ref  := 'authority.policy_definitions',
    p_operation_kind        := 'query',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_label                 := 'Operation: policy.list',
    p_summary               := 'List active policy_definitions rows that bind operator_decisions to data-layer triggers. Filters: target_table, enforcement_kind, include_retired. Returns the policies that govern enforcement at the database layer — companion to architecture-policy standing orders surfaced through praxis_orient.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'compliance-list-receipts',
    p_operation_name        := 'compliance.list_receipts',
    p_handler_ref           := 'runtime.operations.queries.policy_authority.handle_query_compliance_receipts',
    p_input_model_ref       := 'runtime.operations.queries.policy_authority.QueryComplianceReceipts',
    p_authority_domain_ref  := 'authority.policy_definitions',
    p_operation_kind        := 'query',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_label                 := 'Operation: compliance.list_receipts',
    p_summary               := 'List authority_compliance_receipts rows recording policy enforcement outcomes (admit / reject). Filters: policy_id, target_table, outcome, correlation_id. Receipts are populated by the data-layer triggers; the reject-path autonomous-write pattern ships in a follow-up.'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_name, idempotency_policy
--     FROM operation_catalog_registry
--    WHERE operation_ref IN ('policy-list','compliance-list-receipts');
