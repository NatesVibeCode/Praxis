-- Migration 351: Register the three audit-lens query operations in the
-- CQRS catalog.
--
-- Anchor decision:
--   architecture-policy::policy-authority::receipts-immutable
--   (operator_decisions row, registered in migration 296)
--
-- Why this exists
--   Migration 347 added immutability teeth to authority_operation_receipts
--   and a transport_kind column. This migration ships the operator-
--   visible lenses over those tables:
--
--     - search.authority_receipts     — row-level audit search over
--       authority_operation_receipts (gateway dispatch ledger)
--     - search.compliance_receipts    — policy-enforcement audit search
--       over authority_compliance_receipts
--     - audit.summary                 — aggregate trailing-window lens
--       over both ledger tables
--
--   These were forged through praxis_operation_forge per
--   architecture-policy::agent-behavior::cqrs-wizard-before-cqrs-edits.
--   The forge returned ok_to_register=true for each. The wizard's
--   companion tool praxis_register_operation hits an AmbiguousFunctionError
--   on register_operation_atomic (BUG-8DC8A3BA) so this migration uses
--   the same hand-rolled three-table pattern as migration 278 — that
--   pattern is the wizard's documented alternative ("praxis_register_operation
--   OR a numbered migration").
--
-- Idempotency
--   Every INSERT uses ON CONFLICT DO UPDATE on the natural key so re-
--   applying the migration is a no-op.

BEGIN;

-- =====================================================================
-- 1. data_dictionary_objects entries
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('operation.search.authority_receipts',
     'Operation: search.authority_receipts',
     'query',
     'Audit-lens search over authority_operation_receipts (gateway dispatch ledger). Supports extras for transport_kind, execution_status, operation_kind, operation_name, caller_ref, since_hours.',
     '{"source":"migration.351","authority":"runtime.operations.queries.search"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_authority_receipts_search"}'::jsonb),

    ('operation.search.compliance_receipts',
     'Operation: search.compliance_receipts',
     'query',
     'Policy-enforcement audit search over authority_compliance_receipts. Supports extras for target_table, outcome, operation, policy_id, since_hours.',
     '{"source":"migration.351","authority":"runtime.operations.queries.search"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_compliance_receipts_search"}'::jsonb),

    ('operation.audit.summary',
     'Operation: audit.summary',
     'query',
     'Aggregate audit lens: trailing-window totals + per-transport / per-status / per-policy buckets across authority_operation_receipts and authority_compliance_receipts.',
     '{"source":"migration.351","authority":"runtime.operations.queries.audit"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.audit.handle_audit_summary"}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- 2. authority_object_registry entries
-- =====================================================================
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
) VALUES
    ('operation.search.authority_receipts',
     'query',
     'search.authority_receipts',
     NULL,
     'authority.workflow_runs',
     'operation.search.authority_receipts',
     'active',
     'read_model',
     'praxis.engine',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     '{"handler_ref":"runtime.operations.queries.search.handle_authority_receipts_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.compliance_receipts',
     'query',
     'search.compliance_receipts',
     NULL,
     'authority.workflow_runs',
     'operation.search.compliance_receipts',
     'active',
     'read_model',
     'praxis.engine',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     '{"handler_ref":"runtime.operations.queries.search.handle_compliance_receipts_search","source_kind":"operation_query"}'::jsonb),

    ('operation.audit.summary',
     'query',
     'audit.summary',
     NULL,
     'authority.workflow_runs',
     'operation.audit.summary',
     'active',
     'read_model',
     'praxis.engine',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     '{"handler_ref":"runtime.operations.queries.audit.handle_audit_summary","source_kind":"operation_query"}'::jsonb)

ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- 3. operation_catalog_registry entries
-- =====================================================================
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
    authority_domain_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref,
    input_schema_ref,
    output_schema_ref,
    storage_target_ref,
    receipt_required,
    event_required
) VALUES
    ('search-authority-receipts',
     'search.authority_receipts',
     'operation_query',
     'query',
     'POST', '/api/search/authority-receipts',
     'runtime.operations.queries.search.AuthorityReceiptsSearchQuery',
     'runtime.operations.queries.search.handle_authority_receipts_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'read_only', TRUE,
     'binding.operation_catalog_registry.search_authority_receipts.20260429',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     'runtime.operations.queries.search.AuthorityReceiptsSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-compliance-receipts',
     'search.compliance_receipts',
     'operation_query',
     'query',
     'POST', '/api/search/compliance-receipts',
     'runtime.operations.queries.search.ComplianceReceiptsSearchQuery',
     'runtime.operations.queries.search.handle_compliance_receipts_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'read_only', TRUE,
     'binding.operation_catalog_registry.search_compliance_receipts.20260429',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     'runtime.operations.queries.search.ComplianceReceiptsSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('audit-summary',
     'audit.summary',
     'operation_query',
     'query',
     'POST', '/api/audit/summary',
     'runtime.operations.queries.audit.AuditSummaryQuery',
     'runtime.operations.queries.audit.handle_audit_summary',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'read_only', TRUE,
     'binding.operation_catalog_registry.audit_summary.20260429',
     'decision.architecture_policy.policy_authority.receipts_immutable.20260427',
     'runtime.operations.queries.audit.AuditSummaryQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE)

ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    updated_at = now();


COMMIT;

-- Verification (run manually after apply):
--   SELECT operation_name, operation_kind, posture, idempotency_policy, enabled
--     FROM operation_catalog_registry
--    WHERE operation_name IN ('search.authority_receipts', 'search.compliance_receipts', 'audit.summary')
--    ORDER BY operation_name;
--
--   praxis workflow tools call praxis_audit_summary --input-json '{"since_hours": 24}'
--   praxis workflow tools call praxis_search_authority_receipts --input-json '{"query": "search.federated", "limit": 5}'
--   praxis workflow tools call praxis_search_compliance_receipts --input-json '{"query": "authority", "limit": 5}'
