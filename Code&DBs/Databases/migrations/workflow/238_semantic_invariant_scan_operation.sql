-- Migration 238: Register semantic_invariant.scan as a catalog-mounted query.
--
-- The semantic_invariant_scanner already exists in runtime; this migration
-- gives it a typed CQRS surface so it auto-propagates to REST/MCP/CLI through
-- the operation_catalog_gateway.  Calling::
--
--     praxis workflow tools call semantic_invariant.scan --input-json '{}' --yes
--     GET /api/semantic-invariants/scan
--
-- both run the live invariant scan and return structured findings without any
-- bespoke surface code.  Same lever as the predicate catalog itself: declare
-- the operation once, get all three surfaces.

BEGIN;

INSERT INTO authority_object_registry (
    object_ref, object_kind, object_name,
    authority_domain_ref, data_dictionary_object_kind,
    write_model_kind, owner_ref, source_decision_ref, metadata
) VALUES (
    'operation.semantic_invariant.scan',
    'command',
    'semantic_invariant.scan',
    'authority.semantic_predicate_catalog',
    'query.semantic_invariant.scan',
    'read_model',
    'praxis.engine',
    'architecture-policy::semantics::predicate-catalog-propagation',
    '{}'::jsonb
)
ON CONFLICT (object_ref) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'query.semantic_invariant.scan',
    'Scan Semantic Invariants',
    'command',
    'Run the invariant scanner against enabled invariant predicates and return findings.',
    jsonb_build_object('authority_domain_ref', 'authority.semantic_predicate_catalog'),
    '{}'::jsonb
)
ON CONFLICT (object_kind) DO NOTHING;

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
    posture,
    idempotency_policy,
    binding_revision,
    decision_ref,
    authority_domain_ref,
    storage_target_ref,
    input_schema_ref,
    output_schema_ref,
    receipt_required,
    event_required,
    event_type
) VALUES (
    'semantic-invariant-scan',
    'semantic_invariant.scan',
    'operation_query',
    'query',
    'GET',
    '/api/semantic-invariants/scan',
    'runtime.operations.queries.semantic_invariant_scan.ScanSemanticInvariantsQuery',
    'runtime.operations.queries.semantic_invariant_scan.handle_scan_semantic_invariants',
    'authority.semantic_predicate_catalog',
    'observe',
    'read_only',
    'binding.operation_catalog_registry.semantic_invariant_scan.20260424',
    'architecture-policy::semantics::predicate-catalog-propagation',
    'authority.semantic_predicate_catalog',
    'praxis.primary_postgres',
    'runtime.operations.queries.semantic_invariant_scan.ScanSemanticInvariantsQuery',
    'operation.output.default',
    TRUE,
    FALSE,
    NULL
)
ON CONFLICT (operation_ref) DO NOTHING;

COMMIT;
