-- Migration 239: Primitive Catalog.
--
-- Recursive payoff of the semantic predicate work: primitives that build
-- the platform (authorities, engines, repositories, gateway wrappers) are
-- themselves declared as catalog rows.  One declaration per primitive
-- captures its shape, requirements, and consistency contract; downstream
-- engines (consistency scanner, future scaffolder) read this catalog to
-- detect drift between blueprint and reality.
--
-- The catalog itself is catalog-mounted.  Adding a new primitive becomes a
-- single row in primitive_catalog plus the conventional code; the
-- consistency scanner ensures the row and the code agree.
--
-- Primitive kinds:
--
--   * domain_authority — owns a Postgres table, exposes CRUD operations,
--                        mounts to operation_catalog_registry
--   * read_engine      — pure compute over catalog rows (e.g. invariant
--                        scanner, equivalence engine)
--   * write_engine     — propagates events / cascades from catalog
--                        declarations (e.g. propagation engine)
--   * gateway_handler  — Pydantic + handler wrapper around an engine for
--                        catalog mounting
--   * repository       — typed SQL primitives over a single domain table
--
-- The ``spec`` JSONB carries the primitive's blueprint: which module owns
-- it, which catalog rows it requires, which tests prove it, what
-- depends_on graph it sits in.  Different kinds use different spec keys.

BEGIN;

INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    decision_scope_kind,
    decision_scope_ref,
    effective_from,
    decided_at,
    created_at,
    updated_at
) VALUES (
    'operator_decision.architecture_policy.primitives.catalog_managed',
    'architecture-policy::primitives::catalog-managed-blueprints',
    'architecture_policy',
    'decided',
    'Platform primitives are declared in primitive_catalog and consistency-checked against code',
    'Authorities, engines, gateway wrappers, and repositories follow the same '
    'shape across the platform.  Declaring each one as a primitive_catalog row '
    'makes the inventory queryable, lets a consistency scanner detect '
    'blueprint-vs-code drift (orphaned authorities, ghost catalog rows, '
    'missing tests), and creates the seam for a future scaffolder that mints '
    'a new primitive from one row.  Recursive: the primitive_catalog primitive '
    'is itself in the catalog, demonstrating the contract.',
    'nate',
    'conversation.claude.cqrs_audit_thread.20260424',
    'authority_domain',
    'primitives',
    now(),
    now(),
    now(),
    now()
)
ON CONFLICT (decision_key) DO NOTHING;

-- ---------------------------------------------------------------------------
-- primitive_catalog table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS primitive_catalog (
    primitive_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    primitive_slug      TEXT NOT NULL UNIQUE,
    primitive_kind      TEXT NOT NULL CHECK (primitive_kind IN (
                            'domain_authority',
                            'read_engine',
                            'write_engine',
                            'gateway_handler',
                            'repository'
                        )),
    summary             TEXT NOT NULL,
    rationale           TEXT NOT NULL,
    spec                JSONB NOT NULL DEFAULT '{}'::jsonb,
    depends_on          JSONB NOT NULL DEFAULT '[]'::jsonb,
    decision_ref        TEXT NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS primitive_catalog_kind_idx
    ON primitive_catalog(primitive_kind)
    WHERE enabled = TRUE;

-- ---------------------------------------------------------------------------
-- Seed: the 5 primitives this thread built (and the catalog itself).
-- ---------------------------------------------------------------------------

INSERT INTO primitive_catalog (
    primitive_slug, primitive_kind, summary, rationale, spec, depends_on, decision_ref
) VALUES
(
    'semantic_predicate_catalog',
    'domain_authority',
    'Catalog of logic-level semantic predicates (invariants, equivalence, causal, retraction, temporal, trust)',
    'The third leg of the objects -> edges -> semantics propagation chain.  '
    'Engines below read its rows to drive validation, propagation, and dedupe '
    'across the platform.',
    jsonb_build_object(
        'authority_module', 'runtime.semantic_predicate_authority',
        'owns_table', 'semantic_predicate_catalog',
        'authority_domain_ref', 'authority.semantic_predicate_catalog',
        'event_contract_refs', jsonb_build_array('event_contract.semantic_predicate.recorded'),
        'operation_refs', jsonb_build_array(
            'semantic-predicate-record',
            'semantic-predicate-list',
            'semantic-predicate-get'
        ),
        'gateway_command_modules', jsonb_build_array(
            'runtime.operations.commands.semantic_predicates'
        ),
        'gateway_query_modules', jsonb_build_array(
            'runtime.operations.queries.semantic_predicates'
        ),
        'test_modules', jsonb_build_array(
            'tests.unit.test_semantic_predicate_authority'
        ),
        'migration_refs', jsonb_build_array('237_semantic_predicate_catalog.sql')
    ),
    '[]'::jsonb,
    'architecture-policy::primitives::catalog-managed-blueprints'
),
(
    'semantic_propagation_engine',
    'write_engine',
    'Causal propagation engine that fires declared side effects when matching events land',
    'Replaces hand-coded ``aemit_cache_invalidation`` calls scattered across '
    'manual + auto domain paths.  Domain authorities call '
    '``fire_causal_propagations`` after emitting an event; the engine reads '
    'matching causal predicates and dispatches their declared actions.',
    jsonb_build_object(
        'engine_module', 'runtime.semantic_propagation_engine',
        'reads_catalog', 'semantic_predicate_catalog',
        'predicate_kinds_consumed', jsonb_build_array('causal'),
        'action_handlers', jsonb_build_array('cache_invalidate'),
        'depends_on_modules', jsonb_build_array('runtime.cache_invalidation'),
        'wired_callsites', jsonb_build_array(
            'surfaces.api.operator_write.arecord_dataset_promotion',
            'runtime.dataset_candidate_subscriber._maybe_auto_promote'
        ),
        'test_modules', jsonb_build_array('tests.unit.test_semantic_propagation_engine')
    ),
    jsonb_build_array('semantic_predicate_catalog'),
    'architecture-policy::primitives::catalog-managed-blueprints'
),
(
    'semantic_invariant_scanner',
    'read_engine',
    'Static scanner that checks invariant predicates against the live source tree',
    'Reads enabled invariant predicates from the catalog and produces a '
    'structured findings list.  Detects forbidden-callsite regressions, '
    'allow-list drift, and other declared structural rules.  Engine itself '
    'does no DB writes; callers (CI test, gateway handler) decide how to '
    'react to findings.',
    jsonb_build_object(
        'engine_module', 'runtime.semantic_invariant_scanner',
        'reads_catalog', 'semantic_predicate_catalog',
        'predicate_kinds_consumed', jsonb_build_array('invariant'),
        'policy_keys_supported', jsonb_build_array(
            'forbidden_callsites_outside_command_bus',
            'forbidden_callsites'
        ),
        'test_modules', jsonb_build_array('tests.unit.test_semantic_invariant_scanner')
    ),
    jsonb_build_array('semantic_predicate_catalog'),
    'architecture-policy::primitives::catalog-managed-blueprints'
),
(
    'semantic_invariant_scan_handler',
    'gateway_handler',
    'Catalog-mounted query that runs the invariant scanner from REST/MCP/CLI',
    'Wraps semantic_invariant_scanner with a Pydantic command + handler '
    'signature so operators can invoke it on demand.  Single declaration in '
    'operation_catalog_registry surfaces the scanner everywhere.',
    jsonb_build_object(
        'handler_module', 'runtime.operations.queries.semantic_invariant_scan',
        'wraps_engine', 'semantic_invariant_scanner',
        'authority_domain_ref', 'authority.semantic_predicate_catalog',
        'operation_refs', jsonb_build_array('semantic-invariant-scan'),
        'http_method', 'GET',
        'http_path', '/api/semantic-invariants/scan',
        'test_modules', jsonb_build_array('tests.unit.test_semantic_invariant_scan_handler'),
        'migration_refs', jsonb_build_array('238_semantic_invariant_scan_operation.sql')
    ),
    jsonb_build_array('semantic_predicate_catalog', 'semantic_invariant_scanner'),
    'architecture-policy::primitives::catalog-managed-blueprints'
),
(
    'semantic_equivalence_engine',
    'read_engine',
    'Computes equivalence signatures from candidate payloads using catalog predicates',
    'Replaces per-domain dedupe signature schemes (bug fingerprint, dataset '
    'dedupe signature, etc.) with one engine that reads equivalence '
    'predicates and returns compare/fallback signatures + ranked matches.  '
    'Sync because callers (bug_tracker, dataset dedupe) are sync.',
    jsonb_build_object(
        'engine_module', 'runtime.semantic_equivalence_engine',
        'reads_catalog', 'semantic_predicate_catalog',
        'predicate_kinds_consumed', jsonb_build_array('equivalence'),
        'public_api', jsonb_build_array(
            'load_equivalence_predicates',
            'compute_equivalence_signatures',
            'rank_candidate_against_existing'
        ),
        'test_modules', jsonb_build_array('tests.unit.test_semantic_equivalence_engine')
    ),
    jsonb_build_array('semantic_predicate_catalog'),
    'architecture-policy::primitives::catalog-managed-blueprints'
),
(
    'primitive_catalog',
    'domain_authority',
    'Catalog of platform primitives (this catalog itself, recursively)',
    'Closes the meta loop.  The primitive catalog is itself a primitive, '
    'declared in its own table.  Future primitive scaffolders read these '
    'rows to mint new authorities/engines without hand-assembling each one.',
    jsonb_build_object(
        'authority_module', 'runtime.primitive_authority',
        'owns_table', 'primitive_catalog',
        'authority_domain_ref', 'authority.primitive_catalog',
        'operation_refs', jsonb_build_array(
            'primitive-record',
            'primitive-list',
            'primitive-get',
            'primitive-scan-consistency'
        ),
        'gateway_command_modules', jsonb_build_array(
            'runtime.operations.commands.primitives'
        ),
        'gateway_query_modules', jsonb_build_array(
            'runtime.operations.queries.primitives'
        ),
        'consistency_scanner_module', 'runtime.primitive_consistency_scanner',
        'test_modules', jsonb_build_array(
            'tests.unit.test_primitive_authority',
            'tests.unit.test_primitive_consistency_scanner'
        ),
        'migration_refs', jsonb_build_array('239_primitive_catalog.sql')
    ),
    '[]'::jsonb,
    'architecture-policy::primitives::catalog-managed-blueprints'
)
ON CONFLICT (primitive_slug) DO NOTHING;

-- ---------------------------------------------------------------------------
-- authority_domain + event_contract for the catalog itself.
-- ---------------------------------------------------------------------------

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    storage_target_ref,
    decision_ref,
    enabled
) VALUES (
    'authority.primitive_catalog',
    'praxis.engine',
    'stream.authority.primitive_catalog',
    'praxis.primary_postgres',
    'architecture-policy::primitives::catalog-managed-blueprints',
    TRUE
)
ON CONFLICT (authority_domain_ref) DO NOTHING;

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref
) VALUES (
    'event_contract.primitive.recorded',
    'primitive.recorded',
    'authority.primitive_catalog',
    'operation.output.default',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::primitives::catalog-managed-blueprints'
)
ON CONFLICT (authority_domain_ref, event_type) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'primitive_catalog',
    'Primitive catalog',
    'table',
    'DB-backed catalog of platform primitives and their consistency contracts.',
    jsonb_build_object('migration', '239_primitive_catalog.sql'),
    jsonb_build_object('authority_domain_ref', 'authority.primitive_catalog')
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
    'table.public.primitive_catalog',
    'table',
    'primitive_catalog',
    'public',
    'authority.primitive_catalog',
    'primitive_catalog',
    'active',
    'registry',
    'praxis.engine',
    'architecture-policy::primitives::catalog-managed-blueprints',
    jsonb_build_object('migration', '239_primitive_catalog.sql')
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

-- ---------------------------------------------------------------------------
-- authority_object_registry + data_dictionary_objects + operation_catalog_registry
-- for record / list / get / scan_consistency.
-- ---------------------------------------------------------------------------

INSERT INTO authority_object_registry (
    object_ref, object_kind, object_name,
    authority_domain_ref, data_dictionary_object_kind,
    write_model_kind, owner_ref, source_decision_ref, metadata
) VALUES
(
    'operation.primitive.record', 'command', 'primitive.record',
    'authority.primitive_catalog', 'command.primitive.record',
    'command_model', 'praxis.engine',
    'architecture-policy::primitives::catalog-managed-blueprints',
    '{}'::jsonb
),
(
    'operation.primitive.list', 'command', 'primitive.list',
    'authority.primitive_catalog', 'query.primitive.list',
    'read_model', 'praxis.engine',
    'architecture-policy::primitives::catalog-managed-blueprints',
    '{}'::jsonb
),
(
    'operation.primitive.get', 'command', 'primitive.get',
    'authority.primitive_catalog', 'query.primitive.get',
    'read_model', 'praxis.engine',
    'architecture-policy::primitives::catalog-managed-blueprints',
    '{}'::jsonb
),
(
    'operation.primitive.scan_consistency', 'command', 'primitive.scan_consistency',
    'authority.primitive_catalog', 'query.primitive.scan_consistency',
    'read_model', 'praxis.engine',
    'architecture-policy::primitives::catalog-managed-blueprints',
    '{}'::jsonb
)
ON CONFLICT (object_ref) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
(
    'command.primitive.record', 'Record Primitive', 'command',
    'Upsert a primitive blueprint row in the catalog.',
    jsonb_build_object('authority_domain_ref', 'authority.primitive_catalog'),
    '{}'::jsonb
),
(
    'query.primitive.list', 'List Primitives', 'command',
    'List primitives with optional filters by primitive_kind.',
    jsonb_build_object('authority_domain_ref', 'authority.primitive_catalog'),
    '{}'::jsonb
),
(
    'query.primitive.get', 'Get Primitive', 'command',
    'Fetch one primitive by primitive_slug.',
    jsonb_build_object('authority_domain_ref', 'authority.primitive_catalog'),
    '{}'::jsonb
),
(
    'query.primitive.scan_consistency', 'Scan Primitive Consistency', 'command',
    'Check declared module/operation/test paths against the live tree, surface drift.',
    jsonb_build_object('authority_domain_ref', 'authority.primitive_catalog'),
    '{}'::jsonb
)
ON CONFLICT (object_kind) DO NOTHING;

INSERT INTO operation_catalog_registry (
    operation_ref, operation_name, source_kind, operation_kind,
    http_method, http_path,
    input_model_ref, handler_ref,
    authority_ref, posture, idempotency_policy,
    binding_revision, decision_ref,
    authority_domain_ref, storage_target_ref,
    input_schema_ref, output_schema_ref,
    receipt_required, event_required, event_type
) VALUES
(
    'primitive-record', 'primitive.record',
    'operation_command', 'command',
    'POST', '/api/primitives',
    'runtime.operations.commands.primitives.RecordPrimitiveCommand',
    'runtime.operations.commands.primitives.handle_record_primitive',
    'authority.primitive_catalog', 'operate', 'idempotent',
    'binding.operation_catalog_registry.primitive_record.20260424',
    'architecture-policy::primitives::catalog-managed-blueprints',
    'authority.primitive_catalog', 'praxis.primary_postgres',
    'runtime.operations.commands.primitives.RecordPrimitiveCommand',
    'operation.output.default',
    TRUE, TRUE, 'primitive.recorded'
),
(
    'primitive-list', 'primitive.list',
    'operation_query', 'query',
    'GET', '/api/primitives',
    'runtime.operations.queries.primitives.ListPrimitivesQuery',
    'runtime.operations.queries.primitives.handle_list_primitives',
    'authority.primitive_catalog', 'observe', 'read_only',
    'binding.operation_catalog_registry.primitive_list.20260424',
    'architecture-policy::primitives::catalog-managed-blueprints',
    'authority.primitive_catalog', 'praxis.primary_postgres',
    'runtime.operations.queries.primitives.ListPrimitivesQuery',
    'operation.output.default',
    TRUE, FALSE, NULL
),
(
    'primitive-get', 'primitive.get',
    'operation_query', 'query',
    'GET', '/api/primitives/{primitive_slug}',
    'runtime.operations.queries.primitives.GetPrimitiveQuery',
    'runtime.operations.queries.primitives.handle_get_primitive',
    'authority.primitive_catalog', 'observe', 'read_only',
    'binding.operation_catalog_registry.primitive_get.20260424',
    'architecture-policy::primitives::catalog-managed-blueprints',
    'authority.primitive_catalog', 'praxis.primary_postgres',
    'runtime.operations.queries.primitives.GetPrimitiveQuery',
    'operation.output.default',
    TRUE, FALSE, NULL
),
(
    'primitive-scan-consistency', 'primitive.scan_consistency',
    'operation_query', 'query',
    'GET', '/api/primitives/scan-consistency',
    'runtime.operations.queries.primitives.ScanPrimitiveConsistencyQuery',
    'runtime.operations.queries.primitives.handle_scan_primitive_consistency',
    'authority.primitive_catalog', 'observe', 'read_only',
    'binding.operation_catalog_registry.primitive_scan_consistency.20260424',
    'architecture-policy::primitives::catalog-managed-blueprints',
    'authority.primitive_catalog', 'praxis.primary_postgres',
    'runtime.operations.queries.primitives.ScanPrimitiveConsistencyQuery',
    'operation.output.default',
    TRUE, FALSE, NULL
)
ON CONFLICT (operation_ref) DO NOTHING;

COMMIT;
