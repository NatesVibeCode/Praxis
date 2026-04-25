-- Migration 237: Semantic Predicate Catalog.
--
-- Distinct from the existing ``semantic_predicates`` table, which is the
-- RDF-style predicate vocabulary referenced by ``semantic_assertions``
-- (predicates such as ``architecture_policy``, ``is_part_of``,
-- ``sourced_from_bug``).  This catalog is the *logic-level* predicates: rules
-- that constrain or imply behavior across the system.
--
-- Pairs with operation_catalog_registry as the third leg of the
-- objects -> edges -> semantics propagation chain.  An object_type catalog
-- declares what exists; an edge_type catalog declares how things relate; a
-- semantic_predicate_catalog declares WHAT THE RELATIONS MEAN and WHAT
-- MECHANICALLY FOLLOWS from a write.
--
-- Predicate kinds:
--   * invariant         - structural rule that every write must satisfy
--                         (e.g. capability endpoint path > query > body)
--   * equivalence       - "these two records refer to the same thing"
--                         (e.g. bug duplicate detection via failure_signature)
--   * causal            - "writing X mechanically implies firing Y"
--                         (e.g. dataset promotion -> cache invalidation)
--   * retraction        - "retracting X cascades to dependents Y"
--                         (e.g. assertion retracted -> promotion superseded)
--   * temporal_validity - "X is true between effective_from and effective_to"
--   * trust_weight      - "this assertion carries weight W from provenance P"
--
-- The catalog itself is catalog-mounted (record/list/get operations registered
-- below), so it auto-propagates to REST/MCP/CLI without bespoke surface code.
-- That is the whole point: the same lever that gave us object/edge propagation
-- now applies to the meaning layer.

BEGIN;

-- ---------------------------------------------------------------------------
-- Architecture-policy decision row that this migration enacts.
-- ---------------------------------------------------------------------------

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
    'operator_decision.architecture_policy.semantics.predicate_catalog_propagation',
    'architecture-policy::semantics::predicate-catalog-propagation',
    'architecture_policy',
    'decided',
    'Semantic predicates are DB-declared and auto-propagate to every surface',
    'Validators, equivalence checks, causal propagations (cache invalidation, '
    'event cascades), retraction cascades, temporal validity, and trust '
    'weighting all live as typed rows in semantic_predicate_catalog.  Each row '
    'declares applies_to_kind, predicate_kind, optional validator_ref, and a '
    'propagation_policy JSONB.  The catalog is itself catalog-mounted so '
    'record/list/get reach REST/MCP/CLI without bespoke wiring.  Effects: '
    'one declaration replaces N hand-written validators/subscribers; bypass '
    'detection becomes meaningful (did the write satisfy declared invariants '
    'and fire implied propagations?), not just structural; the meaning layer '
    'becomes queryable.',
    'nate',
    'conversation.claude.cqrs_audit_thread.20260424',
    'authority_domain',
    'semantics',
    now(),
    now(),
    now(),
    now()
)
ON CONFLICT (decision_key) DO NOTHING;

-- ---------------------------------------------------------------------------
-- semantic_predicate_catalog table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS semantic_predicate_catalog (
    predicate_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    predicate_slug      TEXT NOT NULL UNIQUE,
    predicate_kind      TEXT NOT NULL CHECK (predicate_kind IN (
                            'invariant',
                            'equivalence',
                            'causal',
                            'retraction',
                            'temporal_validity',
                            'trust_weight'
                        )),
    applies_to_kind     TEXT NOT NULL,
    applies_to_ref      TEXT,
    summary             TEXT NOT NULL,
    rationale           TEXT NOT NULL,
    validator_ref       TEXT,
    propagation_policy  JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision_ref        TEXT NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS semantic_predicate_catalog_kind_idx
    ON semantic_predicate_catalog(predicate_kind)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS semantic_predicate_catalog_applies_to_idx
    ON semantic_predicate_catalog(applies_to_kind, applies_to_ref)
    WHERE enabled = TRUE;

-- ---------------------------------------------------------------------------
-- Seed predicates that codify patterns established earlier in this thread.
-- ---------------------------------------------------------------------------

INSERT INTO semantic_predicate_catalog (
    predicate_slug, predicate_kind, applies_to_kind, applies_to_ref,
    summary, rationale, validator_ref, propagation_policy, decision_ref
) VALUES
(
    'dataset_promotion.invalidates_curated_projection_cache',
    'causal',
    'object_kind',
    'dataset_promotion',
    'Every dataset_promotion write fires a curated-projection cache invalidation',
    'Manual and auto promotions both update the curated projection.  Without a '
    'declared causal predicate, one path can hand-roll the side effect (manual) '
    'while another forgets it (auto subscriber pre-fix).  This predicate '
    'declares the side effect as data so the propagation engine can fire it '
    'wherever a promotion lands.',
    NULL,
    jsonb_build_object(
        'on_event', 'dataset_promotion_recorded',
        'fires', jsonb_build_array(
            jsonb_build_object(
                'action', 'cache_invalidate',
                'cache_kind_ref', 'CACHE_KIND_DATASET_CURATED_PROJECTION',
                'cache_key_template', '{specialist_target}:{dataset_family}:{split_tag|none}'
            )
        )
    ),
    'architecture-policy::semantics::predicate-catalog-propagation'
),
(
    'dataset_evidence_link.retracted_assertion_cascades_to_promotion',
    'retraction',
    'edge_kind',
    'dataset_candidate_evidence_link',
    'Retracted semantic_assertion or WONT_FIX bug cascades candidate to evidence_stale and supersedes active promotions',
    'When a semantic_assertion is retracted or a linked bug closes WONT_FIX, '
    'every dataset_raw_candidate referencing it must flip to evidence_stale '
    'and every active promotion referencing the candidate must be superseded '
    'by a tombstone.  Today this is hand-coded in dataset_staleness; declaring '
    'it as a retraction predicate lets the propagation engine drive the cascade.',
    NULL,
    jsonb_build_object(
        'retraction_signals', jsonb_build_array(
            jsonb_build_object('source_kind', 'semantic_assertion', 'condition', 'assertion_status=retracted'),
            jsonb_build_object('source_kind', 'bug', 'condition', 'status=WONT_FIX')
        ),
        'cascade', jsonb_build_array(
            jsonb_build_object('action', 'mark_candidate_evidence_stale'),
            jsonb_build_object('action', 'supersede_active_promotions', 'tombstone_kind', 'auto')
        )
    ),
    'architecture-policy::semantics::predicate-catalog-propagation'
),
(
    'bugs.duplicate_via_failure_signature',
    'equivalence',
    'object_kind',
    'bug',
    'Bugs with matching failure_signature are equivalent and merge into recurrence_count',
    'Bug deduplication today uses build_failure_signature() but the equivalence '
    'rule lives in code.  Declaring it as a predicate lets new domains add '
    'their own equivalence rules (provider_health alerts, friction patterns) '
    'without inventing a new dedupe pipeline each time.',
    'runtime.bug_tracker.build_failure_signature',
    jsonb_build_object(
        'compare_fields', jsonb_build_array('failure_signature'),
        'fallback_fields', jsonb_build_array('title_anchor'),
        'merge_policy', 'increment_recurrence_count'
    ),
    'architecture-policy::semantics::predicate-catalog-propagation'
),
(
    'rest_capability_endpoint.path_overrides_query_overrides_body',
    'invariant',
    'surface',
    'rest.capability_endpoint',
    'REST capability endpoints enforce path > query > body precedence',
    'Server-routed path values must never be shadowed by attacker-controlled '
    'body or query input.  Encoded as an invariant predicate so future '
    'capability mounters cannot reintroduce the BUG-B3315290 trust-boundary '
    'gap by accident.',
    NULL,
    jsonb_build_object(
        'precedence_order', jsonb_build_array('path_params', 'query_params', 'body'),
        'body_must_be_object', true,
        'enforced_at', jsonb_build_array('surfaces.api.rest._create_capability_endpoint')
    ),
    'architecture-policy::security::rest-capability-endpoint-precedence'
),
(
    'workflow_launch.flow_through_command_bus',
    'invariant',
    'operation_class',
    'workflow_launch_or_cancel',
    'In-runtime workflow launches and cancels must flow through control_commands',
    'Direct calls to runtime.workflow.unified.submit_workflow_inline / cancel_run / '
    'cancel_job from triggers, integrations, schedulers, maintenance, dependency '
    'chains, or CLI surfaces are forbidden.  Encoded as an invariant predicate '
    'so a structural scanner can mechanically flag any new bypass introduction.',
    NULL,
    jsonb_build_object(
        'forbidden_callsites_outside_command_bus', jsonb_build_array(
            'runtime.workflow.unified.submit_workflow_inline',
            'runtime.workflow.unified.cancel_run',
            'runtime.workflow.unified.cancel_job'
        ),
        'allowed_authorities', jsonb_build_array(
            'runtime.control_commands.submit_workflow_command',
            'runtime.control_commands.execute_control_intent'
        ),
        'scan_layers', jsonb_build_array('runtime', 'surfaces')
    ),
    'architecture-policy::cqrs::workflow-launches-through-command-bus'
),
(
    'domain_table.authored_by_one_layer',
    'invariant',
    'object_kind',
    'any_domain_table',
    'Every domain table must have writers in exactly one of runtime/ or surfaces/',
    'Cross-layer write splits duplicate invariant enforcement and break CQRS.  '
    'After the cross-layer audit on 2026-04-24 every domain table is single-'
    'layer.  This invariant locks the property so it stays load-bearing.',
    NULL,
    jsonb_build_object(
        'split_layers', jsonb_build_array('runtime', 'surfaces'),
        'scan_strategy', 'grep_INSERT_UPDATE_DELETE_per_table_then_partition_by_layer'
    ),
    'architecture-policy::table-authoring::no-cross-layer-write-split'
),
(
    'capability_endpoint.body_must_be_json_object',
    'invariant',
    'surface',
    'rest.capability_endpoint',
    'JSON body on capability endpoints must be an object, not a bare scalar or array',
    'Without this guard, a list or scalar body silently degrades to an empty '
    'command_data set and the request appears to succeed.  Encoded as a '
    'sibling to the precedence invariant so both arrive together when a '
    'future capability mounter is built.',
    NULL,
    jsonb_build_object(
        'reject_non_object_with_status', 400,
        'enforced_at', jsonb_build_array('surfaces.api.rest._create_capability_endpoint')
    ),
    'architecture-policy::security::rest-capability-endpoint-precedence'
)
ON CONFLICT (predicate_slug) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Register the authority domain that owns this catalog.  Operation rows and
-- event contract below both FK to it.
-- ---------------------------------------------------------------------------

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    storage_target_ref,
    decision_ref,
    enabled
) VALUES (
    'authority.semantic_predicate_catalog',
    'praxis.engine',
    'stream.authority.semantic_predicate_catalog',
    'praxis.primary_postgres',
    'architecture-policy::semantics::predicate-catalog-propagation',
    TRUE
)
ON CONFLICT (authority_domain_ref) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Register event contract for predicate catalog mutations.
-- ---------------------------------------------------------------------------

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
    'event_contract.semantic_predicate.recorded',
    'semantic_predicate.recorded',
    'authority.semantic_predicate_catalog',
    'operation.output.default',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::semantics::predicate-catalog-propagation'
)
ON CONFLICT (authority_domain_ref, event_type) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Register operations so the catalog itself is catalog-mounted (REST/MCP/CLI
-- auto-propagated).  Eats own dogfood: the predicate catalog is reachable
-- through the same surfaces it governs.
-- ---------------------------------------------------------------------------

INSERT INTO authority_object_registry (
    object_ref, object_kind, object_name,
    authority_domain_ref, data_dictionary_object_kind,
    write_model_kind, owner_ref, source_decision_ref, metadata
) VALUES
(
    'operation.semantic_predicate.record',
    'command',
    'semantic_predicate.record',
    'authority.semantic_predicate_catalog',
    'command.semantic_predicate.record',
    'command_model',
    'praxis.engine',
    'architecture-policy::semantics::predicate-catalog-propagation',
    '{}'::jsonb
),
(
    'operation.semantic_predicate.list',
    'command',
    'semantic_predicate.list',
    'authority.semantic_predicate_catalog',
    'query.semantic_predicate.list',
    'read_model',
    'praxis.engine',
    'architecture-policy::semantics::predicate-catalog-propagation',
    '{}'::jsonb
),
(
    'operation.semantic_predicate.get',
    'command',
    'semantic_predicate.get',
    'authority.semantic_predicate_catalog',
    'query.semantic_predicate.get',
    'read_model',
    'praxis.engine',
    'architecture-policy::semantics::predicate-catalog-propagation',
    '{}'::jsonb
)
ON CONFLICT (object_ref) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
(
    'command.semantic_predicate.record',
    'Record Semantic Predicate',
    'command',
    'Upsert a semantic predicate row in the catalog.',
    jsonb_build_object('authority_domain_ref', 'authority.semantic_predicate_catalog'),
    '{}'::jsonb
),
(
    'query.semantic_predicate.list',
    'List Semantic Predicates',
    'command',
    'List semantic predicates with optional filters.',
    jsonb_build_object('authority_domain_ref', 'authority.semantic_predicate_catalog'),
    '{}'::jsonb
),
(
    'query.semantic_predicate.get',
    'Get Semantic Predicate',
    'command',
    'Fetch one semantic predicate by predicate_slug.',
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
) VALUES
(
    'semantic-predicate-record',
    'semantic_predicate.record',
    'operation_command',
    'command',
    'POST',
    '/api/semantic-predicates',
    'runtime.operations.commands.semantic_predicates.RecordSemanticPredicateCommand',
    'runtime.operations.commands.semantic_predicates.handle_record_semantic_predicate',
    'authority.semantic_predicate_catalog',
    'operate',
    'idempotent',
    'binding.operation_catalog_registry.semantic_predicate_record.20260424',
    'architecture-policy::semantics::predicate-catalog-propagation',
    'authority.semantic_predicate_catalog',
    'praxis.primary_postgres',
    'runtime.operations.commands.semantic_predicates.RecordSemanticPredicateCommand',
    'operation.output.default',
    TRUE,
    TRUE,
    'semantic_predicate.recorded'
),
(
    'semantic-predicate-list',
    'semantic_predicate.list',
    'operation_query',
    'query',
    'GET',
    '/api/semantic-predicates',
    'runtime.operations.queries.semantic_predicates.ListSemanticPredicatesQuery',
    'runtime.operations.queries.semantic_predicates.handle_list_semantic_predicates',
    'authority.semantic_predicate_catalog',
    'observe',
    'read_only',
    'binding.operation_catalog_registry.semantic_predicate_list.20260424',
    'architecture-policy::semantics::predicate-catalog-propagation',
    'authority.semantic_predicate_catalog',
    'praxis.primary_postgres',
    'runtime.operations.queries.semantic_predicates.ListSemanticPredicatesQuery',
    'operation.output.default',
    TRUE,
    FALSE,
    NULL
),
(
    'semantic-predicate-get',
    'semantic_predicate.get',
    'operation_query',
    'query',
    'GET',
    '/api/semantic-predicates/{predicate_slug}',
    'runtime.operations.queries.semantic_predicates.GetSemanticPredicateQuery',
    'runtime.operations.queries.semantic_predicates.handle_get_semantic_predicate',
    'authority.semantic_predicate_catalog',
    'observe',
    'read_only',
    'binding.operation_catalog_registry.semantic_predicate_get.20260424',
    'architecture-policy::semantics::predicate-catalog-propagation',
    'authority.semantic_predicate_catalog',
    'praxis.primary_postgres',
    'runtime.operations.queries.semantic_predicates.GetSemanticPredicateQuery',
    'operation.output.default',
    TRUE,
    FALSE,
    NULL
)
ON CONFLICT (operation_ref) DO NOTHING;

COMMIT;
