-- Migration 229: Experience-template authority + type lattice seed.
--
-- Lands the first shape/template/framework/intent wedge for the surface
-- composition CQRS direction. Uses the existing outcome-graph substrate
-- (memory_entities + memory_edges) so templates/shapes/pill-types are
-- first-class vertices with typed edges — satisfying the standing order
-- architecture-policy::source-linked-outcome-graph::outcome-graph-links-
-- code-ui-bugs-roadmap-integrations-and-external-memory.
--
-- Seeds:
--   * 1 layout_shape: shape.split (the only shape the first template uses)
--   * 3 pill_type vertices (Document base + Invoice + PurchaseOrder subtypes)
--   * 12 pill_field vertices (shared + specialized fields)
--   * 2 experience_templates (generic doc.compare.split + specialized
--     invoice.with_po.review) — exercises lattice-depth specificity ranking
--   * 1 surface_framework.document_review (includes both templates)
--   * 1 intent.invoice_approval (ranks both templates with fallback weight)
--   * Typed edges: subtype_of, has_field, uses_shape, consumes, includes,
--     targets_template
--   * authority_projection_registry + authority_projection_contracts row
--     for projection.surface.legal_templates (CQRS read path)
--
-- Anchored by:
--   architecture-policy::surface-catalog::surface-composition-cqrs-direction
--   architecture-policy::platform-architecture::legal-is-computable-not-permitted
--   architecture-policy::surface-catalog::type-lattice-and-risk-mitigation-is-authority-reuse

BEGIN;

-- =========================================================================
-- 0. Widen memory_entities + memory_edges allowlists for the new surface
--    vertices and typed edges (outcome-graph extension).
-- =========================================================================
ALTER TABLE memory_entities DROP CONSTRAINT IF EXISTS ck_memory_entities_type;
ALTER TABLE memory_entities ADD CONSTRAINT ck_memory_entities_type CHECK (
    entity_type = ANY (ARRAY[
        'person','topic','decision','preference','constraint','fact','task',
        'document','workstream','action','lesson','pattern','module','table',
        'code_unit','tool','metric','roadmap_item','bug','workflow',
        'workflow_build_intent','functional_area','repo_path','operator_decision',
        'issue','workflow_chain','workflow_chain_wave','workflow_chain_wave_run',
        'workflow_job_submission','workflow_run','verification_run','healing_run',
        'receipt','provider','authority_domain','cutover_gate','workflow_class',
        'schedule_definition',
        -- Added by migration 229 for surface-composition typed graph:
        'layout_shape','pill_type','pill_field','experience_template',
        'surface_framework','intent'
    ]::text[])
);

ALTER TABLE memory_edges DROP CONSTRAINT IF EXISTS ck_memory_edges_relation_type;
ALTER TABLE memory_edges ADD CONSTRAINT ck_memory_edges_relation_type CHECK (
    relation_type = ANY (ARRAY[
        'depends_on','constrains','implements','supersedes','related_to',
        'derived_from','caused_by','correlates_with','produced','triggered',
        'covers','verified_by','recorded_in','regressed_from','semantic_neighbor',
        'parent_of','resolves_bug','implements_build','belongs_to_area',
        -- Added by migration 229 for surface-composition typed graph:
        'subtype_of','has_field','uses_shape','consumes','consumes:right',
        'consumes:action_rail','includes','targets_template','preferred_theme',
        'uses_theme','declares','emits'
    ]::text[])
);

-- =========================================================================
-- 1. memory_entities: the typed vertices
-- =========================================================================

INSERT INTO memory_entities (id, entity_type, name, content, metadata, source, confidence, archived, created_at, updated_at) VALUES
    -- Layout shape vocabulary ---------------------------------------------
    (
        'shape.split',
        'layout_shape',
        'Split',
        'Two-panel split layout; slots left + right, optional action rail.',
        jsonb_build_object(
            'quadrant_footprint', jsonb_build_object(
                'grid', '4x4',
                'slots', jsonb_build_object(
                    'left', jsonb_build_object('cells', jsonb_build_array('A1','A2','B1','B2'), 'required', true),
                    'right', jsonb_build_object('cells', jsonb_build_array('A3','A4','B3','B4'), 'required', true),
                    'action_rail', jsonb_build_object('cells', jsonb_build_array('C1','C2','C3','C4'), 'required', false)
                )
            ),
            'migration', '229_surface_template_type_lattice.sql'
        ),
        'migration.229', 1.0, false, now(), now()
    ),

    -- Pill types (Document lattice) ---------------------------------------
    (
        'pill_type.document',
        'pill_type',
        'Document',
        'Base document type: any addressable document artifact with title, issuer, and monetary context.',
        jsonb_build_object('lattice_root', true, 'migration', '229_surface_template_type_lattice.sql'),
        'migration.229', 1.0, false, now(), now()
    ),
    (
        'pill_type.document.invoice',
        'pill_type',
        'Invoice',
        'Document subtype representing a billable invoice with payment terms.',
        jsonb_build_object('parent_type', 'pill_type.document', 'migration', '229_surface_template_type_lattice.sql'),
        'migration.229', 1.0, false, now(), now()
    ),
    (
        'pill_type.document.purchase_order',
        'pill_type',
        'PurchaseOrder',
        'Document subtype representing an authorized purchase order to a vendor.',
        jsonb_build_object('parent_type', 'pill_type.document', 'migration', '229_surface_template_type_lattice.sql'),
        'migration.229', 1.0, false, now(), now()
    ),

    -- Pill fields (shared on Document) ------------------------------------
    ('pill_field.title', 'pill_field', 'title', 'Shared: document title', jsonb_build_object('value_type', 'text'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.issuer', 'pill_field', 'issuer', 'Shared: issuing party reference', jsonb_build_object('value_type', 'text'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.total_amount', 'pill_field', 'total_amount', 'Shared: document monetary total', jsonb_build_object('value_type', 'decimal'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.issued_at', 'pill_field', 'issued_at', 'Shared: document issue date', jsonb_build_object('value_type', 'timestamp'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.currency', 'pill_field', 'currency', 'Shared: ISO-4217 currency code', jsonb_build_object('value_type', 'text'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.attachments', 'pill_field', 'attachments', 'Shared: supporting file refs', jsonb_build_object('value_type', 'ref_list'), 'migration.229', 1.0, false, now(), now()),
    -- Invoice-specialized fields ------------------------------------------
    ('pill_field.invoice_number', 'pill_field', 'invoice_number', 'Invoice-specific: issuer-assigned invoice identifier', jsonb_build_object('value_type', 'text', 'subtype_owner', 'pill_type.document.invoice'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.payment_terms', 'pill_field', 'payment_terms', 'Invoice-specific: payment-terms clause (e.g. Net 30)', jsonb_build_object('value_type', 'text', 'subtype_owner', 'pill_type.document.invoice'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.due_date', 'pill_field', 'due_date', 'Invoice-specific: payment due date', jsonb_build_object('value_type', 'timestamp', 'subtype_owner', 'pill_type.document.invoice'), 'migration.229', 1.0, false, now(), now()),
    -- PurchaseOrder-specialized fields ------------------------------------
    ('pill_field.po_number', 'pill_field', 'po_number', 'PurchaseOrder-specific: buyer-assigned PO number', jsonb_build_object('value_type', 'text', 'subtype_owner', 'pill_type.document.purchase_order'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.vendor_ref', 'pill_field', 'vendor_ref', 'PurchaseOrder-specific: vendor party reference', jsonb_build_object('value_type', 'text', 'subtype_owner', 'pill_type.document.purchase_order'), 'migration.229', 1.0, false, now(), now()),
    ('pill_field.expected_delivery', 'pill_field', 'expected_delivery', 'PurchaseOrder-specific: expected delivery date', jsonb_build_object('value_type', 'timestamp', 'subtype_owner', 'pill_type.document.purchase_order'), 'migration.229', 1.0, false, now(), now()),

    -- Experience templates ------------------------------------------------
    (
        'template.doc.compare.split',
        'experience_template',
        'Document comparison (generic)',
        'Generic two-document comparison in a split layout. Matches any two Document-typed pills.',
        jsonb_build_object(
            'slot_order', jsonb_build_array('left', 'right'),
            'slot_type_refs', jsonb_build_object(
                'left', 'pill_type.document',
                'right', 'pill_type.document'
            ),
            'render_hint', jsonb_build_object(
                'left_module', 'markdown',
                'right_module', 'markdown',
                'theme_ref', 'theme.praxis.default'
            ),
            'migration', '229_surface_template_type_lattice.sql'
        ),
        'migration.229', 1.0, false, now(), now()
    ),
    (
        'template.invoice.with_po.review',
        'experience_template',
        'Invoice review with PO',
        'Specialized invoice approval layout: invoice on left, purchase order on right, approval rail below.',
        jsonb_build_object(
            'slot_order', jsonb_build_array('left', 'right', 'action_rail'),
            'slot_type_refs', jsonb_build_object(
                'left', 'pill_type.document.invoice',
                'right', 'pill_type.document.purchase_order',
                'action_rail', 'action.invoice_approval'
            ),
            'render_hint', jsonb_build_object(
                'left_module', 'markdown',
                'right_module', 'markdown',
                'action_rail_module', 'button-row',
                'theme_ref', 'theme.praxis.default'
            ),
            'fallback_template_ref', 'template.doc.compare.split',
            'migration', '229_surface_template_type_lattice.sql'
        ),
        'migration.229', 1.0, false, now(), now()
    ),

    -- Surface framework ---------------------------------------------------
    (
        'framework.document_review',
        'surface_framework',
        'Document Review',
        'Framework bundling templates and theme for reviewing typed documents (invoices, POs, receipts, contracts).',
        jsonb_build_object('theme_ref', 'theme.praxis.default', 'migration', '229_surface_template_type_lattice.sql'),
        'migration.229', 1.0, false, now(), now()
    ),

    -- Intent --------------------------------------------------------------
    (
        'intent.invoice_approval',
        'intent',
        'Approve an invoice',
        'User intent: approve a pending invoice, ideally with its matching PO visible for cross-check.',
        jsonb_build_object(
            'preferred_framework_ref', 'framework.document_review',
            'migration', '229_surface_template_type_lattice.sql'
        ),
        'migration.229', 1.0, false, now(), now()
    )

ON CONFLICT (id) DO UPDATE SET
    entity_type = EXCLUDED.entity_type,
    name = EXCLUDED.name,
    content = EXCLUDED.content,
    metadata = EXCLUDED.metadata,
    source = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    updated_at = now();


-- =========================================================================
-- 2. memory_edges: the typed edges (outcome graph wiring)
-- =========================================================================

INSERT INTO memory_edges (source_id, target_id, relation_type, weight, metadata) VALUES
    -- Subtype lattice -----------------------------------------------------
    ('pill_type.document.invoice', 'pill_type.document', 'subtype_of', 1.0, '{}'::jsonb),
    ('pill_type.document.purchase_order', 'pill_type.document', 'subtype_of', 1.0, '{}'::jsonb),

    -- Fields shared on Document ------------------------------------------
    ('pill_type.document', 'pill_field.title', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document', 'pill_field.issuer', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document', 'pill_field.total_amount', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document', 'pill_field.issued_at', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document', 'pill_field.currency', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document', 'pill_field.attachments', 'has_field', 1.0, '{}'::jsonb),

    -- Invoice-specific fields --------------------------------------------
    ('pill_type.document.invoice', 'pill_field.invoice_number', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document.invoice', 'pill_field.payment_terms', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document.invoice', 'pill_field.due_date', 'has_field', 1.0, '{}'::jsonb),

    -- PO-specific fields --------------------------------------------------
    ('pill_type.document.purchase_order', 'pill_field.po_number', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document.purchase_order', 'pill_field.vendor_ref', 'has_field', 1.0, '{}'::jsonb),
    ('pill_type.document.purchase_order', 'pill_field.expected_delivery', 'has_field', 1.0, '{}'::jsonb),

    -- Templates use the split shape --------------------------------------
    ('template.doc.compare.split', 'shape.split', 'uses_shape', 1.0, '{}'::jsonb),
    ('template.invoice.with_po.review', 'shape.split', 'uses_shape', 1.0, '{}'::jsonb),

    -- Generic template consumes (left, right ordinals) -------------------
    ('template.doc.compare.split', 'pill_type.document', 'consumes', 1.0, jsonb_build_object('slot', 'left', 'ordinal', 0)),
    -- NOTE: memory_edges UNIQUE is (source_id, target_id, relation_type).
    -- Two edges to the SAME target with relation_type='consumes' would
    -- collide, so we disambiguate by embedding the slot ordinal in a
    -- compound relation_type for the right slot.
    ('template.doc.compare.split', 'pill_type.document', 'consumes:right', 1.0, jsonb_build_object('slot', 'right', 'ordinal', 1)),

    -- Specialized template consumes --------------------------------------
    ('template.invoice.with_po.review', 'pill_type.document.invoice', 'consumes', 1.0, jsonb_build_object('slot', 'left', 'ordinal', 0)),
    ('template.invoice.with_po.review', 'pill_type.document.purchase_order', 'consumes', 1.0, jsonb_build_object('slot', 'right', 'ordinal', 1)),

    -- Framework includes templates ---------------------------------------
    ('framework.document_review', 'template.doc.compare.split', 'includes', 1.0, '{}'::jsonb),
    ('framework.document_review', 'template.invoice.with_po.review', 'includes', 1.0, '{}'::jsonb),

    -- Intent routing with explicit weights (specialized wins, generic is
    -- the graceful fallback when the specialized binding cannot satisfy
    -- its slots):
    ('intent.invoice_approval', 'template.invoice.with_po.review', 'targets_template', 1.0, '{}'::jsonb),
    ('intent.invoice_approval', 'template.doc.compare.split', 'targets_template', 0.3, jsonb_build_object('is_fallback', true))

ON CONFLICT (source_id, target_id, relation_type) DO UPDATE SET
    weight = EXCLUDED.weight,
    metadata = EXCLUDED.metadata;


-- =========================================================================
-- 3. CQRS projection authority for legal_templates
-- =========================================================================

INSERT INTO authority_projection_registry (
    projection_ref,
    authority_domain_ref,
    source_event_stream_ref,
    reducer_ref,
    storage_target_ref,
    freshness_policy_ref,
    enabled,
    decision_ref
) VALUES (
    'projection.surface.legal_templates',
    'authority.surface_catalog',
    'stream.surface_catalog',
    'runtime.surface_template_compiler.legal_templates_reducer',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse'
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_event_stream_ref = EXCLUDED.source_event_stream_ref,
    reducer_ref = EXCLUDED.reducer_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
VALUES ('projection.surface.legal_templates', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
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
    'projection.surface.legal_templates',
    'projection',
    'projection.surface.legal_templates',
    NULL,
    'authority.surface_catalog',
    'projection.surface.legal_templates',
    'active',
    'projection',
    'praxis.engine',
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse',
    jsonb_build_object(
        'migration', '229_surface_template_type_lattice.sql',
        'query_params', jsonb_build_object(
            'intent', 'required — intent entity id in memory_entities',
            'pill', 'repeated — pill_type entity ids available in the accumulator'
        ),
        'output_shape', jsonb_build_object(
            'ranked_templates', 'array of {template_ref, specificity, binding_weight, rank}',
            'winner', 'top-ranked template ref or null',
            'compiled_bundle', 'PraxisSurfaceBundleV4 compiled from the winner or null',
            'typed_gap', 'non-null when no template is legal, carrying repair_actions'
        )
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_projection_contracts (
    projection_contract_ref,
    projection_ref,
    authority_domain_ref,
    source_ref_kind,
    source_ref,
    read_model_object_ref,
    freshness_policy_ref,
    last_event_required,
    last_receipt_required,
    failure_visibility_required,
    replay_supported,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'projection_contract.surface.legal_templates',
    'projection.surface.legal_templates',
    'authority.surface_catalog',
    'table',
    'table.public.memory_entities',
    'projection.surface.legal_templates',
    'projection_freshness.default',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'decision.architecture_policy.surface_catalog.type_lattice_and_risk_mitigation_is_authority_reuse',
    jsonb_build_object(
        'migration', '229_surface_template_type_lattice.sql',
        'reducer_entry', 'runtime.surface_template_compiler.legal_templates_reducer',
        'graph_walk', 'intent --targets_template--> experience_template, filter by consumes subtype_of pill_type'
    )
)
ON CONFLICT (projection_ref) DO UPDATE SET
    source_ref_kind = EXCLUDED.source_ref_kind,
    source_ref = EXCLUDED.source_ref,
    read_model_object_ref = EXCLUDED.read_model_object_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'projection.surface.legal_templates',
    'Surface projection: legal experience templates for an intent + pill set',
    'projection',
    'Typed read-model that takes an intent and a set of pill types and returns ranked experience templates plus the compiled bundle for the winner. Specialized templates beat generic ones by lattice-depth specificity.',
    jsonb_build_object(
        'source', 'migration.229_surface_template_type_lattice',
        'projection_ref', 'projection.surface.legal_templates',
        'projection_contract_ref', 'projection_contract.surface.legal_templates'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'reducer_ref', 'runtime.surface_template_compiler.legal_templates_reducer',
        'consumer_surface', '/api/projections/projection.surface.legal_templates?intent=...&pill=...'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
