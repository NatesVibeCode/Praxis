-- Migration 336: Knowledge digest and structured context assembly authority.
--
-- Condensed knowledge is a projection with input hashes, not source truth.
-- Section/document tags are typed semantic assertions against a small tag
-- catalog, so tags remain queryable, temporal, and evidence-backed.

BEGIN;

CREATE TABLE IF NOT EXISTS knowledge_tag_catalog (
    tag_ref TEXT PRIMARY KEY CHECK (btrim(tag_ref) <> ''),
    tag_key TEXT NOT NULL CHECK (btrim(tag_key) <> ''),
    tag_value TEXT NOT NULL DEFAULT '',
    tag_status TEXT NOT NULL DEFAULT 'active' CHECK (
        tag_status IN ('active', 'deprecated', 'retired')
    ),
    summary TEXT NOT NULL DEFAULT '',
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT
        DEFAULT 'authority.structured_documents',
    source_decision_ref TEXT CHECK (source_decision_ref IS NULL OR btrim(source_decision_ref) <> ''),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT knowledge_tag_catalog_source_authority_check
        CHECK (source_decision_ref IS NOT NULL OR source_receipt_id IS NOT NULL),
    CONSTRAINT knowledge_tag_catalog_key_value_unique
        UNIQUE (tag_key, tag_value)
);

CREATE TABLE IF NOT EXISTS knowledge_digest_revisions (
    digest_ref TEXT NOT NULL CHECK (btrim(digest_ref) <> ''),
    revision_ref TEXT NOT NULL CHECK (btrim(revision_ref) <> ''),
    digest_status TEXT NOT NULL DEFAULT 'active' CHECK (
        digest_status IN ('active', 'superseded', 'retracted')
    ),
    digest_type TEXT NOT NULL CHECK (btrim(digest_type) <> ''),
    subject_kind TEXT NOT NULL CHECK (btrim(subject_kind) <> ''),
    subject_ref TEXT NOT NULL CHECK (btrim(subject_ref) <> ''),
    summary_text TEXT NOT NULL DEFAULT '',
    input_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(input_refs) = 'array'),
    input_hash TEXT NOT NULL CHECK (input_hash LIKE 'sha256:%'),
    output_hash TEXT NOT NULL CHECK (output_hash LIKE 'sha256:%'),
    recipe_ref TEXT NOT NULL CHECK (btrim(recipe_ref) <> ''),
    recipe_version TEXT NOT NULL CHECK (btrim(recipe_version) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT
        DEFAULT 'authority.structured_documents',
    supersedes_digest_ref TEXT CHECK (supersedes_digest_ref IS NULL OR btrim(supersedes_digest_ref) <> ''),
    source_decision_ref TEXT CHECK (source_decision_ref IS NULL OR btrim(source_decision_ref) <> ''),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (digest_ref, revision_ref),
    CONSTRAINT knowledge_digest_revisions_source_authority_check
        CHECK (source_decision_ref IS NOT NULL OR source_receipt_id IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS knowledge_digest_revisions_one_active_idx
    ON knowledge_digest_revisions (digest_ref)
    WHERE digest_status = 'active';

CREATE INDEX IF NOT EXISTS knowledge_digest_revisions_subject_idx
    ON knowledge_digest_revisions (subject_kind, subject_ref, digest_status, created_at DESC);

CREATE INDEX IF NOT EXISTS knowledge_digest_revisions_input_refs_idx
    ON knowledge_digest_revisions USING GIN (input_refs);

CREATE INDEX IF NOT EXISTS knowledge_tag_catalog_key_value_idx
    ON knowledge_tag_catalog (tag_key, tag_value, tag_status);

INSERT INTO semantic_predicates (
    predicate_slug,
    predicate_status,
    subject_kind_allowlist,
    object_kind_allowlist,
    cardinality_mode,
    description
) VALUES (
    'tagged_as',
    'active',
    '["structured_document_section","structured_document_revision","structured_document","knowledge_digest","memory_entity"]'::jsonb,
    '["knowledge_tag"]'::jsonb,
    'many',
    'A knowledge object is tagged with a typed knowledge_tag_catalog entry.'
)
ON CONFLICT (predicate_slug) DO UPDATE SET
    predicate_status = EXCLUDED.predicate_status,
    subject_kind_allowlist = EXCLUDED.subject_kind_allowlist,
    object_kind_allowlist = EXCLUDED.object_kind_allowlist,
    cardinality_mode = EXCLUDED.cardinality_mode,
    description = EXCLUDED.description,
    updated_at = now();

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
    'projection.structured_document.knowledge_digests',
    'authority.structured_documents',
    'stream.structured_documents',
    'runtime.structured_document_semantics.project_knowledge_digests',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.structured_document_semantic_authority.20260422'
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
VALUES ('projection.structured_document.knowledge_digests', 'unknown', NULL)
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'knowledge_tag_catalog',
        'Knowledge tag catalog',
        'table',
        'Typed knowledge tag vocabulary. Actual section/document tag assignment is expressed through semantic_assertions using predicate tagged_as.',
        '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.structured_documents"}'::jsonb
    ),
    (
        'knowledge_digest_revisions',
        'Knowledge digest revisions',
        'table',
        'Append-only digest projections for condensed knowledge with input refs, input hash, output hash, recipe, status, and provenance.',
        '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb
    ),
    (
        'definition.knowledge_tag',
        'Knowledge tag definition',
        'definition',
        'Definition for one typed knowledge tag. Tags are assigned by semantic assertion, not JSON metadata.',
        '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.structured_documents","object_kind":"knowledge_tag"}'::jsonb
    ),
    (
        'definition.knowledge_digest',
        'Knowledge digest definition',
        'definition',
        'Definition for one condensed knowledge projection revision.',
        '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb
    ),
    (
        'semantic.predicate.tagged_as',
        'Semantic predicate tagged_as',
        'definition',
        'Typed assertion that a knowledge object carries a cataloged knowledge tag.',
        '{"migration":"336_knowledge_digest_context_assembly.sql","semantic_predicate":"tagged_as"}'::jsonb,
        '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb
    ),
    (
        'projection.structured_document.knowledge_digests',
        'Structured document knowledge digests projection',
        'projection',
        'Read model for active condensed knowledge projections used during context assembly.',
        '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    display_order,
    origin_ref,
    metadata
) VALUES
    ('definition.knowledge_tag', 'tag_ref', 'auto', 'text', 'Tag ref', 'Stable knowledge tag reference used as semantic assertion object_ref.', TRUE, 10, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb),
    ('definition.knowledge_tag', 'tag_key', 'auto', 'text', 'Tag key', 'Typed tag category.', TRUE, 20, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb),
    ('definition.knowledge_tag', 'tag_value', 'auto', 'text', 'Tag value', 'Typed tag value inside the category.', FALSE, 30, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb),
    ('definition.knowledge_digest', 'input_refs', 'auto', 'json', 'Input refs', 'Exact authority refs condensed into this digest revision.', TRUE, 10, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb),
    ('definition.knowledge_digest', 'input_hash', 'auto', 'text', 'Input hash', 'Canonical sha256 hash of the digest inputs.', TRUE, 20, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{"hash_algorithm":"sha256"}'::jsonb),
    ('definition.knowledge_digest', 'output_hash', 'auto', 'text', 'Output hash', 'Canonical sha256 hash of the condensed output text.', TRUE, 30, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{"hash_algorithm":"sha256"}'::jsonb),
    ('definition.knowledge_digest', 'recipe_ref', 'auto', 'text', 'Recipe ref', 'Deterministic condensation recipe identity.', TRUE, 40, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb),
    ('definition.knowledge_digest', 'recipe_version', 'auto', 'text', 'Recipe version', 'Version of the condensation recipe used for this digest.', TRUE, 50, '{"migration":"336_knowledge_digest_context_assembly.sql"}'::jsonb, '{}'::jsonb)
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    display_order = EXCLUDED.display_order,
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
) VALUES
    ('table.public.knowledge_tag_catalog', 'table', 'knowledge_tag_catalog', 'public', 'authority.structured_documents', 'knowledge_tag_catalog', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('table.public.knowledge_digest_revisions', 'table', 'knowledge_digest_revisions', 'public', 'authority.structured_documents', 'knowledge_digest_revisions', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('definition.knowledge_tag', 'definition', 'knowledge_tag', NULL, 'authority.structured_documents', 'definition.knowledge_tag', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.knowledge_digest', 'definition', 'knowledge_digest', NULL, 'authority.structured_documents', 'definition.knowledge_digest', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('definition.semantic_predicate.tagged_as', 'definition', 'tagged_as', NULL, 'authority.semantic_predicates', 'semantic.predicate.tagged_as', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('projection.projection.structured_document.knowledge_digests', 'projection', 'projection.structured_document.knowledge_digests', NULL, 'authority.structured_documents', 'projection.structured_document.knowledge_digests', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb)
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
    'projection_contract.projection.structured_document.knowledge_digests',
    'projection.structured_document.knowledge_digests',
    'authority.structured_documents',
    'table',
    'knowledge_digest_revisions',
    'projection.projection.structured_document.knowledge_digests',
    'projection_freshness.default',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'decision.structured_document_semantic_authority.20260422',
    '{"read_model":"knowledge_digest_revisions","truth_status":"projection_only"}'::jsonb
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_ref_kind = EXCLUDED.source_ref_kind,
    source_ref = EXCLUDED.source_ref,
    read_model_object_ref = EXCLUDED.read_model_object_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    last_event_required = EXCLUDED.last_event_required,
    last_receipt_required = EXCLUDED.last_receipt_required,
    failure_visibility_required = EXCLUDED.failure_visibility_required,
    replay_supported = EXCLUDED.replay_supported,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'structured-documents-context-assemble',
    p_operation_name        := 'structured_documents.context_assemble',
    p_handler_ref           := 'runtime.operations.queries.structured_documents.handle_assemble_context',
    p_input_model_ref       := 'runtime.operations.queries.structured_documents.AssembleStructuredDocumentContextQuery',
    p_authority_domain_ref  := 'authority.structured_documents',
    p_authority_ref         := 'authority.structured_documents',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/structured_documents_context_assemble',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_decision_ref          := 'decision.structured_document_semantic_authority.20260422',
    p_binding_revision      := 'binding.operation_catalog_registry.structured_documents.context_assemble.20260429',
    p_label                 := 'Operation: structured_documents.context_assemble',
    p_summary               := 'Assemble deterministic structured-document context with provenance, classification tags, digest refs, and selection receipt inputs.'
);

COMMENT ON TABLE knowledge_tag_catalog IS
    'Typed knowledge tags. Assign tags through semantic_assertions(tagged_as), not free-form JSON metadata.';
COMMENT ON TABLE knowledge_digest_revisions IS
    'Append-only condensed knowledge projections with input refs, input hash, output hash, recipe, status, and provenance.';

COMMIT;
