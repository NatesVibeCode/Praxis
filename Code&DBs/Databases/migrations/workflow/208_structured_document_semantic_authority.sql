-- Migration 208: Structured document semantic authority.
--
-- Structure says where a section lives. Semantic assertions say what it
-- means. Embeddings are recall projections only, and context selection must
-- leave an inspectable receipt explaining why a section was included.

BEGIN;

-- pgvector is enabled by the bootstrap/onboarding platform gate before
-- workflow migrations run. This migration consumes the vector type only.

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.structured_documents',
    'praxis.engine',
    'stream.structured_documents',
    'projection.structured_document.sections',
    'praxis.primary_postgres',
    TRUE,
    'decision.structured_document_semantic_authority.20260422'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS structured_document_revisions (
    document_ref TEXT NOT NULL CHECK (btrim(document_ref) <> ''),
    revision_ref TEXT NOT NULL CHECK (btrim(revision_ref) <> ''),
    document_status TEXT NOT NULL DEFAULT 'active' CHECK (
        document_status IN ('active', 'superseded', 'deprecated', 'deleted')
    ),
    title TEXT NOT NULL DEFAULT '' CHECK (title = '' OR btrim(title) <> ''),
    source_kind TEXT NOT NULL CHECK (btrim(source_kind) <> ''),
    source_ref TEXT NOT NULL CHECK (btrim(source_ref) <> ''),
    parser_version TEXT NOT NULL CHECK (btrim(parser_version) <> ''),
    content_hash TEXT NOT NULL CHECK (btrim(content_hash) <> ''),
    authority_domain_ref TEXT NOT NULL REFERENCES authority_domains (authority_domain_ref) ON DELETE RESTRICT
        DEFAULT 'authority.structured_documents',
    source_decision_ref TEXT CHECK (source_decision_ref IS NULL OR btrim(source_decision_ref) <> ''),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (document_ref, revision_ref),
    CONSTRAINT structured_document_revisions_source_authority_check
        CHECK (source_decision_ref IS NOT NULL OR source_receipt_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS structured_document_sections (
    section_ref TEXT PRIMARY KEY CHECK (btrim(section_ref) <> ''),
    document_ref TEXT NOT NULL CHECK (btrim(document_ref) <> ''),
    revision_ref TEXT NOT NULL CHECK (btrim(revision_ref) <> ''),
    parent_section_ref TEXT REFERENCES structured_document_sections (section_ref) ON DELETE SET NULL,
    node_index INTEGER NOT NULL CHECK (node_index >= 0),
    heading_level INTEGER NOT NULL CHECK (heading_level >= 0),
    heading TEXT NOT NULL DEFAULT '' CHECK (heading = '' OR btrim(heading) <> ''),
    heading_path TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    breadcrumb TEXT NOT NULL DEFAULT '',
    content_text TEXT NOT NULL DEFAULT '',
    content_hash TEXT NOT NULL CHECK (btrim(content_hash) <> ''),
    token_estimate INTEGER NOT NULL DEFAULT 0 CHECK (token_estimate >= 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    search_vector TSVECTOR NOT NULL DEFAULT ''::tsvector,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT structured_document_sections_revision_fkey
        FOREIGN KEY (document_ref, revision_ref)
        REFERENCES structured_document_revisions (document_ref, revision_ref)
        ON DELETE CASCADE,
    CONSTRAINT structured_document_sections_node_unique
        UNIQUE (document_ref, revision_ref, node_index)
);

CREATE TABLE IF NOT EXISTS structured_document_section_embeddings (
    section_ref TEXT NOT NULL REFERENCES structured_document_sections (section_ref) ON DELETE CASCADE,
    embedding_model_ref TEXT NOT NULL CHECK (btrim(embedding_model_ref) <> ''),
    embedding_input_recipe_ref TEXT NOT NULL CHECK (btrim(embedding_input_recipe_ref) <> ''),
    embedding_input_hash TEXT NOT NULL CHECK (btrim(embedding_input_hash) <> ''),
    embedding vector(384) NOT NULL,
    projection_status TEXT NOT NULL DEFAULT 'unknown' CHECK (
        projection_status IN ('fresh', 'stale', 'failed', 'unknown')
    ),
    generated_by TEXT NOT NULL DEFAULT 'structured_document_semantics' CHECK (btrim(generated_by) <> ''),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    failure_code TEXT CHECK (failure_code IS NULL OR btrim(failure_code) <> ''),
    failure_detail TEXT CHECK (failure_detail IS NULL OR btrim(failure_detail) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    PRIMARY KEY (section_ref, embedding_model_ref, embedding_input_recipe_ref)
);

CREATE TABLE IF NOT EXISTS structured_document_context_selection_receipts (
    selection_receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_ref TEXT NOT NULL CHECK (btrim(query_ref) <> ''),
    assembler_ref TEXT NOT NULL CHECK (btrim(assembler_ref) <> ''),
    section_ref TEXT NOT NULL REFERENCES structured_document_sections (section_ref) ON DELETE CASCADE,
    selected BOOLEAN NOT NULL DEFAULT FALSE,
    score_total NUMERIC NOT NULL DEFAULT 0,
    score_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(score_breakdown) = 'object'),
    deterministic_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(deterministic_reason_codes) = 'array'
    ),
    semantic_assertion_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(semantic_assertion_ids) = 'array'
    ),
    source_receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    authority_event_id UUID REFERENCES authority_events (event_id) ON DELETE SET NULL,
    idempotency_key TEXT CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT structured_document_context_selection_deterministic_reason_check
        CHECK (
            selected = FALSE
            OR deterministic_reason_codes ?| ARRAY[
                'structure_match',
                'lexical_match',
                'semantic_assertion_match',
                'synonym_expansion_match',
                'authority_weight',
                'operator_policy'
            ]
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS structured_document_context_selection_idempotency_idx
    ON structured_document_context_selection_receipts (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS structured_document_revisions_status_idx
    ON structured_document_revisions (document_status, updated_at DESC);

CREATE INDEX IF NOT EXISTS structured_document_sections_document_idx
    ON structured_document_sections (document_ref, revision_ref, node_index);

CREATE INDEX IF NOT EXISTS structured_document_sections_parent_idx
    ON structured_document_sections (parent_section_ref)
    WHERE parent_section_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS structured_document_sections_search_idx
    ON structured_document_sections USING GIN (search_vector);

CREATE OR REPLACE FUNCTION refresh_structured_document_section_search_vector()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector := to_tsvector(
        'english',
        coalesce(array_to_string(NEW.heading_path, ' > '), '')
        || ' ' || coalesce(NEW.breadcrumb, '')
        || ' ' || coalesce(NEW.heading, '')
        || ' ' || coalesce(NEW.content_text, '')
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_structured_document_sections_search_vector
    ON structured_document_sections;
CREATE TRIGGER trg_structured_document_sections_search_vector
    BEFORE INSERT OR UPDATE OF heading_path, breadcrumb, heading, content_text
    ON structured_document_sections
    FOR EACH ROW
    EXECUTE FUNCTION refresh_structured_document_section_search_vector();

CREATE INDEX IF NOT EXISTS structured_document_section_embeddings_vector_idx
    ON structured_document_section_embeddings USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS structured_document_context_selection_query_idx
    ON structured_document_context_selection_receipts (query_ref, created_at DESC);

CREATE INDEX IF NOT EXISTS structured_document_context_selection_section_idx
    ON structured_document_context_selection_receipts (section_ref, created_at DESC);

CREATE OR REPLACE VIEW structured_document_section_semantics AS
SELECT
    sections.section_ref,
    sections.document_ref,
    sections.revision_ref,
    sections.node_index,
    sections.heading,
    sections.breadcrumb,
    assertions.semantic_assertion_id,
    assertions.predicate_slug,
    assertions.object_kind,
    assertions.object_ref,
    assertions.qualifiers_json,
    assertions.source_kind,
    assertions.source_ref,
    assertions.evidence_ref,
    assertions.bound_decision_id,
    assertions.valid_from,
    assertions.valid_to
FROM structured_document_sections sections
JOIN semantic_current_assertions assertions
  ON assertions.subject_kind = 'structured_document_section'
 AND assertions.subject_ref = sections.section_ref
WHERE assertions.assertion_status = 'active';

CREATE OR REPLACE VIEW structured_document_context_receipt_summary AS
SELECT
    receipts.query_ref,
    receipts.assembler_ref,
    receipts.section_ref,
    sections.document_ref,
    sections.revision_ref,
    sections.breadcrumb,
    receipts.selected,
    receipts.score_total,
    receipts.score_breakdown,
    receipts.deterministic_reason_codes,
    receipts.semantic_assertion_ids,
    receipts.source_receipt_id,
    receipts.authority_event_id,
    receipts.created_at
FROM structured_document_context_selection_receipts receipts
JOIN structured_document_sections sections
  ON sections.section_ref = receipts.section_ref;

INSERT INTO semantic_predicates (
    predicate_slug,
    predicate_status,
    subject_kind_allowlist,
    object_kind_allowlist,
    cardinality_mode,
    description
) VALUES
    (
        'defines',
        'active',
        '["structured_document_section"]'::jsonb,
        '["term","concept","data_dictionary_object","authority_object","definition"]'::jsonb,
        'many',
        'A structured document section defines a term, concept, dictionary object, authority object, or definition.'
    ),
    (
        'cites',
        'active',
        '["structured_document_section"]'::jsonb,
        '["operator_decision","authority_receipt","semantic_assertion","structured_document_section","source"]'::jsonb,
        'many',
        'A structured document section cites an operator decision, receipt, assertion, section, or external source.'
    ),
    (
        'supersedes',
        'active',
        '["structured_document_section","structured_document_revision"]'::jsonb,
        '["structured_document_section","structured_document_revision","operator_decision","definition"]'::jsonb,
        'many',
        'A structured document section or revision supersedes an older section, revision, decision, or definition.'
    ),
    (
        'applies_to',
        'active',
        '["structured_document_section","term"]'::jsonb,
        '["authority_domain","authority_object","data_dictionary_object","workflow_surface","runtime_target","service_bus_channel"]'::jsonb,
        'many',
        'A section or term applies to a domain, object, dictionary entry, workflow surface, runtime target, or service-bus channel.'
    ),
    (
        'constrains',
        'active',
        '["structured_document_section"]'::jsonb,
        '["authority_domain","authority_object","workflow_surface","service_bus_channel","operation","runtime_target"]'::jsonb,
        'many',
        'A structured document section constrains runtime behavior, operations, domains, service-bus channels, or runtime targets.'
    ),
    (
        'aliases',
        'active',
        '["term","structured_document_section","data_dictionary_object"]'::jsonb,
        '["term","data_dictionary_object","authority_domain","concept"]'::jsonb,
        'many',
        'A term, section, or dictionary object aliases another term, dictionary object, authority domain, or concept.'
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
) VALUES
    (
        'projection.structured_document.sections',
        'authority.structured_documents',
        'stream.structured_documents',
        'runtime.structured_document_semantics.project_sections',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.structured_document_semantic_authority.20260422'
    ),
    (
        'projection.structured_document.section_semantics',
        'authority.structured_documents',
        'stream.structured_documents',
        'runtime.structured_document_semantics.project_section_semantics',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.structured_document_semantic_authority.20260422'
    ),
    (
        'projection.structured_document.section_embeddings',
        'authority.structured_documents',
        'stream.structured_documents',
        'runtime.structured_document_semantics.project_section_embeddings',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.structured_document_semantic_authority.20260422'
    ),
    (
        'projection.structured_document.context_selection_receipts',
        'authority.structured_documents',
        'stream.structured_documents',
        'runtime.structured_document_semantics.project_context_selection_receipts',
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
VALUES
    ('projection.structured_document.sections', 'unknown', NULL),
    ('projection.structured_document.section_semantics', 'unknown', NULL),
    ('projection.structured_document.section_embeddings', 'unknown', NULL),
    ('projection.structured_document.context_selection_receipts', 'unknown', NULL)
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    updated_at = now();

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
    decision_ref,
    authority_domain_ref,
    storage_target_ref,
    input_schema_ref,
    output_schema_ref,
    idempotency_key_fields,
    required_capabilities,
    allowed_callers,
    timeout_ms,
    receipt_required,
    event_required,
    event_type,
    projection_freshness_policy_ref
) VALUES
    (
        'structured-documents-record-context-selection',
        'structured_documents.record_context_selection',
        'operation_command',
        'command',
        'POST',
        '/api/structured-documents/context-selections',
        'runtime.operations.commands.structured_documents.RecordStructuredDocumentContextSelectionCommand',
        'runtime.operations.commands.structured_documents.handle_record_context_selection',
        'authority.structured_documents',
        'projection.structured_document.context_selection_receipts',
        'operate',
        'idempotent',
        TRUE,
        'binding.operation_catalog_registry.structured_documents.20260422',
        'decision.structured_document_semantic_authority.20260422',
        'authority.structured_documents',
        'praxis.primary_postgres',
        'schema.structured_documents.record_context_selection.input',
        'schema.structured_documents.record_context_selection.output',
        '["query_ref","assembler_ref","section_ref","score_total","semantic_assertion_ids"]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        TRUE,
        'structured_document_context_selected',
        'projection_freshness.default'
    ),
    (
        'structured-documents-list-context-selections',
        'structured_documents.list_context_selection_receipts',
        'operation_query',
        'query',
        'GET',
        '/api/structured-documents/context-selections',
        'runtime.operations.queries.structured_documents.ListStructuredDocumentContextSelectionsQuery',
        'runtime.operations.queries.structured_documents.handle_list_context_selection_receipts',
        'authority.structured_documents',
        'projection.structured_document.context_selection_receipts',
        'observe',
        'read_only',
        TRUE,
        'binding.operation_catalog_registry.structured_documents.20260422',
        'decision.structured_document_semantic_authority.20260422',
        'authority.structured_documents',
        'praxis.primary_postgres',
        'schema.structured_documents.list_context_selection_receipts.input',
        'schema.structured_documents.list_context_selection_receipts.output',
        '[]'::jsonb,
        '{}'::jsonb,
        '["cli","mcp","http","workflow","heartbeat"]'::jsonb,
        15000,
        TRUE,
        FALSE,
        NULL,
        'projection_freshness.default'
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
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    idempotency_key_fields = EXCLUDED.idempotency_key_fields,
    required_capabilities = EXCLUDED.required_capabilities,
    allowed_callers = EXCLUDED.allowed_callers,
    timeout_ms = EXCLUDED.timeout_ms,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    event_type = EXCLUDED.event_type,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('structured_document_revisions', 'Structured document revisions', 'table', 'Authority-owned structured document revision records with explicit source decision or receipt.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('structured_document_sections', 'Structured document sections', 'table', 'Durable section nodes for structured documents. Sections are semantic assertion subjects.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","subject_kind":"structured_document_section"}'::jsonb),
    ('structured_document_section_embeddings', 'Structured document section embeddings', 'table', 'Derived embedding projection for candidate recall. This table is not semantic truth.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb),
    ('structured_document_context_selection_receipts', 'Structured document context selection receipts', 'table', 'Inspectable receipts for why document sections were included or rejected during context assembly.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('definition.structured_document_revision', 'Structured document revision definition', 'definition', 'Definition for one parsed document revision.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('definition.structured_document_section', 'Structured document section definition', 'definition', 'Definition for one addressable document section subject.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","subject_kind":"structured_document_section"}'::jsonb),
    ('definition.structured_document_section_embedding', 'Structured document section embedding definition', 'definition', 'Versioned recall projection over section text, breadcrumb, and active predicates.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb),
    ('definition.structured_document_context_selection', 'Structured document context selection definition', 'definition', 'Definition for context assembly selection receipts with score parts and deterministic reasons.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('semantic.predicate.defines', 'Semantic predicate defines', 'definition', 'Typed assertion that a section defines a term or object.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"defines"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('semantic.predicate.cites', 'Semantic predicate cites', 'definition', 'Typed assertion that a section cites a decision, receipt, assertion, section, or source.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"cites"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('semantic.predicate.supersedes', 'Semantic predicate supersedes', 'definition', 'Typed assertion that a section or revision supersedes an older authority object.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"supersedes"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('semantic.predicate.applies_to', 'Semantic predicate applies to', 'definition', 'Typed assertion that a section or term applies to an authority target.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"applies_to"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('semantic.predicate.constrains', 'Semantic predicate constrains', 'definition', 'Typed assertion that a section constrains runtime behavior or authority state.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"constrains"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('semantic.predicate.aliases', 'Semantic predicate aliases', 'definition', 'Typed assertion that a term, section, or object aliases another concept.', '{"migration":"208_structured_document_semantic_authority.sql","semantic_predicate":"aliases"}'::jsonb, '{"authority_domain_ref":"authority.semantic_predicates"}'::jsonb),
    ('projection.structured_document.sections', 'Structured document sections projection', 'projection', 'Read model for structured document section nodes.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('projection.structured_document.section_semantics', 'Structured document section semantics projection', 'projection', 'Read model joining section nodes to active semantic assertions.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('projection.structured_document.section_embeddings', 'Structured document section embeddings projection', 'projection', 'Read model for versioned section embedding recall records.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","truth_status":"projection_only"}'::jsonb),
    ('projection.structured_document.context_selection_receipts', 'Structured document context selection receipt projection', 'projection', 'Read model for context selection receipts and score reasons.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('operation.structured_documents.record_context_selection', 'Record structured document context selection', 'command', 'Command for recording context selection receipts through authority gateway.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","operation_kind":"command"}'::jsonb),
    ('operation.structured_documents.list_context_selection_receipts', 'List structured document context selection receipts', 'query', 'Query operation for inspecting context selection receipts.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents","operation_kind":"query"}'::jsonb),
    ('event.structured_document_revision_indexed', 'structured_document_revision_indexed', 'event', 'Authority event for indexed structured document revisions.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('event.structured_document_section_projected', 'structured_document_section_projected', 'event', 'Authority event for projected structured document sections.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb),
    ('event.structured_document_context_selected', 'structured_document_context_selected', 'event', 'Authority event for context selection receipt writes.', '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.structured_documents"}'::jsonb)
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
    ('definition.structured_document_section', 'section_ref', 'auto', 'text', 'Section ref', 'Stable subject reference for semantic assertions.', TRUE, 10, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_section', 'document_ref', 'auto', 'text', 'Document ref', 'Stable structured document identity.', TRUE, 20, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_section', 'revision_ref', 'auto', 'text', 'Revision ref', 'Versioned document revision identity.', TRUE, 30, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_section', 'breadcrumb', 'auto', 'text', 'Breadcrumb', 'Human-readable heading path used for structure-aware retrieval.', FALSE, 40, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_section', 'content_text', 'auto', 'text', 'Content text', 'Local section text. Semantics are asserted separately in semantic_assertions.', FALSE, 50, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_section_embedding', 'embedding', 'auto', 'array', 'Embedding', 'Vector projection used for recall only; it does not establish authority.', TRUE, 10, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{"dimension":384}'::jsonb),
    ('definition.structured_document_context_selection', 'score_breakdown', 'auto', 'json', 'Score breakdown', 'Structured score parts used for context selection.', TRUE, 10, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_context_selection', 'deterministic_reason_codes', 'auto', 'array', 'Deterministic reason codes', 'Reasons that make selected context inspectable. Vector-only recall cannot select authoritative context.', TRUE, 20, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb),
    ('definition.structured_document_context_selection', 'semantic_assertion_ids', 'auto', 'array', 'Semantic assertion ids', 'Active semantic assertions that justified or influenced context selection.', FALSE, 30, '{"migration":"208_structured_document_semantic_authority.sql"}'::jsonb, '{}'::jsonb)
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
    ('table.public.structured_document_revisions', 'table', 'structured_document_revisions', 'public', 'authority.structured_documents', 'structured_document_revisions', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('table.public.structured_document_sections', 'table', 'structured_document_sections', 'public', 'authority.structured_documents', 'structured_document_sections', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"subject_kind":"structured_document_section"}'::jsonb),
    ('table.public.structured_document_section_embeddings', 'table', 'structured_document_section_embeddings', 'public', 'authority.structured_documents', 'structured_document_section_embeddings', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('table.public.structured_document_context_selection_receipts', 'table', 'structured_document_context_selection_receipts', 'public', 'authority.structured_documents', 'structured_document_context_selection_receipts', 'active', 'event_stream', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.structured_document_revision', 'definition', 'structured_document_revision', NULL, 'authority.structured_documents', 'definition.structured_document_revision', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.structured_document_section', 'definition', 'structured_document_section', NULL, 'authority.structured_documents', 'definition.structured_document_section', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"subject_kind":"structured_document_section"}'::jsonb),
    ('definition.structured_document_section_embedding', 'definition', 'structured_document_section_embedding', NULL, 'authority.structured_documents', 'definition.structured_document_section_embedding', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('definition.structured_document_context_selection', 'definition', 'structured_document_context_selection', NULL, 'authority.structured_documents', 'definition.structured_document_context_selection', 'active', 'event_stream', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.defines', 'definition', 'defines', NULL, 'authority.semantic_predicates', 'semantic.predicate.defines', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.cites', 'definition', 'cites', NULL, 'authority.semantic_predicates', 'semantic.predicate.cites', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.supersedes', 'definition', 'supersedes', NULL, 'authority.semantic_predicates', 'semantic.predicate.supersedes', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.applies_to', 'definition', 'applies_to', NULL, 'authority.semantic_predicates', 'semantic.predicate.applies_to', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.constrains', 'definition', 'constrains', NULL, 'authority.semantic_predicates', 'semantic.predicate.constrains', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('definition.semantic_predicate.aliases', 'definition', 'aliases', NULL, 'authority.semantic_predicates', 'semantic.predicate.aliases', 'active', 'definition', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('projection.projection.structured_document.sections', 'projection', 'projection.structured_document.sections', NULL, 'authority.structured_documents', 'projection.structured_document.sections', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('projection.projection.structured_document.section_semantics', 'projection', 'projection.structured_document.section_semantics', NULL, 'authority.structured_documents', 'projection.structured_document.section_semantics', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('projection.projection.structured_document.section_embeddings', 'projection', 'projection.structured_document.section_embeddings', NULL, 'authority.structured_documents', 'projection.structured_document.section_embeddings', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('projection.projection.structured_document.context_selection_receipts', 'projection', 'projection.structured_document.context_selection_receipts', NULL, 'authority.structured_documents', 'projection.structured_document.context_selection_receipts', 'active', 'projection', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('operation.structured_documents.record_context_selection', 'command', 'structured_documents.record_context_selection', NULL, 'authority.structured_documents', 'operation.structured_documents.record_context_selection', 'active', 'command_model', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"operation_ref":"structured-documents-record-context-selection"}'::jsonb),
    ('operation.structured_documents.list_context_selection_receipts', 'query', 'structured_documents.list_context_selection_receipts', NULL, 'authority.structured_documents', 'operation.structured_documents.list_context_selection_receipts', 'active', 'read_model', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{"operation_ref":"structured-documents-list-context-selections"}'::jsonb),
    ('event.structured_document_revision_indexed', 'event', 'structured_document_revision_indexed', NULL, 'authority.structured_documents', 'event.structured_document_revision_indexed', 'active', 'event_stream', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('event.structured_document_section_projected', 'event', 'structured_document_section_projected', NULL, 'authority.structured_documents', 'event.structured_document_section_projected', 'active', 'event_stream', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb),
    ('event.structured_document_context_selected', 'event', 'structured_document_context_selected', NULL, 'authority.structured_documents', 'event.structured_document_context_selected', 'active', 'event_stream', 'praxis.engine', 'decision.structured_document_semantic_authority.20260422', '{}'::jsonb)
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
    decision_ref,
    metadata
) VALUES
    (
        'event_contract.structured_document_revision_indexed',
        'structured_document_revision_indexed',
        'authority.structured_documents',
        'schema.structured_documents.revision_indexed.event',
        'entity_ref',
        '["runtime.structured_document_semantics.project_sections"]'::jsonb,
        '["projection.structured_document.sections"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.structured_document_semantic_authority.20260422',
        '{}'::jsonb
    ),
    (
        'event_contract.structured_document_section_projected',
        'structured_document_section_projected',
        'authority.structured_documents',
        'schema.structured_documents.section_projected.event',
        'entity_ref',
        '["runtime.structured_document_semantics.project_section_semantics"]'::jsonb,
        '["projection.structured_document.section_semantics"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.structured_document_semantic_authority.20260422',
        '{}'::jsonb
    ),
    (
        'event_contract.structured_document_context_selected',
        'structured_document_context_selected',
        'authority.structured_documents',
        'schema.structured_documents.context_selected.event',
        'entity_ref',
        '["runtime.structured_document_semantics.project_context_selection_receipts"]'::jsonb,
        '["projection.structured_document.context_selection_receipts"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'decision.structured_document_semantic_authority.20260422',
        '{"boundary":"vector similarity may recall candidates but cannot select authoritative context alone"}'::jsonb
    )
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs = EXCLUDED.reducer_refs,
    projection_refs = EXCLUDED.projection_refs,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
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
) VALUES
    ('projection_contract.projection.structured_document.sections', 'projection.structured_document.sections', 'authority.structured_documents', 'table', 'structured_document_sections', 'projection.projection.structured_document.sections', 'projection_freshness.default', TRUE, TRUE, TRUE, TRUE, TRUE, 'decision.structured_document_semantic_authority.20260422', '{"read_model":"structured_document_sections"}'::jsonb),
    ('projection_contract.projection.structured_document.section_semantics', 'projection.structured_document.section_semantics', 'authority.structured_documents', 'authority_view', 'structured_document_section_semantics', 'projection.projection.structured_document.section_semantics', 'projection_freshness.default', TRUE, TRUE, TRUE, TRUE, TRUE, 'decision.structured_document_semantic_authority.20260422', '{"source":"semantic_current_assertions"}'::jsonb),
    ('projection_contract.projection.structured_document.section_embeddings', 'projection.structured_document.section_embeddings', 'authority.structured_documents', 'table', 'structured_document_section_embeddings', 'projection.projection.structured_document.section_embeddings', 'projection_freshness.default', TRUE, TRUE, TRUE, TRUE, TRUE, 'decision.structured_document_semantic_authority.20260422', '{"truth_status":"projection_only"}'::jsonb),
    ('projection_contract.projection.structured_document.context_selection_receipts', 'projection.structured_document.context_selection_receipts', 'authority.structured_documents', 'authority_view', 'structured_document_context_receipt_summary', 'projection.projection.structured_document.context_selection_receipts', 'projection_freshness.default', TRUE, TRUE, TRUE, TRUE, TRUE, 'decision.structured_document_semantic_authority.20260422', '{"read_model":"structured_document_context_receipt_summary"}'::jsonb)
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

COMMENT ON TABLE structured_document_revisions IS
    'Authority-owned structured document revisions. Each revision must be backed by a source decision or receipt.';
COMMENT ON TABLE structured_document_sections IS
    'Addressable structured document section nodes. They provide location; semantic_assertions provide meaning.';
COMMENT ON TABLE structured_document_section_embeddings IS
    'Derived vector recall projection for structured document sections. Embeddings are candidates, not authority.';
COMMENT ON TABLE structured_document_context_selection_receipts IS
    'Inspectable context assembly receipts with score parts, deterministic reasons, semantic assertion IDs, and source receipts.';
COMMENT ON VIEW structured_document_section_semantics IS
    'Projection joining section structure to active semantic assertions. Structure locates content; assertions define meaning.';
COMMENT ON VIEW structured_document_context_receipt_summary IS
    'Projection for inspecting why structured document sections were selected or rejected during context assembly.';

COMMIT;
