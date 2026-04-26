-- Migration 278: Register the federated search + per-source search
-- query operations in the CQRS catalog.
--
-- This is the build half of BUG-68256068 (discovery/recall/query/memory
-- retrieval authority spread). The MCP tool praxis_search now dispatches
-- through operation_catalog_gateway.execute_operation_from_subsystems so
-- every search call records a read receipt in
-- authority_operation_receipts and the federation surface lives at the
-- same architectural tier as every other CQRS query.
--
-- Operations registered (all read-only, gateway-dispatched):
--   - search.federated         — fan-out across declared sources
--   - search.code              — code-only (vector + FTS + literal/regex)
--   - search.knowledge         — knowledge graph
--   - search.decisions         — operator_decisions filter
--   - search.research          — research findings filter
--   - search.bugs              — bug tracker
--   - search.receipts          — workflow receipts
--   - search.git_history       — git log/diff/blame
--   - search.files             — path-glob enumeration
--   - search.db                — allowlisted db_read

BEGIN;

-- =====================================================================
-- data_dictionary_objects entries
-- =====================================================================
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES
    ('operation.search.federated', 'Operation: search.federated', 'command',
     'Canonical federated search across code/knowledge/bugs/receipts/decisions/research/git/files/db. Returns one ranked, source-tagged result list with line-context, freshness, and explain.',
     '{"source":"migration.278","authority":"runtime.operations.queries.search"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_federated_search"}'::jsonb),

    ('operation.search.code', 'Operation: search.code', 'command',
     'Code source query: vector + FTS + literal/regex with line-context.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_code_search"}'::jsonb),

    ('operation.search.knowledge', 'Operation: search.knowledge', 'command',
     'Knowledge graph + memory engine recall query.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_knowledge_search"}'::jsonb),

    ('operation.search.decisions', 'Operation: search.decisions', 'command',
     'Operator decisions filter over the recall surface.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_decisions_search"}'::jsonb),

    ('operation.search.research', 'Operation: search.research', 'command',
     'Research findings filter over the recall surface.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_research_search"}'::jsonb),

    ('operation.search.bugs', 'Operation: search.bugs', 'command',
     'Bug tracker query through the search envelope.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_bugs_search"}'::jsonb),

    ('operation.search.receipts', 'Operation: search.receipts', 'command',
     'Workflow receipt search through the search envelope.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_receipts_search"}'::jsonb),

    ('operation.search.git_history', 'Operation: search.git_history', 'command',
     'Git log/diff/blame source through the search envelope.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_git_search"}'::jsonb),

    ('operation.search.files', 'Operation: search.files', 'command',
     'Path-glob enumeration with file metadata.',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_files_search"}'::jsonb),

    ('operation.search.db', 'Operation: search.db', 'command',
     'Allowlisted db_read query (parameterized, never raw SQL).',
     '{"source":"migration.278"}'::jsonb,
     '{"operation_kind":"query","handler_ref":"runtime.operations.queries.search.handle_db_read_search"}'::jsonb)

ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();


-- =====================================================================
-- authority_object_registry entries
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
    ('operation.search.federated', 'command', 'search.federated', NULL,
     'authority.workflow_runs', 'operation.search.federated', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_federated_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.code', 'command', 'search.code', NULL,
     'authority.workflow_runs', 'operation.search.code', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_code_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.knowledge', 'command', 'search.knowledge', NULL,
     'authority.memory_entities', 'operation.search.knowledge', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_knowledge_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.decisions', 'command', 'search.decisions', NULL,
     'authority.operator_decisions', 'operation.search.decisions', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_decisions_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.research', 'command', 'search.research', NULL,
     'authority.memory_entities', 'operation.search.research', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_research_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.bugs', 'command', 'search.bugs', NULL,
     'authority.bugs', 'operation.search.bugs', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_bugs_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.receipts', 'command', 'search.receipts', NULL,
     'authority.workflow_runs', 'operation.search.receipts', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_receipts_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.git_history', 'command', 'search.git_history', NULL,
     'authority.workflow_runs', 'operation.search.git_history', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_git_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.files', 'command', 'search.files', NULL,
     'authority.workflow_runs', 'operation.search.files', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_files_search","source_kind":"operation_query"}'::jsonb),

    ('operation.search.db', 'command', 'search.db', NULL,
     'authority.workflow_runs', 'operation.search.db', 'active', 'read_model',
     'praxis.engine', 'decision.search_consolidation.20260426',
     '{"handler_ref":"runtime.operations.queries.search.handle_db_read_search","source_kind":"operation_query"}'::jsonb)

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
-- operation_catalog_registry entries
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
    ('search-federated', 'search.federated', 'operation_query', 'query',
     'POST', '/api/search/federated',
     'runtime.operations.queries.search.FederatedSearchQuery',
     'runtime.operations.queries.search.handle_federated_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_federated.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.FederatedSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-code', 'search.code', 'operation_query', 'query',
     'POST', '/api/search/code',
     'runtime.operations.queries.search.CodeSearchQuery',
     'runtime.operations.queries.search.handle_code_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_code.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.CodeSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-knowledge', 'search.knowledge', 'operation_query', 'query',
     'POST', '/api/search/knowledge',
     'runtime.operations.queries.search.KnowledgeSearchQuery',
     'runtime.operations.queries.search.handle_knowledge_search',
     'authority.memory_entities', 'authority.memory_entities',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_knowledge.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.KnowledgeSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-decisions', 'search.decisions', 'operation_query', 'query',
     'POST', '/api/search/decisions',
     'runtime.operations.queries.search.DecisionsSearchQuery',
     'runtime.operations.queries.search.handle_decisions_search',
     'authority.operator_decisions', 'authority.operator_decisions',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_decisions.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.DecisionsSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-research', 'search.research', 'operation_query', 'query',
     'POST', '/api/search/research',
     'runtime.operations.queries.search.ResearchSearchQuery',
     'runtime.operations.queries.search.handle_research_search',
     'authority.memory_entities', 'authority.memory_entities',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_research.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.ResearchSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-bugs', 'search.bugs', 'operation_query', 'query',
     'POST', '/api/search/bugs',
     'runtime.operations.queries.search.BugsSearchQuery',
     'runtime.operations.queries.search.handle_bugs_search',
     'authority.bugs', 'authority.bugs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_bugs.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.BugsSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-receipts', 'search.receipts', 'operation_query', 'query',
     'POST', '/api/search/receipts',
     'runtime.operations.queries.search.ReceiptsSearchQuery',
     'runtime.operations.queries.search.handle_receipts_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_receipts.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.ReceiptsSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-git-history', 'search.git_history', 'operation_query', 'query',
     'POST', '/api/search/git-history',
     'runtime.operations.queries.search.GitSearchQuery',
     'runtime.operations.queries.search.handle_git_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_git_history.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.GitSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-files', 'search.files', 'operation_query', 'query',
     'POST', '/api/search/files',
     'runtime.operations.queries.search.FilesSearchQuery',
     'runtime.operations.queries.search.handle_files_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_files.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.FilesSearchQuery',
     'operation.output.default',
     'praxis.primary_postgres',
     TRUE, FALSE),

    ('search-db', 'search.db', 'operation_query', 'query',
     'POST', '/api/search/db',
     'runtime.operations.queries.search.DbReadSearchQuery',
     'runtime.operations.queries.search.handle_db_read_search',
     'authority.workflow_runs', 'authority.workflow_runs',
     'observe', 'non_idempotent', TRUE,
     'binding.operation_catalog_registry.search_db.20260426',
     'decision.search_consolidation.20260426',
     'runtime.operations.queries.search.DbReadSearchQuery',
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

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, idempotency_policy, receipt_required
--    FROM operation_catalog_registry
--    WHERE operation_name LIKE 'search.%'
--    ORDER BY operation_name;
