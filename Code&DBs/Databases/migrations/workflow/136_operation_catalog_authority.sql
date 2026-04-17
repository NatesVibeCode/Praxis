BEGIN;

CREATE TABLE IF NOT EXISTS operation_catalog_registry (
    operation_ref TEXT PRIMARY KEY CHECK (btrim(operation_ref) <> ''),
    operation_name TEXT NOT NULL UNIQUE CHECK (btrim(operation_name) <> ''),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('operation_command', 'operation_query')),
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('command', 'query')),
    http_method TEXT NOT NULL CHECK (btrim(http_method) <> ''),
    http_path TEXT NOT NULL CHECK (btrim(http_path) <> ''),
    input_model_ref TEXT NOT NULL CHECK (btrim(input_model_ref) <> ''),
    handler_ref TEXT NOT NULL CHECK (btrim(handler_ref) <> ''),
    authority_ref TEXT NOT NULL CHECK (btrim(authority_ref) <> ''),
    projection_ref TEXT,
    posture TEXT CHECK (posture IN ('observe', 'operate', 'build')),
    idempotency_policy TEXT CHECK (idempotency_policy IN ('non_idempotent', 'idempotent', 'read_only')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS operation_catalog_registry_source_enabled_idx
    ON operation_catalog_registry (source_kind, enabled, operation_name);

CREATE INDEX IF NOT EXISTS operation_catalog_registry_method_path_idx
    ON operation_catalog_registry (http_method, http_path);

COMMENT ON TABLE operation_catalog_registry IS 'Canonical registry for operation definitions. This is the durable authority for operation identity, transport binding, and owning authority seams.';
COMMENT ON COLUMN operation_catalog_registry.binding_revision IS 'Revision stamp for the operation-catalog binding. Metadata changes must publish a new revision.';
COMMENT ON COLUMN operation_catalog_registry.decision_ref IS 'Decision authority that justified the current operation definition.';
COMMENT ON COLUMN operation_catalog_registry.projection_ref IS 'Optional projection or derived read model that the operation reads from when the authority seam is not a base table.';

CREATE TABLE IF NOT EXISTS operation_catalog_source_policy_registry (
    policy_ref TEXT PRIMARY KEY CHECK (btrim(policy_ref) <> ''),
    source_kind TEXT NOT NULL UNIQUE CHECK (source_kind IN ('operation_command', 'operation_query')),
    posture TEXT NOT NULL CHECK (posture IN ('observe', 'operate', 'build')),
    idempotency_policy TEXT NOT NULL CHECK (idempotency_policy IN ('non_idempotent', 'idempotent', 'read_only')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS operation_catalog_source_policy_registry_enabled_idx
    ON operation_catalog_source_policy_registry (enabled, source_kind);

COMMENT ON TABLE operation_catalog_source_policy_registry IS 'Canonical source-kind policy registry for operation definitions. Command and query rows inherit posture and idempotency defaults from here instead of import-time heuristics.';
COMMENT ON COLUMN operation_catalog_source_policy_registry.binding_revision IS 'Revision stamp for the source-policy binding. Policy changes must publish a new revision.';
COMMENT ON COLUMN operation_catalog_source_policy_registry.decision_ref IS 'Decision authority that justified the current operation source-policy row.';

INSERT INTO operation_catalog_source_policy_registry (
    policy_ref,
    source_kind,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref
) VALUES
    (
        'operation-command',
        'operation_command',
        'operate',
        'non_idempotent',
        TRUE,
        'binding.operation_catalog_source_policy_registry.bootstrap.20260416',
        'decision.operation_catalog_source_policy_registry.bootstrap.20260416'
    ),
    (
        'operation-query',
        'operation_query',
        'observe',
        'read_only',
        TRUE,
        'binding.operation_catalog_source_policy_registry.bootstrap.20260416',
        'decision.operation_catalog_source_policy_registry.bootstrap.20260416'
    )
ON CONFLICT (policy_ref) DO UPDATE SET
    source_kind = EXCLUDED.source_kind,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
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
    decision_ref
) VALUES
    (
        'workflow-build-mutate',
        'workflow_build.mutate',
        'operation_command',
        'command',
        'POST',
        '/api/workflows/{workflow_id}/build/{subpath:path}',
        'runtime.operations.commands.workflow_build.MutateWorkflowBuildCommand',
        'runtime.operations.commands.workflow_build.handle_mutate_workflow_build',
        'authority.workflow_build',
        NULL,
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.bootstrap.20260416',
        'decision.operation_catalog_registry.bootstrap.20260416'
    ),
    (
        'workflow-build-suggest-next',
        'workflow_build.suggest_next',
        'operation_query',
        'query',
        'POST',
        '/api/workflows/{workflow_id}/build/suggest-next',
        'runtime.operations.commands.suggest_next.SuggestNextNodesCommand',
        'runtime.operations.commands.suggest_next.handle_suggest_next_nodes',
        'authority.capability_catalog',
        'projection.capability_catalog',
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.bootstrap.20260416',
        'decision.operation_catalog_registry.bootstrap.20260416'
    ),
    (
        'operator-roadmap-tree',
        'operator.roadmap_tree',
        'operation_query',
        'query',
        'GET',
        '/api/operator/roadmap/tree/{root_roadmap_item_id}',
        'runtime.operations.queries.roadmap_tree.QueryRoadmapTree',
        'runtime.operations.queries.roadmap_tree.handle_query_roadmap_tree',
        'authority.roadmap_items',
        'projection.roadmap_tree',
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.bootstrap.20260416',
        'decision.operation_catalog_registry.bootstrap.20260416'
    ),
    (
        'operator-data-dictionary',
        'operator.data_dictionary',
        'operation_query',
        'query',
        'GET',
        '/api/operator/data-dictionary',
        'runtime.operations.queries.data_dictionary.QueryDataDictionary',
        'runtime.operations.queries.data_dictionary.handle_query_data_dictionary',
        'authority.memory_entities',
        'projection.memory_entities',
        NULL,
        NULL,
        TRUE,
        'binding.operation_catalog_registry.bootstrap.20260416',
        'decision.operation_catalog_registry.bootstrap.20260416'
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
    updated_at = now();

COMMIT;
