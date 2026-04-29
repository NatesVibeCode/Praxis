-- Migration 327: Provider reasoning-effort matrix.
--
-- Effort is a routing axis, not a hidden provider-specific blob. These tables
-- give the router one DB-owned place to answer:
--   task contract -> internal effort slug -> provider/model/transport payload.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.provider_routing',
    'praxis.engine',
    'stream.authority.provider_routing',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS provider_reasoning_effort_matrix (
    effort_matrix_ref text PRIMARY KEY CHECK (btrim(effort_matrix_ref) <> ''),
    provider_slug text NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug text NOT NULL CHECK (btrim(model_slug) <> ''),
    transport_type text NOT NULL DEFAULT 'cli' CHECK (btrim(transport_type) <> ''),
    effort_slug text NOT NULL CHECK (effort_slug IN ('instant', 'low', 'medium', 'high', 'max')),
    supported boolean NOT NULL DEFAULT true,
    provider_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    cost_multiplier numeric(8,4) NOT NULL DEFAULT 1.0 CHECK (cost_multiplier >= 0),
    latency_multiplier numeric(8,4) NOT NULL DEFAULT 1.0 CHECK (latency_multiplier >= 0),
    quality_bias numeric(8,4) NOT NULL DEFAULT 0.0,
    failure_risk numeric(8,4) NOT NULL DEFAULT 0.0 CHECK (failure_risk >= 0),
    decision_ref text NULL CHECK (decision_ref IS NULL OR btrim(decision_ref) <> ''),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_reasoning_effort_matrix_payload_object_check
        CHECK (jsonb_typeof(provider_payload) = 'object'),
    CONSTRAINT provider_reasoning_effort_matrix_metadata_object_check
        CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT provider_reasoning_effort_matrix_unique_route
        UNIQUE (provider_slug, model_slug, transport_type, effort_slug)
);

CREATE INDEX IF NOT EXISTS idx_provider_reasoning_effort_matrix_route
    ON provider_reasoning_effort_matrix (provider_slug, model_slug, transport_type, effort_slug)
    WHERE supported = true;

CREATE TABLE IF NOT EXISTS task_type_effort_policy (
    task_type text NOT NULL CHECK (btrim(task_type) <> ''),
    sub_task_type text NOT NULL DEFAULT '*' CHECK (btrim(sub_task_type) <> ''),
    default_effort_slug text NOT NULL CHECK (default_effort_slug IN ('instant', 'low', 'medium', 'high', 'max')),
    min_effort_slug text NOT NULL DEFAULT 'instant' CHECK (min_effort_slug IN ('instant', 'low', 'medium', 'high', 'max')),
    max_effort_slug text NOT NULL DEFAULT 'max' CHECK (max_effort_slug IN ('instant', 'low', 'medium', 'high', 'max')),
    escalation_rules jsonb NOT NULL DEFAULT '{}'::jsonb,
    decision_ref text NULL CHECK (decision_ref IS NULL OR btrim(decision_ref) <> ''),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_type, sub_task_type),
    CONSTRAINT task_type_effort_policy_escalation_object_check
        CHECK (jsonb_typeof(escalation_rules) = 'object'),
    CONSTRAINT task_type_effort_policy_metadata_object_check
        CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT task_type_effort_policy_rank_order_check CHECK (
        CASE min_effort_slug
            WHEN 'instant' THEN 1 WHEN 'low' THEN 2 WHEN 'medium' THEN 3 WHEN 'high' THEN 4 ELSE 5
        END
        <=
        CASE default_effort_slug
            WHEN 'instant' THEN 1 WHEN 'low' THEN 2 WHEN 'medium' THEN 3 WHEN 'high' THEN 4 ELSE 5
        END
        AND
        CASE default_effort_slug
            WHEN 'instant' THEN 1 WHEN 'low' THEN 2 WHEN 'medium' THEN 3 WHEN 'high' THEN 4 ELSE 5
        END
        <=
        CASE max_effort_slug
            WHEN 'instant' THEN 1 WHEN 'low' THEN 2 WHEN 'medium' THEN 3 WHEN 'high' THEN 4 ELSE 5
        END
    )
);

CREATE OR REPLACE FUNCTION touch_provider_reasoning_effort_matrix_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_provider_reasoning_effort_matrix_touch ON provider_reasoning_effort_matrix;
CREATE TRIGGER trg_provider_reasoning_effort_matrix_touch
    BEFORE UPDATE ON provider_reasoning_effort_matrix
    FOR EACH ROW EXECUTE FUNCTION touch_provider_reasoning_effort_matrix_updated_at();

CREATE OR REPLACE FUNCTION touch_task_type_effort_policy_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_task_type_effort_policy_touch ON task_type_effort_policy;
CREATE TRIGGER trg_task_type_effort_policy_touch
    BEFORE UPDATE ON task_type_effort_policy
    FOR EACH ROW EXECUTE FUNCTION touch_task_type_effort_policy_updated_at();

WITH effort_axis(effort_slug, effort_rank, cost_multiplier, latency_multiplier, quality_bias) AS (
    VALUES
        ('instant', 1, 0.75::numeric, 0.50::numeric, -0.15::numeric),
        ('low',     2, 0.90::numeric, 0.75::numeric, -0.05::numeric),
        ('medium',  3, 1.00::numeric, 1.00::numeric,  0.00::numeric),
        ('high',    4, 1.35::numeric, 1.50::numeric,  0.15::numeric),
        ('max',     5, 1.75::numeric, 2.25::numeric,  0.25::numeric)
),
active_candidates AS (
    SELECT DISTINCT ON (provider_slug, model_slug)
           provider_slug,
           model_slug,
           COALESCE(NULLIF(lower(cli_config ->> 'transport_type'), ''), 'cli') AS transport_type
    FROM provider_model_candidates
    WHERE status = 'active'
    ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
)
INSERT INTO provider_reasoning_effort_matrix (
    effort_matrix_ref,
    provider_slug,
    model_slug,
    transport_type,
    effort_slug,
    supported,
    provider_payload,
    cost_multiplier,
    latency_multiplier,
    quality_bias,
    decision_ref,
    metadata
)
SELECT
    'reasoning_effort.'
        || candidate.provider_slug || '.'
        || regexp_replace(candidate.model_slug, '[^a-zA-Z0-9]+', '-', 'g') || '.'
        || candidate.transport_type || '.'
        || effort.effort_slug AS effort_matrix_ref,
    candidate.provider_slug,
    candidate.model_slug,
    candidate.transport_type,
    effort.effort_slug,
    true AS supported,
    CASE
        WHEN candidate.provider_slug = 'openai' THEN
            jsonb_build_object(
                'provider', 'openai',
                'reasoning_effort', CASE effort.effort_slug
                    WHEN 'instant' THEN 'low'
                    WHEN 'low' THEN 'low'
                    WHEN 'medium' THEN 'medium'
                    ELSE 'high'
                END
            )
        WHEN candidate.provider_slug = 'anthropic' THEN
            jsonb_build_object(
                'provider', 'anthropic',
                'thinking', jsonb_build_object(
                    'type', CASE WHEN effort.effort_slug IN ('instant', 'low') THEN 'disabled' ELSE 'enabled' END,
                    'budget_tokens', CASE effort.effort_slug
                        WHEN 'instant' THEN 0
                        WHEN 'low' THEN 0
                        WHEN 'medium' THEN 4096
                        WHEN 'high' THEN 12000
                        ELSE 24000
                    END
                )
            )
        WHEN candidate.provider_slug = 'google' THEN
            jsonb_build_object(
                'provider', 'google',
                'thinking_budget', CASE effort.effort_slug
                    WHEN 'instant' THEN 0
                    WHEN 'low' THEN 1024
                    WHEN 'medium' THEN 4096
                    WHEN 'high' THEN 12000
                    ELSE 24000
                END
            )
        ELSE
            jsonb_build_object(
                'provider', candidate.provider_slug,
                'internal_effort_slug', effort.effort_slug
            )
    END AS provider_payload,
    effort.cost_multiplier,
    effort.latency_multiplier,
    effort.quality_bias,
    'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension',
    jsonb_build_object('source', 'migration.326_provider_reasoning_effort_matrix')
FROM active_candidates candidate
CROSS JOIN effort_axis effort
ON CONFLICT (provider_slug, model_slug, transport_type, effort_slug) DO UPDATE SET
    supported = EXCLUDED.supported,
    provider_payload = EXCLUDED.provider_payload,
    cost_multiplier = EXCLUDED.cost_multiplier,
    latency_multiplier = EXCLUDED.latency_multiplier,
    quality_bias = EXCLUDED.quality_bias,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO task_type_effort_policy (
    task_type,
    sub_task_type,
    default_effort_slug,
    min_effort_slug,
    max_effort_slug,
    escalation_rules,
    decision_ref,
    metadata
) VALUES
    ('build', '*', 'medium', 'low', 'high', '{"on_failed_verification":"high","on_contract_ambiguity":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('debug', '*', 'high', 'medium', 'max', '{"on_repeated_failure":"max","on_flaky_reproduction":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('review', '*', 'medium', 'low', 'high', '{"on_security_or_data_loss_risk":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('architecture', '*', 'high', 'medium', 'max', '{"on_cross_authority_change":"max"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('analysis', '*', 'medium', 'low', 'high', '{"on_low_confidence":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('wiring', '*', 'low', 'instant', 'medium', '{"on_test_failure":"medium"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('test', '*', 'medium', 'low', 'high', '{"on_collection_or_fixture_failure":"high"}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb),
    ('chat', '*', 'low', 'instant', 'medium', '{}'::jsonb, 'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension', '{"source":"migration.326"}'::jsonb)
ON CONFLICT (task_type, sub_task_type) DO UPDATE SET
    default_effort_slug = EXCLUDED.default_effort_slug,
    min_effort_slug = EXCLUDED.min_effort_slug,
    max_effort_slug = EXCLUDED.max_effort_slug,
    escalation_rules = EXCLUDED.escalation_rules,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

CREATE OR REPLACE VIEW effective_task_type_effort_routes AS
SELECT
    routing.task_type,
    routing.sub_task_type,
    routing.provider_slug,
    routing.model_slug,
    lower(routing.transport_type) AS transport_type,
    policy.default_effort_slug AS effort_slug,
    policy.min_effort_slug,
    policy.max_effort_slug,
    effort.provider_payload,
    effort.cost_multiplier,
    effort.latency_multiplier,
    effort.quality_bias,
    routing.rank,
    routing.benchmark_score,
    routing.cost_per_m_tokens,
    routing.route_health_score,
    routing.permitted,
    effort.supported AS effort_supported,
    policy.decision_ref AS effort_policy_decision_ref,
    effort.decision_ref AS effort_matrix_decision_ref
FROM task_type_routing routing
JOIN task_type_effort_policy policy
  ON policy.task_type = routing.task_type
 AND policy.sub_task_type = routing.sub_task_type
JOIN provider_reasoning_effort_matrix effort
  ON effort.provider_slug = routing.provider_slug
 AND effort.model_slug = routing.model_slug
 AND effort.transport_type = lower(routing.transport_type)
 AND effort.effort_slug = policy.default_effort_slug
WHERE routing.permitted = true;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'provider_reasoning_effort_matrix',
        'Provider reasoning effort matrix',
        'table',
        'Provider/model/transport mapping from Praxis effort slugs to provider-specific reasoning controls.',
        '{"migration":"327_provider_reasoning_effort_matrix.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.provider_routing"}'::jsonb
    ),
    (
        'task_type_effort_policy',
        'Task type effort policy',
        'table',
        'Task contract defaults and allowed bounds for internal reasoning effort slugs.',
        '{"migration":"327_provider_reasoning_effort_matrix.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.provider_routing"}'::jsonb
    ),
    (
        'table:effective_task_type_effort_routes',
        'Effective task type effort routes',
        'projection',
        'Queryable read model joining task routes, task effort policy, and provider effort payloads.',
        '{"migration":"327_provider_reasoning_effort_matrix.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.provider_routing"}'::jsonb
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
) VALUES
    (
        'table.public.provider_reasoning_effort_matrix',
        'table',
        'provider_reasoning_effort_matrix',
        'public',
        'authority.provider_routing',
        'provider_reasoning_effort_matrix',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension',
        '{"purpose":"effort payload authority"}'::jsonb
    ),
    (
        'table.public.task_type_effort_policy',
        'table',
        'task_type_effort_policy',
        'public',
        'authority.provider_routing',
        'task_type_effort_policy',
        'active',
        'registry',
        'praxis.engine',
        'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension',
        '{"purpose":"task effort contract authority"}'::jsonb
    ),
    (
        'view.public.effective_task_type_effort_routes',
        'projection',
        'effective_task_type_effort_routes',
        'public',
        'authority.provider_routing',
        'table:effective_task_type_effort_routes',
        'active',
        'read_model',
        'praxis.engine',
        'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension',
        '{"purpose":"route plus effort read model"}'::jsonb
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

COMMIT;
