-- Migration 392: Durable Model Eval case runs and scorecards.
--
-- Keep suite/case/prompt fixture definitions in code where they version with
-- validators, and persist the operational truth the file-backed V1 lacked:
-- one case-run row per model/case/prompt/trial plus temporal scorecards.
-- Promotion proposals stay with Virtual Lab promotion authority; public
-- benchmarks stay candidate-profile metadata/priors.

BEGIN;

ALTER TABLE compile_artifacts
    DROP CONSTRAINT IF EXISTS compile_artifacts_artifact_kind_check;

ALTER TABLE compile_artifacts
    ADD CONSTRAINT compile_artifacts_artifact_kind_check
    CHECK (
        artifact_kind IN (
            'definition',
            'plan',
            'packet_lineage',
            'model_eval.request',
            'model_eval.raw_response',
            'model_eval.payload',
            'model_eval.emitted_file'
        )
    );

COMMENT ON COLUMN compile_artifacts.artifact_kind IS
    'Artifact class. Compile uses definition/plan/packet_lineage; Model Eval uses model_eval.request/raw_response/payload/emitted_file.';

CREATE TABLE IF NOT EXISTS model_eval_case_runs (
    case_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    matrix_receipt_id uuid NOT NULL REFERENCES authority_operation_receipts (receipt_id) ON DELETE CASCADE,
    task_id text NOT NULL CHECK (btrim(task_id) <> ''),
    suite_slug text NOT NULL CHECK (btrim(suite_slug) <> ''),
    family text NOT NULL CHECK (btrim(family) <> ''),
    config_id text NOT NULL CHECK (btrim(config_id) <> ''),
    prompt_variant_id text NOT NULL CHECK (btrim(prompt_variant_id) <> ''),
    model_config_json jsonb NOT NULL CHECK (jsonb_typeof(model_config_json) = 'object'),
    prompt_variant_json jsonb NOT NULL CHECK (jsonb_typeof(prompt_variant_json) = 'object'),
    prompt_hash text NOT NULL CHECK (btrim(prompt_hash) <> ''),
    fixture_hash text NOT NULL CHECK (btrim(fixture_hash) <> ''),
    provider_requested text NOT NULL DEFAULT '',
    provider_served text,
    model_served text,
    status text NOT NULL CHECK (
        status IN (
            'verified',
            'verification_failed',
            'parse_error',
            'api_error',
            'timeout',
            'privacy_rejected',
            'route_mismatch',
            'artifact_error',
            'permission_refused',
            'budget_stopped'
        )
    ),
    score numeric(8, 4) CHECK (score IS NULL OR (score >= 0 AND score <= 1)),
    checks_json jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(checks_json) = 'array'),
    input_tokens integer CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens integer CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cost_usd numeric(14, 8) CHECK (cost_usd IS NULL OR cost_usd >= 0),
    latency_ms integer CHECK (latency_ms IS NULL OR latency_ms >= 0),
    artifact_refs_json jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(artifact_refs_json) = 'array'),
    raw_response_hash text,
    child_receipt_id uuid REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    catalog_version_hash text NOT NULL CHECK (btrim(catalog_version_hash) <> ''),
    trial_number integer NOT NULL DEFAULT 1 CHECK (trial_number >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (
        matrix_receipt_id,
        task_id,
        config_id,
        prompt_variant_id,
        trial_number
    )
);

CREATE INDEX IF NOT EXISTS idx_model_eval_case_runs_matrix
    ON model_eval_case_runs (matrix_receipt_id, suite_slug, family, status);

CREATE INDEX IF NOT EXISTS idx_model_eval_case_runs_child_receipt
    ON model_eval_case_runs (child_receipt_id);

CREATE INDEX IF NOT EXISTS idx_model_eval_case_runs_score
    ON model_eval_case_runs (family, status, score DESC, cost_usd ASC, latency_ms ASC);

CREATE TABLE IF NOT EXISTS model_eval_scorecards (
    scorecard_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    matrix_receipt_id uuid NOT NULL REFERENCES authority_operation_receipts (receipt_id) ON DELETE CASCADE,
    config_id text NOT NULL CHECK (btrim(config_id) <> ''),
    model_slug text NOT NULL CHECK (btrim(model_slug) <> ''),
    family text NOT NULL CHECK (btrim(family) <> ''),
    trials integer NOT NULL DEFAULT 0 CHECK (trials >= 0),
    pass_count integer NOT NULL DEFAULT 0 CHECK (pass_count >= 0),
    pass_at_1 numeric(8, 4) NOT NULL DEFAULT 0 CHECK (pass_at_1 >= 0 AND pass_at_1 <= 1),
    mean_score numeric(8, 4) NOT NULL DEFAULT 0 CHECK (mean_score >= 0 AND mean_score <= 1),
    score_variance numeric(12, 8) NOT NULL DEFAULT 0 CHECK (score_variance >= 0),
    mean_cost_usd numeric(14, 8),
    mean_latency_ms integer,
    failure_counts_json jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(failure_counts_json) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (matrix_receipt_id, config_id, family)
);

CREATE INDEX IF NOT EXISTS idx_model_eval_scorecards_family
    ON model_eval_scorecards (family, pass_at_1 DESC, mean_score DESC, mean_cost_usd ASC, mean_latency_ms ASC);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'model_eval_case_runs',
        'Model Eval case runs',
        'table',
        'One row per Model Eval matrix cell: task, model config, prompt variant, trial, child receipt, score, cost, latency, provider route truth, and artifacts.',
        '{"migration":"392_model_eval_case_runs_and_scorecards.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.model_eval"}'::jsonb
    ),
    (
        'model_eval_scorecards',
        'Model Eval scorecards',
        'table',
        'Temporal scorecards aggregated by matrix receipt, model config, and task family.',
        '{"migration":"392_model_eval_case_runs_and_scorecards.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.model_eval"}'::jsonb
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
        'table.public.model_eval_case_runs',
        'table',
        'model_eval_case_runs',
        'public',
        'authority.model_eval',
        'model_eval_case_runs',
        'active',
        'command_model',
        'praxis.engine',
        'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
        '{}'::jsonb
    ),
    (
        'table.public.model_eval_scorecards',
        'table',
        'model_eval_scorecards',
        'public',
        'authority.model_eval',
        'model_eval_scorecards',
        'active',
        'projection',
        'praxis.engine',
        'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
        '{}'::jsonb
    )
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
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
    'model_eval_case_run_completed_event',
    'model_eval.case_run.completed event payload',
    'event',
    'Conceptual event emitted when one Model Eval case run completes and writes score/provenance.',
    '{"migration":"392_model_eval_case_runs_and_scorecards.sql"}'::jsonb,
    '{"event_type":"model_eval.case_run.completed","authority_domain_ref":"authority.model_eval"}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
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
) VALUES (
    'event_contract.model_eval.case_run.completed',
    'model_eval.case_run.completed',
    'authority.model_eval',
    'data_dictionary.object.model_eval_case_run_completed_event',
    'operation_ref',
    '[]'::jsonb,
    '["projection.model_eval_runs"]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    jsonb_build_object(
        'expected_payload_fields',
        jsonb_build_array(
            'case_run_id',
            'task_id',
            'suite_slug',
            'family',
            'config_id',
            'model_slug',
            'agent',
            'model_eval_candidate_ref',
            'status',
            'score',
            'cost_usd',
            'latency_ms'
        )
    )
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

SELECT register_operation_atomic(
    p_operation_ref         := 'model_eval.command.run_case',
    p_operation_name        := 'model_eval_run_case',
    p_handler_ref           := 'runtime.operations.commands.model_eval.handle_model_eval_run_case',
    p_input_model_ref       := 'runtime.operations.commands.model_eval.ModelEvalRunCaseCommand',
    p_authority_domain_ref  := 'authority.model_eval',
    p_authority_ref         := 'authority.model_eval',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/model_eval_run_case',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_eval.case_run.completed',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 120000,
    p_execution_lane        := 'background',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'operator_decision.architecture_policy.model_eval.model_eval_authority_supersedes_scratch_lab',
    p_binding_revision      := 'binding.operation_catalog_registry.model_eval_run_case.20260501',
    p_label                 := 'Model Eval Run Case',
    p_summary               := 'Run one model/case/prompt/trial cell and write score/provenance with a child receipt.'
);

COMMIT;
