-- Migration 410: Durable workflow repair queue.
--
-- Failed Solutions, Workflows, and Jobs should become inspectable repair work
-- automatically. The queue keeps the built workflow/run/job references alive
-- so operators and future agents can diagnose, claim, and retry with an
-- explicit delta instead of rediscovering failures from chat history.

BEGIN;

CREATE TABLE IF NOT EXISTS workflow_repair_queue (
    repair_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repair_scope TEXT NOT NULL,
    queue_status TEXT NOT NULL DEFAULT 'queued',
    auto_repair BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,

    solution_id TEXT,
    wave_id TEXT,
    workflow_id TEXT,
    run_id TEXT,
    job_id BIGINT,
    job_label TEXT,
    workflow_phase TEXT,
    spec_path TEXT,
    command_id TEXT,

    reason_code TEXT NOT NULL,
    failure_code TEXT,
    failure_category TEXT,
    failure_zone TEXT,
    is_transient BOOLEAN NOT NULL DEFAULT FALSE,

    repair_strategy TEXT NOT NULL DEFAULT 'diagnose_then_retry',
    retry_delta_required BOOLEAN NOT NULL DEFAULT TRUE,
    source_kind TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    evidence_kind TEXT,
    evidence_ref TEXT,
    repair_dedupe_key TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    result_ref TEXT,
    repair_note TEXT,
    created_by_ref TEXT NOT NULL DEFAULT 'workflow_repair_auto_enqueue',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT workflow_repair_queue_scope_check
        CHECK (repair_scope IN ('solution', 'workflow', 'job')),
    CONSTRAINT workflow_repair_queue_status_check
        CHECK (queue_status IN (
            'queued',
            'claimed',
            'repairing',
            'completed',
            'failed',
            'cancelled',
            'superseded'
        )),
    CONSTRAINT workflow_repair_queue_payload_object_check
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT workflow_repair_queue_solution_target_check
        CHECK (repair_scope <> 'solution' OR solution_id IS NOT NULL),
    CONSTRAINT workflow_repair_queue_workflow_target_check
        CHECK (repair_scope <> 'workflow' OR run_id IS NOT NULL),
    CONSTRAINT workflow_repair_queue_job_target_check
        CHECK (repair_scope <> 'job' OR (run_id IS NOT NULL AND (job_id IS NOT NULL OR job_label IS NOT NULL)))
);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_repair_queue_open_dedupe_idx
    ON workflow_repair_queue (repair_dedupe_key)
    WHERE queue_status IN ('queued', 'claimed', 'repairing');

CREATE INDEX IF NOT EXISTS workflow_repair_queue_status_priority_idx
    ON workflow_repair_queue (queue_status, priority, created_at);

CREATE INDEX IF NOT EXISTS workflow_repair_queue_run_idx
    ON workflow_repair_queue (run_id, queue_status, created_at DESC)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS workflow_repair_queue_solution_idx
    ON workflow_repair_queue (solution_id, queue_status, created_at DESC)
    WHERE solution_id IS NOT NULL;

CREATE OR REPLACE FUNCTION workflow_repair_touch_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_repair_queue_touch_updated_at
    ON workflow_repair_queue;

CREATE TRIGGER workflow_repair_queue_touch_updated_at
    BEFORE UPDATE ON workflow_repair_queue
    FOR EACH ROW
    EXECUTE FUNCTION workflow_repair_touch_updated_at();

CREATE OR REPLACE FUNCTION workflow_repair_enqueue_from_workflow_run()
RETURNS trigger AS $$
DECLARE
    v_state TEXT := COALESCE(NEW.current_state, '');
    v_prior_state TEXT := '';
    v_reason TEXT := COALESCE(NULLIF(NEW.terminal_reason_code, ''), 'workflow_terminal_' || COALESCE(NEW.current_state, 'unknown'));
    v_spec_path TEXT := COALESCE(
        NULLIF(NEW.request_envelope->>'spec_path', ''),
        NULLIF(NEW.request_envelope#>>'{spec_snapshot,path}', ''),
        NULLIF(NEW.request_envelope#>>'{spec_snapshot,source_path}', ''),
        NULLIF(NEW.request_envelope#>>'{spec_snapshot,spec_path}', '')
    );
    v_job RECORD;
    v_solution RECORD;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        v_prior_state := COALESCE(OLD.current_state, '');
    END IF;

    IF v_state NOT IN ('failed', 'dead_letter', 'partial_success') THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' AND v_prior_state = v_state THEN
        RETURN NEW;
    END IF;

    FOR v_job IN
        SELECT id, label, phase, status, last_error_code, failure_category,
               failure_zone, is_transient, stdout_preview, attempt, max_attempts,
               agent_slug, resolved_agent
        FROM workflow_jobs
        WHERE run_id = NEW.run_id
          AND status IN ('failed', 'dead_letter', 'blocked')
    LOOP
        WITH inserted AS (
            INSERT INTO workflow_repair_queue (
                repair_scope,
                workflow_id,
                run_id,
                job_id,
                job_label,
                workflow_phase,
                spec_path,
                reason_code,
                failure_code,
                failure_category,
                failure_zone,
                is_transient,
                repair_strategy,
                source_kind,
                source_ref,
                evidence_kind,
                evidence_ref,
                repair_dedupe_key,
                payload
            ) VALUES (
                'job',
                NEW.workflow_id,
                NEW.run_id,
                v_job.id,
                v_job.label,
                v_job.phase,
                v_spec_path,
                COALESCE(NULLIF(v_job.last_error_code, ''), v_reason),
                NULLIF(v_job.last_error_code, ''),
                NULLIF(v_job.failure_category, ''),
                NULLIF(v_job.failure_zone, ''),
                COALESCE(v_job.is_transient, FALSE),
                'diagnose_job_then_retry',
                'workflow_job',
                v_job.id::text,
                'workflow_run',
                NEW.run_id,
                'job:' || md5(NEW.run_id || '|' || v_job.id::text || '|' || v_job.status),
                jsonb_build_object(
                    'run_state', v_state,
                    'terminal_reason_code', NEW.terminal_reason_code,
                    'job_status', v_job.status,
                    'attempt', v_job.attempt,
                    'max_attempts', v_job.max_attempts,
                    'agent_slug', v_job.agent_slug,
                    'resolved_agent', v_job.resolved_agent,
                    'stdout_preview', v_job.stdout_preview
                )
            )
            ON CONFLICT DO NOTHING
            RETURNING repair_id, repair_scope, reason_code, payload
        )
        INSERT INTO system_events (event_type, source_id, source_type, payload)
        SELECT
            'workflow.repair.queued',
            repair_id::text,
            'workflow_repair_queue',
            jsonb_build_object(
                'repair_id', repair_id::text,
                'repair_scope', repair_scope,
                'workflow_id', NEW.workflow_id,
                'run_id', NEW.run_id,
                'job_id', v_job.id,
                'job_label', v_job.label,
                'reason_code', reason_code,
                'payload', payload
            )
        FROM inserted;
    END LOOP;

    IF v_state IN ('failed', 'dead_letter') THEN
        WITH inserted AS (
            INSERT INTO workflow_repair_queue (
                repair_scope,
                workflow_id,
                run_id,
                spec_path,
                reason_code,
                failure_code,
                repair_strategy,
                source_kind,
                source_ref,
                evidence_kind,
                evidence_ref,
                repair_dedupe_key,
                payload
            ) VALUES (
                'workflow',
                NEW.workflow_id,
                NEW.run_id,
                v_spec_path,
                v_reason,
                v_reason,
                'diagnose_workflow_then_retry_or_resubmit',
                'workflow_run',
                NEW.run_id,
                'workflow_run',
                NEW.run_id,
                'workflow:' || md5(NEW.run_id || '|' || v_state),
                jsonb_build_object(
                    'run_state', v_state,
                    'terminal_reason_code', NEW.terminal_reason_code,
                    'request_envelope', NEW.request_envelope
                )
            )
            ON CONFLICT DO NOTHING
            RETURNING repair_id, repair_scope, reason_code, payload
        )
        INSERT INTO system_events (event_type, source_id, source_type, payload)
        SELECT
            'workflow.repair.queued',
            repair_id::text,
            'workflow_repair_queue',
            jsonb_build_object(
                'repair_id', repair_id::text,
                'repair_scope', repair_scope,
                'workflow_id', NEW.workflow_id,
                'run_id', NEW.run_id,
                'reason_code', reason_code,
                'payload', payload
            )
        FROM inserted;

        FOR v_solution IN
            SELECT wr.chain_id, wr.wave_id, wr.spec_path, wr.command_id,
                   wr.submission_status, wr.run_status,
                   wc.last_error_code, wc.last_error_detail
            FROM workflow_chain_wave_runs wr
            LEFT JOIN workflow_chains wc ON wc.chain_id = wr.chain_id
            WHERE wr.run_id = NEW.run_id
        LOOP
            WITH inserted AS (
                INSERT INTO workflow_repair_queue (
                    repair_scope,
                    solution_id,
                    wave_id,
                    workflow_id,
                    run_id,
                    spec_path,
                    command_id,
                    reason_code,
                    failure_code,
                    repair_strategy,
                    source_kind,
                    source_ref,
                    evidence_kind,
                    evidence_ref,
                    repair_dedupe_key,
                    payload
                ) VALUES (
                    'solution',
                    v_solution.chain_id,
                    v_solution.wave_id,
                    NEW.workflow_id,
                    NEW.run_id,
                    COALESCE(v_solution.spec_path, v_spec_path),
                    v_solution.command_id,
                    COALESCE(NULLIF(v_solution.last_error_code, ''), v_reason, 'workflow.solution.run_failed'),
                    COALESCE(NULLIF(v_solution.last_error_code, ''), v_reason),
                    'diagnose_solution_wave_then_repair_or_resubmit',
                    'workflow_solution',
                    v_solution.chain_id,
                    'workflow_run',
                    NEW.run_id,
                    'solution:' || md5(v_solution.chain_id || '|' || v_solution.wave_id || '|' || COALESCE(v_solution.spec_path, '') || '|' || COALESCE(NEW.run_id, '') || '|' || v_state),
                    jsonb_build_object(
                        'solution_status_source', 'workflow_run_terminal',
                        'run_state', v_state,
                        'wave_id', v_solution.wave_id,
                        'submission_status', v_solution.submission_status,
                        'run_status', v_solution.run_status,
                        'last_error_detail', v_solution.last_error_detail
                    )
                )
                ON CONFLICT DO NOTHING
                RETURNING repair_id, repair_scope, reason_code, payload
            )
            INSERT INTO system_events (event_type, source_id, source_type, payload)
            SELECT
                'workflow.repair.queued',
                repair_id::text,
                'workflow_repair_queue',
                jsonb_build_object(
                    'repair_id', repair_id::text,
                    'repair_scope', repair_scope,
                    'solution_id', v_solution.chain_id,
                    'wave_id', v_solution.wave_id,
                    'workflow_id', NEW.workflow_id,
                    'run_id', NEW.run_id,
                    'reason_code', reason_code,
                    'payload', payload
                )
            FROM inserted;
        END LOOP;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_repair_enqueue_workflow_run_terminal
    ON workflow_runs;

CREATE TRIGGER workflow_repair_enqueue_workflow_run_terminal
    AFTER INSERT OR UPDATE OF current_state ON workflow_runs
    FOR EACH ROW
    EXECUTE FUNCTION workflow_repair_enqueue_from_workflow_run();

CREATE OR REPLACE FUNCTION workflow_repair_enqueue_from_solution_wave()
RETURNS trigger AS $$
DECLARE
    v_failure_status TEXT := '';
    v_reason TEXT;
    v_last_error_code TEXT;
    v_last_error_detail TEXT;
BEGIN
    IF COALESCE(NEW.run_status, '') IN ('failed', 'dead_letter', 'missing') THEN
        v_failure_status := NEW.run_status;
    ELSIF COALESCE(NEW.submission_status, '') IN ('failed', 'dead_letter', 'missing') THEN
        v_failure_status := NEW.submission_status;
    END IF;

    IF v_failure_status = '' THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE'
       AND COALESCE(OLD.run_status, '') = COALESCE(NEW.run_status, '')
       AND COALESCE(OLD.submission_status, '') = COALESCE(NEW.submission_status, '') THEN
        RETURN NEW;
    END IF;

    SELECT last_error_code, last_error_detail
    INTO v_last_error_code, v_last_error_detail
    FROM workflow_chains
    WHERE chain_id = NEW.chain_id;

    v_reason := COALESCE(NULLIF(v_last_error_code, ''), 'workflow.solution.wave_' || v_failure_status);

    WITH inserted AS (
        INSERT INTO workflow_repair_queue (
            repair_scope,
            solution_id,
            wave_id,
            workflow_id,
            run_id,
            spec_path,
            command_id,
            reason_code,
            failure_code,
            repair_strategy,
            source_kind,
            source_ref,
            evidence_kind,
            evidence_ref,
            repair_dedupe_key,
            payload
        ) VALUES (
            'solution',
            NEW.chain_id,
            NEW.wave_id,
            NEW.workflow_id,
            NEW.run_id,
            NEW.spec_path,
            NEW.command_id,
            v_reason,
            v_reason,
            'diagnose_solution_wave_then_repair_or_resubmit',
            'workflow_solution_wave',
            NEW.chain_id || ':' || NEW.wave_id,
            CASE WHEN NEW.run_id IS NULL THEN 'workflow_chain_wave_run' ELSE 'workflow_run' END,
            COALESCE(NEW.run_id, NEW.chain_id || ':' || NEW.wave_id || ':' || NEW.spec_path),
            'solution:' || md5(NEW.chain_id || '|' || NEW.wave_id || '|' || COALESCE(NEW.spec_path, '') || '|' || COALESCE(NEW.run_id, '') || '|' || v_failure_status),
            jsonb_build_object(
                'solution_status_source', 'workflow_chain_wave_run',
                'failure_status', v_failure_status,
                'submission_status', NEW.submission_status,
                'run_status', NEW.run_status,
                'ordinal', NEW.ordinal,
                'spec_name', NEW.spec_name,
                'last_error_detail', v_last_error_detail
            )
        )
        ON CONFLICT DO NOTHING
        RETURNING repair_id, repair_scope, reason_code, payload
    )
    INSERT INTO system_events (event_type, source_id, source_type, payload)
    SELECT
        'workflow.repair.queued',
        repair_id::text,
        'workflow_repair_queue',
        jsonb_build_object(
            'repair_id', repair_id::text,
            'repair_scope', repair_scope,
            'solution_id', NEW.chain_id,
            'wave_id', NEW.wave_id,
            'workflow_id', NEW.workflow_id,
            'run_id', NEW.run_id,
            'reason_code', reason_code,
            'payload', payload
        )
    FROM inserted;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS workflow_repair_enqueue_solution_wave_terminal
    ON workflow_chain_wave_runs;

CREATE TRIGGER workflow_repair_enqueue_solution_wave_terminal
    AFTER INSERT OR UPDATE OF submission_status, run_status, run_id
    ON workflow_chain_wave_runs
    FOR EACH ROW
    EXECUTE FUNCTION workflow_repair_enqueue_from_solution_wave();

INSERT INTO workflow_repair_queue (
    repair_scope,
    workflow_id,
    run_id,
    job_id,
    job_label,
    workflow_phase,
    spec_path,
    reason_code,
    failure_code,
    failure_category,
    failure_zone,
    is_transient,
    repair_strategy,
    source_kind,
    source_ref,
    evidence_kind,
    evidence_ref,
    repair_dedupe_key,
    payload,
    created_by_ref
)
SELECT
    'job',
    wr.workflow_id,
    wr.run_id,
    wj.id,
    wj.label,
    wj.phase,
    COALESCE(
        NULLIF(wr.request_envelope->>'spec_path', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,path}', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,source_path}', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,spec_path}', '')
    ),
    COALESCE(NULLIF(wj.last_error_code, ''), NULLIF(wr.terminal_reason_code, ''), 'workflow_terminal_' || wr.current_state),
    NULLIF(wj.last_error_code, ''),
    NULLIF(wj.failure_category, ''),
    NULLIF(wj.failure_zone, ''),
    COALESCE(wj.is_transient, FALSE),
    'diagnose_job_then_retry',
    'workflow_job',
    wj.id::text,
    'workflow_run',
    wr.run_id,
    'job:' || md5(wr.run_id || '|' || wj.id::text || '|' || wj.status),
    jsonb_build_object(
        'backfilled', TRUE,
        'run_state', wr.current_state,
        'terminal_reason_code', wr.terminal_reason_code,
        'job_status', wj.status,
        'attempt', wj.attempt,
        'max_attempts', wj.max_attempts,
        'agent_slug', wj.agent_slug,
        'resolved_agent', wj.resolved_agent,
        'stdout_preview', wj.stdout_preview
    ),
    'workflow_repair_backfill'
FROM workflow_runs wr
JOIN workflow_jobs wj ON wj.run_id = wr.run_id
WHERE wr.current_state IN ('failed', 'dead_letter', 'partial_success')
  AND wj.status IN ('failed', 'dead_letter', 'blocked')
ON CONFLICT DO NOTHING;

INSERT INTO workflow_repair_queue (
    repair_scope,
    workflow_id,
    run_id,
    spec_path,
    reason_code,
    failure_code,
    repair_strategy,
    source_kind,
    source_ref,
    evidence_kind,
    evidence_ref,
    repair_dedupe_key,
    payload,
    created_by_ref
)
SELECT
    'workflow',
    wr.workflow_id,
    wr.run_id,
    COALESCE(
        NULLIF(wr.request_envelope->>'spec_path', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,path}', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,source_path}', ''),
        NULLIF(wr.request_envelope#>>'{spec_snapshot,spec_path}', '')
    ),
    COALESCE(NULLIF(wr.terminal_reason_code, ''), 'workflow_terminal_' || wr.current_state),
    COALESCE(NULLIF(wr.terminal_reason_code, ''), 'workflow_terminal_' || wr.current_state),
    'diagnose_workflow_then_retry_or_resubmit',
    'workflow_run',
    wr.run_id,
    'workflow_run',
    wr.run_id,
    'workflow:' || md5(wr.run_id || '|' || wr.current_state),
    jsonb_build_object(
        'backfilled', TRUE,
        'run_state', wr.current_state,
        'terminal_reason_code', wr.terminal_reason_code,
        'request_envelope', wr.request_envelope
    ),
    'workflow_repair_backfill'
FROM workflow_runs wr
WHERE wr.current_state IN ('failed', 'dead_letter')
ON CONFLICT DO NOTHING;

INSERT INTO workflow_repair_queue (
    repair_scope,
    solution_id,
    wave_id,
    workflow_id,
    run_id,
    spec_path,
    command_id,
    reason_code,
    failure_code,
    repair_strategy,
    source_kind,
    source_ref,
    evidence_kind,
    evidence_ref,
    repair_dedupe_key,
    payload,
    created_by_ref
)
SELECT
    'solution',
    wcr.chain_id,
    wcr.wave_id,
    wcr.workflow_id,
    wcr.run_id,
    wcr.spec_path,
    wcr.command_id,
    COALESCE(NULLIF(wc.last_error_code, ''), 'workflow.solution.wave_' || COALESCE(NULLIF(wcr.run_status, ''), NULLIF(wcr.submission_status, ''), 'failed')),
    COALESCE(NULLIF(wc.last_error_code, ''), 'workflow.solution.wave_' || COALESCE(NULLIF(wcr.run_status, ''), NULLIF(wcr.submission_status, ''), 'failed')),
    'diagnose_solution_wave_then_repair_or_resubmit',
    'workflow_solution_wave',
    wcr.chain_id || ':' || wcr.wave_id,
    CASE WHEN wcr.run_id IS NULL THEN 'workflow_chain_wave_run' ELSE 'workflow_run' END,
    COALESCE(wcr.run_id, wcr.chain_id || ':' || wcr.wave_id || ':' || wcr.spec_path),
    'solution:' || md5(wcr.chain_id || '|' || wcr.wave_id || '|' || COALESCE(wcr.spec_path, '') || '|' || COALESCE(wcr.run_id, '') || '|' || COALESCE(NULLIF(wcr.run_status, ''), NULLIF(wcr.submission_status, ''), 'failed')),
    jsonb_build_object(
        'backfilled', TRUE,
        'submission_status', wcr.submission_status,
        'run_status', wcr.run_status,
        'ordinal', wcr.ordinal,
        'spec_name', wcr.spec_name,
        'last_error_detail', wc.last_error_detail
    ),
    'workflow_repair_backfill'
FROM workflow_chain_wave_runs wcr
LEFT JOIN workflow_chains wc ON wc.chain_id = wcr.chain_id
WHERE wcr.run_status IN ('failed', 'dead_letter', 'missing')
   OR wcr.submission_status IN ('failed', 'dead_letter', 'missing')
ON CONFLICT DO NOTHING;

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
    'authority_object.workflow_repair_queue',
    'table',
    'workflow_repair_queue',
    'public',
    'authority.workflow_runs',
    'workflow_repair_queue',
    'active',
    'command_model',
    'praxis.workflow',
    'conversation.workflow_repair_queue.20260501',
    '{"source":"migration_410","scopes":["solution","workflow","job"],"auto_enqueue":true}'::jsonb
)
ON CONFLICT (object_ref) DO UPDATE SET
    lifecycle_status = EXCLUDED.lifecycle_status,
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
    'workflow_repair_queue',
    'Workflow Repair Queue',
    'table',
    'Durable queue of automatically enqueued repair intents for failed Solutions, Workflows, and Jobs. Preserves built spec, run, and job references so future agents can diagnose and retry with an explicit delta.',
    '{"migration":"410_workflow_repair_queue.sql"}'::jsonb,
    '{"authority":"authority.workflow_runs","repair_scopes":["solution","workflow","job"],"open_statuses":["queued","claimed","repairing"]}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
