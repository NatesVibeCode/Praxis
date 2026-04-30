-- Migration 383: Action fingerprint authority for tool-opportunity detection.
--
-- One ledger row per executed action *shape* (operation name, command shape,
-- path shape — never literals). Receipts auto-emit shape rows via the trigger
-- below; per-harness PostToolUse hooks can append rows for surfaces that
-- bypass the gateway (raw Bash/Edit/Write inside Claude Code, sandbox CLI
-- agents, etc).
--
-- The convergence point is `action_fingerprints`. Cross-surface frequency
-- counting drives the `tool_opportunities_pending` view: shapes seen ≥3
-- times that don't yet have a registered `decision_kind=tool_opportunity`
-- row in `operator_decisions`.
--
-- Standing-order alignment: feeds into the LLM-first trust compiler — the
-- right tool surfaces at the moment of action because the substrate has
-- been watching the org's actual recurrence. No shims; one table, one
-- trigger, one view. Fingerprinting is fail-open: a fingerprint failure
-- never breaks the underlying receipt or tool call.

BEGIN;

CREATE TABLE IF NOT EXISTS action_fingerprints (
    fingerprint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_surface TEXT NOT NULL CHECK (btrim(source_surface) <> ''),
    action_kind TEXT NOT NULL CHECK (
        action_kind IN ('gateway_op', 'shell', 'edit', 'write', 'multi_edit', 'read')
    ),
    operation_name TEXT,
    normalized_command TEXT,
    path_shape TEXT,
    shape_hash TEXT NOT NULL CHECK (btrim(shape_hash) <> ''),
    session_ref TEXT,
    receipt_id UUID REFERENCES authority_operation_receipts (receipt_id) ON DELETE SET NULL,
    payload_meta JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload_meta) = 'object'),
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE action_fingerprints IS
    'Shape-only ledger of executed actions across all surfaces (gateway receipts, '
    'PostToolUse hooks, sandbox harnesses). Feeds tool-opportunity detection. '
    'Never stores literals — only shape (operation_name, normalized command, '
    'path shape).';
COMMENT ON COLUMN action_fingerprints.shape_hash IS
    'sha256/md5 of (action_kind, operation_name|normalized_command|path_shape). '
    'Cross-surface grouping key.';
COMMENT ON COLUMN action_fingerprints.source_surface IS
    'Origin tag: gateway:<caller_ref>, claude-code:host, sandbox-worker:<agent>, '
    'codex:host, gemini:host, routine:<id>, etc.';

CREATE INDEX IF NOT EXISTS action_fingerprints_shape_ts_idx
    ON action_fingerprints (shape_hash, ts DESC);

CREATE INDEX IF NOT EXISTS action_fingerprints_surface_ts_idx
    ON action_fingerprints (source_surface, ts DESC);

CREATE INDEX IF NOT EXISTS action_fingerprints_session_ts_idx
    ON action_fingerprints (session_ref, ts DESC)
    WHERE session_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS action_fingerprints_operation_ts_idx
    ON action_fingerprints (operation_name, ts DESC)
    WHERE operation_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS action_fingerprints_receipt_idx
    ON action_fingerprints (receipt_id)
    WHERE receipt_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Receipt-side trigger: every authority_operation_receipts insert becomes a
-- gateway-tagged fingerprint row. Operation name + kind is the shape; payload
-- literals are intentionally not part of the fingerprint (we want recurrence
-- by *kind of work*, not exact arguments). Fail-open: any exception is
-- swallowed so receipts always commit.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION action_fingerprints_record_receipt()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_shape_input TEXT;
    v_shape_hash TEXT;
BEGIN
    v_shape_input := 'gateway_op|' || NEW.operation_name
        || '|' || COALESCE(NEW.operation_kind, '');
    v_shape_hash := md5(v_shape_input);

    INSERT INTO action_fingerprints (
        source_surface,
        action_kind,
        operation_name,
        shape_hash,
        receipt_id,
        payload_meta,
        ts
    ) VALUES (
        'gateway:' || COALESCE(NULLIF(btrim(NEW.caller_ref), ''), 'unknown'),
        'gateway_op',
        NEW.operation_name,
        v_shape_hash,
        NEW.receipt_id,
        jsonb_build_object(
            'operation_kind', NEW.operation_kind,
            'execution_status', NEW.execution_status,
            'authority_domain_ref', NEW.authority_domain_ref,
            'shape_input', v_shape_input
        ),
        NEW.created_at
    );

    RETURN NULL;
EXCEPTION
    WHEN OTHERS THEN
        -- Never break receipt insertion. Fingerprinting is best-effort.
        RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS action_fingerprints_receipt_trg ON authority_operation_receipts;
CREATE TRIGGER action_fingerprints_receipt_trg
    AFTER INSERT ON authority_operation_receipts
    FOR EACH ROW
    EXECUTE FUNCTION action_fingerprints_record_receipt();

-- ---------------------------------------------------------------------------
-- View: shapes seen ≥3 times that don't yet have a tool_opportunity decision.
-- Decision-key convention: 'tool-opportunity::' || substring(shape_hash,1,16).
-- Operators register a row in `operator_decisions` with that key (and
-- decision_kind='tool_opportunity') to either claim a build or decline /
-- retire the opportunity, removing it from this view.
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS tool_opportunities_pending;
CREATE VIEW tool_opportunities_pending AS
WITH grouped AS (
    SELECT
        shape_hash,
        COUNT(*)::bigint AS occurrence_count,
        COUNT(DISTINCT source_surface)::bigint AS distinct_surfaces,
        COUNT(DISTINCT session_ref)::bigint AS distinct_sessions,
        array_agg(DISTINCT action_kind) AS action_kinds,
        array_agg(DISTINCT source_surface) AS surfaces,
        (array_agg(DISTINCT operation_name) FILTER (WHERE operation_name IS NOT NULL))
            AS operation_names,
        (array_agg(DISTINCT normalized_command) FILTER (WHERE normalized_command IS NOT NULL))
            AS sample_commands,
        (array_agg(DISTINCT path_shape) FILTER (WHERE path_shape IS NOT NULL))
            AS sample_path_shapes,
        MIN(ts) AS first_seen,
        MAX(ts) AS last_seen
    FROM action_fingerprints
    GROUP BY shape_hash
    HAVING COUNT(*) >= 3
)
SELECT
    g.shape_hash,
    'tool-opportunity::' || substring(g.shape_hash, 1, 16) AS proposed_decision_key,
    g.occurrence_count,
    g.distinct_surfaces,
    g.distinct_sessions,
    g.action_kinds,
    g.surfaces,
    g.operation_names,
    g.sample_commands,
    g.sample_path_shapes,
    g.first_seen,
    g.last_seen
FROM grouped g
WHERE NOT EXISTS (
    SELECT 1
    FROM operator_decisions od
    WHERE od.decision_kind = 'tool_opportunity'
      AND od.decision_status NOT IN ('retired', 'declined', 'rejected')
      AND od.decision_key = 'tool-opportunity::' || substring(g.shape_hash, 1, 16)
)
ORDER BY g.occurrence_count DESC, g.last_seen DESC;

COMMENT ON VIEW tool_opportunities_pending IS
    'Action shapes seen ≥3 times across all surfaces that have no claimed '
    'tool_opportunity decision row. Surface to operators via praxis_orient or '
    '`praxis workflow query "tool opportunities"`.';

COMMIT;
