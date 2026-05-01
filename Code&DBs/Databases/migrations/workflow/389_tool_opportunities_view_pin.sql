-- Migration 389: Pin tool_opportunities_pending view to the refined definition.
--
-- Migration 385 introduced the gateway_op exclusion filter, but the bootstrap
-- path was reverting the view back to 383's un-filtered form on every
-- container restart. This migration is registered in
-- `WORKFLOW_MIGRATIONS_ALWAYS_REAPPLY` so bootstrap re-asserts the refined
-- view definition every start, after 383's CREATE has already run.
--
-- Uses CREATE OR REPLACE VIEW (idempotent — never errors if the view exists,
-- never breaks dependent objects). End state guarantees: the view filters
-- out gateway_op fingerprints (those are already-tools, not opportunities).
--
-- This is a "view pin," not a structural change. Schema 100% matches 385
-- with no new objects.

BEGIN;

CREATE OR REPLACE VIEW tool_opportunities_pending AS
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
    WHERE action_kind <> 'gateway_op'
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
    'Raw action shapes (Bash / Edit / Write / MultiEdit / Read) seen ≥3 times '
    'across all surfaces with no claimed tool_opportunity decision row. '
    'Excludes gateway_op shapes — those are already tools by definition. '
    'Pinned via migration 389 (always-reapply on bootstrap).';

COMMIT;
