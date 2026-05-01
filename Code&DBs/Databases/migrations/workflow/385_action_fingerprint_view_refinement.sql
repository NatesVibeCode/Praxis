-- Migration 385: Refine tool_opportunities_pending view + add raw_action view.
--
-- The original 383 view treated every action_fingerprints row as a candidate
-- opportunity, which surfaces existing gateway ops (already tools) as
-- "opportunities" — false positives, since those operations *are* the tool.
--
-- Refinement:
--   * `tool_opportunities_pending` now excludes pure gateway_op shapes —
--     a registered gateway operation is already a tool by definition.
--     What this view surfaces is unbuilt tools: raw shell / edit / write /
--     read shapes that recur ≥3× and have no claim row.
--   * `gateway_op_recurrence` view exposes the gateway-op corpus separately
--     for future composite-recipe detection (multi-op patterns that should
--     become single composite ops).
--
-- Backwards-compatible: keeps the same view name + column shape; consumers
-- that already query `tool_opportunities_pending` see fewer (more accurate)
-- rows. Decision-key convention unchanged.

BEGIN;

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
    -- Exclude pure gateway_op shapes — those are already tools. Surface only
    -- raw shell / edit / write / read shapes that have no canonical op yet.
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
    'Excludes gateway_op shapes — those are already tools by definition.';

-- Separate view for gateway-op corpus mining. Useful for future composite-
-- recipe detection (e.g., "agents always call X then Y then Z — make it
-- a single composite op").
CREATE OR REPLACE VIEW gateway_op_recurrence AS
SELECT
    operation_name,
    COUNT(*)::bigint AS occurrence_count,
    COUNT(DISTINCT source_surface)::bigint AS distinct_surfaces,
    COUNT(DISTINCT session_ref) FILTER (WHERE session_ref IS NOT NULL)::bigint
        AS distinct_sessions,
    array_agg(DISTINCT source_surface) AS surfaces,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM action_fingerprints
WHERE action_kind = 'gateway_op'
  AND operation_name IS NOT NULL
GROUP BY operation_name
ORDER BY occurrence_count DESC;

COMMENT ON VIEW gateway_op_recurrence IS
    'Gateway-op call frequency for composite-recipe detection. Not a list '
    'of opportunities — these are already tools. Used for future composite '
    'tool detection (multi-op patterns that should become single composite '
    'ops).';

COMMIT;
