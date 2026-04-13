-- 026: Operating model card execution support
-- Partial index for efficient card execution polling
CREATE INDEX IF NOT EXISTS idx_run_nodes_ready_cards
  ON run_nodes(current_state, started_at)
  WHERE current_state = 'ready' AND node_type LIKE 'card_%';

-- Also need definition_edges table for run_edges FK
-- Check if workflow_definition_edges exists
CREATE TABLE IF NOT EXISTS workflow_definition_edges (
    workflow_definition_edge_id TEXT PRIMARY KEY,
    workflow_definition_id TEXT NOT NULL REFERENCES workflow_definitions(workflow_definition_id),
    edge_id TEXT NOT NULL,
    from_node_id TEXT NOT NULL,
    to_node_id TEXT NOT NULL,
    edge_type TEXT NOT NULL DEFAULT 'after_success',
    guard_expression JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_mapping JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(workflow_definition_id, edge_id)
);
