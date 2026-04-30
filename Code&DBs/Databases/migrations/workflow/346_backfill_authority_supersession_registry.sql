-- Migration 346: backfill authority_supersession_registry with established
-- repo-known supersessions so the compose-time canonical resolver has data
-- from day one rather than waiting for new candidate materializations to
-- populate it organically.
--
-- This is the seed pass. It carries the high-confidence pairs that are
-- explicitly attested in the codebase (DEPRECATED ALIAS markers, CLAUDE.md
-- naming policy, etc.). Future backfills should add new pairs as decisions
-- land in operator_decisions; do not bulk-insert from prose evidence.
--
-- Pair set in this migration:
--
--   praxis_run_status     -> praxis_run (action=status)     [compat, mcp_tool]
--   praxis_run_scoreboard -> praxis_run (action=scoreboard) [compat, mcp_tool]
--   praxis_run_graph      -> praxis_run (action=graph)      [compat, mcp_tool]
--   praxis_run_lineage    -> praxis_run (action=lineage)    [compat, mcp_tool]
--
-- The successor unit_ref is the same tool name (praxis_run); the action
-- distinction lives in the obligation_summary text so the compose-time
-- agent learns which action to call without us inventing a per-action
-- subref convention.

BEGIN;

INSERT INTO authority_supersession_registry (
    successor_unit_kind,
    successor_unit_ref,
    predecessor_unit_kind,
    predecessor_unit_ref,
    supersession_status,
    obligation_summary,
    obligation_evidence,
    source_decision_ref
) VALUES
(
    'mcp_tool',
    'praxis_run',
    'mcp_tool',
    'praxis_run_status',
    'compat',
    'Use praxis_run(action=''status'') instead. Old name remains a kind=alias DEPRECATED wrapper for one window per architecture-policy::tool-vocabulary::praxis-run-consolidation-supersedes-four-aliases.',
    jsonb_build_object(
        'replacement', 'workflow tools call praxis_run --input-json ''{"run_id":"<run_id>","action":"status"}''',
        'evidence_path', 'Code&DBs/Workflow/surfaces/mcp/tools/operator.py',
        'evidence_marker', 'DEPRECATED ALIAS — use praxis_run(action=''status'')'
    ),
    'decision.architecture_policy.tool_vocabulary.praxis_run_consolidation_supersedes_four_aliases'
),
(
    'mcp_tool',
    'praxis_run',
    'mcp_tool',
    'praxis_run_scoreboard',
    'compat',
    'Use praxis_run(action=''scoreboard'') instead. Old name remains a kind=alias DEPRECATED wrapper for one window per architecture-policy::tool-vocabulary::praxis-run-consolidation-supersedes-four-aliases.',
    jsonb_build_object(
        'replacement', 'workflow tools call praxis_run --input-json ''{"run_id":"<run_id>","action":"scoreboard"}''',
        'evidence_path', 'Code&DBs/Workflow/surfaces/mcp/tools/operator.py',
        'evidence_marker', 'DEPRECATED ALIAS — use praxis_run(action=''scoreboard'')'
    ),
    'decision.architecture_policy.tool_vocabulary.praxis_run_consolidation_supersedes_four_aliases'
),
(
    'mcp_tool',
    'praxis_run',
    'mcp_tool',
    'praxis_run_graph',
    'compat',
    'Use praxis_run(action=''graph'') instead. Old name remains a kind=alias DEPRECATED wrapper for one window per architecture-policy::tool-vocabulary::praxis-run-consolidation-supersedes-four-aliases.',
    jsonb_build_object(
        'replacement', 'workflow tools call praxis_run --input-json ''{"run_id":"<run_id>","action":"graph"}''',
        'evidence_path', 'Code&DBs/Workflow/surfaces/mcp/tools/operator.py',
        'evidence_marker', 'DEPRECATED ALIAS — use praxis_run(action=''graph'')'
    ),
    'decision.architecture_policy.tool_vocabulary.praxis_run_consolidation_supersedes_four_aliases'
),
(
    'mcp_tool',
    'praxis_run',
    'mcp_tool',
    'praxis_run_lineage',
    'compat',
    'Use praxis_run(action=''lineage'') instead. Old name remains a kind=alias DEPRECATED wrapper for one window per architecture-policy::tool-vocabulary::praxis-run-consolidation-supersedes-four-aliases.',
    jsonb_build_object(
        'replacement', 'workflow tools call praxis_run --input-json ''{"run_id":"<run_id>","action":"lineage"}''',
        'evidence_path', 'Code&DBs/Workflow/surfaces/mcp/tools/operator.py',
        'evidence_marker', 'DEPRECATED ALIAS — use praxis_run(action=''lineage'')'
    ),
    'decision.architecture_policy.tool_vocabulary.praxis_run_consolidation_supersedes_four_aliases'
)
ON CONFLICT (
    successor_unit_kind,
    successor_unit_ref,
    predecessor_unit_kind,
    predecessor_unit_ref
) WHERE supersession_status <> 'rolled_back'
DO UPDATE SET
    supersession_status = EXCLUDED.supersession_status,
    obligation_summary  = EXCLUDED.obligation_summary,
    obligation_evidence = EXCLUDED.obligation_evidence,
    source_decision_ref = EXCLUDED.source_decision_ref,
    updated_at          = now();

COMMIT;
