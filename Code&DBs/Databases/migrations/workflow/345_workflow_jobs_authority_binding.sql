-- Migration 345: workflow_jobs.authority_binding — discrete top-level field.
--
-- Pass 3 (intent_composition) attaches authority_binding to each PlanPacket
-- at compose time. This migration persists that binding through the launch
-- path so the worker context loader can surface it to the agent at claim
-- time without forcing the agent to walk the supersession registry itself.
--
-- The column is intentionally a top-level discrete field rather than nested
-- inside an existing args/payload jsonb. That keeps it composable with
-- field-level node mutation tooling (operator surface) and lets the worker
-- read just this column when it builds the effective workspace for the
-- agent — no hydrating the whole packet provenance blob.
--
-- Shape mirrors `ComposeAuthorityBinding.to_dict()` from
-- runtime/workflow/compose_authority_binding.py:
--
--   {
--     "canonical_write_scope":   [...],
--     "predecessor_obligations": [...],   // do_not_imitate__preserve_tested_invariants
--     "blocked_compat_units":    [...],
--     "unresolved_targets":      [...],
--     "notes":                   [...]
--   }
--
-- NULL means the packet had no authority-bearing targets at compose time
-- (workspace-root scope, docs-only edits, etc.). An empty {} is reserved
-- for an explicitly empty binding (resolver ran, returned nothing actionable).

BEGIN;

ALTER TABLE workflow_jobs
    ADD COLUMN IF NOT EXISTS authority_binding jsonb;

CREATE INDEX IF NOT EXISTS idx_wj_authority_bound
    ON workflow_jobs ((authority_binding IS NOT NULL))
    WHERE authority_binding IS NOT NULL;

COMMENT ON COLUMN workflow_jobs.authority_binding IS
    'Compose-time canonical authority binding: canonical_write_scope, predecessor_obligations (read-only, do not imitate), blocked_compat_units, unresolved_targets, notes. Populated by attach_authority_bindings_to_packets in intent_composition.py and surfaced to the worker via the job context loader. NULL = no authority-bearing targets at compose time.';

COMMIT;
