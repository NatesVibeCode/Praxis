-- Migration 215: Stop stale authority_operation_receipts replay for roadmap tree reads.
--
-- operator.roadmap_tree inherited operation_query's read_only idempotency policy, so
-- identical inputs reused a cached result_payload after roadmap_items mutated (BUG-6F69FFB3).
-- Mark the operation non_idempotent so each read executes the handler; receipts still record
-- outcomes without cross-request replay.

BEGIN;

UPDATE operation_catalog_registry
   SET idempotency_policy = 'non_idempotent',
       binding_revision = 'binding.operation_catalog_registry.roadmap_tree_fresh_reads.20260424',
       decision_ref = 'decision.operator.roadmap_tree.fresh_reads_after_write.20260424',
       updated_at = now()
 WHERE operation_ref = 'operator-roadmap-tree'
   AND operation_name = 'operator.roadmap_tree';

COMMIT;
