-- Migration 353: Bulk operation-lane classification across the catalog.
--
-- Migration 348 added the columns + gateway enforcement; migration 350
-- classified the three known concurrency hazards. This migration types the
-- remaining catalog so the gateway's lane gate is load-bearing across the
-- whole surface, not just three operations.
--
-- Heuristic applied (auto-generated from the live registry):
--   * read_only queries with quick-lookup names → interactive 5000ms
--   * other read_only queries / observe queries  → interactive 15000ms
--   * LLM-bound / workflow-launch handlers       → background + kickoff_required
--   * runtime / heartbeat / catalog mutators     → system
--   * other operate/build commands               → background (default kept)
--
-- The 67 operate/build commands not listed below stay on the background
-- defaults; they're admitted unchanged because they don't trip a known
-- concurrency hazard yet. When one does, it gets promoted to
-- kickoff_required in a follow-up migration.
--
-- Idempotency: every UPDATE has a WHERE-clause guard so re-applying
-- the migration is a no-op once it's converged.

BEGIN;

-- background+kickoff: LLM-bound or workflow launch  (3 operations)
UPDATE operation_catalog_registry
   SET execution_lane = 'background',
       kickoff_required = TRUE,
       timeout_ms = 15000,
       binding_revision = binding_revision || '.lane.20260430',
       updated_at = now()
 WHERE operation_name IN ('compile_preview', 'compose_plan', 'launch_plan')
   AND (
        execution_lane IS DISTINCT FROM 'background'
        OR kickoff_required IS DISTINCT FROM TRUE
        OR timeout_ms IS DISTINCT FROM 15000
   );

-- interactive: quick read-only lookup  (10 operations)
UPDATE operation_catalog_registry
   SET execution_lane = 'interactive',
       kickoff_required = FALSE,
       timeout_ms = 5000,
       binding_revision = binding_revision || '.lane.20260430',
       updated_at = now()
 WHERE operation_name IN ('authority.objects.list', 'compliance.list_receipts', 'feedback.list', 'operator.runtime_truth_snapshot', 'policy.list', 'primitive.list', 'semantic_predicate.list', 'service.lifecycle.get_projection', 'service.lifecycle.list_targets', 'structured_documents.list_context_selection_receipts')
   AND (
        execution_lane IS DISTINCT FROM 'interactive'
        OR kickoff_required IS DISTINCT FROM FALSE
        OR timeout_ms IS DISTINCT FROM 5000
   );

-- interactive: observe query  (36 operations)
UPDATE operation_catalog_registry
   SET execution_lane = 'interactive',
       kickoff_required = FALSE,
       timeout_ms = 15000,
       binding_revision = binding_revision || '.lane.20260430',
       updated_at = now()
 WHERE operation_name IN ('cli_auth_doctor', 'object_schema.field_list', 'object_schema.type_get', 'object_schema.type_list', 'operator.circuit_history', 'operator.circuit_states', 'operator.data_dictionary', 'operator.decision_list', 'operator.graph_projection', 'operator.issue_backlog', 'operator.replay_ready_bugs', 'operator.roadmap_tree', 'operator.run_graph', 'operator.run_lineage', 'operator.run_scoreboard', 'operator.run_status', 'operator.status_snapshot', 'operator.transport_support', 'search.bugs', 'search.code', 'search.db', 'search.decisions', 'search.federated', 'search.files', 'search.git_history', 'search.knowledge', 'search.receipts', 'search.research', 'semantic_assertions.list', 'suggest_plan_atoms', 'synthesize_skeleton', 'test.query_register_atomic.71dfa06e9d99', 'test.query_register_atomic.e9bcd15fa1da', 'trace.walk', 'validate_authored_plan', 'workflow_build.suggest_next')
   AND (
        execution_lane IS DISTINCT FROM 'interactive'
        OR kickoff_required IS DISTINCT FROM FALSE
        OR timeout_ms IS DISTINCT FROM 15000
   );

-- interactive: read-only query  (40 operations)
UPDATE operation_catalog_registry
   SET execution_lane = 'interactive',
       kickoff_required = FALSE,
       timeout_ms = 15000,
       binding_revision = binding_revision || '.lane.20260430',
       updated_at = now()
 WHERE operation_name IN ('audit.summary', 'authority.compose_binding.resolve', 'authority.impact_contract_audit.scan', 'authority.objects.adoption', 'authority.objects.domain_summary', 'authority.objects.drift', 'authority_domain_forge', 'object_truth_compare_versions', 'object_truth_observe_record', 'operation.evolve_field', 'operator.bug_triage_packet', 'operator.execution_proof', 'operator.execution_truth', 'operator.firecheck', 'operator.legal_tools', 'operator.model_access_control_matrix', 'operator.next', 'operator.next_actions', 'operator.next_work', 'operator.operation_forge', 'operator.provider_control_plane', 'operator.provider_route_truth', 'operator.refactor_heatmap', 'operator.remediation_plan', 'operator.repo_policy_contract_current', 'operator.repo_policy_submission_acceptance', 'operator.roadmap_backlog', 'operator.ui_experience_graph', 'operator.work_assignment_matrix', 'operator_patterns', 'primitive.get', 'primitive.scan_consistency', 'runtime.setup.doctor', 'runtime.setup.plan', 'search.authority_receipts', 'search.compliance_receipts', 'semantic_invariant.scan', 'semantic_predicate.get', 'structured_documents.context_assemble', 'workflow_build_get')
   AND (
        execution_lane IS DISTINCT FROM 'interactive'
        OR kickoff_required IS DISTINCT FROM FALSE
        OR timeout_ms IS DISTINCT FROM 15000
   );

-- system: runtime/control plane  (8 operations)
UPDATE operation_catalog_registry
   SET execution_lane = 'system',
       kickoff_required = FALSE,
       timeout_ms = 15000,
       binding_revision = binding_revision || '.lane.20260430',
       updated_at = now()
 WHERE operation_name IN ('catalog_operation_register', 'catalog_operation_retire', 'operator.bug_replay_provenance_backfill', 'operator.daily_heartbeat_refresh', 'operator.metrics_reset', 'operator.semantic_bridges_backfill', 'operator.semantic_projection_refresh', 'shell.session.bootstrapped')
   AND (
        execution_lane IS DISTINCT FROM 'system'
        OR kickoff_required IS DISTINCT FROM FALSE
        OR timeout_ms IS DISTINCT FROM 15000
   );

COMMIT;
