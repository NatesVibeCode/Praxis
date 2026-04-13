BEGIN;

-- Remove stale query-surface artifacts from the canonical backlog tables before
-- writing current phase state back into Postgres.
DELETE FROM roadmap_item_dependencies
WHERE roadmap_item_id ~ '^roadmap_item[.][a-f0-9]{10}[.]query$'
   OR depends_on_roadmap_item_id ~ '^roadmap_item[.][a-f0-9]{10}[.]query$';

DELETE FROM roadmap_items
WHERE roadmap_item_id ~ '^roadmap_item[.][a-f0-9]{10}[.]query$';

DELETE FROM bugs
WHERE bug_id ~ '^bug[.][a-f0-9]{10}[.]query$';

WITH phase_updates AS (
    SELECT *
    FROM (
        VALUES
            (
                'roadmap_item.maintenance.alpha.foundation',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json#stale_bug_audit_and_close',
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json#noise_bug_bulk_close',
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json#collision_guard'
                ]::text[],
                ARRAY[
                    'BUG-C1A2FB09',
                    'BUG-C487AEB4',
                    'BUG-CDB5894B',
                    'BUG-D3DE8841',
                    'BUG-D76B1EA9'
                ]::text[],
                'Wave 0 owns stale-bug auditing, backlog hygiene, and dirty-file collision proof before deeper waves dispatch. BUG-CDB5894B, BUG-D76B1EA9, BUG-C487AEB4, BUG-D3DE8841, and BUG-C1A2FB09 are now treated as fixed/absorbed from live repo code and targeted unit proof (notably dispatch truth + collision-guard changes). Remaining active blockers are BUG-661DC83D, BUG-0388B701, BUG-718C3494, BUG-965E983B, and BUG-D0DC2D32; code seams still under verification are dispatch collision recovery and replay safety across synthetic queue fixtures.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/COLLISION_GUARD.md',
                    'artifacts/workflow/db_only_phase/db_only_phase_writeback.sql',
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.phase.method.bootstrap',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json#bootstrap_surface_parity'
                ]::text[],
                ARRAY[]::text[],
                'DB-only bootstrap keeps workflow and test front doors aligned without reintroducing markdown roadmap mirrors.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.dispatch.truth.contract',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json#dispatch_truth_repair'
                ]::text[],
                ARRAY['BUG-6D64923C']::text[],
                'Wave 0 restores async submit/status truth and keeps wait unsupported instead of stranding orchestrations.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.workflow.trigger.checkpoint_cutover',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json#db_change_trigger_expansion',
                    'artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json#notify_consumption_cutover',
                    'artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json#processed_flag_seam_delete',
                    'artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json#trigger_replay_regression'
                ]::text[],
                ARRAY['BUG-14B00013', 'BUG-661DC83D', 'BUG-0388B701']::text[],
                'Wave 1 is materially implemented: migration 059 wires emit_db_change_event() for workflow_jobs, workflow_runs, and receipt_search; _WorkerNotificationListener in runtime/workflow/unified.py now LISTENs job_ready and run_complete and run_worker_loop() still keeps the 5s poll-based trigger sweep fallback. Active constraints are proof-only: there is not yet a deterministic production canary that proves NOTIFY-driven wakeups plus payload idempotency under concurrent commit/retry load, and replay safety must remain under BUG-661DC83D / BUG-0388B701 closure work.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.workflow.command_bus.hard_cutover',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json#chat_mutation_cutover',
                    'artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json#mcp_api_mutation_cutover',
                    'artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json#control_command_fallback_delete',
                    'artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json#command_bus_proof'
                ]::text[],
                ARRAY['BUG-26DAFEBF', 'BUG-756CD965', 'BUG-5A3AD7C1']::text[],
                'Wave 2 cuts remaining mutation shims over to the command bus and proves the old direct paths are gone.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.orient.canonical.authority',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json#orient_packet_contract',
                    'artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json#workflow_invoke_wiring_proof',
                    'artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json#receipt_search_truth',
                    'artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json#reference_catalog_regression_closeout'
                ]::text[],
                ARRAY['BUG-8B0E04AD', 'BUG-DDB3AA43', 'BUG-CDB5894B']::text[],
                'Wave 3 keeps orient canonical and treats BUG-CDB5894B as regression closeout because _BaseSubsystems.get_pg_conn() now syncs integration_registry before reference_catalog refresh, compiler.compile_prose() refreshes the catalog before reading it, and targeted catalog/startup tests passed in the 2026-04-08 audit.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_inventory.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Wave 4 reuses the existing authority-cleanup queue pack under artifacts/workflow and does not fork a parallel replacement.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_inventory.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.contracts',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Authority cleanup contracts continue to use the dedicated packet already present in artifacts/workflow/authority_cleanup.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.seam_cutover',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Core seam cutover is still owned by the dedicated authority-cleanup queue file.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.provider_runtime',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Provider-runtime cleanup is reused as-is from the authority-cleanup queue pack, with workflow-path refs now canonical.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.dispatch_packets',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Dispatch packetization remains in the existing authority-cleanup pack and now points at artifacts/workflow.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.validation_review',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
                ]::text[],
                ARRAY[]::text[],
                'Validation review remains a dedicated authority-cleanup packet and should run before later failure-semantics proof closes.',
                ARRAY[
                    'artifacts/workflow/authority_cleanup/README.md',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_contract_unification',
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_runtime_cutover',
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_projection_cleanup',
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_route_health_cleanup',
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_semantics_proof'
                ]::text[],
                ARRAY['BUG-BDAD34DC', 'BUG-D0DC2D32', 'BUG-C487AEB4']::text[],
                'Wave 4 is now code-backed: complete_job() writes failure_category, failure_zone, and retry flags onto workflow_jobs; compatibility projection mirrors these fields into receipt_search and receipt_meta; retry_orchestrator now consumes pre-classified retryability before recompute. Live code paths for the canonical contract exist. Remaining active blockers are proof seams: fresh failure-row assertions for category+zone persistence across reruns and legacy compatibility parity; BUG-C487AEB4 is regression-verified by the updated retry flow, while BUG-D0DC2D32 remains open for rerun provenance, and BUG-BDAD34DC remains open for contract cleanup proof.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics.contracts',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_contract_unification'
                ]::text[],
                ARRAY['BUG-BDAD34DC']::text[],
                'Failure contract unification owns the canonical terminal-state and compatibility-field cleanup.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics.runtime_cutover',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_runtime_cutover'
                ]::text[],
                ARRAY['BUG-D0DC2D32']::text[],
                'Runtime cutover keeps every live failure path writing the canonical failure shape.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics.projections',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_projection_cleanup'
                ]::text[],
                ARRAY[]::text[],
                'Projection cleanup removes surviving legacy failure read-model assumptions after runtime cutover.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics.route_health',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_route_health_cleanup'
                ]::text[],
                ARRAY[]::text[],
                'Route-health cleanup keeps retryability and responsibility signals aligned with the unified failure contract.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.authority.cleanup.failure_semantics.proof',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json#failure_semantics_proof'
                ]::text[],
                ARRAY['BUG-D0DC2D32', 'BUG-C487AEB4']::text[],
                'Proof packet verifies every failure path converges on the canonical fields and that retryability still drives behavior.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.activity.truth.loop',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json#debate_metrics_persistence',
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json#retrieval_telemetry_production_wiring',
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json#activity_truth_feedback_proof'
                ]::text[],
                ARRAY['BUG-718C3494', 'BUG-965E983B']::text[],
                'Wave 5 is now partially verified: run_debate() threads metrics_conn into DebateMetricsCollector persistence; DebateMetricsCollector persists via DB-backed collector hooks; HybridRetriever and federated retrieval paths record telemetry through TelemetryStore, and unit tests have passed in the current repo audit. Active remaining work is end-to-end persistence proof (fresh metrics rows in live workflow executions), because no canary covers row-level continuity after debate lifecycle transitions; BUG-718C3494 and BUG-965E983B remain open as the open seams.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/README.md',
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json'
                ]::text[]
            ),
            (
                'roadmap_item.object.state.substrate',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json#retrieval_telemetry_production_wiring'
                ]::text[],
                ARRAY[]::text[],
                'No concrete Wave 5 substrate seam remains: retrieval/debate durability now writes into the existing debate_metrics and retrieval_metrics tables only. This row is closeout-ready for Wave 5; keep future shared retrieval-context/state-history work as a separate object/state roadmap seam if it reappears.',
                ARRAY[
                    'artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json'
                ]::text[]
            )
    ) AS t(roadmap_item_id, dispatch_refs, absorbed_bug_ids, current_state_note, extra_paths)
)
UPDATE roadmap_items AS r
SET
    acceptance_criteria = jsonb_strip_nulls(
        COALESCE(r.acceptance_criteria, '{}'::jsonb)
        || jsonb_build_object(
            'dispatch_refs', to_jsonb(u.dispatch_refs),
            'absorbed_bug_ids', to_jsonb(u.absorbed_bug_ids),
            'packet_strategy', 'wave_micro_jobs',
            'current_state_note', u.current_state_note,
            'reference_doc', CASE
                WHEN u.roadmap_item_id LIKE 'roadmap_item.authority.cleanup%' THEN 'artifacts/workflow/authority_cleanup/README.md'
                ELSE 'artifacts/workflow/db_only_phase/README.md'
            END
        )
        || CASE
            WHEN u.roadmap_item_id = 'roadmap_item.maintenance.alpha.foundation'
            THEN jsonb_build_object(
                'noise_policy',
                'Bulk-close synthetic placeholders, repeated dispatch-failure clones, MCP test artifacts, and opaque a_/b_ stubs during Wave 0 hygiene.'
            )
            ELSE '{}'::jsonb
        END
    ),
    registry_paths = (
        SELECT COALESCE(jsonb_agg(path ORDER BY path), '[]'::jsonb)
        FROM (
            SELECT DISTINCT path
            FROM (
                SELECT jsonb_array_elements_text(COALESCE(r.registry_paths, '[]'::jsonb)) AS path
                UNION ALL
                SELECT unnest(u.extra_paths)
            ) AS all_paths
        ) AS deduped
    ),
    updated_at = now()
FROM phase_updates AS u
WHERE r.roadmap_item_id = u.roadmap_item_id;

WITH active_packet_metadata AS (
    SELECT
        r.roadmap_item_id,
        CASE
            WHEN r.roadmap_item_id IN (
                'roadmap_item.maintenance.alpha.foundation',
                'roadmap_item.phase.method.bootstrap',
                'roadmap_item.dispatch.truth.contract'
            )
                THEN 'queue:artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json'
            WHEN r.roadmap_item_id LIKE 'roadmap_item.workflow.trigger.checkpoint_cutover%'
                THEN 'queue:artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.workflow.command_bus.hard_cutover'
                THEN 'queue:artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.orient.canonical.authority'
                THEN 'queue:artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json'
            WHEN r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.failure_semantics%'
                THEN 'queue:artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.activity.truth.loop',
                'roadmap_item.object.state.substrate'
            )
                THEN 'queue:artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.contracts'
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.authority.cleanup.dispatch_packets'
            )
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.authority.cleanup.dispatch_cancellation_contract',
                'roadmap_item.authority.cleanup.dispatch_setup_split'
            )
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_follow_ons.queue.json'
            WHEN r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.seam_cutover%'
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json'
            WHEN r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.provider_runtime%'
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup'
                THEN 'pack:artifacts/workflow/authority_cleanup/README.md'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.validation_review'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.validation_review.%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.http.surface.consolidation%'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.legacy.route.chain.removal'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.stale.postgres.function.prune'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.workflow.spec.parser.shim.removal'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate%'
                THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.agent.truth.program'
                THEN 'rollup:roadmap_item.agent.truth.program'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.agent.native.cockpit',
                'roadmap_item.bus.brain.cutover',
                'roadmap_item.native.primary.cutover'
            )
                THEN 'queue:artifacts/workflow/native_control_plane/native_control_plane_cutover.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.release.proof',
                'roadmap_item.workflow.public_naming_cleanup'
            )
                THEN 'queue:artifacts/workflow/release_hardening/release_proof_and_public_naming.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.sovereignty'
                THEN 'pack:artifacts/workflow/dependency_sovereignty/README.md'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.dependency.manifest.truth',
                'roadmap_item.dependency.execution.runtime.seam'
            )
                THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.dependency.embedding.runtime',
                'roadmap_item.dependency.vector.store.seam'
            )
                THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.provider.adapter.runtime'
                THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.dependency.database.driver.replacement',
                'roadmap_item.dependency.docker.engine.replacement',
                'roadmap_item.dependency.pgvector.engine.replacement'
            )
                THEN 'directional_eval:roadmap_item.dependency.sovereignty'
            ELSE 'no_queue_yet:' || r.roadmap_item_id
        END AS packet_owner,
        CASE
            WHEN r.roadmap_item_id LIKE 'roadmap_item.workflow.trigger.checkpoint_cutover%'
                THEN 'packetized'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.maintenance.alpha.foundation',
                'roadmap_item.phase.method.bootstrap',
                'roadmap_item.dispatch.truth.contract',
                'roadmap_item.workflow.command_bus.hard_cutover',
                'roadmap_item.orient.canonical.authority',
                'roadmap_item.activity.truth.loop',
                'roadmap_item.object.state.substrate',
                'roadmap_item.authority.cleanup',
                'roadmap_item.authority.cleanup.contracts',
                'roadmap_item.authority.cleanup.dispatch_packets',
                'roadmap_item.authority.cleanup.dispatch_cancellation_contract',
                'roadmap_item.authority.cleanup.dispatch_setup_split',
                'roadmap_item.dependency.sovereignty',
                'roadmap_item.dependency.manifest.truth',
                'roadmap_item.dependency.execution.runtime.seam',
                'roadmap_item.dependency.embedding.runtime',
                'roadmap_item.dependency.provider.adapter.runtime',
                'roadmap_item.dependency.vector.store.seam'
            )
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.failure_semantics%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.seam_cutover%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.provider_runtime%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.validation_review%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate%'
                OR r.roadmap_item_id LIKE 'roadmap_item.authority.cleanup.http.surface.consolidation%'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.legacy.route.chain.removal'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.stale.postgres.function.prune'
                OR r.roadmap_item_id = 'roadmap_item.authority.cleanup.workflow.spec.parser.shim.removal'
                THEN 'packetized_or_pack_owned'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.agent.native.cockpit',
                'roadmap_item.bus.brain.cutover',
                'roadmap_item.native.primary.cutover',
                'roadmap_item.release.proof',
                'roadmap_item.workflow.public_naming_cleanup'
            )
                THEN 'packetized'
            WHEN r.roadmap_item_id = 'roadmap_item.agent.truth.program'
                THEN 'program_rollup'
            WHEN r.roadmap_item_id IN (
                'roadmap_item.dependency.database.driver.replacement',
                'roadmap_item.dependency.docker.engine.replacement',
                'roadmap_item.dependency.pgvector.engine.replacement'
            )
                THEN 'directional_evaluation'
            ELSE 'unpacketized_follow_on'
        END AS packet_state,
        CASE
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup'
                THEN jsonb_build_array(
                    'artifacts/workflow/authority_cleanup/authority_cleanup_inventory.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_contracts.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_seam_cutover.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_provider_runtime.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_packets.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_follow_ons.queue.json',
                    'artifacts/workflow/authority_cleanup/authority_cleanup_validation_review.queue.json'
                )
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.sovereignty'
                THEN jsonb_build_array(
                    'artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json',
                    'artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json',
                    'artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json'
                )
            WHEN left(
                CASE
                    WHEN r.roadmap_item_id IN (
                        'roadmap_item.authority.cleanup.dispatch_cancellation_contract',
                        'roadmap_item.authority.cleanup.dispatch_setup_split'
                    )
                        THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_follow_ons.queue.json'
                    WHEN r.roadmap_item_id IN (
                        'roadmap_item.agent.native.cockpit',
                        'roadmap_item.bus.brain.cutover',
                        'roadmap_item.native.primary.cutover'
                    )
                        THEN 'queue:artifacts/workflow/native_control_plane/native_control_plane_cutover.queue.json'
                    WHEN r.roadmap_item_id IN (
                        'roadmap_item.release.proof',
                        'roadmap_item.workflow.public_naming_cleanup'
                    )
                        THEN 'queue:artifacts/workflow/release_hardening/release_proof_and_public_naming.queue.json'
                    WHEN r.roadmap_item_id IN (
                        'roadmap_item.dependency.manifest.truth',
                        'roadmap_item.dependency.execution.runtime.seam'
                    )
                        THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json'
                    WHEN r.roadmap_item_id IN (
                        'roadmap_item.dependency.embedding.runtime',
                        'roadmap_item.dependency.vector.store.seam'
                    )
                        THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json'
                    WHEN r.roadmap_item_id = 'roadmap_item.dependency.provider.adapter.runtime'
                        THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json'
                    ELSE ''
                END,
                6
            ) = 'queue:'
                THEN jsonb_build_array(substr(
                    CASE
                        WHEN r.roadmap_item_id IN (
                            'roadmap_item.authority.cleanup.dispatch_cancellation_contract',
                            'roadmap_item.authority.cleanup.dispatch_setup_split'
                        )
                            THEN 'queue:artifacts/workflow/authority_cleanup/authority_cleanup_dispatch_follow_ons.queue.json'
                        WHEN r.roadmap_item_id IN (
                            'roadmap_item.agent.native.cockpit',
                            'roadmap_item.bus.brain.cutover',
                            'roadmap_item.native.primary.cutover'
                        )
                            THEN 'queue:artifacts/workflow/native_control_plane/native_control_plane_cutover.queue.json'
                        WHEN r.roadmap_item_id IN (
                            'roadmap_item.release.proof',
                            'roadmap_item.workflow.public_naming_cleanup'
                        )
                            THEN 'queue:artifacts/workflow/release_hardening/release_proof_and_public_naming.queue.json'
                        WHEN r.roadmap_item_id IN (
                            'roadmap_item.dependency.manifest.truth',
                            'roadmap_item.dependency.execution.runtime.seam'
                        )
                            THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_manifest_and_execution.queue.json'
                        WHEN r.roadmap_item_id IN (
                            'roadmap_item.dependency.embedding.runtime',
                            'roadmap_item.dependency.vector.store.seam'
                        )
                            THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_embedding_and_vector.queue.json'
                        WHEN r.roadmap_item_id = 'roadmap_item.dependency.provider.adapter.runtime'
                            THEN 'queue:artifacts/workflow/dependency_sovereignty/dependency_provider_runtime.queue.json'
                        ELSE ''
                    END,
                    7
                ))
            ELSE '[]'::jsonb
        END AS synthesized_dispatch_refs,
        CASE
            WHEN r.roadmap_item_id = 'roadmap_item.agent.truth.program'
                THEN 'Program rollup only: child waves and cleanup packs own execution. Keep this row active until maintenance, orient, truth-from-activity, cockpit, release proof, and dependency tracks all converge.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup'
                THEN 'Pack-owned by artifacts/workflow/authority_cleanup/README.md: the inventory, seam-cutover, provider-runtime, packetization, validation, and dispatch follow-on queues now define the executable cleanup path.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.dispatch_cancellation_contract'
                THEN 'Packetized in authority_cleanup_dispatch_follow_ons.queue.json: cancellation must converge on one durable contract and proof path across CLI, MCP, and API.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.dispatch_setup_split'
                THEN 'Packetized in authority_cleanup_dispatch_follow_ons.queue.json: setup and factory assembly now have a dedicated packet to collapse them onto one deterministic authority boundary.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.http.surface.consolidation'
                THEN 'Validation-review follow-on: duplicate HTTP workflow surfaces still need one canonical backend so the product stops carrying two competing front doors.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.http.surface.consolidation.verb.routing.contract'
                THEN 'Validation-review follow-on: PUT and DELETE handling still need explicit verb-specific routing proof instead of POST-style fallback behavior.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.legacy.route.chain.removal'
                THEN 'Validation-review follow-on: fallback route-chain and legacy receipt compatibility shims still need final deletion after the surviving authority path is proven safe.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.config.bootstrap.fallback.removal'
                THEN 'Provider-runtime follow-on: configuration authority must fail closed or read one declared bootstrap source instead of seeding in-memory defaults when Postgres is unavailable.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.model.context.window.fallback.removal'
                THEN 'Provider-runtime follow-on: model context-window selection still needs one authority path so token limits stop drifting across router, profile, and provider defaults.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.model.routing.implicit.fallback.collapse'
                THEN 'Provider-runtime follow-on: routing still needs one declared authority so chat orchestration cannot silently fall back through multiple model-selection seams.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.persistent.evidence.dual.writer.bridge.removal'
                THEN 'Provider-runtime follow-on: persistent evidence now fails closed on write failure, but the dual-writer bridge still has to be removed so durability never depends on an in-memory success path.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.persistent.evidence.error.propagation.removal'
                THEN 'Provider-runtime follow-on: BUG-0F0E9A7C is fixed, and the 2026-04-09 persistent-evidence audit found no remaining entry points that swallow, mask, or downgrade Postgres write failures; this row is now completed.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.provider.fallback.authority.collapse'
                THEN 'Provider-runtime follow-on: provider failover and endpoint selection still need one runtime authority instead of split control-tower and fallback paths.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.real.subprocess.agent.spawning'
                THEN 'Provider-runtime follow-on: live subprocess spawning exists, but this row stays active until readiness, lifecycle capture, and operator proof make it the declared authority rather than an optional path.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.seam_cutover.canonical.runtime.orchestrator.boundary'
                THEN 'Seam-cutover child: submission and advancement still need one canonical RuntimeOrchestrator boundary without compatibility hops or parallel lifecycle seams.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.stale.postgres.function.prune'
                THEN 'Validation-review follow-on: stale completion compatibility functions must be pruned from Postgres and from any remaining callers before this closes.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate'
                THEN 'Validation-review follow-on: operator authoring still needs one preview-first, auto-fixing gate shared by CLI, MCP, and API.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.canonical.native.operator.binding.projection'
                THEN 'Validation-gate child: native operator bindings still need one canonical projection instead of per-surface revalidation.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.contracts'
                THEN 'Validation-gate child: define the typed authoring contract and template pack so roadmap and operator writes stop depending on hand-built rows.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.derived_views'
                THEN 'Validation-gate child: roadmap markdown and operator views still need to be derived from DB-backed authority instead of sidecar mirrors.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.frontdoors'
                THEN 'Validation-gate child: expose the shared authoring gate through CLI and MCP so direct SQL stops being the practical write surface.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.proof'
                THEN 'Validation-gate child: keep this active until preview parity, transaction safety, and representative write scenarios are proven against the shared gate.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.unified.operator.write.validation.gate.validation_gate'
                THEN 'Validation-gate child: normalize deterministic issues automatically and block only on unsafe ambiguity before writing operator backlog state.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.validation_review.native.self.hosted.smoke.contract.fallback.removal'
                THEN 'Validation-review child: native and self-hosted smoke validation still need one explicit contract without fallback branches.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.validation_review.workflow.receiver.compatibility.surface'
                THEN 'Validation-review child: runtime/workflow/receiver.py is still a compatibility alias until callers move directly to workflow notification authority.'
            WHEN r.roadmap_item_id = 'roadmap_item.authority.cleanup.workflow.spec.parser.shim.removal'
                THEN 'Validation-review follow-on: the parser-only WorkflowSpec shim remains until subsystem imports are cut over to the canonical boundary.'
            WHEN r.roadmap_item_id = 'roadmap_item.bus.brain.cutover'
                THEN 'Packetized in native_control_plane_cutover.queue.json: native frontdoor, bus-brain ownership, and cutover admission now move together instead of churning on separate surface stories.'
            WHEN r.roadmap_item_id = 'roadmap_item.native.primary.cutover'
                THEN 'Packetized in native_control_plane_cutover.queue.json: native-primary admission, scope order, and frontdoor proof now have one bounded cutover packet.'
            WHEN r.roadmap_item_id = 'roadmap_item.workflow.trigger.checkpoint_cutover.db.change.trigger.expansion'
                THEN 'Wave 1 child: keep this active until DB change events are emitted for the core dispatch and evidence tables that actually drive workflow replay, not just for bugs.'
            WHEN r.roadmap_item_id = 'roadmap_item.workflow.trigger.checkpoint_cutover.schedule.fired.emission'
                THEN 'Wave 1 child: keep this active until cron and scheduler state emit schedule.fired as a real trigger source instead of a UI-only concept.'
            WHEN r.roadmap_item_id = 'roadmap_item.workflow.trigger.checkpoint_cutover.unified.polling.fallback.retirement'
                THEN 'Wave 1 child: keep this active until LISTEN/NOTIFY plus checkpoints fully replace the unified polling fallback in runtime/workflow/unified.py.'
            WHEN r.roadmap_item_id = 'roadmap_item.agent.native.cockpit'
                THEN 'Packetized in native_control_plane_cutover.queue.json: cockpit adoption now rides a dedicated packet that has to prove control, truth, and drift, not generic builder parity theater.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.sovereignty'
                THEN 'Pack-owned by artifacts/workflow/dependency_sovereignty/README.md: manifest/execution, embedding/vector, and provider-runtime packets now define the prerequisite seam work before any replacement call.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.embedding.runtime'
                THEN 'Packetized in dependency_embedding_and_vector.queue.json: embedding selection, dimensions, refresh policy, and fallback behavior now have one explicit seam packet.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.execution.runtime.seam'
                THEN 'Packetized in dependency_manifest_and_execution.queue.json: execution-runtime behavior now has a bounded packet to concentrate it behind one backend-agnostic contract.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.manifest.truth'
                THEN 'Packetized in dependency_manifest_and_execution.queue.json: setup, packaging, and runtime now have a dedicated packet to converge on one dependency manifest truth.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.provider.adapter.runtime'
                THEN 'Packetized in dependency_provider_runtime.queue.json: first-party provider transport, route truth, and error mapping now have a bounded runtime packet instead of a CLI-only shadow contract.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.vector.store.seam'
                THEN 'Packetized in dependency_embedding_and_vector.queue.json: vector semantics now have a dedicated adapter-boundary packet instead of leaking pgvector details through runtime code.'
            WHEN r.roadmap_item_id = 'roadmap_item.release.proof'
                THEN 'Packetized in release_proof_and_public_naming.queue.json: release proof now has a dedicated packet to assemble replayable evidence and explicit residual-risk accounting.'
            WHEN r.roadmap_item_id = 'roadmap_item.workflow.public_naming_cleanup'
                THEN 'Packetized in release_proof_and_public_naming.queue.json: the remaining public dispatch-era naming sweep now has one bounded cleanup packet across docs, help text, artifacts, and operator read models.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.database.driver.replacement'
                THEN 'Directional evaluation only: do not packetize this until dependency-seam work is closed and there is hard evidence the current driver blocks control or observability.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.docker.engine.replacement'
                THEN 'Directional evaluation only: do not packetize this until execution-runtime seam work proves Docker itself is the blocker.'
            WHEN r.roadmap_item_id = 'roadmap_item.dependency.pgvector.engine.replacement'
                THEN 'Directional evaluation only: do not packetize this until vector-store seam work is done and pgvector is the proven bottleneck.'
            ELSE 'No dedicated queue yet: keep this roadmap row active until its governing cutover is packetized or explicitly demoted.'
        END AS synthesized_current_state_note
    FROM roadmap_items AS r
    WHERE r.status = 'active'
)
UPDATE roadmap_items AS r
SET
    acceptance_criteria = jsonb_strip_nulls(
        COALESCE(r.acceptance_criteria, '{}'::jsonb)
        || jsonb_build_object(
            'packet_owner', m.packet_owner,
            'packet_state', m.packet_state,
            'current_state_note', CASE
                WHEN COALESCE(r.acceptance_criteria->>'packet_state', '') = 'unpacketized_follow_on'
                    OR COALESCE(r.acceptance_criteria->>'packet_owner', '') LIKE 'no_queue_yet:%'
                    OR COALESCE(r.acceptance_criteria->>'packet_owner', '') LIKE 'follow_on:%'
                    THEN m.synthesized_current_state_note
                ELSE COALESCE(
                    NULLIF(r.acceptance_criteria->>'current_state_note', ''),
                    m.synthesized_current_state_note
                )
            END,
            'dispatch_refs', CASE
                WHEN COALESCE(r.acceptance_criteria->>'packet_state', '') = 'unpacketized_follow_on'
                    OR COALESCE(r.acceptance_criteria->>'packet_owner', '') LIKE 'no_queue_yet:%'
                    OR COALESCE(r.acceptance_criteria->>'packet_owner', '') LIKE 'follow_on:%'
                    OR CASE
                        WHEN r.acceptance_criteria->'dispatch_refs' IS NULL THEN true
                        WHEN jsonb_typeof(r.acceptance_criteria->'dispatch_refs') = 'array'
                             AND jsonb_array_length(r.acceptance_criteria->'dispatch_refs') = 0
                            THEN true
                        ELSE false
                    END
                    THEN m.synthesized_dispatch_refs
                ELSE r.acceptance_criteria->'dispatch_refs'
            END
        )
    ),
    updated_at = now()
FROM active_packet_metadata AS m
WHERE r.roadmap_item_id = m.roadmap_item_id;

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 repo audit closed this as stale-open: migration 037 creates reference_catalog, runtime.reference_catalog.sync_reference_catalog() upserts integration/object/agent-route rows, surfaces._subsystems_base._BaseSubsystems.get_pg_conn() runs integration_registry sync before catalog sync, compiler.compile_prose() refreshes before reading, and tests/unit/test_reference_catalog.py plus tests/unit/test_startup_wiring.py passed on 2026-04-08.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE bug_id = 'BUG-CDB5894B'
  AND lower(status) = 'open';

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 repo audit closed this as stale-open on behavior, not seam purity: runtime.triggers.evaluate_triggers() now calls evaluate_event_subscriptions(), consumers resume from subscription_checkpoints, _upsert_subscription_checkpoint() advances the durable cursor with the same checkpoint:{subscription_id}:{run_id} contract as runtime.subscription_repository.subscription_checkpoint_id(), and tests/unit/test_triggers.py passed on 2026-04-08.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE bug_id = 'BUG-D76B1EA9'
  AND lower(status) = 'open';

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 repo audit closed this as verification-only: runtime.workflow.unified.complete_job() preserves classify_failure().is_retryable, immediately terminal-fails when it is False, and passes pre_classified failures into runtime.retry_orchestrator.decide(); tests/unit/test_unified_workflow.py::test_complete_job_requeues_rate_limit_failures_to_next_agent passed on 2026-04-08, so the retry wire is no longer an unwired surface.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE bug_id = 'BUG-C487AEB4'
  AND lower(status) = 'open';

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 repo audit closed this as stale-open: runtime.integration_registry_sync.sync_integration_registry() upserts static integrations plus projected MCP tool rows from surfaces.mcp.catalog into integration_registry by id, surfaces._subsystems_base._BaseSubsystems.get_pg_conn() runs that sync before reference_catalog refresh, and tests/unit/test_startup_wiring.py plus tests/unit/test_runtime_integrations.py passed on 2026-04-08.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE bug_id = 'BUG-D3DE8841'
  AND lower(status) = 'open';

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 repo audit closed this as stale-open: surfaces._subsystems_base._BaseSubsystems.__init__() invokes _maybe_startup_wiring(), get_pg_conn() followed by _start_heartbeat_background() starts exactly one HeartbeatRunner.run_loop() thread, both API and MCP subsystem containers inherit that path, and tests/unit/test_startup_wiring.py passed on 2026-04-08.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE bug_id = 'BUG-C1A2FB09'
  AND lower(status) = 'open';

UPDATE bugs
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 backlog hygiene closed this lingering cockpit-fixture bug as stale manual residue: test_native_observability_cockpit.py seeds run-scoped cockpit bugs with this exact title and source_kind, sibling rows from the same fixture family are already fixed, and this leftover pair should not remain as live operator backlog.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
WHERE lower(status) = 'open'
  AND source_kind = 'manual'
  AND title = 'Cockpit truth binding bug'
  AND bug_id ~ '^bug[.][a-f0-9]{10}[.](governing|other)[.]cockpit$';

WITH waveform_backlog_hygiene_candidates AS (
    SELECT b.bug_id
    FROM bugs b
    WHERE lower(b.status) IN ('open', 'in_progress', 'deferred')
      AND b.bug_id IS NOT NULL
      AND b.bug_id NOT IN (
            'BUG-6D64923C',
            'BUG-661DC83D',
            'BUG-0388B701',
            'BUG-718C3494',
            'BUG-965E983B',
            'BUG-D0DC2D32'
      )
      AND (
            trim(lower(b.title)) ~ '^(p[0-3]:|p[0-3] )'
            OR trim(lower(b.title)) ~ '^test bug from mcp'
            OR trim(lower(b.title)) ~ '^unique dispatch failure [a-f0-9]+$'
            OR trim(lower(b.title)) ~ '^[ab]_[a-z0-9]+$'
            OR b.title = 'Legacy open casing'
            OR b.title LIKE 'Widget crashes on load [%'
            OR b.title = 'C'
      )
)
UPDATE bugs b
SET
    status = 'FIXED',
    resolution_summary = 'Wave 0 backlog hygiene auto-resolved this as non-architectural noise: synthetic placeholders, repeated dispatch-failure clones, MCP test artifacts, and opaque a_*/b_* runtime stubs were bulk-closed with a single durable summary while architectural exceptions (including BUG-6D64923C) remain open for later truthing.',
    resolved_at = COALESCE(resolved_at, now()),
    updated_at = now()
FROM waveform_backlog_hygiene_candidates c
WHERE b.bug_id = c.bug_id;

UPDATE roadmap_items
SET
    status = 'completed',
    completed_at = COALESCE(completed_at, now()),
    updated_at = now()
WHERE roadmap_item_id = 'roadmap_item.authority.cleanup.provider_runtime.persistent.evidence.error.propagation.removal'
  AND lower(status) = 'active';

UPDATE roadmap_items
SET
    status = 'completed',
    completed_at = COALESCE(completed_at, now()),
    updated_at = now()
WHERE roadmap_item_id = 'roadmap_item.object.state.substrate'
  AND lower(status) = 'active';

COMMIT;
