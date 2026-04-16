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

WITH graph_visibility_items AS (
    SELECT *
    FROM (
        VALUES
            (
                'roadmap_item.platform.live_graph_intelligence',
                'roadmap.platform.live_graph_intelligence',
                'Live Platform Intelligence Graph',
                'initiative',
                'active',
                'p0',
                NULL::text,
                'Build continuously updated graph views for code, data, workflow, and memory so operators and autonomous agents can inspect, reason, and act with shared context.',
                jsonb_build_object(
                    'outcome_gate', 'live_graph_intelligence_program_live',
                    'phase_order', '1',
                    'packet_strategy', 'graph_program'
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.code_map',
                'roadmap.platform.live_graph_intelligence.code_map',
                'Code Intelligence Graph Map',
                'capability',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Continuously map code structure, coupling, size, ownership, and change heat so both humans and agents can understand and safely change the system.',
                jsonb_build_object(
                    'outcome_gate', 'code_map_live_and_queryable',
                    'phase_order', '1.1',
                    'dispatch_refs', jsonb_build_array(
                        'graph/code/collector',
                        'graph/code/projection',
                        'graph/code/ui'
                    )
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.data_map',
                'roadmap.platform.live_graph_intelligence.data_map',
                'Data and Database Intelligence Map',
                'capability',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Continuously map table relations, volume, freshness, and query behavior to expose the real operational data model and its hotspots.',
                jsonb_build_object(
                    'outcome_gate', 'data_map_live_and_queryable',
                    'phase_order', '1.2',
                    'dispatch_refs', jsonb_build_array(
                        'graph/data/introspection',
                        'graph/data/lineage',
                        'graph/data/ui'
                    )
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.workflow_map',
                'roadmap.platform.live_graph_intelligence.workflow_map',
                'Workflow and Agent Lineage Map',
                'capability',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Map workflow and agent execution lineage including prompts, tools, retries, failures, and artifacts so runtime behavior is inspectable and debuggable.',
                jsonb_build_object(
                    'outcome_gate', 'workflow_map_live_and_queryable',
                    'phase_order', '1.3',
                    'dispatch_refs', jsonb_build_array(
                        'graph/workflow/event_ingest',
                        'graph/workflow/lineage_projection',
                        'graph/workflow/ui'
                    )
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.memory_map',
                'roadmap.platform.live_graph_intelligence.memory_map',
                'Context and Memory Provenance Map',
                'capability',
                'active',
                'p1',
                'roadmap_item.platform.live_graph_intelligence',
                'Map session memory provenance, reuse frequency, and staleness so cross-session context continuity can be governed and improved.',
                jsonb_build_object(
                    'outcome_gate', 'memory_map_live_and_queryable',
                    'phase_order', '1.4',
                    'dispatch_refs', jsonb_build_array(
                        'graph/memory/provenance',
                        'graph/memory/quality_signals',
                        'graph/memory/ui'
                    )
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'roadmap.platform.live_graph_intelligence.unified_schema',
                'Unified Graph Schema and Event Model',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Define a canonical node-edge-metric-timeseries model and versioned event contract used by every graph map.',
                jsonb_build_object(
                    'outcome_gate', 'schema_versioned_and_shared',
                    'phase_order', '2.1',
                    'verification', jsonb_build_array('schema_snapshot_test', 'contract_validation_test')
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.ingestion_spine',
                'roadmap.platform.live_graph_intelligence.ingestion_spine',
                'Collector and Ingestion Spine',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Implement collector jobs and event ingestion for code, DB, workflow, and memory domains with replayable checkpoints.',
                jsonb_build_object(
                    'outcome_gate', 'ingestion_live_with_replay',
                    'phase_order', '2.2',
                    'verification', jsonb_build_array('collector_smoke', 'checkpoint_replay_test')
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.projection_api',
                'roadmap.platform.live_graph_intelligence.projection_api',
                'Graph Projection Store and Query API',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Persist graph projections and expose query surfaces for filtered topology, metrics, and temporal slices.',
                jsonb_build_object(
                    'outcome_gate', 'graph_queries_under_slo',
                    'phase_order', '2.3',
                    'verification', jsonb_build_array('query_contract_tests', 'latency_budget_test')
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.operator_ui',
                'roadmap.platform.live_graph_intelligence.operator_ui',
                'Operator UI Graph Surfaces',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.live_graph_intelligence',
                'Deliver interactive UI graph panes with filtering, drilldown, and timeline playback for each map.',
                jsonb_build_object(
                    'outcome_gate', 'ui_live_with_drilldown',
                    'phase_order', '2.4',
                    'verification', jsonb_build_array('ui_smoke', 'end_to_end_graph_render')
                ),
                'decision.2026-04-15.live-graph-intelligence'
            ),
            (
                'roadmap_item.platform.live_graph_intelligence.action_layer',
                'roadmap.platform.live_graph_intelligence.action_layer',
                'LLM Action Layer on Graph Nodes',
                'workstream',
                'active',
                'p1',
                'roadmap_item.platform.live_graph_intelligence',
                'Enable explain, impact analysis, and governed action launches from graph nodes into workflow and code-generation paths.',
                jsonb_build_object(
                    'outcome_gate', 'graph_actions_governed_and_replayable',
                    'phase_order', '2.5',
                    'verification', jsonb_build_array('policy_gate_test', 'dry_run_action_test')
                ),
                'decision.2026-04-15.live-graph-intelligence'
            )
    ) AS t(
        roadmap_item_id,
        roadmap_key,
        title,
        item_kind,
        status,
        priority,
        parent_roadmap_item_id,
        summary,
        acceptance_criteria,
        decision_ref
    )
)
INSERT INTO roadmap_items (
    roadmap_item_id,
    roadmap_key,
    title,
    item_kind,
    status,
    priority,
    parent_roadmap_item_id,
    source_bug_id,
    summary,
    acceptance_criteria,
    decision_ref,
    target_start_at,
    target_end_at,
    completed_at,
    created_at,
    updated_at
)
SELECT
    i.roadmap_item_id,
    i.roadmap_key,
    i.title,
    i.item_kind,
    i.status,
    i.priority,
    i.parent_roadmap_item_id,
    NULL,
    i.summary,
    i.acceptance_criteria,
    i.decision_ref,
    NULL,
    NULL,
    NULL,
    now(),
    now()
FROM graph_visibility_items AS i
ORDER BY
    CASE WHEN i.parent_roadmap_item_id IS NULL THEN 0 ELSE 1 END,
    i.roadmap_item_id
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH graph_visibility_dependencies AS (
    SELECT *
    FROM (
        VALUES
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.code_map.requires_schema',
                'roadmap_item.platform.live_graph_intelligence.code_map',
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.data_map.requires_schema',
                'roadmap_item.platform.live_graph_intelligence.data_map',
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.workflow_map.requires_schema',
                'roadmap_item.platform.live_graph_intelligence.workflow_map',
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.memory_map.requires_schema',
                'roadmap_item.platform.live_graph_intelligence.memory_map',
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.ingestion_spine.requires_schema',
                'roadmap_item.platform.live_graph_intelligence.ingestion_spine',
                'roadmap_item.platform.live_graph_intelligence.unified_schema',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.projection_api.requires_ingestion',
                'roadmap_item.platform.live_graph_intelligence.projection_api',
                'roadmap_item.platform.live_graph_intelligence.ingestion_spine',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.operator_ui.requires_projection_api',
                'roadmap_item.platform.live_graph_intelligence.operator_ui',
                'roadmap_item.platform.live_graph_intelligence.projection_api',
                'blocks'
            ),
            (
                'roadmap_item_dependency.platform.live_graph_intelligence.action_layer.requires_operator_ui',
                'roadmap_item.platform.live_graph_intelligence.action_layer',
                'roadmap_item.platform.live_graph_intelligence.operator_ui',
                'blocks'
            )
    ) AS t(
        roadmap_item_dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        dependency_kind
    )
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id,
    roadmap_item_id,
    depends_on_roadmap_item_id,
    dependency_kind,
    decision_ref,
    created_at
)
SELECT
    d.roadmap_item_dependency_id,
    d.roadmap_item_id,
    d.depends_on_roadmap_item_id,
    d.dependency_kind,
    'decision.2026-04-15.live-graph-intelligence',
    now()
FROM graph_visibility_dependencies AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

WITH graph_visibility_module_items AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy', 'Node Taxonomy Contract', 'roadmap_item.platform.live_graph_intelligence.unified_schema', 'Define canonical node types and required fields shared by code, data, workflow, and memory maps.', '2.1.1'),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy', 'Edge Taxonomy Contract', 'roadmap_item.platform.live_graph_intelligence.unified_schema', 'Define canonical edge semantics and directionality for dependency, lineage, ownership, and influence links.', '2.1.2'),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.event_versioning', 'Event Versioning and Provenance', 'roadmap_item.platform.live_graph_intelligence.unified_schema', 'Define event envelope versioning and provenance metadata to support replay, audit, and backward-compatible evolution.', '2.1.3'),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'Stable Identity Strategy', 'roadmap_item.platform.live_graph_intelligence.unified_schema', 'Define deterministic ID strategy for node and edge identity to prevent duplicate graph entities across collectors.', '2.1.4'),

            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector', 'Code Git Metadata Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect commit churn, ownership, and change velocity signals into graph events.', '2.2.1'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector', 'Code Static Relationship Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect module, import, call, and symbol relationships from static analysis.', '2.2.2'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector', 'Database Schema Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect table, column, key, and relationship metadata from canonical DB introspection.', '2.2.3'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_usage_collector', 'Database Usage Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect query heat, row volume, and freshness signals for data hotspots.', '2.2.4'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector', 'Workflow Runtime Event Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect workflow and agent runtime lineage events across run lifecycle.', '2.2.5'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.memory_provenance_collector', 'Memory Provenance Collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Collect retrieval provenance and context reuse metrics across sessions.', '2.2.6'),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay', 'Checkpoint and Replay Runtime', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine', 'Provide replayable checkpointed ingestion to support rebuild and drift recovery.', '2.2.7'),

            ('roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'Projection Writer Pipeline', 'roadmap_item.platform.live_graph_intelligence.projection_api', 'Project normalized events into query-optimized graph projections.', '2.3.1'),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api', 'Topology Query API', 'roadmap_item.platform.live_graph_intelligence.projection_api', 'Serve filtered graph topology reads by domain, scope, and hierarchy.', '2.3.2'),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.metrics_query_api', 'Metrics and Timeseries Query API', 'roadmap_item.platform.live_graph_intelligence.projection_api', 'Serve graph metrics and timeseries overlays for health and trend analysis.', '2.3.3'),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api', 'Temporal Slice Query API', 'roadmap_item.platform.live_graph_intelligence.projection_api', 'Serve time-window graph slices for replay and before/after comparison.', '2.3.4'),

            ('roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'Graph Workspace Shell', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Provide shared graph workspace shell with map switching, search, and global filters.', '2.4.1'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.code_graph_pane', 'Code Graph Pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Deliver interactive code intelligence map pane with dependency and ownership overlays.', '2.4.2'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.data_graph_pane', 'Data Graph Pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Deliver interactive data intelligence map pane with lineage and hotspot overlays.', '2.4.3'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.workflow_graph_pane', 'Workflow Graph Pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Deliver interactive workflow lineage pane with retry/failure overlays.', '2.4.4'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.memory_graph_pane', 'Memory Graph Pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Deliver interactive memory provenance pane with staleness and reuse overlays.', '2.4.5'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.timeline_controls', 'Timeline and Replay Controls', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Provide graph time slider and replay controls for temporal inspection.', '2.4.6'),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.drilldown_panels', 'Node Drilldown Panels', 'roadmap_item.platform.live_graph_intelligence.operator_ui', 'Provide detailed node and edge inspection panels with linked evidence.', '2.4.7'),

            ('roadmap_item.platform.live_graph_intelligence.action_layer.explain_node', 'Explain Node Action', 'roadmap_item.platform.live_graph_intelligence.action_layer', 'Generate concise model explanations for selected node/edge sets with provenance references.', '2.5.1'),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius', 'Blast Radius Action', 'roadmap_item.platform.live_graph_intelligence.action_layer', 'Compute and visualize likely impact set for proposed changes.', '2.5.2'),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.generate_change', 'Generate Change Action', 'roadmap_item.platform.live_graph_intelligence.action_layer', 'Launch governed code or workflow generation actions from selected graph context.', '2.5.3'),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails', 'Execute with Guardrails Action', 'roadmap_item.platform.live_graph_intelligence.action_layer', 'Enforce policy checks and dry-run gates before executing graph-triggered actions.', '2.5.4')
    ) AS t(
        roadmap_item_id,
        title,
        parent_roadmap_item_id,
        summary,
        phase_order
    )
)
INSERT INTO roadmap_items (
    roadmap_item_id,
    roadmap_key,
    title,
    item_kind,
    status,
    priority,
    parent_roadmap_item_id,
    source_bug_id,
    summary,
    acceptance_criteria,
    decision_ref,
    target_start_at,
    target_end_at,
    completed_at,
    created_at,
    updated_at
)
SELECT
    m.roadmap_item_id,
    replace(m.roadmap_item_id, 'roadmap_item.', 'roadmap.'),
    m.title,
    'task',
    'active',
    'p1',
    m.parent_roadmap_item_id,
    NULL,
    m.summary,
    jsonb_build_object(
        'outcome_gate', 'module_production_ready',
        'phase_order', m.phase_order,
        'verification', jsonb_build_array('module_contract_test', 'integration_smoke')
    ),
    'decision.2026-04-15.live-graph-intelligence',
    NULL,
    NULL,
    NULL,
    now(),
    now()
FROM graph_visibility_module_items AS m
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH graph_visibility_module_dependencies AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item_dependency.platform.live_graph_intelligence.unified_schema.edge_taxonomy.requires_node_taxonomy', 'roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.unified_schema.event_versioning.requires_node_taxonomy', 'roadmap_item.platform.live_graph_intelligence.unified_schema.event_versioning', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.unified_schema.identity_strategy.requires_edge_taxonomy', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.code_git.requires_identity', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.code_static.requires_identity', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.db_schema.requires_identity', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.db_usage.requires_db_schema', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_usage_collector', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.workflow_events.requires_event_versioning', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector', 'roadmap_item.platform.live_graph_intelligence.unified_schema.event_versioning', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.memory_provenance.requires_event_versioning', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.memory_provenance_collector', 'roadmap_item.platform.live_graph_intelligence.unified_schema.event_versioning', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ingestion.checkpoint_replay.requires_collectors', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph_intelligence.projection.writer.requires_replay', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.projection.topology_api.requires_writer', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.projection.metrics_api.requires_writer', 'roadmap_item.platform.live_graph_intelligence.projection_api.metrics_query_api', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.projection.temporal_api.requires_writer', 'roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.workspace.requires_topology_api', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.code_pane.requires_workspace', 'roadmap_item.platform.live_graph_intelligence.operator_ui.code_graph_pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.data_pane.requires_workspace', 'roadmap_item.platform.live_graph_intelligence.operator_ui.data_graph_pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.workflow_pane.requires_workspace', 'roadmap_item.platform.live_graph_intelligence.operator_ui.workflow_graph_pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.memory_pane.requires_workspace', 'roadmap_item.platform.live_graph_intelligence.operator_ui.memory_graph_pane', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.timeline.requires_temporal_api', 'roadmap_item.platform.live_graph_intelligence.operator_ui.timeline_controls', 'roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.ui.drilldown.requires_metrics_api', 'roadmap_item.platform.live_graph_intelligence.operator_ui.drilldown_panels', 'roadmap_item.platform.live_graph_intelligence.projection_api.metrics_query_api', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph_intelligence.actions.explain.requires_drilldown', 'roadmap_item.platform.live_graph_intelligence.action_layer.explain_node', 'roadmap_item.platform.live_graph_intelligence.operator_ui.drilldown_panels', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.actions.blast_radius.requires_topology', 'roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.actions.generate_change.requires_blast_radius', 'roadmap_item.platform.live_graph_intelligence.action_layer.generate_change', 'roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph_intelligence.actions.execute_guardrails.requires_generate_change', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails', 'roadmap_item.platform.live_graph_intelligence.action_layer.generate_change', 'blocks')
    ) AS t(
        roadmap_item_dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        dependency_kind
    )
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id,
    roadmap_item_id,
    depends_on_roadmap_item_id,
    dependency_kind,
    decision_ref,
    created_at
)
SELECT
    d.roadmap_item_dependency_id,
    d.roadmap_item_id,
    d.depends_on_roadmap_item_id,
    d.dependency_kind,
    'decision.2026-04-15.live-graph-intelligence',
    now()
FROM graph_visibility_module_dependencies AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

UPDATE roadmap_items
SET
    acceptance_criteria = COALESCE(acceptance_criteria, '{}'::jsonb) || jsonb_build_object(
        'architecture_style', 'cqrs',
        'program_scope', 'live_graph_intelligence',
        'command_side_required', true,
        'query_side_required', true,
        'projection_required', true,
        'idempotency_required', true,
        'replay_required', true
    ),
    updated_at = now()
WHERE roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence%';

UPDATE roadmap_items
SET
    acceptance_criteria = COALESCE(acceptance_criteria, '{}'::jsonb) || jsonb_build_object(
        'lane', 'command_and_projection',
        'handler_kind', 'command_handler_and_projector',
        'command_contract', 'append_only_graph_events',
        'projection_contract', 'deterministic_projection_from_event_log',
        'query_contract', 'n/a',
        'replay_contract', 'projection_rebuild_from_checkpoint_zero',
        'verification', jsonb_build_array(
            'command_contract_test',
            'projection_idempotency_test',
            'checkpoint_replay_test'
        )
    ),
    updated_at = now()
WHERE roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence.ingestion_spine%'
   OR roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence.unified_schema%';

UPDATE roadmap_items
SET
    acceptance_criteria = COALESCE(acceptance_criteria, '{}'::jsonb) || jsonb_build_object(
        'lane', 'projection_and_query',
        'handler_kind', 'projector_and_query_handler',
        'command_contract', 'n/a',
        'projection_contract', 'write_optimized_graph_projection',
        'query_contract', 'stable_filterable_graph_read_contract',
        'replay_contract', 'read_model_rebuild_from_event_stream',
        'verification', jsonb_build_array(
            'query_contract_test',
            'read_model_consistency_test',
            'temporal_slice_regression_test'
        )
    ),
    updated_at = now()
WHERE roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence.projection_api%';

UPDATE roadmap_items
SET
    acceptance_criteria = COALESCE(acceptance_criteria, '{}'::jsonb) || jsonb_build_object(
        'lane', 'query_consumer',
        'handler_kind', 'ui_query_consumer',
        'command_contract', 'graph_ui_actions_emit_commands',
        'projection_contract', 'consume_projection_api_only',
        'query_contract', 'ui_graph_query_surface',
        'replay_contract', 'timeline_replay_uses_temporal_query_api',
        'verification', jsonb_build_array(
            'ui_graph_smoke_test',
            'ui_query_contract_snapshot',
            'timeline_replay_e2e_test'
        )
    ),
    updated_at = now()
WHERE roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence.operator_ui%';

UPDATE roadmap_items
SET
    acceptance_criteria = COALESCE(acceptance_criteria, '{}'::jsonb) || jsonb_build_object(
        'lane', 'command_orchestration',
        'handler_kind', 'command_handler',
        'command_contract', 'governed_graph_actions',
        'projection_contract', 'action_audit_projection',
        'query_contract', 'action_eligibility_query',
        'replay_contract', 'action_decision_replayable_from_evidence',
        'verification', jsonb_build_array(
            'policy_gate_test',
            'dry_run_guardrail_test',
            'action_audit_trace_test'
        )
    ),
    updated_at = now()
WHERE roadmap_item_id LIKE 'roadmap_item.platform.live_graph_intelligence.action_layer%';

UPDATE roadmap_items
SET
    registry_paths = (
        SELECT jsonb_build_array(
            'Code&DBs/Workflow/runtime/cqrs/',
            'Code&DBs/Workflow/surfaces/api/',
            'Code&DBs/Workflow/observability/',
            'Code&DBs/Workflow/tests/unit/test_cqrs.py'
        )
    ),
    updated_at = now()
WHERE roadmap_item_id = 'roadmap_item.platform.live_graph_intelligence';

WITH cqrs_intelligence_items AS (
    SELECT *
    FROM (
        VALUES
            (
                'roadmap_item.platform.cqrs.intelligence.program',
                'roadmap.platform.cqrs.intelligence.program',
                'CQRS Read/Write Intelligence Program',
                'initiative',
                'active',
                'p0',
                NULL::text,
                'Migrate remaining roadmap/operator surfaces onto explicit CQRS messages and add semantic/graph/math read-model capabilities.',
                jsonb_build_object(
                    'outcome_gate', 'cqrs_intelligence_program_operational',
                    'phase_order', '3',
                    'packet_strategy', 'cqrs_granular_packets',
                    'architecture_style', 'cqrs'
                ),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.message_taxonomy',
                'roadmap.platform.cqrs.intelligence.message_taxonomy',
                'Message Taxonomy: Commands vs Queries',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Define canonical CQRS message classes, metadata envelope, and naming consistency (query/command neutrality).',
                jsonb_build_object('outcome_gate', 'message_contracts_published', 'phase_order', '3.1'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover',
                'roadmap.platform.cqrs.intelligence.operator_read_query_cutover',
                'Cut Over Operator Reads to Query Bus',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Port remaining operator read entrypoints to CQRS query handlers so API/CLI/MCP stop bypassing the bus.',
                jsonb_build_object('outcome_gate', 'operator_reads_all_on_query_bus', 'phase_order', '3.2'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.write_side_parity',
                'roadmap.platform.cqrs.intelligence.write_side_parity',
                'Write-Side Parity and Idempotency Contracts',
                'workstream',
                'active',
                'p0',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Ensure every mutation path is a command handler with consistent idempotency and audit metadata.',
                jsonb_build_object('outcome_gate', 'writes_unified_under_commands', 'phase_order', '3.3'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.cross_surface_unification',
                'roadmap.platform.cqrs.intelligence.cross_surface_unification',
                'Cross-Surface CQRS Unification',
                'workstream',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Guarantee REST, MCP, and CLI use the same CQRS messages and handlers without surface-specific logic drift.',
                jsonb_build_object('outcome_gate', 'surface_parity_on_cqrs', 'phase_order', '3.4'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.dispatch_observability',
                'roadmap.platform.cqrs.intelligence.dispatch_observability',
                'Dispatch Observability and Audit Events',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Emit started/succeeded/failed dispatch events with duration, correlation, and reason codes.',
                jsonb_build_object('outcome_gate', 'cqrs_dispatch_events_live', 'phase_order', '3.5'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank',
                'roadmap.platform.cqrs.intelligence.semantic_hybrid_rank',
                'Hybrid Semantic + Lexical Ranking Query',
                'task',
                'active',
                'p0',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Add CQRS query that combines pgvector similarity, tsvector relevance, and trgm fuzzy matching for robust recall.',
                jsonb_build_object('outcome_gate', 'hybrid_rank_query_operational', 'phase_order', '3.6'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.graph_critical_path',
                'roadmap.platform.cqrs.intelligence.graph_critical_path',
                'Graph Critical-Path Query',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Compute blocking chains and slack over roadmap dependency graph as a first-class read model.',
                jsonb_build_object('outcome_gate', 'critical_path_read_model_live', 'phase_order', '3.7'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.graph_centrality',
                'roadmap.platform.cqrs.intelligence.graph_centrality',
                'Graph Centrality and Risk Hotspots',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Add betweenness/degree centrality projections to identify fragile roadmap bottlenecks.',
                jsonb_build_object('outcome_gate', 'centrality_projection_live', 'phase_order', '3.8'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.portfolio_entropy',
                'roadmap.platform.cqrs.intelligence.portfolio_entropy',
                'Portfolio Entropy and Concentration Metrics',
                'task',
                'active',
                'p2',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Quantify roadmap thematic concentration/diversification via entropy-based metrics for planning balance.',
                jsonb_build_object('outcome_gate', 'portfolio_entropy_metrics_live', 'phase_order', '3.9'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.materialized_projections',
                'roadmap.platform.cqrs.intelligence.materialized_projections',
                'Materialized CQRS Projection Tables',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Create hot read models (semantic clusters, graph metrics) with deterministic projector rebuild support.',
                jsonb_build_object('outcome_gate', 'materialized_projections_refreshing', 'phase_order', '3.10'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.evidence_weighted_closeout',
                'roadmap.platform.cqrs.intelligence.evidence_weighted_closeout',
                'Evidence-Weighted Decision Queries',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Add read models that score closeout readiness from explicit proof-chain and semantic corroboration.',
                jsonb_build_object('outcome_gate', 'evidence_weighted_queries_available', 'phase_order', '3.11'),
                'decision.2026-04-15.cqrs-intelligence'
            ),
            (
                'roadmap_item.platform.cqrs.intelligence.replay_rebuild',
                'roadmap.platform.cqrs.intelligence.replay_rebuild',
                'Projection Replay and Rebuild Drills',
                'task',
                'active',
                'p1',
                'roadmap_item.platform.cqrs.intelligence.program',
                'Run repeatable replay drills from checkpoint zero to prove projection determinism and drift recovery.',
                jsonb_build_object('outcome_gate', 'replay_rebuild_drills_passing', 'phase_order', '3.12'),
                'decision.2026-04-15.cqrs-intelligence'
            )
    ) AS t(
        roadmap_item_id,
        roadmap_key,
        title,
        item_kind,
        status,
        priority,
        parent_roadmap_item_id,
        summary,
        acceptance_criteria,
        decision_ref
    )
)
INSERT INTO roadmap_items (
    roadmap_item_id,
    roadmap_key,
    title,
    item_kind,
    status,
    priority,
    parent_roadmap_item_id,
    source_bug_id,
    summary,
    acceptance_criteria,
    decision_ref,
    target_start_at,
    target_end_at,
    completed_at,
    created_at,
    updated_at
)
SELECT
    i.roadmap_item_id,
    i.roadmap_key,
    i.title,
    i.item_kind,
    i.status,
    i.priority,
    i.parent_roadmap_item_id,
    NULL,
    i.summary,
    i.acceptance_criteria,
    i.decision_ref,
    NULL,
    NULL,
    NULL,
    now(),
    now()
FROM cqrs_intelligence_items AS i
ORDER BY
    CASE WHEN i.parent_roadmap_item_id IS NULL THEN 0 ELSE 1 END,
    i.roadmap_item_id
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH cqrs_intelligence_dependencies AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item_dependency.platform.cqrs.operator_reads.requires_message_taxonomy', 'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover', 'roadmap_item.platform.cqrs.intelligence.message_taxonomy', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.writes.requires_message_taxonomy', 'roadmap_item.platform.cqrs.intelligence.write_side_parity', 'roadmap_item.platform.cqrs.intelligence.message_taxonomy', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.surface_unification.requires_operator_reads', 'roadmap_item.platform.cqrs.intelligence.cross_surface_unification', 'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.surface_unification.requires_write_parity', 'roadmap_item.platform.cqrs.intelligence.cross_surface_unification', 'roadmap_item.platform.cqrs.intelligence.write_side_parity', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.dispatch_obs.requires_reads', 'roadmap_item.platform.cqrs.intelligence.dispatch_observability', 'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.hybrid_rank.requires_materialized_projections', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.critical_path.requires_materialized_projections', 'roadmap_item.platform.cqrs.intelligence.graph_critical_path', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.centrality.requires_materialized_projections', 'roadmap_item.platform.cqrs.intelligence.graph_centrality', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.portfolio_entropy.requires_materialized_projections', 'roadmap_item.platform.cqrs.intelligence.portfolio_entropy', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.evidence_weighted.requires_hybrid_rank', 'roadmap_item.platform.cqrs.intelligence.evidence_weighted_closeout', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.replay_rebuild.requires_materialized_projections', 'roadmap_item.platform.cqrs.intelligence.replay_rebuild', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'blocks')
    ) AS t(
        roadmap_item_dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        dependency_kind
    )
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id,
    roadmap_item_id,
    depends_on_roadmap_item_id,
    dependency_kind,
    decision_ref,
    created_at
)
SELECT
    d.roadmap_item_dependency_id,
    d.roadmap_item_id,
    d.depends_on_roadmap_item_id,
    d.dependency_kind,
    'decision.2026-04-15.cqrs-intelligence',
    now()
FROM cqrs_intelligence_dependencies AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

WITH cqrs_intelligence_expansion_items AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item.platform.cqrs.intelligence.operator_read.bus_enforcement', 'roadmap.platform.cqrs.intelligence.operator_read.bus_enforcement', 'Operator Read Bus Enforcement', 'task', 'active', 'p0', 'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover', 'Ensure query_operator_surface and direct operator_read entrypoints dispatch through CQRS query handlers only.', jsonb_build_object('phase_order','3.2.1','outcome_gate','operator_read_bus_only')),
            ('roadmap_item.platform.cqrs.intelligence.workflow_query.message_split', 'roadmap.platform.cqrs.intelligence.workflow_query.message_split', 'Workflow Query Handler Message Split', 'task', 'active', 'p0', 'roadmap_item.platform.cqrs.intelligence.operator_read_query_cutover', 'Replace workflow_query_core if/else routers with explicit query message handlers (ListBugs, BugHistory, RecallSearch, ConstraintScope, etc.).', jsonb_build_object('phase_order','3.2.2','outcome_gate','workflow_query_messages_split')),
            ('roadmap_item.platform.cqrs.intelligence.write.idempotency_envelopes', 'roadmap.platform.cqrs.intelligence.write.idempotency_envelopes', 'Write Idempotency Envelope Unification', 'task', 'active', 'p0', 'roadmap_item.platform.cqrs.intelligence.write_side_parity', 'Apply one idempotency envelope contract across closeout, roadmap_write, and bug command handlers.', jsonb_build_object('phase_order','3.3.1','outcome_gate','idempotency_envelopes_unified')),
            ('roadmap_item.platform.cqrs.intelligence.surface.reuse_contract', 'roadmap.platform.cqrs.intelligence.surface.reuse_contract', 'Cross-Surface Message Reuse Contract', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.cross_surface_unification', 'Make CLI, MCP, and REST invoke identical CQRS messages and handlers; no business logic in adapters.', jsonb_build_object('phase_order','3.4.1','outcome_gate','cross_surface_message_reuse')),
            ('roadmap_item.platform.cqrs.intelligence.dispatch.event_contract', 'roadmap.platform.cqrs.intelligence.dispatch.event_contract', 'Dispatch Observability Event Contract', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.dispatch_observability', 'Emit canonical dispatch started/succeeded/failed events with duration, row counts, reason_code, and correlation_id.', jsonb_build_object('phase_order','3.5.1','outcome_gate','dispatch_observability_contract_live')),

            ('roadmap_item.platform.cqrs.intelligence.semantic.find_neighbors', 'roadmap.platform.cqrs.intelligence.semantic.find_neighbors', 'Semantic Primitive: FindSemanticNeighbors', 'task', 'active', 'p0', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'Add query primitive FindSemanticNeighbors(entity_kind, id|text, k, filters...) on CQRS read side.', jsonb_build_object('phase_order','3.6.1','outcome_gate','find_neighbors_query_live')),
            ('roadmap_item.platform.cqrs.intelligence.semantic.cluster_roadmap', 'roadmap.platform.cqrs.intelligence.semantic.cluster_roadmap', 'Semantic Primitive: ClusterRoadmapByEmbeddings', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'Add ClusterRoadmapByEmbeddings(window/filter) returning centroid/medoid and member confidence.', jsonb_build_object('phase_order','3.6.2','outcome_gate','roadmap_embedding_clusters_live')),
            ('roadmap_item.platform.cqrs.intelligence.semantic.explain_neighbor', 'roadmap.platform.cqrs.intelligence.semantic.explain_neighbor', 'Semantic Primitive: ExplainNeighborMatch', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'Add ExplainNeighborMatch(a,b) with contributing fields/tags/similarity evidence.', jsonb_build_object('phase_order','3.6.3','outcome_gate','neighbor_match_explainer_live')),

            ('roadmap_item.platform.cqrs.intelligence.math.cycle_and_slack', 'roadmap.platform.cqrs.intelligence.math.cycle_and_slack', 'Math Read Model: Cycle Risk and Topological Slack', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.graph_critical_path', 'Expose cycle risk and slack computations over roadmap dependency graph as CQRS queries.', jsonb_build_object('phase_order','3.7.1','outcome_gate','cycle_and_slack_queries_live')),
            ('roadmap_item.platform.cqrs.intelligence.math.priority_scoring', 'roadmap.platform.cqrs.intelligence.math.priority_scoring', 'Math Read Model: Priority Scoring Blend', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.graph_centrality', 'Blend severity, blocker degree, due-date risk, and similarity density into explicit priority scoring query.', jsonb_build_object('phase_order','3.8.1','outcome_gate','priority_scoring_query_live')),
            ('roadmap_item.platform.cqrs.intelligence.math.portfolio_entropy_query', 'roadmap.platform.cqrs.intelligence.math.portfolio_entropy_query', 'Math Read Model: Portfolio Entropy Query', 'task', 'active', 'p2', 'roadmap_item.platform.cqrs.intelligence.portfolio_entropy', 'Measure roadmap concentration/diversification with entropy metrics as a first-class read model.', jsonb_build_object('phase_order','3.9.1','outcome_gate','portfolio_entropy_query_live')),

            ('roadmap_item.platform.cqrs.intelligence.db.pgvector_tsvector_hybrid', 'roadmap.platform.cqrs.intelligence.db.pgvector_tsvector_hybrid', 'DB Combination: pgvector + tsvector Hybrid Rank', 'task', 'active', 'p0', 'roadmap_item.platform.cqrs.intelligence.semantic_hybrid_rank', 'Implement weighted semantic+lexical ranking query across embeddings and full-text vectors.', jsonb_build_object('phase_order','3.6.4','outcome_gate','pgvector_tsvector_hybrid_live')),
            ('roadmap_item.platform.cqrs.intelligence.db.pgtrgm_fallback', 'roadmap.platform.cqrs.intelligence.db.pgtrgm_fallback', 'DB Combination: pg_trgm Fuzzy Fallback', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.db.pgvector_tsvector_hybrid', 'Add pg_trgm typo/fuzzy fallback when embeddings are missing or low-confidence.', jsonb_build_object('phase_order','3.6.5','outcome_gate','pgtrgm_fallback_live')),
            ('roadmap_item.platform.cqrs.intelligence.db.materialized_dashboards', 'roadmap.platform.cqrs.intelligence.db.materialized_dashboards', 'DB Combination: Materialized Dashboard Read Models', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.materialized_projections', 'Publish hot dashboard materializations refreshed by CQRS projector events.', jsonb_build_object('phase_order','3.10.1','outcome_gate','materialized_dashboard_models_live')),

            ('roadmap_item.platform.cqrs.intelligence.evidence.proof_chain_score', 'roadmap.platform.cqrs.intelligence.evidence.proof_chain_score', 'Evidence-Aware Query: Proof Chain Score', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.evidence_weighted_closeout', 'Join roadmap, bugs, receipts, and runs into one proof-chain score read model.', jsonb_build_object('phase_order','3.11.1','outcome_gate','proof_chain_score_live')),
            ('roadmap_item.platform.cqrs.intelligence.evidence.command_preconditions', 'roadmap.platform.cqrs.intelligence.evidence.command_preconditions', 'Evidence-Aware Commands: Proof Threshold Preconditions', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.evidence_weighted_closeout', 'Gate closeout and sensitive commands behind explicit proof threshold checks.', jsonb_build_object('phase_order','3.11.2','outcome_gate','proof_threshold_preconditions_live')),

            ('roadmap_item.platform.cqrs.intelligence.combo.mcp_thin_adapters', 'roadmap.platform.cqrs.intelligence.combo.mcp_thin_adapters', 'Interface Combo: CQRS + MCP Thin Adapters', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.surface.reuse_contract', 'Reduce MCP tools to thin CQRS adapters with zero duplicate domain logic.', jsonb_build_object('phase_order','3.4.2','outcome_gate','mcp_thin_adapters_live')),
            ('roadmap_item.platform.cqrs.intelligence.combo.projection_tables', 'roadmap.platform.cqrs.intelligence.combo.projection_tables', 'Interface Combo: CQRS + Projection Tables', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.db.materialized_dashboards', 'Run fast read-side tables such as roadmap_semantic_clusters and bug_topic_rollups from projector updates.', jsonb_build_object('phase_order','3.10.2','outcome_gate','projection_tables_live')),
            ('roadmap_item.platform.cqrs.intelligence.combo.policy_engine', 'roadmap.platform.cqrs.intelligence.combo.policy_engine', 'Interface Combo: CQRS + Policy Engine Command Guards', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.write.idempotency_envelopes', 'Centralize scope and authority enforcement in command handlers via policy engine integration.', jsonb_build_object('phase_order','3.3.2','outcome_gate','policy_enforcement_centralized')),
            ('roadmap_item.platform.cqrs.intelligence.combo.replay_debug', 'roadmap.platform.cqrs.intelligence.combo.replay_debug', 'Interface Combo: CQRS + Replay Debug Tooling', 'task', 'active', 'p1', 'roadmap_item.platform.cqrs.intelligence.replay_rebuild', 'Enable deterministic replay of dispatch stream for projection rebuild and drift debugging.', jsonb_build_object('phase_order','3.12.1','outcome_gate','replay_debug_tooling_live'))
    ) AS t(
        roadmap_item_id,
        roadmap_key,
        title,
        item_kind,
        status,
        priority,
        parent_roadmap_item_id,
        summary,
        acceptance_criteria
    )
)
INSERT INTO roadmap_items (
    roadmap_item_id, roadmap_key, title, item_kind, status, priority,
    parent_roadmap_item_id, source_bug_id, summary, acceptance_criteria,
    decision_ref, target_start_at, target_end_at, completed_at, created_at, updated_at
)
SELECT
    i.roadmap_item_id, i.roadmap_key, i.title, i.item_kind, i.status, i.priority,
    i.parent_roadmap_item_id, NULL, i.summary, i.acceptance_criteria,
    'decision.2026-04-15.cqrs-intelligence', NULL, NULL, NULL, now(), now()
FROM cqrs_intelligence_expansion_items AS i
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH cqrs_intelligence_expansion_dependencies AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item_dependency.platform.cqrs.workflow_query_split.requires_operator_read_bus', 'roadmap_item.platform.cqrs.intelligence.workflow_query.message_split', 'roadmap_item.platform.cqrs.intelligence.operator_read.bus_enforcement', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.surface_reuse.requires_query_split', 'roadmap_item.platform.cqrs.intelligence.surface.reuse_contract', 'roadmap_item.platform.cqrs.intelligence.workflow_query.message_split', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.dispatch_event_contract.requires_surface_reuse', 'roadmap_item.platform.cqrs.intelligence.dispatch.event_contract', 'roadmap_item.platform.cqrs.intelligence.surface.reuse_contract', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.cluster_roadmap.requires_find_neighbors', 'roadmap_item.platform.cqrs.intelligence.semantic.cluster_roadmap', 'roadmap_item.platform.cqrs.intelligence.semantic.find_neighbors', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.explain_neighbor.requires_find_neighbors', 'roadmap_item.platform.cqrs.intelligence.semantic.explain_neighbor', 'roadmap_item.platform.cqrs.intelligence.semantic.find_neighbors', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.math.priority_scoring.requires_centrality', 'roadmap_item.platform.cqrs.intelligence.math.priority_scoring', 'roadmap_item.platform.cqrs.intelligence.graph_centrality', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.math.portfolio_entropy.requires_entropy_base', 'roadmap_item.platform.cqrs.intelligence.math.portfolio_entropy_query', 'roadmap_item.platform.cqrs.intelligence.portfolio_entropy', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.db.pgtrgm.requires_hybrid_rank', 'roadmap_item.platform.cqrs.intelligence.db.pgtrgm_fallback', 'roadmap_item.platform.cqrs.intelligence.db.pgvector_tsvector_hybrid', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.proof_chain.requires_hybrid_rank', 'roadmap_item.platform.cqrs.intelligence.evidence.proof_chain_score', 'roadmap_item.platform.cqrs.intelligence.db.pgvector_tsvector_hybrid', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.preconditions.requires_proof_chain', 'roadmap_item.platform.cqrs.intelligence.evidence.command_preconditions', 'roadmap_item.platform.cqrs.intelligence.evidence.proof_chain_score', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.combo.mcp_thin_adapters.requires_surface_reuse', 'roadmap_item.platform.cqrs.intelligence.combo.mcp_thin_adapters', 'roadmap_item.platform.cqrs.intelligence.surface.reuse_contract', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.combo.projection_tables.requires_materialized_dashboards', 'roadmap_item.platform.cqrs.intelligence.combo.projection_tables', 'roadmap_item.platform.cqrs.intelligence.db.materialized_dashboards', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.combo.policy_engine.requires_write_idempotency', 'roadmap_item.platform.cqrs.intelligence.combo.policy_engine', 'roadmap_item.platform.cqrs.intelligence.write.idempotency_envelopes', 'blocks'),
            ('roadmap_item_dependency.platform.cqrs.combo.replay_debug.requires_replay_rebuild', 'roadmap_item.platform.cqrs.intelligence.combo.replay_debug', 'roadmap_item.platform.cqrs.intelligence.replay_rebuild', 'blocks')
    ) AS t(
        roadmap_item_dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        dependency_kind
    )
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id, roadmap_item_id, depends_on_roadmap_item_id,
    dependency_kind, decision_ref, created_at
)
SELECT
    d.roadmap_item_dependency_id, d.roadmap_item_id, d.depends_on_roadmap_item_id,
    d.dependency_kind, 'decision.2026-04-15.cqrs-intelligence', now()
FROM cqrs_intelligence_expansion_dependencies AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

WITH cqrs_intelligence_micro_items AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy.node_kinds', 'roadmap.platform.live_graph_intelligence.unified_schema.node_taxonomy.node_kinds', 'Schema Micro: Node Kind Enum', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy', 'Define and freeze allowed node kind enum values.', jsonb_build_object('phase_order','2.1.1.a','outcome_gate','node_kind_enum_frozen')),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy.required_fields', 'roadmap.platform.live_graph_intelligence.unified_schema.node_taxonomy.required_fields', 'Schema Micro: Required Node Fields', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy', 'Define required node fields and validation behavior.', jsonb_build_object('phase_order','2.1.1.b','outcome_gate','node_required_fields_frozen')),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy.edge_kinds', 'roadmap.platform.live_graph_intelligence.unified_schema.edge_taxonomy.edge_kinds', 'Schema Micro: Edge Kind Enum', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy', 'Define and freeze allowed edge kind enum values.', jsonb_build_object('phase_order','2.1.2.a','outcome_gate','edge_kind_enum_frozen')),
            ('roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy.id_rules', 'roadmap.platform.live_graph_intelligence.unified_schema.identity_strategy.id_rules', 'Schema Micro: Deterministic ID Rules', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy', 'Define deterministic identity rules for nodes and edges.', jsonb_build_object('phase_order','2.1.4.a','outcome_gate','deterministic_id_rules_frozen')),

            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector.commit_churn', 'roadmap.platform.live_graph_intelligence.ingestion_spine.code_git_collector.commit_churn', 'Ingestion Micro: Commit Churn Feed', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector', 'Emit commit churn events from git history into command stream.', jsonb_build_object('phase_order','2.2.1.a','outcome_gate','commit_churn_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector.ownership_feed', 'roadmap.platform.live_graph_intelligence.ingestion_spine.code_git_collector.ownership_feed', 'Ingestion Micro: Ownership Feed', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector', 'Emit ownership signals per module/file into command stream.', jsonb_build_object('phase_order','2.2.1.b','outcome_gate','ownership_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector.import_graph', 'roadmap.platform.live_graph_intelligence.ingestion_spine.code_static_collector.import_graph', 'Ingestion Micro: Import Graph', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector', 'Emit import dependency edges from static analysis.', jsonb_build_object('phase_order','2.2.2.a','outcome_gate','import_graph_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector.call_graph', 'roadmap.platform.live_graph_intelligence.ingestion_spine.code_static_collector.call_graph', 'Ingestion Micro: Call Graph', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector', 'Emit call graph edges for reachable symbol paths.', jsonb_build_object('phase_order','2.2.2.b','outcome_gate','call_graph_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector.tables_fk', 'roadmap.platform.live_graph_intelligence.ingestion_spine.db_schema_collector.tables_fk', 'Ingestion Micro: Tables and FK Graph', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector', 'Emit table and FK topology snapshots as graph commands.', jsonb_build_object('phase_order','2.2.3.a','outcome_gate','table_fk_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_usage_collector.query_heat', 'roadmap.platform.live_graph_intelligence.ingestion_spine.db_usage_collector.query_heat', 'Ingestion Micro: Query Heat Metrics', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_usage_collector', 'Emit query heat and latency metrics linked to tables.', jsonb_build_object('phase_order','2.2.4.a','outcome_gate','query_heat_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.run_lifecycle', 'roadmap.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.run_lifecycle', 'Ingestion Micro: Run Lifecycle Events', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector', 'Emit run state transitions and retries as lineage events.', jsonb_build_object('phase_order','2.2.5.a','outcome_gate','run_lifecycle_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.tool_hops', 'roadmap.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.tool_hops', 'Ingestion Micro: Tool-Hop Events', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector', 'Emit tool invocation hops for workflow path inspection.', jsonb_build_object('phase_order','2.2.5.b','outcome_gate','tool_hop_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.memory_provenance_collector.retrieval_hits', 'roadmap.platform.live_graph_intelligence.ingestion_spine.memory_provenance_collector.retrieval_hits', 'Ingestion Micro: Retrieval Hit Provenance', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.memory_provenance_collector', 'Emit retrieval hit provenance across sessions.', jsonb_build_object('phase_order','2.2.6.a','outcome_gate','retrieval_hit_events_live')),
            ('roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay.offset_checkpointing', 'roadmap.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay.offset_checkpointing', 'Ingestion Micro: Offset Checkpointing', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay', 'Persist deterministic ingestion offsets per collector stream.', jsonb_build_object('phase_order','2.2.7.a','outcome_gate','offset_checkpointing_live')),

            ('roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer.node_upsert', 'roadmap.platform.live_graph_intelligence.projection_api.projection_writer.node_upsert', 'Projection Micro: Node Upsert', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'Implement idempotent node projection upsert handler.', jsonb_build_object('phase_order','2.3.1.a','outcome_gate','node_upsert_live')),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer.edge_upsert', 'roadmap.platform.live_graph_intelligence.projection_api.projection_writer.edge_upsert', 'Projection Micro: Edge Upsert', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer', 'Implement idempotent edge projection upsert handler.', jsonb_build_object('phase_order','2.3.1.b','outcome_gate','edge_upsert_live')),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api.scope_filters', 'roadmap.platform.live_graph_intelligence.projection_api.topology_query_api.scope_filters', 'Query Micro: Scope Filters', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api', 'Add scope/environment/domain filters to topology query handler.', jsonb_build_object('phase_order','2.3.2.a','outcome_gate','topology_scope_filters_live')),
            ('roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api.window_slice', 'roadmap.platform.live_graph_intelligence.projection_api.temporal_query_api.window_slice', 'Query Micro: Time Window Slice', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api', 'Serve deterministic windowed slices of graph state.', jsonb_build_object('phase_order','2.3.4.a','outcome_gate','temporal_window_slices_live')),

            ('roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell.global_filters', 'roadmap.platform.live_graph_intelligence.operator_ui.graph_workspace_shell.global_filters', 'UI Micro: Global Filters', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell', 'Add global graph filters by map, env, and time range.', jsonb_build_object('phase_order','2.4.1.a','outcome_gate','ui_global_filters_live')),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.code_graph_pane.hotspot_overlay', 'roadmap.platform.live_graph_intelligence.operator_ui.code_graph_pane.hotspot_overlay', 'UI Micro: Code Hotspot Overlay', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.operator_ui.code_graph_pane', 'Render code churn and coupling overlays in code map pane.', jsonb_build_object('phase_order','2.4.2.a','outcome_gate','code_hotspot_overlay_live')),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.data_graph_pane.lineage_overlay', 'roadmap.platform.live_graph_intelligence.operator_ui.data_graph_pane.lineage_overlay', 'UI Micro: Data Lineage Overlay', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.operator_ui.data_graph_pane', 'Render table lineage and write/read path overlays.', jsonb_build_object('phase_order','2.4.3.a','outcome_gate','data_lineage_overlay_live')),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.workflow_graph_pane.failure_overlay', 'roadmap.platform.live_graph_intelligence.operator_ui.workflow_graph_pane.failure_overlay', 'UI Micro: Workflow Failure Overlay', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.operator_ui.workflow_graph_pane', 'Render retry and failure paths on workflow graph.', jsonb_build_object('phase_order','2.4.4.a','outcome_gate','workflow_failure_overlay_live')),
            ('roadmap_item.platform.live_graph_intelligence.operator_ui.timeline_controls.time_slider', 'roadmap.platform.live_graph_intelligence.operator_ui.timeline_controls.time_slider', 'UI Micro: Time Slider', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.operator_ui.timeline_controls', 'Implement timeline slider with temporal query integration.', jsonb_build_object('phase_order','2.4.6.a','outcome_gate','timeline_slider_live')),

            ('roadmap_item.platform.live_graph_intelligence.action_layer.explain_node.prompt_contract', 'roadmap.platform.live_graph_intelligence.action_layer.explain_node.prompt_contract', 'Action Micro: Explain Prompt Contract', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.action_layer.explain_node', 'Define explain-node prompt contract and output schema.', jsonb_build_object('phase_order','2.5.1.a','outcome_gate','explain_prompt_contract_live')),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius.impact_query', 'roadmap.platform.live_graph_intelligence.action_layer.blast_radius.impact_query', 'Action Micro: Blast Radius Impact Query', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius', 'Implement impact query for nearest affected nodes and edges.', jsonb_build_object('phase_order','2.5.2.a','outcome_gate','blast_radius_impact_query_live')),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails.policy_checks', 'roadmap.platform.live_graph_intelligence.action_layer.execute_guardrails.policy_checks', 'Action Micro: Guardrail Policy Checks', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails', 'Implement mandatory policy checks before command execution.', jsonb_build_object('phase_order','2.5.4.a','outcome_gate','guardrail_policy_checks_live')),
            ('roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails.dry_run_path', 'roadmap.platform.live_graph_intelligence.action_layer.execute_guardrails.dry_run_path', 'Action Micro: Guardrail Dry-Run Path', 'task', 'active', 'p1', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails', 'Implement dry-run execution path and surfaced diff preview.', jsonb_build_object('phase_order','2.5.4.b','outcome_gate','guardrail_dry_run_live'))
    ) AS t(
        roadmap_item_id, roadmap_key, title, item_kind, status, priority,
        parent_roadmap_item_id, summary, acceptance_criteria
    )
)
INSERT INTO roadmap_items (
    roadmap_item_id, roadmap_key, title, item_kind, status, priority,
    parent_roadmap_item_id, source_bug_id, summary, acceptance_criteria,
    decision_ref, target_start_at, target_end_at, completed_at, created_at, updated_at
)
SELECT
    i.roadmap_item_id, i.roadmap_key, i.title, i.item_kind, i.status, i.priority,
    i.parent_roadmap_item_id, NULL, i.summary,
    i.acceptance_criteria || jsonb_build_object(
        'architecture_style', 'cqrs',
        'granularity', 'micro_task',
        'verification', jsonb_build_array('unit_test', 'integration_smoke', 'replay_or_idempotency_check')
    ),
    'decision.2026-04-15.live-graph-intelligence', NULL, NULL, NULL, now(), now()
FROM cqrs_intelligence_micro_items AS i
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH cqrs_intelligence_micro_dependencies AS (
    SELECT *
    FROM (
        VALUES
            ('roadmap_item_dependency.platform.live_graph.micro.required_fields.requires_node_kinds', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy.required_fields', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy.node_kinds', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.edge_kinds.requires_required_fields', 'roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy.edge_kinds', 'roadmap_item.platform.live_graph_intelligence.unified_schema.node_taxonomy.required_fields', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.id_rules.requires_edge_kinds', 'roadmap_item.platform.live_graph_intelligence.unified_schema.identity_strategy.id_rules', 'roadmap_item.platform.live_graph_intelligence.unified_schema.edge_taxonomy.edge_kinds', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph.micro.ownership.requires_commit_churn', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector.ownership_feed', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_git_collector.commit_churn', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.call_graph.requires_import_graph', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector.call_graph', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.code_static_collector.import_graph', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.db_usage.requires_tables_fk', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_usage_collector.query_heat', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.db_schema_collector.tables_fk', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.tool_hops.requires_run_lifecycle', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.tool_hops', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.run_lifecycle', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.offset_checkpointing.requires_tool_hops', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.checkpoint_replay.offset_checkpointing', 'roadmap_item.platform.live_graph_intelligence.ingestion_spine.workflow_event_collector.tool_hops', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph.micro.edge_upsert.requires_node_upsert', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer.edge_upsert', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer.node_upsert', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.scope_filters.requires_edge_upsert', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api.scope_filters', 'roadmap_item.platform.live_graph_intelligence.projection_api.projection_writer.edge_upsert', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.window_slice.requires_scope_filters', 'roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api.window_slice', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api.scope_filters', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph.micro.code_hotspot.requires_global_filters', 'roadmap_item.platform.live_graph_intelligence.operator_ui.code_graph_pane.hotspot_overlay', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell.global_filters', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.data_lineage.requires_global_filters', 'roadmap_item.platform.live_graph_intelligence.operator_ui.data_graph_pane.lineage_overlay', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell.global_filters', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.workflow_failure.requires_global_filters', 'roadmap_item.platform.live_graph_intelligence.operator_ui.workflow_graph_pane.failure_overlay', 'roadmap_item.platform.live_graph_intelligence.operator_ui.graph_workspace_shell.global_filters', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.time_slider.requires_window_slice', 'roadmap_item.platform.live_graph_intelligence.operator_ui.timeline_controls.time_slider', 'roadmap_item.platform.live_graph_intelligence.projection_api.temporal_query_api.window_slice', 'blocks'),

            ('roadmap_item_dependency.platform.live_graph.micro.impact_query.requires_scope_filters', 'roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius.impact_query', 'roadmap_item.platform.live_graph_intelligence.projection_api.topology_query_api.scope_filters', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.policy_checks.requires_impact_query', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails.policy_checks', 'roadmap_item.platform.live_graph_intelligence.action_layer.blast_radius.impact_query', 'blocks'),
            ('roadmap_item_dependency.platform.live_graph.micro.dry_run.requires_policy_checks', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails.dry_run_path', 'roadmap_item.platform.live_graph_intelligence.action_layer.execute_guardrails.policy_checks', 'blocks')
    ) AS t(
        roadmap_item_dependency_id,
        roadmap_item_id,
        depends_on_roadmap_item_id,
        dependency_kind
    )
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id, roadmap_item_id, depends_on_roadmap_item_id,
    dependency_kind, decision_ref, created_at
)
SELECT
    d.roadmap_item_dependency_id, d.roadmap_item_id, d.depends_on_roadmap_item_id,
    d.dependency_kind, 'decision.2026-04-15.live-graph-intelligence', now()
FROM cqrs_intelligence_micro_dependencies AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

WITH active_non_stale_parents AS (
    SELECT r.roadmap_item_id, r.roadmap_key, r.title, r.summary, r.priority
    FROM roadmap_items AS r
    WHERE lower(r.status) = 'active'
      AND r.roadmap_item_id NOT LIKE 'roadmap_item.platform.live_graph_intelligence%'
      AND r.roadmap_item_id NOT LIKE 'roadmap_item.platform.cqrs.intelligence%'
      AND r.roadmap_item_id NOT LIKE '%.micro.%'
      AND r.roadmap_item_id NOT LIKE '%stale%'
      AND NOT EXISTS (
          SELECT 1
          FROM roadmap_items AS c
          WHERE c.parent_roadmap_item_id = r.roadmap_item_id
      )
),
auto_micro_rows AS (
    SELECT
        p.roadmap_item_id AS parent_roadmap_item_id,
        p.roadmap_key AS parent_roadmap_key,
        p.title AS parent_title,
        p.summary AS parent_summary,
        p.priority AS parent_priority,
        s.suffix,
        s.phase_suffix,
        s.step_title,
        s.step_summary,
        s.step_lane
    FROM active_non_stale_parents AS p
    CROSS JOIN (
        VALUES
            ('contract', 'a', 'Contract and Scope', 'Define bounded contract, invariants, and interface seams for this roadmap item.', 'command_contract'),
            ('implementation', 'b', 'Implementation Slice', 'Implement smallest production path for the declared contract with idempotent writes and projection safety.', 'implementation'),
            ('verification', 'c', 'Verification and Replay Proof', 'Prove behavior with deterministic tests, replay checks, and operator-visible evidence.', 'verification')
    ) AS s(suffix, phase_suffix, step_title, step_summary, step_lane)
)
INSERT INTO roadmap_items (
    roadmap_item_id, roadmap_key, title, item_kind, status, priority,
    parent_roadmap_item_id, source_bug_id, summary, acceptance_criteria,
    decision_ref, target_start_at, target_end_at, completed_at, created_at, updated_at
)
SELECT
    r.parent_roadmap_item_id || '.micro.' || r.suffix,
    replace(r.parent_roadmap_item_id || '.micro.' || r.suffix, 'roadmap_item.', 'roadmap.'),
    r.parent_title || ': ' || r.step_title,
    'task',
    'active',
    CASE WHEN r.parent_priority = 'p0' THEN 'p1' ELSE r.parent_priority END,
    r.parent_roadmap_item_id,
    NULL,
    r.step_summary || ' Parent context: ' || coalesce(r.parent_summary, ''),
    jsonb_build_object(
        'architecture_style', 'cqrs',
        'granularity', 'auto_micro_task',
        'auto_breakdown', true,
        'lane', r.step_lane,
        'phase_order', 'auto.' || r.phase_suffix,
        'outcome_gate', 'auto_micro_' || r.suffix || '_ready',
        'verification', jsonb_build_array('unit_test', 'integration_smoke', 'operator_evidence_note')
    ),
    'decision.2026-04-15.roadmap-auto-micro-breakdown',
    NULL, NULL, NULL, now(), now()
FROM auto_micro_rows AS r
ON CONFLICT (roadmap_item_id) DO UPDATE
SET
    roadmap_key = EXCLUDED.roadmap_key,
    title = EXCLUDED.title,
    item_kind = EXCLUDED.item_kind,
    status = EXCLUDED.status,
    priority = EXCLUDED.priority,
    parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
    summary = EXCLUDED.summary,
    acceptance_criteria = EXCLUDED.acceptance_criteria,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

WITH active_non_stale_parents AS (
    SELECT r.roadmap_item_id
    FROM roadmap_items AS r
    WHERE lower(r.status) = 'active'
      AND r.roadmap_item_id NOT LIKE 'roadmap_item.platform.live_graph_intelligence%'
      AND r.roadmap_item_id NOT LIKE 'roadmap_item.platform.cqrs.intelligence%'
      AND r.roadmap_item_id NOT LIKE '%.micro.%'
      AND r.roadmap_item_id NOT LIKE '%stale%'
      AND EXISTS (
          SELECT 1
          FROM roadmap_items AS c
          WHERE c.parent_roadmap_item_id = r.roadmap_item_id
            AND c.roadmap_item_id LIKE r.roadmap_item_id || '.micro.%'
      )
),
auto_micro_dependency_rows AS (
    SELECT *
    FROM (
        SELECT
            'roadmap_item_dependency.auto_micro.impl_requires_contract.' || replace(p.roadmap_item_id, '.', '_') AS roadmap_item_dependency_id,
            p.roadmap_item_id || '.micro.implementation' AS roadmap_item_id,
            p.roadmap_item_id || '.micro.contract' AS depends_on_roadmap_item_id,
            'blocks' AS dependency_kind
        FROM active_non_stale_parents AS p
        UNION ALL
        SELECT
            'roadmap_item_dependency.auto_micro.verify_requires_impl.' || replace(p.roadmap_item_id, '.', '_') AS roadmap_item_dependency_id,
            p.roadmap_item_id || '.micro.verification' AS roadmap_item_id,
            p.roadmap_item_id || '.micro.implementation' AS depends_on_roadmap_item_id,
            'blocks' AS dependency_kind
        FROM active_non_stale_parents AS p
        UNION ALL
        SELECT
            'roadmap_item_dependency.auto_micro.parent_requires_verify.' || replace(p.roadmap_item_id, '.', '_') AS roadmap_item_dependency_id,
            p.roadmap_item_id AS roadmap_item_id,
            p.roadmap_item_id || '.micro.verification' AS depends_on_roadmap_item_id,
            'blocks' AS dependency_kind
        FROM active_non_stale_parents AS p
    ) AS u
)
INSERT INTO roadmap_item_dependencies (
    roadmap_item_dependency_id, roadmap_item_id, depends_on_roadmap_item_id,
    dependency_kind, decision_ref, created_at
)
SELECT
    d.roadmap_item_dependency_id, d.roadmap_item_id, d.depends_on_roadmap_item_id,
    d.dependency_kind, 'decision.2026-04-15.roadmap-auto-micro-breakdown', now()
FROM auto_micro_dependency_rows AS d
ON CONFLICT (roadmap_item_dependency_id) DO UPDATE
SET
    roadmap_item_id = EXCLUDED.roadmap_item_id,
    depends_on_roadmap_item_id = EXCLUDED.depends_on_roadmap_item_id,
    dependency_kind = EXCLUDED.dependency_kind,
    decision_ref = EXCLUDED.decision_ref;

COMMIT;
