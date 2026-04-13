from __future__ import annotations

from pathlib import Path

from storage.migrations import (
    workflow_migration_expected_objects,
    workflow_migration_manifest,
    workflow_migration_path,
    workflow_migration_statements,
)


def test_workflow_migration_manifest_includes_provider_route_health_budget_migration() -> None:
    filenames = [entry.filename for entry in workflow_migration_manifest()]
    assert "006_platform_authority_schema.sql" in filenames
    assert "007_provider_route_health_budget.sql" in filenames
    assert "008_workflow_class_and_schedule_schema.sql" in filenames
    assert "009_bug_and_roadmap_authority.sql" in filenames
    assert "010_operator_control_authority.sql" in filenames
    assert "011_runtime_breadth_authority.sql" in filenames
    assert "040_control_commands.sql" in filenames
    assert "042_workflow_control_command_types.sql" in filenames
    assert "050_verification_registry.sql" in filenames
    assert "069_compile_index_snapshots.sql" in filenames
    assert "070_compile_artifact_reuse_keys.sql" in filenames
    assert "071_repo_snapshots_runtime_breadth_repair.sql" in filenames
    assert "072_verifier_healer_authority.sql" in filenames
    assert "073_workflow_run_packet_inspection.sql" in filenames
    assert "074_provider_policy_multi_provider_refs.sql" in filenames
    assert "075_notify_system_events.sql" in filenames
    assert "076_provider_cli_profile_transport_metadata.sql" in filenames
    assert "077_provider_cli_profile_prompt_mode.sql" in filenames
    assert "078_provider_transport_admission_receipts.sql" in filenames
    assert "079_workflow_job_runtime_context.sql" in filenames
    assert "080_workflow_job_submissions.sql" in filenames
    assert "087_workflow_chain_authority.sql" in filenames
    assert "088_workflow_chain_dependency_and_adoption_authority.sql" in filenames
    assert "089_control_operator_frames.sql" in filenames
    assert "090_workflow_chain_cancellation_and_alignment.sql" in filenames
    assert "091_control_operator_frame_uniqueness.sql" in filenames
    assert "095_model_profile_auto_seed_trigger.sql" in filenames
    assert filenames[-1] == "095_model_profile_auto_seed_trigger.sql"


def test_platform_authority_migration_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("006_platform_authority_schema.sql")
    names = {item.object_name for item in objects}
    assert "context_bundles" in names
    assert "context_bundle_anchors" in names
    assert "context_bundles_workspace_runtime_idx" in names
    assert "context_bundles_bundle_hash_idx" in names
    assert "context_bundle_anchors_bundle_idx" in names
    assert "context_bundle_anchors_ref_idx" in names
    assert "provider_model_candidates" in names
    assert "model_profile_candidate_bindings" in names
    assert "event_subscriptions" in names
    assert "subscription_checkpoints" in names
    assert "workflow_lanes" in names
    assert "workflow_lane_policies" in names


def test_provider_route_health_budget_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("007_provider_route_health_budget.sql")
    names = {item.object_name for item in objects}
    assert "provider_route_health_windows" in names
    assert "provider_budget_windows" in names
    assert "route_eligibility_states" in names
    assert "provider_route_health_windows_provider_status_idx" in names
    assert "provider_budget_windows_provider_scope_status_idx" in names
    assert "route_eligibility_states_profile_candidate_status_idx" in names


def test_workflow_class_and_schedule_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("008_workflow_class_and_schedule_schema.sql")
    names = {item.object_name for item in objects}
    assert "workflow_classes" in names
    assert "schedule_definitions" in names
    assert "recurring_run_windows" in names
    assert "workflow_classes_name_status_idx" in names
    assert "schedule_definitions_workflow_class_status_idx" in names
    assert "recurring_run_windows_schedule_status_idx" in names


def test_bug_and_roadmap_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("009_bug_and_roadmap_authority.sql")
    names = {item.object_name for item in objects}
    assert "bugs" in names
    assert "bug_evidence_links" in names
    assert "roadmap_items" in names
    assert "roadmap_item_dependencies" in names
    assert "bugs_status_severity_opened_at_idx" in names
    assert "bug_evidence_links_bug_created_at_idx" in names
    assert "roadmap_items_status_priority_target_end_idx" in names
    assert "roadmap_item_dependencies_item_idx" in names


def test_operator_control_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("010_operator_control_authority.sql")
    names = {item.object_name for item in objects}
    assert "operator_decisions" in names
    assert "cutover_gates" in names
    assert "work_item_workflow_bindings" in names
    assert "operator_decisions_kind_status_decided_idx" in names
    assert "cutover_gates_status_kind_opened_idx" in names
    assert "work_item_workflow_bindings_status_kind_idx" in names


def test_runtime_breadth_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("011_runtime_breadth_authority.sql")
    names = {item.object_name for item in objects}
    assert "provider_failover_bindings" in names
    assert "provider_endpoint_bindings" in names
    assert "persona_profiles" in names
    assert "persona_context_bindings" in names
    assert "fork_profiles" in names
    assert "fork_worktree_bindings" in names
    assert "provider_failover_bindings_scope_idx" in names
    assert "provider_endpoint_bindings_policy_status_idx" in names
    assert "persona_profiles_name_status_idx" in names
    assert "persona_context_bindings_profile_idx" in names
    assert "fork_profiles_name_status_idx" in names
    assert "fork_worktree_bindings_profile_status_idx" in names


def test_verification_registry_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("050_verification_registry.sql")
    names = {item.object_name for item in objects}
    assert "verification_registry" in names


def test_compile_spine_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("057_compile_spine_authority.sql")
    names = {item.object_name for item in objects}
    assert "compile_artifacts" in names
    assert "capability_catalog" in names
    assert "verify_refs" in names
    assert "compile_artifacts_kind_revision_idx" in names
    assert "compile_artifacts_content_hash_idx" in names
    assert "capability_catalog_kind_enabled_idx" in names
    assert "capability_catalog_route_idx" in names
    assert "verify_refs_verification_enabled_idx" in names


def test_compile_artifact_reuse_key_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("070_compile_artifact_reuse_keys.sql")
    names = {item.object_name for item in objects}
    assert "compile_artifacts_kind_input_fingerprint_idx" in names


def test_repo_snapshot_runtime_breadth_repair_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("071_repo_snapshots_runtime_breadth_repair.sql")
    names = {item.object_name for item in objects}
    assert "repo_snapshots" in names
    assert "repo_snapshots_repo_fingerprint_idx" in names
    assert "repo_snapshots_workspace_runtime_idx" in names


def test_verifier_healer_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("072_verifier_healer_authority.sql")
    names = {item.object_name for item in objects}
    assert "verifier_registry" in names
    assert "healer_registry" in names
    assert "verifier_healer_bindings" in names
    assert "verification_runs" in names
    assert "healing_runs" in names
    assert "verifier_registry_kind_enabled_idx" in names
    assert "verifier_registry_verification_ref_idx" in names
    assert "healer_registry_kind_enabled_idx" in names
    assert "healer_registry_auto_mode_idx" in names
    assert "verifier_healer_bindings_verifier_enabled_idx" in names
    assert "verifier_healer_bindings_healer_enabled_idx" in names
    assert "verification_runs_verifier_attempted_idx" in names
    assert "verification_runs_target_status_idx" in names
    assert "healing_runs_healer_attempted_idx" in names
    assert "healing_runs_verifier_status_idx" in names


def test_compile_index_snapshots_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("069_compile_index_snapshots.sql")
    names = {item.object_name for item in objects}
    assert "compile_index_snapshots" in names
    assert "compile_index_snapshots_surface_name_refreshed_idx" in names
    assert "compile_index_snapshots_surface_name_revision_idx" in names
    assert "compile_index_snapshots_repo_fingerprint_idx" in names
    assert "compile_index_snapshots_stale_after_idx" in names


def test_execution_packet_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("065_execution_packet_authority.sql")
    names = {item.object_name for item in objects}
    assert "execution_packets" in names
    assert "execution_packets_definition_plan_idx" in names
    assert "execution_packets_run_idx" in names
    assert "execution_packets_packet_hash_idx" in names


def test_control_commands_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("040_control_commands.sql")
    names = {item.object_name for item in objects}
    assert "control_commands" in names
    assert "idx_control_commands_status_requested_at" in names
    assert "idx_control_commands_type_requested_at" in names
    assert "uq_control_commands_idempotency_key" in names
    assert "idx_control_commands_result_ref" in names


def test_workflow_chain_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("087_workflow_chain_authority.sql")
    names = {item.object_name for item in objects}
    assert "workflow_chains" in names
    assert "workflow_chain_waves" in names
    assert "workflow_chain_wave_runs" in names
    assert "workflow_chain_waves_chain_ordinal_idx" in names
    assert "workflow_chain_waves_status_idx" in names
    assert "workflow_chain_wave_runs_chain_wave_ordinal_idx" in names
    assert "workflow_chain_wave_runs_run_idx" in names
    assert "workflow_chain_wave_runs_status_idx" in names


def test_workflow_chain_dependency_and_adoption_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("088_workflow_chain_dependency_and_adoption_authority.sql")
    names = {item.object_name for item in objects}
    assert "workflow_chain_wave_dependencies" in names
    assert "workflow_runs_workflow_id_adoption_key_requested_at_idx" in names
    assert "workflow_chain_wave_dependencies_depends_on_idx" in names


def test_control_operator_frame_uniqueness_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("091_control_operator_frame_uniqueness.sql")
    names = {item.object_name for item in objects}
    assert "run_operator_frames.run_operator_frames_state_check" in names
    assert "run_operator_frames.run_operator_frames_single_position_check" in names
    assert "run_operator_frames_run_node_item_unique_idx" in names
    assert "run_operator_frames_run_node_iteration_unique_idx" in names
    assert "run_operator_frames_run_open_started_idx" in names


def test_control_operator_frames_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("089_control_operator_frames.sql")
    names = {item.object_name for item in objects}
    assert "run_operator_frames" in names
    assert "run_operator_frames_run_node_idx" in names
    assert "run_operator_frames_run_state_idx" in names
    assert "run_operator_frames_run_item_idx" in names
    assert "run_operator_frames_run_iteration_idx" in names


def test_model_profile_auto_seed_trigger_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("095_model_profile_auto_seed_trigger.sql")
    names = {item.object_name for item in objects}
    types = {item.object_type for item in objects}
    assert "seed_model_profile_for_candidate" in names
    assert "function" in types


def test_workflow_run_packet_inspection_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("073_workflow_run_packet_inspection.sql")
    names = {item.object_name for item in objects}
    assert "workflow_runs.packet_inspection" in names
    assert "workflow_runs.workflow_runs_packet_inspection_object_check" in names


def test_provider_policy_multi_provider_refs_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("074_provider_policy_multi_provider_refs.sql")
    names = {item.object_name for item in objects}
    assert "provider_policies.allowed_provider_refs" in names
    assert "provider_policies.preferred_provider_ref" in names
    assert "provider_policies.provider_policies_allowed_provider_refs_array_check" in names


def test_workflow_chain_cancellation_and_alignment_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("090_workflow_chain_cancellation_and_alignment.sql")
    names = {item.object_name for item in objects}
    assert "workflow_chain_waves.workflow_chain_waves_status_v2_check" in names


def test_stale_postgres_completion_prune_migration_is_resolvable_and_targeted() -> None:
    path = workflow_migration_path("064_prune_stale_completion_functions.sql")
    assert path.name == "064_prune_stale_completion_functions.sql"

    statements = workflow_migration_statements("064_prune_stale_completion_functions.sql")
    assert len(statements) == 3
    assert statements[0].strip().endswith("BEGIN")
    assert statements[-1].strip() == "COMMIT"

    body = statements[1]
    assert "DROP TRIGGER IF EXISTS" in body
    assert "legacy_trigger constant text := 'trg_' || 'check_' || 'run_completion';" in body
    assert "legacy_event_fn constant text := 'check_' || 'run_completion' || '_with_events';" in body
    assert "legacy_base_fn constant text := 'check_' || 'run_completion';" in body
    assert "DROP FUNCTION IF EXISTS public.%I()" in body


def test_expected_index_names_match_postgres_identifier_limit() -> None:
    objects = workflow_migration_expected_objects("001_v1_control_plane.sql")
    indexes = {item.object_name for item in objects if item.object_type == "index"}
    assert (
        "capability_grants_subject_type_subject_id_capability_name_grant"
        in indexes
    )
    assert "workflow_definition_edges_workflow_definition_id_from_node_id_i" in indexes
    assert "workflow_definition_edges_workflow_definition_id_position_index" in indexes
    assert "workflow_definition_nodes_workflow_definition_id_position_index" in indexes


def test_legacy_dispatch_completion_symbol_is_absent_from_authority_paths() -> None:
    workflow_root = Path(__file__).resolve().parents[2]
    repo_root = workflow_root.parents[1]
    migration_root = repo_root / "Databases" / "migrations" / "workflow"
    forbidden_symbol = "check_" + "run_completion"

    authority_paths = [
        *sorted(migration_root.glob("*.sql")),
        workflow_root / "runtime" / "model_executor.py",
        workflow_root / "surfaces" / "mcp" / "tools" / "knowledge.py",
    ]

    offenders = [
        str(path)
        for path in authority_paths
        if forbidden_symbol in path.read_text(encoding="utf-8")
    ]

    assert offenders == []
