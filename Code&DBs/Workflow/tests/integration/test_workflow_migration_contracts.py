from __future__ import annotations

from pathlib import Path

from storage.migrations import (
    workflow_bootstrap_migration_path,
    workflow_bootstrap_migration_sql_text,
    workflow_bootstrap_migration_statements,
    workflow_migration_expected_objects,
    workflow_migration_manifest,
    workflow_migration_path,
    workflow_migration_sql_text,
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
    assert "046_provider_model_candidate_profiles.sql" in filenames
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
    assert "081_observability_lineage_and_metrics.sql" in filenames
    assert "087_workflow_chain_authority.sql" in filenames
    assert "088_workflow_chain_dependency_and_adoption_authority.sql" in filenames
    assert "089_control_operator_frames.sql" in filenames
    assert "090_workflow_chain_cancellation_and_alignment.sql" in filenames
    assert "091_control_operator_frame_uniqueness.sql" in filenames
    assert "095_model_profile_auto_seed_trigger.sql" in filenames
    assert "096_workflow_submission_acceptance.sql" in filenames
    assert "100_adapter_config_authority.sql" in filenames
    assert "106_acceptance_status_index.sql" in filenames
    assert "111_cursor_cli_provider_seed.sql" in filenames
    assert "112_sandbox_profile_authority.sql" in filenames
    assert "113_sandbox_cleanup_reconciliation.sql" in filenames
    assert "114_workflow_build_review_decisions.sql" in filenames
    assert "115_surface_catalog_registry.sql" in filenames
    assert "116_native_runtime_registry_authority.sql" in filenames
    assert "117_surface_catalog_review_decisions.sql" in filenames
    assert "118_surface_catalog_source_policy_registry.sql" in filenames
    assert "119_context_bundle_sandbox_profile_ref.sql" in filenames
    assert "120_workflow_build_review_proposal_requests.sql" in filenames
    assert "121_cursor_background_agent_api.sql" in filenames
    assert "122_workflow_build_review_contract_fields.sql" in filenames
    assert "123_workflow_build_planning_state_registry.sql" in filenames
    assert "124_operator_decision_scope_authority.sql" in filenames
    assert "125_cursor_local_cli_provider_seed.sql" in filenames
    assert "126_operator_decision_scope_policy.sql" in filenames
    assert "127_operator_decision_architecture_policies.sql" in filenames
    assert "128_operator_decision_embedding_architecture.sql" in filenames
    assert "132_issue_backlog_authority.sql" in filenames
    assert "135_claim_lifecycle_transition_authority.sql" in filenames
    assert "136_operation_catalog_authority.sql" in filenames
    assert "139_operation_catalog_operator_control_bindings.sql" in filenames
    assert "140_operation_catalog_surface_cleanup.sql" in filenames
    assert "141_operation_catalog_provider_onboarding.sql" in filenames
    assert "142_operation_catalog_operator_decision_bindings.sql" in filenames
    assert "143_object_field_registry_authority.sql" in filenames
    assert "144_object_field_registry_hard_cutover.sql" in filenames
    assert "145_operation_catalog_object_schema_bindings.sql" in filenames
    assert "146_semantic_assertion_substrate.sql" in filenames
    assert "147_operation_catalog_semantic_assertions.sql" in filenames
    assert "148_drop_workflow_notifications.sql" in filenames
    assert "149_native_self_hosted_smoke_definition.sql" in filenames
    assert "150_native_self_hosted_smoke_execution_identity.sql" in filenames
    assert "151_operation_catalog_operator_finish.sql" in filenames
    assert "152_operation_catalog_observability_finish.sql" in filenames
    assert "153_memory_edge_authority_contract.sql" in filenames
    assert "154_roadmap_lifecycle_authority.sql" in filenames
    assert "155_dataset_refinery_authority.sql" in filenames
    assert "156_dataset_refinery_projections.sql" in filenames
    assert "157_dataset_refinery_indexes.sql" in filenames
    assert "158_authority_memory_projection_vocabulary.sql" in filenames
    assert "159_provider_lane_policy.sql" in filenames
    assert "160_pgvector_roadmap_and_decisions.sql" in filenames
    assert "161_workflow_spec_ready.sql" in filenames
    assert "162_split_fanout_and_loop.sql" in filenames
    assert "163_dataset_candidate_score_history.sql" in filenames
    assert "164_dataset_promotion_decision_bridge.sql" in filenames
    assert "165_integration_registry_updated_at.sql" in filenames
    assert "166_data_dictionary_authority.sql" in filenames
    assert "167_scratch_agent_runtime_lane.sql" in filenames
    assert "168_openrouter_provider_authority_repair.sql" in filenames
    assert "220_archive_mobile_v1.sql" in filenames
    assert "222_runtime_setup_operation_catalog_repair.sql" in filenames
    assert "223_roadmap_lifecycle_retired.sql" in filenames
    assert filenames[-1] == "223_roadmap_lifecycle_retired.sql"


def test_every_manifest_migration_has_expected_object_contract() -> None:
    intentionally_empty_contracts = {
        "187_webauthn_challenges.sql",
        "188_mobile_sessions.sql",
    }
    missing_contracts: list[str] = []
    for entry in workflow_migration_manifest():
        try:
            objects = workflow_migration_expected_objects(entry.filename)
        except Exception:
            missing_contracts.append(entry.filename)
            continue
        if not objects and entry.filename not in intentionally_empty_contracts:
            missing_contracts.append(entry.filename)

    assert missing_contracts == []


def test_claim_lease_proposal_runtime_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("004_claim_lease_proposal_runtime.sql")
    names = {item.object_name for item in objects}
    assert "workflow_claim_lease_proposal_runtime" in names
    assert "sandbox_sessions" in names
    assert "sandbox_bindings" in names
    assert "workflow_claim_lease_proposal_runtime_sandbox_session_idx" in names
    assert "sandbox_sessions.sandbox_sessions_owner_route_ref_fkey" in names
    assert "workflow_claim_lease_proposal_runtime.workflow_claim_lease_proposal_runtime_sandbox_session_id_fkey" in names


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


def test_context_bundle_sandbox_profile_migration_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("119_context_bundle_sandbox_profile_ref.sql")
    names = {item.object_name for item in objects}
    assert "context_bundles.sandbox_profile_ref" in names
    assert "context_bundles.context_bundles_sandbox_profile_ref_nonblank" in names
    assert "context_bundles_sandbox_profile_idx" in names


def test_workflow_build_review_proposal_request_migration_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("120_workflow_build_review_proposal_requests.sql")
    names = {item.object_name for item in objects}
    assert "workflow_build_review_decisions.workflow_build_review_decisions_decision_check_v2" in names


def test_workflow_build_review_contract_field_migration_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("122_workflow_build_review_contract_fields.sql")
    names = {item.object_name for item in objects}
    assert "workflow_build_review_decisions.review_group_ref" in names
    assert "workflow_build_review_decisions.slot_ref" in names
    assert "workflow_build_review_decisions.authority_scope" in names
    assert "workflow_build_review_decisions.supersedes_decision_ref" in names
    assert "workflow_build_review_decisions.workflow_build_review_decisions_supersedes_decision_ref_fkey" in names
    assert "idx_workflow_build_review_decisions_group_target" in names


def test_workflow_build_planning_state_registry_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("123_workflow_build_planning_state_registry.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "review_policy_definitions",
            "review_policy_definitions_status_scope_idx",
            "capability_bundle_definitions",
            "capability_bundle_definitions_status_family_idx",
            "workflow_shape_family_definitions",
            "workflow_shape_family_definitions_status_policy_idx",
            "workflow_build_intents",
            "workflow_build_intents_workflow_definition_idx",
            "workflow_build_candidate_manifests",
            "workflow_build_candidate_manifests_workflow_definition_idx",
            "workflow_build_candidate_slots",
            "workflow_build_candidate_slots_manifest_kind_idx",
            "workflow_build_candidates",
            "workflow_build_candidates_manifest_slot_rank_idx",
            "workflow_build_review_sessions",
            "workflow_build_review_sessions_workflow_definition_idx",
            "workflow_build_execution_manifests",
            "workflow_build_execution_manifests_workflow_definition_idx",
        }
    )


def test_operator_decision_scope_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("124_operator_decision_scope_authority.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operator_decisions.decision_scope_kind",
            "operator_decisions.decision_scope_ref",
            "operator_decisions.operator_decisions_scope_pair",
            "operator_decisions_scope_decided_idx",
        }
    )


def test_cursor_local_cli_provider_seed_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("125_cursor_local_cli_provider_seed.sql")
    names = {item.object_name for item in objects}
    assert "provider_cli_profiles" in names
    assert "provider_transport_admissions" in names


def test_operation_catalog_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("136_operation_catalog_authority.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operation_catalog_registry",
            "operation_catalog_registry_source_enabled_idx",
            "operation_catalog_registry_method_path_idx",
            "operation_catalog_source_policy_registry",
            "operation_catalog_source_policy_registry_enabled_idx",
        }
    )


def test_operation_catalog_route_uniqueness_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("138_operation_catalog_route_uniqueness.sql")
    names = {item.object_name for item in objects}
    assert "operation_catalog_registry.operation_catalog_registry_method_path_unique" in names


def test_workflow_trigger_source_id_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("137_workflow_trigger_source_id.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "workflow_triggers.source_trigger_id",
            "idx_workflow_triggers_source_trigger_id",
        }
    )


def test_operation_catalog_operator_control_bindings_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects(
        "139_operation_catalog_operator_control_bindings.sql"
    )
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operation_catalog_registry.operator.roadmap_write",
            "operation_catalog_registry.operator.work_item_closeout",
        }
    )


def test_operation_catalog_surface_cleanup_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("140_operation_catalog_surface_cleanup.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operation_catalog_registry.operation_catalog_registry_source_kind_check",
            "operation_catalog_source_policy_registry.operation_catalog_source_policy_registry_source_kind_check",
            "operation_catalog_registry.operator.task_route_eligibility",
            "operation_catalog_registry.operator.native_primary_cutover_gate",
            "operation_catalog_registry.operator.transport_support",
        }
    )


def test_object_field_registry_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("143_object_field_registry_authority.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "object_field_registry",
        "idx_object_field_registry_active_type_order",
    }


def test_object_field_registry_hard_cutover_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("144_object_field_registry_hard_cutover.sql")
    names = {item.object_name for item in objects}
    assert names == {"object_field_registry.object_field_registry_field_name_nonblank"}


def test_operation_catalog_object_schema_bindings_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("145_operation_catalog_object_schema_bindings.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "operation_catalog_registry.object_schema.type_list",
        "operation_catalog_registry.object_schema.type_get",
        "operation_catalog_registry.object_schema.type_upsert",
        "operation_catalog_registry.object_schema.type_upsert_by_id",
        "operation_catalog_registry.object_schema.type_delete",
        "operation_catalog_registry.object_schema.field_list",
        "operation_catalog_registry.object_schema.field_upsert",
        "operation_catalog_registry.object_schema.field_retire",
    }


def test_semantic_assertion_substrate_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("146_semantic_assertion_substrate.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "semantic_predicates",
            "semantic_predicates_status_updated_idx",
            "semantic_assertions",
            "semantic_assertions_predicate_subject_idx",
            "semantic_current_assertions",
            "semantic_current_assertions_predicate_subject_idx",
        }
    )


def test_semantic_assertion_operation_catalog_expected_rows_are_registered() -> None:
    objects = workflow_migration_expected_objects("147_operation_catalog_semantic_assertions.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "operation_catalog_registry.semantic_assertions.register_predicate",
        "operation_catalog_registry.semantic_assertions.record",
        "operation_catalog_registry.semantic_assertions.retract",
        "operation_catalog_registry.semantic_assertions.list",
    }


def test_drop_workflow_notifications_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("148_drop_workflow_notifications.sql")
    names = {(item.object_type, item.object_name) for item in objects}
    assert names == {("absent_table", "workflow_notifications")}


def test_operation_catalog_operator_finish_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("151_operation_catalog_operator_finish.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "operation_catalog_registry.operator.architecture_policy_record",
        "operation_catalog_registry.operator.functional_area_record",
        "operation_catalog_registry.operator.object_relation_record",
        "operation_catalog_registry.operator.circuit_states",
        "operation_catalog_registry.operator.circuit_history",
        "operation_catalog_registry.operator.circuit_override",
    }


def test_operation_catalog_observability_finish_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("152_operation_catalog_observability_finish.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "operation_catalog_registry.operator.status_snapshot",
        "operation_catalog_registry.operator.issue_backlog",
        "operation_catalog_registry.operator.replay_ready_bugs",
        "operation_catalog_registry.operator.graph_projection",
        "operation_catalog_registry.operator.run_status",
        "operation_catalog_registry.operator.run_scoreboard",
        "operation_catalog_registry.operator.run_graph",
        "operation_catalog_registry.operator.run_lineage",
        "operation_catalog_registry.operator.metrics_reset",
        "operation_catalog_registry.operator.bug_replay_provenance_backfill",
        "operation_catalog_registry.operator.semantic_bridges_backfill",
        "operation_catalog_registry.operator.semantic_projection_refresh",
    }


def test_operator_decision_scope_policy_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("126_operator_decision_scope_policy.sql")
    names = {item.object_name for item in objects}
    assert "operator_decisions.operator_decisions_kind_scope_policy" in names


def test_operator_decision_architecture_policy_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("127_operator_decision_architecture_policies.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operator_decisions",
            "operator_decisions.operator_decisions_kind_scope_policy",
        }
    )


def test_operator_decision_embedding_architecture_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("128_operator_decision_embedding_architecture.sql")
    names = {item.object_name for item in objects}
    assert "operator_decisions" in names


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


def test_issue_backlog_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("132_issue_backlog_authority.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "issues",
            "issues_status_priority_opened_at_idx",
            "bugs.source_issue_id",
            "bugs.bugs_source_issue_fkey",
            "bugs_source_issue_uidx",
            "work_item_workflow_bindings.issue_id",
            "work_item_workflow_bindings.work_item_workflow_bindings_issue_fkey",
            "work_item_workflow_bindings_unique_edge",
        }
    )


def test_operator_ideas_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("195_operator_ideas_authority.sql")
    names = {item.object_name for item in objects}
    assert names.issuperset(
        {
            "operator_ideas",
            "operator_ideas_idea_key_key",
            "operator_ideas_status_check",
            "operator_ideas_resolution_window",
            "operator_ideas_terminal_resolution_summary",
            "operator_ideas_source_idx",
            "operator_idea_promotions",
            "operator_idea_promotions_idea_fkey",
            "operator_idea_promotions_roadmap_fkey",
            "operator_idea_promotions_unique_edge",
            "operator_idea_promotions_idea_idx",
            "operator_idea_promotions_roadmap_idx",
            "roadmap_items.source_idea_id",
            "roadmap_items_source_idea_fkey",
            "roadmap_items_source_idea_idx",
            "operation_catalog_registry.operator.ideas",
        }
    )


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


def test_surface_catalog_registry_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("115_surface_catalog_registry.sql")
    names = {item.object_name for item in objects}
    assert "surface_catalog_registry" in names
    assert "surface_catalog_registry_surface_enabled_order_idx" in names
    assert "surface_catalog_registry_surface_tier_idx" in names


def test_native_runtime_registry_authority_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("116_native_runtime_registry_authority.sql")
    names = {item.object_name for item in objects}
    assert "registry_native_runtime_profile_authority" in names
    assert "registry_native_runtime_defaults" in names


def test_scratch_agent_runtime_lane_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("167_scratch_agent_runtime_lane.sql")
    names = {item.object_name for item in objects}
    assert names == {
        "registry_workspace_authority.scratch_agent",
        "registry_sandbox_profile_authority.sandbox_profile.scratch_agent.default",
        "registry_runtime_profile_authority.scratch_agent",
        "registry_native_runtime_profile_authority.scratch_agent",
    }


def test_sandbox_cleanup_reconciliation_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects("113_sandbox_cleanup_reconciliation.sql")
    names = {item.object_name for item in objects}
    assert "sandbox_sessions.cleanup_status" in names
    assert "sandbox_sessions.cleanup_requested_at" in names
    assert "sandbox_sessions.cleanup_attempted_at" in names
    assert "sandbox_sessions.cleanup_completed_at" in names
    assert "sandbox_sessions.cleanup_attempt_count" in names
    assert "sandbox_sessions.cleanup_last_error" in names
    assert "sandbox_sessions.cleanup_outcome" in names
    assert "sandbox_sessions.ck_sandbox_sessions_cleanup_status" in names
    assert "sandbox_sessions_cleanup_due_idx" in names


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


def test_notify_and_provider_transport_migration_expected_objects_are_registered() -> None:
    notify_objects = workflow_migration_expected_objects("075_notify_system_events.sql")
    notify_names = {item.object_name for item in notify_objects}
    assert "notify_system_event_ready" in notify_names
    notify_sql = workflow_migration_sql_text("075_notify_system_events.sql")
    assert "to_regclass('public.system_events') IS NULL" in notify_sql

    profile_objects = workflow_migration_expected_objects("076_provider_cli_profile_transport_metadata.sql")
    profile_names = {item.object_name for item in profile_objects}
    assert "provider_cli_profiles.default_model" in profile_names
    assert "provider_cli_profiles.provider_cli_profiles_api_key_env_vars_array_check" in profile_names
    profile_sql = workflow_migration_sql_text("076_provider_cli_profile_transport_metadata.sql")
    assert "CREATE TABLE IF NOT EXISTS provider_cli_profiles" in profile_sql
    assert "'anthropic'" in profile_sql
    assert "'openai'" in profile_sql
    assert "'google'" in profile_sql

    prompt_objects = workflow_migration_expected_objects("077_provider_cli_profile_prompt_mode.sql")
    prompt_names = {item.object_name for item in prompt_objects}
    assert "provider_cli_profiles.prompt_mode" in prompt_names
    assert "provider_cli_profiles.provider_cli_profiles_prompt_mode_check" in prompt_names

    transport_objects = workflow_migration_expected_objects("078_provider_transport_admission_receipts.sql")
    transport_names = {item.object_name for item in transport_objects}
    assert "provider_transport_admissions" in transport_names
    assert "provider_transport_probe_receipts" in transport_names
    assert "provider_transport_probe_receipts_decision_idx" in transport_names

    cursor_seed_objects = workflow_migration_expected_objects("111_cursor_cli_provider_seed.sql")
    cursor_seed_names = {item.object_name for item in cursor_seed_objects}
    assert "provider_cli_profiles" in cursor_seed_names
    assert "provider_transport_admissions" in cursor_seed_names
    cursor_seed_sql = workflow_migration_sql_text("111_cursor_cli_provider_seed.sql")
    assert "'cursor'" in cursor_seed_sql
    assert "'cursor-agent'" in cursor_seed_sql
    assert "'provider_transport_admission.cursor.cli_llm'" in cursor_seed_sql

    cursor_api_sql = workflow_migration_sql_text("121_cursor_background_agent_api.sql")
    assert "'cursor_background_agent'" in cursor_api_sql
    assert "'provider_transport_admission.cursor.llm_task'" in cursor_api_sql
    assert "'candidate.cursor.auto'" in cursor_api_sql

    cursor_local_sql = workflow_migration_sql_text("125_cursor_local_cli_provider_seed.sql")
    assert "'cursor_local'" in cursor_local_sql
    assert "'cursor-agent'" in cursor_local_sql
    assert "'provider_transport_admission.cursor_local.cli_llm'" in cursor_local_sql


def test_openrouter_provider_authority_repair_expected_objects_are_registered() -> None:
    objects = workflow_migration_expected_objects(
        "168_openrouter_provider_authority_repair.sql"
    )
    names = {item.object_name for item in objects}

    assert names == {
        "provider_cli_profiles.openrouter",
        "provider_transport_admissions.provider_transport_admission.openrouter.llm_task",
        "provider_lane_policy.openrouter",
        "provider_model_candidates.candidate.openrouter.auto",
    }


def test_openrouter_provider_authority_repair_uses_openrouter_api_not_deepseek() -> None:
    sql_text = workflow_migration_sql_text(
        "168_openrouter_provider_authority_repair.sql"
    )

    assert "https://openrouter.ai/api/v1/chat/completions" in sql_text
    assert "OPENROUTER_API_KEY" in sql_text
    assert "provider_transport_admission.openrouter.llm_task" in sql_text
    assert "ARRAY['llm_task']" in sql_text
    assert "https://api.deepseek.com" not in sql_text
    assert "DEEPSEEK_API_KEY" not in sql_text


def test_stale_postgres_completion_prune_migration_is_resolvable_and_targeted() -> None:
    path = workflow_bootstrap_migration_path("064_prune_stale_completion_functions.sql")
    assert path.name == "064_prune_stale_completion_functions.sql"

    statements = workflow_bootstrap_migration_statements(
        "064_prune_stale_completion_functions.sql"
    )
    assert len(statements) == 3
    assert statements[0].strip().endswith("BEGIN")
    assert statements[-1].strip() == "COMMIT"

    body = statements[1]
    assert "DROP TRIGGER IF EXISTS" in body
    assert "legacy_trigger constant text := 'trg_' || 'check_' || 'run_completion';" in body
    assert "legacy_event_fn constant text := 'check_' || 'run_completion' || '_with_events';" in body
    assert "legacy_base_fn constant text := 'check_' || 'run_completion';" in body
    assert "DROP FUNCTION IF EXISTS public.%I()" in body


def test_reference_catalog_migrations_alias_json_array_values_explicitly() -> None:
    legacy_catalog_sql = workflow_bootstrap_migration_sql_text("034_reference_catalog.sql")
    assert "AS cap(value)" in legacy_catalog_sql
    assert "(cap.value->>'action')" in legacy_catalog_sql
    assert "AS prop(value)" in legacy_catalog_sql
    assert "(prop.value->>'name')" in legacy_catalog_sql

    refreshed_catalog_sql = workflow_bootstrap_migration_sql_text("037_reference_catalog.sql")
    assert "AS cap(value)" in refreshed_catalog_sql
    assert "(cap.value->>'action')" in refreshed_catalog_sql
    assert "ADD COLUMN IF NOT EXISTS schema_def" in refreshed_catalog_sql
    assert "ADD COLUMN IF NOT EXISTS examples" in refreshed_catalog_sql
    assert "ADD COLUMN IF NOT EXISTS updated_at" in refreshed_catalog_sql


def test_workflow_chain_dependency_migration_aliases_wave_definition_values_explicitly() -> None:
    sql_text = workflow_migration_sql_text(
        "088_workflow_chain_dependency_and_adoption_authority.sql"
    )
    assert "AS wave_definition(value)" in sql_text
    assert "wave_definition.value->'depends_on'" in sql_text
    assert "wave_definition.value->>'wave_id'" in sql_text


def test_workflow_runtime_cutover_migration_backfills_missing_dispatch_idempotency_column() -> None:
    sql_text = workflow_bootstrap_migration_sql_text("041_workflow_runtime_cutover.sql")
    assert "ALTER TABLE dispatch_runs" in sql_text
    assert "ADD COLUMN IF NOT EXISTS idempotency_key TEXT" in sql_text


def test_provider_model_candidate_profiles_migration_seeds_cli_configured_candidates() -> None:
    sql_text = workflow_migration_sql_text("046_provider_model_candidate_profiles.sql")
    assert "ADD COLUMN IF NOT EXISTS cli_config jsonb" in sql_text
    assert "INSERT INTO provider_model_candidates" in sql_text
    assert "\"cmd_template\":[\"claude\"" in sql_text
    assert "\"cmd_template\":[\"codex\"" in sql_text
    assert "\"cmd_template\":[\"gemini\"" in sql_text


def test_google_gemini_mcp_settings_migration_sets_project_scoped_mcp_authority() -> None:
    sql_text = workflow_migration_sql_text("129_google_gemini_mcp_settings_authority.sql")
    assert "UPDATE provider_cli_profiles" in sql_text
    assert "gemini_project_settings" in sql_text
    assert "--allowed-mcp-server-names" in sql_text
    assert "WHERE provider_slug = 'google'" in sql_text


def test_bugs_resume_context_migration_adds_handoff_column() -> None:
    sql_text = workflow_migration_sql_text("130_bugs_resume_context.sql")
    assert "ALTER TABLE bugs" in sql_text
    assert "resume_context" in sql_text
    assert "jsonb" in sql_text


def test_roadmap_items_semantic_clustering_migration_adds_embedding_lane() -> None:
    sql_text = workflow_migration_sql_text("131_roadmap_items_semantic_clustering.sql")
    assert "ALTER TABLE roadmap_items" in sql_text
    assert "embedding" in sql_text
    assert "vector(384)" in sql_text
    assert "roadmap_items_hnsw_idx" in sql_text


def test_observability_lineage_migration_guards_optional_tables_before_indexing() -> None:
    sql_text = workflow_migration_sql_text("081_observability_lineage_and_metrics.sql")
    assert "ALTER TABLE IF EXISTS system_events" in sql_text
    assert "CREATE TABLE IF NOT EXISTS workflow_metrics" in sql_text
    assert "platform_events" not in sql_text
    assert "system_events_parent_run_idx" in sql_text
    assert "workflow_metrics_parent_idx" in sql_text


def test_acceptance_status_index_migration_avoids_concurrent_builds_in_bootstrap() -> None:
    sql_text = workflow_migration_sql_text("106_acceptance_status_index.sql")
    assert "CREATE INDEX IF NOT EXISTS idx_wjs_acceptance_status" in sql_text
    assert "CREATE INDEX CONCURRENTLY" not in sql_text


def test_adapter_config_authority_migration_creates_platform_config_defaults() -> None:
    sql_text = workflow_migration_sql_text("100_adapter_config_authority.sql")
    assert "CREATE TABLE IF NOT EXISTS platform_config" in sql_text
    assert "'breaker.failure_threshold'" in sql_text
    assert "'breaker.recovery_timeout_s'" in sql_text
    assert "'health.max_consecutive_failures'" in sql_text
    assert "'context.preview_chars'" in sql_text


def test_sandbox_cleanup_reconciliation_migration_guards_optional_maintenance_policy_seed() -> None:
    sql_text = workflow_migration_sql_text("113_sandbox_cleanup_reconciliation.sql")
    assert "to_regclass('public.maintenance_policies') IS NULL" in sql_text


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
