"""Canonical workflow migration path helpers.

This module is intentionally tiny so other packages can resolve migration files
without importing the heavier Postgres storage surface and creating circular
dependencies.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_POSTGRES_IDENTIFIER_MAX_CHARS = 63


class WorkflowMigrationError(RuntimeError):
    """Raised when the canonical workflow migration tree is missing or invalid."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        path: Path | None = None,
        filename: str | None = None,
        details: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.path = path
        self.filename = filename
        self._details = dict(details or {})

    @property
    def details(self) -> dict[str, str]:
        details = dict(self._details)
        if self.path is not None:
            details["path"] = str(self.path)
        if self.filename is not None:
            details["filename"] = self.filename
        return details


WorkflowMigrationPathError = WorkflowMigrationError

_WORKFLOW_MIGRATION_SEQUENCE = (
    "001_v1_control_plane.sql",
    "002_registry_authority.sql",
    "003_gate_and_promotion_policy.sql",
    "004_claim_lease_proposal_runtime.sql",
    "005_workflow_outbox.sql",
    "006_platform_authority_schema.sql",
    "007_provider_route_health_budget.sql",
    "008_workflow_class_and_schedule_schema.sql",
    "009_bug_and_roadmap_authority.sql",
    "010_operator_control_authority.sql",
    "011_runtime_breadth_authority.sql",
    "040_control_commands.sql",
    "042_workflow_control_command_types.sql",
    "050_verification_registry.sql",
    "057_compile_spine_authority.sql",
    "069_compile_index_snapshots.sql",
    "065_execution_packet_authority.sql",
    "070_compile_artifact_reuse_keys.sql",
    "071_repo_snapshots_runtime_breadth_repair.sql",
    "072_verifier_healer_authority.sql",
    "073_workflow_run_packet_inspection.sql",
    "074_provider_policy_multi_provider_refs.sql",
    "075_notify_system_events.sql",
    "076_provider_cli_profile_transport_metadata.sql",
    "077_provider_cli_profile_prompt_mode.sql",
    "078_provider_transport_admission_receipts.sql",
    "079_workflow_job_runtime_context.sql",
    "080_workflow_job_submissions.sql",
    "087_workflow_chain_authority.sql",
    "088_workflow_chain_dependency_and_adoption_authority.sql",
    "089_control_operator_frames.sql",
    "090_workflow_chain_cancellation_and_alignment.sql",
    "091_control_operator_frame_uniqueness.sql",
    "095_model_profile_auto_seed_trigger.sql",
    "096_workflow_submission_acceptance.sql",
)


@dataclass(frozen=True, slots=True)
class WorkflowMigrationExpectedObject:
    """One canonical object that a workflow migration must materialize."""

    object_type: str
    object_name: str


@dataclass(frozen=True, slots=True)
class WorkflowMigrationManifestEntry:
    """One canonical workflow migration entry."""

    sequence_no: int
    filename: str
    path: Path


def _expected_objects(
    *,
    tables: tuple[str, ...] = (),
    indexes: tuple[str, ...] = (),
    columns: tuple[str, ...] = (),
    constraints: tuple[str, ...] = (),
    functions: tuple[str, ...] = (),
) -> tuple[WorkflowMigrationExpectedObject, ...]:
    return tuple(
        WorkflowMigrationExpectedObject(object_type="table", object_name=name)
        for name in tables
    ) + tuple(
        WorkflowMigrationExpectedObject(
            object_type="index",
            object_name=name[:_POSTGRES_IDENTIFIER_MAX_CHARS],
        )
        for name in indexes
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="column", object_name=name)
        for name in columns
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="constraint", object_name=name)
        for name in constraints
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="function", object_name=name)
        for name in functions
    )


_WORKFLOW_MIGRATION_EXPECTED_OBJECTS = {
    "001_v1_control_plane.sql": _expected_objects(
        tables=(
            "workflow_definitions",
            "workflow_definition_nodes",
            "workflow_definition_edges",
            "admission_decisions",
            "workflow_runs",
            "run_nodes",
            "run_edges",
            "capability_grants",
            "workflow_events",
            "receipts",
            "promotion_decisions",
            "model_profiles",
            "provider_policies",
        ),
        indexes=(
            "workflow_definitions_status_created_at_idx",
            "workflow_definition_nodes_workflow_definition_id_node_type_idx",
            "workflow_definition_nodes_workflow_definition_id_position_index_idx",
            "workflow_definition_edges_workflow_definition_id_from_node_id_idx",
            "workflow_definition_edges_workflow_definition_id_to_node_id_idx",
            "workflow_definition_edges_workflow_definition_id_position_index_idx",
            "admission_decisions_workflow_id_decided_at_idx",
            "admission_decisions_request_id_idx",
            "admission_decisions_decision_decided_at_idx",
            "workflow_runs_workflow_id_requested_at_idx",
            "workflow_runs_current_state_requested_at_idx",
            "workflow_runs_context_bundle_id_idx",
            "run_nodes_workflow_definition_node_id_idx",
            "run_nodes_run_id_current_state_idx",
            "run_nodes_run_id_node_type_idx",
            "run_nodes_receipt_id_idx",
            "run_edges_workflow_definition_edge_id_idx",
            "run_edges_run_id_from_node_id_idx",
            "run_edges_run_id_to_node_id_idx",
            "run_edges_run_id_release_state_idx",
            "capability_grants_subject_type_subject_id_capability_name_grant_state_idx",
            "capability_grants_run_id_capability_name_idx",
            "capability_grants_expires_at_idx",
            "workflow_events_run_id_evidence_seq_idx",
            "workflow_events_workflow_id_occurred_at_evidence_seq_idx",
            "workflow_events_node_id_evidence_seq_idx",
            "receipts_run_id_evidence_seq_idx",
            "receipts_workflow_id_started_at_idx",
            "receipts_receipt_type_started_at_idx",
            "promotion_decisions_decision_decided_at_idx",
            "promotion_decisions_workflow_id_decided_at_idx",
            "model_profiles_profile_name_status_idx",
            "model_profiles_provider_name_model_name_idx",
            "model_profiles_effective_from_idx",
            "provider_policies_scope_provider_name_status_idx",
            "provider_policies_provider_name_effective_from_idx",
            "provider_policies_decision_ref_idx",
        ),
    ),
    "005_workflow_outbox.sql": _expected_objects(
        tables=(
            "workflow_outbox",
        ),
        indexes=(
            "workflow_outbox_workflow_id_run_id_evidence_seq_idx",
            "workflow_outbox_envelope_kind_run_id_evidence_seq_idx",
        ),
    ),
    "006_platform_authority_schema.sql": _expected_objects(
        tables=(
            "context_bundles",
            "context_bundle_anchors",
            "provider_model_candidates",
            "model_profile_candidate_bindings",
            "event_subscriptions",
            "subscription_checkpoints",
            "workflow_lanes",
            "workflow_lane_policies",
        ),
        indexes=(
            "context_bundles_workspace_runtime_idx",
            "context_bundles_bundle_hash_idx",
            "context_bundle_anchors_bundle_idx",
            "context_bundle_anchors_ref_idx",
            "provider_model_candidates_provider_ref_status_idx",
            "provider_model_candidates_slug_idx",
            "provider_model_candidates_decision_ref_idx",
            "model_profile_candidate_bindings_profile_idx",
            "model_profile_candidate_bindings_candidate_idx",
            "event_subscriptions_status_consumer_idx",
            "event_subscriptions_workflow_run_idx",
            "subscription_checkpoints_subscription_idx",
            "subscription_checkpoints_run_idx",
            "workflow_lanes_name_status_idx",
            "workflow_lanes_kind_effective_idx",
            "workflow_lane_policies_lane_idx",
            "workflow_lane_policies_scope_kind_idx",
            "workflow_lane_policies_decision_ref_idx",
        ),
    ),
    "007_provider_route_health_budget.sql": _expected_objects(
        tables=(
            "provider_route_health_windows",
            "provider_budget_windows",
            "route_eligibility_states",
        ),
        indexes=(
            "provider_route_health_windows_provider_status_idx",
            "provider_route_health_windows_candidate_window_idx",
            "provider_budget_windows_provider_scope_status_idx",
            "provider_budget_windows_policy_window_idx",
            "route_eligibility_states_profile_candidate_status_idx",
            "route_eligibility_states_decision_ref_idx",
        ),
    ),
    "008_workflow_class_and_schedule_schema.sql": _expected_objects(
        tables=(
            "workflow_classes",
            "schedule_definitions",
            "recurring_run_windows",
        ),
        indexes=(
            "workflow_classes_name_status_idx",
            "workflow_classes_kind_lane_idx",
            "schedule_definitions_workflow_class_status_idx",
            "schedule_definitions_target_kind_idx",
            "recurring_run_windows_schedule_status_idx",
            "recurring_run_windows_window_status_idx",
        ),
    ),
    "009_bug_and_roadmap_authority.sql": _expected_objects(
        tables=(
            "bugs",
            "bug_evidence_links",
            "roadmap_items",
            "roadmap_item_dependencies",
        ),
        indexes=(
            "bugs_status_severity_opened_at_idx",
            "bugs_discovered_in_run_idx",
            "bugs_discovered_in_receipt_idx",
            "bug_evidence_links_bug_created_at_idx",
            "bug_evidence_links_kind_ref_idx",
            "roadmap_items_status_priority_target_end_idx",
            "roadmap_items_parent_idx",
            "roadmap_items_source_bug_idx",
            "roadmap_item_dependencies_item_idx",
            "roadmap_item_dependencies_depends_on_idx",
        ),
    ),
    "010_operator_control_authority.sql": _expected_objects(
        tables=(
            "operator_decisions",
            "cutover_gates",
            "work_item_workflow_bindings",
        ),
        indexes=(
            "operator_decisions_kind_status_decided_idx",
            "operator_decisions_source_effective_idx",
            "cutover_gates_status_kind_opened_idx",
            "cutover_gates_roadmap_idx",
            "cutover_gates_workflow_class_idx",
            "cutover_gates_schedule_definition_idx",
            "work_item_workflow_bindings_status_kind_idx",
            "work_item_workflow_bindings_roadmap_idx",
            "work_item_workflow_bindings_bug_idx",
            "work_item_workflow_bindings_cutover_gate_idx",
            "work_item_workflow_bindings_workflow_class_idx",
            "work_item_workflow_bindings_workflow_run_idx",
        ),
    ),
    "011_runtime_breadth_authority.sql": _expected_objects(
        tables=(
            "provider_failover_bindings",
            "provider_endpoint_bindings",
            "persona_profiles",
            "persona_context_bindings",
            "fork_profiles",
            "fork_worktree_bindings",
        ),
        indexes=(
            "provider_failover_bindings_scope_idx",
            "provider_failover_bindings_candidate_idx",
            "provider_failover_bindings_decision_ref_idx",
            "provider_endpoint_bindings_policy_status_idx",
            "provider_endpoint_bindings_candidate_endpoint_idx",
            "provider_endpoint_bindings_decision_ref_idx",
            "persona_profiles_name_status_idx",
            "persona_profiles_kind_effective_idx",
            "persona_profiles_decision_ref_idx",
            "persona_context_bindings_profile_idx",
            "persona_context_bindings_context_idx",
            "persona_context_bindings_model_policy_idx",
            "fork_profiles_name_status_idx",
            "fork_profiles_kind_effective_idx",
            "fork_profiles_decision_ref_idx",
            "fork_worktree_bindings_profile_status_idx",
            "fork_worktree_bindings_sandbox_idx",
            "fork_worktree_bindings_worktree_idx",
        ),
    ),
    "050_verification_registry.sql": _expected_objects(
        tables=(
            "verification_registry",
        ),
    ),
    "057_compile_spine_authority.sql": _expected_objects(
        tables=(
            "compile_artifacts",
            "capability_catalog",
            "verify_refs",
        ),
        indexes=(
            "compile_artifacts_kind_revision_idx",
            "compile_artifacts_content_hash_idx",
            "capability_catalog_kind_enabled_idx",
            "capability_catalog_route_idx",
            "verify_refs_verification_enabled_idx",
        ),
    ),
    "069_compile_index_snapshots.sql": _expected_objects(
        tables=(
            "compile_index_snapshots",
        ),
        indexes=(
            "compile_index_snapshots_surface_name_refreshed_idx",
            "compile_index_snapshots_surface_name_revision_idx",
            "compile_index_snapshots_repo_fingerprint_idx",
            "compile_index_snapshots_stale_after_idx",
        ),
    ),
    "065_execution_packet_authority.sql": _expected_objects(
        tables=(
            "execution_packets",
        ),
        indexes=(
            "execution_packets_definition_plan_idx",
            "execution_packets_run_idx",
            "execution_packets_packet_hash_idx",
        ),
    ),
    "070_compile_artifact_reuse_keys.sql": _expected_objects(
        indexes=(
            "compile_artifacts_kind_input_fingerprint_idx",
        ),
    ),
    "071_repo_snapshots_runtime_breadth_repair.sql": _expected_objects(
        tables=(
            "repo_snapshots",
        ),
        indexes=(
            "repo_snapshots_repo_fingerprint_idx",
            "repo_snapshots_workspace_runtime_idx",
        ),
    ),
    "072_verifier_healer_authority.sql": _expected_objects(
        tables=(
            "verifier_registry",
            "healer_registry",
            "verifier_healer_bindings",
            "verification_runs",
            "healing_runs",
        ),
        indexes=(
            "verifier_registry_kind_enabled_idx",
            "verifier_registry_verification_ref_idx",
            "healer_registry_kind_enabled_idx",
            "healer_registry_auto_mode_idx",
            "verifier_healer_bindings_verifier_enabled_idx",
            "verifier_healer_bindings_healer_enabled_idx",
            "verification_runs_verifier_attempted_idx",
            "verification_runs_target_status_idx",
            "healing_runs_healer_attempted_idx",
            "healing_runs_verifier_status_idx",
        ),
    ),
    "073_workflow_run_packet_inspection.sql": _expected_objects(
        columns=(
            "workflow_runs.packet_inspection",
        ),
        constraints=(
            "workflow_runs.workflow_runs_packet_inspection_object_check",
        ),
    ),
    "074_provider_policy_multi_provider_refs.sql": _expected_objects(
        columns=(
            "provider_policies.allowed_provider_refs",
            "provider_policies.preferred_provider_ref",
        ),
        constraints=(
            "provider_policies.provider_policies_allowed_provider_refs_array_check",
        ),
    ),
    "079_workflow_job_runtime_context.sql": _expected_objects(
        tables=(
            "workflow_job_runtime_context",
        ),
        indexes=(
            "idx_workflow_job_runtime_context_workflow",
        ),
    ),
    "080_workflow_job_submissions.sql": _expected_objects(
        tables=(
            "workflow_job_submissions",
            "workflow_job_submission_reviews",
        ),
        indexes=(
            "workflow_job_submissions_run_job_attempt_key",
            "workflow_job_submission_reviews_submission_reviewed_idx",
        ),
    ),
    "096_workflow_submission_acceptance.sql": _expected_objects(
        columns=(
            "workflow_job_submissions.acceptance_status",
            "workflow_job_submissions.acceptance_report",
        ),
    ),
    "012_task_type_route_eligibility.sql": _expected_objects(
        tables=(
            "task_type_route_eligibility",
        ),
        indexes=(
            "task_type_route_eligibility_provider_window_idx",
            "task_type_route_eligibility_scope_idx",
            "task_type_route_eligibility_decision_ref_idx",
        ),
    ),
    "040_control_commands.sql": _expected_objects(
        tables=(
            "control_commands",
        ),
        indexes=(
            "idx_control_commands_status_requested_at",
            "idx_control_commands_type_requested_at",
            "uq_control_commands_idempotency_key",
            "idx_control_commands_result_ref",
        ),
    ),
    "087_workflow_chain_authority.sql": _expected_objects(
        tables=(
            "workflow_chains",
            "workflow_chain_waves",
            "workflow_chain_wave_runs",
        ),
        indexes=(
            "workflow_chain_waves_chain_ordinal_idx",
            "workflow_chain_waves_status_idx",
            "workflow_chain_wave_runs_chain_wave_ordinal_idx",
            "workflow_chain_wave_runs_run_idx",
            "workflow_chain_wave_runs_status_idx",
        ),
    ),
    "088_workflow_chain_dependency_and_adoption_authority.sql": _expected_objects(
        tables=(
            "workflow_chain_wave_dependencies",
        ),
        indexes=(
            "workflow_runs_workflow_id_adoption_key_requested_at_idx",
            "workflow_chain_wave_dependencies_depends_on_idx",
        ),
    ),
    "089_control_operator_frames.sql": _expected_objects(
        tables=(
            "run_operator_frames",
        ),
        indexes=(
            "run_operator_frames_run_node_idx",
            "run_operator_frames_run_state_idx",
            "run_operator_frames_run_item_idx",
            "run_operator_frames_run_iteration_idx",
        ),
    ),
    "090_workflow_chain_cancellation_and_alignment.sql": _expected_objects(
        constraints=(
            "workflow_chain_waves.workflow_chain_waves_status_v2_check",
        ),
    ),
    "091_control_operator_frame_uniqueness.sql": _expected_objects(
        constraints=(
            "run_operator_frames.run_operator_frames_state_check",
            "run_operator_frames.run_operator_frames_single_position_check",
        ),
        indexes=(
            "run_operator_frames_run_node_item_unique_idx",
            "run_operator_frames_run_node_iteration_unique_idx",
            "run_operator_frames_run_open_started_idx",
        ),
    ),
    "095_model_profile_auto_seed_trigger.sql": _expected_objects(
        functions=(
            "seed_model_profile_for_candidate",
        ),
    ),
}


def _workflow_migrations_root_path() -> Path:
    return Path(__file__).resolve().parents[2] / "Databases" / "migrations" / "workflow"


@lru_cache(maxsize=1)
def workflow_migrations_root() -> Path:
    """Return the one canonical workflow migration root."""

    root = _workflow_migrations_root_path()
    if not root.is_dir():
        raise WorkflowMigrationPathError(
            "workflow.migration_root_missing",
            "canonical workflow migration root is missing",
            path=root,
        )
    return root


@lru_cache(maxsize=1)
def workflow_migration_manifest() -> tuple[WorkflowMigrationManifestEntry, ...]:
    """Return the exact canonical migration sequence and fail closed on drift."""

    root = workflow_migrations_root()
    actual_filenames = {path.name for path in root.glob("*.sql")}
    expected_filenames = set(_WORKFLOW_MIGRATION_SEQUENCE)

    missing_filenames = tuple(
        filename for filename in _WORKFLOW_MIGRATION_SEQUENCE if filename not in actual_filenames
    )
    if missing_filenames:
        raise WorkflowMigrationPathError(
            "workflow.migration_manifest_incomplete",
            "canonical workflow migration manifest is incomplete",
            path=root,
            filename=missing_filenames[0],
            details={
                "missing_filenames": ",".join(missing_filenames),
                "expected_filenames": ",".join(_WORKFLOW_MIGRATION_SEQUENCE),
            },
        )

    return tuple(
        WorkflowMigrationManifestEntry(
            sequence_no=index,
            filename=filename,
            path=root / filename,
        )
        for index, filename in enumerate(_WORKFLOW_MIGRATION_SEQUENCE, start=1)
    )


def workflow_migration_path(filename: str) -> Path:
    """Resolve one canonical workflow migration file and fail closed if missing."""

    for entry in workflow_migration_manifest():
        if entry.filename == filename:
            return entry.path

    root = workflow_migrations_root()
    candidate = root / filename
    if candidate.is_file() and filename.endswith(".sql") and filename[:3].isdigit():
        return candidate

    raise WorkflowMigrationPathError(
        "workflow.migration_unknown",
        "workflow migration filename is not in the canonical manifest",
        path=candidate,
        filename=filename,
        details={"expected_filenames": ",".join(_WORKFLOW_MIGRATION_SEQUENCE)},
    )


@lru_cache(maxsize=64)
def workflow_migration_sql_text(filename: str) -> str:
    """Load one canonical workflow migration file."""

    path = workflow_migration_path(filename)
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        workflow_migration_manifest.cache_clear()
        try:
            workflow_migration_path(filename)
        except WorkflowMigrationPathError as manifest_exc:
            raise manifest_exc from exc
        raise WorkflowMigrationPathError(
            "workflow.migration_read_failed",
            "canonical workflow migration file could not be read",
            path=path,
            filename=filename,
        ) from exc
    except OSError as exc:  # pragma: no cover - defensive failure path
        raise WorkflowMigrationPathError(
            "workflow.migration_read_failed",
            "canonical workflow migration file could not be read",
            path=path,
            filename=filename,
        ) from exc


@lru_cache(maxsize=64)
def workflow_migration_statements(filename: str) -> tuple[str, ...]:
    """Load canonical workflow migration statements using one shared parser."""

    sql_text = workflow_migration_sql_text(filename)
    path = workflow_migration_path(filename)
    statements = _split_sql_statements(sql_text)
    if not statements:
        raise WorkflowMigrationPathError(
            "workflow.migration_empty",
            "canonical workflow migration file did not contain executable statements",
            path=path,
            filename=filename,
        )
    return statements


def _split_sql_statements(sql_text: str) -> tuple[str, ...]:
    """Split SQL text on statement terminators outside quoted bodies.

    The canonical workflow migrations include PL/pgSQL functions, so a plain
    ``split(';')`` corrupts function bodies and makes schema bootstrap lie
    about migration failures. Keep this parser local and boring: it only needs
    to respect line comments, block comments, quoted identifiers/strings, and
    dollar-quoted bodies.
    """

    statements: list[str] = []
    current: list[str] = []
    index = 0
    in_single_quote = False
    in_double_quote = False
    line_comment = False
    block_comment_depth = 0
    dollar_tag: str | None = None

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if line_comment:
            current.append(char)
            index += 1
            if char == "\n":
                line_comment = False
            continue

        if block_comment_depth:
            current.append(char)
            if char == "/" and next_char == "*":
                current.append(next_char)
                block_comment_depth += 1
                index += 2
                continue
            if char == "*" and next_char == "/":
                current.append(next_char)
                block_comment_depth -= 1
                index += 2
                continue
            index += 1
            continue

        if dollar_tag is not None:
            if sql_text.startswith(dollar_tag, index):
                current.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(char)
            index += 1
            continue

        if in_single_quote:
            current.append(char)
            index += 1
            if char == "'" and next_char == "'":
                current.append(next_char)
                index += 1
                continue
            if char == "'":
                in_single_quote = False
            continue

        if in_double_quote:
            current.append(char)
            index += 1
            if char == '"' and next_char == '"':
                current.append(next_char)
                index += 1
                continue
            if char == '"':
                in_double_quote = False
            continue

        if char == "-" and next_char == "-":
            current.extend((char, next_char))
            line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            current.extend((char, next_char))
            block_comment_depth = 1
            index += 2
            continue

        if char == "'":
            current.append(char)
            in_single_quote = True
            index += 1
            continue

        if char == '"':
            current.append(char)
            in_double_quote = True
            index += 1
            continue

        if char == "$":
            tag_end = index + 1
            while tag_end < len(sql_text) and (
                sql_text[tag_end].isalnum() or sql_text[tag_end] == "_"
            ):
                tag_end += 1
            if tag_end < len(sql_text) and sql_text[tag_end] == "$":
                dollar_tag = sql_text[index : tag_end + 1]
                current.append(dollar_tag)
                index = tag_end + 1
                continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return tuple(statements)


@lru_cache(maxsize=64)
def workflow_migration_expected_objects(
    filename: str,
) -> tuple[WorkflowMigrationExpectedObject, ...]:
    """Return the explicit expected-object contract for one canonical migration."""

    path = workflow_migration_path(filename)
    objects = _WORKFLOW_MIGRATION_EXPECTED_OBJECTS.get(filename)
    if objects is None:
        raise WorkflowMigrationPathError(
            "workflow.migration_expected_objects_missing",
            "canonical workflow migration is missing an expected-object contract",
            path=path,
            filename=filename,
        )
    if not objects:
        raise WorkflowMigrationPathError(
            "workflow.migration_expected_objects_empty",
            "canonical workflow migration expected-object contract is empty",
            path=path,
            filename=filename,
        )
    return objects


def clear_workflow_migration_caches() -> None:
    """Reset cached canonical migration lookups for tests and patched call sites."""

    workflow_migrations_root.cache_clear()
    workflow_migration_manifest.cache_clear()
    workflow_migration_sql_text.cache_clear()
    workflow_migration_statements.cache_clear()
    workflow_migration_expected_objects.cache_clear()


__all__ = [
    "clear_workflow_migration_caches",
    "WorkflowMigrationError",
    "WorkflowMigrationExpectedObject",
    "WorkflowMigrationManifestEntry",
    "WorkflowMigrationPathError",
    "workflow_migration_expected_objects",
    "workflow_migration_manifest",
    "workflow_migration_path",
    "workflow_migration_sql_text",
    "workflow_migration_statements",
    "workflow_migrations_root",
]
