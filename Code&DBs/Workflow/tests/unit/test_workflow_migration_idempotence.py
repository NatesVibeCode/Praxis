from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_WORKFLOW_MIGRATIONS = _REPO_ROOT / "Code&DBs" / "Databases" / "migrations" / "workflow"


def _migration_sql(filename: str) -> str:
    return (_WORKFLOW_MIGRATIONS / filename).read_text(encoding="utf-8")


def test_base_provider_candidate_schema_declares_current_profile_contract() -> None:
    sql = _migration_sql("006_platform_authority_schema.sql")

    required_columns = (
        "cli_config",
        "route_tier",
        "route_tier_rank",
        "latency_class",
        "latency_rank",
        "reasoning_control",
        "task_affinities",
        "benchmark_profile",
    )

    for column in required_columns:
        assert column in sql

    assert "provider_model_candidates_route_tier_check" in sql
    assert "provider_model_candidates_latency_class_check" in sql
    assert "provider_model_candidates_reasoning_control_object_check" in sql


def test_base_task_type_routing_schema_declares_current_route_contract() -> None:
    sql = _migration_sql("024_task_type_routing.sql")

    required_columns = (
        "sub_task_type",
        "transport_type",
        "route_tier",
        "route_tier_rank",
        "latency_class",
        "latency_rank",
        "reasoning_control",
        "route_health_score",
        "observed_completed_count",
        "observed_execution_failure_count",
        "observed_external_failure_count",
        "observed_config_failure_count",
        "observed_downstream_failure_count",
        "observed_downstream_bug_count",
        "consecutive_internal_failures",
        "recent_successes",
        "recent_failures",
        "route_source",
        "temperature",
        "max_tokens",
    )

    for column in required_columns:
        assert column in sql

    assert "PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type)" in sql
    assert "ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)" in sql


def test_vector_migrations_do_not_create_privileged_extension() -> None:
    for filename in (
        "018_module_embeddings.sql",
        "208_structured_document_semantic_authority.sql",
    ):
        sql = _migration_sql(filename)
        assert "CREATE EXTENSION IF NOT EXISTS vector" not in sql
        assert "bootstrap/onboarding platform gate" in sql


def test_route_scoring_migration_preserves_current_routing_primary_key() -> None:
    sql = _migration_sql("055_task_route_scoring_authority.sql")

    assert "PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type)" in sql
    assert "PRIMARY KEY (task_type, provider_slug, model_slug)" not in sql


def test_route_seed_migrations_use_current_conflict_target() -> None:
    stale_targets = (
        "ON CONFLICT (task_type, model_slug, provider_slug)",
        "ON CONFLICT (task_type, provider_slug, model_slug)",
    )
    current_target = (
        "ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)"
    )

    for path in _WORKFLOW_MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"):
        sql = path.read_text(encoding="utf-8")
        for stale_target in stale_targets:
            assert stale_target not in sql, f"{path.name} uses stale task_type_routing identity"
        if "INSERT INTO task_type_routing" in sql and "ON CONFLICT (task_type" in sql:
            assert current_target in sql


def test_base_authority_object_registry_allows_query_kind() -> None:
    sql = _migration_sql("202_cqrs_authority_object_registry.sql")

    assert "'query'" in sql
    assert "'operation.' || operation_name,\n    operation_kind," in sql


def test_workflow_plumbing_dictionary_constraint_preserves_query_category() -> None:
    sql = _migration_sql("247_workflow_plumbing_data_dictionary_objects.sql")

    assert "'query'" in sql
    assert sql.index("'command'") < sql.index("'query'") < sql.index("'event'")


def test_query_kind_migration_skips_constraint_alter_when_already_current() -> None:
    sql = _migration_sql("279_authority_object_registry_query_kind.sql")

    assert "constraint_body NOT LIKE '%query%'" in sql
    assert sql.index("constraint_body NOT LIKE '%query%'") < sql.index(
        "ALTER TABLE authority_object_registry"
    )
    assert "FROM operation_catalog_registry" in sql
    assert "'operation.' || operation_name,\n    operation_kind," in sql
    assert "object_kind = EXCLUDED.object_kind" in sql


def test_bug_triage_packet_operation_is_registered_as_query() -> None:
    sql = _migration_sql("279_register_bug_triage_packet_operation.sql")

    assert "'operation.operator.bug_triage_packet',\n    'query'," in sql
    assert "'operation.operator.bug_triage_packet',\n    'command'," not in sql
    assert "object_kind                  = EXCLUDED.object_kind" in sql


def test_picker_admission_candidates_are_seeded_before_runtime_admission() -> None:
    seed_sql = _migration_sql("278_register_openrouter_picker_candidates.sql")
    admission_sql = _migration_sql("283_admit_picker_winners_runtime_profile_routes.sql")

    for candidate_ref in (
        "candidate.openrouter.google-gemini-3-flash-preview",
        "candidate.openrouter.openai-gpt-5-4-mini",
    ):
        assert candidate_ref in seed_sql
        assert candidate_ref in admission_sql


def test_policy_compliance_receipts_migration_does_not_create_dblink_extension() -> None:
    sql = _migration_sql("298_policy_authority_compliance_receipts.sql")

    assert "CREATE EXTENSION IF NOT EXISTS dblink" not in sql
    assert "scripts/setup-dblink-for-policy-authority.sh" in sql


def test_mobile_capability_migration_adds_revoked_at_before_active_indexes() -> None:
    sql = _migration_sql("185_mobile_capability_ledger.sql")

    revoked_at_position = sql.index("ADD COLUMN IF NOT EXISTS revoked_at")
    principal_index_position = sql.index("capability_grants_principal_active_idx")
    device_index_position = sql.index("capability_grants_device_active_idx")

    assert revoked_at_position < principal_index_position
    assert revoked_at_position < device_index_position


def test_gate_evaluation_grant_ref_fk_is_added_after_column_compatibility() -> None:
    sql = _migration_sql("186_gate_evaluations_grant_coverage.sql")

    inline_fk = (
        "ADD COLUMN IF NOT EXISTS grant_ref TEXT NULL REFERENCES "
        "capability_grants (grant_id)"
    )
    plain_column = "ADD COLUMN IF NOT EXISTS grant_ref TEXT NULL;"
    explicit_fk = "ADD CONSTRAINT gate_evaluations_grant_ref_fkey"

    assert inline_fk not in sql
    assert plain_column in sql
    assert sql.index(plain_column) < sql.index(explicit_fk)


def test_mobile_sessions_migration_adds_bootstrap_session_and_budget_ledgers() -> None:
    sql = _migration_sql("188_mobile_sessions.sql")

    assert "CREATE TABLE IF NOT EXISTS mobile_bootstrap_tokens" in sql
    assert "token_hash TEXT NOT NULL UNIQUE" in sql
    assert "CREATE TABLE IF NOT EXISTS mobile_sessions" in sql
    assert "budget_used <= budget_limit" in sql
    assert "CREATE TABLE IF NOT EXISTS mobile_session_budget_events" in sql


def test_mobile_v1_archive_migration_drops_mobile_only_tables_not_capability_grants() -> None:
    sql = _migration_sql("220_archive_mobile_v1.sql")

    for table_name in (
        "mobile_session_budget_events",
        "mobile_sessions",
        "mobile_bootstrap_tokens",
        "webauthn_challenges",
        "approval_requests",
        "device_enrollments",
    ):
        assert f"DROP TABLE IF EXISTS {table_name} CASCADE" in sql

    assert "DROP TABLE IF EXISTS capability_grants" not in sql
    assert "authority.mobile_access" in sql
    assert "legacy_domain.rule.mobile" in sql
    assert "enabled = FALSE" in sql


def test_interactive_agent_session_migration_extends_agent_sessions_authority() -> None:
    sql = _migration_sql("211_interactive_agent_session_authority.sql")

    assert "ADD COLUMN IF NOT EXISTS session_kind" in sql
    assert "ADD COLUMN IF NOT EXISTS external_session_id" in sql
    assert "CREATE TABLE IF NOT EXISTS agent_session_events" in sql
    assert "REFERENCES agent_sessions (session_id)" in sql
    assert "idx_agent_session_events_session_created" in sql


def test_cqrs_authority_kernel_deduplicates_authority_domain_seed_rows() -> None:
    sql = _migration_sql("200_cqrs_authority_kernel.sql")

    assert "MIN(NULLIF(btrim(projection_ref), ''))" in sql
    assert "GROUP BY authority_ref" in sql
    assert "ON CONFLICT (authority_domain_ref) DO UPDATE SET" in sql


def test_service_lifecycle_authority_is_target_neutral_cqrs_state() -> None:
    sql = _migration_sql("201_service_lifecycle_authority.sql")

    for table_name in (
        "runtime_targets",
        "service_definitions",
        "service_desired_states",
        "service_instance_events",
        "service_instance_projection",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql

    assert "authority.service_lifecycle" in sql
    assert "projection.service_lifecycle.instances" in sql
    assert "service.lifecycle.declare_desired_state" in sql
    assert "registry_workspace_base_path_authority" in sql
    assert "windows_host" not in sql
    assert "mac_mini" not in sql
    assert "linux_host" not in sql
    assert "WORKFLOW_DATABASE_URL" not in sql


def test_cqrs_authority_object_registry_classifies_durable_objects() -> None:
    sql = _migration_sql("202_cqrs_authority_object_registry.sql")

    assert "CREATE TABLE IF NOT EXISTS authority_object_registry" in sql
    assert "CREATE TABLE IF NOT EXISTS service_bus_channel_registry" in sql
    assert "CREATE TABLE IF NOT EXISTS service_bus_message_ledger" in sql
    assert "CREATE OR REPLACE VIEW authority_object_drift_report" in sql
    assert "authority.objects.drift" in sql
    assert "runtime_target" in sql
    assert "mac_mini" not in sql
    assert "windows_host" not in sql


def test_cqrs_operation_contract_enforcement_is_deferred_and_authority_backed() -> None:
    sql = _migration_sql("203_cqrs_operation_contract_enforcement.sql")

    assert "CREATE CONSTRAINT TRIGGER trg_operation_catalog_cqrs_contract" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "enabled command must require an authority event" in sql
    assert "enabled operation is missing authority object registry row" in sql
    assert "CREATE OR REPLACE VIEW authority_contract_validation_report" in sql


def test_cqrs_event_projection_contracts_are_registered() -> None:
    sql = _migration_sql("204_cqrs_event_projection_contracts.sql")

    assert "CREATE TABLE IF NOT EXISTS authority_event_contracts" in sql
    assert "CREATE TABLE IF NOT EXISTS authority_projection_contracts" in sql
    assert "ADD COLUMN IF NOT EXISTS last_receipt_id" in sql
    assert "authority_event_projection_contract_report" in sql
    assert "replay_policy IN ('replayable', 'snapshot_only', 'not_replayable')" in sql


def test_feedback_authority_records_feedback_without_target_domain_mutation() -> None:
    sql = _migration_sql("205_feedback_authority.sql")

    assert "CREATE TABLE IF NOT EXISTS authority_feedback_streams" in sql
    assert "CREATE TABLE IF NOT EXISTS authority_feedback_events" in sql
    assert "CREATE OR REPLACE VIEW authority_feedback_event_projection" in sql
    assert "feedback.record" in sql
    assert "feedback_recorded" in sql
    assert "Feedback is evidence, not direct mutation authority" in sql


def test_feedback_list_is_classified_as_a_query_across_authorities() -> None:
    sql = _migration_sql("205_feedback_authority.sql")

    assert "'feedback.list',\n        'operation_query',\n        'query'," in sql
    assert (
        "('feedback.list', 'List feedback events', 'query', "
        "'Cataloged query for feedback intake projection.'"
    ) in sql
    assert "('operation.feedback.list', 'query', 'feedback.list'" in sql
    assert "('operation.feedback.list', 'command', 'feedback.list'" not in sql


def test_legacy_schema_authority_backfill_marks_history_as_legacy_inventory() -> None:
    sql = _migration_sql("206_legacy_schema_authority_backfill.sql")

    assert "authority.legacy_schema" in sql
    assert "projection.legacy.schema_catalog" in sql
    assert "information_schema.tables" in sql
    assert "information_schema.columns" in sql
    assert "authority_schema_adoption_report" in sql
    assert "authority_legacy_backfill_summary" in sql
    assert "legacy_inventory" in sql
    assert "authority.objects.adoption" in sql


def test_authority_objects_adoption_is_classified_as_a_query_across_authorities() -> None:
    sql = _migration_sql("206_legacy_schema_authority_backfill.sql")

    assert (
        "'authority-objects-adoption',\n"
        "    'authority.objects.adoption',\n"
        "    'operation_query',\n"
        "    'query',"
    ) in sql
    assert (
        "'authority.objects.adoption',\n"
        "        'List authority adoption state',\n"
        "        'query',"
    ) in sql
    assert (
        "'operation.authority.objects.adoption',\n"
        "        'query',\n"
        "        'authority.objects.adoption',"
    ) in sql
    assert (
        "'operation.authority.objects.adoption',\n"
        "        'command',\n"
        "        'authority.objects.adoption',"
    ) not in sql


def test_authority_objects_domain_summary_is_classified_as_a_query_across_authorities() -> None:
    sql = _migration_sql("207_legacy_domain_authority_assignment.sql")

    assert (
        "'authority-objects-domain-summary',\n"
        "    'authority.objects.domain_summary',\n"
        "    'operation_query',\n"
        "    'query',"
    ) in sql
    assert (
        "'authority.objects.domain_summary',\n"
        "        'List authority domain assignment summary',\n"
        "        'query',"
    ) in sql
    assert (
        "'operation.authority.objects.domain_summary',\n"
        "        'query',\n"
        "        'authority.objects.domain_summary',"
    ) in sql
    assert (
        "'operation.authority.objects.domain_summary',\n"
        "        'command',\n"
        "        'authority.objects.domain_summary',"
    ) not in sql


def test_document_objects_migration_handles_object_field_cutover() -> None:
    sql = _migration_sql("025_document_objects.sql")

    property_probe = "column_name = 'property_definitions'"
    registry_probe = "to_regclass('public.object_field_registry')"
    legacy_insert = "property_definitions,"
    registry_insert = "INSERT INTO object_field_registry"

    assert property_probe in sql
    assert registry_probe in sql
    assert sql.index(property_probe) < sql.index(legacy_insert)
    assert sql.index(registry_probe) < sql.index(registry_insert)
    assert "ADD COLUMN IF NOT EXISTS property_definitions" in sql
