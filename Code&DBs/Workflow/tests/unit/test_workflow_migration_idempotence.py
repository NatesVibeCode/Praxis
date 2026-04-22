from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_WORKFLOW_MIGRATIONS = _REPO_ROOT / "Code&DBs" / "Databases" / "migrations" / "workflow"


def _migration_sql(filename: str) -> str:
    return (_WORKFLOW_MIGRATIONS / filename).read_text(encoding="utf-8")


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


def test_legacy_domain_assignment_rehomes_inventory_without_modernization_theater() -> None:
    sql = _migration_sql("207_legacy_domain_authority_assignment.sql")

    assert "CREATE TABLE IF NOT EXISTS authority_legacy_domain_assignment_rules" in sql
    assert "authority_legacy_domain_assignment_summary" in sql
    assert "domain_assigned_legacy" in sql
    assert "authority.mobile_access" in sql
    assert "authority.objects.domain_summary" in sql
    assert "Column discovered from information_schema for the CQRS legacy domain assignment rules table" in sql
    assert "legacy_domain.rule.schema_migrations" in sql
    assert "legacy_domain.rule.provider_route_health', 319" in sql
    assert "legacy_domain.rule.capability_outcomes', 259" in sql
    assert "These rules do not claim command/event modernization" in sql


def test_structured_document_semantic_authority_splits_structure_meaning_and_recall() -> None:
    sql = _migration_sql("208_structured_document_semantic_authority.sql")

    for table_name in (
        "structured_document_revisions",
        "structured_document_sections",
        "structured_document_section_embeddings",
        "structured_document_context_selection_receipts",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql

    for predicate in (
        "'defines'",
        "'cites'",
        "'supersedes'",
        "'applies_to'",
        "'constrains'",
        "'aliases'",
    ):
        assert predicate in sql

    assert "authority.structured_documents" in sql
    assert "structured_document_section_semantics" in sql
    assert "semantic_current_assertions" in sql
    assert "semantic_assertions provide meaning" in sql
    assert "embedding vector(384)" in sql
    assert "projection_only" in sql
    assert "vector similarity may recall candidates but cannot select authoritative context alone" in sql
    assert "structured_documents.record_context_selection" in sql
    assert "deterministic_reason_codes ?| ARRAY" in sql


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
