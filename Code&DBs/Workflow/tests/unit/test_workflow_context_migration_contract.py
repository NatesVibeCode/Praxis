from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "Databases/migrations/workflow/386_workflow_context_authority.sql"


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_workflow_context_migration_records_forge_receipts() -> None:
    sql = _sql()

    for receipt in [
        "34f5f1a5-c01a-48fc-be3e-05b567b7ac56",
        "901bc8ca-4432-49cf-9112-65e469475978",
        "85c45a4b-6e15-4d55-a4d0-d35550bd4973",
        "9b4188b4-cad2-4c74-8b96-0a26e89eef4d",
        "feed9a4c-a0b3-4912-afc6-2a244db9fbb5",
        "4fc66691-6269-4825-a6c6-fd8a02e61e2d",
    ]:
        assert receipt in sql


def test_workflow_context_migration_registers_cqrs_operations() -> None:
    sql = _sql()

    expected = {
        "workflow_context_compile": ("command", "workflow_context.compiled"),
        "workflow_context_read": ("query", None),
        "workflow_context_transition": ("command", "workflow_context.transitioned"),
        "workflow_context_bind": ("command", "workflow_context.bound"),
        "workflow_context_guardrail_check": ("query", None),
        "object_truth_latest_version_read": ("query", None),
    }
    for operation_name, (kind, event_type) in expected.items():
        assert f"p_operation_name        := '{operation_name}'" in sql
        assert f"p_operation_kind        := '{kind}'" in sql
        if kind == "query":
            operation_block = sql.split(f"p_operation_name        := '{operation_name}'", 1)[1].split(");", 1)[0]
            assert "p_idempotency_policy    := 'read_only'" in operation_block
            assert "p_event_required        := FALSE" in operation_block
        else:
            operation_block = sql.split(f"p_operation_name        := '{operation_name}'", 1)[1].split(");", 1)[0]
            assert "p_posture               := 'operate'" in operation_block
            assert "p_event_required        := TRUE" in operation_block
            assert f"p_event_type            := '{event_type}'" in operation_block


def test_workflow_context_migration_registers_authority_tables() -> None:
    sql = _sql()

    assert "'authority.workflow_context'" in sql
    for table_name in [
        "workflow_context_packs",
        "workflow_context_entities",
        "workflow_context_bindings",
        "workflow_context_transitions",
    ]:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        assert f"'{table_name}'" in sql
        assert f"'table.public.{table_name}'" in sql
