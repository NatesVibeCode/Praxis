from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "Databases/migrations/workflow/388_synthetic_environment_authority.sql"


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_synthetic_environment_migration_records_forge_receipts() -> None:
    sql = _sql()

    for receipt_id in [
        "b7426fc3-8237-4d3b-bdb8-fe55ba3a1bd9",
        "24159410-5c36-4ec0-b8b9-1ce173a9a8f5",
        "b063caee-5f11-49da-9b97-749cd9a8a541",
        "dd7f5204-f755-4ed9-9f30-80d05c465284",
        "56c661dc-c7d0-4d09-8b5d-55a2ac139b36",
        "304ef9ed-2684-4f29-8741-57bddcb9ad5a",
        "4a496aaf-937d-4d88-a0eb-160da8a09b35",
    ]:
        assert receipt_id in sql


def test_synthetic_environment_migration_registers_authority_tables() -> None:
    sql = _sql()

    assert "'authority.synthetic_environment'" in sql
    for table_name in ["synthetic_environments", "synthetic_environment_effects"]:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        assert f"'{table_name}'" in sql
        assert f"'table.public.{table_name}'" in sql


def test_synthetic_environment_migration_registers_event_contracts() -> None:
    sql = _sql()

    for event_type in [
        "synthetic_environment.created",
        "synthetic_environment.cleared",
        "synthetic_environment.reset",
        "synthetic_environment.event_injected",
        "synthetic_environment.clock_advanced",
    ]:
        assert f"'{event_type}'" in sql


def test_synthetic_environment_migration_registers_cqrs_operations() -> None:
    sql = _sql()

    expected = {
        "synthetic_environment_create": ("command", "synthetic_environment.created"),
        "synthetic_environment_clear": ("command", "synthetic_environment.cleared"),
        "synthetic_environment_reset": ("command", "synthetic_environment.reset"),
        "synthetic_environment_event_inject": ("command", "synthetic_environment.event_injected"),
        "synthetic_environment_clock_advance": ("command", "synthetic_environment.clock_advanced"),
        "synthetic_environment_read": ("query", None),
    }
    for operation_name, (kind, event_type) in expected.items():
        assert f"p_operation_name        := '{operation_name}'" in sql
        assert f"p_operation_kind        := '{kind}'" in sql
        operation_block = sql.split(f"p_operation_name        := '{operation_name}'", 1)[1].split(");", 1)[0]
        assert "p_authority_domain_ref  := 'authority.synthetic_environment'" in operation_block
        if kind == "query":
            assert "p_idempotency_policy    := 'read_only'" in operation_block
            assert "p_event_required        := FALSE" in operation_block
        else:
            assert "p_posture               := 'operate'" in operation_block
            assert "p_event_required        := TRUE" in operation_block
            assert f"p_event_type            := '{event_type}'" in operation_block
