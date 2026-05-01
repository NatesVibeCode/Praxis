from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "Databases/migrations/workflow/387_synthetic_data_authority.sql"


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_synthetic_data_migration_records_forge_receipts() -> None:
    sql = _sql()

    assert "8efe7870-70fa-46c6-a4f6-f1eba273376b" in sql
    assert "c68ec71d-4644-4a42-8373-7e10033ae9d1" in sql


def test_synthetic_data_migration_registers_authority_tables() -> None:
    sql = _sql()

    assert "'authority.synthetic_data'" in sql
    for table_name in ["synthetic_data_sets", "synthetic_data_records"]:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
        assert f"'{table_name}'" in sql
        assert f"'table.public.{table_name}'" in sql


def test_synthetic_data_migration_registers_cqrs_operations() -> None:
    sql = _sql()

    expected = {
        "synthetic_data_generate": ("command", "synthetic_data.generated"),
        "synthetic_data_read": ("query", None),
    }
    for operation_name, (kind, event_type) in expected.items():
        assert f"p_operation_name        := '{operation_name}'" in sql
        assert f"p_operation_kind        := '{kind}'" in sql
        operation_block = sql.split(f"p_operation_name        := '{operation_name}'", 1)[1].split(");", 1)[0]
        assert "p_authority_domain_ref  := 'authority.synthetic_data'" in operation_block
        if kind == "query":
            assert "p_idempotency_policy    := 'read_only'" in operation_block
            assert "p_event_required        := FALSE" in operation_block
        else:
            assert "p_posture               := 'operate'" in operation_block
            assert "p_event_required        := TRUE" in operation_block
            assert f"p_event_type            := '{event_type}'" in operation_block
