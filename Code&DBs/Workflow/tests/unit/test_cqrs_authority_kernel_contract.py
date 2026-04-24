from __future__ import annotations

import re
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_WORKFLOW_ROOT = _REPO_ROOT / "Code&DBs" / "Workflow"
_MIGRATION = (
    _REPO_ROOT
    / "Code&DBs"
    / "Databases"
    / "migrations"
    / "workflow"
    / "200_cqrs_authority_kernel.sql"
)


def test_cqrs_authority_kernel_migration_declares_registry_event_receipt_tables() -> None:
    sql = _MIGRATION.read_text(encoding="utf-8")

    for table_name in (
        "authority_storage_targets",
        "authority_domains",
        "authority_projection_registry",
        "authority_projection_state",
        "authority_operation_receipts",
        "authority_events",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql

    for column_name in (
        "authority_domain_ref",
        "storage_target_ref",
        "idempotency_key_fields",
        "receipt_required",
        "event_required",
        "projection_freshness_policy_ref",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column_name}" in sql


def test_operation_gateway_persists_authority_events_and_receipts() -> None:
    source = (_WORKFLOW_ROOT / "runtime" / "operation_catalog_gateway.py").read_text(
        encoding="utf-8"
    )

    assert "INSERT INTO authority_operation_receipts" in source
    assert "INSERT INTO authority_events" in source
    assert "UPDATE authority_events" in source


def test_primary_surfaces_do_not_mutate_authority_tables_directly() -> None:
    forbidden = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM)\b", re.I)
    checked_files = (
        "surfaces/api/rest.py",
        "surfaces/mcp/tools/operator.py",
        "surfaces/cli/commands/workflow.py",
        "surfaces/cli/workflow_cli.py",
    )

    offenders: list[str] = []
    for relative_path in checked_files:
        source = (_WORKFLOW_ROOT / relative_path).read_text(encoding="utf-8")
        if forbidden.search(source):
            offenders.append(relative_path)

    assert offenders == []
