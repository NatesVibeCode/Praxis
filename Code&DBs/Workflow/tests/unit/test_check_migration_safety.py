"""Tests for scripts/check-migration-safety.py.

Imports the script as a module so we can drive `scan_migrations` directly
without spawning subprocesses. The script lives outside the python package
tree, so we load it by path.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check-migration-safety.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "check_migration_safety", SCRIPT_PATH
    )
    assert spec and spec.loader, f"could not load {SCRIPT_PATH}"
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_migration_safety"] = module
    spec.loader.exec_module(module)
    return module


cms = _load_module()


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def test_clean_migration_passes(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0001_clean.sql",
        "BEGIN;\nCREATE TABLE foo (id text PRIMARY KEY);\nCOMMIT;\n",
    )
    report = cms.scan_migrations([path])
    assert report.findings == []
    assert report.bypassed == []
    assert report.has_blocking is False


def test_session_replication_role_replica_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0002_bad.sql",
        "SET session_replication_role = replica;\n",
    )
    report = cms.scan_migrations([path])
    assert len(report.findings) == 1
    assert report.findings[0].rule_id == "session_replication_role_replica"
    assert report.has_blocking is True


def test_alter_table_disable_trigger_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0003_bad.sql",
        "ALTER TABLE operator_decisions DISABLE TRIGGER policy_authority_check;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "alter_table_disable_trigger" for f in report.findings)


def test_drop_trigger_policy_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0004_bad.sql",
        "DROP TRIGGER policy_compliance_check ON operator_decisions;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "drop_trigger_policy" for f in report.findings)


def test_delete_from_operator_decisions_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0005_bad.sql",
        "DELETE FROM operator_decisions WHERE decision_key = 'old';\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "delete_from_operator_decisions" for f in report.findings)


def test_truncate_authority_receipts_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0006_bad.sql",
        "TRUNCATE TABLE authority_operation_receipts;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "truncate_authority_receipts" for f in report.findings)


def test_truncate_authority_events_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0007_bad.sql",
        "TRUNCATE authority_events;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "truncate_authority_events" for f in report.findings)


def test_alter_table_disable_rls_blocks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0008_bad.sql",
        "ALTER TABLE operator_decisions DISABLE ROW LEVEL SECURITY;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "alter_table_disable_rls" for f in report.findings)


def test_safety_bypass_sentinel_routes_to_bypassed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0009_bypass.sql",
        "SET session_replication_role = replica;  -- safety-bypass: replication test rig\n",
    )
    report = cms.scan_migrations([path])
    # Bypass routes to bypassed list, NOT to blocking list.
    assert report.findings == []
    assert report.has_blocking is False
    assert len(report.bypassed) == 1
    assert report.bypassed[0].bypass_reason == "replication test rig"


def test_comment_only_lines_ignored(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0010_comments.sql",
        "-- This migration mentions SET session_replication_role = replica in docs only\n"
        "BEGIN; SELECT 1; COMMIT;\n",
    )
    report = cms.scan_migrations([path])
    # The mention is inside a leading comment that the inline-comment
    # stripper deletes before the regex sees the line.
    assert report.findings == []


def test_case_insensitive_matching(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "0011_caps.sql",
        "set Session_Replication_Role = REPLICA;\n",
    )
    report = cms.scan_migrations([path])
    assert any(f.rule_id == "session_replication_role_replica" for f in report.findings)


def test_existing_migrations_are_clean() -> None:
    """Smoke gate: every committed migration in the repo must already pass.

    This test is the floor — if a banned pattern slips into the repo
    historically, this test will surface it on the next run.
    """
    migrations_dir = REPO_ROOT / "Code&DBs" / "Databases" / "migrations" / "workflow"
    if not migrations_dir.exists():  # fresh clone path may differ; degrade.
        pytest.skip("migrations dir not present in this checkout")
    sql_files = sorted(migrations_dir.glob("*.sql"))
    assert sql_files, "no migration files found — unexpected for this repo"
    report = cms.scan_migrations(sql_files)
    assert report.findings == [], (
        f"found {len(report.findings)} blocking pattern(s) in committed migrations: "
        + "; ".join(f"{f.path.name}:{f.line_number}[{f.rule_id}]" for f in report.findings[:10])
    )
