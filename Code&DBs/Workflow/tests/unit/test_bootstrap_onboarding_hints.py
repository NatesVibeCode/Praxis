from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_bug_f613bbfe_db_resolver_surfaces_traceback_and_dsn_hint() -> None:
    bootstrap = _read("scripts/bootstrap")
    assert "resolve_database_authority.stderr" in bootstrap
    assert "postgresql://user@host:5432/praxis" in bootstrap
    assert "Precedence: WORKFLOW_DATABASE_URL env var" in bootstrap


def test_bug_7b02fe94_smoke_failure_distinguishes_modes() -> None:
    bootstrap = _read("scripts/bootstrap")
    assert "smoke diagnostics" in bootstrap
    assert "has exited. Last 20 lines" in bootstrap
    assert "workflow run-status" in bootstrap
    assert "API log: $api_log_file" in bootstrap


def test_bug_d9b929a2_python_314_error_explains_pin() -> None:
    bootstrap = _read("scripts/bootstrap")
    env_helper = _read("scripts/_workflow_env.sh")
    assert "native-operator-common.sh" in bootstrap
    assert "python3.14 python@3.14" not in bootstrap
    assert "compatibility fallback" in env_helper
    assert "WORKFLOW_PYTHON_FALLBACK_WARNED" in env_helper


def test_bug_ba265a8a_no_backslash_escaped_ampersand_path() -> None:
    bootstrap = _read("scripts/bootstrap")
    readme = _read("README.md")
    assert "Code\\&DBs" not in bootstrap
    assert "Code\\&DBs" not in readme
    assert '"Code&DBs/Workflow/surfaces/app"' in bootstrap
    assert '"Code&DBs/Workflow/surfaces/app"' in readme
