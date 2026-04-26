from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_bug_f613bbfe_db_resolver_surfaces_traceback_and_dsn_hint() -> None:
    bootstrap = _read("scripts/bootstrap")
    assert "workflow_database_authority_for_repo" in bootstrap
    assert "database authority:" in bootstrap
    assert "could not resolve WORKFLOW_DATABASE_URL from repo authority" in bootstrap


def test_bug_7b02fe94_smoke_failure_distinguishes_modes() -> None:
    bootstrap = _read("scripts/bootstrap")
    assert "deterministic worker smoke" in bootstrap
    assert "workflow stream" in bootstrap
    assert "bootstrap smoke failed or timed out" in bootstrap
    assert "bootstrap smoke result file did not contain run_id" in bootstrap


def test_bug_d9b929a2_python_314_error_explains_pin() -> None:
    bootstrap = _read("scripts/bootstrap")
    env_helper = _read("scripts/_workflow_env.sh")
    assert "command -v python3.14" in bootstrap
    assert "python3.14 python@3.14" not in bootstrap
    assert "python3.14 python3.13 python3" in env_helper
    assert "workflow_python_bin" in env_helper
    assert "command -v python" in env_helper
    assert "sys.version_info[0]" in env_helper


def test_bin_python_shim_delegates_to_workflow_python_bin() -> None:
    shim = _read("bin/python")
    assert "workflow_python_bin" in shim
    assert 'exec "$(workflow_python_bin)"' in shim


def test_bug_ba265a8a_no_backslash_escaped_ampersand_path() -> None:
    bootstrap = _read("scripts/bootstrap")
    readme = _read("README.md")
    assert "Code\\&DBs" not in bootstrap
    assert "Code\\&DBs" not in readme
    assert '"Code&DBs/Workflow/surfaces/app"' in bootstrap
    assert '"Code&DBs/Workflow/surfaces/app"' in readme
