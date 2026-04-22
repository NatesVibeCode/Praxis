"""Static contracts for the repo bootstrap shell front door."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
BOOTSTRAP_SCRIPT = REPO_ROOT / "scripts" / "bootstrap"


def test_bootstrap_redacts_database_urls_before_logging() -> None:
    source = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

    assert "redact_url_for_log()" in source
    assert 'WORKFLOW_DATABASE_URL=$bootstrap_database_url' not in source
    assert (
        'WORKFLOW_DATABASE_URL=$(redact_url_for_log "$resolved_database_url")'
        in source
    )
    assert 'for $maintenance_url' not in source
    assert 'for $(redact_url_for_log "$maintenance_url")' in source


def test_bootstrap_does_not_bake_host_default_paths() -> None:
    source = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

    assert 'postgresql://localhost:5432/praxis' not in source
    assert '$HOME/.local/bin' not in source


def test_bootstrap_can_seed_repo_env_from_explicit_database_authority() -> None:
    source = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")
    body = source.split("resolve_database_authority() {", 1)[1].split("\n}", 1)[0]

    assert "workflow_database_authority_for_repo" in body
    assert "without_contract_env" not in body


def test_bootstrap_installs_runtime_launcher_not_repo_local_symlink() -> None:
    source = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

    assert "Installing praxis runtime launcher" in source
    assert "Praxis runtime launcher. Managed by ./scripts/bootstrap." in source
    assert "Self-contained: resolves the checkout through launcher authority" in source
    assert "runtime/launcher_authority.py" in source
    assert "PRAXIS_LAUNCHER_WORKFLOW_ROOT" not in source
    assert "WORKFLOW_ROOT = Path" not in source
    assert "ln -s \"$shim_source\" \"$shim_target\"" not in source
