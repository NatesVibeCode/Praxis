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
        'WORKFLOW_DATABASE_URL=$(redact_url_for_log "$bootstrap_database_url")'
        in source
    )
    assert 'for $maintenance_url' not in source
    assert 'for $(redact_url_for_log "$maintenance_url")' in source
