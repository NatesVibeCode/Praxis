"""Guardrails for operator-facing ready/ launcher documentation."""

from __future__ import annotations

from pathlib import Path


def test_ready_readme_does_not_embed_retired_localhost_dsn() -> None:
    """Regression for BUG-07C3C161 — localhost Praxis DSNs are not portable authority."""
    root = Path(__file__).resolve().parents[2]
    readme = root / "artifacts" / "workflow" / "ready" / "README.md"
    text = readme.read_text(encoding="utf-8")
    assert "localhost:5432" not in text
    assert "postgresql://localhost" not in text
    assert "WORKFLOW_DATABASE_URL" in text
