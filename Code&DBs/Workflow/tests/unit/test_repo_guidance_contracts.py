from __future__ import annotations

from pathlib import Path

from surfaces.mcp.catalog import get_tool_catalog


def test_workflow_tool_description_prefers_kickoff_first() -> None:
    definition = get_tool_catalog()["praxis_workflow"]

    assert "kickoff call" in definition.description
    assert "default wait=true" not in definition.description
    assert "Run and wait:" not in definition.description


def test_repo_claude_guidance_uses_catalog_driven_cli() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    text = (repo_root / ".claude" / "CLAUDE.md").read_text(encoding="utf-8")

    assert "42 catalog-backed tools" in text
    assert "workflow tools list" in text
    assert "workflow query \"status\"" in text
    assert 'praxis_query("status")' not in text
    assert "38 tools organized by surface" not in text
    assert "No second call needed" not in text


def test_bug_authority_reference_uses_praxis_bugs() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    text = (
        repo_root
        / "Skills"
        / "praxis-bug-logging"
        / "references"
        / "bug-authority.md"
    ).read_text(encoding="utf-8")

    assert "workflow tools describe praxis_bugs" in text
    assert "workflow tools call praxis_bugs" in text
    assert "praxis_bugs(action=\"file\"" in text
    assert "dag_bugs" not in text
