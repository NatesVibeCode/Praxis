from __future__ import annotations

from pathlib import Path

_WORKFLOW = Path(__file__).resolve().parents[2]


def test_ready_readme_updated() -> None:
    readme_path = _WORKFLOW / "artifacts" / "workflow" / "ready" / "README.md"
    assert readme_path.exists()
    content = readme_path.read_text()
    
    # Check that it doesn't contain the hardcoded localhost psql
    assert "psql postgresql://localhost:5432/praxis" not in content
    
    # Check that it contains the canonical db query command
    assert "./scripts/praxis db query" in content
