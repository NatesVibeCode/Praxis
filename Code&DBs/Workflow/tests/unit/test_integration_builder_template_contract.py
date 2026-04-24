"""Contract tests for integration-builder queue specs and live MCP tool shape."""

from __future__ import annotations

from pathlib import Path


def test_integration_builder_queue_specs_avoid_stale_praxis_integration_actions() -> None:
    """``praxis_integration`` uses call/describe/list, not legacy execute/slug-only examples."""
    repo_root = Path(__file__).resolve().parents[4]
    paths = [
        repo_root / "config" / "cascade" / "specs" / "W_integration_builder_template.queue.json",
        repo_root
        / "artifacts"
        / "workflow"
        / "integration_builder"
        / "connector_openweather.queue.json",
    ]
    stale_snippets = (
        '"action":"execute"',
        '"action": "execute"',
        "action='execute'",
        '\\"action\\":\\"execute\\"',
    )
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for bad in stale_snippets:
            assert bad not in text, f"{path} contains stale praxis_integration example {bad!r}"
