from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_COMPOSE_PATH = _REPO_ROOT / "docker-compose.yml"


def test_sandbox_debug_and_live_overlay_are_operator_opt_in() -> None:
    source = _COMPOSE_PATH.read_text(encoding="utf-8")

    assert "PRAXIS_SANDBOX_DEBUG: ${PRAXIS_SANDBOX_DEBUG:-0}" in source
    assert "PRAXIS_HOST_WORKSPACE_ROOT: ${PRAXIS_HOST_WORKSPACE_ROOT:-}" in source
    assert "PRAXIS_SANDBOX_DEBUG: \"1\"" not in source
    assert "PRAXIS_HOST_WORKSPACE_ROOT: ${PRAXIS_HOST_WORKSPACE_ROOT:-${PWD}}" not in source
