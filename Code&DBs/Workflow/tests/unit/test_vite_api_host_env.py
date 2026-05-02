"""Smoke for BUG-89F67F22: Vite dev config reads PRAXIS_API_HOST env var.

The Canvas dev server's ``API_HOST`` constant must read ``process.env.PRAXIS_API_HOST``
so operators can override the bind address (mirroring the existing
``PRAXIS_UI_HOST`` pattern). Without the env var, ``127.0.0.1`` is the only
allowed bind, which breaks dev setups behind a different loopback or remote.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
VITE_CONFIG = REPO_ROOT / "Code&DBs/Workflow/surfaces/app/vite.config.ts"


def test_vite_api_host_reads_env_var() -> None:
    text = VITE_CONFIG.read_text(encoding="utf-8")
    assert "process.env.PRAXIS_API_HOST" in text, (
        "vite.config.ts must read process.env.PRAXIS_API_HOST for the API_HOST constant"
    )
    # Confirm the literal hardcoded const is gone
    assert "const API_HOST = '127.0.0.1';" not in text, (
        "vite.config.ts still has the hardcoded API_HOST literal without env fallback"
    )
