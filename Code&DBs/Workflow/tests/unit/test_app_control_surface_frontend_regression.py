"""Regression wrapper for app-control frontend authority tests."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_app_control_surface_frontend_regressions() -> None:
    app_root = Path(__file__).resolve().parents[2] / "surfaces" / "app"
    result = subprocess.run(
        [
            "npm",
            "-C",
            str(app_root),
            "test",
            "--",
            "src/dashboard/ChatPanel.test.tsx",
            "src/workspace/MarkdownRenderer.test.tsx",
            "src/workspace/useChat.test.ts",
            "src/dashboard/CostsPanel.test.tsx",
            "src/dashboard/Dashboard.test.tsx",
            "src/App.test.tsx",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
