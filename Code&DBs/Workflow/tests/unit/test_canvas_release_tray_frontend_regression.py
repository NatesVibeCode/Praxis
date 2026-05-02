"""Regression wrapper for Canvas release tray frontend authority tests."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_canvas_release_tray_frontend_authority_regressions() -> None:
    app_root = Path(__file__).resolve().parents[2] / "surfaces" / "app"
    result = subprocess.run(
        [
            "npm",
            "-C",
            str(app_root),
            "test",
            "--",
            "CanvasReleaseTray.test.tsx",
            "buildGraphDefinition.test.ts",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
