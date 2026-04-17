"""Regression for the dashboard empty-state action affordance."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_dashboard_typechecks_after_empty_state_action_update() -> None:
    app_root = Path(__file__).resolve().parents[2] / "surfaces" / "app"
    result = subprocess.run(
        ["npm", "-C", str(app_root), "run", "typecheck"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
