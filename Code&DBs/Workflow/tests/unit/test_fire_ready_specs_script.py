from __future__ import annotations

import subprocess
import os
import json
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _WORKFLOW_ROOT / "scripts" / "fire_ready_specs.sh"


def test_fire_ready_specs_sh_exists() -> None:
    # Basic check that the script exists and is executable
    script_path = _SCRIPT
    assert script_path.exists()
    assert os.access(script_path, os.X_OK)

def test_fire_ready_specs_uses_json_flag() -> None:
    # Check that the script contains the --json flag and jq call
    script_path = _SCRIPT
    content = script_path.read_text()
    assert "--json" in content
    assert "jq -r '.run_id // empty'" in content
