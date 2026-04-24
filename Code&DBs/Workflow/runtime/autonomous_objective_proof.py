"""Fresh import shim for the autonomous objective proof builder.

Long-lived workflow workers may already have ``runtime.workflow_eval`` in
memory. This module gives live specs a new import path and reloads the helper
module before resolving the builder.
"""

from __future__ import annotations

import importlib
from typing import Any

from runtime import workflow_eval


def run_autonomous_objective_proof(payload: dict[str, Any]) -> dict[str, Any]:
    module = importlib.reload(workflow_eval)
    return module.run_autonomous_objective_proof(payload)


__all__ = ["run_autonomous_objective_proof"]
