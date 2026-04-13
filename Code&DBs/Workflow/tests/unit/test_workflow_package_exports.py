from __future__ import annotations

import json
import subprocess
import sys


def test_workflow_package_capability_exports_do_not_preload_orchestrator():
    script = """
import importlib
import json
import sys

workflow = importlib.import_module("runtime.workflow")
orchestrator_preloaded = "runtime.workflow.orchestrator" in sys.modules
route_helper = workflow.get_route_outcomes
orchestrator_after_route_helper = "runtime.workflow.orchestrator" in sys.modules
capability_type = workflow.WorkflowCapabilities
orchestrator_after_caps = "runtime.workflow.orchestrator" in sys.modules

print(json.dumps({
    "orchestrator_preloaded": orchestrator_preloaded,
    "orchestrator_after_route_helper": orchestrator_after_route_helper,
    "orchestrator_after_caps": orchestrator_after_caps,
    "route_helper_module": route_helper.__module__,
    "capability_type_module": capability_type.__module__,
}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
    )
    payload = json.loads(completed.stdout)

    assert payload == {
        "orchestrator_preloaded": False,
        "orchestrator_after_route_helper": False,
        "orchestrator_after_caps": False,
        "route_helper_module": "runtime.workflow._capabilities",
        "capability_type_module": "runtime.workflow._capabilities",
    }
