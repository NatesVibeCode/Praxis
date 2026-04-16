"""Workflow runtime package with lazy re-exports."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "WorkflowCapabilities",
    "WorkflowResult",
    "WorkflowSpec",
    "WorkflowWorker",
    "_build_registry",
    "_build_workflow_graph",
    "build_workflow_runtime_setup",
    "claim_one",
    "run_workflow",
    "run_workflow_batch_from_file",
    "run_workflow_from_spec_file",
    "run_workflow_parallel",
    "run_workflow_pipeline",
    "run_single_workflow",
    "get_route_outcomes",
    "get_run_status",
    "orchestrator",
    "preview_workflow_execution",
    "run_worker_loop",
    "submit_workflow",
    "unified",
    "wait_for_run",
    "worker",
]

_EXPORT_MODULES = {
    "WorkflowCapabilities": "._capabilities",
    "WorkflowResult": ".orchestrator",
    "WorkflowSpec": ".orchestrator",
    "WorkflowWorker": ".worker",
    "_build_registry": ".runtime_setup",
    "_build_workflow_graph": ".runtime_setup",
    "build_workflow_runtime_setup": ".runtime_setup",
    "claim_one": ".unified",
    "run_workflow": ".orchestrator",
    "run_workflow_batch_from_file": ".orchestrator",
    "run_workflow_from_spec_file": ".orchestrator",
    "run_workflow_parallel": ".orchestrator",
    "run_workflow_pipeline": ".orchestrator",
    "run_single_workflow": ".orchestrator",
    "get_route_outcomes": "._capabilities",
    "get_run_status": ".unified",
    "orchestrator": ".orchestrator",
    "preview_workflow_execution": ".unified",
    "run_worker_loop": ".unified",
    "submit_workflow": ".unified",
    "unified": ".unified",
    "wait_for_run": ".unified",
    "worker": ".worker",
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name, __name__)
    value = module if name in {"orchestrator", "unified", "worker"} else getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
