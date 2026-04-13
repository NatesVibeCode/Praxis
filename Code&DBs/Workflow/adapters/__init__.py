"""Adapter translation boundary for workflow_event and receipt envelopes."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "APITaskAdapter",
    "AdapterRegistry",
    "AdapterResolutionError",
    "CLILLMAdapter",
    "CodeBlock",
    "ControlOperatorAdapter",
    "DeterministicTaskAdapter",
    "DeterministicExecutionControl",
    "DeterministicTaskRequest",
    "DeterministicTaskResult",
    "ExecutionResult",
    "LLMTaskAdapter",
    "MCPTaskAdapter",
    "StructuredOutput",
    "TaskAdapter",
    "build_claim_received_proof",
    "build_transition_proof",
    "parse_model_output",
    "run_model",
]

_EXPORT_MODULES = {
    "APITaskAdapter": ".api_task",
    "AdapterRegistry": ".deterministic",
    "AdapterResolutionError": ".deterministic",
    "CLILLMAdapter": ".cli_llm",
    "CodeBlock": ".structured_output",
    "ControlOperatorAdapter": ".deterministic",
    "DeterministicTaskAdapter": ".deterministic",
    "DeterministicExecutionControl": ".deterministic",
    "DeterministicTaskRequest": ".deterministic",
    "DeterministicTaskResult": ".deterministic",
    "ExecutionResult": ".docker_runner",
    "LLMTaskAdapter": ".llm_task",
    "MCPTaskAdapter": ".mcp_task",
    "StructuredOutput": ".structured_output",
    "TaskAdapter": ".deterministic",
    "build_claim_received_proof": ".evidence",
    "build_transition_proof": ".evidence",
    "parse_model_output": ".structured_output",
    "run_model": ".docker_runner",
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
