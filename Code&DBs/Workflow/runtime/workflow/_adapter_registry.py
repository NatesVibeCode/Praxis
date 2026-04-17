"""Canonical workflow adapter registry authority.

One module owns which adapter types the deterministic workflow runtime can
instantiate. Callers build concrete registries from this authority instead of
hand-assembling slightly different registries in multiple places.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from adapters import AdapterRegistry, CLILLMAdapter, LLMTaskAdapter, MCPTaskAdapter
from adapters.api_task import APITaskAdapter
from adapters.context_adapter import ContextCompilerAdapter
from adapters.file_writer_adapter import FileWriterAdapter
from adapters.output_parser_adapter import OutputParserAdapter
from adapters.verify_adapter import VerifyAdapter

_WORKFLOW_TRANSPORT_ADAPTER_TYPES: tuple[str, ...] = (
    "api_task",
    "llm_task",
    "cli_llm",
    "mcp_task",
)
_WORKFLOW_AUXILIARY_ADAPTER_TYPES: tuple[str, ...] = (
    "context_compiler",
    "output_parser",
    "file_writer",
    "verifier",
)
_WORKFLOW_CORE_ADAPTER_TYPES: tuple[str, ...] = (
    "deterministic_task",
    "control_operator",
)
_WORKFLOW_RUNTIME_ADAPTER_TYPES: frozenset[str] = frozenset(
    (
        *_WORKFLOW_CORE_ADAPTER_TYPES,
        *_WORKFLOW_TRANSPORT_ADAPTER_TYPES,
        *_WORKFLOW_AUXILIARY_ADAPTER_TYPES,
    )
)


def workflow_runtime_adapter_types() -> tuple[str, ...]:
    """Return the canonical adapter types the workflow runtime can execute."""

    return tuple(sorted(_WORKFLOW_RUNTIME_ADAPTER_TYPES))


def workflow_transport_adapter_types() -> tuple[str, ...]:
    """Return provider-facing transport adapters admitted by the runtime."""

    return _WORKFLOW_TRANSPORT_ADAPTER_TYPES


def runtime_supports_workflow_adapter_type(adapter_type: str) -> bool:
    """Report whether the workflow runtime can instantiate the adapter type."""

    normalized = str(adapter_type or "").strip()
    return normalized in _WORKFLOW_RUNTIME_ADAPTER_TYPES


def build_workflow_adapter_registry(
    *,
    adapter_types: Iterable[str],
    shadow_packet_config: Mapping[str, Any] | None = None,
) -> AdapterRegistry:
    """Build the canonical runtime adapter registry for one workflow execution."""

    normalized_adapter_types = {
        str(adapter_type).strip()
        for adapter_type in adapter_types
        if str(adapter_type).strip()
    }
    registry = AdapterRegistry(
        api_task_adapter=APITaskAdapter() if "api_task" in normalized_adapter_types else None,
        llm_task_adapter=LLMTaskAdapter() if "llm_task" in normalized_adapter_types else None,
        cli_llm_adapter=CLILLMAdapter() if "cli_llm" in normalized_adapter_types else None,
        mcp_task_adapter=MCPTaskAdapter() if "mcp_task" in normalized_adapter_types else None,
    )
    if "context_compiler" in normalized_adapter_types:
        registry.register(
            "context_compiler",
            ContextCompilerAdapter(
                shadow_packet_config=dict(shadow_packet_config) if shadow_packet_config else None,
            ),
        )
    if "output_parser" in normalized_adapter_types:
        registry.register("output_parser", OutputParserAdapter())
    if "file_writer" in normalized_adapter_types:
        registry.register("file_writer", FileWriterAdapter())
    if "verifier" in normalized_adapter_types:
        registry.register("verifier", VerifyAdapter())
    return registry


__all__ = [
    "build_workflow_adapter_registry",
    "runtime_supports_workflow_adapter_type",
    "workflow_runtime_adapter_types",
    "workflow_transport_adapter_types",
]
