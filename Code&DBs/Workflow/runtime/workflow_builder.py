"""Multi-step workflow builder.

Turns a list of WorkflowStep descriptors into a valid WorkflowRequest
with proper node wiring, dependency edges, and payload mappings so the
RuntimeOrchestrator can execute variable-length pipelines.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from adapters.provider_registry import default_llm_adapter_type
from contracts.domain import (
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from runtime.native_authority import default_native_authority_refs


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _unique_id() -> str:
    return uuid.uuid4().hex[:10]


def _default_workspace() -> str:
    return default_native_authority_refs()[0]


def _default_runtime_profile() -> str:
    return default_native_authority_refs()[1]


def _default_llm_adapter() -> str:
    return default_llm_adapter_type()


@dataclass(frozen=True, slots=True)
class WorkflowStep:
    """One step in a multi-step pipeline.

    Parameters
    ----------
    name:
        Human-readable step name (also used as the node display_name).
    prompt:
        The prompt or task description for this step.
    adapter_type:
        Adapter to execute the step (default resolved from the global LLM transport).
    provider_slug:
        Provider override (e.g. ``"anthropic"``, ``"openai"``).
        ``None`` means the adapter/router decides.
    model_slug:
        Model override.  ``None`` means the adapter/router decides.
    tier:
        Routing tier (``"frontier"``, ``"mid"``, ``"economy"``).
    max_tokens:
        Token budget for this step.
    depends_on:
        Names of upstream steps whose output this step receives.
        When empty **and** no other step declares this step as a
        dependency, ``build_workflow_request`` falls back to a
        linear chain (step 0 -> step 1 -> ... -> step N).
    edge_type:
        Edge type for edges created from ``depends_on`` entries.
        One of ``"after_success"`` (default), ``"after_failure"``,
        ``"after_any"``, or ``"conditional"``.  When using
        ``"conditional"``, set ``release_condition`` to the predicate.
    release_condition:
        Predicate for ``"conditional"`` edges.  Format:
        ``{"field": "output_key", "equals": "expected_value"}``.
        Ignored for non-conditional edge types.
    scope_source:
        How to resolve scope for this step. One of:
          - ``"none"`` (default): static scope, no dynamic resolution
          - ``"static"``: scope declared in spec at build time
          - ``"upstream"``: scope discovered from prior step outputs
    scope_strict:
        When ``True``, unresolved file references fail the step before
        execution. When ``False`` (default), unresolved refs are logged
        but execution proceeds.
    """

    name: str
    prompt: str
    adapter_type: str = field(default_factory=_default_llm_adapter)
    provider_slug: str | None = None
    model_slug: str | None = None
    tier: str | None = None
    max_tokens: int = 4096
    depends_on: tuple[str, ...] = ()
    edge_type: str = "after_success"
    release_condition: Mapping[str, Any] | None = None
    fan_out: bool = False
    fan_out_prompt: str | None = None
    fan_out_max_parallel: int = 4
    scope_source: str = "none"
    scope_strict: bool = False


def build_workflow_request(
    steps: list[WorkflowStep],
    *,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
) -> WorkflowRequest:
    """Build a ``WorkflowRequest`` from an ordered list of steps.

    Dependency wiring
    -----------------
    * If **any** step declares ``depends_on``, explicit dependency mode is
      used: edges are created only for declared dependencies.
    * If **no** step declares ``depends_on``, the steps are chained
      linearly (step 0 -> step 1 -> ... -> step N-1).

    For each edge the ``payload_mapping`` forwards the upstream node's
    ``completion`` output as ``upstream_completion`` in the downstream
    node's ``dependency_inputs``.
    """
    normalized_workspace_ref = workspace_ref or _default_workspace()
    normalized_runtime_profile_ref = runtime_profile_ref or _default_runtime_profile()

    if not steps:
        raise ValueError("At least one step is required")

    suffix = _unique_id()
    workflow_id = f"workflow.pipeline.{suffix}"
    request_id = f"request.pipeline.{suffix}"
    definition_id = f"workflow_definition.pipeline.{suffix}:v1"
    definition_hash = f"sha256:pipeline:{suffix}"

    # Map step name -> index for dependency lookup
    name_to_index: dict[str, int] = {}
    for i, step in enumerate(steps):
        if step.name in name_to_index:
            raise ValueError(f"Duplicate step name: {step.name!r}")
        name_to_index[step.name] = i

    # Determine whether we use explicit deps or linear chain
    has_explicit_deps = any(step.depends_on for step in steps)

    # --- Build nodes ---
    nodes: list[WorkflowNodeContract] = []
    for i, step in enumerate(steps):
        input_payload: dict[str, Any] = {
            "prompt": step.prompt,
            "max_tokens": step.max_tokens,
        }
        if step.provider_slug is not None:
            input_payload["provider_slug"] = step.provider_slug
        if step.model_slug is not None:
            input_payload["model_slug"] = step.model_slug
            input_payload["model"] = step.model_slug

        # Add scope resolution metadata if specified
        if step.scope_source != "none":
            input_payload["scope_source"] = step.scope_source
            input_payload["scope_strict"] = step.scope_strict

        nodes.append(
            WorkflowNodeContract(
                node_id=f"node_{i}",
                node_type="deterministic_task",
                adapter_type=step.adapter_type,
                display_name=step.name,
                inputs=input_payload,
                expected_outputs={},
                success_condition={"kind": "always"},
                failure_behavior={"kind": "stop"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={
                    "workspace_ref": workspace_ref,
                },
                position_index=i,
            )
        )

    # --- Build edges ---
    edges: list[WorkflowEdgeContract] = []
    edge_counter = 0

    if has_explicit_deps:
        for i, step in enumerate(steps):
            for dep_name in step.depends_on:
                if dep_name not in name_to_index:
                    raise ValueError(
                        f"Step {step.name!r} depends on unknown step {dep_name!r}"
                    )
                from_idx = name_to_index[dep_name]
                edge_type = step.edge_type
                if edge_type == "conditional" and step.release_condition:
                    release_condition = dict(step.release_condition)
                else:
                    release_condition = {"kind": "always"}
                edges.append(
                    WorkflowEdgeContract(
                        edge_id=f"edge_{edge_counter}",
                        edge_type=edge_type,
                        from_node_id=f"node_{from_idx}",
                        to_node_id=f"node_{i}",
                        release_condition=release_condition,
                        payload_mapping={"upstream_completion": "completion"},
                        position_index=edge_counter,
                    )
                )
                edge_counter += 1
    else:
        # Linear chain: step 0 -> step 1 -> ... -> step N-1
        for i in range(len(steps) - 1):
            edges.append(
                WorkflowEdgeContract(
                    edge_id=f"edge_{edge_counter}",
                    edge_type="after_success",
                    from_node_id=f"node_{i}",
                    to_node_id=f"node_{i + 1}",
                    release_condition={"kind": "always"},
                    payload_mapping={"upstream_completion": "completion"},
                    position_index=edge_counter,
                )
            )
            edge_counter += 1

    return WorkflowRequest(
        schema_version=1,
        workflow_id=workflow_id,
        request_id=request_id,
        workflow_definition_id=definition_id,
        definition_hash=definition_hash,
        workspace_ref=normalized_workspace_ref,
        runtime_profile_ref=normalized_runtime_profile_ref,
        nodes=tuple(nodes),
        edges=tuple(edges),
        requested_at=_utc_now(),
    )


def build_pipeline(
    steps: list[WorkflowStep],
) -> WorkflowRequest:
    """Convenience wrapper: build a pipeline WorkflowRequest with defaults."""

    return build_workflow_request(steps)


__all__ = [
    "WorkflowStep",
    "build_pipeline",
    "build_workflow_request",
]
