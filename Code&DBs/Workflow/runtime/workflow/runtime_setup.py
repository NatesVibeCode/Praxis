"""Workflow runtime setup authority.

This module owns graph construction and runtime wiring so the orchestrator can
focus on execution flow instead of building every dependency inline.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from adapters import AdapterRegistry
from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from runtime.domain import AtomicEvidenceWriter
from runtime.native_authority import default_native_authority_refs
from runtime._workflow_database import resolve_runtime_database_url
from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from registry.native_runtime_profile_sync import (
    resolve_native_runtime_profile_config,
)
from storage.postgres.validators import PostgresConfigurationError
from ._adapter_registry import build_workflow_adapter_registry

if TYPE_CHECKING:
    from .orchestrator import WorkflowSpec


def _default_workspace() -> str:
    return default_native_authority_refs()[0]


def _default_runtime_profile() -> str:
    return default_native_authority_refs()[1]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _unique_id() -> str:
    return uuid.uuid4().hex[:10]


@dataclass(frozen=True, slots=True)
class WorkflowRuntimeSetup:
    """Concrete runtime dependencies for one workflow execution."""

    registry: RegistryResolver
    adapter_registry: AdapterRegistry
    evidence_writer: AtomicEvidenceWriter


def _build_workflow_graph(spec: "WorkflowSpec") -> WorkflowRequest:
    """Build the deterministic workflow graph for a single workflow spec."""

    import hashlib as _hashlib

    workspace_ref = spec.workspace_ref or _default_workspace()
    runtime_profile_ref = spec.runtime_profile_ref or _default_runtime_profile()
    suffix = _unique_id()
    workflow_id = f"workflow.run.{suffix}"
    request_id = f"request.run.{suffix}"

    authority_requirements = {
        "workspace_ref": workspace_ref,
        "runtime_profile_ref": runtime_profile_ref,
    }
    execution_boundary = {"workspace_ref": workspace_ref}

    nodes: list[WorkflowNodeContract] = []
    edges: list[WorkflowEdgeContract] = []
    position_index = 0
    edge_index = 0

    def _node(node_id, adapter_type, display_name, inputs, expected_outputs=None):
        nonlocal position_index
        node = WorkflowNodeContract(
            node_id=node_id,
            node_type="deterministic_task",
            adapter_type=adapter_type,
            display_name=display_name,
            inputs=inputs,
            expected_outputs=expected_outputs or {},
            success_condition={"kind": "always"},
            failure_behavior={"kind": "stop"},
            authority_requirements=authority_requirements,
            execution_boundary=execution_boundary,
            position_index=position_index,
        )
        nodes.append(node)
        position_index += 1
        return node_id

    def _edge(from_id, to_id, payload_mapping=None):
        nonlocal edge_index
        edges.append(
            WorkflowEdgeContract(
                edge_id=f"edge_{edge_index}",
                edge_type="after_success",
                from_node_id=from_id,
                to_node_id=to_id,
                release_condition={"kind": "always"},
                payload_mapping=payload_mapping or {},
                position_index=edge_index,
            )
        )
        edge_index += 1

    packet_runtime_enabled = _packet_runtime_enabled(spec)

    prev = _node(
        "context",
        "context_compiler",
        "compile context",
        {
            "prompt": spec.prompt,
            "scope_read": spec.scope_read,
            "scope_write": spec.scope_write,
            "workdir": spec.workdir,
            "context_sections": spec.context_sections,
            "system_prompt": spec.system_prompt,
            "provider_slug": spec.provider_slug,
            "model_slug": spec.model_slug,
        },
    )

    llm_inputs = {
        "provider_slug": spec.provider_slug,
        "model_slug": spec.model_slug,
        "max_tokens": spec.max_tokens,
        "temperature": spec.temperature,
        "timeout": spec.timeout,
        "task_type": spec.task_type,
        "scope_write": spec.scope_write,
    }
    if packet_runtime_enabled:
        llm_inputs.update(
            {
                "packet_required": True,
                "definition_revision": getattr(spec, "definition_revision", None),
                "plan_revision": getattr(spec, "plan_revision", None),
            }
        )
        _edge(
            prev,
            "llm",
            payload_mapping={
                "execution_packet_ref": "execution_packet_ref",
                "execution_packet_hash": "execution_packet_hash",
            },
        )
    else:
        _edge(prev, "llm", payload_mapping={"prompt": "user_message", "system_prompt": "system_message"})
    prev = _node(
        "llm",
        spec.adapter_type,
        spec.label or "workflow",
        llm_inputs,
    )

    _edge(prev, "parser", payload_mapping={"completion": "completion"})
    prev = _node("parser", "output_parser", "parse output", {"scope_write": spec.scope_write})

    if spec.scope_write and spec.workdir:
        _edge(prev, "writer", payload_mapping={"code_blocks": "code_blocks"})
        prev = _node("writer", "file_writer", "write files", {"workspace_root": spec.workdir})

    verify_bindings = list(spec.verify_refs or [])
    if verify_bindings:
        _edge(prev, "verifier")
        prev = _node(
            "verifier",
            "verifier",
            "verify",
            {
                "bindings": verify_bindings,
                "workdir": spec.workdir,
            },
        )

    _edge(prev, "terminal")
    _node("terminal", "deterministic_task", "terminal", {"allow_passthrough_echo": True}, {"terminal": True})

    topology = json.dumps(
        [(node.node_id, node.adapter_type) for node in nodes]
        + [(edge.from_node_id, edge.to_node_id) for edge in edges],
        sort_keys=True,
    )
    definition_hash = f"sha256:{_hashlib.sha256(topology.encode()).hexdigest()[:16]}"
    definition_id = f"workflow_definition.run.{definition_hash}:v1"

    return WorkflowRequest(
        schema_version=1,
        workflow_id=workflow_id,
        request_id=request_id,
        workflow_definition_id=definition_id,
        definition_hash=definition_hash,
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=tuple(nodes),
        edges=tuple(edges),
        requested_at=_utc_now(),
    )


def _build_registry(spec: "WorkflowSpec") -> RegistryResolver:
    """Build runtime registry state for workflow intake."""

    runtime_profile_ref = spec.runtime_profile_ref or _default_runtime_profile()
    config = resolve_native_runtime_profile_config(runtime_profile_ref)
    workspace_ref = spec.workspace_ref or config.workspace_ref or _default_workspace()
    workdir = spec.workdir or config.workdir
    repo_root = workdir

    return RegistryResolver(
        workspace_records={
            workspace_ref: [
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=repo_root,
                    workdir=workdir,
                ),
            ],
        },
        runtime_profile_records={
            runtime_profile_ref: [
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id=config.model_profile_id,
                    provider_policy_id=config.provider_policy_id,
                    sandbox_profile_ref=config.sandbox_profile_ref,
                ),
            ],
        },
    )


def _shadow_packet_config(spec: "WorkflowSpec") -> dict[str, object] | None:
    packet_provenance = getattr(spec, "packet_provenance", None)
    normalized_packet_provenance = (
        dict(packet_provenance)
        if isinstance(packet_provenance, dict) and packet_provenance
        else None
    )
    definition_revision = str(getattr(spec, "definition_revision", "") or "").strip()
    plan_revision = str(getattr(spec, "plan_revision", "") or "").strip()
    if normalized_packet_provenance is None and (not definition_revision or not plan_revision):
        return None
    config: dict[str, object] = {
        "adapter_type": spec.adapter_type,
        "allowed_tools": list(spec.allowed_tools or []),
        "capabilities": list(spec.capabilities or []),
        "definition_revision": definition_revision or None,
        "job_label": _shadow_packet_job_label(spec),
        "packet_provenance": normalized_packet_provenance,
        "plan_revision": plan_revision or None,
        "task_type": spec.task_type,
        "verify_refs": list(spec.verify_refs or []),
    }
    return config


def _packet_runtime_enabled(spec: "WorkflowSpec") -> bool:
    return _shadow_packet_config(spec) is not None


def _shadow_packet_job_label(spec: "WorkflowSpec") -> str:
    label = str(spec.label or "").strip()
    if label:
        return label

    packet_provenance = getattr(spec, "packet_provenance", None)
    if not isinstance(packet_provenance, dict):
        return "workflow"

    labels: list[str] = []
    for source_name in ("compiled_spec_row", "definition_row"):
        source = packet_provenance.get(source_name)
        if not isinstance(source, dict):
            continue
        jobs = source.get("jobs")
        if not isinstance(jobs, list):
            continue
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_label = str(job.get("label") or "").strip()
            if job_label:
                labels.append(job_label)

    unique_labels = list(dict.fromkeys(labels))
    if len(unique_labels) == 1:
        return unique_labels[0]
    return "workflow"


def _build_adapter_registry(spec: "WorkflowSpec") -> AdapterRegistry:
    """Build the adapter registry for deterministic workflow execution."""
    adapter_types = {
        "context_compiler",
        str(spec.adapter_type or "").strip(),
        "output_parser",
    }
    if spec.scope_write and spec.workdir:
        adapter_types.add("file_writer")
    if spec.verify_refs:
        adapter_types.add("verifier")
    return build_workflow_adapter_registry(
        adapter_types=adapter_types,
        shadow_packet_config=_shadow_packet_config(spec),
    )


def _build_evidence_writer(spec: "WorkflowSpec") -> AtomicEvidenceWriter:
    """Build the canonical workflow evidence writer."""

    del spec
    try:
        database_url = resolve_runtime_database_url(required=True)
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "WORKFLOW_DATABASE_URL is required; in-memory evidence fallback has been removed."
        ) from exc
    from ..persistent_evidence import PostgresEvidenceWriter

    return PostgresEvidenceWriter(database_url=database_url)


def build_workflow_runtime_setup(spec: "WorkflowSpec") -> WorkflowRuntimeSetup:
    """Build the runtime dependencies required to execute one workflow spec."""

    return WorkflowRuntimeSetup(
        registry=_build_registry(spec),
        adapter_registry=_build_adapter_registry(spec),
        evidence_writer=_build_evidence_writer(spec),
    )


__all__ = [
    "WorkflowRuntimeSetup",
    "_build_registry",
    "_build_workflow_graph",
    "build_workflow_runtime_setup",
]
