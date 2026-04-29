"""Workflow submission pipeline: spec parsing, job creation, and idempotency."""
from __future__ import annotations

from runtime.async_bridge import run_sync_safe
from collections.abc import Mapping
import concurrent.futures
from dataclasses import replace
import hashlib
import json
import logging
import os
import time
import uuid
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from adapters import AdapterRegistry
from contracts.domain import WorkflowEdgeContract, WorkflowNodeContract, WorkflowRequest
from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from registry.native_runtime_profile_sync import resolve_native_runtime_profile_config
from runtime.admission_evidence import (
    AdmissionEvidenceRecord,
    persist_admission_evidence,
)
from runtime.execution_strategy import StepCompiler
from runtime.domain import RunState
from runtime.execution.request_building import _workflow_request_payload
from runtime.idempotency import canonical_hash, check_idempotency, record_idempotency
from runtime.intake import WorkflowIntakePlanner
from runtime._workflow_database import resolve_runtime_database_url
from runtime.native_authority import default_native_authority_refs
from runtime.provider_authority import provider_authority_fail
from runtime.workspace_paths import container_workspace_root
from runtime.persistent_evidence import PostgresEvidenceWriter
from runtime.workflow._workflow_execution import (
    WorkflowExecutionContext,
    execute_admitted_workflow_request,
)
from runtime.workflow_graph_compiler import (
    GraphWorkflowCompileError,
    compile_graph_workflow_request,
    spec_uses_graph_runtime,
)
from ._adapter_registry import build_workflow_adapter_registry
from ._shared import (
    _WORKFLOW_REPLAYABLE_RUN_STATES,
    _definition_version_for_hash,
    _json_loads_maybe,
    _normalize_paths,
    _slugify,
    _workflow_id_for_spec,
)
from ._routing import (
    _build_request_envelope,
    _derive_touch_keys,
    _runtime_profile_ref_from_spec,
    _workspace_ref_from_spec,
)
from ._workflow_state import (
    _ensure_workflow_authority,
    _recompute_workflow_run_state,
)
from ._context_building import (
    _build_job_execution_context_shards,
    _build_job_execution_bundles,
    _build_execution_packet,
    _execution_model_messages,
    _render_execution_context_shard,
    _shadow_packet_inspection_from_rows,
    assemble_full_prompt,
    build_platform_context,
)
from runtime.workflow.execution_bundle import render_execution_bundle
from runtime.dynamic_timeout import (
    calculate_timeout_seconds,
    max_complexity_tier,
)
from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.queue_admission import (
    DEFAULT_QUEUE_CRITICAL_THRESHOLD,
    QueueAdmissionGate,
)
from runtime.workflow.job_runtime_context import persist_workflow_job_runtime_contexts
from runtime.workflow.submission_capture import WorkflowSubmissionServiceError
from storage.postgres.validators import PostgresConfigurationError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = [
    "IdempotencyConflict",
    "_retry_packet_reuse_provenance",
    "load_execution_packets",
    "preview_workflow_execution",
    "submit_workflow",
    "submit_workflow_inline",
]


class IdempotencyConflict(Exception):
    def __init__(
        self,
        idempotency_key: str,
        existing_run_id: str | None,
        first_seen_at: datetime | None,
    ) -> None:
        super().__init__(f"Idempotency conflict: key={idempotency_key} exists with different payload")
        self.idempotency_key = idempotency_key
        self.existing_run_id = existing_run_id
        self.first_seen_at = first_seen_at


def _enforce_queue_admission(
    conn: SyncPostgresConnection,
    *,
    job_count: int = 1,
    critical_threshold: int = DEFAULT_QUEUE_CRITICAL_THRESHOLD,
) -> int:
    """Fail closed when admitting more jobs would overflow the queue."""
    decision = QueueAdmissionGate(critical_threshold=critical_threshold).check_connection(
        conn,
        job_count=job_count,
    )
    if not decision.admitted:
        message = f"queue admission rejected: {decision.reason}"
        logger.warning(message)
        raise RuntimeError(message)
    return decision.queue_depth


def _default_workspace_ref() -> str:
    return default_native_authority_refs()[0]


def _default_runtime_profile_ref() -> str:
    return default_native_authority_refs()[1]


def _run_async(coro):
    try:
        import asyncio

        asyncio.get_running_loop()
    except RuntimeError:
        import asyncio

        return run_sync_safe(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        import asyncio

        return pool.submit(asyncio.run, coro).result()


def _submit_transaction(conn: SyncPostgresConnection):
    transaction = getattr(conn, "transaction", None)
    if callable(transaction):
        return transaction()
    return nullcontext(conn)


def _graph_request_envelope(request: WorkflowRequest) -> dict[str, object]:
    workflow_definition_id = request.workflow_definition_id
    definition_version = _definition_version_for_hash(request.definition_hash)
    return {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": definition_version,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
        "nodes": [
            {
                "workflow_definition_node_id": f"{workflow_definition_id}:{node.node_id}",
                "workflow_definition_id": workflow_definition_id,
                "node_id": node.node_id,
                "node_type": node.node_type,
                "schema_version": request.schema_version,
                "adapter_type": node.adapter_type,
                "display_name": node.display_name,
                "inputs": dict(node.inputs),
                "expected_outputs": dict(node.expected_outputs),
                "success_condition": dict(node.success_condition),
                "failure_behavior": dict(node.failure_behavior),
                "authority_requirements": dict(node.authority_requirements),
                "execution_boundary": dict(node.execution_boundary),
                "position_index": node.position_index,
                "template_owner_node_id": node.template_owner_node_id,
            }
            for node in request.nodes
        ],
        "edges": [
            {
                "workflow_definition_edge_id": f"{workflow_definition_id}:{edge.edge_id}",
                "workflow_definition_id": workflow_definition_id,
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "schema_version": request.schema_version,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": dict(edge.release_condition),
                "payload_mapping": dict(edge.payload_mapping),
                "position_index": edge.position_index,
                "template_owner_node_id": edge.template_owner_node_id,
            }
            for edge in request.edges
        ],
    }


def _graph_request_from_envelope(payload: Mapping[str, object]) -> WorkflowRequest:
    raw_nodes = payload.get("nodes")
    raw_edges = payload.get("edges")
    if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
        raise RuntimeError("graph runtime request_envelope is missing nodes/edges arrays")
    nodes = tuple(
        WorkflowNodeContract(
            node_id=str(item["node_id"]),
            node_type=str(item["node_type"]),
            adapter_type=str(item["adapter_type"]),
            display_name=str(item["display_name"]),
            inputs=dict(item.get("inputs") or {}),
            expected_outputs=dict(item.get("expected_outputs") or {}),
            success_condition=dict(item.get("success_condition") or {}),
            failure_behavior=dict(item.get("failure_behavior") or {}),
            authority_requirements=dict(item.get("authority_requirements") or {}),
            execution_boundary=dict(item.get("execution_boundary") or {}),
            position_index=int(item["position_index"]),
            template_owner_node_id=(
                str(item["template_owner_node_id"])
                if item.get("template_owner_node_id") is not None
                else None
            ),
        )
        for item in raw_nodes
        if isinstance(item, Mapping)
    )
    edges = tuple(
        WorkflowEdgeContract(
            edge_id=str(item["edge_id"]),
            edge_type=str(item["edge_type"]),
            from_node_id=str(item["from_node_id"]),
            to_node_id=str(item["to_node_id"]),
            release_condition=dict(item.get("release_condition") or {}),
            payload_mapping=dict(item.get("payload_mapping") or {}),
            position_index=int(item["position_index"]),
            template_owner_node_id=(
                str(item["template_owner_node_id"])
                if item.get("template_owner_node_id") is not None
                else None
            ),
        )
        for item in raw_edges
        if isinstance(item, Mapping)
    )
    return WorkflowRequest(
        schema_version=int(payload["schema_version"]),
        workflow_id=str(payload["workflow_id"]),
        request_id=str(payload["request_id"]),
        workflow_definition_id=str(payload["workflow_definition_id"]),
        definition_hash=str(payload["definition_hash"]),
        workspace_ref=str(payload["workspace_ref"]),
        runtime_profile_ref=str(payload["runtime_profile_ref"]),
        nodes=nodes,
        edges=edges,
    )


def _graph_execution_identity(request: WorkflowRequest) -> tuple[str, str | None, str]:
    for node in request.nodes:
        provider_slug = str(node.inputs.get("provider_slug") or "").strip()
        if provider_slug:
            model_slug = str(node.inputs.get("model_slug") or "").strip() or None
            return provider_slug, model_slug, node.adapter_type
    return "openai", None, "graph_runtime"


def _graph_packet_jobs(
    spec_dict: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    raw_jobs = spec_dict.get("jobs")
    if not isinstance(raw_jobs, list):
        return ()
    return tuple(item for item in raw_jobs if isinstance(item, Mapping))


def _graph_packet_source_kind(packet_provenance: Mapping[str, object] | None) -> str:
    value = str((packet_provenance or {}).get("source_kind") or "").strip()
    return value or "inline_submit"


def _graph_packet_definition_revision(
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
    packet_provenance: Mapping[str, object] | None,
) -> str:
    provenance = packet_provenance or {}
    compiled_spec_row = (
        provenance.get("compiled_spec_row")
        if isinstance(provenance.get("compiled_spec_row"), Mapping)
        else {}
    )
    definition_row = (
        provenance.get("definition_row")
        if isinstance(provenance.get("definition_row"), Mapping)
        else {}
    )
    for candidate in (
        compiled_spec_row.get("definition_revision"),
        definition_row.get("definition_revision"),
        spec_dict.get("definition_revision"),
        request.workflow_definition_id,
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    raise RuntimeError("graph execution packet requires a definition_revision")


def _graph_packet_plan_revision(
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
    packet_provenance: Mapping[str, object] | None,
) -> str:
    provenance = packet_provenance or {}
    compiled_spec_row = (
        provenance.get("compiled_spec_row")
        if isinstance(provenance.get("compiled_spec_row"), Mapping)
        else {}
    )
    for candidate in (
        compiled_spec_row.get("plan_revision"),
        spec_dict.get("plan_revision"),
        request.workflow_definition_id,
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    raise RuntimeError("graph execution packet requires a plan_revision")


def _graph_packet_model_messages(
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    for job in _graph_packet_jobs(spec_dict):
        prompt = str(job.get("prompt") or "")
        system_prompt = str(job.get("system_prompt") or "").strip()
        if not prompt and not system_prompt:
            continue
        job_messages: list[dict[str, str]] = []
        if system_prompt:
            job_messages.append({"role": "system", "content": system_prompt})
        if prompt:
            job_messages.append({"role": "user", "content": prompt})
        if not job_messages:
            continue
        messages.append(
            {
                "job_label": str(job.get("label") or "job"),
                "agent_slug": str(job.get("agent") or "").strip(),
                "messages": job_messages,
            }
        )
    if messages:
        return messages

    for node in request.nodes:
        prompt = str(node.inputs.get("prompt") or "")
        system_prompt = str(node.inputs.get("system_prompt") or "").strip()
        if not prompt and not system_prompt:
            continue
        node_messages: list[dict[str, str]] = []
        if system_prompt:
            node_messages.append({"role": "system", "content": system_prompt})
        if prompt:
            node_messages.append({"role": "user", "content": prompt})
        if not node_messages:
            continue
        provider_slug = str(node.inputs.get("provider_slug") or "").strip()
        model_slug = str(node.inputs.get("model_slug") or "").strip()
        agent_slug = str(node.inputs.get("agent_slug") or "").strip()
        if not agent_slug and provider_slug and model_slug:
            agent_slug = f"{provider_slug}/{model_slug}"
        messages.append(
            {
                "job_label": node.display_name,
                "agent_slug": agent_slug,
                "messages": node_messages,
            }
        )
    return messages


def _graph_packet_reference_bindings(
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
) -> list[dict[str, object]]:
    bindings: list[dict[str, object]] = []
    for job in _graph_packet_jobs(spec_dict):
        prompt = str(job.get("prompt") or "")
        if not prompt:
            continue
        agent_slug = str(job.get("agent") or "").strip()
        route_candidates = [agent_slug] if agent_slug else []
        bindings.append(
            {
                "job_label": str(job.get("label") or "job"),
                "agent_slug": agent_slug,
                "depends_on": [
                    str(item).strip()
                    for item in (job.get("depends_on") or [])
                    if str(item).strip()
                ],
                "prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16],
                "route_task_type": str(job.get("task_type") or "general").strip() or "general",
                "route_origin_slug": agent_slug,
                "route_candidates": route_candidates,
            }
        )
    if bindings:
        return bindings

    inbound_edges: dict[str, list[str]] = {}
    for edge in request.edges:
        inbound_edges.setdefault(edge.to_node_id, []).append(edge.from_node_id)
    for node in request.nodes:
        prompt = str(node.inputs.get("prompt") or "")
        if not prompt:
            continue
        provider_slug = str(node.inputs.get("provider_slug") or "").strip()
        model_slug = str(node.inputs.get("model_slug") or "").strip()
        agent_slug = str(node.inputs.get("agent_slug") or "").strip()
        if not agent_slug and provider_slug and model_slug:
            agent_slug = f"{provider_slug}/{model_slug}"
        route_candidates = [agent_slug] if agent_slug else []
        bindings.append(
            {
                "job_label": node.display_name,
                "agent_slug": agent_slug,
                "depends_on": inbound_edges.get(node.node_id, []),
                "prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16],
                "route_task_type": str(node.inputs.get("task_type") or "general").strip() or "general",
                "route_origin_slug": node.adapter_type,
                "route_candidates": route_candidates,
            }
        )
    return bindings


def _graph_packet_capability_bindings(
    *,
    spec_dict: Mapping[str, object],
) -> list[dict[str, object]]:
    bindings: list[dict[str, object]] = []
    for job in _graph_packet_jobs(spec_dict):
        capabilities = [
            str(item).strip()
            for item in (job.get("capabilities") or [])
            if str(item).strip()
        ]
        if not capabilities:
            continue
        agent_slug = str(job.get("agent") or "").strip()
        route_candidates = [agent_slug] if agent_slug else []
        bindings.append(
            {
                "job_label": str(job.get("label") or "job"),
                "agent_slug": agent_slug,
                "route_task_type": str(job.get("task_type") or "general").strip() or "general",
                "capabilities": capabilities,
                "route_candidates": route_candidates,
            }
        )
    return bindings


def _graph_packet_verify_refs(
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
) -> list[str]:
    refs: list[str] = []
    for job in _graph_packet_jobs(spec_dict):
        refs.extend(
            str(item).strip()
            for item in (job.get("verify_refs") or [])
            if str(item).strip()
        )
    if refs:
        return list(dict.fromkeys(refs))
    for node in request.nodes:
        if node.adapter_type != "verifier":
            continue
        refs.extend(
            str(item).strip()
            for item in (node.inputs.get("bindings") or [])
            if str(item).strip()
        )
    return list(dict.fromkeys(refs))


def _build_graph_execution_packet(
    conn: SyncPostgresConnection,
    *,
    request: WorkflowRequest,
    spec_dict: Mapping[str, object],
    packet_provenance: Mapping[str, object] | None,
) -> dict[str, object]:
    provenance = packet_provenance or {}
    request_payload = _workflow_request_payload(request)
    source_kind = _graph_packet_source_kind(provenance)
    definition_revision = _graph_packet_definition_revision(
        request=request,
        spec_dict=spec_dict,
        packet_provenance=provenance,
    )
    plan_revision = _graph_packet_plan_revision(
        request=request,
        spec_dict=spec_dict,
        packet_provenance=provenance,
    )
    model_messages = _graph_packet_model_messages(request=request, spec_dict=spec_dict)
    reference_bindings = _graph_packet_reference_bindings(
        request=request,
        spec_dict=spec_dict,
    )
    capability_bindings = _graph_packet_capability_bindings(spec_dict=spec_dict)
    verify_refs = _graph_packet_verify_refs(request=request, spec_dict=spec_dict)
    provenance_file_inputs = (
        provenance.get("file_inputs")
        if isinstance(provenance.get("file_inputs"), Mapping)
        else {}
    )
    reuse_file_inputs = json.loads(
        json.dumps(
            {
                **dict(provenance_file_inputs),
                "spec_snapshot": dict(spec_dict),
            },
            default=str,
        )
    )
    reuse_authority_inputs = json.loads(
        json.dumps(
            {
                "workflow_definition": {
                    "workflow_definition_id": request.workflow_definition_id,
                    "definition_hash": request.definition_hash,
                },
                "workflow_row": (
                    dict(provenance.get("workflow_row"))
                    if isinstance(provenance.get("workflow_row"), Mapping)
                    else {"id": request.workflow_id}
                ),
                "definition_row": (
                    dict(provenance.get("definition_row"))
                    if isinstance(provenance.get("definition_row"), Mapping)
                    else {"definition_revision": definition_revision}
                ),
                "compiled_spec_row": (
                    dict(provenance.get("compiled_spec_row"))
                    if isinstance(provenance.get("compiled_spec_row"), Mapping)
                    else {
                        "definition_revision": definition_revision,
                        "plan_revision": plan_revision,
                    }
                ),
            },
            default=str,
        )
    )
    file_inputs = json.loads(
        json.dumps(
            {
                **dict(provenance_file_inputs),
                "spec_snapshot": dict(spec_dict),
                "workflow_request": request_payload,
            },
            default=str,
        )
    )
    authority_inputs = json.loads(
        json.dumps(
            {
                "workflow_request": request_payload,
                "workflow_definition": {
                    "workflow_definition_id": request.workflow_definition_id,
                    "definition_hash": request.definition_hash,
                },
                "workflow_row": (
                    dict(provenance.get("workflow_row"))
                    if isinstance(provenance.get("workflow_row"), Mapping)
                    else {"id": request.workflow_id}
                ),
                "definition_row": (
                    dict(provenance.get("definition_row"))
                    if isinstance(provenance.get("definition_row"), Mapping)
                    else {"definition_revision": definition_revision}
                ),
                "compiled_spec_row": (
                    dict(provenance.get("compiled_spec_row"))
                    if isinstance(provenance.get("compiled_spec_row"), Mapping)
                    else {
                        "definition_revision": definition_revision,
                        "plan_revision": plan_revision,
                    }
                ),
            },
            default=str,
        )
    )
    packet_payload: dict[str, object] = {
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "packet_revision_authority": {
            "kind": "execution_packet_revision_authority",
            "provenance_kind": "compiled",
            "definition_revision": definition_revision,
            "plan_revision": plan_revision,
            "synthetic_fields": [],
            "reason_code": "packet.revision.compiled",
            "workflow_id": request.workflow_id,
            "run_id": request.request_id,
        },
        "packet_version": 1,
        "workflow_id": request.workflow_id,
        "run_id": request.request_id,
        "spec_name": str(spec_dict.get("name") or request.workflow_id or "inline"),
        "source_kind": source_kind,
        "authority_refs": [definition_revision, plan_revision],
        "model_messages": model_messages,
        "reference_bindings": reference_bindings,
        "capability_bindings": capability_bindings,
        "verify_refs": verify_refs,
        "authority_inputs": authority_inputs,
        "file_inputs": file_inputs,
        "compile_provenance": {
            "artifact_kind": "packet_lineage",
            "input_fingerprint": "",
            "surface_revision": "workflow_graph_runtime.packet_submit",
            "definition_revision": definition_revision,
            "plan_revision": plan_revision,
            "workflow_id": request.workflow_id,
            "spec_name": str(spec_dict.get("name") or request.workflow_id or "inline"),
            "source_kind": source_kind,
            "file_inputs": reuse_file_inputs,
            "authority_inputs": reuse_authority_inputs,
        },
    }
    compile_provenance = dict(packet_payload["compile_provenance"])
    compile_input_payload = {
        "artifact_kind": compile_provenance["artifact_kind"],
        "surface_revision": compile_provenance["surface_revision"],
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "workflow_id": request.workflow_id,
        "spec_name": str(packet_payload["spec_name"]),
        "source_kind": source_kind,
        "model_messages": model_messages,
        "reference_bindings": reference_bindings,
        "capability_bindings": capability_bindings,
        "verify_refs": verify_refs,
        "file_inputs": compile_provenance["file_inputs"],
        "authority_inputs": compile_provenance["authority_inputs"],
    }
    compile_provenance["input_fingerprint"] = canonical_hash(compile_input_payload)
    packet_payload["compile_provenance"] = compile_provenance
    try:
        return CompileArtifactStore(conn).persist_execution_packet_with_reuse(
            packet=packet_payload,
            authority_refs=[definition_revision, plan_revision],
            parent_artifact_ref=plan_revision,
        )
    except Exception as exc:
        raise RuntimeError(
            f"workflow packet lineage reuse failed closed: {exc}"
        ) from exc


def _persist_graph_submission_evidence(
    *,
    evidence_writer: PostgresEvidenceWriter,
    intake_outcome,
    request: WorkflowRequest,
) -> None:
    persist_admission_evidence(
        evidence_writer,
        admission=AdmissionEvidenceRecord(
            route_identity=intake_outcome.route_identity,
            request_payload=_workflow_request_payload(request),
            admitted_definition_ref=(
                intake_outcome.admitted_definition_ref or request.workflow_definition_id
            ),
            admitted_definition_hash=(
                intake_outcome.admitted_definition_hash or request.definition_hash
            ),
            current_state=RunState(intake_outcome.current_state.value),
            reason_code=intake_outcome.admission_decision.reason_code,
            decided_at=intake_outcome.admission_decision.decided_at,
            validation_result_ref=intake_outcome.validation_result.validation_result_ref,
            authority_context_ref=intake_outcome.admission_decision.authority_context_ref,
            admission_decision_id=intake_outcome.admission_decision.admission_decision_id,
            request_digest=intake_outcome.request_digest,
        ),
    )


def _execute_admitted_graph_run(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
) -> object:
    lock_key = _graph_run_lock_key(run_id)
    with conn.transaction() as graph_conn:
        lock_rows = graph_conn.execute(
            "SELECT pg_try_advisory_xact_lock($1::bigint) AS locked",
            lock_key,
        )
        locked = bool(lock_rows and bool(dict(lock_rows[0]).get("locked")))
        if not locked:
            return {
                "run_id": run_id,
                "status": "locked",
                "reason_code": "workflow.graph_run_already_locked",
            }
        return _execute_admitted_graph_run_locked(graph_conn, run_id=run_id)


def _graph_run_lock_key(run_id: str) -> int:
    digest = hashlib.sha256(f"workflow_graph_run:{run_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _execute_admitted_graph_run_locked(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
) -> object:
    rows = conn.execute(
        """SELECT request_envelope, current_state, context_bundle_id, authority_context_digest
           FROM workflow_runs
           WHERE run_id = $1""",
        run_id,
    )
    if not rows:
        raise RuntimeError(f"graph runtime run {run_id!r} is missing workflow_runs authority")
    row = dict(rows[0])
    current_state = str(row.get("current_state") or "").strip()
    if current_state != RunState.CLAIM_ACCEPTED.value:
        return {"run_id": run_id, "status": current_state or "unknown"}
    request_envelope = row.get("request_envelope")
    if not isinstance(request_envelope, Mapping):
        request_envelope = _json_loads_maybe(request_envelope, {}) or {}
    if not isinstance(request_envelope, Mapping):
        raise RuntimeError(f"graph runtime run {run_id!r} has invalid request_envelope")
    request = _graph_request_from_envelope(request_envelope)
    registry = _graph_registry_from_authority(conn, request)
    intake_outcome = WorkflowIntakePlanner(registry=registry).plan(request=request)
    if intake_outcome.run_id != run_id:
        raise RuntimeError(
            f"graph runtime run {run_id!r} does not match reconstructed intake {intake_outcome.run_id!r}"
        )
    persisted_context_ref = str(row.get("context_bundle_id") or "").strip()
    persisted_context_digest = str(row.get("authority_context_digest") or "").strip()
    if persisted_context_ref and persisted_context_digest:
        persisted_bundle_rows = conn.execute(
            """
            SELECT context_bundle_id, bundle_hash, bundle_payload, workspace_ref
            FROM context_bundles
            WHERE context_bundle_id = $1
            """,
            persisted_context_ref,
        )
        persisted_authority_context = intake_outcome.authority_context
        if persisted_bundle_rows and persisted_authority_context is not None:
            bundle_row = dict(persisted_bundle_rows[0])
            persisted_bundle_payload = bundle_row.get("bundle_payload")
            if not isinstance(persisted_bundle_payload, Mapping):
                persisted_bundle_payload = _json_loads_maybe(persisted_bundle_payload, {}) or {}
            persisted_authority_context = replace(
                persisted_authority_context,
                context_bundle_id=str(bundle_row.get("context_bundle_id") or persisted_context_ref),
                bundle_hash=str(bundle_row.get("bundle_hash") or persisted_context_digest),
                bundle_payload=persisted_bundle_payload,
                workspace_ref=str(bundle_row.get("workspace_ref") or persisted_authority_context.workspace_ref),
            )
        intake_outcome = replace(
            intake_outcome,
            authority_context=persisted_authority_context,
            route_identity=replace(
                intake_outcome.route_identity,
                authority_context_ref=persisted_context_ref,
                authority_context_digest=persisted_context_digest,
            ),
        )
    database_url = resolve_runtime_database_url(required=True)
    evidence_writer = PostgresEvidenceWriter(database_url=database_url)
    provider_slug, model_slug, adapter_type = _graph_execution_identity(request)
    context = WorkflowExecutionContext(
        provider_slug=provider_slug,
        model_slug=model_slug,
        adapter_type=adapter_type,
        started_at=datetime.now(timezone.utc),
        start_ns=time.monotonic_ns(),
    )
    try:
        execution_result, failure = execute_admitted_workflow_request(
            intake_outcome=intake_outcome,
            adapter_registry=_graph_adapter_registry(request),
            evidence_writer=evidence_writer,
            context=context,
            timeout=_graph_runtime_timeout_seconds(conn, spec_dict={"name": request.workflow_id}),
        )
    finally:
        evidence_writer.close_blocking()
    return failure or execution_result


def _graph_registry_from_authority(
    conn: SyncPostgresConnection,
    request: WorkflowRequest,
) -> RegistryResolver:
    """Load the admitted graph registry from durable registry authority rows."""

    workspace_ref = str(request.workspace_ref or "").strip()
    runtime_profile_ref = str(request.runtime_profile_ref or "").strip()
    if not workspace_ref or workspace_ref.lower() == "none":
        workspace_ref = _default_workspace_ref()
    if not runtime_profile_ref or runtime_profile_ref.lower() == "none":
        runtime_profile_ref = _default_runtime_profile_ref()
    workspace_rows = conn.execute(
        """
        SELECT workspace_ref, repo_root, workdir
        FROM registry_workspace_authority
        WHERE workspace_ref = $1
        ORDER BY workspace_ref
        """,
        workspace_ref,
    )
    runtime_profile_rows = conn.execute(
        """
        SELECT runtime_profile_ref, model_profile_id, provider_policy_id, sandbox_profile_ref
        FROM registry_runtime_profile_authority
        WHERE runtime_profile_ref = $1
        ORDER BY runtime_profile_ref
        """,
        runtime_profile_ref,
    )
    if not workspace_rows or not runtime_profile_rows:
        return _graph_registry_for_request(request)
    workspace_records = [
        WorkspaceAuthorityRecord(
            workspace_ref=str(row["workspace_ref"]),
            # Graph runtime evidence must rebuild the same admitted context
            # hash in the host CLI and docker worker. Physical paths from the
            # DB row are execution materialization details, not bundle identity.
            repo_root=str(row["workspace_ref"]),
            workdir=str(row["workspace_ref"]),
        )
        for row in workspace_rows
    ]
    runtime_profile_records = [
        RuntimeProfileAuthorityRecord(
            runtime_profile_ref=str(row["runtime_profile_ref"]),
            model_profile_id=str(row["model_profile_id"]),
            provider_policy_id=str(row["provider_policy_id"]),
            sandbox_profile_ref=str(row["sandbox_profile_ref"] or ""),
        )
        for row in runtime_profile_rows
    ]
    return RegistryResolver(
        workspace_records={workspace_ref: workspace_records},
        runtime_profile_records={runtime_profile_ref: runtime_profile_records},
    )


def _graph_registry_for_request(request: WorkflowRequest) -> RegistryResolver:
    runtime_profile_ref = request.runtime_profile_ref or _default_runtime_profile_ref()
    config = resolve_native_runtime_profile_config(runtime_profile_ref)
    workspace_ref = request.workspace_ref or config.workspace_ref or _default_workspace_ref()
    # The bundle_hash must be stable across admission (host CLI) and execution
    # (docker worker). repo_root/workdir resolve to different absolute paths on
    # different hosts even when they point at the same logical workspace, which
    # would otherwise cause route_identity.authority_context_digest to diverge
    # mid-run. Use the workspace_ref itself as the canonical path identity —
    # actual filesystem paths come from NativeRuntimeProfileConfig at the point
    # of execution, not from this authority record.
    return RegistryResolver(
        workspace_records={
            workspace_ref: [
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=workspace_ref,
                    workdir=workspace_ref,
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


def _graph_adapter_registry(request: WorkflowRequest) -> AdapterRegistry:
    adapter_types = {str(node.adapter_type).strip() for node in request.nodes if str(node.adapter_type).strip()}
    return build_workflow_adapter_registry(
        adapter_types=adapter_types,
        shadow_packet_config=None,
    )


def _graph_runtime_history_p95_seconds(
    conn: SyncPostgresConnection,
    *,
    spec_name: str,
    limit: int = 50,
) -> float | None:
    rows = conn.execute(
        """
        SELECT started_at, finished_at
        FROM workflow_runs
        WHERE COALESCE(request_envelope->>'name', '') = $1
          AND started_at IS NOT NULL
          AND finished_at IS NOT NULL
        ORDER BY requested_at DESC
        LIMIT $2
        """,
        spec_name,
        max(1, int(limit)),
    )
    durations = sorted(
        max((row["finished_at"] - row["started_at"]).total_seconds(), 0.0)
        for row in rows
        if isinstance(row["started_at"], datetime) and isinstance(row["finished_at"], datetime)
    )
    if not durations:
        return None
    index = min(int(len(durations) * 0.95), len(durations) - 1)
    return durations[index]


def _graph_runtime_timeout_seconds(
    conn: SyncPostgresConnection,
    *,
    spec_dict: Mapping[str, object],
) -> int:
    spec_name = str(spec_dict.get("name") or "inline").strip() or "inline"
    raw_jobs = spec_dict.get("jobs")
    job_complexities: list[object] = []
    if isinstance(raw_jobs, list):
        for job in raw_jobs:
            if isinstance(job, Mapping):
                job_complexities.append(job.get("complexity"))

    explicit_timeout = spec_dict.get("timeout")
    base_timeout = 900
    explicit_timeout_provided = False
    if explicit_timeout is not None:
        try:
            base_timeout = int(explicit_timeout)
            explicit_timeout_provided = True
        except (TypeError, ValueError):
            base_timeout = 900

    historical_p95_seconds = _graph_runtime_history_p95_seconds(conn, spec_name=spec_name)
    computed_timeout = calculate_timeout_seconds(
        spec_name,
        max_complexity_tier(job_complexities),
        default_timeout=base_timeout,
        historical_p95_seconds=historical_p95_seconds,
    )
    if explicit_timeout_provided:
        return max(base_timeout, computed_timeout)
    return computed_timeout


def _inline_spec_object(spec_dict: dict[str, object]):
    """Build the lightweight spec object consumed by admission helpers."""

    return type(
        "InlineSpec",
        (),
        {
            "name": spec_dict.get("name", "inline"),
            "workflow_id": spec_dict.get("workflow_id", "workflow.inline"),
            "phase": spec_dict.get("phase", "build"),
            "jobs": spec_dict.get("jobs", []),
            "outcome_goal": spec_dict.get("outcome_goal", ""),
            "output_dir": spec_dict.get("output_dir", ""),
            "workspace_ref": spec_dict.get("workspace_ref"),
            "runtime_profile_ref": spec_dict.get("runtime_profile_ref"),
            "_raw": spec_dict,
        },
    )()


def _preview_spec_ref(
    raw_snapshot: Mapping[str, object],
    *,
    spec,
    field_name: str,
) -> str | None:
    """Resolve an execution-lane ref only when it is explicit in the spec."""

    value = raw_snapshot.get(field_name)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized

    attr = getattr(spec, field_name, None)
    if isinstance(attr, str):
        normalized = attr.strip()
        if normalized:
            return normalized

    return None


def _apply_write_scope_auto_dependencies(spec) -> None:
    """Populate depends_on from declared write scope when the authority is explicit."""

    scope = {"write_scope": [], "read_scope": {}}
    file_to_job_labels: dict[str, set[str]] = {}
    for job in spec.jobs:
        label = job.get("label")
        write_scope = job.get("write_scope")
        if isinstance(write_scope, str):
            write_scope = [write_scope]
        if not write_scope:
            scoped_write = (job.get("scope") or {}).get("write", [])
            write_scope = [scoped_write] if isinstance(scoped_write, str) else scoped_write
        read_scope = job.get("read_scope")
        if isinstance(read_scope, str):
            read_scope = [read_scope]
        for path in write_scope or []:
            if not path:
                continue
            scope["write_scope"].append({"path": path, "action": "modify"})
            file_to_job_labels.setdefault(path, set()).add(label)
            if isinstance(read_scope, dict):
                path_reads = read_scope.get(path, [])
                if isinstance(path_reads, str):
                    path_reads = [path_reads]
                if path_reads:
                    scope["read_scope"][path] = list(path_reads)
            elif read_scope:
                scope["read_scope"][path] = list(read_scope)

    if not scope["write_scope"]:
        return

    try:
        compiler = StepCompiler()
        plan = compiler.compile(scope)
    except Exception as exc:
        raise RuntimeError(
            f"workflow submit failed closed while resolving write-scope authority: {exc}",
        ) from exc

    step_id_to_path = {step.step_id: step.file_path for step in plan.steps}
    # Spec order is authoritative: auto-derived edges must flow forward in
    # declared order so a later-indexed job can never be injected as a
    # parent of an earlier-indexed one. Without this guard the StepCompiler's
    # file-level DAG (which has no concept of job order) silently produced
    # reverse-direction edges that created cycles in workflow_job_edges,
    # which stalled admission forever (every ready_at stayed NULL).
    job_order_index = {
        job.get("label"): idx for idx, job in enumerate(spec.jobs) if job.get("label")
    }
    auto_deps: dict[str, set[str]] = {}
    for step in plan.steps:
        child_labels = file_to_job_labels.get(step.file_path, set())
        if not child_labels or not step.depends_on:
            continue
        for dep_step_id in step.depends_on:
            dep_path = step_id_to_path.get(dep_step_id)
            if not dep_path:
                continue
            for parent_label in file_to_job_labels.get(dep_path, set()):
                for child_label in child_labels:
                    if parent_label == child_label:
                        continue
                    parent_idx = job_order_index.get(parent_label)
                    child_idx = job_order_index.get(child_label)
                    if parent_idx is None or child_idx is None:
                        continue
                    if parent_idx >= child_idx:
                        # Reverse-direction edge — spec author already
                        # ordered these; skip to preserve acyclicity.
                        continue
                    auto_deps.setdefault(child_label, set()).add(parent_label)

    # Only auto-populate depends_on when the author did not declare any.
    # Treat an explicit empty list `depends_on: []` as "I have no parents
    # and I do not want the auto-dep heuristic" — distinguishing it from
    # a missing key. Previously `not []` was True, so explicit-empty still
    # triggered the heuristic.
    for job in spec.jobs:
        label = job.get("label")
        if label in auto_deps and job.get("depends_on") is None:
            job["depends_on"] = sorted(auto_deps[label])


def _preview_route_payload(job: Mapping[str, object]) -> dict[str, object]:
    adapter_type = str(job.get("adapter_type") or "").strip().lower()
    requested_agent = str(job.get("agent") or "").strip()
    if adapter_type and adapter_type not in {"cli_llm", "llm_task"}:
        return {
            "requested_agent": requested_agent or None,
            "resolved_agent": None,
            "route_status": "not_applicable",
        }
    requested_agent = requested_agent or "auto/build"
    route_plan = job.get("_route_plan")
    if route_plan is not None:
        return {
            "requested_agent": str(getattr(route_plan, "original_slug", "") or requested_agent),
            "resolved_agent": str(getattr(route_plan, "primary", "") or requested_agent),
            "route_status": "resolved",
        }
    if requested_agent.startswith("auto/"):
        return {
            "requested_agent": requested_agent,
            "resolved_agent": None,
            "route_status": "unresolved",
            "route_reason": (
                "task_type_router could not resolve this auto/* slug — "
                "submit would reject for the same reason"
            ),
        }
    return {
        "requested_agent": requested_agent,
        "resolved_agent": requested_agent,
        "route_status": "explicit",
    }


def preview_workflow_execution(
    conn: SyncPostgresConnection,
    *,
    spec_path: str | None = None,
    inline_spec: Mapping[str, object] | None = None,
    repo_root: str | None = None,
) -> dict[str, object]:
    """Build the exact worker-facing execution payload without submitting a run."""

    if bool(spec_path) == bool(inline_spec):
        raise ValueError("pass exactly one of spec_path or inline_spec")

    preview_repo_root = str(repo_root or os.getcwd()).strip() or os.getcwd()
    preview_source: str
    resolved_spec_path: str | None = None
    if spec_path is not None:
        from runtime.workflow_spec import WorkflowSpec

        resolved_path = Path(spec_path)
        if not resolved_path.is_absolute():
            resolved_path = Path(preview_repo_root) / resolved_path
        resolved_spec_path = str(resolved_path)
        spec = WorkflowSpec.load(resolved_spec_path)
        raw_snapshot = dict(getattr(spec, "_raw", {}) or {})
        preview_source = "spec_path"
    else:
        raw_snapshot = json.loads(json.dumps(dict(inline_spec or {}), default=str))
        spec = _inline_spec_object(raw_snapshot)
        preview_source = "inline_spec"

    _apply_write_scope_auto_dependencies(spec)

    provenance = {
        "source_kind": f"{preview_source}_preview",
        "repo_root": preview_repo_root,
        "spec_path": resolved_spec_path,
        "file_inputs": raw_snapshot,
    }
    # Preview must not consult native authority defaults. It only reflects the
    # refs that are already explicit on the spec, so the lane stays DB-optional.
    runtime_profile_ref = _preview_spec_ref(raw_snapshot, spec=spec, field_name="runtime_profile_ref")
    workspace_ref = _preview_spec_ref(raw_snapshot, spec=spec, field_name="workspace_ref")
    execution_context_shards = _build_job_execution_context_shards(
        conn=conn,
        spec=spec,
        raw_snapshot=raw_snapshot,
        provenance=provenance,
    )
    execution_bundles = _build_job_execution_bundles(
        conn=conn,
        spec=spec,
        raw_snapshot=raw_snapshot,
        execution_context_shards=execution_context_shards,
        run_id=None,
        workflow_id=str(getattr(spec, "workflow_id", "") or "").strip() or None,
        runtime_profile_ref=runtime_profile_ref,
    )

    spec_verify_refs = _normalize_paths(raw_snapshot.get("verify_refs"))
    warnings: list[str] = []

    auto_llm_jobs = [
        str(job.get("label") or job.get("agent") or "").strip()
        for job in spec.jobs
        if str(job.get("agent") or "auto/build").strip().startswith("auto/")
        and str(job.get("adapter_type") or "").strip().lower()
        in {"", "cli_llm", "llm_task"}
    ]
    if auto_llm_jobs and not runtime_profile_ref:
        warnings.append(
            "task_type_router skipped: runtime_profile_ref is required to "
            "resolve auto/* routes in preview; refusing to route against "
            "the global provider candidate catalog"
        )
    else:
        try:
            from runtime.task_type_router import TaskTypeRouter

            TaskTypeRouter(conn).resolve_spec_jobs(
                spec.jobs,
                runtime_profile_ref=runtime_profile_ref or None,
            )
        except Exception as exc:
            if runtime_profile_ref:
                raise RuntimeError(
                    f"workflow preview failed closed for runtime profile "
                    f"{runtime_profile_ref!r}: {exc}",
                ) from exc
            warnings.append(f"task_type_router unavailable: {exc}")

    jobs: list[dict[str, object]] = []
    for index, job in enumerate(spec.jobs):
        label = str(job.get("label") or f"job_{index}")
        route_payload = _preview_route_payload(job)
        route_reason = str(route_payload.get("route_reason") or "").strip()
        if route_reason:
            warnings.append(f"{label}: {route_reason}")
        context_shard = dict(execution_context_shards.get(label) or {})
        execution_bundle = dict(execution_bundles.get(label) or {})
        job_payload = dict(job)
        if context_shard:
            job_payload["_execution_context"] = context_shard
        if execution_bundle:
            job_payload["_execution_bundle"] = execution_bundle
        messages = _execution_model_messages(job_payload)
        rendered_user_prompt = next(
            (
                str(message.get("content") or "")
                for message in messages
                if str(message.get("role") or "").strip().lower() == "user"
            ),
            "",
        )
        rendered_system_prompt = next(
            (
                str(message.get("content") or "")
                for message in messages
                if str(message.get("role") or "").strip().lower() == "system"
            ),
            None,
        )
        # BUG-D3CD86B8: compute the exact backend-bound prompt (prompt +
        # platform_context + shard + bundle) via the shared assembler so
        # preview no longer drifts from execution. rendered_user_prompt keeps
        # the pre-platform-context form for debugging parity; rendered_full_prompt
        # is what the worker actually sends to the backend.
        _preview_platform_context = build_platform_context(preview_repo_root)
        _preview_shard_text = _render_execution_context_shard(context_shard)
        _preview_bundle_text = render_execution_bundle(execution_bundle)
        rendered_full_prompt = assemble_full_prompt(
            prompt=str(job.get("prompt") or ""),
            platform_context=_preview_platform_context,
            execution_context_shard_text=_preview_shard_text,
            execution_bundle_text=_preview_bundle_text,
        )
        job_workdir = str(job.get("workdir") or raw_snapshot.get("workdir") or "").strip()
        if not job_workdir:
            raise ValueError(
                f"preview job {label!r} requires an explicit job.workdir or top-level workdir"
            )
        verify_refs = list(
            dict.fromkeys(
                [
                    *_normalize_paths(context_shard.get("verify_refs")),
                    *_normalize_paths(job.get("verify_refs")),
                    *spec_verify_refs,
                ]
            )
        )
        preview_job: dict[str, object] = {
            "label": label,
            "prompt": str(job.get("prompt") or ""),
            "adapter_type": str(job.get("adapter_type") or "").strip() or None,
            "task_type": (
                str(
                    job.get("task_type")
                    or getattr(job.get("_route_plan"), "task_type", "")
                    or job.get("route_task_type")
                    or ""
                ).strip()
                or None
            ),
            "messages": messages,
            "rendered_user_prompt": rendered_user_prompt,
            "rendered_system_prompt": rendered_system_prompt,
            "rendered_prompt": rendered_user_prompt,
            # BUG-D3CD86B8: backend-bound prompt the worker actually sends.
            "rendered_full_prompt": rendered_full_prompt,
            "rendered_platform_context": _preview_platform_context,
            "execution_context_shard": context_shard,
            "rendered_execution_context_shard": _render_execution_context_shard(context_shard),
            "execution_bundle": execution_bundle,
            "rendered_execution_bundle": render_execution_bundle(execution_bundle),
            "verify_refs": verify_refs,
            "allowed_tools": _normalize_paths(execution_bundle.get("allowed_tools")),
            "mcp_tool_names": _normalize_paths(execution_bundle.get("mcp_tool_names")),
            "skill_refs": _normalize_paths(execution_bundle.get("skill_refs")),
            "completion_contract": dict(execution_bundle.get("completion_contract") or {}),
            "workspace": {
                # Pre-existing fields — host-side values echoed from the spec.
                # Kept for back-compat with callers that read workspace.repo_root.
                "repo_root": preview_repo_root,
                "workdir": job_workdir,
                # BUG-31C147A8: truthful labels for what these values really are.
                "host_repo_root": preview_repo_root,
                "host_workdir": job_workdir,
                # BUG-31C147A8: sharded runs replace repo_root/workdir with
                # materialized_repo_root/materialized_workdir resolved from the
                # fork/worktree binding at execution time. Preview cannot know
                # this binding yet, so mark it explicitly unresolved rather
                # than fabricating a host path that will be wrong inside the
                # worker sandbox.
                "materialized": {
                    "status": "unresolved_until_execution",
                    "note": (
                        "materialized_repo_root and materialized_workdir are resolved from the "
                        "active fork/worktree binding in runtime/workflow/_execution_core.py. "
                        f"For sharded workflows the worker sees paths like {container_workspace_root()}/... — NOT "
                        "the host_repo_root above. Use host_* fields only to understand where "
                        "output/receipts will land on the host."
                    ),
                },
                "workspace_ref": workspace_ref,
                "runtime_profile_ref": runtime_profile_ref,
            },
            **route_payload,
        }
        jobs.append(preview_job)

    return {
        "action": "preview",
        "preview_mode": "execution",
        "preview_source": preview_source,
        "spec_path": resolved_spec_path,
        "spec_snapshot": json.loads(json.dumps(raw_snapshot, default=str)),
        "spec_name": str(getattr(spec, "name", "inline") or "inline"),
        "workflow_id": str(getattr(spec, "workflow_id", "") or "").strip() or None,
        "phase": str(getattr(spec, "phase", "build") or "build"),
        "total_jobs": len(getattr(spec, "jobs", []) or []),
        "workspace": {
            # Pre-existing; host-side value — preserved for back-compat.
            "repo_root": preview_repo_root,
            # BUG-31C147A8: truthful companion fields so callers can tell the
            # host path from the materialized-at-execution path.
            "host_repo_root": preview_repo_root,
            "materialized": {
                "status": "unresolved_until_execution",
                "note": (
                    "materialized_repo_root resolves from the active fork/worktree binding "
                    "at execution time (runtime/workflow/_execution_core.py). Sharded workflows "
                    f"see a different path (for example {container_workspace_root()}) inside the worker sandbox."
                ),
            },
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
        },
        "execution_context_shards": execution_context_shards,
        "execution_bundles": execution_bundles,
        "jobs": jobs,
        "warnings": list(dict.fromkeys(warnings)),
    }


def _persist_graph_authority(
    conn: SyncPostgresConnection,
    *,
    intake_outcome,
    request: WorkflowRequest,
    requested_at: datetime,
) -> None:
    envelope = _graph_request_envelope(request)
    decision = intake_outcome.admission_decision
    definition_hash = intake_outcome.admitted_definition_hash or request.definition_hash
    definition_id = intake_outcome.admitted_definition_ref or request.workflow_definition_id
    request_envelope_json = json.dumps(envelope, default=str)
    now = decision.decided_at

    conn.execute(
        """INSERT INTO workflow_definitions (
               workflow_definition_id, workflow_id, schema_version, definition_version,
               definition_hash, status, request_envelope, normalized_definition, created_at
           ) VALUES ($1, $2, $3, $4, $5, 'active', $6::jsonb, $6::jsonb, $7)
           ON CONFLICT (workflow_definition_id) DO NOTHING""",
        definition_id,
        request.workflow_id,
        request.schema_version,
        envelope["definition_version"],
        definition_hash,
        request_envelope_json,
        requested_at,
    )
    for node in envelope["nodes"]:
        conn.execute(
            """INSERT INTO workflow_definition_nodes (
                   workflow_definition_node_id, workflow_definition_id,
                   node_id, node_type, schema_version, adapter_type,
                   display_name, inputs, expected_outputs, success_condition,
                   failure_behavior, authority_requirements, execution_boundary,
                   position_index
               ) VALUES (
                   $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb,
                   $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14
               )
               ON CONFLICT (workflow_definition_node_id) DO NOTHING""",
            node["workflow_definition_node_id"],
            node["workflow_definition_id"],
            node["node_id"],
            node["node_type"],
            node["schema_version"],
            node["adapter_type"],
            node["display_name"],
            json.dumps(node["inputs"], default=str),
            json.dumps(node["expected_outputs"], default=str),
            json.dumps(node["success_condition"], default=str),
            json.dumps(node["failure_behavior"], default=str),
            json.dumps(node["authority_requirements"], default=str),
            json.dumps(node["execution_boundary"], default=str),
            node["position_index"],
        )
    for edge in envelope["edges"]:
        conn.execute(
            """INSERT INTO workflow_definition_edges (
                   workflow_definition_edge_id, workflow_definition_id,
                   edge_id, edge_type, schema_version, from_node_id, to_node_id,
                   release_condition, payload_mapping, position_index
               ) VALUES (
                   $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10
               )
               ON CONFLICT (workflow_definition_edge_id) DO NOTHING""",
            edge["workflow_definition_edge_id"],
            edge["workflow_definition_id"],
            edge["edge_id"],
            edge["edge_type"],
            edge["schema_version"],
            edge["from_node_id"],
            edge["to_node_id"],
            json.dumps(edge["release_condition"], default=str),
            json.dumps(edge["payload_mapping"], default=str),
            edge["position_index"],
        )
    conn.execute(
        """INSERT INTO admission_decisions (
               admission_decision_id, workflow_id, request_id, decision, reason_code,
               decided_at, decided_by, policy_snapshot_ref, validation_result_ref, authority_context_ref
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           ON CONFLICT (admission_decision_id) DO NOTHING""",
        decision.admission_decision_id,
        request.workflow_id,
        request.request_id,
        decision.decision.value,
        decision.reason_code,
        decision.decided_at,
        decision.decided_by,
        decision.policy_snapshot_ref,
        decision.validation_result_ref,
        decision.authority_context_ref,
    )
    # Accepted graph-runtime submissions hand off workflow_runs + evidence to the
    # persistent evidence authority. Writing the run row here would split lifecycle
    # ownership and leave the executor trying to replay the initial submission over
    # a run that already claims to be admitted.
    if intake_outcome.current_state.value != "claim_accepted":
        conn.execute(
            """INSERT INTO workflow_runs (
                   run_id, workflow_id, request_id, request_digest, authority_context_digest,
                   workflow_definition_id, admitted_definition_hash, run_idempotency_key,
                   schema_version, request_envelope, context_bundle_id, admission_decision_id,
                   current_state, terminal_reason_code, requested_at, admitted_at, started_at, finished_at, last_event_id
               ) VALUES (
                   $1, $2, $3, $4, $5, $6, $7, $8,
                   $9, $10::jsonb, $11, $12, $13, NULL, $14, $15, NULL, NULL, NULL
               )
               ON CONFLICT (run_id) DO UPDATE
               SET workflow_id = EXCLUDED.workflow_id,
                   request_envelope = EXCLUDED.request_envelope,
                   workflow_definition_id = EXCLUDED.workflow_definition_id,
                   admitted_definition_hash = EXCLUDED.admitted_definition_hash,
                   current_state = EXCLUDED.current_state,
                   admission_decision_id = EXCLUDED.admission_decision_id""",
            intake_outcome.run_id,
            request.workflow_id,
            request.request_id,
            intake_outcome.request_digest,
            intake_outcome.route_identity.authority_context_digest,
            definition_id,
            definition_hash,
            intake_outcome.run_idempotency_key,
            request.schema_version,
            request_envelope_json,
            decision.authority_context_ref,
            decision.admission_decision_id,
            intake_outcome.current_state.value,
            requested_at,
            now,
        )


def _submit_graph_workflow_inline(
    conn: SyncPostgresConnection,
    spec_dict: dict[str, object],
    *,
    run_id: str | None,
    packet_provenance: dict[str, object] | None = None,
) -> dict:
    requested_at = datetime.now(timezone.utc)
    try:
        request = compile_graph_workflow_request(spec_dict, run_id=run_id, conn=conn)
    except Exception as exc:
        raise RuntimeError(f"graph-capable workflow submit failed closed: {exc}") from exc
    registry = _graph_registry_for_request(request)
    planner = WorkflowIntakePlanner(registry=registry)
    intake_outcome = planner.plan(request=request)
    _persist_graph_authority(
        conn,
        intake_outcome=intake_outcome,
        request=request,
        requested_at=requested_at,
    )

    if not intake_outcome.validation_result.is_valid or intake_outcome.current_state.value != "claim_accepted":
        return {
            "run_id": intake_outcome.run_id,
            "status": intake_outcome.current_state.value,
            "total_jobs": len(spec_dict.get("jobs", [])) if isinstance(spec_dict.get("jobs"), list) else 0,
            "spec_name": str(spec_dict.get("name") or "inline"),
            "workflow_id": request.workflow_id,
            "error": intake_outcome.validation_result.reason_code,
        }

    try:
        database_url = resolve_runtime_database_url(required=True)
    except PostgresConfigurationError as exc:
        raise RuntimeError(
            "graph-capable workflow submission requires WORKFLOW_DATABASE_URL for durable runtime execution",
        ) from exc
    evidence_writer = PostgresEvidenceWriter(database_url=database_url)
    try:
        _persist_graph_submission_evidence(
            evidence_writer=evidence_writer,
            intake_outcome=intake_outcome,
            request=request,
        )
    finally:
        evidence_writer.close_blocking()
    execution_packet = _build_graph_execution_packet(
        conn,
        request=request,
        spec_dict=spec_dict,
        packet_provenance=packet_provenance,
    )
    packet_reuse_provenance = None
    compile_provenance = execution_packet.get("compile_provenance")
    if isinstance(compile_provenance, dict) and isinstance(
        compile_provenance.get("reuse"),
        dict,
    ):
        packet_reuse_provenance = dict(compile_provenance["reuse"])
        packet_reuse_provenance.setdefault(
            "input_fingerprint",
            str(compile_provenance.get("input_fingerprint") or "").strip(),
        )
    payload = {
        "run_id": intake_outcome.run_id,
        "status": intake_outcome.current_state.value,
        "total_jobs": len(spec_dict.get("jobs", [])) if isinstance(spec_dict.get("jobs"), list) else 0,
        "spec_name": str(spec_dict.get("name") or "inline"),
        "workflow_id": request.workflow_id,
        "packet_reuse_provenance": packet_reuse_provenance,
        "execution_mode": "graph_runtime",
    }
    payload["reason_code"] = intake_outcome.admission_decision.reason_code
    conn.execute("SELECT pg_notify('system_event', $1)", intake_outcome.run_id)
    return payload


# ── Submission ────────────────────────────────────────────────────────

def _enforce_effective_provider_job_catalog(
    conn: SyncPostgresConnection,
    *,
    runtime_profile_ref: str | None,
    route_task_type: str,
    failover_chain: list[str],
    job_label: str,
) -> None:
    normalized_runtime_profile_ref = str(runtime_profile_ref or "").strip()
    normalized_task_type = str(route_task_type or "").strip()
    normalized_candidates = [
        str(candidate).strip()
        for candidate in failover_chain
        if "/" in str(candidate or "").strip()
    ]
    if not normalized_task_type or not normalized_candidates:
        return
    if not normalized_runtime_profile_ref:
        return

    from storage.postgres import PostgresTransportEligibilityRepository

    catalog_rows = PostgresTransportEligibilityRepository(
        conn
    ).list_effective_provider_job_catalog(
        runtime_profile_ref=normalized_runtime_profile_ref,
        job_type=normalized_task_type,
    )
    catalog_slugs = {
        f"{row.provider_slug}/{row.model_slug}"
        for row in catalog_rows
    }
    if any(candidate in catalog_slugs for candidate in normalized_candidates):
        return

    # The submit failed admission. Pull the full control-plane snapshot for the
    # rejected candidates so the caller gets the per-row `removal_reasons` AND
    # a `next_action.tool` hint pointing at the operator tool that owns the
    # gate. Without this, the error is just "no candidate" and the operator
    # has to query four other surfaces to find out why.
    rejection_rows = _build_admission_rejection_rows(
        conn,
        runtime_profile_ref=normalized_runtime_profile_ref,
        task_type=normalized_task_type,
        requested_candidates=normalized_candidates,
    )
    next_actions = _build_next_action_hints(rejection_rows)
    raise provider_authority_fail(
        "provider_authority.effective_catalog_rejected",
        "workflow submit failed closed: no effective provider job catalog "
        f"candidate for job {job_label!r}, task_type={normalized_task_type!r}, "
        f"runtime_profile_ref={normalized_runtime_profile_ref!r}; "
        f"requested_candidates={normalized_candidates!r}",
        job_label=job_label,
        route_task_type=normalized_task_type,
        runtime_profile_ref=normalized_runtime_profile_ref,
        requested_candidates=normalized_candidates,
        rejection_rows=rejection_rows,
        next_actions=next_actions,
    )


# ──────────────────────────────────────────────────────────────────────────
# Admission-rejection enrichment
#
# When `_enforce_effective_provider_job_catalog` rejects a submit, we pull
# `private_provider_control_plane_snapshot` for the requested candidates so
# the caller sees:
#   - is_runnable + breaker_state per row
#   - removal_reasons (every gate that rejected: matrix, profile-admission,
#     transport-admission, breaker, credentials)
#   - next_action.tool (the operator tool that owns each blocking gate)
#
# Reason-code → tool routing lives here so the model-facing failure response
# names the tool to use instead of leaving the operator to guess.
# ──────────────────────────────────────────────────────────────────────────

_REASON_CODE_TO_TOOL: dict[str, dict[str, str]] = {
    "control_panel.transport_turned_off": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "API/CLI transport policy or model-access denial",
    },
    "control_panel.model_access_method_turned_off": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "explicit denial row blocking this provider/model",
    },
    "runtime_profile_route.not_admitted": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "runtime_profile_admitted_routes is missing this candidate",
    },
    "provider_job_catalog.availability_disabled": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "candidate exists but is disabled in the runtime profile job catalog",
    },
    "provider_transport.missing": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "provider_transport_admissions row missing",
    },
    "provider_transport.policy_denied": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "transport policy denies this adapter",
    },
    "provider_transport.disabled": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "transport admission is inactive",
    },
    "circuit_breaker.manual_override_open": {
        "tool": "praxis_circuits",
        "action": "reset",
        "concern": "operator forced this provider's breaker OPEN",
    },
    "circuit_breaker.runtime_open": {
        "tool": "praxis_circuits",
        "action": "list",
        "concern": "circuit breaker is open due to recent failures",
    },
    "credentials.missing": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "API key / CLI credential missing for this provider",
    },
}


def _build_admission_rejection_rows(
    conn: SyncPostgresConnection,
    *,
    runtime_profile_ref: str,
    task_type: str,
    requested_candidates: list[str],
) -> list[dict[str, Any]]:
    if not requested_candidates:
        return []
    pairs = [c.split("/", 1) for c in requested_candidates if "/" in c]
    if not pairs:
        return []
    placeholders = ", ".join(
        f"(${i*2 + 3}::text, ${i*2 + 4}::text)" for i in range(len(pairs))
    )
    sql = f"""
        SELECT provider_slug, model_slug, transport_type, adapter_type,
               is_runnable, breaker_state, manual_override_state,
               manual_override_reason, primary_removal_reason_code,
               removal_reasons, credential_availability_state
        FROM private_provider_control_plane_snapshot
        WHERE runtime_profile_ref = $1
          AND job_type = $2
          AND (provider_slug, model_slug) IN ({placeholders})
        ORDER BY transport_type DESC, provider_slug, model_slug
    """
    args: list[Any] = [runtime_profile_ref, task_type]
    for prov, model in pairs:
        args.append(prov)
        args.append(model)
    try:
        rows = conn.execute(sql, *args)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in rows or []:
        rd = dict(row)
        out.append({
            "candidate": f"{rd.get('provider_slug')}/{rd.get('model_slug')}",
            "transport_type": rd.get("transport_type"),
            "adapter_type": rd.get("adapter_type"),
            "is_runnable": rd.get("is_runnable"),
            "breaker_state": rd.get("breaker_state"),
            "manual_override_state": rd.get("manual_override_state"),
            "manual_override_reason": rd.get("manual_override_reason"),
            "primary_removal_reason_code": rd.get("primary_removal_reason_code"),
            "removal_reasons": rd.get("removal_reasons") or [],
            "credential_availability_state": rd.get("credential_availability_state"),
        })
    return out


def _build_next_action_hints(
    rejection_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map each unique blocking reason_code → operator tool that owns it.

    Returned as a deduplicated list so the failure response says EXACTLY which
    tools to call to lift each gate, not just one generic 'fix admission'.
    """
    seen_reasons: set[str] = set()
    actions: list[dict[str, Any]] = []
    for row in rejection_rows:
        if row.get("is_runnable"):
            continue
        primary = str(row.get("primary_removal_reason_code") or "").strip()
        for reason_obj in row.get("removal_reasons") or []:
            rc = ""
            if isinstance(reason_obj, dict):
                rc = str(reason_obj.get("reason_code") or "").strip()
            elif isinstance(reason_obj, str):
                rc = reason_obj.strip()
            if rc and rc not in seen_reasons:
                seen_reasons.add(rc)
                hint = _REASON_CODE_TO_TOOL.get(rc)
                if hint:
                    actions.append({
                        "reason_code": rc,
                        "blocked_candidates": [
                            r["candidate"] for r in rejection_rows
                            if any(
                                (isinstance(rr, dict) and rr.get("reason_code") == rc)
                                or (isinstance(rr, str) and rr == rc)
                                for rr in (r.get("removal_reasons") or [])
                            )
                        ],
                        **hint,
                    })
        if primary and primary not in seen_reasons:
            seen_reasons.add(primary)
            hint = _REASON_CODE_TO_TOOL.get(primary)
            if hint:
                actions.append({
                    "reason_code": primary,
                    "blocked_candidates": [row["candidate"]],
                    **hint,
                })
    return actions


def _do_submit_workflow(
    conn: SyncPostgresConnection,
    spec,
    run_id: str,
    *,
    force_fresh_run: bool = False,
    parent_run_id: str | None = None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    trigger_depth: int = 0,
    lineage_depth: int | None = None,
    packet_provenance: dict[str, object] | None = None,
) -> dict:
    """Core submission logic: INSERT workflow_runs + workflow_jobs rows.

    Called by both submit_workflow (file-based) and submit_workflow_inline (dict-based).
    Returns {run_id, status, total_jobs, spec_name}.
    """
    now = datetime.now(timezone.utc)
    runtime_profile_ref = _runtime_profile_ref_from_spec(spec, conn=conn)

    # Resolve auto/ agent slugs via task_type_router
    try:
        from runtime.task_type_router import TaskTypeRouter
        router = TaskTypeRouter(conn)
        router.resolve_spec_jobs(
            spec.jobs,
            runtime_profile_ref=runtime_profile_ref or None,
        )
    except Exception as exc:
        if runtime_profile_ref:
            raise RuntimeError(
                f"workflow submit failed closed for runtime profile {runtime_profile_ref!r}: {exc}",
            ) from exc
        logger.error("Task type routing failed: %s", exc)

    raw_snapshot = spec._raw.copy()
    if "jobs" in raw_snapshot:
        raw_snapshot["jobs"] = [
            {k: v for k, v in j.items() if k != "_route_plan"}
            for j in raw_snapshot["jobs"]
        ]
    route_plan_manifest: dict[str, dict[str, object]] = {}
    for job in spec.jobs:
        label = str(job.get("label", "") or "").strip()
        route_plan = job.get("_route_plan")
        route_task_type = (
            str(getattr(route_plan, "task_type", "") or job.get("task_type") or "").strip()
        )
        failover_chain = list(job.get("route_candidates") or [])
        if not failover_chain and route_plan:
            failover_chain = [str(item).strip() for item in route_plan.chain]
        if not failover_chain:
            failover_chain = [str(job.get("agent") or "auto/build").strip()]
        route_origin_slug = str(getattr(route_plan, "original_slug", "") or "").strip()
        if label:
            route_plan_manifest[label] = {
                "route_task_type": route_task_type,
                "failover_chain": [str(item).strip() for item in failover_chain if str(item).strip()],
                "route_origin_slug": route_origin_slug,
            }
    authority = _ensure_workflow_authority(
        conn,
        run_id=run_id,
        spec=spec,
        raw_snapshot=raw_snapshot,
        now=now,
        parent_run_id=parent_run_id,
        parent_job_label=parent_job_label,
        dispatch_reason=dispatch_reason,
        trigger_depth=trigger_depth,
        lineage_depth=lineage_depth,
        route_plan_manifest={"jobs": route_plan_manifest} if route_plan_manifest else None,
    )

    # 2. Build label → job_id mapping for dependency wiring
    label_to_id: dict[str, int] = {}
    job_rows = []
    replayed_jobs: list[dict[str, str | None]] = []
    replayed_labels: set[str] = set()

    for job in spec.jobs:
        label = job["label"]
        agent_slug = job.get("agent", "auto/build")
        prompt = job.get("prompt", "")
        depends_on = [dep for dep in job.get("depends_on", []) if dep not in replayed_labels]
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]

        # Step complexity — spec author declares; "low" triggers prefer_cost routing
        complexity = str(job.get("complexity", "moderate")).strip().lower()
        if complexity not in ("low", "moderate", "high"):
            complexity = "moderate"

        # Integration metadata (for direct tool execution)
        integration_id = job.get("integration_id")
        integration_action = job.get("integration_action")
        integration_args = job.get("integration_args")
        if integration_args and not isinstance(integration_args, str):
            integration_args = json.dumps(integration_args)

        # Route plan from task_type_router (if resolved)
        route_plan = job.get("_route_plan")
        route_task_type = (
            str(getattr(route_plan, "task_type", "") or job.get("task_type") or "").strip()
        )
        route_origin_slug = str(getattr(route_plan, "original_slug", "") or "").strip()
        failover_chain = list(job.get("route_candidates") or [])
        if not failover_chain and route_plan:
            failover_chain = list(route_plan.chain)
        if not failover_chain:
            failover_chain = [agent_slug]
        _enforce_effective_provider_job_catalog(
            conn,
            runtime_profile_ref=runtime_profile_ref,
            route_task_type=route_task_type,
            failover_chain=[str(item).strip() for item in failover_chain if str(item).strip()],
            job_label=str(label),
        )

        initial_status = "pending" if depends_on else "ready"
        max_attempts = int(job.get("max_attempts", 3) or 3)
        dependency_threshold = job.get("dependency_threshold")
        if dependency_threshold is not None:
            dependency_threshold = int(dependency_threshold)
        # Prefer queue_id for idempotency so changing it clears conflicts
        # without needing to also rename the spec (BUG-0551295A).
        _idem_ns = str(spec._raw.get("queue_id") or "").strip() or spec.name
        ledger_idempotency_key = f"{_idem_ns}:{label}:{prompt_hash}"
        job_idempotency_key = f"{run_id}:{_idem_ns}:{label}:{prompt_hash}"
        payload = {
            "spec_name": spec.name,
            "label": label,
            "prompt_hash": prompt_hash,
        }
        payload_hash = canonical_hash(payload)
        if not force_fresh_run:
            result = check_idempotency(
                conn,
                "workflow.run",
                ledger_idempotency_key,
                payload_hash,
                replayable_run_states=_WORKFLOW_REPLAYABLE_RUN_STATES,
            )
            if result.is_replay:
                logger.info("Idempotent replay: returning existing run_id=%s", result.existing_run_id)
                replayed_labels.add(label)
                replayed_jobs.append({"label": label, "existing_run_id": result.existing_run_id})
                continue
            if result.is_conflict:
                logger.warning("Idempotency conflict: key=%s exists with different payload", ledger_idempotency_key)
                raise IdempotencyConflict(ledger_idempotency_key, result.existing_run_id, result.created_at)

        _enforce_queue_admission(conn, job_count=1)

        rows = conn.execute(
            """INSERT INTO workflow_jobs
               (run_id, label, job_type, phase, agent_slug, resolved_agent, prompt,
                prompt_hash, status, ready_at, failover_chain, route_task_type,
                route_origin_slug, idempotency_key,
                max_attempts, created_at,
                integration_id, integration_action, integration_args, touch_keys,
                dependency_threshold, complexity)
               VALUES ($1, $2, 'dispatch', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                       $15, $16, $17, $18::jsonb, $19::jsonb, $20, $21)
               ON CONFLICT (run_id, label) DO UPDATE SET status = EXCLUDED.status
               RETURNING id""",
            run_id, label, spec.phase, agent_slug,
            agent_slug if not agent_slug.startswith("auto/") else None,
            prompt, prompt_hash, initial_status,
            now if initial_status == "ready" else None,
            failover_chain,
            route_task_type,
            route_origin_slug,
            job_idempotency_key, max_attempts, now,
            integration_id, integration_action,
            integration_args if integration_args else None,
            json.dumps(_derive_touch_keys(job)),
            dependency_threshold,
            complexity,
        )
        job_id = rows[0]["id"]
        record_idempotency(conn, "workflow.run", ledger_idempotency_key, payload_hash, run_id=run_id)
        label_to_id[label] = job_id
        job_rows.append((job_id, label, tuple(depends_on)))

    if not job_rows and replayed_jobs:
        existing_run_ids = sorted(
            {
                str(job["existing_run_id"])
                for job in replayed_jobs
                if job.get("existing_run_id")
            }
        )
        canonical_run_id = existing_run_ids[0] if existing_run_ids else run_id
        conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", run_id)
        logger.info(
            "Workflow %s fully replayed onto existing run %s; discarding empty replay shell",
            run_id,
            canonical_run_id,
        )
        return {
            "run_id": canonical_run_id,
            "status": "replayed",
            "total_jobs": len(spec.jobs),
            "spec_name": spec.name,
            "workflow_id": authority["workflow_id"],
            "replayed_jobs": replayed_jobs,
        }

    execution_packet = _build_execution_packet(
        conn=conn,
        spec=spec,
        raw_snapshot=raw_snapshot,
        run_id=run_id,
        workflow_id=authority["workflow_id"],
        authority=authority,
        parent_run_id=parent_run_id,
        trigger_depth=trigger_depth,
        provenance=packet_provenance,
    )
    packet_reuse_provenance = None
    if execution_packet is not None:
        compile_provenance = execution_packet.get("compile_provenance")
        if isinstance(compile_provenance, dict) and isinstance(compile_provenance.get("reuse"), dict):
            packet_reuse_provenance = dict(compile_provenance["reuse"])
            packet_reuse_provenance.setdefault(
                "input_fingerprint",
                str(compile_provenance.get("input_fingerprint") or "").strip(),
            )

    persisted_job_labels = {label for _, label, _ in job_rows}
    if persisted_job_labels:
        file_inputs = execution_packet.get("file_inputs") if isinstance(execution_packet, dict) else None
        if isinstance(file_inputs, dict):
            raw_execution_context_shards = file_inputs.get("execution_context_shards")
            raw_execution_bundles = file_inputs.get("execution_bundles")
        else:
            raw_execution_context_shards = None
            raw_execution_bundles = None
        execution_context_shards = (
            {
                label: dict(value)
                for label, value in raw_execution_context_shards.items()
                if label in persisted_job_labels and isinstance(value, dict)
            }
            if isinstance(raw_execution_context_shards, dict)
            else _build_job_execution_context_shards(
                conn=conn,
                spec=spec,
                raw_snapshot=raw_snapshot,
                provenance=packet_provenance,
            )
        )
        execution_context_shards = {
            label: value
            for label, value in execution_context_shards.items()
            if label in persisted_job_labels
        }
        execution_bundles = (
            {
                label: dict(value)
                for label, value in raw_execution_bundles.items()
                if label in persisted_job_labels and isinstance(value, dict)
            }
            if isinstance(raw_execution_bundles, dict)
            else _build_job_execution_bundles(
                conn=conn,
                spec=spec,
                raw_snapshot=raw_snapshot,
                execution_context_shards=execution_context_shards,
                run_id=run_id,
                workflow_id=authority["workflow_id"],
                runtime_profile_ref=_runtime_profile_ref_from_spec(spec, conn=conn),
            )
        )
        execution_bundles = {
            label: value
            for label, value in execution_bundles.items()
            if label in persisted_job_labels
        }
        persist_workflow_job_runtime_contexts(
            conn,
            run_id=run_id,
            workflow_id=authority["workflow_id"],
            execution_context_shards=execution_context_shards,
            execution_bundles=execution_bundles,
        )

    # 3. Wire dependency edges
    #
    # Fail-closed cycle check: admission must NEVER wire an edge whose
    # parent comes later in spec order than its child. Such an edge
    # creates a dependency cycle — no job's predecessors can ever all
    # become terminal, so `ready_at` stays NULL forever and the run
    # sits queued. We previously saw this produced by
    # _apply_write_scope_auto_dependencies deriving file-level edges
    # without respecting spec order; the fix there is in place, but we
    # enforce the invariant at the write site so any future derivation
    # path that regresses on this cannot silently stall a workflow.
    label_order_index = {label: idx for idx, (_, label, _) in enumerate(job_rows)}
    expected_edges: set[tuple[int, int]] = set()
    for job_id, label, depends_on in job_rows:
        remaining_depends_on = [dep for dep in depends_on if dep not in replayed_labels]
        if not remaining_depends_on:
            conn.execute(
                "UPDATE workflow_jobs SET status = 'ready', ready_at = now() WHERE id = $1 AND status = 'pending'",
                job_id,
            )
            continue
        for dep_label in remaining_depends_on:
            parent_id = label_to_id.get(dep_label)
            if not parent_id:
                raise RuntimeError(
                    "workflow submit failed closed while wiring dependency edges: "
                    f"missing parent mapping for {dep_label!r} -> {label!r}",
                )
            parent_idx = label_order_index.get(dep_label)
            child_idx = label_order_index.get(label)
            if (
                parent_idx is not None
                and child_idx is not None
                and parent_idx >= child_idx
            ):
                raise RuntimeError(
                    "workflow submit failed closed: dependency edge would "
                    f"create a cycle (parent={dep_label!r} idx={parent_idx}, "
                    f"child={label!r} idx={child_idx}). depends_on for child: "
                    f"{list(depends_on)!r}. Spec ordering must be forward-only.",
                )
            expected_edges.add((parent_id, job_id))
            conn.execute(
                "INSERT INTO workflow_job_edges (parent_id, child_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                parent_id, job_id,
            )

    _assert_dependency_edges_persisted(conn, expected_edges=expected_edges)

    _recompute_workflow_run_state(conn, run_id)

    logger.info("Submitted workflow %s: %d jobs (%s) [depth=%d, parent=%s]",
                run_id, len(spec.jobs), spec.name, trigger_depth, parent_run_id or "none")

    return {
        "run_id": run_id,
        "status": "queued",
        "total_jobs": len(spec.jobs),
        "spec_name": spec.name,
        "workflow_id": authority["workflow_id"],
        "replayed_jobs": replayed_jobs,
        "packet_reuse_provenance": packet_reuse_provenance,
    }


def _assert_dependency_edges_persisted(
    conn: SyncPostgresConnection,
    *,
    expected_edges: set[tuple[int, int]],
) -> None:
    if not expected_edges:
        return

    child_ids = sorted({child_id for _, child_id in expected_edges})
    rows = conn.execute(
        """SELECT parent_id, child_id
           FROM workflow_job_edges
           WHERE child_id = ANY($1::bigint[])""",
        child_ids,
    )
    actual_edges = {
        (int(row["parent_id"]), int(row["child_id"]))
        for row in rows or []
    }
    missing = sorted(expected_edges - actual_edges)
    if missing:
        sample = ", ".join(f"{parent}->{child}" for parent, child in missing[:5])
        raise RuntimeError(
            "workflow submit failed closed because dependency edges were not persisted"
            + (f": {sample}" if sample else ""),
        )


class WorkflowSubmitConflict(RuntimeError):
    """Raised when an otherwise-valid spec collides with state already in the
    admission tables (typically: same workflow_id already has a registered
    definition). Carries the offending workflow_id plus a remediation hint so
    CLI/MCP surfaces can show something actionable instead of a psycopg
    UniqueViolation with a constraint name.
    """

    def __init__(self, *, workflow_id: str, remediation: str, underlying: Exception | None = None) -> None:
        super().__init__(remediation)
        self.workflow_id = workflow_id
        self.remediation = remediation
        self.reason_code = "workflow.submit.definition_collision"
        self.underlying = underlying


def _translate_definition_collision(
    exc: Exception,
    *,
    workflow_id: str,
) -> WorkflowSubmitConflict | None:
    """Detect the `(workflow_id, definition_version)` unique-violation and turn
    it into an actionable error. Returns None if the exception is unrelated
    so the caller can re-raise unchanged.

    Matches on constraint name (psycopg3) and falls back to substring search
    on the message so the translation survives driver-version churn.
    """
    constraint = ""
    diag = getattr(exc, "diag", None)
    if diag is not None:
        constraint = str(getattr(diag, "constraint_name", "") or "")
    message = str(exc)
    marker = "workflow_definitions_workflow_id_definition_version_key"
    if marker not in constraint and marker not in message:
        return None
    remediation = (
        f"workflow_id {workflow_id!r} already has a registered definition; "
        "the admission tables treat (workflow_id, definition_version) as unique. "
        "Either bump the workflow_id in the spec (e.g. append '_v2') or drop the "
        f"existing definition with: DELETE FROM workflow_definitions WHERE "
        f"workflow_id = '{workflow_id}';"
    )
    return WorkflowSubmitConflict(
        workflow_id=workflow_id,
        remediation=remediation,
        underlying=exc,
    )


def _assert_provider_availability_for_submit(conn: SyncPostgresConnection, spec: object) -> None:
    """Block launch when selected providers are known unavailable.

    This is the submit-time enforcement twin of workflow validation preflight.
    Runtime execution still has a circuit-breaker guard, but admission should
    reject a workflow before it queues doomed jobs against an exhausted provider.
    """
    from runtime.workflow_validation import _preflight_provider_availability

    class _DurableOnlyCircuitBreakers:
        def all_states(self) -> dict[str, dict[str, object]]:
            return {}

    findings = _preflight_provider_availability(
        spec,
        pg_conn=conn,
        circuit_breakers=_DurableOnlyCircuitBreakers(),
    )
    blockers = [finding for finding in findings if finding.get("severity") == "error"]
    if not blockers:
        return
    first = blockers[0]
    raise RuntimeError(
        "workflow submit blocked by provider availability: "
        f"{first.get('message') or first.get('kind') or 'provider unavailable'}"
    )


def submit_workflow(
    conn: SyncPostgresConnection,
    spec_path: str,
    repo_root: str,
    run_id: str | None = None,
    force_fresh_run: bool = False,
    parent_run_id: str | None = None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    lineage_depth: int | None = None,
) -> dict:
    """Parse a workflow spec file and submit it."""
    from runtime.workflow_spec import WorkflowSpec

    path_obj = Path(spec_path)
    full_path = str(path_obj if path_obj.is_absolute() else Path(repo_root) / path_obj)
    spec = WorkflowSpec.load(full_path)
    _apply_write_scope_auto_dependencies(spec)

    force_fresh_run = bool(force_fresh_run or run_id is not None)
    run_id = run_id or f"workflow_{uuid.uuid4().hex[:12]}"
    workflow_id_hint = _workflow_id_for_spec(spec)
    if spec_uses_graph_runtime(spec._raw):
        try:
            return _submit_graph_workflow_inline(
                conn,
                spec._raw,
                run_id=run_id,
                packet_provenance={
                    "source_kind": "file_submit",
                    "spec_path": full_path,
                    "repo_root": repo_root,
                    "file_inputs": spec._raw,
                },
            )
        except GraphWorkflowCompileError as exc:
            raise RuntimeError(
                f"graph-capable workflow submit failed closed: {exc}",
            ) from exc
        except Exception as exc:
            translated = _translate_definition_collision(exc, workflow_id=workflow_id_hint)
            if translated is not None:
                raise translated from exc
            raise
    with _submit_transaction(conn) as submit_conn:
        _assert_provider_availability_for_submit(submit_conn, spec)
        try:
            return _do_submit_workflow(
                submit_conn,
                spec,
                run_id,
                force_fresh_run=force_fresh_run,
                parent_run_id=parent_run_id,
                parent_job_label=parent_job_label,
                dispatch_reason=dispatch_reason,
                lineage_depth=lineage_depth,
                packet_provenance={
                    "source_kind": "file_submit",
                    "spec_path": full_path,
                    "repo_root": repo_root,
                    "file_inputs": spec._raw,
                },
            )
        except Exception as exc:
            translated = _translate_definition_collision(exc, workflow_id=workflow_id_hint)
            if translated is not None:
                raise translated from exc
            raise


def submit_workflow_inline(
    conn: SyncPostgresConnection,
    spec_dict: dict,
    run_id: str | None = None,
    force_fresh_run: bool = False,
    parent_run_id: str | None = None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    trigger_depth: int = 0,
    lineage_depth: int | None = None,
    packet_provenance: dict[str, object] | None = None,
) -> dict:
    """Submit a workflow from an in-memory spec dict (no file required).

    Used by workflow invocation and trigger system. Direct servicebus operation.
    """
    if trigger_depth > 3:
        raise RuntimeError(f"Trigger depth {trigger_depth} exceeds maximum (3). Possible infinite loop.")

    provenance_source_kind = str((packet_provenance or {}).get("source_kind") or "").strip()
    inline_submit_lane = provenance_source_kind == "inline_submit"

    if not inline_submit_lane and spec_uses_graph_runtime(spec_dict):
        try:
            return _submit_graph_workflow_inline(
                conn,
                spec_dict,
                run_id=run_id,
                packet_provenance=packet_provenance,
            )
        except GraphWorkflowCompileError as exc:
            raise RuntimeError(
                f"graph-capable workflow submit failed closed: {exc}",
            ) from exc

    force_fresh_run = bool(force_fresh_run or run_id is not None)
    run_id = run_id or f"workflow_{uuid.uuid4().hex[:12]}"

    # Build a lightweight spec object with the attributes _do_submit needs
    spec = _inline_spec_object(spec_dict)

    with _submit_transaction(conn) as submit_conn:
        _assert_provider_availability_for_submit(submit_conn, spec)
        return _do_submit_workflow(
            submit_conn,
            spec,
            run_id,
            force_fresh_run=force_fresh_run,
            parent_run_id=parent_run_id,
            parent_job_label=parent_job_label,
            dispatch_reason=dispatch_reason,
            trigger_depth=trigger_depth,
            lineage_depth=lineage_depth,
            packet_provenance=packet_provenance
            or {
                "source_kind": "inline_submit",
                "file_inputs": spec_dict,
            },
        )


def load_execution_packets(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
) -> tuple[dict[str, object], ...]:
    """Load shadow execution packet truth for runtime inspection."""

    store = CompileArtifactStore(conn)
    return tuple(dict(packet.payload) for packet in store.load_execution_packets(run_id=run_id))


def _retry_packet_reuse_provenance(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
) -> dict[str, object] | None:
    """Load and validate reusable packet lineage for a retry when available.

    Retries still support legacy runs that predate execution-packet authority, so
    absence of packet records is tolerated. Once a run advertises packet lineage,
    reuse validation is strict and stale artifacts fail closed.
    """

    store = CompileArtifactStore(conn)
    packets = store.load_execution_packets(run_id=run_id)
    if not packets:
        return None

    input_fingerprints = {
        str(packet.payload.get("compile_provenance", {}).get("input_fingerprint") or "").strip()
        for packet in packets
        if isinstance(packet.payload.get("compile_provenance"), dict)
        and str(packet.payload.get("compile_provenance", {}).get("input_fingerprint") or "").strip()
    }
    if len(input_fingerprints) > 1:
        raise RuntimeError(
            "retry compile reuse failed closed: run has conflicting execution packet input fingerprints",
        )

    packet = packets[0]
    compile_provenance = (
        dict(packet.payload.get("compile_provenance"))
        if isinstance(packet.payload.get("compile_provenance"), dict)
        else {}
    )
    input_fingerprint = str(compile_provenance.get("input_fingerprint") or "").strip()
    if not input_fingerprint:
        return None

    recorded_lineage_revision = str(compile_provenance.get("packet_lineage_revision") or "").strip()
    recorded_lineage_hash = str(compile_provenance.get("packet_lineage_hash") or "").strip()
    try:
        reusable_lineage = store.load_reusable_artifact(
            artifact_kind="packet_lineage",
            input_fingerprint=input_fingerprint,
        )
    except CompileArtifactError as exc:
        raise RuntimeError(f"retry compile reuse failed closed: {exc}") from exc
    if reusable_lineage is None:
        raise RuntimeError(
            "retry compile reuse failed closed: recorded execution packet is missing packet lineage authority",
        )
    if recorded_lineage_revision and reusable_lineage.revision_ref != recorded_lineage_revision:
        raise RuntimeError(
            "retry compile reuse failed closed: recorded packet lineage revision does not match reusable artifact",
        )
    reusable_lineage_hash = str(reusable_lineage.payload.get("packet_hash") or "").strip()
    if recorded_lineage_hash and reusable_lineage_hash != recorded_lineage_hash:
        raise RuntimeError(
            "retry compile reuse failed closed: recorded packet lineage hash does not match reusable artifact",
        )

    recorded_reuse = (
        dict(compile_provenance.get("reuse"))
        if isinstance(compile_provenance.get("reuse"), dict)
        else {}
    )
    return {
        "artifact_kind": "packet_lineage",
        "decision": "reused",
        "reason_code": "packet.retry.existing_execution_packet",
        "input_fingerprint": input_fingerprint,
        "artifact_ref": reusable_lineage.artifact_ref,
        "revision_ref": reusable_lineage.revision_ref,
        "content_hash": reusable_lineage.content_hash,
        "packet_lineage_hash": reusable_lineage_hash,
        "decision_ref": reusable_lineage.decision_ref,
        "execution_packet_ref": packet.packet_revision,
        "recorded_submission_reuse": recorded_reuse,
    }
