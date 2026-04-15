"""Workflow submission pipeline: spec parsing, job creation, and idempotency."""
from __future__ import annotations

import concurrent.futures
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

from adapters import AdapterRegistry, CLILLMAdapter, LLMTaskAdapter, MCPTaskAdapter
from adapters.api_task import APITaskAdapter
from adapters.context_adapter import ContextCompilerAdapter
from adapters.file_writer_adapter import FileWriterAdapter
from adapters.output_parser_adapter import OutputParserAdapter
from adapters.provider_registry import default_provider_slug
from adapters.verify_adapter import VerifyAdapter
from contracts.domain import WorkflowRequest
from registry.domain import RegistryResolver, RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord
from registry.native_runtime_profile_sync import resolve_native_runtime_profile_config
from runtime.execution_strategy import StepCompiler
from runtime.idempotency import canonical_hash, check_idempotency, record_idempotency
from runtime.intake import WorkflowIntakePlanner
from runtime.native_authority import default_native_authority_refs
from runtime.persistent_evidence import PostgresEvidenceWriter
from runtime.workflow._workflow_execution import WorkflowExecutionContext, execute_workflow_request
from runtime.workflow_graph_compiler import (
    GraphWorkflowCompileError,
    compile_graph_workflow_request,
    spec_uses_graph_runtime,
)
from ._shared import (
    _WORKFLOW_REPLAYABLE_RUN_STATES,
    _json_loads_maybe,
    _normalize_paths,
    _slugify,
    _workflow_id_for_spec,
)
from ._routing import (
    _build_request_envelope,
    _derive_touch_keys,
    _runtime_profile_ref_from_spec,
)
from ._workflow_state import (
    _ensure_workflow_authority,
    _recompute_workflow_run_state,
)
from ._context_building import (
    _build_job_execution_context_shards,
    _build_job_execution_bundles,
    _build_execution_packet,
    _shadow_packet_inspection_from_rows,
)
from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.workflow.job_runtime_context import persist_workflow_job_runtime_contexts
from runtime.workflow.submission_capture import WorkflowSubmissionServiceError

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = [
    "IdempotencyConflict",
    "_retry_packet_reuse_provenance",
    "load_execution_packets",
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


_QUEUE_ADMISSION_CRITICAL_THRESHOLD = 1000


def _queue_depth_from_workflow_jobs(conn: SyncPostgresConnection) -> int:
    rows = conn.execute(
        "SELECT COUNT(*) FROM workflow_jobs WHERE status IN ('pending', 'ready')"
    )
    if not rows:
        return 0
    row = rows[0]
    keys = None
    if isinstance(row, dict):
        keys = row.keys()
    else:
        try:
            keys = row.keys()
        except Exception:
            keys = None
    if keys is not None:
        try:
            if "count" in keys:
                return int(row["count"] or 0)
            if "?column?" in keys:
                return int(row["?column?"] or 0)
        except Exception:
            pass
    if isinstance(row, (tuple, list)):
        return int(row[0] or 0) if row else 0
    try:
        return int(row or 0)
    except (TypeError, ValueError):
        return 0


def _enforce_queue_admission(
    conn: SyncPostgresConnection,
    *,
    job_count: int = 1,
    critical_threshold: int = _QUEUE_ADMISSION_CRITICAL_THRESHOLD,
) -> int:
    """Fail closed when admitting more jobs would overflow the queue."""
    queue_depth = _queue_depth_from_workflow_jobs(conn)
    projected_depth = queue_depth + max(0, int(job_count))
    if queue_depth >= critical_threshold or projected_depth > critical_threshold:
        message = (
            f"queue admission rejected: queue depth {queue_depth} "
            f"with {max(0, int(job_count))} new job(s) would exceed critical threshold "
            f"{critical_threshold}"
        )
        logger.warning(message)
        raise RuntimeError(message)
    return queue_depth


_DEFAULT_WORKSPACE_REF, _DEFAULT_RUNTIME_PROFILE_REF = default_native_authority_refs()


def _run_async(coro):
    try:
        import asyncio

        asyncio.get_running_loop()
    except RuntimeError:
        import asyncio

        return asyncio.run(coro)
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
    return {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": 1,
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


def _graph_registry_for_request(request: WorkflowRequest) -> RegistryResolver:
    runtime_profile_ref = request.runtime_profile_ref or _DEFAULT_RUNTIME_PROFILE_REF
    config = resolve_native_runtime_profile_config(runtime_profile_ref)
    workspace_ref = request.workspace_ref or config.workspace_ref or _DEFAULT_WORKSPACE_REF
    workdir = config.workdir or os.getcwd()
    return RegistryResolver(
        workspace_records={
            workspace_ref: [
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=workdir,
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
                ),
            ],
        },
    )


def _graph_adapter_registry() -> AdapterRegistry:
    registry = AdapterRegistry(
        api_task_adapter=APITaskAdapter(),
        llm_task_adapter=LLMTaskAdapter(),
        cli_llm_adapter=CLILLMAdapter(),
        mcp_task_adapter=MCPTaskAdapter(),
    )
    registry.register("context_compiler", ContextCompilerAdapter(shadow_packet_config=None))
    registry.register("output_parser", OutputParserAdapter())
    registry.register("file_writer", FileWriterAdapter())
    registry.register("verifier", VerifyAdapter())
    return registry


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
           ) VALUES ($1, $2, $3, 1, $4, 'active', $5::jsonb, $5::jsonb, $6)
           ON CONFLICT (workflow_definition_id) DO NOTHING""",
        definition_id,
        request.workflow_id,
        request.schema_version,
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
) -> dict:
    requested_at = datetime.now(timezone.utc)
    request = compile_graph_workflow_request(spec_dict, run_id=run_id)
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

    database_url = os.environ.get("WORKFLOW_DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "graph-capable workflow submission requires WORKFLOW_DATABASE_URL for durable runtime execution",
        )
    evidence_writer = PostgresEvidenceWriter(database_url=database_url)
    context = WorkflowExecutionContext(
        provider_slug=default_provider_slug(),
        model_slug=None,
        adapter_type="graph_queue_submit",
        started_at=requested_at,
        start_ns=time.monotonic_ns(),
    )
    try:
        execution_result, failure = execute_workflow_request(
            intake_outcome=intake_outcome,
            adapter_registry=_graph_adapter_registry(),
            evidence_writer=evidence_writer,
            context=context,
            timeout=int(spec_dict.get("timeout") or 900),
        )
    finally:
        evidence_writer.close_blocking()
    result = failure or execution_result
    if result is None:
        raise RuntimeError("graph-capable workflow submission returned no execution result")
    return {
        "run_id": intake_outcome.run_id,
        "status": result.status,
        "total_jobs": len(spec_dict.get("jobs", [])) if isinstance(spec_dict.get("jobs"), list) else 0,
        "spec_name": str(spec_dict.get("name") or "inline"),
        "workflow_id": request.workflow_id,
        "packet_reuse_provenance": None,
        "execution_mode": "graph_runtime",
    }


# ── Submission ────────────────────────────────────────────────────────

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
    runtime_profile_ref = _runtime_profile_ref_from_spec(spec)

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
        CompileArtifactStore(conn).record_execution_packet(
            packet=execution_packet,
            authority_refs=[execution_packet["definition_revision"], execution_packet["plan_revision"]],
            decision_ref=str(execution_packet["decision_ref"]),
            parent_artifact_ref=str(execution_packet["parent_artifact_ref"]),
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
                spec=spec,
                execution_context_shards=execution_context_shards,
                run_id=run_id,
                workflow_id=authority["workflow_id"],
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

    full_path = str(Path(repo_root) / spec_path)
    spec = WorkflowSpec.load(full_path)

    # Auto-dependency resolution from write_scope.
    # If we have write-scope authority, unresolved compiler errors must fail
    # closed instead of silently submitting through a second mutation path.
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
    if scope["write_scope"]:
        try:
            compiler = StepCompiler()
            plan = compiler.compile(scope)
        except Exception as exc:
            raise RuntimeError(
                f"workflow submit failed closed while resolving write-scope authority: {exc}",
            ) from exc
        step_id_to_path = {step.step_id: step.file_path for step in plan.steps}
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
                        if parent_label != child_label:
                            auto_deps.setdefault(child_label, set()).add(parent_label)
        for job in spec.jobs:
            label = job.get("label")
            if label in auto_deps and not job.get("depends_on"):
                job["depends_on"] = sorted(auto_deps[label])

    force_fresh_run = bool(force_fresh_run or run_id is not None)
    run_id = run_id or f"workflow_{uuid.uuid4().hex[:12]}"
    if spec_uses_graph_runtime(spec._raw):
        try:
            return _submit_graph_workflow_inline(
                conn,
                spec._raw,
                run_id=run_id,
            )
        except GraphWorkflowCompileError as exc:
            raise RuntimeError(
                f"graph-capable workflow submit failed closed: {exc}",
            ) from exc
    with _submit_transaction(conn) as submit_conn:
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

    if spec_uses_graph_runtime(spec_dict):
        try:
            return _submit_graph_workflow_inline(
                conn,
                spec_dict,
                run_id=run_id,
            )
        except GraphWorkflowCompileError as exc:
            raise RuntimeError(
                f"graph-capable workflow submit failed closed: {exc}",
            ) from exc

    force_fresh_run = bool(force_fresh_run or run_id is not None)
    run_id = run_id or f"workflow_{uuid.uuid4().hex[:12]}"

    # Build a lightweight spec object with the attributes _do_submit needs
    spec = type("InlineSpec", (), {
        "name": spec_dict.get("name", "inline"),
        "phase": spec_dict.get("phase", "build"),
        "jobs": spec_dict.get("jobs", []),
        "outcome_goal": spec_dict.get("outcome_goal", ""),
        "output_dir": spec_dict.get("output_dir", ""),
        "_raw": spec_dict,
    })()

    with _submit_transaction(conn) as submit_conn:
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
