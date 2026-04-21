"""Smoke service functions: DB contract loading and local smoke flow execution."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from registry.repository import bootstrap_registry_authority_schema
from runtime.execution import RuntimeOrchestrator
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
)
from runtime.outbox import PostgresWorkflowOutboxSubscriber
from runtime.workflow._admission import _execute_admitted_graph_run
from storage.postgres import PostgresEvidenceReader, connect_workflow_database
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
from surfaces.api import frontdoor, native_ops
from surfaces._workflow_database import workflow_database_url_for_repo
from ._operator_helpers import _run_async
from ._operator_repository import _repo_root


class _FrontdoorService(Protocol):
    def submit(
        self,
        *,
        request_payload: Mapping[str, Any],
        env: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        """Submit one workflow request."""

    def status(
        self,
        *,
        run_id: str,
        env: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        """Read one durable run status."""


@dataclass(frozen=True, slots=True)
class NativeSelfHostedSmokeContract:
    """Native smoke contract resolved from one canonical DB authority row."""

    queue_path: str
    request_payload: Mapping[str, Any]
    runtime_env: Mapping[str, str]


def _require_frontdoor_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_response",
            f"{field_name} must be an object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_authority_env(env: Mapping[str, str] | None) -> dict[str, str]:
    if env is None:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.authority_missing",
            "operator flow requires one explicit runtime authority env mapping",
        )
    source: dict[str, str] = {}
    for key, value in env.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.invalid_request",
                "operator flow env must be a mapping of string keys to string values",
                details={
                    "key_type": type(key).__name__,
                    "value_type": type(value).__name__,
                },
            )
        source[key] = value
    return source


_SMOKE_WORKFLOW_DEFINITION_ID = "workflow_definition.native_self_hosted_smoke.v1"
_SMOKE_WORKFLOW_ID = "workflow.native-self-hosted-smoke"
_SMOKE_DEFAULT_RUNTIME_ENV = {
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR": "Code&DBs/Databases/postgres-dev/data",
    PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: "config/runtime_profiles.json",
}
_SMOKE_PATH_ENV_NAMES = {
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR",
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
}
_SMOKE_REQUIRED_TERMINAL_STATE = "succeeded"
_SMOKE_REQUIRED_NODE_ORDER = ("node_0", "node_1")
_SMOKE_REQUIRED_RECEIPT_TYPE = "workflow_completion_receipt"
_SMOKE_EXECUTION_LOCKED_STATE = "locked"
_SMOKE_TERMINAL_STATES = {"succeeded", "failed", "cancelled", "dead_letter"}
_SMOKE_LOCK_WAIT_TIMEOUT_SECONDS = 30.0
_SMOKE_LOCK_WAIT_INTERVAL_SECONDS = 0.25


def _default_smoke_runtime_env() -> dict[str, str]:
    source_env = os.environ
    try:
        workflow_database_url = workflow_database_url_for_repo(_repo_root(), env=source_env)
    except Exception as exc:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.authority_missing",
            "WORKFLOW_DATABASE_URL must be set to load the native smoke contract",
        ) from exc
    raw_env = {
        "WORKFLOW_DATABASE_URL": workflow_database_url,
        **{
            name: str(source_env.get(name, default_value))
            for name, default_value in _SMOKE_DEFAULT_RUNTIME_ENV.items()
        },
    }
    return _resolve_smoke_env(raw_env)


def _resolve_smoke_env(raw_env: Mapping[str, object]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    repo_root = _repo_root()
    for name, value in raw_env.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.invalid_smoke_contract",
                "native smoke runtime_env must be a mapping of string keys to string values",
                details={
                    "key_type": type(name).__name__,
                    "value_type": type(value).__name__,
                },
            )
        if name in _SMOKE_PATH_ENV_NAMES:
            path = Path(value)
            if not path.is_absolute():
                path = repo_root / path
            resolved[name] = str(path.resolve())
            continue
        resolved[name] = value
    return resolved


def _normalize_smoke_request_payload(raw_payload: object) -> dict[str, Any]:
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.invalid_smoke_contract",
                "workflow_definitions.request_envelope must be valid JSON when stored as text",
                details={"field": "workflow_definitions.request_envelope"},
            ) from exc
    payload = dict(
        _require_frontdoor_mapping(
            raw_payload,
            field_name="workflow_definitions.request_envelope",
        )
    )
    payload.pop("definition_version", None)

    normalized_nodes: list[dict[str, Any]] = []
    raw_nodes = payload.get("nodes", [])
    if not isinstance(raw_nodes, list):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_smoke_contract",
            "native smoke request nodes must be a list",
            details={"field": "workflow_definitions.request_envelope.nodes"},
        )
    for index, node in enumerate(raw_nodes):
        normalized_node = dict(
            _require_frontdoor_mapping(
                node,
                field_name=f"workflow_definitions.request_envelope.nodes[{index}]",
            )
        )
        normalized_node.pop("schema_version", None)
        normalized_node.pop("workflow_definition_id", None)
        normalized_node.pop("workflow_definition_node_id", None)
        normalized_nodes.append(normalized_node)
    payload["nodes"] = normalized_nodes

    normalized_edges: list[dict[str, Any]] = []
    raw_edges = payload.get("edges", [])
    if not isinstance(raw_edges, list):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_smoke_contract",
            "native smoke request edges must be a list",
            details={"field": "workflow_definitions.request_envelope.edges"},
        )
    for index, edge in enumerate(raw_edges):
        normalized_edge = dict(
            _require_frontdoor_mapping(
                edge,
                field_name=f"workflow_definitions.request_envelope.edges[{index}]",
            )
        )
        normalized_edge.pop("schema_version", None)
        normalized_edge.pop("workflow_definition_id", None)
        normalized_edge.pop("workflow_definition_edge_id", None)
        normalized_edges.append(normalized_edge)
    payload["edges"] = normalized_edges
    return payload


def _isolate_smoke_request_payload(
    request_payload: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(
        _require_frontdoor_mapping(
            request_payload,
            field_name="native_smoke.workflow_request",
        )
    )
    suffix = uuid.uuid4().hex[:10]
    for field_name in ("workflow_id", "request_id", "workflow_definition_id", "definition_hash"):
        raw_value = payload.get(field_name)
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.invalid_smoke_contract",
                f"native smoke workflow_request.{field_name} must be a non-empty string",
                details={"field": f"native_smoke.workflow_request.{field_name}"},
            )
        payload[field_name] = f"{raw_value}.{suffix}"
    return payload


async def _load_native_self_hosted_smoke_contract_from_db_async() -> NativeSelfHostedSmokeContract:
    runtime_env = _default_smoke_runtime_env()
    conn = await connect_workflow_database(env=runtime_env)
    try:
        row = await conn.fetchrow(
            """
            SELECT workflow_definition_id, workflow_id, request_envelope
            FROM workflow_definitions
            WHERE status IN ('active', 'admitted')
              AND workflow_definition_id = $1
            """,
            _SMOKE_WORKFLOW_DEFINITION_ID,
        )
    finally:
        await conn.close()

    if row is None:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_contract_missing",
            "native smoke workflow definition is missing from DB authority",
            details={
                "workflow_definition_id": _SMOKE_WORKFLOW_DEFINITION_ID,
            },
        )

    return NativeSelfHostedSmokeContract(
        queue_path=f"workflow_definitions:{row['workflow_definition_id']}",
        request_payload=_normalize_smoke_request_payload(row["request_envelope"]),
        runtime_env=runtime_env,
    )


def _load_native_self_hosted_smoke_contract_from_db() -> NativeSelfHostedSmokeContract:
    return _run_async(
        _load_native_self_hosted_smoke_contract_from_db_async(),
        message=(
            "operator_flow.async_boundary_required: "
            "native smoke contract DB resolution requires a non-async call boundary"
        ),
    )


def load_native_self_hosted_smoke_contract() -> NativeSelfHostedSmokeContract:
    """Load the native smoke contract from DB authority."""
    return _load_native_self_hosted_smoke_contract_from_db()


async def _load_smoke_registry_async(
    *,
    env: Mapping[str, str],
    request_payload: Mapping[str, Any],
) -> RegistryResolver:
    workspace_ref = request_payload.get("workspace_ref")
    runtime_profile_ref = request_payload.get("runtime_profile_ref")
    if not isinstance(workspace_ref, str) or not workspace_ref.strip():
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_smoke_contract",
            "native smoke workflow_request.workspace_ref must be a non-empty string",
            details={"field": "native_smoke.workflow_request.workspace_ref"},
        )
    if not isinstance(runtime_profile_ref, str) or not runtime_profile_ref.strip():
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_smoke_contract",
            "native smoke workflow_request.runtime_profile_ref must be a non-empty string",
            details={"field": "native_smoke.workflow_request.runtime_profile_ref"},
        )

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_registry_authority_schema(conn)
        workspace_rows = await conn.fetch(
            """
            SELECT workspace_ref, repo_root, workdir
            FROM registry_workspace_authority
            WHERE workspace_ref = $1
            ORDER BY workspace_ref
            """,
            workspace_ref,
        )
        runtime_profile_rows = await conn.fetch(
            """
            SELECT runtime_profile_ref, model_profile_id, provider_policy_id, sandbox_profile_ref
            FROM registry_runtime_profile_authority
            WHERE runtime_profile_ref = $1
            ORDER BY runtime_profile_ref
            """,
            runtime_profile_ref,
        )
    finally:
        await conn.close()
    if not workspace_rows:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.registry_authority_missing",
            "native smoke registry workspace authority row is missing",
            details={"workspace_ref": workspace_ref},
        )
    if not runtime_profile_rows:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.registry_authority_missing",
            "native smoke registry runtime profile authority row is missing",
            details={"runtime_profile_ref": runtime_profile_ref},
        )
    workspace_records = [
        WorkspaceAuthorityRecord(
            workspace_ref=str(row["workspace_ref"]),
            # Native smoke can submit on the host while the worker executes in
            # docker. Physical repo_root/workdir values are materialization
            # details; the admitted context identity must stay host-agnostic.
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


def _load_smoke_registry(
    *,
    env: Mapping[str, str],
    request_payload: Mapping[str, Any],
) -> RegistryResolver:
    return _run_async(
        _load_smoke_registry_async(env=env, request_payload=request_payload),
        message=(
            "operator_flow.async_boundary_required: "
            "operator flow sync entrypoints require a non-async call boundary"
        ),
    )


def _require_database_ready(
    *,
    status: Mapping[str, Any],
    field_name: str,
) -> None:
    database_reachable = status.get("database_reachable")
    if not isinstance(database_reachable, bool):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_response",
            f"{field_name}.database_reachable must be a boolean",
            details={"field": f"{field_name}.database_reachable"},
        )
    if not database_reachable:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.database_unreachable",
            f"{field_name} did not prove the repo-local Postgres database was reachable",
            details={"field": field_name, "status": dict(status)},
        )

    schema_bootstrapped = status.get("schema_bootstrapped")
    if not isinstance(schema_bootstrapped, bool):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_response",
            f"{field_name}.schema_bootstrapped must be a boolean",
            details={"field": f"{field_name}.schema_bootstrapped"},
        )
    if not schema_bootstrapped:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.schema_not_bootstrapped",
            f"{field_name} did not prove the repo-local Postgres schema was bootstrapped",
            details={"field": field_name, "status": dict(status)},
        )


def _load_database_status(
    *,
    env: Mapping[str, str],
    field_name: str,
    bootstrap: bool,
) -> dict[str, Any]:
    payload = _require_frontdoor_mapping(
        frontdoor.health(env=env, bootstrap=bootstrap),
        field_name=field_name,
    )
    return dict(
        _require_frontdoor_mapping(
            payload.get("database"),
            field_name=f"{field_name}.database",
        )
    )


def _merge_run_envelopes(
    *,
    submit_run: Mapping[str, Any],
    status_run: Mapping[str, Any],
    admission_decision: object,
    inspection: object,
) -> dict[str, Any]:
    immutable_fields = {
        "run_id",
        "workflow_id",
        "request_id",
        "workflow_definition_id",
        "admitted_definition_hash",
    }
    merged_run = dict(submit_run)
    for key, value in status_run.items():
        if key in immutable_fields and key in merged_run and merged_run[key] != value:
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.run_mismatch",
                "frontdoor submit and status returned conflicting immutable run fields",
                details={
                    "field": key,
                    "submit_value": merged_run[key],
                    "status_value": value,
                },
            )
        merged_run[key] = value

    if admission_decision is not None:
        merged_run["admission_decision"] = dict(
            _require_frontdoor_mapping(
                admission_decision,
                field_name="submit_payload.admission_decision",
            )
        )
    if inspection is not None:
        merged_run["inspection"] = dict(
            _require_frontdoor_mapping(inspection, field_name="status_payload.inspection")
        )
    return merged_run


@contextmanager
def _temporary_process_env(env: Mapping[str, str]):
    original: dict[str, str | None] = {}
    for key, value in env.items():
        original[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, prior_value in original.items():
            if prior_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prior_value


def _normalize_execution_result(result: object) -> dict[str, Any]:
    if isinstance(result, Mapping):
        raw_current_state = result.get("current_state", result.get("status"))
        raw_terminal_reason = result.get("terminal_reason", result.get("terminal_reason_code"))
        raw_node_order = result.get("node_order", ())
    else:
        raw_current_state = getattr(result, "current_state", None)
        if raw_current_state is None:
            raw_current_state = getattr(result, "status", None)
        raw_terminal_reason = getattr(result, "terminal_reason", None)
        if raw_terminal_reason is None:
            raw_terminal_reason = getattr(result, "terminal_reason_code", None)
        if raw_terminal_reason is None:
            raw_terminal_reason = getattr(result, "reason_code", None)
        raw_node_order = getattr(result, "node_order", ())

    if hasattr(raw_current_state, "value"):
        raw_current_state = raw_current_state.value
    node_order = (
        [str(node_id) for node_id in raw_node_order]
        if isinstance(raw_node_order, (list, tuple))
        else []
    )
    return {
        "current_state": str(raw_current_state or "").strip() or None,
        "terminal_reason": (
            str(raw_terminal_reason).strip()
            if isinstance(raw_terminal_reason, str) and raw_terminal_reason.strip()
            else None
        ),
        "node_order": node_order,
    }


def _node_order_from_timeline(node_timeline: object) -> list[str]:
    node_order: list[str] = []
    if not isinstance(node_timeline, (list, tuple)):
        return node_order
    for entry in node_timeline:
        node_id = str(entry).split(":", 1)[0].strip()
        if node_id and node_id not in node_order:
            node_order.append(node_id)
    return node_order


def _wait_for_locked_smoke_execution(
    *,
    run_id: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    reader = PostgresEvidenceReader(env=env)
    deadline = time.monotonic() + _SMOKE_LOCK_WAIT_TIMEOUT_SECONDS
    while True:
        inspection = RuntimeOrchestrator(evidence_reader=reader).inspect_run(run_id=run_id)
        node_order = _node_order_from_timeline(inspection.node_timeline)
        if inspection.current_state in _SMOKE_TERMINAL_STATES:
            return {
                "current_state": inspection.current_state,
                "terminal_reason": inspection.terminal_reason,
                "node_order": node_order,
            }
        if time.monotonic() >= deadline:
            return {
                "current_state": _SMOKE_EXECUTION_LOCKED_STATE,
                "terminal_reason": "workflow.graph_run_already_locked",
                "node_order": node_order,
            }
        time.sleep(_SMOKE_LOCK_WAIT_INTERVAL_SECONDS)


def _execute_smoke_run(
    *,
    run_id: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    with _temporary_process_env(env):
        conn = SyncPostgresConnection(get_workflow_pool(env=env))
        try:
            result = _execute_admitted_graph_run(conn, run_id=run_id)
        finally:
            conn.close()
    execution = _normalize_execution_result(result)
    if execution["current_state"] == _SMOKE_EXECUTION_LOCKED_STATE:
        return _wait_for_locked_smoke_execution(run_id=run_id, env=env)
    return execution


def _load_smoke_proof(
    *,
    run_id: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    reader = PostgresEvidenceReader(env=env)
    canonical_evidence = tuple(reader.evidence_timeline(run_id))
    inspection = RuntimeOrchestrator(evidence_reader=reader).inspect_run(run_id=run_id)
    outbox_batch = PostgresWorkflowOutboxSubscriber(env=env).read_batch(
        run_id=run_id,
        limit=64,
    )
    node_order = _node_order_from_timeline(inspection.node_timeline)
    first_evidence_seq = canonical_evidence[0].evidence_seq if canonical_evidence else None
    last_evidence_seq = canonical_evidence[-1].evidence_seq if canonical_evidence else None
    last_outbox_row = outbox_batch.rows[-1] if outbox_batch.rows else None
    last_envelope = last_outbox_row.envelope if last_outbox_row is not None else {}
    if not isinstance(last_envelope, Mapping):
        last_envelope = {}
    return {
        "inspection": {
            "current_state": inspection.current_state,
            "terminal_reason": inspection.terminal_reason,
            "node_order": node_order,
            "node_timeline": list(inspection.node_timeline),
            "evidence_refs": list(inspection.evidence_refs),
            "completeness": {
                "is_complete": inspection.completeness.is_complete,
                "missing_evidence_refs": list(inspection.completeness.missing_evidence_refs),
            },
            "watermark": {
                "evidence_seq": inspection.watermark.evidence_seq,
                "source": inspection.watermark.source,
            },
        },
        "evidence": {
            "count": len(canonical_evidence),
            "first_evidence_seq": first_evidence_seq,
            "last_evidence_seq": last_evidence_seq,
        },
        "outbox": {
            "row_count": len(outbox_batch.rows),
            "cursor_last_evidence_seq": outbox_batch.cursor.last_evidence_seq,
            "has_more": outbox_batch.has_more,
            "first_authority_table": (
                outbox_batch.rows[0].authority_table if outbox_batch.rows else None
            ),
            "last_authority_table": (
                last_outbox_row.authority_table if last_outbox_row is not None else None
            ),
            "last_envelope_kind": (
                last_outbox_row.envelope_kind if last_outbox_row is not None else None
            ),
            "last_receipt_type": (
                str(last_envelope.get("receipt_type")).strip()
                if isinstance(last_envelope.get("receipt_type"), str)
                and str(last_envelope.get("receipt_type")).strip()
                else None
            ),
            "last_status": (
                str(last_envelope.get("status")).strip()
                if isinstance(last_envelope.get("status"), str)
                and str(last_envelope.get("status")).strip()
                else None
            ),
        },
    }


def _require_smoke_terminal_proof(
    *,
    status_run: Mapping[str, Any],
    execution: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> None:
    execution_state = execution.get("current_state")
    if execution_state != _SMOKE_REQUIRED_TERMINAL_STATE:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_execution_invalid",
            "native smoke execution did not report a succeeded terminal state",
            details={
                "expected_state": _SMOKE_REQUIRED_TERMINAL_STATE,
                "actual_state": execution_state,
                "terminal_reason": execution.get("terminal_reason"),
            },
        )

    inspection = proof.get("inspection")
    inspection_node_order = (
        list(inspection.get("node_order", ())) if isinstance(inspection, Mapping) else []
    )
    if inspection_node_order != list(_SMOKE_REQUIRED_NODE_ORDER):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_execution_invalid",
            "native smoke proof did not preserve the expected node order",
            details={
                "expected_node_order": list(_SMOKE_REQUIRED_NODE_ORDER),
                "actual_node_order": inspection_node_order,
            },
        )

    if not isinstance(inspection, Mapping) or inspection.get("current_state") != _SMOKE_REQUIRED_TERMINAL_STATE:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_proof_invalid",
            "native smoke inspection proof did not report a succeeded terminal state",
            details={"inspection": inspection},
        )

    evidence = proof.get("evidence")
    if not isinstance(evidence, Mapping) or int(evidence.get("count", 0) or 0) <= 0:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_proof_invalid",
            "native smoke proof must include canonical evidence rows",
            details={"evidence": evidence},
        )

    outbox = proof.get("outbox")
    if not isinstance(outbox, Mapping):
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_proof_invalid",
            "native smoke proof must include outbox replay evidence",
            details={"outbox": outbox},
        )
    if outbox.get("last_receipt_type") != _SMOKE_REQUIRED_RECEIPT_TYPE:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_proof_invalid",
            "native smoke must end with the canonical workflow completion receipt in outbox",
            details={
                "expected_receipt_type": _SMOKE_REQUIRED_RECEIPT_TYPE,
                "actual_receipt_type": outbox.get("last_receipt_type"),
                "actual_authority_table": outbox.get("last_authority_table"),
            },
        )
    if outbox.get("last_status") != _SMOKE_REQUIRED_TERMINAL_STATE:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.smoke_proof_invalid",
            "native smoke outbox proof did not record a succeeded terminal receipt",
            details={
                "expected_status": _SMOKE_REQUIRED_TERMINAL_STATE,
                "actual_status": outbox.get("last_status"),
            },
        )


def run_native_self_hosted_smoke(
    *,
    registry: RegistryResolver | None = None,
    frontdoor_service: _FrontdoorService | None = None,
) -> dict[str, Any]:
    """Execute the packaged native smoke sequence from DB authority."""

    contract = load_native_self_hosted_smoke_contract()
    request_payload = _isolate_smoke_request_payload(contract.request_payload)
    smoke_registry = registry
    if smoke_registry is None:
        smoke_registry = _load_smoke_registry(
            env=contract.runtime_env,
            request_payload=request_payload,
        )
    return run_local_operator_flow(
        request_payload=request_payload,
        env=contract.runtime_env,
        registry=smoke_registry,
        frontdoor_service=frontdoor_service,
    )


def run_local_operator_flow(
    *,
    request_payload: Mapping[str, Any],
    env: Mapping[str, str] | None = None,
    registry: RegistryResolver | None = None,
    frontdoor_service: _FrontdoorService | None = None,
) -> dict[str, Any]:
    """Execute the packaged local operator sequence."""

    source = _require_authority_env(env)
    instance_contract = dict(
        _require_frontdoor_mapping(
            native_ops.show_instance_contract(env=source),
            field_name="native_instance",
        )
    )
    bootstrap_status = _load_database_status(
        env=source,
        field_name="bootstrap",
        bootstrap=True,
    )
    health_status = _load_database_status(
        env=source,
        field_name="health",
        bootstrap=False,
    )
    _require_database_ready(status=bootstrap_status, field_name="bootstrap")
    _require_database_ready(status=health_status, field_name="health")

    service = frontdoor_service
    if service is None:
        if registry is None:
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.registry_missing",
                "operator flow submit requires explicit registry authority",
            )
        service = frontdoor.NativeWorkflowFrontdoor(registry=registry)

    submit_payload = _require_frontdoor_mapping(
        service.submit(request_payload=request_payload, env=source),
        field_name="submit_payload",
    )
    submit_instance = _require_frontdoor_mapping(
        submit_payload.get("native_instance"),
        field_name="submit_payload.native_instance",
    )
    if dict(submit_instance) != instance_contract:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.instance_mismatch",
            "frontdoor submit returned a different native instance contract",
            details={
                "expected": instance_contract,
                "actual": dict(submit_instance),
            },
        )

    submit_run = _require_frontdoor_mapping(
        submit_payload.get("run"),
        field_name="submit_payload.run",
    )
    run_id = submit_run.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.invalid_response",
            "submit payload run_id must be a non-empty string",
            details={"field": "submit_payload.run.run_id"},
        )

    execution = _execute_smoke_run(run_id=run_id, env=source)
    status_payload = _require_frontdoor_mapping(
        service.status(run_id=run_id, env=source),
        field_name="status_payload",
    )
    status_instance = _require_frontdoor_mapping(
        status_payload.get("native_instance"),
        field_name="status_payload.native_instance",
    )
    if dict(status_instance) != instance_contract:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.instance_mismatch",
            "frontdoor status returned a different native instance contract",
            details={
                "expected": instance_contract,
                "actual": dict(status_instance),
            },
        )

    status_run = _require_frontdoor_mapping(
        status_payload.get("run"),
        field_name="status_payload.run",
    )
    status_run_id = status_run.get("run_id")
    if status_run_id != run_id:
        raise frontdoor.NativeFrontdoorError(
            "operator_flow.run_mismatch",
            "frontdoor status did not round-trip the same run_id",
            details={
                "expected": run_id,
                "actual": status_run_id,
            },
        )
    proof = _load_smoke_proof(run_id=run_id, env=source)
    _require_smoke_terminal_proof(
        status_run=status_run,
        execution=execution,
        proof=proof,
    )
    inspection_payload = status_payload.get("inspection")
    if inspection_payload is None:
        inspection_payload = proof.get("inspection")

    return {
        "step_order": [
            "show_instance_contract",
            "db_bootstrap",
            "db_health",
            "submit",
            "execute",
            "status",
            "proof",
        ],
        "native_instance": instance_contract,
        "bootstrap": bootstrap_status,
        "health": health_status,
        "execution": execution,
        "proof": proof,
        "run": _merge_run_envelopes(
            submit_run=submit_run,
            status_run=status_run,
            admission_decision=submit_payload.get("admission_decision"),
            inspection=inspection_payload,
        ),
    }


__all__ = [
    "NativeSelfHostedSmokeContract",
    "load_native_self_hosted_smoke_contract",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
