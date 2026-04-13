"""Smoke service functions: DB contract loading and local smoke flow execution."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from registry.domain import RegistryResolver
from registry.repository import bootstrap_registry_authority_schema, load_registry_resolver
from runtime.instance import (
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
)
from storage.postgres import connect_workflow_database
from surfaces.api import frontdoor, native_ops
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
    "WORKFLOW_DATABASE_URL": os.environ["WORKFLOW_DATABASE_URL"],
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR": "Code&DBs/Databases/postgres-dev/data",
    PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: "config/runtime_profiles.json",
}
_SMOKE_PATH_ENV_NAMES = {
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR",
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
}
def _default_smoke_runtime_env() -> dict[str, str]:
    source_env = __import__("os").environ
    raw_env = {
        name: str(source_env.get(name, default_value))
        for name, default_value in _SMOKE_DEFAULT_RUNTIME_ENV.items()
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
        return await load_registry_resolver(
            conn,
            workspace_refs=(workspace_ref,),
            runtime_profile_refs=(runtime_profile_ref,),
        )
    finally:
        await conn.close()


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


def _merge_run_envelopes(
    *,
    submit_run: Mapping[str, Any],
    status_run: Mapping[str, Any],
    admission_decision: object,
    inspection: object,
) -> dict[str, Any]:
    merged_run = dict(status_run)
    for key, value in submit_run.items():
        if key in merged_run and merged_run[key] != value:
            raise frontdoor.NativeFrontdoorError(
                "operator_flow.run_mismatch",
                "frontdoor submit and status returned conflicting run fields",
                details={
                    "field": key,
                    "submit_value": value,
                    "status_value": merged_run[key],
                },
            )
        merged_run.setdefault(key, value)

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
    bootstrap_status = dict(
        _require_frontdoor_mapping(
            native_ops.db_bootstrap(env=source),
            field_name="bootstrap",
        )
    )
    health_status = dict(
        _require_frontdoor_mapping(
            native_ops.db_health(env=source),
            field_name="health",
        )
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
        service = frontdoor.NativeDagFrontdoor(registry=registry)

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

    return {
        "step_order": [
            "show_instance_contract",
            "db_bootstrap",
            "db_health",
            "submit",
            "status",
        ],
        "native_instance": instance_contract,
        "bootstrap": bootstrap_status,
        "health": health_status,
        "run": _merge_run_envelopes(
            submit_run=submit_run,
            status_run=status_run,
            admission_decision=submit_payload.get("admission_decision"),
            inspection=status_payload.get("inspection"),
        ),
    }


__all__ = [
    "NativeSelfHostedSmokeContract",
    "load_native_self_hosted_smoke_contract",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
