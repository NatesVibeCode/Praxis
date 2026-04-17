"""Shared execution gateway for DB-backed operation catalog bindings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from storage.postgres import SyncPostgresConnection, get_workflow_pool

from .operation_catalog import get_resolved_operation_definition
from .operation_catalog_bindings import (
    ResolvedHttpOperationBinding,
    resolve_http_operation_binding,
)


@dataclass(slots=True)
class _EnvironmentBackedSubsystems:
    env: Mapping[str, str]
    conn: Any

    def get_pg_conn(self) -> Any:
        return self.conn

    def _postgres_env(self) -> dict[str, str]:
        return dict(self.env)


def resolve_named_operation_binding(
    conn: Any,
    *,
    operation_name: str,
) -> ResolvedHttpOperationBinding:
    definition = get_resolved_operation_definition(conn, operation_name=operation_name)
    return resolve_http_operation_binding(definition)


def build_operation_command(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    command_data = dict(payload or {})
    return binding.command_class(**command_data)


def _command_receipt(
    binding: ResolvedHttpOperationBinding,
    *,
    result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    result_status = result.get("status") if isinstance(result, Mapping) else None
    return {
        "operation_ref": binding.operation_ref,
        "operation_name": binding.operation_name,
        "operation_kind": binding.operation_kind,
        "source_kind": binding.source_kind,
        "authority_ref": binding.authority_ref,
        "projection_ref": binding.projection_ref,
        "posture": binding.posture,
        "idempotency_policy": binding.idempotency_policy,
        "binding_revision": binding.binding_revision,
        "decision_ref": binding.decision_ref,
        "execution_status": "completed",
        "result_status": str(result_status).strip() if isinstance(result_status, str) else None,
    }


def _with_command_receipt(
    binding: ResolvedHttpOperationBinding,
    result: Any,
) -> Any:
    if binding.operation_kind != "command":
        return result
    if isinstance(result, Mapping):
        payload = dict(result)
        payload["command_receipt"] = _command_receipt(binding, result=payload)
        return payload
    return {
        "result": result,
        "command_receipt": _command_receipt(binding, result=None),
    }


def execute_operation_binding(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
    subsystems: Any,
) -> Any:
    command = build_operation_command(binding, payload=payload)
    result = binding.handler(command, subsystems)
    return _with_command_receipt(binding, result)


def execute_operation_from_subsystems(
    subsystems: Any,
    *,
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    if not hasattr(subsystems, "get_pg_conn") or not callable(subsystems.get_pg_conn):
        raise RuntimeError("operation execution requires subsystems.get_pg_conn()")
    binding = resolve_named_operation_binding(
        subsystems.get_pg_conn(),
        operation_name=operation_name,
    )
    return execute_operation_binding(
        binding,
        payload=payload,
        subsystems=subsystems,
    )


def execute_operation_from_env(
    *,
    env: Mapping[str, str],
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
) -> Any:
    source = dict(env)
    conn = SyncPostgresConnection(get_workflow_pool(env=source))
    subsystems = _EnvironmentBackedSubsystems(env=source, conn=conn)
    return execute_operation_from_subsystems(
        subsystems,
        operation_name=operation_name,
        payload=payload,
    )


__all__ = [
    "build_operation_command",
    "execute_operation_binding",
    "execute_operation_from_env",
    "execute_operation_from_subsystems",
    "resolve_named_operation_binding",
]
