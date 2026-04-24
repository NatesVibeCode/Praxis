"""Shared execution gateway for DB-backed operation catalog bindings."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import inspect
import json
import time
from typing import Any
from uuid import uuid4

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


@dataclass(frozen=True, slots=True)
class AuthorityGateway:
    """Universal CQRS front door over registered authority operations."""

    subsystems: Any

    def execute(self, operation_name: str, payload: Mapping[str, Any] | None = None) -> Any:
        return execute_operation_from_subsystems(
            self.subsystems,
            operation_name=operation_name,
            payload=payload,
        )

    def query(self, operation_name: str, payload: Mapping[str, Any] | None = None) -> Any:
        return execute_operation_from_subsystems(
            self.subsystems,
            operation_name=operation_name,
            payload=payload,
        )


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


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _binding_value(binding: Any, name: str, default: Any) -> Any:
    return getattr(binding, name, default)


def _idempotency_key(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None,
    input_hash: str,
) -> str | None:
    policy = str(_binding_value(binding, "idempotency_policy", "") or "").strip()
    if policy not in {"idempotent", "read_only"}:
        return None
    fields = _binding_value(binding, "idempotency_key_fields", ()) or ()
    source = dict(payload or {})
    if fields:
        material = {str(field): source.get(str(field)) for field in fields}
    else:
        material = {"input_hash": input_hash}
    return f"{binding.operation_ref}:{_stable_hash(material)}"


def _fetch_existing_idempotent_result(conn: Any, *, operation_ref: str, idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    row = conn.fetchrow(
        """
        SELECT receipt_id, result_payload
          FROM authority_operation_receipts
         WHERE operation_ref = $1
           AND idempotency_key = $2
           AND execution_status IN ('completed', 'replayed')
         ORDER BY created_at DESC
         LIMIT 1
        """,
        operation_ref,
        idempotency_key,
    )
    if row is None:
        return None
    payload = row.get("result_payload") if isinstance(row, Mapping) else row["result_payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    return dict(payload) if isinstance(payload, Mapping) else None


def _projection_freshness(result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        return {}
    raw = result.get("projection_freshness")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _result_authority_event_ids(result: Any) -> list[str]:
    if not isinstance(result, Mapping):
        return []
    raw = result.get("authority_event_ids")
    if not isinstance(raw, list):
        return []
    event_ids: list[str] = []
    for value in raw:
        if isinstance(value, str) and value.strip():
            event_ids.append(value.strip())
    return event_ids


def _receipt_payload(
    binding: ResolvedHttpOperationBinding,
    *,
    receipt_id: str,
    input_hash: str,
    output_hash: str | None,
    idempotency_key: str | None,
    execution_status: str,
    result_status: str | None,
    error_code: str | None,
    error_detail: str | None,
    event_ids: list[str],
    projection_freshness: Mapping[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    return {
        "receipt_id": receipt_id,
        "operation_ref": binding.operation_ref,
        "operation_name": binding.operation_name,
        "operation_kind": binding.operation_kind,
        "source_kind": binding.source_kind,
        "authority_ref": binding.authority_ref,
        "authority_domain_ref": _binding_value(binding, "authority_domain_ref", binding.authority_ref),
        "projection_ref": binding.projection_ref,
        "storage_target_ref": _binding_value(binding, "storage_target_ref", "praxis.primary_postgres"),
        "posture": binding.posture,
        "idempotency_policy": binding.idempotency_policy,
        "idempotency_key": idempotency_key,
        "binding_revision": binding.binding_revision,
        "decision_ref": binding.decision_ref,
        "execution_status": execution_status,
        "result_status": result_status,
        "error_code": error_code,
        "error_detail": error_detail,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "event_ids": list(event_ids),
        "projection_freshness": dict(projection_freshness),
        "duration_ms": duration_ms,
    }


def _insert_operation_receipt(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    receipt: Mapping[str, Any],
    result_payload: Any,
) -> None:
    conn.execute(
        """
        INSERT INTO authority_operation_receipts (
            receipt_id,
            operation_ref,
            operation_name,
            operation_kind,
            authority_domain_ref,
            authority_ref,
            projection_ref,
            storage_target_ref,
            input_hash,
            output_hash,
            idempotency_key,
            caller_ref,
            execution_status,
            result_status,
            error_code,
            error_detail,
            event_ids,
            projection_freshness,
            result_payload,
            duration_ms,
            binding_revision,
            decision_ref
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb,
            $19::jsonb, $20, $21, $22
        )
        """,
        receipt["receipt_id"],
        binding.operation_ref,
        binding.operation_name,
        binding.operation_kind,
        receipt["authority_domain_ref"],
        binding.authority_ref,
        binding.projection_ref,
        receipt["storage_target_ref"],
        receipt["input_hash"],
        receipt["output_hash"],
        receipt["idempotency_key"],
        "authority_gateway",
        receipt["execution_status"],
        receipt["result_status"],
        receipt["error_code"],
        receipt["error_detail"],
        json.dumps(receipt["event_ids"], sort_keys=True, default=str),
        json.dumps(receipt["projection_freshness"], sort_keys=True, default=str),
        json.dumps(result_payload, sort_keys=True, default=str),
        receipt["duration_ms"],
        binding.binding_revision,
        binding.decision_ref,
    )


def _insert_authority_event(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    receipt_id: str,
    input_hash: str,
    output_hash: str,
    idempotency_key: str | None,
    result_status: str | None,
) -> str:
    event_id = str(uuid4())
    authority_domain_ref = _binding_value(binding, "authority_domain_ref", binding.authority_ref)
    event_type = _binding_value(binding, "event_type", None) or binding.operation_name.replace(".", "_")
    payload = {
        "operation_name": binding.operation_name,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "result_status": result_status,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    conn.execute(
        """
        INSERT INTO authority_events (
            event_id,
            authority_domain_ref,
            aggregate_ref,
            event_type,
            event_payload,
            idempotency_key,
            operation_ref,
            receipt_id,
            emitted_by
        ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, $7, $8::uuid, $9)
        """,
        event_id,
        authority_domain_ref,
        binding.operation_ref,
        event_type,
        json.dumps(payload, sort_keys=True, default=str),
        idempotency_key,
        binding.operation_ref,
        receipt_id,
        "authority_gateway",
    )
    return event_id


def _attach_receipt_to_authority_events(
    conn: Any,
    *,
    receipt_id: str,
    event_ids: list[str],
) -> None:
    if not event_ids:
        return
    conn.execute(
        """
        UPDATE authority_events
           SET receipt_id = $1::uuid
         WHERE event_id = ANY($2::uuid[])
           AND receipt_id IS NULL
        """,
        receipt_id,
        event_ids,
    )


def _persist_operation_outcome(
    conn: Any,
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None,
    result: Any,
    input_hash: str,
    idempotency_key: str | None,
    started_ns: int,
    execution_status: str = "completed",
    error_code: str | None = None,
    error_detail: str | None = None,
) -> dict[str, Any]:
    output_hash = None if result is None else _stable_hash(result)
    result_status = result.get("status") if isinstance(result, Mapping) else None
    normalized_result_status = str(result_status).strip() if isinstance(result_status, str) else None
    projection_freshness = _projection_freshness(result)
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    receipt_id = str(uuid4())
    event_ids: list[str] = _result_authority_event_ids(result)
    receipt = _receipt_payload(
        binding,
        receipt_id=receipt_id,
        input_hash=input_hash,
        output_hash=output_hash,
        idempotency_key=idempotency_key,
        execution_status=execution_status,
        result_status=normalized_result_status,
        error_code=error_code,
        error_detail=error_detail,
        event_ids=event_ids,
        projection_freshness=projection_freshness,
        duration_ms=duration_ms,
    )
    if _binding_value(binding, "receipt_required", True):
        _insert_operation_receipt(conn, binding, receipt=receipt, result_payload=result)
    if execution_status == "completed" and event_ids:
        _attach_receipt_to_authority_events(conn, receipt_id=receipt_id, event_ids=event_ids)
    if (
        execution_status == "completed"
        and binding.operation_kind == "command"
        and _binding_value(binding, "event_required", True)
        and not event_ids
    ):
        event_ids.append(
            _insert_authority_event(
                conn,
                binding,
                receipt_id=receipt_id,
                input_hash=input_hash,
                output_hash=output_hash or "",
                idempotency_key=idempotency_key,
                result_status=normalized_result_status,
            )
        )
        receipt = dict(receipt)
        receipt["event_ids"] = event_ids
        conn.execute(
            """
            UPDATE authority_operation_receipts
               SET event_ids = $2::jsonb
             WHERE receipt_id = $1::uuid
            """,
            receipt_id,
            json.dumps(event_ids, sort_keys=True, default=str),
        )
    return dict(receipt)


def _operation_receipt(
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


def _with_operation_receipt(
    binding: ResolvedHttpOperationBinding,
    result: Any,
    *,
    receipt: Mapping[str, Any] | None = None,
) -> Any:
    operation_receipt = dict(receipt) if isinstance(receipt, Mapping) else _operation_receipt(
        binding,
        result=result if isinstance(result, Mapping) else None,
    )
    if isinstance(result, Mapping):
        payload = dict(result)
        if "ok" not in payload:
            payload["ok"] = True
        if binding.operation_kind == "query" and "results" not in payload:
            # If a query returns a mapping that isn't 'results' but contains a list,
            # we should encourage the handler to use 'results', but as a fallback
            # we don't force it here yet to avoid breaking very specific contracts.
            pass
        payload["operation_receipt"] = operation_receipt
        return payload

    if binding.operation_kind == "query" and isinstance(result, list):
        return {
            "ok": True,
            "results": result,
            "operation_receipt": operation_receipt,
        }

    return {
        "ok": True,
        "result": result,
        "operation_receipt": operation_receipt,
    }


async def _await_handler_result(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


def _run_awaitable_sync(result: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(result)
    raise RuntimeError(
        "operation.sync_execution_in_async_boundary: "
        "use aexecute_operation_binding() when invoking async operation handlers "
        "from an active event loop"
    )


async def aexecute_operation_binding(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
    subsystems: Any,
) -> Any:
    conn = subsystems.get_pg_conn()
    input_hash = _stable_hash(payload or {})
    idempotency_key = _idempotency_key(binding, payload=payload, input_hash=input_hash)
    cached = _fetch_existing_idempotent_result(
        conn,
        operation_ref=binding.operation_ref,
        idempotency_key=idempotency_key,
    )
    if cached is not None:
        return cached
    command = build_operation_command(binding, payload=payload)
    started_ns = time.monotonic_ns()
    try:
        result = await _await_handler_result(binding.handler(command, subsystems))
    except Exception as exc:
        _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=None,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="failed",
            error_code=type(exc).__name__,
            error_detail=str(exc),
        )
        return {
            "ok": False,
            "error": str(exc),
            "error_code": type(exc).__name__,
        }
    receipt = _persist_operation_outcome(
        conn,
        binding,
        payload=payload,
        result=result,
        input_hash=input_hash,
        idempotency_key=idempotency_key,
        started_ns=started_ns,
    )
    return _with_operation_receipt(binding, result, receipt=receipt)


def execute_operation_binding(
    binding: ResolvedHttpOperationBinding,
    *,
    payload: Mapping[str, Any] | None = None,
    subsystems: Any,
) -> Any:
    conn = subsystems.get_pg_conn()
    input_hash = _stable_hash(payload or {})
    idempotency_key = _idempotency_key(binding, payload=payload, input_hash=input_hash)
    cached = _fetch_existing_idempotent_result(
        conn,
        operation_ref=binding.operation_ref,
        idempotency_key=idempotency_key,
    )
    if cached is not None:
        return cached
    command = build_operation_command(binding, payload=payload)
    started_ns = time.monotonic_ns()
    try:
        result = binding.handler(command, subsystems)
        if inspect.isawaitable(result):
            result = _run_awaitable_sync(result)
    except Exception as exc:
        _persist_operation_outcome(
            conn,
            binding,
            payload=payload,
            result=None,
            input_hash=input_hash,
            idempotency_key=idempotency_key,
            started_ns=started_ns,
            execution_status="failed",
            error_code=type(exc).__name__,
            error_detail=str(exc),
        )
        return {
            "ok": False,
            "error": str(exc),
            "error_code": type(exc).__name__,
        }
    receipt = _persist_operation_outcome(
        conn,
        binding,
        payload=payload,
        result=result,
        input_hash=input_hash,
        idempotency_key=idempotency_key,
        started_ns=started_ns,
    )
    return _with_operation_receipt(binding, result, receipt=receipt)


async def aexecute_operation_from_subsystems(
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
    return await aexecute_operation_binding(
        binding,
        payload=payload,
        subsystems=subsystems,
    )


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
    "AuthorityGateway",
    "aexecute_operation_binding",
    "aexecute_operation_from_subsystems",
    "build_operation_command",
    "execute_operation_binding",
    "execute_operation_from_env",
    "execute_operation_from_subsystems",
    "resolve_named_operation_binding",
]
