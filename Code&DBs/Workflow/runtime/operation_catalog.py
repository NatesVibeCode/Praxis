"""Runtime boundary for operation-catalog authority reads."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from contracts.operation_catalog import (
    DEFAULT_OPERATION_EXECUTION_LANE,
    DEFAULT_OPERATION_KICKOFF_REQUIRED,
)
from storage.postgres.operation_catalog_repository import (
    list_operation_catalog_records as _list_operation_catalog_records,
    list_operation_source_policy_records as _list_operation_source_policy_records,
    load_operation_catalog_record as _load_operation_catalog_record,
    load_operation_catalog_record_by_name as _load_operation_catalog_record_by_name,
)
from storage.postgres.validators import PostgresWriteError


@dataclass(frozen=True, slots=True)
class OperationCatalogRecord:
    operation_ref: str
    operation_name: str
    source_kind: str
    operation_kind: str
    http_method: str
    http_path: str
    input_model_ref: str
    handler_ref: str
    authority_ref: str
    authority_domain_ref: str
    projection_ref: str | None
    storage_target_ref: str
    input_schema_ref: str
    output_schema_ref: str
    idempotency_key_fields: list[Any]
    required_capabilities: dict[str, Any]
    allowed_callers: list[Any]
    timeout_ms: int
    receipt_required: bool
    event_required: bool
    event_type: str | None
    projection_freshness_policy_ref: str | None
    posture: str | None
    idempotency_policy: str | None
    execution_lane: str
    kickoff_required: bool
    enabled: bool
    binding_revision: str
    decision_ref: str


@dataclass(frozen=True, slots=True)
class OperationSourcePolicyRecord:
    policy_ref: str
    source_kind: str
    posture: str
    idempotency_policy: str
    enabled: bool
    binding_revision: str
    decision_ref: str


@dataclass(frozen=True, slots=True)
class ResolvedOperationDefinition:
    operation_ref: str
    operation_name: str
    source_kind: str
    operation_kind: str
    http_method: str
    http_path: str
    input_model_ref: str
    handler_ref: str
    authority_ref: str
    authority_domain_ref: str
    projection_ref: str | None
    storage_target_ref: str
    input_schema_ref: str
    output_schema_ref: str
    idempotency_key_fields: list[Any]
    required_capabilities: dict[str, Any]
    allowed_callers: list[Any]
    timeout_ms: int
    receipt_required: bool
    event_required: bool
    event_type: str | None
    projection_freshness_policy_ref: str | None
    posture: str
    idempotency_policy: str
    execution_lane: str
    kickoff_required: bool
    enabled: bool
    operation_enabled: bool
    source_policy_ref: str | None
    source_policy_enabled: bool | None
    binding_revision: str
    decision_ref: str


class OperationCatalogBoundaryError(RuntimeError):
    """Raised when operation catalog ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise OperationCatalogBoundaryError(str(exc), status_code=status_code) from exc


def _canonicalize_operation_catalog_row(row: dict[str, Any]) -> dict[str, Any]:
    """Project known route repairs before surfaces consume raw catalog rows.

    Migration 314 canonicalized ``compile_materialize`` from the legacy
    ``/api/compile_materialize`` path to the compile-family front door
    ``/api/compile/materialize``. Some runtimes can lag the migration, so the
    read surface repairs that one stale row at projection time instead of
    letting route discovery and capability mounting diverge.
    """

    repaired = deepcopy(row)
    repaired.setdefault("execution_lane", DEFAULT_OPERATION_EXECUTION_LANE)
    repaired.setdefault("kickoff_required", DEFAULT_OPERATION_KICKOFF_REQUIRED)

    if (
        row.get("operation_ref") == "compile.materialize"
        and row.get("operation_name") == "compile_materialize"
        and row.get("http_path") == "/api/compile_materialize"
    ):
        repaired["http_path"] = "/api/compile/materialize"
    if (
        row.get("operation_ref") == "compose-plan"
        and row.get("operation_name") == "compose_plan"
        and (
            row.get("execution_lane") != "interactive"
            or row.get("kickoff_required") is not False
            or int(row.get("timeout_ms") or 0) < 35000
        )
    ):
        # Migration 373 makes this durable. Keep the projection repaired for
        # already-booted catalogs that still carry migration 353's broad
        # background-only classification.
        repaired["execution_lane"] = "interactive"
        repaired["kickoff_required"] = False
        repaired["timeout_ms"] = 35000
    return repaired


def _operation_record_from_row(row: dict[str, Any]) -> OperationCatalogRecord:
    return OperationCatalogRecord(**_canonicalize_operation_catalog_row(row))


def _source_policy_record_from_row(row: dict[str, Any]) -> OperationSourcePolicyRecord:
    return OperationSourcePolicyRecord(**row)


def _resolve_operation_definition(
    record: OperationCatalogRecord,
    *,
    source_policy: OperationSourcePolicyRecord | None,
) -> ResolvedOperationDefinition:
    posture = record.posture or (source_policy.posture if source_policy else None)
    idempotency_policy = record.idempotency_policy or (
        source_policy.idempotency_policy if source_policy else None
    )
    if posture is None or idempotency_policy is None:
        raise OperationCatalogBoundaryError(
            f"Operation {record.operation_name} is missing source-policy defaults",
            status_code=500,
        )

    source_policy_enabled = source_policy.enabled if source_policy else None
    enabled = record.enabled and (source_policy_enabled if source_policy_enabled is not None else True)
    return ResolvedOperationDefinition(
        operation_ref=record.operation_ref,
        operation_name=record.operation_name,
        source_kind=record.source_kind,
        operation_kind=record.operation_kind,
        http_method=record.http_method,
        http_path=record.http_path,
        input_model_ref=record.input_model_ref,
        handler_ref=record.handler_ref,
        authority_ref=record.authority_ref,
        authority_domain_ref=record.authority_domain_ref,
        projection_ref=record.projection_ref,
        storage_target_ref=record.storage_target_ref,
        input_schema_ref=record.input_schema_ref,
        output_schema_ref=record.output_schema_ref,
        idempotency_key_fields=record.idempotency_key_fields,
        required_capabilities=record.required_capabilities,
        allowed_callers=record.allowed_callers,
        timeout_ms=record.timeout_ms,
        receipt_required=record.receipt_required,
        event_required=record.event_required,
        event_type=record.event_type,
        projection_freshness_policy_ref=record.projection_freshness_policy_ref,
        posture=posture,
        idempotency_policy=idempotency_policy,
        execution_lane=record.execution_lane,
        kickoff_required=record.kickoff_required,
        enabled=enabled,
        operation_enabled=record.enabled,
        source_policy_ref=source_policy.policy_ref if source_policy else None,
        source_policy_enabled=source_policy_enabled,
        binding_revision=record.binding_revision,
        decision_ref=record.decision_ref,
    )


def list_operation_catalog_records(
    conn: Any,
    *,
    source_kind: str | None = None,
    include_disabled: bool = False,
    limit: int = 100,
) -> list[OperationCatalogRecord]:
    try:
        rows = _list_operation_catalog_records(
            conn,
            source_kind=source_kind,
            include_disabled=include_disabled,
            limit=limit,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    return [_operation_record_from_row(row) for row in rows]


def list_operation_source_policies(
    conn: Any,
    *,
    include_disabled: bool = False,
    limit: int = 100,
) -> list[OperationSourcePolicyRecord]:
    try:
        rows = _list_operation_source_policy_records(
            conn,
            include_disabled=include_disabled,
            limit=limit,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    return [_source_policy_record_from_row(row) for row in rows]


def get_operation_catalog_record(
    conn: Any,
    *,
    operation_ref: str | None = None,
    operation_name: str | None = None,
) -> OperationCatalogRecord:
    if bool(operation_ref) == bool(operation_name):
        raise OperationCatalogBoundaryError(
            "exactly one of operation_ref or operation_name must be provided",
            status_code=400,
        )
    try:
        row = (
            _load_operation_catalog_record(conn, operation_ref=str(operation_ref))
            if operation_ref
            else _load_operation_catalog_record_by_name(conn, operation_name=str(operation_name))
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        missing_value = operation_ref or operation_name or "<unknown>"
        raise OperationCatalogBoundaryError(
            f"Operation not found: {missing_value}",
            status_code=404,
        )
    return _operation_record_from_row(row)


def get_resolved_operation_definition(
    conn: Any,
    *,
    operation_ref: str | None = None,
    operation_name: str | None = None,
) -> ResolvedOperationDefinition:
    record = get_operation_catalog_record(
        conn,
        operation_ref=operation_ref,
        operation_name=operation_name,
    )
    policies = {
        policy.source_kind: policy
        for policy in list_operation_source_policies(conn, include_disabled=True)
    }
    return _resolve_operation_definition(record, source_policy=policies.get(record.source_kind))


def list_resolved_operation_definitions(
    conn: Any,
    *,
    include_disabled: bool = False,
    limit: int = 100,
) -> list[ResolvedOperationDefinition]:
    records = list_operation_catalog_records(
        conn,
        include_disabled=True,
        limit=limit,
    )
    policies = {
        policy.source_kind: policy
        for policy in list_operation_source_policies(conn, include_disabled=True, limit=limit)
    }
    resolved = [
        _resolve_operation_definition(record, source_policy=policies.get(record.source_kind))
        for record in records
    ]
    if include_disabled:
        return resolved
    return [record for record in resolved if record.enabled]


__all__ = [
    "OperationCatalogBoundaryError",
    "OperationCatalogRecord",
    "OperationSourcePolicyRecord",
    "ResolvedOperationDefinition",
    "get_operation_catalog_record",
    "get_resolved_operation_definition",
    "list_operation_catalog_records",
    "list_operation_source_policies",
    "list_resolved_operation_definitions",
]
