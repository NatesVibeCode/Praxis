"""Shared operation-catalog contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

OPERATION_CATALOG_CONTRACT_VERSION = 1
OPERATION_CATALOG_QUERY_PATH = "/api/catalog/operations"
OPERATION_KINDS = frozenset({"command", "query"})
OPERATION_SOURCE_KINDS = frozenset({"operation_command", "operation_query"})
OPERATION_POSTURES = frozenset({"observe", "operate", "build"})
OPERATION_IDEMPOTENCY_POLICIES = frozenset({"non_idempotent", "idempotent", "read_only"})


def _normalize_enum(
    value: object,
    *,
    field_name: str,
    allowed: frozenset[str],
    allow_none: bool = False,
) -> str | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name} must be provided")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized


def normalize_operation_kind(value: object, *, field_name: str = "operation_kind") -> str:
    return str(_normalize_enum(value, field_name=field_name, allowed=OPERATION_KINDS))


def normalize_operation_source_kind(value: object, *, field_name: str = "source_kind") -> str:
    return str(_normalize_enum(value, field_name=field_name, allowed=OPERATION_SOURCE_KINDS))


def normalize_operation_posture(
    value: object | None,
    *,
    field_name: str = "posture",
    allow_none: bool = False,
) -> str | None:
    return _normalize_enum(
        value,
        field_name=field_name,
        allowed=OPERATION_POSTURES,
        allow_none=allow_none,
    )


def normalize_operation_idempotency_policy(
    value: object | None,
    *,
    field_name: str = "idempotency_policy",
    allow_none: bool = False,
) -> str | None:
    return _normalize_enum(
        value,
        field_name=field_name,
        allowed=OPERATION_IDEMPOTENCY_POLICIES,
        allow_none=allow_none,
    )


def _normalize_iso(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def operation_catalog_contract_descriptor() -> dict[str, Any]:
    return {
        "name": "operation_catalog",
        "version": OPERATION_CATALOG_CONTRACT_VERSION,
        "query_path": OPERATION_CATALOG_QUERY_PATH,
        "item_fields": [
            "operation_ref",
            "operation_name",
            "source_kind",
            "operation_kind",
            "http_method",
            "http_path",
            "input_model_ref",
            "handler_ref",
            "authority_ref",
            "projection_ref",
            "posture",
            "idempotency_policy",
            "enabled",
            "operation_enabled",
            "source_policy_ref",
            "source_policy_enabled",
            "binding_revision",
            "decision_ref",
        ],
        "source_policy_fields": [
            "policy_ref",
            "source_kind",
            "posture",
            "idempotency_policy",
            "enabled",
            "binding_revision",
            "decision_ref",
        ],
    }


def build_operation_catalog_response(
    *,
    operations: Sequence[Mapping[str, Any]],
    source_policies: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    generated_at_iso = _normalize_iso(generated_at)
    return {
        "routed_to": "operation_catalog",
        "contract_version": OPERATION_CATALOG_CONTRACT_VERSION,
        "contract": operation_catalog_contract_descriptor(),
        "generated_at": generated_at_iso,
        "operations": [dict(operation) for operation in operations],
        "count": len(operations),
        "source_policies": [dict(policy) for policy in source_policies],
        "source_policy_count": len(source_policies),
    }


__all__ = [
    "OPERATION_CATALOG_CONTRACT_VERSION",
    "OPERATION_CATALOG_QUERY_PATH",
    "OPERATION_IDEMPOTENCY_POLICIES",
    "OPERATION_KINDS",
    "OPERATION_POSTURES",
    "OPERATION_SOURCE_KINDS",
    "build_operation_catalog_response",
    "normalize_operation_idempotency_policy",
    "normalize_operation_kind",
    "normalize_operation_posture",
    "normalize_operation_source_kind",
    "operation_catalog_contract_descriptor",
]
