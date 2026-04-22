"""Resolve operation-catalog metadata into live HTTP bindings."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import importlib
from typing import Any, Callable

from pydantic import BaseModel

from runtime.operation_catalog import ResolvedOperationDefinition


class OperationBindingResolutionError(RuntimeError):
    """Raised when an operation-catalog binding cannot be resolved safely."""


@dataclass(frozen=True, slots=True)
class ResolvedHttpOperationBinding:
    operation_ref: str
    operation_name: str
    source_kind: str
    operation_kind: str
    http_method: str
    http_path: str
    command_class: type[BaseModel]
    handler: Callable[..., Any]
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
    binding_revision: str
    decision_ref: str
    summary: str


def _resolve_from_module(module: Any, attrs: tuple[str, ...], *, reference: str) -> Any:
    resolved = module
    for attr in attrs:
        if not hasattr(resolved, attr):
            raise OperationBindingResolutionError(
                f"Reference '{reference}' is missing attribute '{attr}'",
            )
        resolved = getattr(resolved, attr)
    return resolved


@lru_cache(maxsize=256)
def resolve_python_reference(reference: str) -> Any:
    """Resolve a dotted module/object reference into a live Python object."""

    if not isinstance(reference, str) or not reference.strip():
        raise OperationBindingResolutionError("reference must be a non-empty string")

    parts = tuple(part for part in reference.strip().split(".") if part)
    if len(parts) < 2:
        raise OperationBindingResolutionError(
            f"Reference '{reference}' must include a module and object path",
        )

    for index in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:index])
        attr_path = parts[index:]
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        try:
            return _resolve_from_module(module, attr_path, reference=reference)
        except OperationBindingResolutionError:
            raise
        except Exception as exc:  # pragma: no cover - defensive guard
            raise OperationBindingResolutionError(
                f"Reference '{reference}' failed during attribute resolution",
            ) from exc

    raise OperationBindingResolutionError(
        f"Reference '{reference}' could not be imported",
    )


def _resolve_command_class(reference: str) -> type[BaseModel]:
    candidate = resolve_python_reference(reference)
    if not isinstance(candidate, type) or not issubclass(candidate, BaseModel):
        raise OperationBindingResolutionError(
            f"Reference '{reference}' did not resolve to a Pydantic model class",
        )
    return candidate


def _resolve_handler(reference: str) -> Callable[..., Any]:
    candidate = resolve_python_reference(reference)
    if not callable(candidate):
        raise OperationBindingResolutionError(
            f"Reference '{reference}' did not resolve to a callable handler",
        )
    return candidate


def resolve_http_operation_binding(
    definition: ResolvedOperationDefinition,
) -> ResolvedHttpOperationBinding:
    return ResolvedHttpOperationBinding(
        operation_ref=definition.operation_ref,
        operation_name=definition.operation_name,
        source_kind=definition.source_kind,
        operation_kind=definition.operation_kind,
        http_method=definition.http_method,
        http_path=definition.http_path,
        command_class=_resolve_command_class(definition.input_model_ref),
        handler=_resolve_handler(definition.handler_ref),
        authority_ref=definition.authority_ref,
        authority_domain_ref=definition.authority_domain_ref,
        projection_ref=definition.projection_ref,
        storage_target_ref=definition.storage_target_ref,
        input_schema_ref=definition.input_schema_ref,
        output_schema_ref=definition.output_schema_ref,
        idempotency_key_fields=list(definition.idempotency_key_fields),
        required_capabilities=dict(definition.required_capabilities),
        allowed_callers=list(definition.allowed_callers),
        timeout_ms=definition.timeout_ms,
        receipt_required=definition.receipt_required,
        event_required=definition.event_required,
        event_type=definition.event_type,
        projection_freshness_policy_ref=definition.projection_freshness_policy_ref,
        posture=definition.posture,
        idempotency_policy=definition.idempotency_policy,
        binding_revision=definition.binding_revision,
        decision_ref=definition.decision_ref,
        summary=definition.operation_name,
    )


__all__ = [
    "OperationBindingResolutionError",
    "ResolvedHttpOperationBinding",
    "resolve_http_operation_binding",
    "resolve_python_reference",
]
