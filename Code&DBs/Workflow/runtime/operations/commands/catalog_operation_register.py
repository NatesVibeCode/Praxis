"""Runtime CQRS-registration wizard.

Exposes the migration-only ``register_operation_atomic`` SQL helper as two
gateway-dispatched catalog operations so agents (and operators) can register
or retire operations from CLI / MCP / HTTP without authoring a migration.

The standing-order foot-gun this wizard exists to dodge:

  * Past migrations have shipped operation_catalog_registry rows whose
    ``handler_ref`` pointed at Python modules that didn't exist (BUG class:
    "API startup degrades when operation catalog bindings point at missing
    runtime exports"). The migration applied; API startup degraded.

  * The ``register_operation_atomic`` helper itself shipped with two
    sibling bugs (BUG-110F4EA3 + BUG-ECD0E5B3) that silently broke every
    query-op registration until 2026-04-26.

So this wizard refuses to land catalog rows whose ``handler_ref`` or
``input_model_ref`` cannot be import-resolved at registration time. The
class of foot-guns that historically required an API restart to surface is
caught here, before the row exists.

Idempotency: ``idempotent`` — same payload returns the cached gateway
receipt (the helper's ``ON CONFLICT DO UPDATE`` makes re-registration
side-effect-free anyway).
"""

from __future__ import annotations

import importlib
from typing import Any

from pydantic import BaseModel, Field, field_validator


_CATALOG_OPERATION_DECISION_REF = (
    "decision.architecture_policy.platform_architecture."
    "conceptual_events_register_through_operation_catalog_registry"
)


class RegisterOperationCommand(BaseModel):
    """Mirrors the ``register_operation_atomic`` SQL helper params, minus the
    fields that are derivable or have non-LLM-friendly defaults.

    ``handler_ref`` and ``input_model_ref`` MUST be import-resolvable in the
    server process — the handler validates this before writing rows, so a
    fabricated ref fails fast instead of degrading API startup later.
    """

    operation_ref: str = Field(min_length=1)
    operation_name: str = Field(min_length=1)
    handler_ref: str = Field(min_length=1)
    input_model_ref: str = Field(min_length=1)
    authority_domain_ref: str = Field(min_length=1)
    operation_kind: str = "command"
    posture: str = "operate"
    idempotency_policy: str = "non_idempotent"
    event_type: str | None = None
    event_required: bool | None = None
    label: str | None = None
    summary: str | None = None
    decision_ref: str = _CATALOG_OPERATION_DECISION_REF
    owner_ref: str = "praxis.engine"
    storage_target_ref: str = "praxis.primary_postgres"
    http_method: str = "POST"
    http_path: str | None = None

    @field_validator("operation_kind")
    @classmethod
    def _check_kind(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"command", "query"}:
            raise ValueError(f"operation_kind must be 'command' or 'query'; got {value!r}")
        return normalized

    @field_validator("idempotency_policy")
    @classmethod
    def _check_idempotency(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"non_idempotent", "idempotent", "read_only"}:
            raise ValueError(
                f"idempotency_policy must be one of "
                f"'non_idempotent', 'idempotent', 'read_only'; got {value!r}"
            )
        return normalized


class RetireOperationCommand(BaseModel):
    """Soft-retires a catalog operation.

    Sets ``operation_catalog_registry.enabled = FALSE`` so the gateway stops
    binding it, and flips the matching ``authority_object_registry``
    row's ``lifecycle_status`` to ``deprecated``. The rows remain for
    audit-trail / replay continuity; physical deletion is not supported here.
    """

    operation_ref: str = Field(min_length=1)
    reason_code: str = "catalog.operation.retired"
    operator_message: str | None = None


def _resolve_import_ref(ref: str) -> tuple[bool, str]:
    """Try to resolve a dotted ``'pkg.mod.attr'`` reference.

    Returns (ok, reason). On success reason is empty; on failure reason is a
    short structured message identifying which step failed.
    """

    cleaned = (ref or "").strip()
    if not cleaned or "." not in cleaned:
        return False, f"ref {ref!r} must be a dotted module.attribute path"
    module_path, _, attr_name = cleaned.rpartition(".")
    if not module_path or not attr_name:
        return False, f"ref {ref!r} must split into module + attribute"
    try:
        module = importlib.import_module(module_path)
    except Exception as exc:  # noqa: BLE001 — surface any import-time failure
        return False, f"module {module_path!r} import failed: {exc!r}"
    if not hasattr(module, attr_name):
        return False, f"module {module_path!r} has no attribute {attr_name!r}"
    return True, ""


def _validate_refs(command: RegisterOperationCommand) -> dict[str, Any] | None:
    """Return a structured failure payload if either ref doesn't resolve."""

    failures: list[dict[str, str]] = []
    for field_name, ref in (
        ("handler_ref", command.handler_ref),
        ("input_model_ref", command.input_model_ref),
    ):
        ok, reason = _resolve_import_ref(ref)
        if not ok:
            failures.append({"field": field_name, "ref": ref, "reason": reason})
    if not failures:
        return None
    return {
        "ok": False,
        "error_code": "catalog.operation.register.unresolvable_ref",
        "error": (
            "Refusing to register operation: handler_ref and/or input_model_ref "
            "do not import-resolve in the server process. Land the handler module "
            "first, then re-call this wizard."
        ),
        "operation_ref": command.operation_ref,
        "operation_name": command.operation_name,
        "unresolved_refs": failures,
    }


def handle_register_operation(
    command: RegisterOperationCommand, subsystems: Any
) -> dict[str, Any]:
    """Register (or upsert) an operation in the catalog via the SQL helper.

    Validates that ``handler_ref`` and ``input_model_ref`` import-resolve
    BEFORE writing rows. Catalog rows with broken refs have historically
    degraded API startup; refuse upstream.
    """

    validation_failure = _validate_refs(command)
    if validation_failure is not None:
        return validation_failure

    conn = subsystems.get_pg_conn()
    # NOTE: There are no explicit ::text/::boolean casts on the named
    # parameters below because casts cannot disambiguate when both
    # overloads have identical types at the supplied positions — Postgres
    # named-argument resolution drops candidates by name match first,
    # then by type compatibility. Two overloads of register_operation_atomic
    # with the same names at the same types remain co-equal candidates
    # regardless of how the caller types its inputs. The structural
    # invariant — exactly one register_operation_atomic overload after
    # migrations apply — is enforced by a separate test
    # (test_register_operation_atomic_overload_singleton). Any migration
    # that redefines this function MUST follow migration 350's
    # drop-all-then-create pattern. (BUG-8DC8A3BA.)
    conn.execute(
        """
        SELECT register_operation_atomic(
            p_operation_ref         := $1,
            p_operation_name        := $2,
            p_handler_ref           := $3,
            p_input_model_ref       := $4,
            p_authority_domain_ref  := $5,
            p_operation_kind        := $6,
            p_posture               := $7,
            p_idempotency_policy    := $8,
            p_event_type            := $9,
            p_event_required        := $10,
            p_label                 := $11,
            p_summary               := $12,
            p_decision_ref          := $13,
            p_owner_ref             := $14,
            p_storage_target_ref    := $15,
            p_http_method           := $16,
            p_http_path             := $17
        )
        """,
        command.operation_ref,
        command.operation_name,
        command.handler_ref,
        command.input_model_ref,
        command.authority_domain_ref,
        command.operation_kind,
        command.posture,
        command.idempotency_policy,
        command.event_type,
        command.event_required,
        command.label,
        command.summary,
        command.decision_ref,
        command.owner_ref,
        command.storage_target_ref,
        command.http_method,
        command.http_path,
    )

    row = conn.fetchrow(
        """
        SELECT operation_ref, operation_name, operation_kind, source_kind,
               http_method, http_path, handler_ref, input_model_ref,
               authority_domain_ref, posture, idempotency_policy,
               event_required, event_type, enabled
          FROM operation_catalog_registry
         WHERE operation_ref = $1
        """,
        command.operation_ref,
    )

    return {
        "ok": True,
        "action": "register",
        "operation": dict(row) if row is not None else None,
        "event_payload": {
            "operation_ref": command.operation_ref,
            "operation_name": command.operation_name,
            "operation_kind": command.operation_kind,
            "authority_domain_ref": command.authority_domain_ref,
            "handler_ref": command.handler_ref,
            "input_model_ref": command.input_model_ref,
        },
    }


def handle_retire_operation(
    command: RetireOperationCommand, subsystems: Any
) -> dict[str, Any]:
    """Soft-retire a catalog operation.

    Sets ``operation_catalog_registry.enabled = FALSE`` and flips the
    matching ``authority_object_registry`` row's ``lifecycle_status`` to
    ``deprecated``. Physical deletion is intentionally not supported —
    receipts and events still reference the row by ``operation_ref``.
    """

    conn = subsystems.get_pg_conn()
    row = conn.fetchrow(
        """
        UPDATE operation_catalog_registry
           SET enabled = FALSE,
               updated_at = now()
         WHERE operation_ref = $1
        RETURNING operation_ref, operation_name, operation_kind,
                  authority_domain_ref, enabled
        """,
        command.operation_ref,
    )
    if row is None:
        return {
            "ok": False,
            "error_code": "catalog.operation.retire.not_found",
            "error": (
                f"No operation_catalog_registry row found for operation_ref="
                f"{command.operation_ref!r}; nothing to retire."
            ),
            "operation_ref": command.operation_ref,
        }

    operation = dict(row)
    object_ref = f"operation.{operation['operation_name']}"
    conn.execute(
        """
        UPDATE authority_object_registry
           SET lifecycle_status = 'deprecated',
               updated_at = now()
         WHERE object_ref = $1
        """,
        object_ref,
    )

    return {
        "ok": True,
        "action": "retire",
        "operation": operation,
        "event_payload": {
            "operation_ref": command.operation_ref,
            "operation_name": operation["operation_name"],
            "reason_code": command.reason_code,
            "operator_message": command.operator_message,
        },
    }


__all__ = [
    "RegisterOperationCommand",
    "RetireOperationCommand",
    "handle_register_operation",
    "handle_retire_operation",
]
