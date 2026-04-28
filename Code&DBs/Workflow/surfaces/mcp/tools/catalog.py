"""MCP tools for the runtime CQRS-registration wizard.

Thin wrappers over the gateway-dispatched ``catalog.operation.register`` and
``catalog.operation.retire`` operations (registered in migration 285). Both
tools just translate the MCP params dict into the gateway payload and dispatch
through ``execute_operation_from_env`` — every receipt, event, and drift
signal is identical to calling the operation through CLI or HTTP.
"""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_register_operation(params: dict, _progress_emitter=None) -> dict:
    """Register a new operation in the catalog from CLI / MCP / HTTP.

    The handler import-resolves ``handler_ref`` and ``input_model_ref`` BEFORE
    writing the three CQRS rows, so a fabricated binding fails fast instead
    of degrading API startup later. Idempotent on (operation_ref) — same
    payload returns the cached gateway receipt.
    """

    payload = {key: value for key, value in params.items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Registering operation {payload.get('operation_ref') or payload.get('operation_name') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="catalog_operation_register",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done — register {status}",
        )
    return result


def tool_praxis_retire_operation(params: dict, _progress_emitter=None) -> dict:
    """Soft-retire an operation in the catalog.

    Sets ``operation_catalog_registry.enabled=FALSE`` and flips the matching
    ``authority_object_registry`` row's ``lifecycle_status`` to ``deprecated``.
    Physical deletion is intentionally not supported — receipts and events
    still resolve the row by ``operation_ref``.
    """

    operation_ref = str(params.get("operation_ref", "")).strip()
    if not operation_ref:
        return {
            "ok": False,
            "error_code": "catalog.operation.retire.invalid_input",
            "error": "operation_ref is required",
        }

    payload: dict[str, Any] = {"operation_ref": operation_ref}
    reason_code = params.get("reason_code")
    if reason_code:
        payload["reason_code"] = str(reason_code).strip()
    operator_message = params.get("operator_message")
    if operator_message:
        payload["operator_message"] = str(operator_message)

    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Retiring operation {operation_ref}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="catalog_operation_retire",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done — retire {status}",
        )
    return result


def tool_praxis_authority_domain_forge(params: dict, _progress_emitter=None) -> dict:
    """Preview authority-domain ownership before creating or attaching work."""

    payload = {key: value for key, value in params.items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Forging authority domain {payload.get('authority_domain_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority_domain_forge",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done — authority-domain forge {status}",
        )
    return result


def tool_praxis_register_authority_domain(params: dict, _progress_emitter=None) -> dict:
    """Register or update one authority domain through the CQRS gateway."""

    payload = {key: value for key, value in params.items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Registering authority domain {payload.get('authority_domain_ref') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority_domain_register",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done — authority-domain register {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, object]]] = {
    "praxis_authority_domain_forge": (
        tool_praxis_authority_domain_forge,
        {
            "operation_names": ["authority_domain_forge"],
            "description": (
                "Preview the authority-domain ownership path before creating a new "
                "authority boundary or attaching operations, tables, workflows, or "
                "tools to it. Returns existing domain state, nearby domains, attached "
                "operations, authority objects, missing inputs, reject paths, and the "
                "safe register payload.\n\n"
                "USE WHEN: a new capability needs a home for durable truth and you "
                "need to decide whether to reuse an existing authority domain or create "
                "a new one.\n\n"
                "GUARDS: read-only. It does not create domains. Use "
                "praxis_register_authority_domain only after the forge shows "
                "ok_to_register=true."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["authority_domain_ref"],
                "properties": {
                    "authority_domain_ref": {
                        "type": "string",
                        "description": "Authority domain ref, e.g. 'authority.object_truth'.",
                    },
                    "owner_ref": {"type": "string", "default": "praxis.engine"},
                    "event_stream_ref": {
                        "type": "string",
                        "description": "Defaults to 'stream.<authority_domain_ref>'.",
                    },
                    "current_projection_ref": {"type": "string"},
                    "storage_target_ref": {
                        "type": "string",
                        "default": "praxis.primary_postgres",
                    },
                    "decision_ref": {
                        "type": "string",
                        "description": "Decision/policy ref justifying a new domain.",
                    },
                },
            },
        },
    ),
    "praxis_register_authority_domain": (
        tool_praxis_register_authority_domain,
        {
            "operation_names": ["authority_domain_register"],
            "description": (
                "Register or update an authority domain through a receipt-backed CQRS "
                "command before operations, tables, workflows, or MCP tools are attached "
                "to it.\n\n"
                "USE WHEN: praxis_authority_domain_forge has shown that a new authority "
                "boundary is needed and the decision_ref/storage target are explicit.\n\n"
                "GUARDS: verifies storage_target_ref exists and writes only through the "
                "authority_domain_register gateway operation. Emits "
                "authority.domain.registered on completed command receipts."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["authority_domain_ref", "decision_ref"],
                "properties": {
                    "authority_domain_ref": {
                        "type": "string",
                        "description": "Authority domain ref, e.g. 'authority.object_truth'.",
                    },
                    "owner_ref": {"type": "string", "default": "praxis.engine"},
                    "event_stream_ref": {
                        "type": "string",
                        "description": "Defaults to 'stream.<authority_domain_ref>'.",
                    },
                    "current_projection_ref": {"type": "string"},
                    "storage_target_ref": {
                        "type": "string",
                        "default": "praxis.primary_postgres",
                    },
                    "decision_ref": {
                        "type": "string",
                        "description": "Decision/policy ref justifying this authority boundary.",
                    },
                    "enabled": {"type": "boolean", "default": True},
                },
            },
        },
    ),
    "praxis_register_operation": (
        tool_praxis_register_operation,
        {
            "operation_names": ["catalog_operation_register"],
            "description": (
                "Register a new CQRS operation in the catalog from CLI / MCP / HTTP "
                "without authoring a migration. Lands the data_dictionary_objects + "
                "authority_object_registry + operation_catalog_registry row triple "
                "atomically through register_operation_atomic.\n\n"
                "USE WHEN: an agent or operator needs to add a runtime operation that "
                "wasn't shipped in a migration — e.g. a workflow-generated handler that "
                "should be reachable through the gateway with proper receipts + events.\n\n"
                "GUARDS: handler_ref and input_model_ref are import-resolved in the "
                "server process BEFORE rows are written. If either fails to resolve, "
                "the registration is refused with structured 'unresolved_refs' detail "
                "so you fix the binding (or land the handler module first) and retry. "
                "This catches the historical 'API startup degrades when operation "
                "catalog bindings point at missing runtime exports' foot-gun upstream.\n\n"
                "Idempotent on operation_ref — same payload returns the cached gateway "
                "receipt; the helper's ON CONFLICT DO UPDATE makes re-registration a no-op."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "operation_ref",
                    "operation_name",
                    "handler_ref",
                    "input_model_ref",
                    "authority_domain_ref",
                ],
                "properties": {
                    "operation_ref": {
                        "type": "string",
                        "description": "Stable id for the operation (e.g. 'catalog.operation.register').",
                    },
                    "operation_name": {
                        "type": "string",
                        "description": "Snake-case name used as the gateway dispatch key (e.g. 'catalog_operation_register').",
                    },
                    "handler_ref": {
                        "type": "string",
                        "description": "Dotted import path to the handler callable, e.g. 'runtime.operations.commands.foo.handle_foo'. MUST import-resolve in the server process.",
                    },
                    "input_model_ref": {
                        "type": "string",
                        "description": "Dotted import path to the Pydantic input class, e.g. 'runtime.operations.commands.foo.FooCommand'. MUST import-resolve.",
                    },
                    "authority_domain_ref": {
                        "type": "string",
                        "description": "Owning authority domain (e.g. 'authority.cqrs', 'authority.workflow_runs').",
                    },
                    "operation_kind": {
                        "type": "string",
                        "enum": ["command", "query"],
                        "default": "command",
                    },
                    "posture": {
                        "type": "string",
                        "default": "operate",
                        "description": "'operate' for command ops, 'observe' for query ops.",
                    },
                    "idempotency_policy": {
                        "type": "string",
                        "enum": ["non_idempotent", "idempotent", "read_only"],
                        "default": "non_idempotent",
                    },
                    "event_type": {
                        "type": "string",
                        "description": "Required for command ops; the event_type emitted on completed receipts.",
                    },
                    "event_required": {
                        "type": "boolean",
                        "description": "Override the kind-based default (TRUE for commands, FALSE for queries).",
                    },
                    "label": {"type": "string"},
                    "summary": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "owner_ref": {"type": "string", "default": "praxis.engine"},
                    "storage_target_ref": {
                        "type": "string",
                        "default": "praxis.primary_postgres",
                    },
                    "http_method": {"type": "string", "default": "POST"},
                    "http_path": {
                        "type": "string",
                        "description": "Defaults to '/api/<operation_name>' if omitted.",
                    },
                },
            },
        },
    ),
    "praxis_retire_operation": (
        tool_praxis_retire_operation,
        {
            "operation_names": ["catalog_operation_retire"],
            "description": (
                "Soft-retire a CQRS operation. Sets operation_catalog_registry.enabled "
                "to FALSE so the gateway stops binding it, and flips the matching "
                "authority_object_registry row's lifecycle_status to 'deprecated'.\n\n"
                "USE WHEN: an operation is no longer wanted but you need to preserve "
                "receipts/events that reference it. Physical deletion is not supported "
                "here — the rows remain for audit-trail and replay continuity."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["operation_ref"],
                "properties": {
                    "operation_ref": {
                        "type": "string",
                        "description": "operation_ref of the row to retire (e.g. 'catalog.operation.register').",
                    },
                    "reason_code": {
                        "type": "string",
                        "default": "catalog.operation.retired",
                        "description": "Short reason code recorded on the retirement event payload.",
                    },
                    "operator_message": {
                        "type": "string",
                        "description": "Optional human-readable note recorded on the retirement event payload.",
                    },
                },
            },
        },
    ),
}
