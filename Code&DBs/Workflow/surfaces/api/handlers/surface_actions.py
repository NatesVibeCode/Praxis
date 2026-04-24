"""HTTP frontdoor for /api/surface/action.

Delegates to ``operation_catalog_gateway`` for the typed command
``surface.action.performed`` registered by migration 234. The gateway
writes the receipt in ``authority_operation_receipts`` and fires the
``surface.action.performed`` event as a side-effect of dispatch, so
this handler holds no direct DB writes — the scope_note debt from the
original action-rail wedge (commit de172040) is closed.

Anchored by:
  architecture-policy::platform-architecture::conceptual-events-register-through-operation-catalog-registry
  architecture-policy::surface-catalog::surface-composition-cqrs-direction
"""
from __future__ import annotations

from typing import Any

from ._shared import _read_json_body


_OPERATION_NAME = "surface.action.performed"


def _handle_surface_action_post(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception as exc:  # noqa: BLE001 — malformed body surfaces as 400.
        request._send_json(400, {"error": "invalid_json_body", "detail": f"{type(exc).__name__}: {exc}"})
        return

    if not isinstance(body, dict):
        request._send_json(400, {"error": "expected_json_object"})
        return

    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    try:
        gateway_result = execute_operation_from_subsystems(
            request.subsystems,
            operation_name=_OPERATION_NAME,
            payload=body,
        )
    except ValueError as exc:
        # Pydantic validation errors from the gateway surface as 400.
        request._send_json(400, {"error": "invalid_input", "detail": str(exc)})
        return
    except Exception as exc:  # noqa: BLE001 — gateway failures surface as 500.
        request._send_json(500, {"error": "gateway_dispatch_failed", "detail": f"{type(exc).__name__}: {exc}"})
        return

    # execute_operation_from_subsystems returns the handler's dict result
    # merged with the operation_receipt key at the top level (see
    # runtime.operation_catalog_gateway._with_operation_receipt). Surface
    # the receipt id + typed payload directly so the frontend ButtonRowModule
    # response shape stays small and stable.
    if not isinstance(gateway_result, dict):
        request._send_json(500, {"error": "gateway_returned_non_dict", "detail": repr(type(gateway_result))})
        return

    receipt = gateway_result.get("operation_receipt") or {}

    request._send_json(
        200,
        {
            "ok": bool(gateway_result.get("ok", True)),
            "operation": _OPERATION_NAME,
            "receipt_id": receipt.get("receipt_id"),
            "event_ids": receipt.get("event_ids") or [],
            "action_ref": gateway_result.get("action_ref"),
            "intent_ref": gateway_result.get("intent_ref"),
            "template_ref": gateway_result.get("template_ref"),
            "pill_refs": gateway_result.get("pill_refs") or [],
            "caller_ref": gateway_result.get("caller_ref"),
        },
    )


SURFACE_ACTION_POST_ROUTES = [
    (lambda candidate: candidate == "/api/surface/action", _handle_surface_action_post),
]
