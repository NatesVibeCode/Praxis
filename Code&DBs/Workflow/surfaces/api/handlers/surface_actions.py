"""Surface composition action endpoint.

Minimal typed-action sink for composed surfaces: when a ButtonRowModule
rendered by the template compiler POSTs /api/surface/action with a typed
context ``{action_ref, intent_ref, pill_refs[], template_ref}``, this
handler writes an ``authority_operation_receipts`` row so the click is
durably auditable and queryable.

Anchored by:
  architecture-policy::surface-catalog::surface-composition-cqrs-direction
  architecture-policy::platform-architecture::legal-is-computable-not-permitted

Scope: proves the UI -> receipt loop through the existing CQRS receipt
substrate. The follow-up packet registers this as a formal
``operation_catalog_registry`` command and routes through
``operation_catalog_gateway`` per the conceptual-events-through-OCR
policy; the next-packet marker is recorded inline on each receipt under
``result_payload.scope_note``.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from ._shared import _read_json_body


_OPERATION_REF = "surface.action.performed"
_OPERATION_NAME = "surface.action_performed"
_AUTHORITY_DOMAIN_REF = "authority.surface_catalog"
_AUTHORITY_REF = "authority.surface_catalog"
_STORAGE_TARGET_REF = "praxis.primary_postgres"
_BINDING_REVISION = "binding.surface_action.first_wedge.20260424"
_DECISION_REF = "decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction"


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

    action_ref = str(body.get("action_ref") or "").strip()
    intent_ref = str(body.get("intent_ref") or "").strip()
    template_ref = str(body.get("template_ref") or "").strip()
    raw_pills = body.get("pill_refs") or []
    pill_refs = [str(p).strip() for p in raw_pills if isinstance(p, str) and p.strip()]

    if not action_ref:
        request._send_json(400, {"error": "action_ref_required"})
        return
    if not intent_ref:
        request._send_json(400, {"error": "intent_ref_required"})
        return

    input_payload = {
        "action_ref": action_ref,
        "intent_ref": intent_ref,
        "template_ref": template_ref,
        "pill_refs": pill_refs,
    }
    input_hash = hashlib.sha256(
        json.dumps(input_payload, sort_keys=True).encode("utf-8"),
    ).hexdigest()

    result_payload = dict(input_payload)
    result_payload["scope_note"] = (
        "First wedge of the surface-action command path. Writes authority_"
        "operation_receipts directly; follow-up packet registers "
        f"{_OPERATION_REF} in operation_catalog_registry and routes through "
        "operation_catalog_gateway per conceptual-events-through-OCR policy."
    )

    try:
        pg = request.subsystems.get_pg_conn()
    except Exception as exc:  # noqa: BLE001
        request._send_json(503, {"error": "receipt_authority_unavailable", "detail": f"{type(exc).__name__}: {exc}"})
        return

    try:
        row = pg.fetchrow(
            """
            INSERT INTO authority_operation_receipts (
                operation_ref,
                operation_name,
                operation_kind,
                authority_domain_ref,
                authority_ref,
                projection_ref,
                storage_target_ref,
                input_hash,
                output_hash,
                caller_ref,
                execution_status,
                result_payload,
                duration_ms,
                binding_revision,
                decision_ref
            ) VALUES (
                $1, $2, 'command', $3, $4, NULL, $5, $6, $7, $8, 'completed', $9::jsonb, 0, $10, $11
            )
            RETURNING receipt_id, created_at
            """,
            _OPERATION_REF,
            _OPERATION_NAME,
            _AUTHORITY_DOMAIN_REF,
            _AUTHORITY_REF,
            _STORAGE_TARGET_REF,
            input_hash,
            input_hash,
            str(body.get("caller_ref") or "surface.compose.button_row"),
            json.dumps(result_payload),
            _BINDING_REVISION,
            _DECISION_REF,
        )
    except Exception as exc:  # noqa: BLE001 — surface the failure, don't swallow.
        request._send_json(
            500,
            {"error": "receipt_write_failed", "detail": f"{type(exc).__name__}: {exc}"},
        )
        return

    request._send_json(
        200,
        {
            "ok": True,
            "receipt_id": str(row["receipt_id"]),
            "operation_ref": _OPERATION_REF,
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "action_ref": action_ref,
            "intent_ref": intent_ref,
            "template_ref": template_ref,
            "pill_refs": pill_refs,
        },
    )


SURFACE_ACTION_POST_ROUTES = [
    (lambda candidate: candidate == "/api/surface/action", _handle_surface_action_post),
]
