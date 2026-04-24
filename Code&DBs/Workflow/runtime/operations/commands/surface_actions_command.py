"""Surface action command — typed sink for composed-surface interactions.

Registered by migration 234. Fires when a user clicks an action on a
composed Surface template (e.g. Approve on an invoice review). Routes
through operation_catalog_gateway, which writes the receipt and fires
the ``surface.action.performed`` event as a side-effect of dispatch —
honoring architecture-policy::platform-architecture::conceptual-events-
register-through-operation-catalog-registry.

The handler is intentionally thin: it validates the typed context,
normalizes the payload, and returns it. The gateway handles receipt
writing + event emission. No direct authority_operation_receipts
INSERT exists here — that was the scope_note debt this wedge closes.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SurfaceActionPerformedCommand(BaseModel):
    """Input contract for ``surface.action.performed``.

    ``action_ref`` is the typed action identifier (e.g.
    ``invoice_approval.approve``). ``intent_ref`` is the composing
    intent id from memory_entities. ``template_ref`` is the
    experience_template that rendered the button. ``pill_refs`` are
    the pill_type ids bound to the template slots at render time.
    ``caller_ref`` identifies the surface subsystem that fired the
    click (default ``surface.compose.button_row``).
    """

    action_ref: str
    intent_ref: str
    template_ref: str | None = None
    pill_refs: list[str] = Field(default_factory=list)
    caller_ref: str = "surface.compose.button_row"


def handle_surface_action_performed(
    command: SurfaceActionPerformedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Validate + normalize. Return the typed payload; gateway writes
    the receipt and fires the event as part of dispatch."""
    del subsystems  # no direct DB writes — gateway owns receipt + event.

    normalized_pills = [p for p in command.pill_refs if isinstance(p, str) and p.strip()]

    return {
        "ok": True,
        "action_ref": command.action_ref,
        "intent_ref": command.intent_ref,
        "template_ref": command.template_ref,
        "pill_refs": normalized_pills,
        "caller_ref": command.caller_ref,
    }
