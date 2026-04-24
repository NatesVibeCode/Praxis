"""Typed gap event emission.

Emit ``typed_gap.created`` events when a gap-producing surface detects
unsatisfied preconditions. Pairs with the ``authority_event_contracts``
row registered in migration 226 (Phase 1.6 of the public beta ramp).

Known emitters (wiring lands packet-by-packet):
- ``runtime.catalog_type_contract_validation`` findings
- ``runtime.spec_compiler.UnresolvedSourceRefError``
- ``runtime.spec_compiler.UnresolvedStageError``
- ``runtime.spec_compiler.UnresolvedWriteScopeError``
- ``runtime.spec_compiler._compute_verification_gaps`` entries
- future: Moon composer + ``compose_plan_from_intent`` type-flow errors

Payload shape (matches ``payload_keys`` declared in migration 226):
::

    {
        "gap_id": "typed_gap.{16-hex}",
        "gap_kind": "source_ref" | "stage" | "write_scope" | "verifier"
                    | "type_contract_slug" | "type_flow" | ...,
        "missing_type": str,
        "reason_code": str,
        "legal_repair_actions": [str, ...],
        "source_ref": str | None,
        "context": {...}
    }
"""
from __future__ import annotations

import uuid
from typing import Any


def emit_typed_gap(
    conn: Any,
    *,
    gap_kind: str,
    missing_type: str,
    reason_code: str,
    legal_repair_actions: list[str] | None = None,
    source_ref: str | None = None,
    context: dict[str, Any] | None = None,
) -> str | None:
    """Emit a ``typed_gap.created`` event and return the generated gap_id.

    Writes through ``runtime.system_events.emit_system_event`` (observability
    path). Formal operation_receipt-backed emission follows when the
    emitting call sites register as operations in operation_catalog_registry
    per architecture-policy::platform-architecture::conceptual-events-
    register-through-operation-catalog-registry.

    Best-effort: returns ``None`` on emission failure (module import or
    per-call exception). Callers that care about durability check the
    return value; callers that don't can ignore it — the observable
    effect at the surface level is the same.
    """
    gap_id = f"typed_gap.{uuid.uuid4().hex[:16]}"
    payload: dict[str, Any] = {
        "gap_id": gap_id,
        "gap_kind": str(gap_kind),
        "missing_type": str(missing_type),
        "reason_code": str(reason_code),
        "legal_repair_actions": [str(a) for a in (legal_repair_actions or ())],
        "source_ref": None if source_ref is None else str(source_ref),
        "context": dict(context or {}),
    }
    try:
        from runtime.system_events import emit_system_event
    except Exception:
        return None
    try:
        emit_system_event(
            conn,
            event_type="typed_gap.created",
            source_id=gap_id,
            source_type="typed_gap",
            payload=payload,
        )
    except Exception:
        return None
    return gap_id


def emit_typed_gaps_for_verification_gaps(
    conn: Any,
    gaps: list[dict[str, Any]] | None,
    *,
    source_ref: str | None = None,
) -> int:
    """Promote ``_compute_verification_gaps`` entries to ``typed_gap.created``
    events.

    Opt-in companion to ``runtime.spec_compiler._compute_verification_gaps``.
    Callers with a live conn (typically at packet_map assembly time in
    ``launch_plan`` or ``launch_proposed``) can pass the gap list here to
    get one ``typed_gap.created`` event per gap. ``source_ref`` (e.g.
    ``"packet:p1"`` or ``"workflow_run:{run_id}"``) lands on each event
    so consumers can correlate gaps back to the producing context.

    Verification gaps today have the shape
    ``{"file", "missing_type", "reason_code"}``. Event gap_kind is
    ``verifier``. Legal repair actions default to
    ``["add_verifier_catalog_entry"]`` — Phase 1.5's catalog-backed
    dispatch is queued; until then, adding a verifier means coding one.

    Returns the count of successfully emitted events. Best-effort: no
    exceptions propagate.
    """
    emitted = 0
    for gap in gaps or ():
        if not isinstance(gap, dict):
            continue
        file_path = str(gap.get("file") or "")
        missing_type = str(gap.get("missing_type") or "verifier")
        reason_code = str(
            gap.get("reason_code") or "verifier.no_admitted_for_extension"
        )
        gap_id = emit_typed_gap(
            conn,
            gap_kind="verifier",
            missing_type=missing_type,
            reason_code=reason_code,
            legal_repair_actions=["add_verifier_catalog_entry"],
            source_ref=source_ref,
            context={"file": file_path},
        )
        if gap_id:
            emitted += 1
    return emitted


__all__ = ["emit_typed_gap", "emit_typed_gaps_for_verification_gaps"]
