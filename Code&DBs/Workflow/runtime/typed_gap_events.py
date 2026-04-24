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


def emit_typed_gaps_for_compile_errors(
    conn: Any,
    error: Exception,
    *,
    source_ref: str | None = None,
) -> int:
    """Promote a spec_compiler Unresolved* error's entries to
    ``typed_gap.created`` events (one per unresolved entry).

    Dispatches by error type:
      - ``UnresolvedSourceRefError`` → gap_kind="source_ref", one event
        per ref; context={"ref": ref_string}; missing_type=
        "source_authority_resolver"; legal_repair_actions=
        ["add_resolver_for_prefix"].
      - ``UnresolvedStageError`` → gap_kind="stage", one event per
        packet; context={"packet_index", "packet_label", "stage"};
        missing_type="stage_template"; legal_repair_actions=
        ["add_stage_template", "use_known_stage"].
      - ``UnresolvedWriteScopeError`` → gap_kind="write_scope", one
        event per packet; context={"packet_index", "packet_label",
        "description_preview"}; missing_type="write_scope";
        legal_repair_actions=["supply_write", "add_source_ref",
        "run_scope_resolver"].

    Opt-in: callers with a conn catch the error, call this before
    re-raising. Errors that don't match a known type silently return 0
    — forward-compat when new Unresolved* classes are added.

    Returns the count of emitted events. Best-effort on emission.
    """
    try:
        from runtime.spec_compiler import (
            UnresolvedSourceRefError,
            UnresolvedStageError,
            UnresolvedWriteScopeError,
        )
    except Exception:
        return 0

    emitted = 0
    if isinstance(error, UnresolvedSourceRefError):
        for ref in error.unresolved_refs or ():
            gap_id = emit_typed_gap(
                conn,
                gap_kind="source_ref",
                missing_type="source_authority_resolver",
                reason_code="source_ref.unresolvable_prefix",
                legal_repair_actions=["add_resolver_for_prefix"],
                source_ref=source_ref,
                context={"ref": str(ref)},
            )
            if gap_id:
                emitted += 1
        return emitted
    if isinstance(error, UnresolvedStageError):
        for entry in error.unresolved_stages or ():
            if not isinstance(entry, dict):
                continue
            gap_id = emit_typed_gap(
                conn,
                gap_kind="stage",
                missing_type="stage_template",
                reason_code="stage.template_missing",
                legal_repair_actions=["add_stage_template", "use_known_stage"],
                source_ref=source_ref,
                context={
                    "packet_index": entry.get("index"),
                    "packet_label": entry.get("label"),
                    "stage": entry.get("stage"),
                },
            )
            if gap_id:
                emitted += 1
        return emitted
    if isinstance(error, UnresolvedWriteScopeError):
        for entry in error.unresolved_writes or ():
            if not isinstance(entry, dict):
                continue
            gap_id = emit_typed_gap(
                conn,
                gap_kind="write_scope",
                missing_type="write_scope",
                reason_code="write_scope.empty_no_source_authority",
                legal_repair_actions=[
                    "supply_write",
                    "add_source_ref",
                    "run_scope_resolver",
                ],
                source_ref=source_ref,
                context={
                    "packet_index": entry.get("index"),
                    "packet_label": entry.get("label"),
                    "description_preview": entry.get("description_preview"),
                },
            )
            if gap_id:
                emitted += 1
        return emitted
    return 0


def emit_typed_gaps_for_type_flow_errors(
    conn: Any,
    errors: list[str] | None,
    *,
    source_ref: str | None = None,
) -> int:
    """Promote ``validate_workflow_request_type_flow`` error strings to
    ``typed_gap.created`` events (one event per error).

    Type-flow errors have the shape
    ``"workflow.type_flow.unsatisfied_inputs:{node_id}:{missing_types_csv}"``
    (from :func:`runtime.workflow_type_contracts.validate_workflow_request_type_flow`).
    Each becomes one event with ``gap_kind="type_flow"`` and the node_id /
    missing types parsed into the event context. Unparseable error
    strings still emit with the raw error captured — no drop-on-floor.

    Opt-in with conn: callers (Moon commit handler, compose_plan_from_intent)
    pass the live Postgres conn and their source_ref ("commit:wf_id" /
    "compose_plan:{plan_name}"). Callers without a conn skip emission.

    Returns the count of successfully emitted events.
    """
    emitted = 0
    for raw in errors or ():
        err = str(raw)
        context: dict[str, Any] = {"error": err}
        if err.startswith("workflow.type_flow.unsatisfied_inputs:"):
            parts = err.split(":", 2)
            if len(parts) == 3:
                context["node_id"] = parts[1]
                context["missing_types"] = [
                    m.strip() for m in parts[2].split(",") if m.strip()
                ]
        gap_id = emit_typed_gap(
            conn,
            gap_kind="type_flow",
            missing_type="type_flow_input",
            reason_code="workflow.type_flow.unsatisfied",
            legal_repair_actions=[
                "add_producer_node",
                "remove_consumer_node",
                "narrow_consumes_contract",
            ],
            source_ref=source_ref,
            context=context,
        )
        if gap_id:
            emitted += 1
    return emitted


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


__all__ = [
    "emit_typed_gap",
    "emit_typed_gaps_for_compile_errors",
    "emit_typed_gaps_for_type_flow_errors",
    "emit_typed_gaps_for_verification_gaps",
]
