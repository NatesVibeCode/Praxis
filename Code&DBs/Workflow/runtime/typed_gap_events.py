"""Typed gap event emission.

Emit ``typed_gap.created`` events when a gap-producing surface detects
unsatisfied preconditions. Pairs with the ``authority_event_contracts``
row registered in migration 226 (Phase 1.6 of the public beta ramp).

Known emitters (wiring lands packet-by-packet):
- ``runtime.catalog_type_contract_validation`` findings
- ``runtime.spec_materializer.UnresolvedSourceRefError``
- ``runtime.spec_materializer.UnresolvedStageError``
- ``runtime.spec_materializer.UnresolvedWriteScopeError``
- ``runtime.spec_materializer._compute_verification_gaps`` entries
- ``runtime.build_authority`` typed build issues
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

Gateway routing (Phase D — close-the-bypass):
    Migration 354 registers ``typed_gap.emit`` in ``operation_catalog_registry``
    so emission flows through ``operation_catalog_gateway`` and produces a
    gateway receipt with ``receipt_id`` linkage on the resulting
    ``authority_events`` row. Direct-write fallback is preserved for
    bootstrap/test contexts where the catalog row isn't loaded yet.
"""
from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field


class EmitTypedGapCommand(BaseModel):
    """Gateway-dispatch shape for ``typed_gap.emit``.

    Mirrors the kwargs of the legacy ``emit_typed_gap`` helper so callers can
    keep using the helper while the dispatch goes through the gateway.
    """

    gap_kind: str
    missing_type: str
    reason_code: str
    legal_repair_actions: list[str] = Field(default_factory=list)
    source_ref: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


def _build_typed_gap_payload(
    *,
    gap_kind: str,
    missing_type: str,
    reason_code: str,
    legal_repair_actions: list[str] | None,
    source_ref: str | None,
    context: dict[str, Any] | None,
) -> tuple[str, dict[str, Any]]:
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
    return gap_id, payload


def _emit_typed_gap_inner(
    conn: Any,
    *,
    gap_kind: str,
    missing_type: str,
    reason_code: str,
    legal_repair_actions: list[str] | None,
    source_ref: str | None,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    """Inner emission logic — writes the authority_event row + system_events
    sidecar, returns ``{gap_id, authority_event_ids}``. Used by both the
    gateway-dispatched handler and the direct-write fallback."""

    gap_id, payload = _build_typed_gap_payload(
        gap_kind=gap_kind,
        missing_type=missing_type,
        reason_code=reason_code,
        legal_repair_actions=legal_repair_actions,
        source_ref=source_ref,
        context=context,
    )
    authority_event_id = _write_typed_gap_to_authority_events(
        conn,
        gap_id=gap_id,
        payload=payload,
        source_ref=source_ref,
    )
    sidecar_written = False
    try:
        from runtime.system_events import emit_system_event
    except Exception:
        emit_system_event = None  # type: ignore[assignment]
    if emit_system_event is not None:
        try:
            emit_system_event(
                conn,
                event_type="typed_gap.created",
                source_id=gap_id,
                source_type="typed_gap",
                payload=payload,
            )
            sidecar_written = True
        except Exception:
            sidecar_written = False
    if authority_event_id is None and not sidecar_written:
        return {"gap_id": None, "authority_event_ids": []}
    return {
        "gap_id": gap_id,
        "authority_event_ids": [authority_event_id] if authority_event_id else [],
    }


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

    Routes through the CQRS gateway (operation ``typed_gap.emit``) so the
    resulting authority_events row carries a ``receipt_id``. Falls back to
    a direct inner write when the gateway can't be reached (bootstrap,
    isolated test contexts, or operation_catalog_registry pre-migration-354).
    """

    command = EmitTypedGapCommand(
        gap_kind=gap_kind,
        missing_type=missing_type,
        reason_code=reason_code,
        legal_repair_actions=list(legal_repair_actions or ()),
        source_ref=source_ref,
        context=dict(context or {}),
    )
    try:
        from runtime.operation_catalog_gateway import (
            execute_operation_from_subsystems,
        )

        class _ConnSubsystems:
            def __init__(self, c: Any) -> None:
                self._c = c

            def get_pg_conn(self) -> Any:
                return self._c

        result = execute_operation_from_subsystems(
            _ConnSubsystems(conn),
            operation_name="typed_gap.emit",
            payload=command.model_dump(),
        )
        if isinstance(result, dict):
            inner = result if "gap_id" in result else result.get("result", {})
            if isinstance(inner, dict) and inner.get("gap_id"):
                return str(inner["gap_id"])
        # Gateway dispatch landed but the inner result is empty — that's
        # the same "both writes failed" path the legacy helper returned
        # None on. Surface it the same way.
        return None
    except Exception:
        # Gateway not reachable — bootstrap, missing operation row, or
        # transient. Fall through to a direct inner write so observability
        # of the gap is never lost.
        inner = _emit_typed_gap_inner(
            conn,
            gap_kind=gap_kind,
            missing_type=missing_type,
            reason_code=reason_code,
            legal_repair_actions=legal_repair_actions,
            source_ref=source_ref,
            context=context,
        )
        return inner.get("gap_id")


def handle_emit_typed_gap(
    command: EmitTypedGapCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Gateway handler for ``typed_gap.emit``.

    The handler writes the authority_event row directly and returns the
    list of event_ids so the gateway's receipt-stitching path
    (``_attach_receipt_to_authority_events``) can back-fill ``receipt_id``
    on the row it just inserted.
    """

    return _emit_typed_gap_inner(
        subsystems.get_pg_conn(),
        gap_kind=command.gap_kind,
        missing_type=command.missing_type,
        reason_code=command.reason_code,
        legal_repair_actions=command.legal_repair_actions,
        source_ref=command.source_ref,
        context=command.context,
    )


def _write_typed_gap_to_authority_events(
    conn: Any,
    *,
    gap_id: str,
    payload: dict[str, Any],
    source_ref: str | None,
) -> str | None:
    """Write the typed_gap.created event directly to authority_events.

    operation_ref is set to the source_ref (the calling operation, e.g.
    'compose_plan_from_intent:smoke_test') when provided; otherwise the
    synthetic 'typed_gap.emit' fallback so the foreign key constraint on
    operation_ref still validates against operation_catalog_registry-style
    refs.

    Returns the freshly-minted ``event_id`` (UUID string) so the gateway's
    ``_attach_receipt_to_authority_events`` can back-fill ``receipt_id``
    on this row when the dispatch happens through ``typed_gap.emit``.
    Returns ``None`` if the write failed or the connection rejected it.
    """

    import json as _json
    from uuid import uuid4

    operation_ref = (source_ref or "typed_gap.emit").strip() or "typed_gap.emit"
    event_id = str(uuid4())
    try:
        execute = getattr(conn, "execute", None)
        if execute is None:
            return None
        execute(
            """
            INSERT INTO authority_events (
                event_id,
                authority_domain_ref,
                aggregate_ref,
                event_type,
                event_payload,
                operation_ref,
                emitted_by
            ) VALUES (
                $1::uuid,
                $2,
                $3,
                $4,
                $5::jsonb,
                $6,
                $7
            )
            """,
            event_id,
            "authority.workflow_runs",
            gap_id,
            "typed_gap.created",
            _json.dumps(payload, sort_keys=True, default=str),
            operation_ref,
            "typed_gap_helper",
        )
        return event_id
    except Exception:
        return None


def emit_typed_gaps_for_compile_errors(
    conn: Any,
    error: Exception,
    *,
    source_ref: str | None = None,
) -> int:
    """Promote a spec_materializer Unresolved* error's entries to
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
        from runtime.spec_materializer import (
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

    Opt-in companion to ``runtime.spec_materializer._compute_verification_gaps``.
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


def emit_typed_gaps_for_build_issues(
    conn: Any,
    issues: list[dict[str, Any]] | None,
    *,
    source_ref: str | None = None,
) -> int:
    """Promote build-authority typed issues to ``typed_gap.created`` events.

    ``runtime.build_authority`` remains a pure projection builder, so it
    exposes typed gap metadata on each issue. Callers with a live connection
    use this helper to persist the conceptual event. Non-typed issues are
    ignored; malformed typed issues fall back to a generic build-authority gap
    instead of disappearing.
    """
    emitted = 0
    for issue in issues or ():
        if not isinstance(issue, dict):
            continue
        typed_gap = issue.get("typed_gap")
        if not isinstance(typed_gap, dict):
            gate_rule = issue.get("gate_rule")
            typed_gap = gate_rule if isinstance(gate_rule, dict) and gate_rule.get("gap_kind") else None
        if not isinstance(typed_gap, dict):
            continue
        context = typed_gap.get("context") if isinstance(typed_gap.get("context"), dict) else {}
        event_context = {
            **context,
            "issue_id": issue.get("issue_id"),
            "issue_kind": issue.get("kind"),
            "node_id": issue.get("node_id") or context.get("node_id"),
            "label": issue.get("label"),
            "summary": issue.get("summary"),
        }
        gap_id = emit_typed_gap(
            conn,
            gap_kind=str(typed_gap.get("gap_kind") or "build_authority"),
            missing_type=str(typed_gap.get("missing_type") or "build_authority_gap"),
            reason_code=str(typed_gap.get("reason_code") or "build_authority.typed_gap"),
            legal_repair_actions=[
                str(action)
                for action in (
                    typed_gap.get("legal_repair_actions")
                    if isinstance(typed_gap.get("legal_repair_actions"), list)
                    else []
                )
            ],
            source_ref=source_ref,
            context=event_context,
        )
        if gap_id:
            emitted += 1
    return emitted


__all__ = [
    "emit_typed_gap",
    "emit_typed_gaps_for_compile_errors",
    "emit_typed_gaps_for_build_issues",
    "emit_typed_gaps_for_type_flow_errors",
    "emit_typed_gaps_for_verification_gaps",
]
