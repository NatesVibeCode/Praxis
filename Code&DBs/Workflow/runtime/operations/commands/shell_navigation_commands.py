"""Shell navigation commands — typed sinks for React app shell navigation.

Registered by migration 258. One command per intent so each conceptual event
fires through ``operation_catalog_gateway`` with its own receipt and
``authority_events`` row, honoring architecture-policy::platform-architecture::
conceptual-events-register-through-operation-catalog-registry.

Handlers emit the ``authority_events`` row themselves (rather than relying on
the gateway's generic event-shape) because the ``ui_shell_state.live``
reducer needs the full input payload — specifically ``session_aggregate_ref``
plus the route_id / slot_values / shell_state_diff — to fold per-browser-tab
state. The gateway sees ``authority_event_ids`` in the handler result and
links them to the receipt without double-emitting.

Aggregate scope is ``session_aggregate_ref`` — a per-browser-tab UUID that
the React shell stores in sessionStorage. The ``ui_shell_state.live``
projection (migration 259) folds these events keyed by that aggregate so
each tab has an independent live state.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


_NavReason = Literal["click", "keyboard", "event_bus", "history_pop", "deep_link"]
_DraftDecision = Literal["leave", "stay"]

_AUTHORITY_DOMAIN_REF = "authority.surface_catalog"


def _normalize(value: str | None, *, default: str = "") -> str:
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def _normalize_caller(caller_ref: str | None) -> str:
    return _normalize(caller_ref, default="shell.unknown")


def _emit_event(
    subsystems: Any,
    *,
    event_type: str,
    operation_ref: str,
    aggregate_ref: str,
    payload: dict[str, Any],
) -> str:
    """Insert an authority_events row with the full input payload.

    Returns the event_id so the handler can hand it back through
    ``authority_event_ids`` and the gateway skips its own emission.
    """
    conn = subsystems.get_pg_conn()
    event_id = str(uuid4())
    enriched = dict(payload)
    enriched.setdefault("emitted_at", datetime.now(timezone.utc).isoformat())
    conn.execute(
        """
        INSERT INTO authority_events (
            event_id,
            authority_domain_ref,
            aggregate_ref,
            event_type,
            event_payload,
            operation_ref,
            emitted_by
        ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, $7)
        """,
        event_id,
        _AUTHORITY_DOMAIN_REF,
        aggregate_ref,
        event_type,
        json.dumps(enriched, sort_keys=True, default=str),
        operation_ref,
        "shell_navigation_handler",
    )
    return event_id


# 1. shell.surface.opened ------------------------------------------------------

class ShellSurfaceOpenedCommand(BaseModel):
    """Input contract for ``shell.surface.opened``.

    Fires when the React shell enters any surface — static tab activation,
    build/manifest/run-detail/manifest-editor/compose entry, dashboard-detail
    drill-in. ``shell_state_diff`` is the optimistic state mutation the client
    applied locally; the projection reducer reapplies it server-side.
    """

    session_aggregate_ref: str
    route_id: str
    slot_values: dict[str, Any] = Field(default_factory=dict)
    shell_state_diff: dict[str, Any] = Field(default_factory=dict)
    reason: _NavReason = "click"
    caller_ref: str = "shell.unknown"


def handle_shell_surface_opened(
    command: ShellSurfaceOpenedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    payload = {
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "route_id": _normalize(command.route_id),
        "slot_values": dict(command.slot_values),
        "shell_state_diff": dict(command.shell_state_diff),
        "reason": command.reason,
        "caller_ref": _normalize_caller(command.caller_ref),
    }
    event_id = _emit_event(
        subsystems,
        event_type="surface.opened",
        operation_ref="shell-surface-opened",
        aggregate_ref=payload["session_aggregate_ref"],
        payload=payload,
    )
    return {"ok": True, "authority_event_ids": [event_id], **payload}


# 2. shell.tab.closed ----------------------------------------------------------

class ShellTabClosedCommand(BaseModel):
    """Input contract for ``shell.tab.closed``.

    Fires when a dynamic tab is closed (compose / manifest / manifest-editor /
    run-detail-legacy). ``fallback_route_id`` is the route the shell will
    activate next if the closed tab was active.
    """

    session_aggregate_ref: str
    dynamic_tab_id: str
    fallback_route_id: str = "route.app.dashboard"
    caller_ref: str = "shell.unknown"


def handle_shell_tab_closed(
    command: ShellTabClosedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    payload = {
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "dynamic_tab_id": _normalize(command.dynamic_tab_id),
        "fallback_route_id": _normalize(command.fallback_route_id, default="route.app.dashboard"),
        "caller_ref": _normalize_caller(command.caller_ref),
    }
    event_id = _emit_event(
        subsystems,
        event_type="tab.closed",
        operation_ref="shell-tab-closed",
        aggregate_ref=payload["session_aggregate_ref"],
        payload=payload,
    )
    return {"ok": True, "authority_event_ids": [event_id], **payload}


# 3. shell.draft.guard.consulted ----------------------------------------------

class ShellDraftGuardConsultedCommand(BaseModel):
    """Input contract for ``shell.draft.guard.consulted``.

    Records the user's leave/stay decision when the build-draft guard prompts.
    Analytic only — does not mutate ``ui_shell_state.live``.
    """

    session_aggregate_ref: str
    decision: _DraftDecision
    source_route_id: str
    target_route_id: str
    draft_message: str = ""
    caller_ref: str = "shell.draft_guard"


def handle_shell_draft_guard_consulted(
    command: ShellDraftGuardConsultedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    payload = {
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "decision": command.decision,
        "source_route_id": _normalize(command.source_route_id),
        "target_route_id": _normalize(command.target_route_id),
        "draft_message": _normalize(command.draft_message),
        "caller_ref": _normalize_caller(command.caller_ref),
    }
    event_id = _emit_event(
        subsystems,
        event_type="draft.guard.consulted",
        operation_ref="shell-draft-guard-consulted",
        aggregate_ref=payload["session_aggregate_ref"],
        payload=payload,
    )
    return {"ok": True, "authority_event_ids": [event_id], **payload}


# 4. shell.history.popped -----------------------------------------------------

class ShellHistoryPoppedCommand(BaseModel):
    """Input contract for ``shell.history.popped``.

    Fires when the user uses browser back/forward. The follow-up
    ``shell.surface.opened`` (with reason='history_pop') is what mutates
    projection state — this row preserves the cause.
    """

    session_aggregate_ref: str
    target_route_id: str
    slot_values: dict[str, Any] = Field(default_factory=dict)
    caller_ref: str = "shell.history"


def handle_shell_history_popped(
    command: ShellHistoryPoppedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    payload = {
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "target_route_id": _normalize(command.target_route_id),
        "slot_values": dict(command.slot_values),
        "caller_ref": _normalize_caller(command.caller_ref),
    }
    event_id = _emit_event(
        subsystems,
        event_type="history.popped",
        operation_ref="shell-history-popped",
        aggregate_ref=payload["session_aggregate_ref"],
        payload=payload,
    )
    return {"ok": True, "authority_event_ids": [event_id], **payload}


# 5. shell.session.bootstrapped -----------------------------------------------

class ShellSessionBootstrappedCommand(BaseModel):
    """Input contract for ``shell.session.bootstrapped``.

    Fires once per browser-tab session when the React shell first mounts.
    Initializes the per-tab session_aggregate_ref aggregate in
    ``ui_shell_state.live`` and applies any deep-link route from the initial
    URL.
    """

    session_aggregate_ref: str
    initial_route_id: str = "route.app.dashboard"
    initial_slot_values: dict[str, Any] = Field(default_factory=dict)
    deep_link_search: str = ""


def handle_shell_session_bootstrapped(
    command: ShellSessionBootstrappedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    payload = {
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "initial_route_id": _normalize(command.initial_route_id, default="route.app.dashboard"),
        "initial_slot_values": dict(command.initial_slot_values or {}),
        "deep_link_search": _normalize(command.deep_link_search),
    }
    event_id = _emit_event(
        subsystems,
        event_type="session.bootstrapped",
        operation_ref="shell-session-bootstrapped",
        aggregate_ref=payload["session_aggregate_ref"],
        payload=payload,
    )
    return {"ok": True, "authority_event_ids": [event_id], **payload}


__all__ = [
    "ShellSurfaceOpenedCommand",
    "ShellTabClosedCommand",
    "ShellDraftGuardConsultedCommand",
    "ShellHistoryPoppedCommand",
    "ShellSessionBootstrappedCommand",
    "handle_shell_surface_opened",
    "handle_shell_tab_closed",
    "handle_shell_draft_guard_consulted",
    "handle_shell_history_popped",
    "handle_shell_session_bootstrapped",
]
