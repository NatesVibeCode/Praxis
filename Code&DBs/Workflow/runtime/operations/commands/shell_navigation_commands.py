"""Shell navigation commands — typed sinks for React app shell navigation.

Registered by migration 258. One command per intent so each conceptual event
fires through ``operation_catalog_gateway`` with its own receipt and
``authority_events`` row, honoring architecture-policy::platform-architecture::
conceptual-events-register-through-operation-catalog-registry.

The handlers are intentionally thin: validate + normalize + return dict.
The gateway owns receipt insertion and event emission as a side-effect of
dispatch (event_required=TRUE on the operation_catalog_registry row).

Aggregate scope is ``session_aggregate_ref`` — a per-browser-tab UUID that
the React shell stores in sessionStorage. The ``ui_shell_state.live``
projection (migration 259) folds these events keyed by that aggregate so
each tab has an independent live state.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


_NavReason = Literal["click", "keyboard", "event_bus", "history_pop", "deep_link"]
_DraftDecision = Literal["leave", "stay"]


def _normalize(value: str | None, *, default: str = "") -> str:
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def _normalize_caller(caller_ref: str | None) -> str:
    return _normalize(caller_ref, default="shell.unknown")


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
    del subsystems  # gateway owns receipt + event persistence

    return {
        "ok": True,
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "route_id": _normalize(command.route_id),
        "slot_values": dict(command.slot_values),
        "shell_state_diff": dict(command.shell_state_diff),
        "reason": command.reason,
        "caller_ref": _normalize_caller(command.caller_ref),
    }


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
    del subsystems

    return {
        "ok": True,
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "dynamic_tab_id": _normalize(command.dynamic_tab_id),
        "fallback_route_id": _normalize(command.fallback_route_id, default="route.app.dashboard"),
        "caller_ref": _normalize_caller(command.caller_ref),
    }


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
    del subsystems

    return {
        "ok": True,
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "decision": command.decision,
        "source_route_id": _normalize(command.source_route_id),
        "target_route_id": _normalize(command.target_route_id),
        "draft_message": _normalize(command.draft_message),
        "caller_ref": _normalize_caller(command.caller_ref),
    }


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
    del subsystems

    return {
        "ok": True,
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "target_route_id": _normalize(command.target_route_id),
        "slot_values": dict(command.slot_values),
        "caller_ref": _normalize_caller(command.caller_ref),
    }


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
    deep_link_search: str = ""


def handle_shell_session_bootstrapped(
    command: ShellSessionBootstrappedCommand,
    subsystems: Any,
) -> dict[str, Any]:
    del subsystems

    return {
        "ok": True,
        "session_aggregate_ref": _normalize(command.session_aggregate_ref),
        "initial_route_id": _normalize(command.initial_route_id, default="route.app.dashboard"),
        "deep_link_search": _normalize(command.deep_link_search),
    }


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
