"""Reducers for Surface workspace module projections.

Each entry in ``REDUCERS`` is the target of an ``authority_projection_registry``
row's ``reducer_ref`` column. Handlers look projections up by ``projection_ref``
in ``authority_projection_contracts`` and dispatch through this registry.

Scope is read-side only. The legality compiler that narrows the module palette
by consuming pills and gates is downstream of Phase 1.4 DataPill/PillBinding
and the ``typed_gap.created`` emission wiring (event contract registered in
migration 226).

Anchored by:
  architecture-policy::surface-catalog::surface-composition-cqrs-direction
  architecture-policy::platform-architecture::legal-is-computable-not-permitted
"""
from __future__ import annotations

from typing import Any, Callable


def _platform_overview_snapshot(subs: Any) -> dict[str, Any]:
    """Single upstream call for every ``platform-overview`` scalar reducer.

    Reuses ``operator.status_snapshot`` so values stay identical to the
    pre-existing ``/api/platform-overview`` read that Surface workspace metric
    modules used to fetch directly.
    """
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    return execute_operation_from_subsystems(
        subs,
        operation_name="operator.status_snapshot",
        payload={"since_hours": 24},
    )


def _scalar_output(value: Any, *, fmt: str) -> dict[str, Any]:
    if value is None:
        return {"value": None, "format": fmt}
    try:
        coerced: Any = float(value) if fmt == "percent" else int(value)
    except (TypeError, ValueError):
        return {"value": None, "format": fmt}
    return {"value": coerced, "format": fmt}


def _pass_rate_reducer(subs: Any, *, source_ref: str, **_: Any) -> dict[str, Any]:
    del source_ref
    return _scalar_output(_platform_overview_snapshot(subs).get("pass_rate"), fmt="percent")


def _open_bugs_reducer(subs: Any, *, source_ref: str, **_: Any) -> dict[str, Any]:
    del source_ref
    # open_bugs is not in operator.status_snapshot — pull from the full platform-overview read.
    return _scalar_output(_platform_overview_full(subs).get("open_bugs"), fmt="number")


def _total_runs_reducer(subs: Any, *, source_ref: str, **_: Any) -> dict[str, Any]:
    del source_ref
    return _scalar_output(_platform_overview_full(subs).get("total_workflow_runs"), fmt="number")


def _legal_templates_reducer(subs: Any, *, source_ref: str, **kwargs: Any) -> dict[str, Any]:
    from runtime.surface_template_materializer import legal_templates_reducer as _reducer

    return _reducer(subs, source_ref=source_ref, query_params=kwargs.get("query_params"))


_SHELL_NAVIGATION_EVENT_TYPES = (
    "session.bootstrapped",
    "surface.opened",
    "tab.closed",
    "history.popped",
)


def _default_shell_state() -> dict[str, Any]:
    return {
        "activeTabId": "dashboard",
        "activeRouteId": "route.app.dashboard",
        "dynamicTabs": [],
        "buildWorkflowId": None,
        "buildIntent": None,
        "builderSeed": None,
        "buildView": "canvas",
        "canvasRunId": None,
        "dashboardDetail": None,
    }


def _apply_shell_state_diff(state: dict[str, Any], diff: Any) -> dict[str, Any]:
    if not isinstance(diff, dict):
        return state
    next_state = dict(state)
    for key, value in diff.items():
        if key == "dynamicTabs" and isinstance(value, list):
            next_state["dynamicTabs"] = list(value)
            continue
        next_state[key] = value
    return next_state


def _close_dynamic_tab(state: dict[str, Any], dynamic_tab_id: str, fallback_route_id: str) -> dict[str, Any]:
    tabs = [tab for tab in state.get("dynamicTabs") or [] if isinstance(tab, dict)]
    remaining = [tab for tab in tabs if tab.get("id") != dynamic_tab_id]
    next_state = dict(state)
    next_state["dynamicTabs"] = remaining
    if state.get("activeTabId") == dynamic_tab_id:
        # Fall back to the active route's tab id if the closing tab was active.
        # The route_id → activeTabId mapping is the route's surface_name for static
        # tabs; for dynamic kinds the activeTabId equals the dynamic tab id.
        next_state["activeRouteId"] = fallback_route_id or "route.app.dashboard"
        next_state["activeTabId"] = _surface_name_for_route(fallback_route_id) or "dashboard"
    return next_state


# Minimal route_id → static-surface-name map. Dynamic routes (compose / manifest /
# manifest-editor / run-detail-legacy) keep their dynamic_tab_id as activeTabId
# rather than resolving through this map.
_ROUTE_SURFACE_NAME = {
    "route.app.dashboard": "dashboard",
    "route.app.dashboard_costs": "dashboard",
    "route.app.workflow": "build",
    "route.app.build.legacy": "build",
    "route.app.run": "build",
    "route.app.atlas": "atlas",
    "route.app.manifests": "manifests",
}


def _surface_name_for_route(route_id: str | None) -> str | None:
    if not route_id:
        return None
    return _ROUTE_SURFACE_NAME.get(route_id)


def _shell_event_payload(row: Any) -> dict[str, Any]:
    # asyncpg Record supports __getitem__ but is not a dict. Indexing is the
    # universal access path; fall through to None on KeyError.
    try:
        payload = row["event_payload"]
    except (KeyError, TypeError):
        payload = None
    if isinstance(payload, str):
        import json
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    return dict(payload) if isinstance(payload, dict) else {}


def _fold_shell_event(state: dict[str, Any], event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if event_type == "session.bootstrapped":
        next_state = _default_shell_state()
        initial_route = str(payload.get("initial_route_id") or "route.app.dashboard")
        next_state["activeRouteId"] = initial_route
        surface = _surface_name_for_route(initial_route)
        if surface:
            next_state["activeTabId"] = surface
        slot_values = payload.get("initial_slot_values") if isinstance(payload.get("initial_slot_values"), dict) else {}
        if initial_route == "route.app.workflow":
            workflow_id = str(slot_values.get("workflow") or "").strip()
            intent = str(slot_values.get("intent") or "").strip()
            next_state["activeTabId"] = "build"
            next_state["buildWorkflowId"] = workflow_id or None
            next_state["buildIntent"] = intent or None
            next_state["buildView"] = "canvas"
            next_state["canvasRunId"] = None
            next_state["dashboardDetail"] = None
        elif initial_route == "route.app.run":
            run_id = str(slot_values.get("run_id") or "").strip()
            next_state["activeTabId"] = "build"
            next_state["canvasRunId"] = run_id or None
            next_state["dashboardDetail"] = None
        return next_state

    if event_type == "surface.opened":
        diff = payload.get("shell_state_diff") or {}
        route_id = str(payload.get("route_id") or "")
        next_state = _apply_shell_state_diff(state, diff)
        if route_id:
            next_state["activeRouteId"] = route_id
            surface = _surface_name_for_route(route_id)
            if surface and "activeTabId" not in (diff or {}):
                next_state["activeTabId"] = surface
        return next_state

    if event_type == "tab.closed":
        dynamic_tab_id = str(payload.get("dynamic_tab_id") or "")
        fallback_route_id = str(payload.get("fallback_route_id") or "route.app.dashboard")
        return _close_dynamic_tab(state, dynamic_tab_id, fallback_route_id)

    if event_type == "history.popped":
        # history.popped is paired with a follow-up surface.opened from the
        # popstate handler. The surface.opened mutates state; this branch is a
        # no-op so two reducers don't race over the same slot.
        return state

    # Unknown / draft.guard.consulted etc. — analytic only, no state mutation.
    return state


def _reduce_ui_shell_state(subs: Any, *, source_ref: str, **kwargs: Any) -> dict[str, Any]:
    """Fold shell-navigation events for one session_aggregate_ref into ShellState.

    ``query_params['session']`` carries the per-browser-tab session UUID. Events
    from ``authority_events`` filtered to session-scoped shell event types are
    folded in chronological order. The default ShellState is returned when no
    bootstrapping event has fired yet.
    """
    del source_ref
    query_params = kwargs.get("query_params") or {}
    session = query_params.get("session")
    if isinstance(session, list):
        session = session[0] if session else None
    if not session:
        return _default_shell_state()
    session = str(session).strip()
    if not session:
        return _default_shell_state()

    pg = subs.get_pg_conn()
    rows = pg.fetch(
        """
        SELECT event_type, event_payload, emitted_at
          FROM authority_events
         WHERE event_type = ANY($1::text[])
           AND event_payload->>'session_aggregate_ref' = $2
         ORDER BY emitted_at ASC, event_id ASC
        """,
        list(_SHELL_NAVIGATION_EVENT_TYPES),
        session,
    )

    state = _default_shell_state()
    for row in rows or []:
        try:
            event_type = row["event_type"]
        except (KeyError, TypeError):
            continue
        payload = _shell_event_payload(row)
        state = _fold_shell_event(state, str(event_type), payload)
    return state


def _platform_overview_full(subs: Any) -> dict[str, Any]:
    """Full platform-overview shape (bugs + totals + probes).

    status_snapshot has pass_rate + total_workflows only; other scalars
    (open_bugs, total_workflow_runs) need the wider platform-overview
    aggregate. Calls the same handler the REST route dispatches to so values
    stay byte-for-byte identical.
    """
    from surfaces.api.handlers.workflow_admin import _handle_platform_overview_get

    captured: dict[str, Any] = {}

    class _Capturer:
        def __init__(self, subsystems: Any) -> None:
            self.subsystems = subsystems
            self.path = "/api/platform-overview"

        def _send_json(self, _status: int, payload: dict[str, Any]) -> None:
            captured.update(payload)

    _handle_platform_overview_get(_Capturer(subs), "/api/platform-overview")
    return captured


Reducer = Callable[..., dict[str, Any]]


REDUCERS: dict[str, Reducer] = {
    "runtime.surface_projections.pass_rate_reducer": _pass_rate_reducer,
    "runtime.surface_projections.open_bugs_reducer": _open_bugs_reducer,
    "runtime.surface_projections.total_runs_reducer": _total_runs_reducer,
    "runtime.surface_template_materializer.legal_templates_reducer": _legal_templates_reducer,
    "runtime.surface_projections.reduce_ui_shell_state": _reduce_ui_shell_state,
}


def resolve_reducer(reducer_ref: str) -> Reducer | None:
    return REDUCERS.get(reducer_ref)
