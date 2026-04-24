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


def _pass_rate_reducer(subs: Any, *, source_ref: str) -> dict[str, Any]:
    del source_ref
    return _scalar_output(_platform_overview_snapshot(subs).get("pass_rate"), fmt="percent")


def _open_bugs_reducer(subs: Any, *, source_ref: str) -> dict[str, Any]:
    del source_ref
    # open_bugs is not in operator.status_snapshot — pull from the full platform-overview read.
    return _scalar_output(_platform_overview_full(subs).get("open_bugs"), fmt="number")


def _total_runs_reducer(subs: Any, *, source_ref: str) -> dict[str, Any]:
    del source_ref
    return _scalar_output(_platform_overview_full(subs).get("total_workflow_runs"), fmt="number")


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
}


def resolve_reducer(reducer_ref: str) -> Reducer | None:
    return REDUCERS.get(reducer_ref)
