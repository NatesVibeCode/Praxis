"""HTTP surface for the schema-drift detector.

GET  /api/data-dictionary/drift                 -> latest snapshot + diff + impact
GET  /api/data-dictionary/drift/snapshots       -> recent snapshot list
GET  /api/data-dictionary/drift/diff?from=&to=  -> explicit diff between two snapshots
POST /api/data-dictionary/drift/snapshot        -> force a fresh snapshot now
"""
from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

from runtime.data_dictionary_drift import (
    DataDictionaryDriftError,
    detect_drift,
    diff_snapshots,
    drift_history,
    impact_of_diff,
    take_snapshot,
)

from ._shared import RouteEntry, _exact


_PREFIX = "/api/data-dictionary/drift"


def _query(path: str) -> dict[str, str]:
    qs = parse_qs(urlparse(path).query)
    return {k: v[0] for k, v in qs.items() if v}


def _send_error(request: Any, exc: Exception) -> None:
    if isinstance(exc, DataDictionaryDriftError):
        request._send_json(exc.status_code, {"error": str(exc)})
        return
    request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_latest(request: Any, path: str) -> None:
    """Diff the two most recent snapshots without writing a new one."""
    try:
        pg = request.subsystems.get_pg_conn()
        request._send_json(
            200, detect_drift(pg, snapshot_first=False),
        )
    except Exception as exc:
        _send_error(request, exc)


def _handle_snapshots(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query(path)
        limit = int(params.get("limit", "50"))
        request._send_json(200, drift_history(pg, limit=limit))
    except Exception as exc:
        _send_error(request, exc)


def _handle_diff(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query(path)
        old_id = params.get("from", "").strip()
        new_id = params.get("to", "").strip()
        if not old_id or not new_id:
            request._send_json(400, {"error": "from and to query params required"})
            return
        diff = diff_snapshots(pg, old_id=old_id, new_id=new_id)
        impact = [i.to_payload() for i in impact_of_diff(pg, diff)]
        request._send_json(200, {"diff": diff.to_payload(), "impact": impact})
    except Exception as exc:
        _send_error(request, exc)


def _handle_snapshot_now(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        snap = take_snapshot(pg, triggered_by="operator_http")
        # Convert datetime to ISO string.
        if hasattr(snap.get("taken_at"), "isoformat"):
            snap["taken_at"] = snap["taken_at"].isoformat()
        request._send_json(200, snap)
    except Exception as exc:
        _send_error(request, exc)


DATA_DICTIONARY_DRIFT_GET_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/snapshots"), _handle_snapshots),
    (_exact(_PREFIX + "/diff"), _handle_diff),
    (_exact(_PREFIX), _handle_latest),
]

DATA_DICTIONARY_DRIFT_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/snapshot"), _handle_snapshot_now),
]


__all__ = [
    "DATA_DICTIONARY_DRIFT_GET_ROUTES",
    "DATA_DICTIONARY_DRIFT_POST_ROUTES",
]
