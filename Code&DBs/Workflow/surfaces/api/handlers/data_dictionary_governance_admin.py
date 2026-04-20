"""HTTP surface for the data-dictionary governance compliance scan.

GET  /api/data-dictionary/governance            -> dry-run scan
POST /api/data-dictionary/governance/enforce    -> scan + file bugs
"""
from __future__ import annotations

from typing import Any

from runtime.data_dictionary_governance import (
    compute_scorecard,
    run_governance_scan,
)
from runtime.data_dictionary_governance_clustering import suggest_cluster_fixes
from runtime.data_dictionary_governance_change_feed import (
    drain_change_feed,
    peek_pending,
)
from runtime.data_dictionary_governance_remediation import (
    suggest_all_remediations,
)
from storage.postgres.data_dictionary_governance_scans_repository import (
    fetch_scan_by_id,
    list_scans,
)

from ._shared import RouteEntry, _exact, _prefix


_PREFIX = "/api/data-dictionary/governance"


def _handle_scan(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        payload = run_governance_scan(
            pg, tracker=None, dry_run=True, triggered_by="operator_http",
        )
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_scorecard(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        payload = compute_scorecard(pg)
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_cluster(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        payload = suggest_cluster_fixes(pg)
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_remediate(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()

        def _discover(query: str, limit: int) -> list[dict[str, Any]]:
            try:
                indexer = request.subsystems.get_module_indexer()
                rows = indexer.search(query=query, limit=limit, threshold=0.3) or []
            except Exception:
                return []
            return [
                {
                    "name": r.get("name", ""),
                    "kind": r.get("kind", ""),
                    "path": r.get("module_path", ""),
                    "similarity": round(float(r.get("cosine_similarity") or 0), 2),
                }
                for r in rows
            ]

        payload = suggest_all_remediations(pg, discover=_discover)
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _iso(v: Any) -> Any:
    return v.isoformat() if hasattr(v, "isoformat") else v


def _handle_scan_list(request: Any, path: str) -> None:
    """GET /api/data-dictionary/governance/scans[?limit=&triggered_by=]"""
    try:
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(path).query)
        limit = int(qs.get("limit", ["25"])[0])
        triggered_by = qs.get("triggered_by", [None])[0]
        pg = request.subsystems.get_pg_conn()
        scans = list_scans(pg, limit=limit, triggered_by=triggered_by)
        request._send_json(200, {
            "count": len(scans),
            "scans": [
                {**s, "scanned_at": _iso(s.get("scanned_at"))}
                for s in scans
            ],
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_pending(request: Any, path: str) -> None:
    """GET /api/data-dictionary/governance/pending[?limit=]"""
    try:
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(path).query)
        limit = int(qs.get("limit", ["20"])[0])
        pg = request.subsystems.get_pg_conn()
        request._send_json(200, peek_pending(pg, limit=limit))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_drain(request: Any, path: str) -> None:
    """POST /api/data-dictionary/governance/drain

    Body JSON: {"dry_run": bool, "limit": int}
    Drains the change-feed ledger and files governance bugs for any new
    violations on affected objects.
    """
    try:
        from runtime.bug_tracker import BugTracker
        from ._shared import _read_json_body

        body = _read_json_body(request) or {}
        dry_run = bool(body.get("dry_run", False))
        limit = int(body.get("limit", 100))
        pg = request.subsystems.get_pg_conn()
        tracker = None if dry_run else BugTracker(pg)
        payload = drain_change_feed(
            pg, tracker=tracker, limit=limit, triggered_by="operator_http",
        )
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_scan_detail(request: Any, path: str) -> None:
    """GET /api/data-dictionary/governance/scans/<scan_id>"""
    try:
        scan_id = path.rsplit("?", 1)[0].rsplit("/", 1)[-1]
        if not scan_id:
            request._send_json(400, {"error": "scan_id required"})
            return
        pg = request.subsystems.get_pg_conn()
        row = fetch_scan_by_id(pg, scan_id)
        if not row:
            request._send_json(404, {"error": f"scan not found: {scan_id}"})
            return
        row["scanned_at"] = _iso(row.get("scanned_at"))
        request._send_json(200, row)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_enforce(request: Any, path: str) -> None:
    try:
        from runtime.bug_tracker import BugTracker

        pg = request.subsystems.get_pg_conn()
        tracker = BugTracker(pg)
        payload = run_governance_scan(
            pg, tracker=tracker, dry_run=False, triggered_by="operator_http",
        )
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


DATA_DICTIONARY_GOVERNANCE_GET_ROUTES: list[RouteEntry] = [
    # Specific paths BEFORE the exact match on `_PREFIX`.
    (_exact(_PREFIX + "/scorecard"), _handle_scorecard),
    (_exact(_PREFIX + "/remediate"), _handle_remediate),
    (_exact(_PREFIX + "/clusters"), _handle_cluster),
    (_exact(_PREFIX + "/scans"), _handle_scan_list),
    # Prefix matcher for /scans/<id> — must come BEFORE the exact /scans
    # match would be swallowed; prefix handler disambiguates via last segment.
    (_prefix(_PREFIX + "/scans/"), _handle_scan_detail),
    (_exact(_PREFIX + "/pending"), _handle_pending),
    (_exact(_PREFIX), _handle_scan),
]


DATA_DICTIONARY_GOVERNANCE_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/enforce"), _handle_enforce),
    (_exact(_PREFIX + "/drain"), _handle_drain),
]


__all__ = [
    "DATA_DICTIONARY_GOVERNANCE_GET_ROUTES",
    "DATA_DICTIONARY_GOVERNANCE_POST_ROUTES",
]
