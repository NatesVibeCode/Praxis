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
from runtime.data_dictionary_governance_remediation import (
    suggest_all_remediations,
)

from ._shared import RouteEntry, _exact


_PREFIX = "/api/data-dictionary/governance"


def _handle_scan(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        payload = run_governance_scan(pg, tracker=None, dry_run=True)
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


def _handle_enforce(request: Any, path: str) -> None:
    try:
        from runtime.bug_tracker import BugTracker

        pg = request.subsystems.get_pg_conn()
        tracker = BugTracker(pg)
        payload = run_governance_scan(pg, tracker=tracker, dry_run=False)
        request._send_json(200, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


DATA_DICTIONARY_GOVERNANCE_GET_ROUTES: list[RouteEntry] = [
    # Specific paths BEFORE the exact match on `_PREFIX`.
    (_exact(_PREFIX + "/scorecard"), _handle_scorecard),
    (_exact(_PREFIX + "/remediate"), _handle_remediate),
    (_exact(_PREFIX + "/clusters"), _handle_cluster),
    (_exact(_PREFIX), _handle_scan),
]


DATA_DICTIONARY_GOVERNANCE_POST_ROUTES: list[RouteEntry] = [
    (_exact(_PREFIX + "/enforce"), _handle_enforce),
]


__all__ = [
    "DATA_DICTIONARY_GOVERNANCE_GET_ROUTES",
    "DATA_DICTIONARY_GOVERNANCE_POST_ROUTES",
]
