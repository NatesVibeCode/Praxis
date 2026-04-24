"""Surface workspace typed-projection handler.

Resolves ``/api/projections/<projection_ref>`` by joining
``authority_projection_contracts`` + ``authority_projection_registry`` +
``authority_projection_state`` (CQRS substrate from migrations 200 and 204),
dispatching to a reducer registered in ``runtime.surface_projections``, and
returning a typed envelope.

This is the read-side surface of the decision filed under
``architecture-policy::surface-catalog::surface-composition-cqrs-direction``.
``useModuleData`` fetches through this handler when a quadrant config has
``source.projection_ref``, replacing the raw REST endpoint string path for the
migrated preset (currently only ``pass-rate``).
"""
from __future__ import annotations

from typing import Any

from ._shared import _serialize


_PROJECTIONS_PATH_PREFIX = "/api/projections/"


def _handle_projection_get(request: Any, path: str) -> None:
    path_only = path.split("?", 1)[0]
    projection_ref = path_only[len(_PROJECTIONS_PATH_PREFIX):].strip()
    if not projection_ref:
        request._send_json(400, {"error": "projection_ref_required", "error_code": "missing_projection_ref"})
        return

    try:
        pg = request.subsystems.get_pg_conn()
    except Exception as exc:  # noqa: BLE001 - substrate missing degrades to 503.
        request._send_json(
            503,
            {
                "error": "projection_authority_missing",
                "error_code": "projection_authority_missing",
                "detail": f"{type(exc).__name__}: {exc}",
            },
        )
        return

    try:
        row = pg.fetchrow(
            """
            SELECT
                reg.projection_ref,
                reg.enabled AS registry_enabled,
                reg.reducer_ref,
                reg.source_event_stream_ref,
                reg.authority_domain_ref,
                contracts.source_ref_kind,
                contracts.source_ref,
                contracts.read_model_object_ref,
                contracts.freshness_policy_ref,
                contracts.enabled AS contract_enabled,
                state.last_event_id,
                state.last_receipt_id,
                state.last_refreshed_at,
                state.freshness_status,
                state.error_code,
                state.error_detail
            FROM authority_projection_registry reg
            LEFT JOIN authority_projection_contracts contracts
              ON contracts.projection_ref = reg.projection_ref
            LEFT JOIN authority_projection_state state
              ON state.projection_ref = reg.projection_ref
            WHERE reg.projection_ref = $1
            """,
            projection_ref,
        )
    except Exception as exc:  # noqa: BLE001 - migration not applied degrades to 503.
        request._send_json(
            503,
            {
                "error": "projection_authority_missing",
                "error_code": "projection_authority_missing",
                "detail": f"{type(exc).__name__}: {exc}",
            },
        )
        return

    if row is None:
        request._send_json(
            404,
            {"error": "projection_not_found", "error_code": "projection_not_found", "projection_ref": projection_ref},
        )
        return

    if not row["registry_enabled"] or row["contract_enabled"] is False:
        request._send_json(
            410,
            {"error": "projection_disabled", "error_code": "projection_disabled", "projection_ref": projection_ref},
        )
        return

    reducer_ref = row["reducer_ref"]
    from runtime.surface_projections import resolve_reducer

    reducer = resolve_reducer(reducer_ref)
    if reducer is None:
        request._send_json(
            501,
            {
                "error": "reducer_not_registered",
                "error_code": "reducer_not_registered",
                "projection_ref": projection_ref,
                "reducer_ref": reducer_ref,
            },
        )
        return

    warnings: list[str] = []
    source_ref = row["source_ref"] or ""
    try:
        output = reducer(request.subsystems, source_ref=source_ref)
    except Exception as exc:  # noqa: BLE001 - reducer failure surfaces in envelope, not 500, per failure_visibility_required.
        output = None
        warnings.append(f"reducer_failed: {type(exc).__name__}: {exc}")

    envelope = {
        "projection_ref": row["projection_ref"],
        "output": _serialize(output),
        "last_event_id": str(row["last_event_id"]) if row["last_event_id"] else None,
        "last_receipt_id": str(row["last_receipt_id"]) if row["last_receipt_id"] else None,
        "last_refreshed_at": row["last_refreshed_at"].isoformat() if row["last_refreshed_at"] else None,
        "freshness_status": row["freshness_status"] or "unknown",
        "source_refs": [
            {"kind": row["source_ref_kind"], "ref": source_ref},
        ],
        "read_model_object_ref": row["read_model_object_ref"],
        "authority_domain_ref": row["authority_domain_ref"],
        "warnings": warnings,
    }
    if row["error_code"]:
        envelope["warnings"].append(f"state_error: {row['error_code']}")
    request._send_json(200, envelope)


def _projection_path_matcher(candidate: str) -> bool:
    path = candidate.split("?", 1)[0]
    if not path.startswith(_PROJECTIONS_PATH_PREFIX):
        return False
    tail = path[len(_PROJECTIONS_PATH_PREFIX):]
    return bool(tail) and "/" not in tail


PROJECTION_GET_ROUTES = [
    (_projection_path_matcher, _handle_projection_get),
]
