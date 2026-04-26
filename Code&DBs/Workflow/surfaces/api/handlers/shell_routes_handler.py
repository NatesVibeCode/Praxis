"""GET /api/shell/routes — bootstrap the React shell with the route registry.

Returns the full enabled-rows projection of ``ui_shell_route_registry`` so
the client-side ``routeRegistry.ts`` can drive URL parsing/building, tab strip
composition, command-menu items, lazy-component binding, and keyboard
shortcuts off DB-backed metadata instead of hand-rolled TypeScript.

Anchored to decision.shell_navigation_cqrs.20260426.
"""

from __future__ import annotations

import json
from typing import Any

from ._shared import RouteEntry, _exact


def _coerce_jsonb(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _serialize_row(row: Any) -> dict[str, Any]:
    get = (lambda key: row.get(key)) if isinstance(row, dict) else (lambda key: row[key])
    return {
        "route_id": get("route_id"),
        "path_template": get("path_template"),
        "surface_name": get("surface_name"),
        "state_effect": get("state_effect"),
        "notes": get("notes"),
        "source_refs": _coerce_jsonb(get("source_refs")) or [],
        "status": get("status"),
        "display_order": get("display_order"),
        "binding_revision": get("binding_revision"),
        "decision_ref": get("decision_ref"),
        "component_ref": get("component_ref"),
        "tab_kind_label": get("tab_kind_label"),
        "tab_label_template": get("tab_label_template"),
        "context_label": get("context_label"),
        "context_detail_template": get("context_detail_template"),
        "nav_description_template": get("nav_description_template"),
        "nav_keywords": _coerce_jsonb(get("nav_keywords")) or [],
        "event_bus_kind": get("event_bus_kind"),
        "keyboard_shortcut": get("keyboard_shortcut"),
        "draft_guard_required": bool(get("draft_guard_required")),
        "is_dynamic": bool(get("is_dynamic")),
        "is_canonical_for_surface": bool(get("is_canonical_for_surface")),
        "tab_strip_position": get("tab_strip_position"),
    }


def _handle_shell_routes_get(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.fetch(
            """
            SELECT
                route_id,
                path_template,
                surface_name,
                state_effect,
                notes,
                source_refs,
                status,
                display_order,
                binding_revision,
                decision_ref,
                component_ref,
                tab_kind_label,
                tab_label_template,
                context_label,
                context_detail_template,
                nav_description_template,
                nav_keywords,
                event_bus_kind,
                keyboard_shortcut,
                draft_guard_required,
                is_dynamic,
                is_canonical_for_surface,
                tab_strip_position
              FROM ui_shell_route_registry
             WHERE enabled = TRUE
             ORDER BY display_order, route_id
            """
        )
    except Exception as exc:  # noqa: BLE001 - surface authority unavailable degrades to 503.
        request._send_json(
            503,
            {
                "error": "ui_shell_route_registry_unavailable",
                "error_code": "ui_shell_route_registry_unavailable",
                "detail": f"{type(exc).__name__}: {exc}",
            },
        )
        return

    routes = [_serialize_row(row) for row in rows or []]
    request._send_json(
        200,
        {
            "routes": routes,
            "count": len(routes),
            "decision_ref": "decision.shell_navigation_cqrs.20260426",
        },
    )


SHELL_ROUTES_GET_ROUTES: list[RouteEntry] = [
    (_exact("/api/shell/routes"), _handle_shell_routes_get),
]


__all__ = ["SHELL_ROUTES_GET_ROUTES"]
