"""Shared catalog authority for workflow API surfaces.

Both the FastAPI app and the legacy HTTP handler expose ``/api/catalog``.
This module keeps their item projection logic in one place so new runtime
capabilities are surfaced consistently across both entrypoints.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any

from runtime.integrations.display_names import display_name_for_integration

logger = logging.getLogger(__name__)

_KIND_TO_FAMILY: dict[str, str] = {
    "task": "think",
    "memory": "gather",
    "fanout": "think",
    "cli": "gather",
    "integration": "act",
}

_KIND_TO_ICON: dict[str, str] = {
    "task": "classify",
    "memory": "research",
    "fanout": "classify",
    "cli": "research",
    "integration": "tool",
}


def _catalog_truth(item: dict[str, Any]) -> dict[str, str]:
    if _text(item.get("status")) == "coming_soon":
        return {
            "category": "coming_soon",
            "badge": "Soon",
            "detail": "Listed in the catalog, but not enabled in the current surface.",
        }

    source = _text(item.get("source"))

    if source in {"capability", "integration", "connector"}:
        return {
            "category": "partial",
            "badge": "Policy missing",
            "detail": "Dynamic catalog row is missing DB-authored truth metadata.",
        }

    return {
        "category": "partial",
        "badge": "Unverified",
        "detail": "Catalog row is missing DB-authored truth metadata.",
    }


def _catalog_surface_policy(
    item: dict[str, Any],
    truth: dict[str, str],
) -> dict[str, str]:
    if _text(item.get("status")) == "coming_soon":
        return {
            "tier": "hidden",
            "badge": "Soon",
            "detail": "Keep this off the main builder until the route and config surface are real.",
        }

    source = _text(item.get("source"))

    if source in {"capability", "integration", "connector"}:
        return {
            "tier": "hidden",
            "badge": "Policy missing",
            "detail": "Dynamic catalog row is missing DB-authored surface policy.",
        }

    return {
        "tier": "hidden",
        "badge": "Removed",
        "detail": "Catalog row stays hidden until DB-authored surface policy exists.",
    }


def _decorate_catalog_item(item: dict[str, Any]) -> dict[str, Any]:
    decorated = dict(item)
    truth = decorated.get("truth")
    if not isinstance(truth, dict):
        truth = _catalog_truth(decorated)
    surface_policy = decorated.get("surfacePolicy")
    if not isinstance(surface_policy, dict):
        surface_policy = _catalog_surface_policy(decorated, truth)
    decorated["truth"] = truth
    decorated["surfacePolicy"] = surface_policy
    return decorated


def _surface_catalog_item_from_row(row: dict[str, Any]) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": _text(row.get("catalog_item_id")),
        "label": _text(row.get("label")),
        "icon": _text(row.get("icon")),
        "family": _text(row.get("family")),
        "status": _text(row.get("status")),
        "dropKind": _text(row.get("drop_kind")),
        "description": _text(row.get("description")),
        "source": "surface_registry",
    }

    action_value = _text(row.get("action_value"))
    if action_value:
        item["actionValue"] = action_value

    gate_family = _text(row.get("gate_family"))
    if gate_family:
        item["gateFamily"] = gate_family

    truth_category = _text(row.get("truth_category"))
    if truth_category:
        item["truth"] = {
            "category": truth_category,
            "badge": _text(row.get("truth_badge")),
            "detail": _text(row.get("truth_detail")),
        }

    surface_tier = _text(row.get("surface_tier"))
    if surface_tier:
        item["surfacePolicy"] = {
            "tier": surface_tier,
            "badge": _text(row.get("surface_badge")),
            "detail": _text(row.get("surface_detail")),
        }
        hard_choice = _text(row.get("hard_choice"))
        if hard_choice:
            item["surfacePolicy"]["hardChoice"] = hard_choice

    return _decorate_catalog_item(item)


def _load_surface_catalog_items(pg: Any, *, surface_name: str = "moon") -> list[dict[str, Any]]:
    rows = pg.execute(
        """SELECT catalog_item_id, label, icon, family, status, drop_kind,
                  action_value, gate_family, description,
                  truth_category, truth_badge, truth_detail,
                  surface_tier, surface_badge, surface_detail, hard_choice
             FROM surface_catalog_registry
            WHERE enabled = TRUE
              AND surface_name = $1
            ORDER BY display_order, catalog_item_id""",
        surface_name,
    )
    return [_surface_catalog_item_from_row(row) for row in rows or []]


def _source_policy_from_row(row: dict[str, Any]) -> dict[str, Any]:
    policy: dict[str, Any] = {}
    truth_category = _text(row.get("truth_category"))
    if truth_category:
        policy["truth"] = {
            "category": truth_category,
            "badge": _text(row.get("truth_badge")),
            "detail": _text(row.get("truth_detail")),
        }
    surface_tier = _text(row.get("surface_tier"))
    if surface_tier:
        policy["surfacePolicy"] = {
            "tier": surface_tier,
            "badge": _text(row.get("surface_badge")),
            "detail": _text(row.get("surface_detail")),
        }
        hard_choice = _text(row.get("hard_choice"))
        if hard_choice:
            policy["surfacePolicy"]["hardChoice"] = hard_choice
    return policy


def _load_surface_source_policies(pg: Any, *, surface_name: str = "moon") -> dict[str, dict[str, Any]]:
    rows = pg.execute(
        """SELECT source_kind,
                  truth_category, truth_badge, truth_detail,
                  surface_tier, surface_badge, surface_detail, hard_choice
             FROM surface_catalog_source_policy_registry
            WHERE enabled = TRUE
              AND surface_name = $1
            ORDER BY source_kind""",
        surface_name,
    )
    policies: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        source_kind = _text(row.get("source_kind"))
        if source_kind:
            policies[source_kind] = _source_policy_from_row(row)
    return policies


def _with_source_policy(
    item: dict[str, Any],
    source_policies: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    merged = dict(item)
    policy = source_policies.get(_text(item.get("source")))
    if not isinstance(policy, dict):
        return merged
    truth = policy.get("truth")
    if isinstance(truth, dict):
        merged["truth"] = json.loads(json.dumps(truth))
    surface_policy = policy.get("surfacePolicy")
    if isinstance(surface_policy, dict):
        merged["surfacePolicy"] = json.loads(json.dumps(surface_policy))
    return merged


def build_catalog_payload(pg: Any) -> dict[str, Any]:
    """Project DB-backed surface registry and live catalog items into API payload form."""

    items: list[dict[str, Any]] = []
    sources: dict[str, int] = {
        "surface_registry": 0,
        "source_policy_registry": 0,
        "capabilities": 0,
        "integrations": 0,
        "connectors": 0,
    }
    source_policies: dict[str, dict[str, Any]] = {}

    try:
        items.extend(_load_surface_catalog_items(pg))
        sources["surface_registry"] = len(items)
    except Exception as exc:
        logger.warning("catalog: surface_catalog_registry query failed: %s", exc)

    try:
        source_policies = _load_surface_source_policies(pg)
        sources["source_policy_registry"] = len(source_policies)
    except Exception as exc:
        logger.warning("catalog: surface_catalog_source_policy_registry query failed: %s", exc)

    try:
        capability_rows = pg.execute(
            """SELECT capability_ref, capability_slug, capability_kind,
                      title, summary, description, route
                 FROM capability_catalog
                WHERE enabled = TRUE
                ORDER BY capability_kind, title"""
        )
        for row in capability_rows or []:
            kind = _text(row.get("capability_kind")) or "task"
            slug = _text(row.get("capability_slug"))
            items.append(
                _decorate_catalog_item(
                    _with_source_policy(
                        {
                        "id": f"cap-{slug.replace('/', '-')}",
                        "label": _text(row.get("title")) or slug,
                        "icon": _KIND_TO_ICON.get(kind, "classify"),
                        "family": _KIND_TO_FAMILY.get(kind, "think"),
                        "status": "ready",
                        "dropKind": "node",
                        "actionValue": _text(row.get("route")) or f"auto/{slug}",
                        "description": _text(row.get("summary")) or _text(row.get("description")),
                        "source": "capability",
                        },
                        source_policies,
                    )
                )
            )
            sources["capabilities"] += 1
    except Exception as exc:
        logger.warning("catalog: capability_catalog query failed: %s", exc)

    try:
        integration_rows = pg.execute(
            "SELECT id, name, description, provider, capabilities, auth_status, icon "
            "FROM integration_registry ORDER BY name"
        )
        for row in integration_rows or []:
            integration_id = _text(row.get("id"))
            name = display_name_for_integration(row)
            auth = _text(row.get("auth_status")) or "unknown"
            capabilities = _json_array(row.get("capabilities"))
            if not capabilities:
                items.append(
                    _decorate_catalog_item(
                        _with_source_policy(
                            {
                            "id": f"int-{integration_id}",
                            "label": name,
                            "icon": _text(row.get("icon")) or "tool",
                            "family": "act",
                            "status": "ready" if auth == "connected" else "coming_soon",
                            "dropKind": "node",
                            "actionValue": f"@{integration_id}",
                            "description": _text(row.get("description")) or f"Use {name}",
                            "source": "integration",
                            "connectionStatus": auth,
                            },
                            source_policies,
                        )
                    )
                )
                sources["integrations"] += 1
                continue

            for capability in capabilities:
                action = _text(capability.get("action")) if isinstance(capability, dict) else _text(capability)
                description = (
                    _text(capability.get("description"))
                    if isinstance(capability, dict)
                    else ""
                )
                items.append(
                    _decorate_catalog_item(
                        _with_source_policy(
                            {
                            "id": f"int-{integration_id}-{action}".replace(" ", "-").lower(),
                            "label": f"{name}: {action}" if action else name,
                            "icon": _text(row.get("icon")) or "tool",
                            "family": "act",
                            "status": "ready" if auth == "connected" else "coming_soon",
                            "dropKind": "node",
                            "actionValue": f"@{integration_id}/{action}" if action else f"@{integration_id}",
                            "description": description or _text(row.get("description")) or f"Use {name}",
                            "source": "integration",
                            "connectionStatus": auth,
                            },
                            source_policies,
                        )
                    )
                )
                sources["integrations"] += 1
    except Exception as exc:
        logger.warning("catalog: integration_registry query failed: %s", exc)

    try:
        connector_rows = pg.execute(
            "SELECT slug, display_name, version, auth_type, base_url, status, health_status "
            "FROM connector_registry WHERE status = 'active' ORDER BY display_name"
        )
        for row in connector_rows or []:
            slug = _text(row.get("slug"))
            health = _text(row.get("health_status")) or "unknown"
            items.append(
                _decorate_catalog_item(
                    _with_source_policy(
                        {
                        "id": f"conn-{slug}",
                        "label": _text(row.get("display_name")) or slug,
                        "icon": "tool",
                        "family": "act",
                        "status": "ready" if health in ("healthy", "degraded") else "coming_soon",
                        "dropKind": "node",
                        "actionValue": f"@{slug}",
                        "description": (
                            f"v{_text(row.get('version')) or '?'}"
                            f" — {_text(row.get('auth_type'))} auth"
                            f" — {_text(row.get('base_url'))}"
                        ),
                        "source": "connector",
                        "connectionStatus": health,
                        },
                        source_policies,
                    )
                )
            )
            sources["connectors"] += 1
    except Exception:
        # connector_registry is optional in some test/dev footprints
        pass

    return {
        "items": items,
        "sources": sources,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(value, list):
        return value
    return []


def _text(value: Any) -> str:
    return str(value) if value is not None else ""


__all__ = ["build_catalog_payload"]
