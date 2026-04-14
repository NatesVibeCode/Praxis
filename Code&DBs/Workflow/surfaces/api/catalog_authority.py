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


STATIC_CATALOG_ITEMS: tuple[dict[str, Any], ...] = (
    {
        "id": "trigger-manual",
        "label": "Manual",
        "icon": "trigger",
        "family": "trigger",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "trigger",
        "description": "User-initiated run",
    },
    {
        "id": "trigger-webhook",
        "label": "Webhook",
        "icon": "tool",
        "family": "trigger",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "trigger/webhook",
        "description": "Inbound webhook with HMAC verification",
    },
    {
        "id": "trigger-schedule",
        "label": "Schedule",
        "icon": "trigger",
        "family": "trigger",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "trigger/schedule",
        "description": "Cron or interval trigger",
    },
    {
        "id": "gather-research",
        "label": "Web Research",
        "icon": "research",
        "family": "gather",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/research",
        "description": "Search and analyze web sources",
    },
    {
        "id": "gather-docs",
        "label": "Docs",
        "icon": "research",
        "family": "gather",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/research",
        "description": "Read and extract from documents",
    },
    {
        "id": "think-classify",
        "label": "Classify",
        "icon": "classify",
        "family": "think",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/classify",
        "description": "Score, triage, or categorize",
    },
    {
        "id": "think-draft",
        "label": "Draft",
        "icon": "draft",
        "family": "think",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/draft",
        "description": "Generate or compose content",
    },
    {
        "id": "think-fan-out",
        "label": "Fan Out",
        "icon": "classify",
        "family": "think",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/fan-out",
        "description": "Split into parallel sub-tasks and aggregate",
    },
    {
        "id": "act-notify",
        "label": "Notify",
        "icon": "notify",
        "family": "act",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "@notifications/send",
        "description": "Send notification (Slack, email, etc.)",
    },
    {
        "id": "act-webhook-out",
        "label": "HTTP Request",
        "icon": "tool",
        "family": "act",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "@webhook/post",
        "description": "Call an external webhook or API",
    },
    {
        "id": "act-invoke",
        "label": "Run Workflow",
        "icon": "tool",
        "family": "act",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "@workflow/invoke",
        "description": "Invoke another workflow as a sub-workflow",
    },
    {
        "id": "ctrl-approval",
        "label": "Approval",
        "icon": "gate",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "approval",
        "description": "Human approval gate",
    },
    {
        "id": "ctrl-review",
        "label": "Human Review",
        "icon": "review",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "human_review",
        "description": "Manual review before proceeding",
    },
    {
        "id": "ctrl-validation",
        "label": "Validation",
        "icon": "gate",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "validation",
        "description": "Automated check gate",
    },
    {
        "id": "ctrl-branch",
        "label": "Branch",
        "icon": "gate",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "conditional",
        "description": "Conditional path (equals, in, not_equals, not_in)",
    },
    {
        "id": "ctrl-retry",
        "label": "Retry",
        "icon": "gate",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "retry",
        "description": "Retry with backoff + provider failover chain",
    },
    {
        "id": "ctrl-on-failure",
        "label": "On Failure",
        "icon": "gate",
        "family": "control",
        "status": "ready",
        "dropKind": "edge",
        "gateFamily": "after_failure",
        "description": "Run only if upstream step failed",
    },
)

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


def build_catalog_payload(pg: Any) -> dict[str, Any]:
    """Project static and database-backed catalog items into API payload form."""

    items: list[dict[str, Any]] = [dict(item) for item in STATIC_CATALOG_ITEMS]
    sources: dict[str, int] = {
        "static": len(items),
        "capabilities": 0,
        "integrations": 0,
        "connectors": 0,
    }

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
                }
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
                    }
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
                    }
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
                {
                    "id": f"conn-{slug}",
                    "label": _text(row.get("display_name")) or slug,
                    "icon": "tool",
                    "family": "act",
                    "status": "ready" if health in ("healthy", "degraded") else "coming_soon",
                    "dropKind": "node",
                    "actionValue": f"@connector/{slug}",
                    "description": (
                        f"v{_text(row.get('version')) or '?'}"
                        f" — {_text(row.get('auth_type'))} auth"
                        f" — {_text(row.get('base_url'))}"
                    ),
                    "source": "connector",
                    "connectionStatus": health,
                }
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


__all__ = ["STATIC_CATALOG_ITEMS", "build_catalog_payload"]
