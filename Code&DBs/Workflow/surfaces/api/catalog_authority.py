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

_EXECUTABLE_GATE_FAMILIES = frozenset({"approval", "conditional", "after_failure"})
_PERSISTED_GATE_FAMILIES = frozenset({"human_review"})
_RUNTIME_NODE_ROUTES = frozenset(
    {
        "trigger",
        "trigger/schedule",
        "trigger/webhook",
        "auto/research",
        "auto/draft",
        "auto/classify",
        "workflow.fanout",
        "@notifications/send",
        "@webhook/post",
        "@workflow/invoke",
    }
)


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
        "actionValue": "workflow.fanout",
        "description": "Split into parallel sub-tasks and aggregate",
    },
    {
        "id": "think-fan-out-legacy",
        "label": "Fan Out (Legacy)",
        "icon": "classify",
        "family": "think",
        "status": "ready",
        "dropKind": "node",
        "actionValue": "auto/fan-out",
        "description": "Legacy fan-out token kept for older saved graphs",
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
        "description": "Automated verification command gate",
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


def _catalog_truth(item: dict[str, Any]) -> dict[str, str]:
    if _text(item.get("status")) == "coming_soon":
        return {
            "category": "coming_soon",
            "badge": "Soon",
            "detail": "Listed in the catalog, but not enabled in the current surface.",
        }

    if _text(item.get("dropKind")) == "edge":
        gate_family = _text(item.get("gateFamily"))
        if gate_family in _EXECUTABLE_GATE_FAMILIES:
            return {
                "category": "runtime",
                "badge": "Executes",
                "detail": "Compiled into dependency edges that change runtime flow today.",
            }
        if gate_family == "validation":
            return {
                "category": "runtime",
                "badge": "Executes",
                "detail": "Runs the configured verification command before the downstream step can continue.",
            }
        if gate_family == "retry":
            return {
                "category": "runtime",
                "badge": "Executes",
                "detail": "Sets the downstream job's max_attempts so failed work can requeue through the runtime retry loop.",
            }
        if gate_family in _PERSISTED_GATE_FAMILIES:
            return {
                "category": "persisted",
                "badge": "Saved only",
                "detail": "Stored in edge metadata now, but not enforced by the planner yet.",
            }
        return {
            "category": "partial",
            "badge": "Unverified",
            "detail": "Stored in the graph, but the runtime meaning is not verified yet.",
        }

    item_id = _text(item.get("id"))
    action_value = _text(item.get("actionValue"))
    source = _text(item.get("source"))

    if item_id == "gather-docs":
        return {
            "category": "alias",
            "badge": "Alias",
            "detail": "Uses the same `auto/research` route as Web Research today.",
        }

    if action_value == "auto/classify":
        return {
            "category": "runtime",
            "badge": "Runs on release",
            "detail": "Uses the analysis lane backed by task_type_route_profiles and task_type_routing authority.",
        }

    if action_value == "auto/fan-out":
        return {
            "category": "alias",
            "badge": "Alias",
            "detail": "Legacy fan-out token kept for existing saved graphs; Moon uses `workflow.fanout` now.",
        }

    if action_value == "workflow.fanout":
        return {
            "category": "runtime",
            "badge": "Runs on release",
            "detail": "Fan-out now has a verified runtime lane and compiles into the same release path as other core step routes.",
        }

    if source in {"capability", "integration", "connector"} or action_value in _RUNTIME_NODE_ROUTES:
        return {
            "category": "runtime",
            "badge": "Runs on release",
            "detail": (
                "Creates trigger intent that is preserved into compiled triggers."
                if action_value.startswith("trigger")
                else "Persists into the build graph and becomes a planned runtime route at release."
            ),
        }

    return {
        "category": "partial",
        "badge": "Unverified",
        "detail": "The UI can assign this action, but the runtime lane is not verified in source yet.",
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

    if _text(item.get("dropKind")) == "edge":
        gate_family = _text(item.get("gateFamily"))
        if gate_family in {"conditional", "after_failure"}:
            return {
                "tier": "primary",
                "badge": "Core now",
                "detail": "This is one of the few gate types that changes execution today.",
            }
        if gate_family == "approval":
            return {
                "tier": "primary",
                "badge": "Core now",
                "detail": "Pauses the downstream step behind a human approval checkpoint before execution continues.",
            }
        if gate_family == "validation":
            return {
                "tier": "primary",
                "badge": "Core now",
                "detail": "Executes the configured verification command before the downstream step proceeds.",
            }
        if gate_family == "human_review":
            return {
                "tier": "hidden",
                "badge": "Removed",
                "detail": "Folded into Approval so Moon keeps one obvious human gate concept.",
                "hardChoice": "Collapsed into Approval. Two human gate names for one future concept would be noise.",
            }
        if gate_family == "retry":
            return {
                "tier": "advanced",
                "badge": "Later",
                "detail": "Feeds retry policy into downstream job max_attempts, but stays outside the core gate set.",
            }
        if truth.get("category") == "runtime":
            return {
                "tier": "advanced",
                "badge": "Later",
                "detail": "Real edge behavior, but not part of the curated Moon gate set yet.",
            }
        return {
            "tier": "hidden",
            "badge": "Removed",
            "detail": "Saved-only edge metadata stays out of the main gate surface until it changes execution.",
        }

    item_id = _text(item.get("id"))
    action_value = _text(item.get("actionValue"))
    source = _text(item.get("source"))

    if item_id == "gather-docs":
        return {
            "tier": "hidden",
            "badge": "Merged",
            "detail": "Merged into Web Research because both buttons point at the same route today.",
            "hardChoice": "Merged into Web Research. One route gets one obvious button.",
        }

    if action_value == "auto/classify":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Backed by a real analysis lane instead of borrowing the support route.",
        }

    if _text(item.get("family")) == "trigger":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Primary trigger primitive with real compile and release authority.",
        }

    if action_value in {"auto/research", "auto/classify", "auto/draft"}:
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Primary Moon step primitive with a real planned runtime route.",
        }

    if action_value == "workflow.fanout":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Fan-out now has a verified runtime lane, so Moon can surface it as a core builder primitive.",
        }

    if action_value == "auto/fan-out":
        return {
            "tier": "hidden",
            "badge": "Alias",
            "detail": "Legacy token only, kept so older graphs still open cleanly.",
            "hardChoice": "Compatibility alias for saved graphs only.",
        }

    if action_value == "@notifications/send":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Real action primitive with a stable property surface in the node inspector.",
        }

    if action_value == "@webhook/post":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Visible now that Moon offers opinionated request presets instead of a blank transport form.",
        }

    if action_value == "@workflow/invoke":
        return {
            "tier": "primary",
            "badge": "Core now",
            "detail": "Visible now that Moon can pick saved child workflows by name from the inspector.",
        }

    if source in {"capability", "integration", "connector"}:
        if truth.get("category") == "runtime":
            return {
                "tier": "hidden",
                "badge": "Hidden",
                "detail": "Live catalog lanes stay out of the main Moon builder until they map cleanly onto the core primitive set.",
            }
        return {
            "tier": "hidden",
            "badge": "Removed",
            "detail": "Keep non-core live catalog items off the main builder unless their runtime contract is explicit.",
        }

    if truth.get("category") == "runtime":
        return {
            "tier": "advanced",
            "badge": "Later",
            "detail": "Real route, but not part of the curated core surface yet.",
        }

    return {
        "tier": "hidden",
        "badge": "Removed",
        "detail": (
            "Alias routes stay out of the main builder."
            if truth.get("category") == "alias"
            else "Non-core buttons stay hidden until they have one obvious runtime meaning."
        ),
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


def build_catalog_payload(pg: Any) -> dict[str, Any]:
    """Project static and database-backed catalog items into API payload form."""

    items: list[dict[str, Any]] = [_decorate_catalog_item(dict(item)) for item in STATIC_CATALOG_ITEMS]
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
                _decorate_catalog_item(
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
                    }
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


__all__ = ["STATIC_CATALOG_ITEMS", "build_catalog_payload"]
