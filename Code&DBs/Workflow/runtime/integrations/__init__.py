"""Integration execution bindings.

Postgres owns integration identity, descriptions, and advertised actions through
`integration_registry`. Python only owns the executor binding that implements a
registered `(integration_id, action)` capability.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, TypedDict

logger = logging.getLogger(__name__)


class IntegrationResult(TypedDict, total=False):
    status: str
    data: Any
    summary: str
    error: str | None


IntegrationHandler = Callable[[dict, Any], IntegrationResult]


def _build_bindings() -> dict[tuple[str, str], IntegrationHandler]:
    bindings: dict[tuple[str, str], IntegrationHandler] = {}
    try:
        from .platform import (
            execute_check_status,
            execute_dispatch_job,
            execute_notification,
            execute_search_receipts,
            execute_workflow_cancel,
            execute_workflow_invoke,
        )
        bindings[("notifications", "send")] = execute_notification
        bindings[("praxis-dispatch", "dispatch_job")] = execute_dispatch_job
        bindings[("praxis-dispatch", "check_status")] = execute_check_status
        bindings[("praxis-dispatch", "search_receipts")] = execute_search_receipts
        bindings[("workflow", "invoke")] = execute_workflow_invoke
        bindings[("workflow", "cancel")] = execute_workflow_cancel
    except Exception as exc:  # pragma: no cover
        logger.warning("platform integration handlers unavailable: %s", exc)
    try:
        from .webhook import execute_webhook
        bindings[("webhook", "post")] = execute_webhook
    except Exception as exc:  # pragma: no cover
        logger.warning("webhook handler unavailable: %s", exc)
    return bindings


_INTEGRATION_BINDINGS: dict[tuple[str, str], IntegrationHandler] = _build_bindings()


def _load_integration_authority(pg: Any, integration_id: str) -> dict[str, Any] | None:
    from runtime.integrations.integration_registry import load_authority
    return load_authority(pg, integration_id)


def _is_catalog_mcp_integration(definition: dict[str, Any]) -> bool:
    return bool(definition.get("catalog_dispatch"))


def _execute_catalog_mcp_integration(
    integration_id: str,
    action: str,
    args: dict,
) -> IntegrationResult:
    from surfaces.mcp.catalog import get_tool_catalog, resolve_tool_entry

    tool = get_tool_catalog().get(integration_id)
    if tool is None:
        return {
            "status": "failed",
            "data": None,
            "summary": f"MCP tool '{integration_id}' is not registered in the catalog.",
            "error": "mcp_tool_not_found",
        }

    params = dict(args or {})
    if tool.selector_field == "action":
        if action in tool.action_enum:
            params["action"] = action
        else:
            params.pop("action", None)
    elif tool.selector_field == "view":
        if action in tool.view_enum:
            params["view"] = action
        else:
            params.pop("view", None)
    else:
        params.pop("action", None)

    try:
        handler, _ = resolve_tool_entry(integration_id)
        result = handler(params)
    except Exception as exc:
        logger.error("MCP integration %s/%s failed: %s", integration_id, action, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"MCP tool error: {exc}",
            "error": "mcp_tool_exception",
        }

    if isinstance(result, dict) and result.get("error"):
        return {
            "status": "failed",
            "data": result,
            "summary": str(result.get("error")),
            "error": "mcp_tool_failed",
        }

    summary = (
        str(result.get("message"))
        if isinstance(result, dict) and result.get("message")
        else f"MCP tool {integration_id}/{action} completed."
    )
    return {
        "status": "succeeded",
        "data": result,
        "summary": summary,
        "error": None,
    }


def _resolve_manifest_handler(
    definition: dict[str, Any],
    action: str,
) -> IntegrationHandler | None:
    """Try to build a handler from manifest-shaped data in the definition.

    Any integration whose capabilities carry a full-URL ``path`` + ``method``
    can be served by the generic webhook executor — regardless of whether
    the row was sourced from a TOML manifest on disk, a DB-native API POST,
    or an MCP ``create`` call. The ``manifest_source`` column labels origin;
    it does not gate executability.
    """
    source = str(definition.get("manifest_source") or "").strip().lower()
    if source not in {"manifest", "api", "mcp", "ui"}:
        return None
    try:
        from runtime.integration_manifest import build_manifest_handler

        return build_manifest_handler(definition, action)
    except Exception as exc:
        logger.warning("manifest handler resolution failed: %s", exc)
        return None


def execute_integration(
    integration_id: str,
    action: str,
    args: dict,
    pg: Any,
) -> IntegrationResult:
    """Execute an integration tool using Postgres-owned integration authority."""
    if pg is None:
        return {
            "status": "failed",
            "data": None,
            "summary": "Integration execution requires Postgres authority.",
            "error": "integration_authority_unavailable",
        }

    try:
        definition = _load_integration_authority(pg, integration_id)
    except Exception as exc:
        logger.error("Integration authority lookup failed for %s/%s: %s", integration_id, action, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"Integration authority lookup failed: {exc}",
            "error": "integration_authority_lookup_failed",
        }

    if definition is None:
        return {
            "status": "failed",
            "data": None,
            "summary": f"Unknown integration: {integration_id}",
            "error": "integration_not_found",
        }

    if str(definition.get("auth_status") or "").strip().lower() != "connected":
        return {
            "status": "failed",
            "data": None,
            "summary": f"Integration '{integration_id}' is not connected.",
            "error": "integration_not_connected",
        }

    available_actions = [
        str(item.get("action"))
        for item in definition.get("capabilities") or []
        if isinstance(item, dict) and item.get("action")
    ]
    if action not in available_actions:
        return {
            "status": "failed",
            "data": None,
            "summary": (
                f"Unknown action '{action}' for integration '{integration_id}'. "
                f"Available: {available_actions}"
            ),
            "error": "action_not_found",
        }

    if _is_catalog_mcp_integration(definition):
        return _execute_catalog_mcp_integration(integration_id, action, args, )

    if definition.get("connector_slug"):
        from runtime.integrations.connector_executor import execute_connector

        return execute_connector(definition, action, args, pg)

    handler = _INTEGRATION_BINDINGS.get((integration_id, action))
    if handler is None:
        handler = _resolve_manifest_handler(definition, action)
    if handler is None:
        return {
            "status": "failed",
            "data": None,
            "summary": (
                f"Integration '{integration_id}' action '{action}' is registered in Postgres "
                "but has no bound executor."
            ),
            "error": "integration_executor_not_bound",
        }

    try:
        result = handler(args, pg)
        logger.info(
            "Integration %s/%s completed: status=%s",
            integration_id,
            action,
            result.get("status", "unknown"),
        )
        return result
    except Exception as exc:
        logger.error("Integration %s/%s failed: %s", integration_id, action, exc)
        return {
            "status": "failed",
            "data": None,
            "summary": f"Integration error: {exc}",
            "error": "integration_exception",
        }
