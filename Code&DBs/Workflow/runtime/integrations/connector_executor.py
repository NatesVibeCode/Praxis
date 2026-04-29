"""Execute a generated connector client as an integration.

Dynamically imports the built connector module, resolves credentials,
calls the requested method, and returns an IntegrationResult.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import concurrent.futures
import importlib
import inspect
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 30
_ALLOWED_MODULE_PREFIXES = ("artifacts.connectors.",)


def execute_connector(
    definition: dict[str, Any],
    action: str,
    args: dict[str, Any],
    pg: Any,
) -> dict[str, Any]:
    """Call a generated connector client method.

    ``definition`` is the integration_registry row (must have ``connector_slug``).
    """
    connector_slug = (definition.get("connector_slug") or "").strip()
    if not connector_slug:
        return _fail("connector_slug missing on integration definition", "connector_slug_missing")

    # ── Circuit breaker gate ─────────────────────────────────────────
    if not _circuit_breaker_allows(connector_slug):
        return _fail(
            f"Circuit breaker open for connector '{connector_slug}'",
            "connector_circuit_open",
        )

    # ── Load connector details from registry ─────────────────────────
    from runtime.integrations.connector_registry import get_connector

    connector = get_connector(pg, connector_slug)
    if connector is None:
        return _fail(f"Connector '{connector_slug}' not found in connector_registry", "connector_not_found")

    module_path = (connector.get("module_path") or "").strip()
    if not module_path:
        return _fail(f"Connector '{connector_slug}' has no module_path", "connector_module_path_missing")

    # ── Module path validation ───────────────────────────────────────
    if not any(module_path.startswith(prefix) for prefix in _ALLOWED_MODULE_PREFIXES):
        return _fail(
            f"Connector module path '{module_path}' is outside allowed prefixes",
            "connector_module_path_rejected",
        )

    # ── Dynamic import ───────────────────────────────────────────────
    try:
        mod = importlib.import_module(module_path)
    except Exception as exc:
        return _fail(f"Failed to import connector module '{module_path}': {exc}", "connector_import_failed")

    from runtime.integrations.connector_registrar import find_client_class

    client_class = find_client_class(mod)
    if client_class is None:
        return _fail(
            f"No client class found in '{module_path}' (expected a class ending in 'Client')",
            "connector_client_class_not_found",
        )

    # ── Credential pre-flight ────────────────────────────────────────
    from runtime.integrations.integration_registry import parse_jsonb

    auth_shape = parse_jsonb(definition.get("auth_shape"))

    from runtime.integration_manifest import resolve_token

    token = resolve_token(auth_shape, pg, definition.get("id", ""))

    auth_kind = str(auth_shape.get("kind", "")).strip().lower()
    if token is None and auth_kind not in ("none", "anonymous", ""):
        return _fail(
            f"Credential resolution failed for connector '{connector_slug}' "
            f"(auth kind: {auth_kind})",
            "connector_credential_missing",
        )

    # ── Instantiate client ───────────────────────────────────────────
    try:
        client = _instantiate_client(client_class, token=token, base_url=connector.get("base_url"))
    except Exception as exc:
        return _fail(f"Failed to instantiate connector client: {exc}", "connector_instantiation_failed")

    method = getattr(client, action, None)
    if method is None or not callable(method):
        return _fail(
            f"Connector '{connector_slug}' has no method '{action}'",
            "connector_method_not_found",
        )

    # ── Execute with timeout ─────────────────────────────────────────
    timeout_s = connector.get("timeout_s") or _DEFAULT_TIMEOUT_S
    try:
        if inspect.iscoroutinefunction(method):
            result = _call_async(method, args, timeout_s)
        else:
            result = _call_sync(method, args, timeout_s)
    except Exception as exc:
        error_code = _classify_error(exc)
        _record_outcome(pg, connector_slug, succeeded=False, error_code=error_code)
        return _fail(f"Connector call {connector_slug}/{action} failed: {exc}", error_code)

    _record_outcome(pg, connector_slug, succeeded=True, error_code=None)

    if isinstance(result, dict):
        data = result
    else:
        data = {"result": result}

    return {
        "status": "succeeded",
        "data": data,
        "summary": f"{connector_slug}/{action} completed",
        "error": None,
    }


# ── Call helpers with timeout ────────────────────────────────────────


def _call_async(method: Any, args: dict, timeout_s: int) -> Any:
    """Bridge async method to sync with a timeout."""
    async def _run():
        return await asyncio.wait_for(method(**args), timeout=timeout_s)
    return run_sync_safe(_run())


def _call_sync(method: Any, args: dict, timeout_s: int) -> Any:
    """Call sync method with a thread-pool timeout."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(method, **args)
        return future.result(timeout=timeout_s)


# ── Error classification ─────────────────────────────────────────────


def _classify_error(exc: Exception) -> str:
    """Classify a connector exception into a specific error code."""
    exc_str = str(exc).lower()

    if isinstance(exc, (asyncio.TimeoutError, concurrent.futures.TimeoutError, TimeoutError)):
        return "connector_timeout"
    if isinstance(exc, (ConnectionError, OSError)):
        return "connector_network_error"

    status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        if status == 429:
            return "connector_rate_limited"
        if status in (401, 403):
            return "connector_auth_error"
        if 400 <= status < 500:
            return "connector_input_error"
        if status >= 500:
            return "connector_server_error"

    if "rate limit" in exc_str or "too many requests" in exc_str:
        return "connector_rate_limited"
    if "unauthorized" in exc_str or "forbidden" in exc_str or "auth" in exc_str:
        return "connector_auth_error"
    if "timeout" in exc_str:
        return "connector_timeout"
    if "connection" in exc_str or "dns" in exc_str or "resolve" in exc_str:
        return "connector_network_error"

    return "connector_call_failed"


# ── Circuit breaker ──────────────────────────────────────────────────


def _circuit_breaker_allows(slug: str) -> bool:
    try:
        from runtime.circuit_breaker import get_circuit_breakers
        return get_circuit_breakers().allow_request(slug)
    except Exception:
        return True


def _record_outcome(pg: Any, slug: str, *, succeeded: bool, error_code: str | None) -> None:
    """Update circuit breaker + registry health counters."""
    try:
        from runtime.circuit_breaker import get_circuit_breakers
        get_circuit_breakers().record_outcome(slug, succeeded=succeeded, failure_code=error_code)
    except Exception:
        pass

    try:
        from runtime.integrations.connector_registry import update_health
        update_health(
            pg, slug,
            health_status="healthy" if succeeded else "degraded",
            total_calls_delta=1,
            total_errors_delta=0 if succeeded else 1,
            error=not succeeded,
        )
    except Exception as exc:
        logger.debug("connector health update failed for %s: %s", slug, exc)


# ── Helpers ──────────────────────────────────────────────────────────


def _fail(summary: str, error_code: str) -> dict[str, Any]:
    logger.warning("connector_executor: %s", summary)
    return {"status": "failed", "data": None, "summary": summary, "error": error_code}


def _instantiate_client(client_class: type, *, token: str | None, base_url: str | None) -> Any:
    """Instantiate the client class, passing whichever kwargs it accepts."""
    sig = inspect.signature(client_class.__init__)
    params = set(sig.parameters.keys()) - {"self"}

    kwargs: dict[str, Any] = {}
    if token:
        for name in ("api_key", "token", "auth_token", "access_token", "key"):
            if name in params:
                kwargs[name] = token
                break
    if base_url:
        for name in ("base_url", "base", "url"):
            if name in params:
                kwargs[name] = base_url
                break

    return client_class(**kwargs)
