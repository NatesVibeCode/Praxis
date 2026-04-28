"""In-app integration management — create, secret, test, reload.

Exposes the previously orphaned runtime primitives (``upsert_integration``,
``keychain_set``, ``resolve_token``, ``sync_registries``) as HTTP routes so
Moon and other clients can add a third-party integration without authoring a
TOML manifest or running the builder workflow.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote, urlparse

from surfaces._boot import sync_registries

from ._shared import RouteEntry, _exact, _prefix, _read_json_body


_VALID_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,126}[a-zA-Z0-9]$")
_VALID_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
_VALID_AUTH_KINDS = {"none", "env_var", "api_key", "oauth2"}


def _path_tail(path: str, prefix: str) -> list[str]:
    """Return the URL-decoded segments of ``path`` after ``prefix``.

    The dispatcher now hands us the URL-encoded path with an optional
    ``?query`` suffix, so we must strip the query before slicing and
    ``unquote`` each segment so encoded characters (``%2F`` inside a
    capability name, percent-encoded spaces, etc.) are restored
    after splitting.
    """
    raw = urlparse(path).path
    tail = raw[len(prefix):] if raw.startswith(prefix) else ""
    return [unquote(part) for part in tail.split("/") if part]


def _validate_capability(cap: Any) -> str | None:
    if not isinstance(cap, dict):
        return "each capability must be an object"
    action = str(cap.get("action", "")).strip()
    if not action:
        return "capability.action is required"
    method = str(cap.get("method", "POST")).upper()
    if method not in _VALID_METHODS:
        return f"capability.method invalid: {method!r}"
    path = str(cap.get("path", "")).strip()
    if path and not (path.startswith("https://") or path.startswith("http://")):
        return f"capability.path must be a full http(s) URL: {path!r}"
    return None


def _normalize_capabilities(raw: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(raw, list) or not raw:
        return [], "capabilities must be a non-empty list"
    out: list[dict[str, Any]] = []
    for cap in raw:
        err = _validate_capability(cap)
        if err:
            return [], err
        entry: dict[str, Any] = {
            "action": str(cap["action"]).strip(),
            "description": str(cap.get("description", "")),
            "method": str(cap.get("method", "POST")).upper(),
        }
        if cap.get("path"):
            entry["path"] = str(cap["path"]).strip()
        if isinstance(cap.get("body_template"), dict):
            entry["body_template"] = cap["body_template"]
        if cap.get("response_extract"):
            entry["response_extract"] = str(cap["response_extract"])
        out.append(entry)
    return out, None


def _normalize_auth(raw: Any) -> tuple[dict[str, Any], str | None]:
    if raw is None:
        return {"kind": "none"}, None
    if not isinstance(raw, dict):
        return {}, "auth must be an object"
    kind = str(raw.get("kind", "none")).strip().lower()
    if kind not in _VALID_AUTH_KINDS:
        return {}, f"auth.kind must be one of {sorted(_VALID_AUTH_KINDS)}"
    if kind in ("api_key", "env_var") and not str(raw.get("env_var", "")).strip():
        return {}, f"auth.env_var is required for kind={kind!r}"
    shape: dict[str, Any] = {
        "kind": kind,
        "credential_ref": str(raw.get("credential_ref", "")),
        "env_var": str(raw.get("env_var", "")),
        "scopes": list(raw.get("scopes") or ()),
        "token_url": str(raw.get("token_url", "")),
        "authorize_url": str(raw.get("authorize_url", "")),
    }
    return shape, None


# ── Handlers ─────────────────────────────────────────────────────────


def _handle_list(request: Any, path: str) -> None:
    del path
    try:
        from runtime.integrations.integration_registry import list_integrations
        pg = request.subsystems.get_pg_conn()
        integrations = list_integrations(pg)
        request._send_json(200, {"integrations": integrations, "count": len(integrations)})
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_describe(request: Any, path: str) -> None:
    segments = _path_tail(path, "/api/integrations/")
    if len(segments) != 1:
        # Route only matches GETs with exactly one segment; suffix routes
        # ('/secret', '/test') handle their own posts/puts.
        request._send_json(404, {"error": "not found"})
        return
    integration_id = segments[0]
    try:
        from runtime.integrations.integration_registry import describe_integration
        pg = request.subsystems.get_pg_conn()
        result = describe_integration(pg, integration_id)
        if result is None:
            request._send_json(404, {"error": f"integration {integration_id!r} not found"})
            return
        request._send_json(200, result)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_create(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception as exc:
        request._send_json(400, {"error": f"invalid JSON: {exc}"})
        return
    if not isinstance(body, dict):
        request._send_json(400, {"error": "body must be an object"})
        return

    integration_id = str(body.get("id", "")).strip()
    if not _VALID_ID_RE.match(integration_id):
        request._send_json(400, {"error": "id must be 2-128 chars of [a-zA-Z0-9._-], start/end alphanumeric"})
        return

    name = str(body.get("name", "")).strip()
    if not name:
        request._send_json(400, {"error": "name is required"})
        return

    caps, cap_err = _normalize_capabilities(body.get("capabilities"))
    if cap_err:
        request._send_json(400, {"error": cap_err})
        return

    auth_shape, auth_err = _normalize_auth(body.get("auth"))
    if auth_err:
        request._send_json(400, {"error": auth_err})
        return

    description = str(body.get("description", ""))
    provider = str(body.get("provider", "http")).strip() or "http"
    manifest_source = str(body.get("manifest_source", "api")).strip() or "api"

    try:
        from runtime.integrations.integration_registry import upsert_integration
        pg = request.subsystems.get_pg_conn()
        upsert_integration(
            pg,
            integration_id=integration_id,
            name=name,
            description=description,
            provider=provider,
            capabilities=caps,
            auth_status="connected" if auth_shape.get("kind") == "none" else "pending",
            manifest_source=manifest_source,
            connector_slug=None,
            auth_shape=auth_shape,
        )
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return

    request._send_json(201, {
        "integration_id": integration_id,
        "status": "created",
        "capabilities": [c["action"] for c in caps],
        "auth_shape": auth_shape,
    })


def _handle_set_secret(request: Any, path: str) -> None:
    segments = _path_tail(path, "/api/integrations/")
    if len(segments) != 2 or segments[1] != "secret":
        request._send_json(404, {"error": "not found"})
        return
    integration_id = segments[0]
    try:
        body = _read_json_body(request)
    except Exception as exc:
        request._send_json(400, {"error": f"invalid JSON: {exc}"})
        return
    if not isinstance(body, dict) or not str(body.get("value", "")).strip():
        request._send_json(400, {"error": "value is required"})
        return
    value = str(body["value"])

    try:
        from runtime.integrations.integration_registry import load_authority, parse_jsonb
        pg = request.subsystems.get_pg_conn()
        definition = load_authority(pg, integration_id)
        if definition is None:
            request._send_json(404, {"error": f"integration {integration_id!r} not found"})
            return
        auth_shape = parse_jsonb(definition.get("auth_shape"))
        env_var = str(auth_shape.get("env_var", "")).strip()
        if not env_var:
            request._send_json(400, {"error": "integration has no auth.env_var — nothing to store"})
            return
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return

    try:
        from adapters.keychain import keychain_set
        ok = keychain_set(env_var, value)
    except Exception as exc:
        request._send_json(500, {"error": f"keychain write failed: {exc}"})
        return

    if not ok:
        request._send_json(503, {
            "error": "keychain_set unavailable (non-Darwin or security CLI failed)",
            "hint": f"set env var {env_var} via .env or environment variable as fallback",
        })
        return

    try:
        pg.execute(
            "UPDATE integration_registry SET auth_status = 'connected', updated_at = now() WHERE id = $1",
            integration_id,
        )
    except Exception:
        pass

    request._send_json(200, {
        "integration_id": integration_id,
        "env_var": env_var,
        "stored": True,
    })


def _handle_test(request: Any, path: str) -> None:
    segments = _path_tail(path, "/api/integrations/")
    if len(segments) != 2 or segments[1] != "test":
        request._send_json(404, {"error": "not found"})
        return
    integration_id = segments[0]
    try:
        from surfaces.mcp.tools.integration import tool_praxis_integration
        result = tool_praxis_integration({
            "action": "test_credentials",
            "integration_id": integration_id,
        })
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return

    status = 200 if result.get("credential_status") == "valid" else 200
    if result.get("error"):
        status = 404 if "not found" in result["error"] else 400
    request._send_json(status, result)


def _handle_reload(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        succeeded, failures = sync_registries(pg)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return
    request._send_json(200, {
        "synced": len(succeeded),
        "components": succeeded,
        "failures": failures,
    })


# ── Route tables ─────────────────────────────────────────────────────


INTEGRATIONS_GET_ROUTES: list[RouteEntry] = [
    (_exact("/api/integrations"), _handle_list),
    (_prefix("/api/integrations/"), _handle_describe),
]

INTEGRATIONS_POST_ROUTES: list[RouteEntry] = [
    (_exact("/api/integrations"), _handle_create),
    (_exact("/api/integrations/reload"), _handle_reload),
    (_prefix("/api/integrations/"), _handle_test),
]

INTEGRATIONS_PUT_ROUTES: list[RouteEntry] = [
    (_prefix("/api/integrations/"), _handle_set_secret),
]


__all__ = [
    "INTEGRATIONS_GET_ROUTES",
    "INTEGRATIONS_POST_ROUTES",
    "INTEGRATIONS_PUT_ROUTES",
    "_handle_create",
    "_handle_describe",
    "_handle_list",
    "_handle_reload",
    "_handle_set_secret",
    "_handle_test",
]
