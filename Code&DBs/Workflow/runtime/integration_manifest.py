"""Declarative integration manifest loader.

Scans TOML files from the manifests directory, parses them into
registry rows, and generates HTTP handlers for simple REST integrations.
Handlers delegate to execute_webhook — no duplicate HTTP logic.
"""

from __future__ import annotations

import logging
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MANIFEST_DIR = Path(__file__).resolve().parents[2] / "Integrations" / "manifests"
_MAX_MANIFEST_BYTES = 64 * 1024  # 64 KB
_MAX_CAPABILITIES = 50
_VALID_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,126}[a-zA-Z0-9]$")
_ALLOWED_URL_SCHEMES = {"http", "https"}


@dataclass(frozen=True, slots=True)
class AuthShape:
    kind: str  # "env_var", "oauth2", "api_key"
    credential_ref: str = ""
    env_var: str = ""
    scopes: tuple[str, ...] = ()
    token_url: str = ""
    authorize_url: str = ""


@dataclass(frozen=True, slots=True)
class ActionSpec:
    action: str
    description: str = ""
    method: str = "POST"
    path: str = ""
    body_template: dict[str, Any] | None = None
    response_extract: str | None = None


@dataclass(frozen=True, slots=True)
class IntegrationManifest:
    id: str
    name: str
    description: str
    provider: str
    icon: str
    auth_shape: AuthShape
    capabilities: tuple[ActionSpec, ...]


def load_manifests(manifest_dir: Path | None = None) -> list[IntegrationManifest]:
    """Scan *.toml files and return parsed manifests."""
    directory = manifest_dir or _MANIFEST_DIR
    if not directory.is_dir():
        return []

    manifests: list[IntegrationManifest] = []
    for path in sorted(directory.glob("*.toml")):
        try:
            manifests.append(_parse_manifest(path))
        except Exception as exc:
            logger.warning("manifest parse failed for %s: %s", path.name, exc)
    return manifests


def _validate_url_scheme(url: str) -> bool:
    """Reject non-HTTP(S) URL schemes."""
    if not url:
        return True  # empty is fine, means no path
    try:
        from urllib.parse import urlparse

        scheme = urlparse(url).scheme.lower()
        return scheme in _ALLOWED_URL_SCHEMES
    except Exception:
        return False


def _parse_manifest(path: Path) -> IntegrationManifest:
    if path.stat().st_size > _MAX_MANIFEST_BYTES:
        raise ValueError(f"manifest exceeds {_MAX_MANIFEST_BYTES} byte limit")

    with open(path, "rb") as f:
        raw = tomllib.load(f)

    integration = raw.get("integration", raw)
    manifest_id = str(integration.get("id", path.stem))
    if not _VALID_ID_RE.match(manifest_id):
        raise ValueError(
            f"invalid manifest id {manifest_id!r} — "
            "must be 2-128 chars of [a-zA-Z0-9._-], starting/ending alphanumeric"
        )

    auth_raw = raw.get("auth", {})
    caps_raw = raw.get("capabilities", [])
    if len(caps_raw) > _MAX_CAPABILITIES:
        raise ValueError(f"manifest has {len(caps_raw)} capabilities, max is {_MAX_CAPABILITIES}")

    auth = AuthShape(
        kind=str(auth_raw.get("kind", "env_var")),
        credential_ref=str(auth_raw.get("credential_ref", "")),
        env_var=str(auth_raw.get("env_var", "")),
        scopes=tuple(auth_raw.get("scopes", ())),
        token_url=str(auth_raw.get("token_url", "")),
        authorize_url=str(auth_raw.get("authorize_url", "")),
    )

    capabilities: list[ActionSpec] = []
    for cap in caps_raw:
        action = str(cap.get("action", "")).strip()
        if not action:
            continue
        cap_path = str(cap.get("path", ""))
        if cap_path and not _validate_url_scheme(cap_path):
            raise ValueError(f"action {action!r} has disallowed URL scheme in path: {cap_path!r}")
        method = str(cap.get("method", "POST")).upper()
        if method not in ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"):
            raise ValueError(f"action {action!r} has invalid HTTP method: {method!r}")
        capabilities.append(ActionSpec(
            action=action,
            description=str(cap.get("description", "")),
            method=method,
            path=cap_path,
            body_template=cap.get("body_template"),
            response_extract=cap.get("response_extract"),
        ))

    return IntegrationManifest(
        id=manifest_id,
        name=str(integration.get("name", path.stem)),
        description=str(integration.get("description", "")),
        provider=str(integration.get("provider", "http")),
        icon=str(integration.get("icon", "puzzle")),
        auth_shape=auth,
        capabilities=tuple(capabilities),
    )


def manifest_to_registry_row(manifest: IntegrationManifest) -> dict[str, Any]:
    """Convert a manifest to an integration_registry upsert row.

    Delegates to the canonical implementation in integration_registry.
    """
    from runtime.integrations.integration_registry import manifest_to_registry_row as _canonical
    return _canonical(manifest)


def build_manifest_handler(
    definition: dict[str, Any],
    action: str,
) -> Any:
    """Build an HTTP handler that delegates to execute_webhook.

    Returns an IntegrationHandler callable or None if the definition
    doesn't have enough information to construct one.
    """
    from runtime.integrations.integration_registry import find_capability, parse_jsonb

    cap = find_capability(definition, action)
    if not cap or not cap.get("path"):
        return None

    auth_shape = parse_jsonb(definition.get("auth_shape"))

    def handler(args: dict, pg: Any) -> dict[str, Any]:
        from runtime.integrations.webhook import execute_webhook

        # Build body from template + args
        body_template = cap.get("body_template")
        if body_template:
            body = _interpolate_template(body_template, args)
        else:
            body = {k: v for k, v in args.items() if not k.startswith("_")} or None

        # Resolve token for auth strategy
        token = resolve_token(auth_shape, pg, args.get("_integration_id", ""))

        webhook_args: dict[str, Any] = {
            "url": cap["path"],
            "method": cap.get("method", "POST"),
            "body": body,
            "timeout": 30,
        }
        # Set token directly in headers — do NOT use auth_strategy,
        # which would try to resolve the credentialRef and fail.
        if token:
            webhook_args["headers"] = {"Authorization": f"Bearer {token}"}

        result = execute_webhook(webhook_args, pg)

        # Apply response_extract if specified
        response_extract = cap.get("response_extract")
        if (
            response_extract
            and result.get("status") == "succeeded"
            and isinstance(result.get("data"), dict)
            and isinstance(result["data"].get("response"), dict)
        ):
            result["data"]["response"] = result["data"]["response"].get(
                response_extract, result["data"]["response"]
            )

        return result

    return handler


def resolve_token(
    auth_shape: dict[str, Any],
    pg: Any,
    integration_id: str,
) -> str | None:
    """Resolve a token: credential ref (OAuth) → env var via full secret chain."""
    # Check credential_ref first — OAuth tokens are short-lived and more
    # specific than static env var keys.
    credential_ref = str(auth_shape.get("credential_ref", "")).strip()
    if credential_ref:
        try:
            from adapters.credentials import resolve_credential

            cred = resolve_credential(credential_ref, conn=pg, integration_id=integration_id)
            return cred.api_key
        except Exception as exc:
            logger.debug("credential resolution failed for %s: %s", integration_id, exc)

    # Fall back to env-var lookup through the standard chain:
    # .env → macOS Keychain (service=praxis) → os.environ.
    env_var = str(auth_shape.get("env_var", "")).strip()
    if env_var:
        from adapters.keychain import resolve_secret

        val = resolve_secret(env_var)
        if val:
            return val

    return None


def _interpolate_template(
    template: dict[str, Any],
    args: dict,
) -> dict[str, Any]:
    """Replace {{key}} placeholders in template values with args values."""
    result: dict[str, Any] = {}
    for key, value in template.items():
        if isinstance(value, str) and "{{" in value:
            for arg_key, arg_val in args.items():
                value = value.replace("{{" + arg_key + "}}", str(arg_val))
            result[key] = value
        elif isinstance(value, dict):
            result[key] = _interpolate_template(value, args)
        else:
            result[key] = value
    return result
