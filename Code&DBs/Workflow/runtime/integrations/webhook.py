"""Generic webhook integration.

Sends HTTP requests to arbitrary URLs — the universal integration escape hatch.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import socket
import urllib.parse
import urllib.request
import urllib.error
from typing import Any

from adapters.credentials import CredentialResolutionError, resolve_credential

logger = logging.getLogger(__name__)

_BLOCKED_WEBHOOK_HOSTS = frozenset(
    {
        "localhost",
        "metadata.google.internal",
    }
)
_BLOCKED_WEBHOOK_NETWORKS = tuple(
    ipaddress.ip_network(network)
    for network in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "::/128",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
    )
)


def _resolve_auth_strategy(
    args: dict,
    headers: dict[str, str],
    url: str,
    pg: Any = None,
) -> tuple[dict[str, str], str] | tuple[None, None]:
    auth_strategy = args.get("auth_strategy")
    if not isinstance(auth_strategy, dict):
        return headers, url

    mode = str(auth_strategy.get("mode") or "none").strip().lower()
    if mode == "none":
        return headers, url

    credential_ref = str(auth_strategy.get("credentialRef") or auth_strategy.get("credential_ref") or "").strip()
    if not credential_ref:
        return None, None

    integration_id = str(auth_strategy.get("integration_id") or args.get("_integration_id") or "").strip() or None
    try:
        credential = resolve_credential(
            credential_ref,
            conn=pg,
            integration_id=integration_id,
            auth_shape=auth_strategy,
        )
    except CredentialResolutionError as exc:
        logger.warning("webhook credential resolution failed for %s: %s", credential_ref, exc)
        return None, None

    token = credential.api_key
    if mode == "bearer_token":
        header_name = str(auth_strategy.get("headerName") or auth_strategy.get("header_name") or "Authorization").strip() or "Authorization"
        token_prefix = str(auth_strategy.get("tokenPrefix") or auth_strategy.get("token_prefix") or "Bearer").strip() or "Bearer"
        headers[header_name] = f"{token_prefix} {token}".strip()
        return headers, url

    if mode == "api_key_header":
        header_name = str(auth_strategy.get("headerName") or auth_strategy.get("header_name") or "X-API-Key").strip() or "X-API-Key"
        headers[header_name] = token
        return headers, url

    if mode == "api_key_query":
        query_param = str(auth_strategy.get("queryParam") or auth_strategy.get("query_param") or "api_key").strip() or "api_key"
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        query.append((query_param, token))
        next_url = urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))
        return headers, next_url

    return headers, url


def _resolve_url(args: dict) -> str:
    endpoint_map = args.get("endpoint_map")
    connector_spec = args.get("connector_spec")
    endpoint = args.get("url", args.get("endpoint", ""))

    primary_endpoint = None
    if isinstance(endpoint_map, list):
        primary_endpoint_id = ""
        if isinstance(connector_spec, dict):
            primary_endpoint_id = str(connector_spec.get("primaryEndpointId") or connector_spec.get("primary_endpoint_id") or "").strip()
        if primary_endpoint_id:
            primary_endpoint = next((item for item in endpoint_map if isinstance(item, dict) and str(item.get("id")) == primary_endpoint_id), None)
        if primary_endpoint is None:
            primary_endpoint = next((item for item in endpoint_map if isinstance(item, dict)), None)

    if not endpoint and isinstance(primary_endpoint, dict):
        endpoint = str(primary_endpoint.get("path") or "").strip()

    if endpoint.startswith("http"):
        return endpoint

    base_url = ""
    if isinstance(connector_spec, dict):
        base_url = str(connector_spec.get("baseUrl") or connector_spec.get("base_url") or "").strip()

    if base_url and endpoint:
        return urllib.parse.urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))

    return endpoint


def _resolve_method_and_body(args: dict) -> tuple[str, Any]:
    method = str(args.get("method", "POST")).upper()
    body_raw = args.get("body", args.get("body_template", ""))
    endpoint_map = args.get("endpoint_map")
    connector_spec = args.get("connector_spec")
    if not isinstance(endpoint_map, list):
        return method, body_raw

    primary_endpoint_id = ""
    if isinstance(connector_spec, dict):
        primary_endpoint_id = str(connector_spec.get("primaryEndpointId") or connector_spec.get("primary_endpoint_id") or "").strip()
    primary_endpoint = None
    if primary_endpoint_id:
        primary_endpoint = next((item for item in endpoint_map if isinstance(item, dict) and str(item.get("id")) == primary_endpoint_id), None)
    if primary_endpoint is None:
        primary_endpoint = next((item for item in endpoint_map if isinstance(item, dict)), None)
    if not isinstance(primary_endpoint, dict):
        return method, body_raw

    if "method" not in args and primary_endpoint.get("method"):
        method = str(primary_endpoint.get("method")).upper()
    if "body" not in args and "body_template" not in args and primary_endpoint.get("requestBodyTemplate") is not None:
        body_raw = primary_endpoint.get("requestBodyTemplate")
    return method, body_raw


def _method_supports_body(method: str) -> bool:
    normalized = str(method or "").upper()
    return normalized not in {"GET", "DELETE"}


def _webhook_ip_is_blocked(address: str) -> bool:
    try:
        ip_address = ipaddress.ip_address(address.split("%", 1)[0])
    except ValueError:
        return False
    if any(ip_address in network for network in _BLOCKED_WEBHOOK_NETWORKS):
        return True
    return (
        ip_address.is_loopback
        or ip_address.is_link_local
        or ip_address.is_private
        or ip_address.is_multicast
        or ip_address.is_unspecified
        or ip_address.is_reserved
    )


def _validate_webhook_url(url: str) -> tuple[bool, str]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        return False, f"Invalid URL: {url}"
    try:
        port = parsed.port
    except ValueError:
        return False, f"Invalid URL: {url}"

    host = (parsed.hostname or "").strip().rstrip(".").lower()
    if not host:
        return False, f"Invalid URL: {url}"
    if (
        host in _BLOCKED_WEBHOOK_HOSTS
        or host.endswith(".localhost")
        or host.endswith(".metadata.google.internal")
    ):
        return False, f"Blocked internal webhook target: {host}"
    if _webhook_ip_is_blocked(host):
        return False, f"Blocked internal webhook target: {host}"

    try:
        resolved = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        return True, ""
    for item in resolved:
        sockaddr = item[4]
        if sockaddr and _webhook_ip_is_blocked(str(sockaddr[0])):
            return False, f"Blocked internal webhook target: {host}"
    return True, ""


def execute_webhook(args: dict, pg: Any) -> dict:
    """Execute a webhook call.

    args:
        url: str — target URL
        method: str — HTTP method (default POST)
        headers: dict — HTTP headers
        body: str | dict — request body (dict → JSON, str → raw)
        timeout: int — seconds (default 30)
    """
    url = _resolve_url(args)
    method, body_raw = _resolve_method_and_body(args)
    headers = args.get("headers", {})
    timeout = args.get("timeout", 30)

    if not url:
        return {
            "status": "failed",
            "data": None,
            "summary": "No URL provided for webhook.",
            "error": "missing_url",
        }

    valid_url, invalid_summary = _validate_webhook_url(url)
    if not valid_url:
        return {
            "status": "failed",
            "data": None,
            "summary": invalid_summary,
            "error": (
                "invalid_url"
                if invalid_summary.startswith("Invalid URL:")
                else "ssrf_blocked"
            ),
        }

    if not isinstance(headers, dict):
        headers = {}
    else:
        headers = {str(key): str(value) for key, value in headers.items()}

    if not _method_supports_body(method):
        body_raw = None

    # Encode body
    if isinstance(body_raw, dict):
        data = json.dumps(body_raw).encode()
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
    elif body_raw:
        data = body_raw.encode() if isinstance(body_raw, str) else body_raw
    else:
        data = None

    resolved_headers, resolved_url = _resolve_auth_strategy(args, headers, url, pg=pg)
    if resolved_headers is None or resolved_url is None:
        return {
            "status": "failed",
            "data": None,
            "summary": "Failed to resolve connector authentication.",
            "error": "auth_resolution_failed",
        }
    headers = resolved_headers
    url = resolved_url

    valid_url, invalid_summary = _validate_webhook_url(url)
    if not valid_url:
        return {
            "status": "failed",
            "data": None,
            "summary": invalid_summary,
            "error": (
                "invalid_url"
                if invalid_summary.startswith("Invalid URL:")
                else "ssrf_blocked"
            ),
        }

    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")[:4000]
            status_code = resp.status

            # Try to parse response as JSON
            try:
                resp_data = json.loads(resp_body)
            except (json.JSONDecodeError, ValueError):
                resp_data = resp_body

            return {
                "status": "succeeded",
                "data": {
                    "http_status": status_code,
                    "response": resp_data,
                    "url": url,
                    "method": method,
                },
                "summary": f"{method} {url} → {status_code}",
                "error": None,
            }
    except urllib.error.HTTPError as e:
        resp_body = ""
        try:
            resp_body = e.read().decode("utf-8", errors="replace")[:2000]
        except Exception:
            pass
        return {
            "status": "failed",
            "data": {"http_status": e.code, "response": resp_body},
            "summary": f"{method} {url} → HTTP {e.code}",
            "error": f"http_{e.code}",
        }
    except urllib.error.URLError as e:
        return {
            "status": "failed",
            "data": None,
            "summary": f"{method} {url} → {e.reason}",
            "error": "connection_error",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "data": None,
            "summary": f"{method} {url} → {exc}",
            "error": "webhook_exception",
        }
