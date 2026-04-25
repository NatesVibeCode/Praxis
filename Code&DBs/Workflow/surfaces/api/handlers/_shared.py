"""Shared constants and helpers for workflow HTTP handlers."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from runtime.workspace_paths import repo_root as workspace_repo_root


REPO_ROOT = workspace_repo_root()
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
RECEIPTS_DIR = str(REPO_ROOT / "artifacts" / "workflow_receipts")
MAX_REQUEST_BODY_BYTES = 1024 * 1024

RouteMatcher = Callable[[str], bool]
RouteHandler = Callable[[Any, str], None]
RouteEntry = tuple[RouteMatcher, RouteHandler]


class _ClientError(Exception):
    """Raised for 400-level request errors."""


_DEMO_PLACEHOLDER_IDS_BY_FIELD: dict[str, frozenset[str]] = {
    "entity_id": frozenset({"entity_abc123"}),
    "sandbox_id": frozenset({"sandbox_abc123"}),
    "wave_id": frozenset({"wave_abc123"}),
}


def is_demo_placeholder(field_name: str, value: object) -> bool:
    """Return True when *value* is a known non-live example ID."""
    raw = str(value or "").strip()
    return raw in _DEMO_PLACEHOLDER_IDS_BY_FIELD.get(field_name, frozenset())


def placeholder_error_message(field_name: str, value: object) -> str:
    raw = str(value or "").strip()
    return (
        f"{field_name} '{raw}' is an example placeholder and cannot be used "
        "as a live resource selector"
    )


def _serialize(obj: Any) -> Any:
    """Convert dataclass / datetime / enum / tuple to JSON-safe form."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if is_dataclass(obj):
        return {k: _serialize(v) for k, v in asdict(obj).items()}
    if hasattr(obj, "value"):
        return obj.value
    return str(obj)


def _bug_field(bug: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(bug, dict) and name in bug:
            value = bug.get(name)
        else:
            value = getattr(bug, name, None)
        if value is not None:
            return value
    return default


def _enum_value(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    return getattr(value, "value", value)


def _bug_to_dict(bug: Any) -> dict[str, Any]:
    """Convert a Bug dataclass to a plain dict. Omits null/empty fields."""
    severity = _enum_value(_bug_field(bug, "severity"))
    status = _enum_value(_bug_field(bug, "status"))
    category = _enum_value(_bug_field(bug, "category"))
    filed_at = _bug_field(bug, "filed_at", "opened_at", "created_at")
    bug_id = str(_bug_field(bug, "bug_id", default=""))

    out: dict[str, Any] = {
        "bug_id": bug_id,
        "title": str(_bug_field(bug, "title", default="")),
        "status": status,
        "severity": severity,
        "category": category,
    }

    if filed_at:
        out["filed_at"] = filed_at.isoformat() if hasattr(filed_at, "isoformat") else filed_at
    updated_at = _bug_field(bug, "updated_at")
    if updated_at:
        out["updated_at"] = updated_at.isoformat() if hasattr(updated_at, "isoformat") else updated_at
    resolved_at = _bug_field(bug, "resolved_at")
    if resolved_at:
        out["resolved_at"] = resolved_at.isoformat() if hasattr(resolved_at, "isoformat") else resolved_at

    description = str(_bug_field(bug, "description", "summary", default="") or "").strip()
    if description:
        out["description"] = description

    tags = list(_bug_field(bug, "tags", default=()) or ())
    if tags:
        out["tags"] = tags

    resume_ctx = _bug_field(bug, "resume_context", default=None)
    if isinstance(resume_ctx, dict) and resume_ctx:
        out["resume_context"] = resume_ctx

    for field in ("filed_by", "assigned_to", "owner_ref", "source_issue_id", "decision_ref",
                  "resolution_summary", "discovered_in_run_id", "discovered_in_receipt_id"):
        v = _bug_field(bug, field, default=None)
        if v is not None and str(v).strip():
            out[field] = str(v).strip()

    source_kind = str(_bug_field(bug, "source_kind", default="") or "").strip()
    if source_kind and source_kind != "manual":
        out["source_kind"] = source_kind

    return out


def _matches(text: str, keywords: list[str]) -> bool:
    return any(kw in text for kw in keywords)


def _route_path(candidate: str) -> str:
    """Return just the path portion of a route candidate.

    The dispatcher now hands matchers the URL-encoded path plus any
    ``?query`` suffix (so handlers can read query params and preserve
    percent-encoded path segments). Route matchers only care about the
    path, so they must strip the query before comparing.
    """
    return candidate.split("?", 1)[0] if "?" in candidate else candidate


def _exact(path: str) -> RouteMatcher:
    return lambda candidate, expected=path: _route_path(candidate) == expected


def _prefix(path_prefix: str) -> RouteMatcher:
    return (
        lambda candidate, prefix=path_prefix:
        _route_path(candidate).startswith(prefix)
    )


def _prefix_suffix(path_prefix: str, path_suffix: str) -> RouteMatcher:
    return (
        lambda candidate, prefix=path_prefix, suffix=path_suffix:
        _route_path(candidate).startswith(prefix)
        and _route_path(candidate).endswith(suffix)
    )


def _read_json_body(request: Any) -> Any:
    try:
        content_length = int(request.headers.get("Content-Length", 0))
    except (TypeError, ValueError):
        raise ValueError("Content-Length header must be a non-negative integer")
    if content_length < 0:
        raise ValueError("Content-Length header must be a non-negative integer")
    if content_length > MAX_REQUEST_BODY_BYTES:
        raise ValueError(
            "Request body exceeds maximum size of 1,048,576 bytes"
        )
    raw = request.rfile.read(content_length) if content_length else b""
    return json.loads(raw) if raw else {}


def _query_params(raw_path: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(raw_path).query)
