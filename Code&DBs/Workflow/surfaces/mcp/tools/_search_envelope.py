"""Shared search envelope: request parsing + response shape.

Single source of truth for the praxis_search request/response contract
so every per-source plugin (code, knowledge, bugs, receipts, git, files,
db_read, ...) reads and writes the same shape. Keeps source plugins
ignorant of MCP wire details.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any


SHAPE_MATCH = "match"
SHAPE_CONTEXT = "context"
SHAPE_FULL = "full"
_VALID_SHAPES = (SHAPE_MATCH, SHAPE_CONTEXT, SHAPE_FULL)

MODE_AUTO = "auto"
MODE_SEMANTIC = "semantic"
MODE_EXACT = "exact"
MODE_REGEX = "regex"
_VALID_MODES = (MODE_AUTO, MODE_SEMANTIC, MODE_EXACT, MODE_REGEX)

SOURCE_CODE = "code"
SOURCE_KNOWLEDGE = "knowledge"
SOURCE_BUGS = "bugs"
SOURCE_RECEIPTS = "receipts"
SOURCE_AUTHORITY_RECEIPTS = "authority_receipts"
SOURCE_COMPLIANCE_RECEIPTS = "compliance_receipts"
SOURCE_DECISIONS = "decisions"
SOURCE_RESEARCH = "research"
SOURCE_GIT = "git_history"
SOURCE_FILES = "files"
SOURCE_DB = "db"
SOURCE_DATA_DICTIONARY = "data_dictionary"
SOURCE_LINEAGE = "lineage"

DEFAULT_SOURCES = (SOURCE_CODE,)
DEFAULT_LIMIT = 20
DEFAULT_CONTEXT_LINES = 5
MAX_LIMIT = 200
MAX_CONTEXT_LINES = 200


class SearchEnvelopeError(ValueError):
    """Raised when a search envelope cannot be parsed."""


@dataclass(frozen=True, slots=True)
class SearchScope:
    paths: tuple[str, ...] = ()
    exclude_paths: tuple[str, ...] = ()
    since_iso: str | None = None
    until_iso: str | None = None
    type_slug: str | None = None
    exclude_terms: tuple[str, ...] = ()
    entity_kind: str | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchEnvelope:
    query: str
    mode: str
    sources: tuple[str, ...]
    scope: SearchScope
    shape: str
    context_lines: int
    limit: int
    cursor: str | None
    explain: bool

    def with_resolved_mode(self, resolved: str) -> "SearchEnvelope":
        return SearchEnvelope(
            query=self.query,
            mode=resolved,
            sources=self.sources,
            scope=self.scope,
            shape=self.shape,
            context_lines=self.context_lines,
            limit=self.limit,
            cursor=self.cursor,
            explain=self.explain,
        )


def _coerce_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, Iterable):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raise SearchEnvelopeError(f"expected list of strings, got {type(value).__name__}")


def _coerce_optional_str(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    raise SearchEnvelopeError(f"{field_name} must be a string")


def _coerce_int(value: Any, *, default: int, field_name: str, lo: int, hi: int) -> int:
    if value is None:
        return default
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SearchEnvelopeError(f"{field_name} must be an integer") from exc
    if result < lo:
        return lo
    if result > hi:
        return hi
    return result


def _coerce_bool(value: Any, *, default: bool, field_name: str) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes", "y", "on"):
            return True
        if normalized in ("false", "0", "no", "n", "off", ""):
            return False
    raise SearchEnvelopeError(f"{field_name} must be boolean-like")


_KNOWN_SCOPE_FIELDS = frozenset(
    {
        "paths",
        "exclude_paths",
        "since_iso",
        "until_iso",
        "type_slug",
        "exclude_terms",
        "entity_kind",
        "extras",
    }
)


def _parse_scope(raw: Any) -> SearchScope:
    if raw is None:
        return SearchScope()
    if not isinstance(raw, Mapping):
        raise SearchEnvelopeError("scope must be an object")
    extras: dict[str, Any] = {}
    nested_extras = raw.get("extras")
    if isinstance(nested_extras, Mapping):
        extras.update(nested_extras)
    elif nested_extras is not None:
        raise SearchEnvelopeError("scope.extras must be an object")
    for key, value in raw.items():
        if key not in _KNOWN_SCOPE_FIELDS:
            extras[key] = value
    return SearchScope(
        paths=_coerce_str_tuple(raw.get("paths")),
        exclude_paths=_coerce_str_tuple(raw.get("exclude_paths")),
        since_iso=_coerce_optional_str(raw.get("since_iso"), field_name="scope.since_iso"),
        until_iso=_coerce_optional_str(raw.get("until_iso"), field_name="scope.until_iso"),
        type_slug=_coerce_optional_str(raw.get("type_slug"), field_name="scope.type_slug"),
        exclude_terms=_coerce_str_tuple(raw.get("exclude_terms")),
        entity_kind=_coerce_optional_str(raw.get("entity_kind"), field_name="scope.entity_kind"),
        extras=extras,
    )


def parse_envelope(params: Mapping[str, Any]) -> SearchEnvelope:
    """Parse and normalize a praxis_search request payload."""

    if not isinstance(params, Mapping):
        raise SearchEnvelopeError("params must be an object")

    query_raw = params.get("query", "")
    if not isinstance(query_raw, str):
        raise SearchEnvelopeError("query must be a string")
    query = query_raw.strip()
    if not query:
        raise SearchEnvelopeError("query is required")

    mode = str(params.get("mode") or MODE_AUTO).strip().lower() or MODE_AUTO
    if mode not in _VALID_MODES:
        raise SearchEnvelopeError(
            f"mode must be one of {_VALID_MODES}, got '{mode}'"
        )

    sources_raw = params.get("sources")
    if sources_raw is None:
        sources = DEFAULT_SOURCES
    else:
        sources = _coerce_str_tuple(sources_raw) or DEFAULT_SOURCES

    shape = str(params.get("shape") or SHAPE_CONTEXT).strip().lower() or SHAPE_CONTEXT
    if shape not in _VALID_SHAPES:
        raise SearchEnvelopeError(
            f"shape must be one of {_VALID_SHAPES}, got '{shape}'"
        )

    context_lines = _coerce_int(
        params.get("context_lines"),
        default=DEFAULT_CONTEXT_LINES,
        field_name="context_lines",
        lo=0,
        hi=MAX_CONTEXT_LINES,
    )

    limit = _coerce_int(
        params.get("limit"),
        default=DEFAULT_LIMIT,
        field_name="limit",
        lo=1,
        hi=MAX_LIMIT,
    )

    cursor = _coerce_optional_str(params.get("cursor"), field_name="cursor")
    explain = _coerce_bool(params.get("explain"), default=False, field_name="explain")

    return SearchEnvelope(
        query=query,
        mode=mode,
        sources=sources,
        scope=_parse_scope(params.get("scope")),
        shape=shape,
        context_lines=context_lines,
        limit=limit,
        cursor=cursor,
        explain=explain,
    )


def resolve_mode(envelope: SearchEnvelope) -> str:
    """Pick a concrete mode when envelope.mode == 'auto'."""

    if envelope.mode != MODE_AUTO:
        return envelope.mode
    query = envelope.query
    if len(query) >= 2 and query.startswith("/") and query.endswith("/"):
        return MODE_REGEX
    if len(query) >= 2 and query[0] == query[-1] and query[0] in ("'", '"'):
        return MODE_EXACT
    return MODE_SEMANTIC


def build_response(
    *,
    envelope: SearchEnvelope,
    results: Sequence[Mapping[str, Any]],
    sources_status: Mapping[str, str],
    freshness: Mapping[str, Mapping[str, Any]],
    next_cursor: str | None = None,
    suggested_refinements: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build the canonical praxis_search response envelope."""

    payload: dict[str, Any] = {
        "ok": True,
        "query": envelope.query,
        "results": [dict(row) for row in results],
        "count": len(results),
        "_meta": {
            "mode_resolved": resolve_mode(envelope),
            "shape": envelope.shape,
            "sources_queried": list(envelope.sources),
            "source_status": dict(sources_status),
            "index_freshness_per_source": {
                source: dict(snapshot) for source, snapshot in freshness.items()
            },
        },
    }
    if next_cursor:
        payload["_meta"]["next_cursor"] = next_cursor
    if suggested_refinements:
        payload["_meta"]["suggested_refinements"] = [
            dict(row) for row in suggested_refinements
        ]
    return payload


__all__ = [
    "DEFAULT_CONTEXT_LINES",
    "DEFAULT_LIMIT",
    "DEFAULT_SOURCES",
    "MAX_CONTEXT_LINES",
    "MAX_LIMIT",
    "MODE_AUTO",
    "MODE_EXACT",
    "MODE_REGEX",
    "MODE_SEMANTIC",
    "SHAPE_CONTEXT",
    "SHAPE_FULL",
    "SHAPE_MATCH",
    "SOURCE_AUTHORITY_RECEIPTS",
    "SOURCE_BUGS",
    "SOURCE_CODE",
    "SOURCE_COMPLIANCE_RECEIPTS",
    "SOURCE_DATA_DICTIONARY",
    "SOURCE_DB",
    "SOURCE_DECISIONS",
    "SOURCE_FILES",
    "SOURCE_GIT",
    "SOURCE_KNOWLEDGE",
    "SOURCE_LINEAGE",
    "SOURCE_RECEIPTS",
    "SOURCE_RESEARCH",
    "SearchEnvelope",
    "SearchEnvelopeError",
    "SearchScope",
    "build_response",
    "parse_envelope",
    "resolve_mode",
]
