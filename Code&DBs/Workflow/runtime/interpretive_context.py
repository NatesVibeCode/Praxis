"""Bounded interpretive authority attachments for agent-facing surfaces.

Interpretive context is read authority, not admission authority. It gives
LLM-facing surfaces nearby semantics from the data dictionary while keeping
human judgment on the canonical bug / operator-decision plane.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable, Mapping

from runtime.data_dictionary import DataDictionaryBoundaryError, describe_object


INTERPRETIVE_AUTHORITY_CONTRACT: dict[str, Any] = {
    "authority_mode": "interpretive",
    "primary_consumer": "llm",
    "enforcement": "non_blocking",
    "review_plane": "none_inline",
    "escalation_plane": "canonical_bug_or_operator_decision",
    "escalation": "automated_resolve_else_escalate_when_unresolvable",
    "dedupe_key_policy": "<authority_domain>::<subject_ref>::<issue_type>",
}


@dataclass(frozen=True)
class InterpretiveContextCandidate:
    """A possible dictionary object to attach to a surface item."""

    object_kind: str
    reason: str


CandidateFn = Callable[[Mapping[str, Any]], Iterable[InterpretiveContextCandidate]]


def discover_result_candidates(
    item: Mapping[str, Any],
) -> list[InterpretiveContextCandidate]:
    """Infer dictionary candidates for one `praxis_discover` result.

    The rule is intentionally conservative: code-search results only get
    context when they can be mapped to a cataloged MCP tool object. Other
    surfaces can supply their own candidate function without changing the
    attachment machinery.
    """
    candidates: list[InterpretiveContextCandidate] = []
    seen: set[str] = set()

    def _add(object_kind: str, reason: str) -> None:
        if object_kind in seen:
            return
        seen.add(object_kind)
        candidates.append(InterpretiveContextCandidate(object_kind, reason))

    name = str(item.get("name") or "").strip()
    if name.startswith("tool_praxis_"):
        _add(f"tool:{name.removeprefix('tool_')}", "discover_result.name")

    path = str(item.get("path") or "").strip()
    if path.startswith("surfaces/mcp/tools/") and path.endswith(".py"):
        stem = PurePosixPath(path).stem
        if stem and stem != "__init__":
            _add(f"tool:praxis_{stem}", "discover_result.path")

    return candidates


def tool_name_candidates(
    tool_name: str,
    *,
    reason: str,
) -> list[InterpretiveContextCandidate]:
    """Return the dictionary object candidate for one MCP tool name."""
    name = str(tool_name or "").strip()
    if not name:
        return []
    if name.startswith("tool:"):
        object_kind = name
    elif name.startswith("tool_praxis_"):
        object_kind = f"tool:{name.removeprefix('tool_')}"
    elif name.startswith("praxis_"):
        object_kind = f"tool:{name}"
    else:
        return []
    return [InterpretiveContextCandidate(object_kind=object_kind, reason=reason)]


def tool_catalog_item_candidates(
    item: Mapping[str, Any],
) -> list[InterpretiveContextCandidate]:
    """Infer dictionary candidates for a catalog/list payload item."""
    return tool_name_candidates(
        str(item.get("name") or ""),
        reason="tool_catalog.item",
    )


def build_tool_interpretive_context(
    conn: Any,
    *,
    tool_name: str,
    reason: str,
    max_fields_per_object: int = 6,
) -> dict[str, Any]:
    """Return compact dictionary context for one MCP tool, if cataloged."""
    return build_interpretive_context(
        conn,
        candidates=tool_name_candidates(tool_name, reason=reason),
        max_objects=1,
        max_fields_per_object=max_fields_per_object,
    )


def attach_interpretive_context_to_items(
    conn: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    candidate_fn: CandidateFn,
    max_context_items: int = 5,
    max_objects_per_item: int = 2,
    max_fields_per_object: int = 6,
) -> list[dict[str, Any]]:
    """Attach bounded context to surface items when dictionary entries exist.

    Lookup failures are intentionally non-fatal. Interpretive authority should
    make good results easier to use; it must not become a hidden control gate.
    """
    max_context_items = max(0, int(max_context_items))
    max_objects_per_item = max(0, int(max_objects_per_item))
    max_fields_per_object = max(0, int(max_fields_per_object))

    out: list[dict[str, Any]] = []
    attachments_remaining = max_context_items
    for raw_item in items:
        item = dict(raw_item)
        if attachments_remaining > 0 and max_objects_per_item > 0:
            context = build_interpretive_context(
                conn,
                candidates=candidate_fn(item),
                max_objects=max_objects_per_item,
                max_fields_per_object=max_fields_per_object,
            )
            if context:
                item["interpretive_context"] = context
                attachments_remaining -= 1
        out.append(item)
    return out


def build_interpretive_context(
    conn: Any,
    *,
    candidates: Iterable[InterpretiveContextCandidate],
    max_objects: int = 2,
    max_fields_per_object: int = 6,
) -> dict[str, Any]:
    """Return compact dictionary context for candidate object kinds."""
    max_objects = max(0, int(max_objects))
    max_fields_per_object = max(0, int(max_fields_per_object))
    if max_objects <= 0:
        return {}

    items: list[dict[str, Any]] = []
    candidate_count = 0
    seen: set[str] = set()
    for candidate in candidates:
        object_kind = str(candidate.object_kind or "").strip()
        if not object_kind or object_kind in seen:
            continue
        candidate_count += 1
        seen.add(object_kind)
        if len(items) >= max_objects:
            continue
        compact = _compact_dictionary_object(
            conn,
            object_kind=object_kind,
            reason=str(candidate.reason or ""),
            max_fields=max_fields_per_object,
        )
        if compact:
            items.append(compact)

    if not items:
        return {}
    return {
        **INTERPRETIVE_AUTHORITY_CONTRACT,
        "sources": ["data_dictionary_effective"],
        "items": items,
        "omitted_candidates": max(0, candidate_count - len(items)),
        "payload_limits": {
            "max_objects": max_objects,
            "max_fields_per_object": max_fields_per_object,
        },
    }


def _compact_dictionary_object(
    conn: Any,
    *,
    object_kind: str,
    reason: str,
    max_fields: int,
) -> dict[str, Any] | None:
    try:
        payload = describe_object(
            conn,
            object_kind=object_kind,
            include_layers=False,
        )
    except DataDictionaryBoundaryError:
        return None
    except Exception:
        return None

    obj = dict(payload.get("object") or {})
    fields = list(payload.get("fields") or [])
    compact_fields = [
        _compact_field(row)
        for row in fields[:max(0, max_fields)]
        if isinstance(row, Mapping)
    ]
    return {
        "object_kind": str(obj.get("object_kind") or object_kind),
        "category": str(obj.get("category") or ""),
        "label": str(obj.get("label") or ""),
        "summary": _clip(str(obj.get("summary") or ""), 240),
        "attached_because": reason,
        "entries_by_source": dict(payload.get("entries_by_source") or {}),
        "fields": compact_fields,
        "omitted_fields": max(0, len(fields) - len(compact_fields)),
    }


def _compact_field(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "field_path": str(row.get("field_path") or ""),
        "field_kind": str(row.get("field_kind") or ""),
    }
    description = _clip(str(row.get("description") or ""), 180)
    if description:
        out["description"] = description
    if "required" in row:
        out["required"] = bool(row.get("required"))
    effective_source = row.get("effective_source") or row.get("source")
    if effective_source:
        out["effective_source"] = str(effective_source)
    valid_values = row.get("valid_values")
    if isinstance(valid_values, list) and valid_values:
        out["valid_values"] = valid_values[:8]
        if len(valid_values) > 8:
            out["omitted_valid_values"] = len(valid_values) - 8
    return out


def _clip(value: str, limit: int) -> str:
    cleaned = " ".join(value.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)].rstrip() + "..."


__all__ = [
    "INTERPRETIVE_AUTHORITY_CONTRACT",
    "InterpretiveContextCandidate",
    "attach_interpretive_context_to_items",
    "build_interpretive_context",
    "build_tool_interpretive_context",
    "discover_result_candidates",
    "tool_catalog_item_candidates",
    "tool_name_candidates",
]
