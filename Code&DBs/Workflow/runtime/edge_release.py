"""Canonical build-graph edge release normalization.

This module centralizes release semantics for authoring/build graph edges so
the UI, build graph projection, planner, and runtime all derive control flow
from one explicit contract.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

_EDGE_TYPES = frozenset({"after_success", "after_failure", "after_any", "conditional"})
_ALWAYS_RELEASE_CONDITION = {"kind": "always"}


def _as_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return json.loads(json.dumps(value, default=str))
    return None


def _branch_label(reason: str | None) -> str | None:
    normalized = _as_text(reason)
    if not normalized:
        return None
    if normalized == "then":
        return "Then"
    if normalized == "else":
        return "Else"
    return " ".join(
        part[:1].upper() + part[1:]
        for part in normalized.replace("-", " ").replace("_", " ").split()
        if part
    )


def _invert_condition(condition: Mapping[str, Any]) -> dict[str, Any]:
    return {"op": "not", "conditions": [json.loads(json.dumps(dict(condition), default=str))]}


def _unwrap_else_condition(condition: Mapping[str, Any]) -> dict[str, Any]:
    op = _as_text(condition.get("op")).lower()
    conditions = condition.get("conditions")
    if op == "not" and isinstance(conditions, list) and len(conditions) == 1:
        candidate = _json_object(conditions[0])
        if candidate is not None:
            return candidate
    return json.loads(json.dumps(dict(condition), default=str))


def _family_from_edge_type(edge_type: str) -> str:
    if edge_type in {"conditional", "after_failure", "after_any"}:
        return edge_type
    return "after_success"


def _normalize_edge_type(value: Any, family: str) -> str:
    candidate = _as_text(value)
    if candidate in _EDGE_TYPES:
        return candidate
    if family in {"conditional", "after_failure", "after_any"}:
        return family
    return "after_success"


def normalize_edge_release(edge: Mapping[str, Any]) -> dict[str, Any]:
    raw_release = edge.get("release") if isinstance(edge.get("release"), Mapping) else {}
    gate = edge.get("gate") if isinstance(edge.get("gate"), Mapping) else {}
    family = (
        _as_text(raw_release.get("family"))
        or _as_text(gate.get("family"))
        or _as_text(edge.get("family"))
        or _family_from_edge_type(
            _normalize_edge_type(
                raw_release.get("edge_type") or edge.get("edge_type"),
                _as_text(edge.get("family")) or ("conditional" if _as_text(edge.get("kind")) == "conditional" else ""),
            )
        )
    )
    edge_type = _normalize_edge_type(
        raw_release.get("edge_type") or edge.get("edge_type"),
        family or ("conditional" if _as_text(edge.get("kind")) == "conditional" else ""),
    )
    branch_reason = _as_text(raw_release.get("branch_reason")) or _as_text(edge.get("branch_reason"))
    explicit_release_condition = _json_object(raw_release.get("release_condition")) or _json_object(edge.get("release_condition"))

    release_condition = explicit_release_condition
    if release_condition is None and edge_type == "conditional":
        base_condition = (
            _json_object((raw_release.get("config") or {}).get("condition"))
            or _json_object((gate.get("config") or {}).get("condition"))
            or _json_object((edge.get("config") or {}).get("condition"))
        )
        if base_condition is not None:
            release_condition = _invert_condition(base_condition) if branch_reason.lower() == "else" else base_condition

    config = _json_object(raw_release.get("config")) or _json_object(gate.get("config")) or _json_object(edge.get("config"))
    label = _as_text(raw_release.get("label")) or _as_text(gate.get("label")) or _as_text(edge.get("label")) or _branch_label(branch_reason) or ""
    state = _as_text(raw_release.get("state")) or _as_text(gate.get("state")) or _as_text(edge.get("state")) or ("configured" if family != "after_success" else "")

    release: dict[str, Any] = {
        "family": family or _family_from_edge_type(edge_type),
        "edge_type": edge_type,
        "release_condition": release_condition or dict(_ALWAYS_RELEASE_CONDITION),
    }
    if label:
        release["label"] = label
    if branch_reason:
        release["branch_reason"] = branch_reason
    if state:
        release["state"] = state
    if config:
        release["config"] = config
    return release


def base_condition_from_release(release: Mapping[str, Any]) -> dict[str, Any] | None:
    if _as_text(release.get("edge_type")) != "conditional":
        return _json_object((release.get("config") or {}).get("condition"))
    runtime_condition = _json_object(release.get("release_condition")) or _json_object((release.get("config") or {}).get("condition"))
    if runtime_condition is None:
        return None
    if _as_text(release.get("branch_reason")).lower() == "else":
        return _unwrap_else_condition(runtime_condition)
    return runtime_condition


def with_edge_release(edge: Mapping[str, Any], release: Mapping[str, Any] | None = None) -> dict[str, Any]:
    base_edge = dict(edge)
    if release is None:
        normalized = normalize_edge_release(
            {
                **base_edge,
                "release": {
                    "family": "after_success",
                    "edge_type": "after_success",
                    "release_condition": dict(_ALWAYS_RELEASE_CONDITION),
                },
            }
        )
    else:
        normalized = normalize_edge_release(
            {
                **base_edge,
                "release": dict(release),
            }
        )

    next_edge = dict(base_edge)
    next_edge["release"] = normalized
    next_edge.pop("release_condition", None)
    next_edge.pop("branch_reason", None)
    next_edge.pop("gate", None)

    existing_kind = _as_text(next_edge.get("kind"))
    if existing_kind not in {"authority_gate", "state_informs"}:
        if _as_text(normalized.get("edge_type")) == "conditional":
            next_edge["kind"] = "conditional"
        elif existing_kind and existing_kind != "conditional":
            next_edge["kind"] = existing_kind
        else:
            next_edge["kind"] = "sequence"
    return next_edge


def edge_gate_entry_from_edge(edge: Mapping[str, Any]) -> dict[str, Any] | None:
    release = normalize_edge_release(edge)
    family = _as_text(release.get("family"))
    from_id = _as_text(edge.get("from_node_id"))
    to_id = _as_text(edge.get("to_node_id"))
    if not family or family == "after_success" or not from_id or not to_id:
        return None
    entry: dict[str, Any] = {
        "edge_id": _as_text(edge.get("edge_id")) or f"edge-{from_id}-{to_id}",
        "from_node_id": from_id,
        "to_node_id": to_id,
        "release": release,
    }
    return entry


__all__ = [
    "base_condition_from_release",
    "edge_gate_entry_from_edge",
    "normalize_edge_release",
    "with_edge_release",
]
