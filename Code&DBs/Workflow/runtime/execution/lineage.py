"""Lineage helpers: execution ancestry tracking and payload enrichment."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from contracts.domain import WorkflowNodeContract


def _node_lineage(node: WorkflowNodeContract) -> dict[str, Any]:
    lineage = node.inputs.get("_operator_lineage")
    if isinstance(lineage, Mapping):
        return dict(lineage)
    return {}


def _lineage_value(lineage: Mapping[str, Any], key: str) -> Any:
    value = lineage.get(key)
    return value if value is not None else None


def _with_lineage(
    payload: Mapping[str, Any] | None,
    *,
    lineage: Mapping[str, Any],
) -> dict[str, Any]:
    enriched = dict(payload or {})
    for key in ("operator_frame_id", "logical_parent_node_id", "iteration_index"):
        value = _lineage_value(lineage, key)
        if value is not None:
            enriched[key] = value
    return enriched


__all__ = ["_lineage_value", "_node_lineage", "_with_lineage"]
