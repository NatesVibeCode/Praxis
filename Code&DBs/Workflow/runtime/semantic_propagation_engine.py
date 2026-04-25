"""Causal propagation engine for the semantic_predicate_catalog.

When a domain authority records a write that emits an authority event,
this engine looks up causal predicates that fire on that event_type and
executes their declared side effects.  The point: side effects (cache
invalidation, downstream cascades) are no longer bespoke per call site —
they're declared once in the catalog and fire automatically wherever the
triggering event lands.

Today's first action handler is ``cache_invalidate``, which replaces
hand-coded ``aemit_cache_invalidation`` calls scattered across the manual
and auto promotion paths.  The same engine extends to other action kinds
(``emit_event``, ``mark_stale``, ``supersede``) as predicates land.

Usage from a domain authority::

    from runtime.semantic_propagation_engine import fire_causal_propagations

    await fire_causal_propagations(
        conn,
        event_type="dataset_promotion_recorded",
        event_payload={
            "promotion_id": pid,
            "specialist_target": specialist_target,
            "dataset_family": dataset_family,
            "split_tag": split_tag,
        },
        emitted_by="operator_write.arecord_dataset_promotion",
    )

The caller does *not* need to know which side effects fire — that is the
catalog's contract.  Adding a new effect = inserting a predicate row.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any


class SemanticPropagationError(RuntimeError):
    """Raised when a causal propagation cannot be fulfilled."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


# Resolve a small set of well-known cache-kind constants when a predicate
# stores their Python identifier (e.g. CACHE_KIND_DATASET_CURATED_PROJECTION)
# instead of the literal string.  Predicates can also store the literal
# value directly; the engine falls back to that path when no resolution
# match is found.
_KNOWN_CACHE_KIND_CONSTANTS: dict[str, str] = {
    "CACHE_KIND_DATASET_CURATED_PROJECTION": "dataset_curated_projection",
    "CACHE_KIND_DATASET_SCORING_POLICY": "dataset_scoring_policy",
    "CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE": "circuit_breaker_manual_override",
    "CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT": "route_authority_snapshot",
}


_CONSTANT_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]+$")


def _resolve_cache_kind(value: str) -> str:
    """Return the literal cache_kind string for a predicate spec.

    Accepts either a literal kind value (``dataset_curated_projection``) or
    a Python constant name that resolves to one (``CACHE_KIND_DATASET_*``).
    """

    text = str(value or "").strip()
    if not text:
        raise SemanticPropagationError(
            "semantic_propagation.invalid_cache_kind",
            "cache_kind / cache_kind_ref must be non-empty",
        )
    if _CONSTANT_NAME_PATTERN.match(text) and text in _KNOWN_CACHE_KIND_CONSTANTS:
        return _KNOWN_CACHE_KIND_CONSTANTS[text]
    return text


_TEMPLATE_TOKEN = re.compile(r"\{([^{}]+)\}")


def _render_template(template: str, payload: Mapping[str, Any]) -> str:
    """Render ``{field}`` and ``{field|fallback}`` tokens against payload.

    Missing fields with a literal fallback render as the fallback; missing
    fields with no fallback render as the empty string so cache keys remain
    deterministic even when optional fields are absent.
    """

    def _resolve(match: re.Match[str]) -> str:
        token = match.group(1)
        if "|" in token:
            field, fallback = token.split("|", 1)
        else:
            field, fallback = token, ""
        value = payload.get(field.strip())
        if value is None or value == "":
            return fallback
        return str(value)

    return _TEMPLATE_TOKEN.sub(_resolve, template)


# ---------------------------------------------------------------------------
# Action handlers.  Each takes (conn, action_spec, event_payload, emitted_by)
# and returns a structured result.  The engine dispatches on ``action.action``.
# ---------------------------------------------------------------------------


async def _action_cache_invalidate(
    conn: Any,
    *,
    action_spec: Mapping[str, Any],
    event_payload: Mapping[str, Any],
    emitted_by: str,
) -> dict[str, Any]:
    from .cache_invalidation import aemit_cache_invalidation

    cache_kind_value = action_spec.get("cache_kind") or action_spec.get("cache_kind_ref")
    if not cache_kind_value:
        raise SemanticPropagationError(
            "semantic_propagation.cache_invalidate_missing_kind",
            "cache_invalidate action must declare cache_kind or cache_kind_ref",
            details={"action_spec": dict(action_spec)},
        )
    cache_kind = _resolve_cache_kind(str(cache_kind_value))
    template = str(action_spec.get("cache_key_template") or "")
    cache_key = _render_template(template, event_payload) if template else ""
    reason = str(
        action_spec.get("reason_template")
        or f"semantic_propagation:{action_spec.get('action')}"
    )
    reason_text = _render_template(reason, event_payload)
    event_id = await aemit_cache_invalidation(
        conn,
        cache_kind=cache_kind,
        cache_key=cache_key,
        reason=reason_text,
        invalidated_by=emitted_by,
    )
    return {
        "action": "cache_invalidate",
        "cache_kind": cache_kind,
        "cache_key": cache_key,
        "event_id": event_id,
    }


_ACTION_HANDLERS: dict[str, Any] = {
    "cache_invalidate": _action_cache_invalidate,
}


# ---------------------------------------------------------------------------
# Predicate lookup
# ---------------------------------------------------------------------------


async def _load_causal_predicates_for_event(
    conn: Any,
    *,
    event_type: str,
) -> list[dict[str, Any]]:
    """Return enabled causal predicates whose propagation_policy.on_event
    matches the given event_type."""

    rows = await conn.fetch(
        """
        SELECT predicate_slug, predicate_kind, propagation_policy, decision_ref
          FROM semantic_predicate_catalog
         WHERE enabled = TRUE
           AND predicate_kind = 'causal'
           AND propagation_policy ->> 'on_event' = $1
         ORDER BY predicate_slug
        """,
        event_type,
    )
    predicates: list[dict[str, Any]] = []
    for row in rows or []:
        record = dict(row)
        policy = record.get("propagation_policy")
        if isinstance(policy, str):
            try:
                record["propagation_policy"] = json.loads(policy)
            except json.JSONDecodeError:
                record["propagation_policy"] = {}
        elif not isinstance(policy, dict):
            record["propagation_policy"] = {}
        predicates.append(record)
    return predicates


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def fire_causal_propagations(
    conn: Any,
    *,
    event_type: str,
    event_payload: Mapping[str, Any],
    emitted_by: str,
) -> dict[str, Any]:
    """Look up causal predicates for ``event_type`` and execute their actions.

    Returns ``{"fired": [...], "skipped": [...], "predicate_count": N}``.
    Unknown action kinds are skipped (recorded in ``skipped``) so the engine
    can roll out incrementally as new handlers land.
    """

    predicates = await _load_causal_predicates_for_event(conn, event_type=event_type)
    fired: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for predicate in predicates:
        policy = predicate.get("propagation_policy") or {}
        actions = policy.get("fires") or []
        if not isinstance(actions, list):
            continue
        for action_spec in actions:
            if not isinstance(action_spec, Mapping):
                continue
            kind = str(action_spec.get("action") or "").strip()
            handler = _ACTION_HANDLERS.get(kind)
            if handler is None:
                skipped.append(
                    {
                        "predicate_slug": predicate["predicate_slug"],
                        "action": kind or None,
                        "reason": "no_handler",
                    }
                )
                continue
            try:
                result = await handler(
                    conn,
                    action_spec=action_spec,
                    event_payload=event_payload,
                    emitted_by=emitted_by,
                )
            except SemanticPropagationError as exc:
                skipped.append(
                    {
                        "predicate_slug": predicate["predicate_slug"],
                        "action": kind,
                        "reason": exc.reason_code,
                        "details": exc.details,
                    }
                )
                continue
            result.setdefault("predicate_slug", predicate["predicate_slug"])
            fired.append(result)
    return {
        "fired": fired,
        "skipped": skipped,
        "predicate_count": len(predicates),
    }


__all__ = [
    "SemanticPropagationError",
    "fire_causal_propagations",
]
