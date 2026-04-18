"""Cache-invalidation cursor advances.

Every time an authority write invalidates a process-local cache, we
also record an explicit event on ``CHANNEL_CACHE_INVALIDATION``. That
event is the audit trail for the invisible cache bump: why the cache
was advanced, which key, on whose behalf.

Callers should:

1. Record the authority change (decision, eligibility window, ...)
2. Emit the cache-invalidation event inside the same transaction via
   :func:`aemit_cache_invalidation`
3. After the transaction commits, call the in-process invalidator
   (e.g. :func:`invalidate_circuit_breaker_override_cache`)

This ordering binds the cache advance to the lifecycle change: if the
authority write rolls back, the event rolls back too, so subscribers
never see a phantom invalidation.
"""

from __future__ import annotations

from typing import Any

from .event_log import CHANNEL_CACHE_INVALIDATION, aemit


EVENT_CACHE_INVALIDATED = "cache_invalidated"

CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE = "circuit_breaker_manual_override"
CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT = "route_authority_snapshot"


async def aemit_cache_invalidation(
    conn: Any,
    *,
    cache_kind: str,
    cache_key: str,
    reason: str,
    invalidated_by: str,
    decision_ref: str | None = None,
) -> int:
    """Record a cache-advance event on ``CHANNEL_CACHE_INVALIDATION``.

    Returns the event id. Inside an active transaction the event
    commits atomically with the surrounding authority write; outside
    one, it becomes its own autocommit row.
    """

    payload: dict[str, Any] = {
        "cache_kind": cache_kind,
        "cache_key": cache_key,
        "reason": reason,
    }
    if decision_ref is not None:
        payload["decision_ref"] = decision_ref
    return await aemit(
        conn,
        channel=CHANNEL_CACHE_INVALIDATION,
        event_type=EVENT_CACHE_INVALIDATED,
        entity_id=cache_key,
        entity_kind=cache_kind,
        payload=payload,
        emitted_by=invalidated_by,
    )


__all__ = [
    "CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE",
    "CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT",
    "EVENT_CACHE_INVALIDATED",
    "aemit_cache_invalidation",
]
