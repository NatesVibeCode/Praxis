"""Automatic provider/model routing by DB-backed route tier."""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .route_outcomes import RouteOutcomeStore

_log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class RouteCandidate:
    """One provider/model pair available for dispatch."""

    provider_slug: str
    model_slug: str
    tier: str
    priority: int  # lower = preferred


@dataclass(frozen=True, slots=True)
class RouteDecision:
    """The resolved routing decision."""

    provider_slug: str
    model_slug: str
    tier: str
    reason: str  # why this candidate was picked
    candidates_considered: int
    candidates_healthy: int
    decided_at: datetime


# ---------------------------------------------------------------------------
# Candidate registry — the source of truth for what's available
# ---------------------------------------------------------------------------

# No hardcoded candidates. The DB (provider_model_candidates table) is the
# single source of truth. If the DB is unreachable, routing fails closed
# rather than falling back to stale model IDs that may not exist anymore.
_CANDIDATES: tuple[RouteCandidate, ...] = ()

_TIER_CANDIDATES: dict[str, tuple[RouteCandidate, ...]] = {}
for _c in _CANDIDATES:
    _TIER_CANDIDATES.setdefault(_c.tier, ())
    _TIER_CANDIDATES[_c.tier] = _TIER_CANDIDATES[_c.tier] + (_c,)

_AUTO_TIER_FALLBACK_ORDER = ("mid", "frontier", "economy")

# Boundary mapping between the DB tier vocabulary and the runtime tier
# vocabulary. provider_model_candidates.route_tier is constrained to
# ('high', 'medium', 'low') by migration 046 and its historical seeds
# (091_openrouter_deepseek_onboarding, 093_deepseek_direct_provider, etc.).
# The runtime routing code and sync scripts speak ('frontier', 'mid',
# 'economy'). This map translates at the I/O edge so we don't silently
# drop every DB row (which was the behaviour before this entry existed).
# A full rename would require editing every historical migration and is
# rejected under the append-only migration policy.
_DB_ROUTE_TIER_TO_RUNTIME = {
    "high": "frontier",
    "medium": "mid",
    "low": "economy",
}

# ---------------------------------------------------------------------------
# DB-backed candidate loading
# ---------------------------------------------------------------------------

_db_cache_candidates: tuple[RouteCandidate, ...] | None = None
_db_cache_time: float = 0.0
_DB_CACHE_TTL: float = 5.0  # seconds


async def _load_candidates_async() -> tuple[RouteCandidate, ...]:
    """Connect to Postgres and load active provider_model_candidates."""
    from storage.postgres import connect_workflow_database

    conn = await connect_workflow_database()
    try:
        rows = await conn.fetch(
            """
            SELECT
                provider_slug,
                model_slug,
                status,
                priority,
                route_tier
            FROM provider_model_candidates
            WHERE status = 'active'
              AND route_tier IS NOT NULL
            ORDER BY priority, candidate_ref
            """
        )
    finally:
        await conn.close()

    candidates: list[RouteCandidate] = []
    for row in rows:
        provider_slug = row["provider_slug"]
        model_slug = row["model_slug"]
        priority = row["priority"]

        db_tier = str(row.get("route_tier") or "").strip().lower()
        tier = _DB_ROUTE_TIER_TO_RUNTIME.get(db_tier)
        if tier is None:
            # Unknown tier value — fail-skip rather than silently substituting
            # a fallback, so a new tier surfaces as a rejection in logs.
            _log.warning(
                "auto_router: dropping candidate %s/%s with unknown route_tier=%r",
                provider_slug,
                model_slug,
                db_tier,
            )
            continue

        candidates.append(RouteCandidate(
            provider_slug=provider_slug,
            model_slug=model_slug,
            tier=tier,
            priority=priority,
        ))

    return tuple(candidates)


def load_candidates_from_db() -> tuple[RouteCandidate, ...]:
    """Load active candidates from Postgres (synchronous wrapper)."""
    return run_sync_safe(_load_candidates_async())


def refresh_candidates(candidates: tuple[RouteCandidate, ...]) -> None:
    """Replace the module-level _CANDIDATES and rebuild _TIER_CANDIDATES."""
    global _CANDIDATES, _TIER_CANDIDATES
    _CANDIDATES = candidates
    new_tiers: dict[str, tuple[RouteCandidate, ...]] = {}
    for c in _CANDIDATES:
        new_tiers.setdefault(c.tier, ())
        new_tiers[c.tier] = new_tiers[c.tier] + (c,)
    _TIER_CANDIDATES = new_tiers


def _get_db_candidates() -> tuple[RouteCandidate, ...] | None:
    """Return DB candidates with a 5-second TTL cache. None on failure."""
    global _db_cache_candidates, _db_cache_time
    now = time.monotonic()
    if _db_cache_candidates is not None and (now - _db_cache_time) < _DB_CACHE_TTL:
        return _db_cache_candidates
    try:
        loaded = load_candidates_from_db()
        if loaded:
            _db_cache_candidates = loaded
            _db_cache_time = now
            return loaded
    except Exception as exc:
        _log.debug("auto_router: DB candidate load failed: %s", exc)
    return None


def resolve_route_from_db(
    tier: str,
    *,
    route_outcomes: RouteOutcomeStore | None = None,
    max_consecutive_failures: int = 3,
) -> RouteDecision:
    """Resolve a route using DB-backed route tiers."""
    db_candidates = _get_db_candidates()
    if db_candidates is not None:
        refresh_candidates(db_candidates)
    elif not _CANDIDATES:
        # DB unreachable and no cached candidates — fail closed
        raise RuntimeError(
            "auto_router: no candidates available. "
            "DB is unreachable and no DB-backed candidates were cached. "
            "Seed provider_model_candidates table or check WORKFLOW_DATABASE_URL."
        )
    return resolve_route(
        tier,
        route_outcomes=route_outcomes,
        max_consecutive_failures=max_consecutive_failures,
    )


def candidates_for_tier(tier: str) -> tuple[RouteCandidate, ...]:
    """Return candidates for a tier, sorted by priority."""
    return tuple(sorted(_TIER_CANDIDATES.get(tier, ()), key=lambda c: c.priority))


def all_tiers() -> tuple[str, ...]:
    """Return all known tier names."""
    return tuple(sorted(_TIER_CANDIDATES.keys()))


def resolve_route(
    tier: str,
    *,
    route_outcomes: RouteOutcomeStore | None = None,
    max_consecutive_failures: int = 3,
) -> RouteDecision:
    """Pick the best available candidate for a tier.

    If tier is "auto", tries mid → frontier → economy.
    Within a tier, picks the highest-priority healthy candidate.
    If route_outcomes is provided, skips unhealthy routes.
    """

    tiers_to_try = _AUTO_TIER_FALLBACK_ORDER if tier == "auto" else (tier,)
    all_considered = 0
    all_healthy = 0

    for try_tier in tiers_to_try:
        candidates = candidates_for_tier(try_tier)
        all_considered += len(candidates)

        healthy = []
        for c in candidates:
            if route_outcomes is not None:
                if not route_outcomes.is_route_healthy(
                    c.provider_slug,
                    model_slug=c.model_slug,
                    max_consecutive_failures=max_consecutive_failures,
                ):
                    continue
            healthy.append(c)

        all_healthy += len(healthy)

        if healthy:
            # Pick highest priority (lowest number). On tie, randomize.
            best_priority = healthy[0].priority
            top_candidates = [c for c in healthy if c.priority == best_priority]
            picked = random.choice(top_candidates) if len(top_candidates) > 1 else top_candidates[0]

            return RouteDecision(
                provider_slug=picked.provider_slug,
                model_slug=picked.model_slug,
                tier=try_tier,
                reason=f"selected from {try_tier} tier (priority={picked.priority}, {len(healthy)}/{len(candidates)} healthy)",
                candidates_considered=all_considered,
                candidates_healthy=all_healthy,
                decided_at=_utc_now(),
            )

    if all_considered:
        raise RuntimeError(
            f"no healthy candidates available for tier={tier!r} "
            f"(considered={all_considered}, max_consecutive_failures={max_consecutive_failures})"
        )

    raise RuntimeError(f"no candidates available for tier={tier!r}")
