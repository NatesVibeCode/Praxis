"""Global provider concurrency load balancer.

Solves the N-orchestrators problem: when 20 sessions each independently check
capacity and fire a request, per-session circuit breakers are blind to the
aggregate load. This module uses Postgres advisory locks + a shared
``provider_concurrency`` table to enforce global slot limits.

Provider limits (defaults, overridable via the DB table):
  - anthropic: max_concurrent=4
  - openai:    max_concurrent=4
  - google:    max_concurrent=8

Usage
-----
Acquire / release explicitly::

    balancer = get_load_balancer()
    acquired = balancer.acquire_slot("openai", cost_weight=1.0, timeout_s=30.0)
    if not acquired:
        raise RuntimeError("provider at capacity")
    try:
        ... call the adapter ...
    finally:
        balancer.release_slot("openai", cost_weight=1.0)

Or use the context manager::

    with balancer.slot("openai") as acquired:
        if not acquired:
            return failed_result(...)
        ... call the adapter ...

Stale-slot reaper
-----------------
Slots held by crashed sessions will never be released. Before checking
capacity, ``acquire_slot`` reaps any row whose ``updated_at`` is older than
``_STALE_SLOT_THRESHOLD_S`` (default 600 s / 10 min) by resetting
``active_slots`` to 0.

Degraded mode
-------------
If Postgres is unavailable, all methods degrade gracefully: ``acquire_slot``
returns True (no limit enforced), ``release_slot`` is a no-op, and
``has_capacity`` returns True. The caller in dispatch.py should treat the
balancer as optional.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, Protocol

from storage.postgres import (
    DEFAULT_PROVIDER_COST_WEIGHT,
    PostgresProviderConcurrencyRepository,
)
from ._workflow_database import resolve_runtime_database_url

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STALE_SLOT_THRESHOLD_S: float = float(
    os.environ.get("PRAXIS_LB_STALE_SLOT_S", "600")  # 10 minutes
)

_POLL_INTERVAL_S: float = 0.25  # how often to retry when waiting for a slot


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProviderConcurrencyLimit:
    """Snapshot of one provider's global concurrency state."""

    provider_slug: str
    max_concurrent: int
    current_active: float  # REAL in DB — fractional cost_weight sums
    cost_weight: float     # default cost_weight for this provider

    @property
    def available(self) -> float:
        return max(0.0, self.max_concurrent - self.current_active)

    def to_dict(self) -> dict:
        return {
            "provider_slug": self.provider_slug,
            "max_concurrent": self.max_concurrent,
            "current_active": self.current_active,
            "cost_weight_default": self.cost_weight,
            "available": self.available,
        }


def _get_database_url() -> str | None:
    return resolve_runtime_database_url(required=False)


class ProviderConcurrencyRepository(Protocol):
    """Storage contract for provider concurrency bootstrap and slot state."""

    async def ensure_schema(self, conn: Any) -> None: ...
    async def ensure_provider(self, conn: Any, *, provider_slug: str) -> None: ...
    async def ensure_default_providers(self, conn: Any) -> None: ...
    async def reap_stale_slots(
        self,
        conn: Any,
        *,
        provider_slug: str,
        stale_after_s: float,
    ) -> None: ...
    async def try_acquire_slot(
        self,
        conn: Any,
        *,
        provider_slug: str,
        cost_weight: float,
    ) -> bool: ...
    async def release_slot(
        self,
        conn: Any,
        *,
        provider_slug: str,
        cost_weight: float,
    ) -> None: ...
    async def fetch_slot_status(self, conn: Any) -> dict[str, dict[str, float | int | str]]: ...
    async def has_capacity(self, conn: Any, *, provider_slug: str) -> bool: ...


# ---------------------------------------------------------------------------
# GlobalLoadBalancer
# ---------------------------------------------------------------------------

class GlobalLoadBalancer:
    """Cross-session provider concurrency control backed by Postgres.

    All public methods are synchronous wrappers that spin up a fresh
    ``asyncio.run()`` call internally, which keeps them safe to call from
    any thread without managing a shared event loop.

    If the Postgres URL is not configured, or the DB is unreachable, every
    method degrades gracefully (slots are not enforced).
    """

    def __init__(
        self,
        database_url: str | None = None,
        *,
        repository: ProviderConcurrencyRepository | None = None,
    ) -> None:
        self._database_url = database_url or _get_database_url()
        self._available = self._database_url is not None
        self._repository = repository or PostgresProviderConcurrencyRepository()
        if not self._available:
            _log.debug("load_balancer: no database URL — concurrency control disabled")

    # -- internal helpers --------------------------------------------------

    def _run(self, coro):
        """Run an async coroutine synchronously. Returns None on any error."""
        try:
            return asyncio.run(coro)
        except Exception as exc:
            _log.debug("load_balancer: async error: %s", exc)
            return None

    async def _connect(self):
        import asyncpg
        return await asyncpg.connect(self._database_url)

    async def _setup_and_acquire(
        self,
        provider_slug: str,
        cost_weight: float,
        timeout_s: float,
    ) -> bool:
        conn = await self._connect()
        try:
            await self._repository.ensure_schema(conn)
            await self._repository.ensure_provider(conn, provider_slug=provider_slug)
            await self._repository.reap_stale_slots(
                conn,
                provider_slug=provider_slug,
                stale_after_s=_STALE_SLOT_THRESHOLD_S,
            )

            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    acquired = await self._repository.try_acquire_slot(
                        conn,
                        provider_slug=provider_slug,
                        cost_weight=cost_weight,
                    )
                except Exception as exc:
                    # Lock contention (NOWAIT) or transient error — treat as "not acquired"
                    _log.debug("load_balancer: acquire contention for %s: %s", provider_slug, exc)
                    acquired = False

                if acquired:
                    _log.debug(
                        "load_balancer: slot acquired for %s (cost_weight=%.2f)",
                        provider_slug,
                        cost_weight,
                    )
                    return True

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _log.info(
                        "load_balancer: timeout waiting for slot on %s after %.1fs",
                        provider_slug,
                        timeout_s,
                    )
                    return False

                await asyncio.sleep(min(_POLL_INTERVAL_S, remaining))
        finally:
            await conn.close()

    async def _do_release(self, provider_slug: str, cost_weight: float) -> None:
        conn = await self._connect()
        try:
            await self._repository.ensure_schema(conn)
            await self._repository.release_slot(
                conn,
                provider_slug=provider_slug,
                cost_weight=cost_weight,
            )
            _log.debug(
                "load_balancer: slot released for %s (cost_weight=%.2f)",
                provider_slug,
                cost_weight,
            )
        finally:
            await conn.close()

    async def _do_slot_status(self) -> dict[str, ProviderConcurrencyLimit]:
        conn = await self._connect()
        try:
            await self._repository.ensure_schema(conn)
            await self._repository.ensure_default_providers(conn)
            rows = await self._repository.fetch_slot_status(conn)
            return {
                provider_slug: ProviderConcurrencyLimit(
                    provider_slug=provider_slug,
                    max_concurrent=int(payload["max_concurrent"]),
                    current_active=float(payload["active_slots"]),
                    cost_weight=float(payload["cost_weight_default"]),
                )
                for provider_slug, payload in rows.items()
            }
        finally:
            await conn.close()

    async def _do_has_capacity(self, provider_slug: str) -> bool:
        conn = await self._connect()
        try:
            await self._repository.ensure_schema(conn)
            await self._repository.ensure_provider(conn, provider_slug=provider_slug)
            return await self._repository.has_capacity(conn, provider_slug=provider_slug)
        finally:
            await conn.close()

    # -- public API --------------------------------------------------------

    def acquire_slot(
        self,
        provider_slug: str,
        *,
        cost_weight: float = DEFAULT_PROVIDER_COST_WEIGHT,
        timeout_s: float = 30.0,
    ) -> bool:
        """Atomically acquire a provider slot.

        Blocks (polls) until a slot is available or timeout_s elapses.
        Always runs the stale-slot reaper before checking capacity.

        Returns True if a slot was acquired, False if timed out.
        Degrades to True (no limit) if DB is unavailable.
        """
        if not self._available:
            return True

        result = self._run(
            self._setup_and_acquire(provider_slug, cost_weight, timeout_s)
        )
        if result is None:
            # DB error — degrade gracefully
            _log.warning(
                "load_balancer: DB error on acquire for %s, proceeding without limit",
                provider_slug,
            )
            return True
        return result

    def release_slot(
        self,
        provider_slug: str,
        *,
        cost_weight: float = DEFAULT_PROVIDER_COST_WEIGHT,
    ) -> None:
        """Decrement active_slots for the provider. Best-effort, never raises."""
        if not self._available:
            return

        try:
            self._run(self._do_release(provider_slug, cost_weight))
        except Exception as exc:
            _log.warning("load_balancer: release failed for %s: %s", provider_slug, exc)

    def slot_status(self) -> dict[str, ProviderConcurrencyLimit]:
        """Return current concurrency state for all providers.

        Returns an empty dict if DB is unavailable.
        """
        if not self._available:
            return {}

        result = self._run(self._do_slot_status())
        return result or {}

    def has_capacity(self, provider_slug: str) -> bool:
        """Quick non-acquiring capacity check.

        Returns True if the provider has available slots, or if the DB is
        unavailable (degrade to allow).
        """
        if not self._available:
            return True

        result = self._run(self._do_has_capacity(provider_slug))
        if result is None:
            return True  # degrade
        return result

    @contextmanager
    def slot(
        self,
        provider_slug: str,
        *,
        cost_weight: float = DEFAULT_PROVIDER_COST_WEIGHT,
        timeout_s: float = 30.0,
    ) -> Generator[bool, None, None]:
        """Context manager that acquires/releases a slot around a block.

        Yields True if the slot was acquired, False if timed out.
        The slot is always released in the finally block — even if the caller
        did not acquire it (release is a no-op when nothing was acquired).

        Usage::

            with balancer.slot("openai") as acquired:
                if not acquired:
                    return _capacity_failure(...)
                ... call the adapter ...
        """
        acquired = self.acquire_slot(
            provider_slug, cost_weight=cost_weight, timeout_s=timeout_s
        )
        try:
            yield acquired
        finally:
            if acquired:
                self.release_slot(provider_slug, cost_weight=cost_weight)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_LOAD_BALANCER: GlobalLoadBalancer | None = None


def get_load_balancer() -> GlobalLoadBalancer:
    """Return the module-level GlobalLoadBalancer singleton."""
    global _LOAD_BALANCER
    if _LOAD_BALANCER is None:
        _LOAD_BALANCER = GlobalLoadBalancer()
    return _LOAD_BALANCER
