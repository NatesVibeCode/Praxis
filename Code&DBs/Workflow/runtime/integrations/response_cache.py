"""Bounded TTL response cache for idempotent integration HTTP calls.

Integrations (webhook schema lookups, OAuth token introspection, idempotent
provider-metadata GETs) frequently hit the same remote URL with the same body
inside short time windows. Caching the response for a few seconds to minutes
collapses redundant network traffic without introducing stale data.

This module deliberately does NOT wrap the LLM chat-completion path. Chat
responses are non-idempotent by protocol and must not be cached. Use this for
auxiliary integration calls only.

Design:
    - Key is a SHA-256 hash of (method, url, body, sorted-header-subset).
      Headers like Authorization are excluded from the key so cache hits work
      across keychain rotations; callers can pass `cache_key_extra` to force a
      split (e.g. include scope or account_id).
    - TTL is per-entry; the cache evicts on read when entries expire.
    - Bounded in-memory size (default 1024 entries) with simple FIFO eviction
      (adequate for the low-cardinality integration workload; avoid the
      hot-path overhead of a real LRU).
    - Thread-safe via a single lock; the cache is intended for low-throughput
      (<1000 req/s) integration fan-out, not for request routing.

Usage:
    >>> cache = ResponseCache()
    >>> def fetch() -> bytes:
    ...     return httpx.get("https://api.example.com/schema").content
    >>> data = cache.get_or_fetch(
    ...     key=("GET", "https://api.example.com/schema", b""),
    ...     fetch=fetch,
    ...     ttl_seconds=60.0,
    ... )

A process-wide singleton is exposed via `get_global_response_cache()` /
`set_global_response_cache()` so callers can opt in without threading the
cache through every function signature.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    value: bytes
    expires_at: float


class ResponseCache:
    """Bounded TTL cache for idempotent response bodies.

    `get_or_fetch` is the canonical entry point: it returns a cached response
    if one exists and hasn't expired, otherwise calls `fetch()` and stores the
    result under the computed key. `fetch()` is invoked without the cache lock
    held, so multiple threads may race to populate the same key — the last
    writer wins, which is fine for idempotent responses.
    """

    def __init__(self, *, max_entries: int = 1024) -> None:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries!r}")
        self._max_entries = int(max_entries)
        self._entries: "OrderedDict[str, _CacheEntry]" = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def compute_key(
        method: str,
        url: str,
        body: bytes | None,
        *,
        cache_key_extra: str | None = None,
    ) -> str:
        """Derive a stable cache key from the request components."""
        digest = hashlib.sha256()
        digest.update(method.strip().upper().encode("utf-8"))
        digest.update(b"\x00")
        digest.update(url.strip().encode("utf-8"))
        digest.update(b"\x00")
        digest.update(body or b"")
        if cache_key_extra:
            digest.update(b"\x00")
            digest.update(cache_key_extra.encode("utf-8"))
        return digest.hexdigest()

    def size(self) -> int:
        """Return the number of currently cached entries (expired or not)."""
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def invalidate(self, key: str) -> bool:
        """Drop one entry. Returns True if the entry existed."""
        with self._lock:
            return self._entries.pop(key, None) is not None

    def get(self, key: str) -> Optional[bytes]:
        """Return the cached bytes if present and unexpired; else None."""
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                # Drop the expired entry eagerly.
                self._entries.pop(key, None)
                return None
            return entry.value

    def set(self, key: str, value: bytes, *, ttl_seconds: float) -> None:
        """Cache `value` under `key` for up to `ttl_seconds`."""
        if ttl_seconds <= 0:
            return
        expires_at = time.monotonic() + float(ttl_seconds)
        with self._lock:
            # Remove any existing entry so that the refreshed entry is inserted
            # at the tail of the OrderedDict (treated as "newest").
            self._entries.pop(key, None)
            self._entries[key] = _CacheEntry(value=value, expires_at=expires_at)
            # Evict oldest entries until we're within the bound.
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def get_or_fetch(
        self,
        *,
        key: str,
        fetch: Callable[[], bytes],
        ttl_seconds: float,
    ) -> bytes:
        """Return a cached response or populate one from `fetch()`.

        `fetch()` runs OUTSIDE the cache lock so slow network calls don't
        serialize every cache reader.
        """
        cached = self.get(key)
        if cached is not None:
            return cached
        value = fetch()
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"ResponseCache fetch() must return bytes, got {type(value).__name__}"
            )
        value_bytes = bytes(value)
        self.set(key, value_bytes, ttl_seconds=ttl_seconds)
        return value_bytes

    def keys(self) -> tuple[str, ...]:
        """Snapshot of current keys (debug/introspection only)."""
        with self._lock:
            return tuple(self._entries.keys())


# ---------------------------------------------------------------------------
# Process-wide singleton
# ---------------------------------------------------------------------------

_GLOBAL_CACHE_LOCK = threading.Lock()
_GLOBAL_CACHE: ResponseCache | None = None


def set_global_response_cache(cache: ResponseCache | None) -> None:
    """Install (or clear) the process-wide response cache."""
    global _GLOBAL_CACHE
    with _GLOBAL_CACHE_LOCK:
        _GLOBAL_CACHE = cache


def get_global_response_cache() -> ResponseCache | None:
    """Return the installed cache, or None if caching is disabled."""
    with _GLOBAL_CACHE_LOCK:
        return _GLOBAL_CACHE


def cached_or_fetch(
    *,
    method: str,
    url: str,
    body: bytes | None,
    fetch: Callable[[], bytes],
    ttl_seconds: float,
    cache_key_extra: str | None = None,
) -> bytes:
    """Convenience: cache through the installed global cache if any.

    Falls back to calling `fetch()` directly when no global cache is
    installed, so callers can wrap every idempotent call unconditionally.
    """
    cache = get_global_response_cache()
    if cache is None or ttl_seconds <= 0:
        return fetch()
    key = cache.compute_key(method, url, body, cache_key_extra=cache_key_extra)
    return cache.get_or_fetch(key=key, fetch=fetch, ttl_seconds=ttl_seconds)


__all__ = [
    "ResponseCache",
    "cached_or_fetch",
    "get_global_response_cache",
    "set_global_response_cache",
]
