"""Content-addressed context compilation cache.

Eliminates redundant compilation when the same admitted authority inputs are
compiled repeatedly into identical bounded context packets.
"""
from __future__ import annotations

import hashlib
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger("context_cache")

_DEFAULT_MAX_ENTRIES = 128


@dataclass(frozen=True)
class CacheKey:
    """Content-addressed cache key for compiled context."""

    definition_hash: str
    workspace_path: str
    profile_name: str
    token_budget: int

    def sha256(self) -> str:
        """Compute the stable SHA256 identity for this key."""
        content = (
            f"{self.definition_hash}|{self.workspace_path}|"
            f"{self.profile_name}|{self.token_budget}"
        )
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


class ContextCompilationCache:
    """Thread-safe LRU cache for compiled bounded context packets."""

    def __init__(self, max_entries: int = _DEFAULT_MAX_ENTRIES):
        self._max_entries = max_entries
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: CacheKey) -> Any | None:
        """Look up a cached packet. Returns ``None`` on miss."""
        sha = key.sha256()
        with self._lock:
            packet = self._cache.get(sha)
            if packet is None:
                self._misses += 1
                return None
            self._cache.move_to_end(sha)
            self._hits += 1
            _log.debug("context cache HIT: %s (hits=%d)", sha[:12], self._hits)
            return packet

    def put(self, key: CacheKey, packet: Any) -> None:
        """Store a compiled packet and evict the least-recently-used entry."""
        sha = key.sha256()
        with self._lock:
            if sha in self._cache:
                self._cache.move_to_end(sha)
                self._cache[sha] = packet
                return
            self._cache[sha] = packet
            if len(self._cache) > self._max_entries:
                evicted_key, _ = self._cache.popitem(last=False)
                _log.debug(
                    "context cache eviction: %s (size=%d)",
                    evicted_key[:12],
                    len(self._cache),
                )

    def stats(self) -> dict[str, int | float]:
        """Return cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            return {
                "hits": self._hits,
                "misses": self._misses,
                "total": total,
                "hit_rate": round(self._hits / total, 4) if total > 0 else 0.0,
                "size": len(self._cache),
                "max_entries": self._max_entries,
            }

    def clear(self) -> None:
        """Clear the cache and reset counters."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0


_global_cache = ContextCompilationCache()


def get_context_cache() -> ContextCompilationCache:
    """Return the module-level cache singleton."""
    return _global_cache
