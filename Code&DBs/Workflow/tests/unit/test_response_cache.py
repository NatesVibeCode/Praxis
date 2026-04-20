from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from runtime.integrations.response_cache import (
    ResponseCache,
    cached_or_fetch,
    get_global_response_cache,
    set_global_response_cache,
)


# ---------------------------------------------------------------------------
# ResponseCache
# ---------------------------------------------------------------------------


def test_cache_rejects_non_positive_max_entries_without_db_bootstrap() -> None:
    with pytest.raises(ValueError):
        ResponseCache(max_entries=0)
    with pytest.raises(ValueError):
        ResponseCache(max_entries=-1)


def test_cache_returns_stored_value_before_ttl_expires_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=8)
    cache.set("k", b"payload", ttl_seconds=5.0)
    assert cache.get("k") == b"payload"
    assert cache.size() == 1


def test_cache_drops_value_after_ttl_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=8)
    cache.set("k", b"payload", ttl_seconds=0.01)
    time.sleep(0.02)
    assert cache.get("k") is None
    # get() must clean up the expired entry so the cache stays bounded.
    assert cache.size() == 0


def test_cache_ignores_non_positive_ttl_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=8)
    cache.set("k", b"payload", ttl_seconds=0.0)
    cache.set("k2", b"payload2", ttl_seconds=-1.0)
    assert cache.get("k") is None
    assert cache.get("k2") is None


def test_cache_evicts_oldest_when_over_capacity_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=2)
    cache.set("a", b"A", ttl_seconds=10.0)
    cache.set("b", b"B", ttl_seconds=10.0)
    cache.set("c", b"C", ttl_seconds=10.0)
    # "a" should have been evicted as the oldest entry.
    assert cache.get("a") is None
    assert cache.get("b") == b"B"
    assert cache.get("c") == b"C"


def test_cache_re_set_moves_entry_to_tail_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=2)
    cache.set("a", b"A", ttl_seconds=10.0)
    cache.set("b", b"B", ttl_seconds=10.0)
    # Refreshing "a" must make it newest; then inserting "c" should evict "b".
    cache.set("a", b"A2", ttl_seconds=10.0)
    cache.set("c", b"C", ttl_seconds=10.0)
    assert cache.get("a") == b"A2"
    assert cache.get("b") is None
    assert cache.get("c") == b"C"


def test_cache_invalidate_returns_presence_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)
    cache.set("k", b"x", ttl_seconds=5.0)
    assert cache.invalidate("k") is True
    assert cache.invalidate("k") is False
    assert cache.get("k") is None


def test_cache_clear_empties_all_entries_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)
    cache.set("a", b"A", ttl_seconds=5.0)
    cache.set("b", b"B", ttl_seconds=5.0)
    cache.clear()
    assert cache.size() == 0


# ---------------------------------------------------------------------------
# compute_key
# ---------------------------------------------------------------------------


def test_compute_key_is_stable_for_equivalent_requests_without_db_bootstrap() -> None:
    k1 = ResponseCache.compute_key("GET", "https://api.example.com/x", b"")
    k2 = ResponseCache.compute_key(" get ", "https://api.example.com/x", b"")
    assert k1 == k2  # method is normalized (case, whitespace)


def test_compute_key_differs_for_different_urls_without_db_bootstrap() -> None:
    k_a = ResponseCache.compute_key("GET", "https://api.example.com/a", b"")
    k_b = ResponseCache.compute_key("GET", "https://api.example.com/b", b"")
    assert k_a != k_b


def test_compute_key_differs_for_different_bodies_without_db_bootstrap() -> None:
    k_a = ResponseCache.compute_key("POST", "https://example/x", b"payload-1")
    k_b = ResponseCache.compute_key("POST", "https://example/x", b"payload-2")
    assert k_a != k_b


def test_compute_key_respects_cache_key_extra_without_db_bootstrap() -> None:
    base = ResponseCache.compute_key("GET", "https://example/x", b"")
    with_scope = ResponseCache.compute_key(
        "GET", "https://example/x", b"", cache_key_extra="account=42"
    )
    assert base != with_scope


# ---------------------------------------------------------------------------
# get_or_fetch
# ---------------------------------------------------------------------------


def test_get_or_fetch_populates_cache_on_miss_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)
    calls = {"count": 0}

    def _fetch() -> bytes:
        calls["count"] += 1
        return b"hello"

    first = cache.get_or_fetch(key="k", fetch=_fetch, ttl_seconds=1.0)
    second = cache.get_or_fetch(key="k", fetch=_fetch, ttl_seconds=1.0)
    assert first == b"hello"
    assert second == b"hello"
    # Second lookup must be a cache hit — fetch() ran exactly once.
    assert calls["count"] == 1


def test_get_or_fetch_rejects_non_bytes_value_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)

    def _fetch_str():
        return "not bytes"  # type: ignore[return-value]

    with pytest.raises(TypeError):
        cache.get_or_fetch(key="k", fetch=_fetch_str, ttl_seconds=1.0)


def test_get_or_fetch_accepts_bytearray_and_memoryview_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)

    def _fetch_bytearray() -> bytes:
        return bytearray(b"ba")  # type: ignore[return-value]

    def _fetch_memoryview() -> bytes:
        return memoryview(b"mv")  # type: ignore[return-value]

    assert cache.get_or_fetch(key="ba", fetch=_fetch_bytearray, ttl_seconds=1.0) == b"ba"
    assert cache.get_or_fetch(key="mv", fetch=_fetch_memoryview, ttl_seconds=1.0) == b"mv"


def test_get_or_fetch_is_safe_under_thread_contention_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=8)
    counter = {"value": 0}

    def _fetch() -> bytes:
        counter["value"] += 1
        # Small sleep to widen the race window.
        time.sleep(0.005)
        return b"v"

    results: list[bytes] = []
    lock = threading.Lock()

    def _worker() -> None:
        val = cache.get_or_fetch(key="k", fetch=_fetch, ttl_seconds=1.0)
        with lock:
            results.append(val)

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every caller returns the correct value; the cache is stable.
    assert results == [b"v"] * 8
    # After the race settles, subsequent lookups must be cache hits.
    before = counter["value"]
    cache.get_or_fetch(key="k", fetch=_fetch, ttl_seconds=1.0)
    assert counter["value"] == before


# ---------------------------------------------------------------------------
# Global singleton + cached_or_fetch
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_global_cache():
    set_global_response_cache(None)
    yield
    set_global_response_cache(None)


def test_cached_or_fetch_bypasses_cache_when_no_global_cache_without_db_bootstrap() -> None:
    assert get_global_response_cache() is None
    calls = {"count": 0}

    def _fetch() -> bytes:
        calls["count"] += 1
        return b"raw"

    a = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=5.0,
    )
    b = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=5.0,
    )
    assert a == b == b"raw"
    # With no global cache, every call hits fetch().
    assert calls["count"] == 2


def test_cached_or_fetch_uses_global_cache_when_installed_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)
    set_global_response_cache(cache)
    calls = {"count": 0}

    def _fetch() -> bytes:
        calls["count"] += 1
        return b"data"

    first = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=5.0,
    )
    second = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=5.0,
    )
    assert first == second == b"data"
    assert calls["count"] == 1


def test_cached_or_fetch_splits_keys_by_cache_key_extra_without_db_bootstrap() -> None:
    cache = ResponseCache(max_entries=4)
    set_global_response_cache(cache)
    calls: list[str] = []

    def _fetch_a() -> bytes:
        calls.append("a")
        return b"A"

    def _fetch_b() -> bytes:
        calls.append("b")
        return b"B"

    a = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch_a, ttl_seconds=5.0, cache_key_extra="account=1",
    )
    b = cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch_b, ttl_seconds=5.0, cache_key_extra="account=2",
    )
    assert a == b"A"
    assert b == b"B"
    assert calls == ["a", "b"]


def test_cached_or_fetch_zero_ttl_disables_cache_without_db_bootstrap() -> None:
    set_global_response_cache(ResponseCache(max_entries=4))
    calls = {"count": 0}

    def _fetch() -> bytes:
        calls["count"] += 1
        return b"x"

    cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=0.0,
    )
    cached_or_fetch(
        method="GET", url="https://example/x", body=None,
        fetch=_fetch, ttl_seconds=0.0,
    )
    # TTL <= 0 means "skip cache entirely" — every call hits fetch.
    assert calls["count"] == 2
