"""Shared static authority snapshots for task-type routing.

Lifetime rules:
- Snapshots are process-local and reused across ``TaskTypeRouter`` instances
  that share the same authority source.
- Static authority does not auto-refresh. Callers must explicitly invalidate
  the cache after mutating routing authority tables.
- Runtime state remains live and is not cached here.
"""
from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RouteAuthoritySnapshot:
    """Static authority reused across router instances for one authority source."""

    route_policy: Any
    failure_zones: dict[str, str]
    task_profiles: dict[str, Any]
    benchmark_metrics: dict[str, Any]


class RouteAuthoritySnapshotStore:
    """Process-local cache for static routing authority.

    The cache is keyed by the concrete authority source object. For
    ``SyncPostgresConnection`` this is the underlying pool so short-lived
    wrapper instances reuse the same snapshot.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshots: weakref.WeakKeyDictionary[object, RouteAuthoritySnapshot] = weakref.WeakKeyDictionary()
        self._task_policies: weakref.WeakKeyDictionary[object, dict[str, Any]] = weakref.WeakKeyDictionary()

    @staticmethod
    def authority_scope(conn: object) -> object:
        return getattr(conn, "_pool", conn)

    def get_snapshot(
        self,
        conn: object,
        *,
        load_snapshot: Callable[[object], RouteAuthoritySnapshot],
    ) -> RouteAuthoritySnapshot:
        scope = self.authority_scope(conn)
        with self._lock:
            cached = self._snapshots.get(scope)
            if cached is not None:
                return cached
        snapshot = load_snapshot(conn)
        with self._lock:
            cached = self._snapshots.get(scope)
            if cached is not None:
                return cached
            self._snapshots[scope] = snapshot
            self._task_policies.setdefault(scope, {})
            return snapshot

    def get_task_policy(
        self,
        conn: object,
        *,
        task_type: str,
        load_policy: Callable[[object, str], Any],
    ) -> Any:
        scope = self.authority_scope(conn)
        with self._lock:
            cache = self._task_policies.setdefault(scope, {})
            cached = cache.get(task_type)
            if cached is not None:
                return cached
        policy = load_policy(conn, task_type)
        with self._lock:
            cache = self._task_policies.setdefault(scope, {})
            cached = cache.get(task_type)
            if cached is not None:
                return cached
            cache[task_type] = policy
            return policy

    def invalidate(self, conn: object) -> None:
        """Drop cached static authority for one authority source."""
        scope = self.authority_scope(conn)
        with self._lock:
            self._snapshots.pop(scope, None)
            self._task_policies.pop(scope, None)

    def invalidate_all(self) -> None:
        """Drop all cached static authority in this process."""
        with self._lock:
            self._snapshots = weakref.WeakKeyDictionary()
            self._task_policies = weakref.WeakKeyDictionary()


_store = RouteAuthoritySnapshotStore()


def get_route_authority_snapshot(
    conn: object,
    *,
    load_snapshot: Callable[[object], RouteAuthoritySnapshot],
) -> RouteAuthoritySnapshot:
    return _store.get_snapshot(conn, load_snapshot=load_snapshot)


def get_task_route_policy(
    conn: object,
    *,
    task_type: str,
    load_policy: Callable[[object, str], Any],
) -> Any:
    return _store.get_task_policy(conn, task_type=task_type, load_policy=load_policy)


def invalidate_route_authority_snapshot(conn: object) -> None:
    _store.invalidate(conn)


def invalidate_all_route_authority_snapshots() -> None:
    _store.invalidate_all()
