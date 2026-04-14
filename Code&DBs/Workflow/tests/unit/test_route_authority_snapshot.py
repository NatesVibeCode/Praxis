from __future__ import annotations

from runtime.route_authority_snapshot import RouteAuthoritySnapshot, RouteAuthoritySnapshotStore


class _PoolLike:
    __slots__ = ()


class _Conn:
    def __init__(self, pool: object) -> None:
        self._pool = pool


class _ExplicitKeyConn:
    def __init__(self, cache_key: str, scope: object | None = None) -> None:
        self._authority_cache_key = cache_key
        self._authority_scope = scope if scope is not None else object()


def test_store_reuses_snapshot_for_non_weakrefable_authority_scope() -> None:
    store = RouteAuthoritySnapshotStore()
    pool = _PoolLike()
    conn_a = _Conn(pool)
    conn_b = _Conn(pool)
    calls: list[str] = []

    def _load_snapshot(_conn: object) -> RouteAuthoritySnapshot:
        calls.append("snapshot")
        return RouteAuthoritySnapshot(
            route_policy={"default": "ok"},
            failure_zones={},
            task_profiles={},
            benchmark_metrics={},
        )

    first = store.get_snapshot(conn_a, load_snapshot=_load_snapshot)
    second = store.get_snapshot(conn_b, load_snapshot=_load_snapshot)

    assert first is second
    assert calls == ["snapshot"]


def test_store_reuses_task_policy_for_non_weakrefable_authority_scope_and_invalidates() -> None:
    store = RouteAuthoritySnapshotStore()
    pool = _PoolLike()
    conn_a = _Conn(pool)
    conn_b = _Conn(pool)
    calls: list[str] = []

    def _load_policy(_conn: object, task_type: str) -> dict[str, str]:
        calls.append(task_type)
        return {"task_type": task_type}

    first = store.get_task_policy(conn_a, task_type="build", load_policy=_load_policy)
    second = store.get_task_policy(conn_b, task_type="build", load_policy=_load_policy)

    assert first == {"task_type": "build"}
    assert second == {"task_type": "build"}
    assert calls == ["build"]

    store.invalidate(conn_a)

    third = store.get_task_policy(conn_b, task_type="build", load_policy=_load_policy)

    assert third == {"task_type": "build"}
    assert calls == ["build", "build"]


def test_store_reuses_snapshot_for_explicit_authority_cache_key() -> None:
    store = RouteAuthoritySnapshotStore()
    conn_a = _ExplicitKeyConn("workflow_pool:test-a")
    conn_b = _ExplicitKeyConn("workflow_pool:test-a")
    calls: list[str] = []

    def _load_snapshot(_conn: object) -> RouteAuthoritySnapshot:
        calls.append("snapshot")
        return RouteAuthoritySnapshot(
            route_policy={"default": "ok"},
            failure_zones={},
            task_profiles={},
            benchmark_metrics={},
        )

    first = store.get_snapshot(conn_a, load_snapshot=_load_snapshot)
    second = store.get_snapshot(conn_b, load_snapshot=_load_snapshot)

    assert first is second
    assert calls == ["snapshot"]


def test_store_invalidates_explicit_authority_cache_key_across_wrappers() -> None:
    store = RouteAuthoritySnapshotStore()
    conn_a = _ExplicitKeyConn("workflow_pool:test-b")
    conn_b = _ExplicitKeyConn("workflow_pool:test-b")
    calls: list[str] = []

    def _load_policy(_conn: object, task_type: str) -> dict[str, str]:
        calls.append(task_type)
        return {"task_type": task_type}

    first = store.get_task_policy(conn_a, task_type="verify", load_policy=_load_policy)
    store.invalidate(conn_b)
    second = store.get_task_policy(conn_a, task_type="verify", load_policy=_load_policy)

    assert first == {"task_type": "verify"}
    assert second == {"task_type": "verify"}
    assert calls == ["verify", "verify"]
