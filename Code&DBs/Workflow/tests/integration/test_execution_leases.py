"""Integration tests for the execution lease manager.

These tests exercise the canonical Postgres-backed execution_leases table.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from unittest import mock

import pytest

import importlib
import sys

from storage.postgres import ensure_postgres_available

# Import the module directly to avoid runtime/__init__.py which requires
# Python 3.10+ (slots=True dataclasses in domain.py).
_mod_path = str(
    __import__("pathlib").Path(__file__).resolve().parents[2] / "runtime" / "execution_leases.py"
)
_spec = importlib.util.spec_from_file_location("execution_leases", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["execution_leases"] = _mod
_spec.loader.exec_module(_mod)

LeaseHandle = _mod.LeaseHandle
LeaseInfo = _mod.LeaseInfo
LeaseManager = _mod.LeaseManager
PostgresLeaseBackend = _mod.PostgresLeaseBackend


@pytest.fixture()
def backend() -> PostgresLeaseBackend:
    conn = ensure_postgres_available()
    conn.execute_script(
        """
        CREATE TABLE IF NOT EXISTS execution_leases (
            lease_id text PRIMARY KEY,
            holder_id text NOT NULL,
            resource_key text UNIQUE NOT NULL,
            acquired_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL,
            renewed_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS execution_leases_expires_at_idx
            ON execution_leases (expires_at);
        """
    )
    conn.execute_script("TRUNCATE execution_leases")
    backend = PostgresLeaseBackend(conn)
    try:
        yield backend
    finally:
        conn.execute_script("TRUNCATE execution_leases")
        backend.close()


@pytest.fixture()
def mgr(backend: PostgresLeaseBackend) -> LeaseManager:
    return LeaseManager(backend=backend)


# -- acquire / release lifecycle -------------------------------------------

class TestAcquireRelease:
    def test_acquire_returns_handle(self, mgr: LeaseManager) -> None:
        handle = mgr.acquire("res-1", "holder-a")
        assert handle is not None
        assert isinstance(handle, LeaseHandle)
        assert handle.resource_key == "res-1"
        assert handle.holder_id == "holder-a"

    def test_release_returns_true(self, mgr: LeaseManager) -> None:
        handle = mgr.acquire("res-1", "holder-a")
        assert handle is not None
        assert mgr.release(handle) is True

    def test_release_idempotent(self, mgr: LeaseManager) -> None:
        handle = mgr.acquire("res-1", "holder-a")
        assert handle is not None
        mgr.release(handle)
        assert mgr.release(handle) is False

    def test_is_held_after_acquire(self, mgr: LeaseManager) -> None:
        handle = mgr.acquire("res-1", "holder-a")
        assert handle is not None
        info = mgr.is_held("res-1")
        assert info is not None
        assert isinstance(info, LeaseInfo)
        assert info.holder_id == "holder-a"

    def test_is_held_after_release(self, mgr: LeaseManager) -> None:
        handle = mgr.acquire("res-1", "holder-a")
        assert handle is not None
        mgr.release(handle)
        assert mgr.is_held("res-1") is None


# -- concurrent acquire ----------------------------------------------------

class TestConcurrentAcquire:
    def test_second_holder_gets_none(self, mgr: LeaseManager) -> None:
        h1 = mgr.acquire("res-1", "holder-a")
        assert h1 is not None
        h2 = mgr.acquire("res-1", "holder-b")
        assert h2 is None

    def test_same_holder_gets_none(self, mgr: LeaseManager) -> None:
        h1 = mgr.acquire("res-1", "holder-a")
        assert h1 is not None
        h2 = mgr.acquire("res-1", "holder-a")
        assert h2 is None

    def test_different_resources_both_succeed(self, mgr: LeaseManager) -> None:
        h1 = mgr.acquire("res-1", "holder-a")
        h2 = mgr.acquire("res-2", "holder-b")
        assert h1 is not None
        assert h2 is not None


# -- TTL expiration --------------------------------------------------------

class TestTTLExpiration:
    def test_expired_lease_reaped_on_acquire(self, mgr: LeaseManager) -> None:
        # Acquire with a very short TTL
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        future_short = past + timedelta(seconds=1)
        future_long = past + timedelta(hours=1)

        # First acquire happens at `past`, expires at `past + 1s`
        with mock.patch.object(LeaseManager, "_now", return_value=past):
            h1 = mgr.acquire("res-1", "holder-a", ttl_seconds=1)
            assert h1 is not None

        # Second acquire at a time after expiry succeeds because reap runs
        with mock.patch.object(LeaseManager, "_now", return_value=future_long):
            h2 = mgr.acquire("res-1", "holder-b", ttl_seconds=300)
            assert h2 is not None
            assert h2.holder_id == "holder-b"

    def test_is_held_returns_none_after_expiry(self, mgr: LeaseManager) -> None:
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        future = past + timedelta(hours=1)

        with mock.patch.object(LeaseManager, "_now", return_value=past):
            mgr.acquire("res-1", "holder-a", ttl_seconds=1)

        with mock.patch.object(LeaseManager, "_now", return_value=future):
            assert mgr.is_held("res-1") is None


# -- renew -----------------------------------------------------------------

class TestRenew:
    def test_renew_extends_ttl(self, mgr: LeaseManager) -> None:
        t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
        t1 = t0 + timedelta(seconds=100)
        t2 = t0 + timedelta(seconds=400)

        with mock.patch.object(LeaseManager, "_now", return_value=t0):
            handle = mgr.acquire("res-1", "holder-a", ttl_seconds=200)
            assert handle is not None

        # Renew at t1 for another 500s => new expires = t1 + 500s
        with mock.patch.object(LeaseManager, "_now", return_value=t1):
            assert mgr.renew(handle, ttl_seconds=500) is True

        # At t2 (400s after t0), original TTL would have expired (200s),
        # but the renewal means it's still held
        with mock.patch.object(LeaseManager, "_now", return_value=t2):
            info = mgr.is_held("res-1")
            assert info is not None
            assert info.holder_id == "holder-a"

    def test_renew_expired_returns_false(self, mgr: LeaseManager) -> None:
        t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
        t_after = t0 + timedelta(hours=1)

        with mock.patch.object(LeaseManager, "_now", return_value=t0):
            handle = mgr.acquire("res-1", "holder-a", ttl_seconds=1)
            assert handle is not None

        with mock.patch.object(LeaseManager, "_now", return_value=t_after):
            assert mgr.renew(handle) is False


# -- context manager -------------------------------------------------------

class TestContextManager:
    def test_hold_acquires_and_releases(self, mgr: LeaseManager) -> None:
        with mgr.hold("res-1", "holder-a") as handle:
            assert handle is not None
            assert mgr.is_held("res-1") is not None
        # After exit, lease is released
        assert mgr.is_held("res-1") is None

    def test_hold_raises_when_held(self, mgr: LeaseManager) -> None:
        h = mgr.acquire("res-1", "holder-a")
        assert h is not None
        with pytest.raises(RuntimeError, match="resource is held"):
            with mgr.hold("res-1", "holder-b"):
                pass  # pragma: no cover

    def test_hold_releases_on_exception(self, mgr: LeaseManager) -> None:
        with pytest.raises(ValueError, match="boom"):
            with mgr.hold("res-1", "holder-a"):
                raise ValueError("boom")
        assert mgr.is_held("res-1") is None
