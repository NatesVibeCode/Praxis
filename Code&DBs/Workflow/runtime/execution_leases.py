"""Execution lease manager for distributed resource coordination.

Provides time-bounded exclusive leases on named resources. Expired leases
are reaped lazily on acquire. Uses the canonical Postgres
``execution_leases`` table via the ``backend`` parameter.
"""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Protocol

from storage.postgres.connection import SyncPostgresConnection


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LeaseHandle:
    """Opaque handle returned after a successful acquire."""
    lease_id: str
    resource_key: str
    holder_id: str
    expires_at: datetime


@dataclass(frozen=True)
class LeaseInfo:
    """Read-only snapshot of an active lease."""
    lease_id: str
    resource_key: str
    holder_id: str
    acquired_at: datetime
    expires_at: datetime
    renewed_at: Optional[datetime]


# ---------------------------------------------------------------------------
# Backend protocol
# ---------------------------------------------------------------------------

class LeaseBackend(Protocol):
    """Minimal DB interface used by LeaseManager."""

    def reap_expired(self, now: datetime) -> int: ...
    def try_acquire(
        self, lease_id: str, resource_key: str, holder_id: str,
        now: datetime, expires_at: datetime,
    ) -> bool: ...
    def release(self, lease_id: str) -> bool: ...
    def renew(self, lease_id: str, new_expires: datetime, now: datetime) -> bool: ...
    def is_held(self, resource_key: str, now: datetime) -> Optional[LeaseInfo]: ...


# ---------------------------------------------------------------------------
# Postgres backend
# ---------------------------------------------------------------------------

class PostgresLeaseBackend:
    """Postgres-backed lease storage using the canonical execution_leases table."""

    def __init__(self, conn: SyncPostgresConnection) -> None:
        self._conn = conn

    def reap_expired(self, now: datetime) -> int:
        row_count = self._conn.fetchval(
            """
            WITH deleted AS (
                DELETE FROM execution_leases
                WHERE expires_at <= $1
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            """,
            now,
        )
        return int(row_count or 0)

    def try_acquire(
        self,
        lease_id: str,
        resource_key: str,
        holder_id: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        row = self._conn.fetchrow(
            """
            INSERT INTO execution_leases (
                lease_id,
                holder_id,
                resource_key,
                acquired_at,
                expires_at
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (resource_key) DO NOTHING
            RETURNING lease_id
            """,
            lease_id,
            holder_id,
            resource_key,
            now,
            expires_at,
        )
        return row is not None

    def release(self, lease_id: str) -> bool:
        row = self._conn.fetchrow(
            """
            DELETE FROM execution_leases
            WHERE lease_id = $1
            RETURNING lease_id
            """,
            lease_id,
        )
        return row is not None

    def renew(self, lease_id: str, new_expires: datetime, now: datetime) -> bool:
        row = self._conn.fetchrow(
            """
            UPDATE execution_leases
            SET expires_at = $1,
                renewed_at = $2
            WHERE lease_id = $3
              AND expires_at > $4
            RETURNING lease_id
            """,
            new_expires,
            now,
            lease_id,
            now,
        )
        return row is not None

    def is_held(self, resource_key: str, now: datetime) -> Optional[LeaseInfo]:
        row = self._conn.fetchrow(
            """
            SELECT lease_id, resource_key, holder_id, acquired_at, expires_at, renewed_at
            FROM execution_leases
            WHERE resource_key = $1
              AND expires_at > $2
            """,
            resource_key,
            now,
        )
        if row is None:
            return None
        return LeaseInfo(
            lease_id=str(row["lease_id"]),
            resource_key=str(row["resource_key"]),
            holder_id=str(row["holder_id"]),
            acquired_at=row["acquired_at"],
            expires_at=row["expires_at"],
            renewed_at=row["renewed_at"],
        )

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Lease manager
# ---------------------------------------------------------------------------

class LeaseManager:
    """High-level lease operations over a pluggable backend.

    Parameters
    ----------
    database_url:
        Connection string. Ignored when *backend* is supplied directly.
    backend:
        Explicit backend instance (useful for tests with SQLite).
    """

    def __init__(
        self,
        database_url: str = "",
        *,
        backend: Optional[LeaseBackend] = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        else:
            raise RuntimeError(
                "LeaseManager requires an explicit backend; runtime must not fall back to SQLite in-memory leases."
            )
        self._database_url = database_url

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    # -- public API ----------------------------------------------------------

    def acquire(
        self,
        resource_key: str,
        holder_id: str,
        ttl_seconds: int = 300,
    ) -> Optional[LeaseHandle]:
        """Try to acquire an exclusive lease on *resource_key*.

        Reaps expired leases first, then attempts INSERT. Returns ``None``
        if the resource is already held by another active holder.
        """
        now = self._now()
        self._backend.reap_expired(now)
        expires_at = now + timedelta(seconds=ttl_seconds)
        lease_id = uuid.uuid4().hex
        if self._backend.try_acquire(lease_id, resource_key, holder_id, now, expires_at):
            return LeaseHandle(
                lease_id=lease_id,
                resource_key=resource_key,
                holder_id=holder_id,
                expires_at=expires_at,
            )
        return None

    def release(self, handle: LeaseHandle) -> bool:
        """Release a lease by its handle. Returns True if deleted."""
        return self._backend.release(handle.lease_id)

    def renew(self, handle: LeaseHandle, ttl_seconds: int = 300) -> bool:
        """Extend a lease's TTL. Returns True if the lease was still active."""
        now = self._now()
        new_expires = now + timedelta(seconds=ttl_seconds)
        return self._backend.renew(handle.lease_id, new_expires, now)

    def is_held(self, resource_key: str) -> Optional[LeaseInfo]:
        """Return lease info if *resource_key* has an active (non-expired) lease."""
        return self._backend.is_held(resource_key, self._now())

    @contextlib.contextmanager
    def hold(
        self,
        resource_key: str,
        holder_id: str,
        ttl_seconds: int = 300,
    ) -> Generator[LeaseHandle, None, None]:
        """Context manager: acquire on enter, release on exit.

        Raises ``RuntimeError`` if the lease cannot be acquired.
        """
        handle = self.acquire(resource_key, holder_id, ttl_seconds=ttl_seconds)
        if handle is None:
            raise RuntimeError(
                f"Cannot acquire lease on {resource_key!r}: resource is held"
            )
        try:
            yield handle
        finally:
            self.release(handle)
