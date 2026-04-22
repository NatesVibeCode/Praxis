"""Mobile bootstrap/session ledgers."""

from __future__ import annotations

import hashlib
import secrets
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4


class MobileSessionError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _tx(conn: Any):
    if hasattr(conn, "transaction"):
        return conn.transaction()
    return nullcontext(conn)


def hash_secret(secret: str) -> str:
    if not isinstance(secret, str) or not secret:
        raise MobileSessionError("mobile.secret_required", "secret must be non-empty")
    return "sha256:" + hashlib.sha256(secret.encode("utf-8")).hexdigest()


def issue_bootstrap_token(
    conn: Any,
    *,
    principal_ref: str,
    token_secret: str,
    ttl_s: int = 600,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    rows = conn.execute(
        """
        INSERT INTO mobile_bootstrap_tokens (
            principal_ref, token_hash, issued_at, expires_at
        ) VALUES (
            $1, $2, $3, $4
        )
        RETURNING token_id, principal_ref, token_hash, issued_at, expires_at
        """,
        principal_ref,
        hash_secret(token_secret),
        effective_now,
        effective_now + timedelta(seconds=max(1, int(ttl_s))),
    )
    return dict(rows[0])


def create_mobile_session(
    conn: Any,
    *,
    principal_ref: str,
    device_id: str,
    session_token_secret: str,
    session_id: str | None = None,
    ttl_s: int = 3600,
    budget_limit: int = 25,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    resolved_session_id = session_id or str(uuid4())
    rows = conn.execute(
        """
        INSERT INTO mobile_sessions (
            session_id, principal_ref, device_id, session_token_hash,
            created_at, expires_at, last_step_up_at, budget_limit, budget_used
        ) VALUES (
            $1::uuid, $2, $3::uuid, $4, $5, $6, $5, $7, 0
        )
        RETURNING session_id, principal_ref, device_id, created_at, expires_at,
                  last_step_up_at, budget_limit, budget_used
        """,
        resolved_session_id,
        principal_ref,
        device_id,
        hash_secret(session_token_secret),
        effective_now,
        effective_now + timedelta(seconds=max(1, int(ttl_s))),
        max(0, int(budget_limit)),
    )
    return dict(rows[0])


def exchange_bootstrap_token(
    conn: Any,
    *,
    bootstrap_token_secret: str,
    device_id: str,
    session_token_secret: str | None = None,
    ttl_s: int = 3600,
    budget_limit: int = 25,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Consume one bootstrap token and create a mobile session atomically."""

    effective_now = now or _utc_now()
    resolved_session_secret = session_token_secret or secrets.token_urlsafe(32)
    with _tx(conn) as tx:
        rows = tx.execute(
            """
            SELECT token_id, principal_ref, token_hash, expires_at, consumed_at, revoked_at
            FROM mobile_bootstrap_tokens
            WHERE token_hash = $1
            FOR UPDATE
            """,
            hash_secret(bootstrap_token_secret),
        )
        if not rows:
            raise MobileSessionError("mobile.bootstrap_token_invalid", "bootstrap token was not found")
        token = dict(rows[0])
        if token.get("consumed_at") is not None:
            raise MobileSessionError("mobile.bootstrap_token_consumed", "bootstrap token was already consumed")
        if token.get("revoked_at") is not None:
            raise MobileSessionError("mobile.bootstrap_token_revoked", "bootstrap token was revoked")
        expires_at = token.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at <= effective_now:
            raise MobileSessionError("mobile.bootstrap_token_expired", "bootstrap token has expired")

        session_id = str(uuid4())
        session = create_mobile_session(
            tx,
            principal_ref=str(token["principal_ref"]),
            device_id=device_id,
            session_token_secret=resolved_session_secret,
            session_id=session_id,
            ttl_s=ttl_s,
            budget_limit=budget_limit,
            now=effective_now,
        )
        tx.execute(
            """
            UPDATE mobile_bootstrap_tokens
            SET consumed_at = $2,
                consumed_by_session_id = $3::uuid
            WHERE token_id = $1::uuid
              AND consumed_at IS NULL
              AND revoked_at IS NULL
            """,
            token["token_id"],
            effective_now,
            session_id,
        )
    return {
        "session": session,
        "session_token_secret": resolved_session_secret,
        "token_id": token["token_id"],
    }


def spend_session_budget(
    conn: Any,
    *,
    session_id: str,
    units: int = 1,
    reason_code: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or _utc_now()
    spend_units = max(0, int(units))
    rows = conn.execute(
        """
        WITH spent AS (
            UPDATE mobile_sessions
            SET budget_used = budget_used + $2
            WHERE session_id = $1::uuid
              AND revoked_at IS NULL
              AND expires_at > $4
              AND budget_used + $2 <= budget_limit
            RETURNING session_id, principal_ref, budget_used
        ), event AS (
            INSERT INTO mobile_session_budget_events (
                session_id, principal_ref, event_kind, units,
                budget_used_after, reason_code, recorded_at
            )
            SELECT session_id, principal_ref, 'spend', $2, budget_used, $3, $4
            FROM spent
            RETURNING budget_event_id
        )
        SELECT spent.session_id, spent.principal_ref, spent.budget_used,
               event.budget_event_id
        FROM spent, event
        """,
        session_id,
        spend_units,
        reason_code,
        effective_now,
    )
    if not rows:
        raise MobileSessionError(
            "mobile.session_budget_denied",
            "mobile session budget could not be spent atomically",
        )
    return dict(rows[0])


__all__ = [
    "MobileSessionError",
    "create_mobile_session",
    "exchange_bootstrap_token",
    "hash_secret",
    "issue_bootstrap_token",
    "spend_session_budget",
]
