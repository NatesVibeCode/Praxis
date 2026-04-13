"""Tests for adapters.oauth_lifecycle — token store and refresh."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from adapters.oauth_lifecycle import (
    OAuthToken,
    OAuthTokenError,
    _is_expired,
    resolve_or_refresh,
    store_token,
)


# ---------------------------------------------------------------------------
# Fake Postgres connection
# ---------------------------------------------------------------------------

class _FakeConn:
    """Minimal conn stub that returns pre-loaded rows."""

    def __init__(self, rows_by_query: dict[str, list[dict]] | None = None):
        self._rows = rows_by_query or {}
        self.executed: list[tuple[str, ...]] = []

    def execute(self, sql: str, *args):
        self.executed.append((sql, *args))
        for key, rows in self._rows.items():
            if key in sql:
                return list(rows)
        return []


# ---------------------------------------------------------------------------
# _is_expired
# ---------------------------------------------------------------------------

class TestIsExpired:
    def test_none_expires_at_is_not_expired(self):
        assert _is_expired(None, 300) is False

    def test_future_token_is_not_expired(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        assert _is_expired(future, 300) is False

    def test_past_token_is_expired(self):
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _is_expired(past, 300) is True

    def test_within_buffer_is_expired(self):
        almost = datetime.now(timezone.utc) + timedelta(seconds=60)
        assert _is_expired(almost, 300) is True

    def test_naive_datetime_treated_as_utc(self):
        past = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
        assert _is_expired(past, 300) is True


# ---------------------------------------------------------------------------
# resolve_or_refresh — happy path
# ---------------------------------------------------------------------------

class TestResolveOrRefresh:
    def test_returns_valid_token(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        conn = _FakeConn({
            "token_kind = 'access'": [
                {
                    "access_token": "tok_abc",
                    "refresh_token": "ref_xyz",
                    "expires_at": future,
                    "scopes": ["read", "write"],
                    "token_type": "Bearer",
                }
            ]
        })
        token = resolve_or_refresh(conn, "test-integration")
        assert token.access_token == "tok_abc"
        assert token.integration_id == "test-integration"
        assert token.scopes == ("read", "write")

    def test_no_token_raises(self):
        conn = _FakeConn()
        with pytest.raises(OAuthTokenError, match="no stored token"):
            resolve_or_refresh(conn, "missing")

    def test_empty_integration_id_raises(self):
        conn = _FakeConn()
        with pytest.raises(OAuthTokenError, match="integration_id is required"):
            resolve_or_refresh(conn, "")


# ---------------------------------------------------------------------------
# store_token
# ---------------------------------------------------------------------------

class TestStoreToken:
    def test_upsert_executed(self):
        conn = _FakeConn()
        store_token(
            conn,
            "test-int",
            access_token="tok_new",
            refresh_token="ref_new",
            expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
            scopes=("read",),
            token_type="Bearer",
        )
        assert len(conn.executed) == 1
        sql = conn.executed[0][0]
        assert "credential_tokens" in sql
        assert "ON CONFLICT" in sql

    def test_args_passed(self):
        conn = _FakeConn()
        store_token(conn, "int-1", access_token="tok")
        args = conn.executed[0]
        assert args[1] == "int-1"
        assert args[2] == "tok"


# ---------------------------------------------------------------------------
# Credential resolution fallback
# ---------------------------------------------------------------------------

class TestCredentialFallback:
    def test_resolve_credential_with_conn_tries_oauth_first(self):
        """When conn and integration_id provided, OAuth store is checked."""
        from adapters.credentials import resolve_credential, CredentialResolutionError

        future = datetime.now(timezone.utc) + timedelta(hours=1)
        conn = _FakeConn({
            "token_kind = 'access'": [
                {
                    "access_token": "oauth_tok",
                    "refresh_token": None,
                    "expires_at": future,
                    "scopes": [],
                    "token_type": "Bearer",
                }
            ]
        })
        cred = resolve_credential("secret.test.dummy", conn=conn, integration_id="test-int")
        assert cred.api_key == "oauth_tok"
        assert cred.provider_hint == "bearer"

    def test_expired_token_without_refresh_raises(self):
        """Expired token with no refresh_token raises."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        conn = _FakeConn({
            "token_kind = 'access'": [
                {
                    "access_token": "expired",
                    "refresh_token": None,
                    "expires_at": past,
                    "scopes": [],
                    "token_type": "Bearer",
                }
            ]
        })
        with pytest.raises(OAuthTokenError, match="no refresh_token"):
            resolve_or_refresh(conn, "test-int")

    def test_refresh_missing_access_token_raises(self):
        """Refresh response without access_token raises."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        conn = _FakeConn({
            "token_kind = 'access'": [
                {
                    "access_token": "expired",
                    "refresh_token": "ref_tok",
                    "expires_at": past,
                    "scopes": [],
                    "token_type": "Bearer",
                }
            ]
        })
        from unittest.mock import patch, MagicMock
        import io

        # Mock _refresh_token to return response without access_token
        with patch("adapters.oauth_lifecycle._refresh_token") as mock_refresh:
            mock_refresh.return_value = {"token_type": "Bearer"}  # no access_token
            with pytest.raises(OAuthTokenError, match="missing access_token"):
                resolve_or_refresh(
                    conn, "test-int",
                    auth_shape={"token_url": "https://auth.example.com/token"},
                )

    def test_insecure_token_url_rejected(self):
        """Refresh with HTTP token_url raises."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        conn = _FakeConn({
            "token_kind = 'access'": [
                {
                    "access_token": "expired",
                    "refresh_token": "ref",
                    "expires_at": past,
                    "scopes": [],
                    "token_type": "Bearer",
                }
            ]
        })
        with pytest.raises(OAuthTokenError, match="HTTPS"):
            resolve_or_refresh(
                conn, "test-int",
                auth_shape={"token_url": "http://insecure.com/token"},
            )

    def test_resolve_credential_without_conn_uses_env(self):
        """Without conn, falls back to env var lookup."""
        from adapters.credentials import resolve_credential

        cred = resolve_credential(
            "secret.test.anthropic",
            env={"ANTHROPIC_API_KEY": "env_key"},
        )
        assert cred.api_key == "env_key"
