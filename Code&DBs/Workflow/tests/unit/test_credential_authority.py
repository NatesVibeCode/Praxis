from __future__ import annotations

import pytest

import runtime.credential_authority as credential_authority


class _CredConn:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.executed.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT access_token, provider_hint"):
            integration_id = args[0]
            token_kind = args[1]
            return [
                row
                for row in self.rows
                if row.get("integration_id") == integration_id
                and row.get("token_kind") == token_kind
            ]
        if normalized.startswith("INSERT INTO credential_tokens"):
            return []
        if normalized.startswith("SELECT integration_id, provider_hint, updated_at"):
            return [
                {
                    "integration_id": row["integration_id"],
                    "provider_hint": row.get("provider_hint", ""),
                    "updated_at": row.get("updated_at"),
                }
                for row in self.rows
                if row.get("integration_id", "").startswith("provider:")
                and row.get("token_kind") == "api_key"
            ]
        raise AssertionError(query)


def test_resolve_returns_env_var_mapping_when_credential_provisioned() -> None:
    conn = _CredConn(
        rows=[
            {
                "integration_id": "provider:openai",
                "token_kind": "api_key",
                "access_token": "sk-test",
                "provider_hint": "OPENAI_API_KEY",
            }
        ]
    )
    out = credential_authority.resolve_provider_credentials(
        provider_slug="openai", conn=conn
    )
    assert out == {"OPENAI_API_KEY": "sk-test"}


def test_resolve_returns_empty_when_no_credential_provisioned() -> None:
    conn = _CredConn(rows=[])
    out = credential_authority.resolve_provider_credentials(
        provider_slug="openai", conn=conn
    )
    assert out == {}


def test_resolve_normalizes_provider_slug() -> None:
    conn = _CredConn(
        rows=[
            {
                "integration_id": "provider:anthropic",
                "token_kind": "api_key",
                "access_token": "tok-A",
                "provider_hint": "CLAUDE_CODE_OAUTH_TOKEN",
            }
        ]
    )
    out = credential_authority.resolve_provider_credentials(
        provider_slug="  Anthropic  ", conn=conn
    )
    assert out == {"CLAUDE_CODE_OAUTH_TOKEN": "tok-A"}


def test_resolve_returns_empty_when_provider_slug_blank() -> None:
    conn = _CredConn(rows=[])
    assert credential_authority.resolve_provider_credentials(
        provider_slug="", conn=conn
    ) == {}


def test_resolve_falls_back_to_registry_when_provider_hint_missing(monkeypatch) -> None:
    conn = _CredConn(
        rows=[
            {
                "integration_id": "provider:openai",
                "token_kind": "api_key",
                "access_token": "sk-test",
                "provider_hint": "",
            }
        ]
    )

    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_api_key_env_vars",
        lambda provider_slug: ("OPENAI_API_KEY",) if provider_slug == "openai" else (),
    )

    out = credential_authority.resolve_provider_credentials(
        provider_slug="openai", conn=conn
    )
    assert out == {"OPENAI_API_KEY": "sk-test"}


def test_store_provider_credential_upserts_and_mirrors_to_keychain(monkeypatch) -> None:
    conn = _CredConn(rows=[])
    keychain_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "adapters.keychain.keychain_set",
        lambda env, value: keychain_calls.append((env, value)) or True,
    )

    result = credential_authority.store_provider_credential(
        provider_slug="openai",
        env_var_name="OPENAI_API_KEY",
        value="sk-new",
        conn=conn,
    )

    assert result["provider_slug"] == "openai"
    assert result["integration_id"] == "provider:openai"
    assert result["env_var_name"] == "OPENAI_API_KEY"
    assert result["keychain_mirrored"] is True
    assert keychain_calls == [("OPENAI_API_KEY", "sk-new")]
    assert any("INSERT INTO credential_tokens" in q for q, _ in conn.executed)


def test_store_provider_credential_rejects_blank_args() -> None:
    conn = _CredConn(rows=[])
    with pytest.raises(ValueError, match="provider_slug"):
        credential_authority.store_provider_credential(
            provider_slug="", env_var_name="X", value="y", conn=conn
        )
    with pytest.raises(ValueError, match="env_var_name"):
        credential_authority.store_provider_credential(
            provider_slug="openai", env_var_name="", value="y", conn=conn
        )
    with pytest.raises(ValueError, match="value"):
        credential_authority.store_provider_credential(
            provider_slug="openai", env_var_name="OPENAI_API_KEY", value="", conn=conn
        )


def test_list_provisioned_providers_returns_one_row_per_provider() -> None:
    conn = _CredConn(
        rows=[
            {
                "integration_id": "provider:openai",
                "token_kind": "api_key",
                "access_token": "x",
                "provider_hint": "OPENAI_API_KEY",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            {
                "integration_id": "provider:anthropic",
                "token_kind": "api_key",
                "access_token": "y",
                "provider_hint": "CLAUDE_CODE_OAUTH_TOKEN",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            # Non-provider integration (e.g. OAuth) should not appear.
            {
                "integration_id": "integration:hubspot",
                "token_kind": "access",
                "access_token": "z",
                "provider_hint": "",
                "updated_at": "2026-04-30T00:00:00Z",
            },
        ]
    )
    listed = credential_authority.list_provisioned_providers(conn=conn)
    slugs = sorted(item["provider_slug"] for item in listed)
    assert slugs == ["anthropic", "openai"]
