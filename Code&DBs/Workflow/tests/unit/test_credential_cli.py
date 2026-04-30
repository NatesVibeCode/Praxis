from __future__ import annotations

import io
import json

import surfaces.cli.commands.credential as credential_cli


def test_credential_command_list_emits_empty_when_none_provisioned(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.credential_authority.list_provisioned_providers",
        lambda: [],
    )
    stdout = io.StringIO()
    rc = credential_cli._credential_command(["list"], stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert rc == 0
    assert payload["ok"] is True
    assert payload["providers"] == []


def test_credential_command_list_renders_provisioned_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.credential_authority.list_provisioned_providers",
        lambda: [
            {"provider_slug": "openai", "env_var_name": "OPENAI_API_KEY", "updated_at": "2026-04-30"},
            {"provider_slug": "anthropic", "env_var_name": "CLAUDE_CODE_OAUTH_TOKEN", "updated_at": None},
        ],
    )
    stdout = io.StringIO()
    rc = credential_cli._credential_command(["list"], stdout=stdout)
    payload = json.loads(stdout.getvalue())
    assert rc == 0
    slugs = [p["provider_slug"] for p in payload["providers"]]
    assert slugs == ["openai", "anthropic"]


def test_credential_command_onboard_writes_through_authority(monkeypatch) -> None:
    captured: list[dict] = []

    def _fake_store(*, provider_slug, env_var_name, value):
        captured.append(
            {
                "provider_slug": provider_slug,
                "env_var_name": env_var_name,
                "value": value,
            }
        )
        return {
            "provider_slug": provider_slug,
            "integration_id": f"provider:{provider_slug}",
            "env_var_name": env_var_name,
            "keychain_mirrored": True,
        }

    monkeypatch.setattr(
        "runtime.credential_authority.store_provider_credential",
        _fake_store,
    )
    stdout = io.StringIO()
    rc = credential_cli._credential_command(
        ["onboard", "openai", "--env", "OPENAI_API_KEY", "--secret", "sk-test"],
        stdout=stdout,
    )
    assert rc == 0
    assert captured == [
        {"provider_slug": "openai", "env_var_name": "OPENAI_API_KEY", "value": "sk-test"}
    ]
    payload = json.loads(stdout.getvalue())
    assert payload["ok"] is True
    assert payload["env_var_name"] == "OPENAI_API_KEY"
    assert payload["keychain_mirrored"] is True


def test_credential_command_onboard_defaults_env_var_from_registry(monkeypatch) -> None:
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_api_key_env_vars",
        lambda provider_slug: ("OPENROUTER_API_KEY",) if provider_slug == "openrouter" else (),
    )
    captured: list[str] = []
    monkeypatch.setattr(
        "runtime.credential_authority.store_provider_credential",
        lambda *, provider_slug, env_var_name, value: captured.append(env_var_name)
        or {
            "provider_slug": provider_slug,
            "integration_id": f"provider:{provider_slug}",
            "env_var_name": env_var_name,
            "keychain_mirrored": False,
        },
    )
    stdout = io.StringIO()
    rc = credential_cli._credential_command(
        ["onboard", "openrouter", "--secret", "sk-or-test"],
        stdout=stdout,
    )
    assert rc == 0
    assert captured == ["OPENROUTER_API_KEY"]


def test_credential_command_onboard_errors_when_env_unresolvable(monkeypatch) -> None:
    monkeypatch.setattr(
        "registry.provider_execution_registry.resolve_api_key_env_vars",
        lambda provider_slug: (),
    )
    stdout = io.StringIO()
    rc = credential_cli._credential_command(
        ["onboard", "unknown-provider", "--secret", "x"],
        stdout=stdout,
    )
    assert rc == 2
    assert "cannot infer env var name" in stdout.getvalue()


def test_credential_command_onboard_errors_when_secret_missing() -> None:
    stdout = io.StringIO()
    rc = credential_cli._credential_command(
        ["onboard", "openai", "--env", "OPENAI_API_KEY"],
        stdout=stdout,
    )
    assert rc == 2
    assert "secret is required" in stdout.getvalue()


def test_credential_command_help_lists_subcommands() -> None:
    stdout = io.StringIO()
    rc = credential_cli._credential_command([], stdout=stdout)
    assert rc == 0
    text = stdout.getvalue()
    assert "list" in text
    assert "onboard" in text
    assert "remove" in text
