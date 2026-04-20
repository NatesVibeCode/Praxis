"""Regression pin for BUG-49388D90.

Before the fix, ``registry.provider_execution_registry._load_from_db`` logged a
warning but still wrote ``loaded_aliases[alias] = profile.provider_slug`` even
when a different provider had already claimed the alias. That silently remapped
``resolve_provider_from_alias`` based on DB row order — operationally a routing
switch, surfaced as a log line rather than a load failure.

The fix turns alias ownership into a load-time authority check: any alias
claimed by >1 distinct ``provider_slug`` refuses the entire load, leaving
``_ALIAS_MAP`` empty and health as ``load_failed``. These pins cover:

1. Two rows sharing an ``aliases`` entry.
2. A second row's ``binary_name`` colliding with the first row's ``aliases``
   entry (binaries share the alias namespace).
3. The safe path — a provider listing its own binary inside aliases — is not
   a conflict because the claimant set has a single slug.
4. The typed error carries the conflict map for observability.
"""
from __future__ import annotations

import pytest

import registry.provider_execution_registry as provider_registry_mod


_ALL_PROFILE_COLUMNS = (
    "provider_slug",
    "binary_name",
    "base_flags",
    "model_flag",
    "system_prompt_flag",
    "json_schema_flag",
    "output_format",
    "output_envelope_key",
    "forbidden_flags",
    "default_timeout",
    "aliases",
    "default_model",
    "api_endpoint",
    "api_protocol_family",
    "api_key_env_vars",
    "adapter_economics",
    "prompt_mode",
    "mcp_config_style",
    "mcp_args_template",
    "sandbox_env_overrides",
    "exclude_from_rotation",
    "lane_policies",
)


def _profile_row(
    *,
    provider_slug: str,
    binary_name: str,
    aliases: tuple[str, ...] = (),
) -> dict[str, object]:
    row: dict[str, object] = {key: None for key in _ALL_PROFILE_COLUMNS}
    row["provider_slug"] = provider_slug
    row["binary_name"] = binary_name
    row["aliases"] = list(aliases)
    # _parse_profile_row calls int(row["default_timeout"]) unconditionally, so
    # this must be set even on minimal fixtures.
    row["default_timeout"] = 60
    return row


def _install_fake_db(monkeypatch, rows: list[dict[str, object]]) -> None:
    """Make ``reload_from_db`` load ``rows`` without touching a real DB."""

    async def _fake_fetch(_db_url: str):
        return rows, [], []

    monkeypatch.setattr(provider_registry_mod, "_ASYNCPG_AVAILABLE", True)
    monkeypatch.setattr(provider_registry_mod, "_require_database_url", lambda: "postgresql://fake/db")
    monkeypatch.setattr(provider_registry_mod, "_fetch_from_db", _fake_fetch)


@pytest.fixture
def _registry_state_guard():
    """Snapshot and restore module-level registry state around each test.

    The registry is a global singleton. Tests that call ``reload_from_db``
    mutate it; restoring at teardown keeps one test from leaking state into
    the next and avoids polluting any parallel real-DB integration runs.
    """
    snapshot = {
        "_REGISTRY": dict(provider_registry_mod._REGISTRY),
        "_ALIAS_MAP": dict(provider_registry_mod._ALIAS_MAP),
        "_ADAPTER_CONFIG": dict(provider_registry_mod._ADAPTER_CONFIG),
        "_ADAPTER_FAILURE_MAPPINGS": dict(provider_registry_mod._ADAPTER_FAILURE_MAPPINGS),
        "_DB_LOADED": provider_registry_mod._DB_LOADED,
        "_load_status": provider_registry_mod._load_status,
        "_load_error": provider_registry_mod._load_error,
        "_load_timestamp": provider_registry_mod._load_timestamp,
    }
    try:
        yield
    finally:
        provider_registry_mod._REGISTRY.clear()
        provider_registry_mod._REGISTRY.update(snapshot["_REGISTRY"])
        provider_registry_mod._ALIAS_MAP.clear()
        provider_registry_mod._ALIAS_MAP.update(snapshot["_ALIAS_MAP"])
        provider_registry_mod._ADAPTER_CONFIG.clear()
        provider_registry_mod._ADAPTER_CONFIG.update(snapshot["_ADAPTER_CONFIG"])
        provider_registry_mod._ADAPTER_FAILURE_MAPPINGS.clear()
        provider_registry_mod._ADAPTER_FAILURE_MAPPINGS.update(snapshot["_ADAPTER_FAILURE_MAPPINGS"])
        provider_registry_mod._DB_LOADED = snapshot["_DB_LOADED"]
        provider_registry_mod._load_status = snapshot["_load_status"]
        provider_registry_mod._load_error = snapshot["_load_error"]
        provider_registry_mod._load_timestamp = snapshot["_load_timestamp"]


def test_alias_conflict_between_two_providers_fails_load_closed(
    monkeypatch, _registry_state_guard
) -> None:
    """Two distinct provider_slugs claiming the same alias must refuse the load."""
    rows = [
        _profile_row(
            provider_slug="acme",
            binary_name="acme-cli",
            aliases=("shared-a",),
        ),
        _profile_row(
            provider_slug="contoso",
            binary_name="contoso-cli",
            aliases=("shared-a",),
        ),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    # Load refused. The alias must NOT silently resolve to either slug —
    # that's exactly the BUG-49388D90 routing bug.
    assert health["status"] == "load_failed"
    assert "alias ownership conflict" in (health["error"] or "")
    assert health["authority_available"] is False
    assert health["provider_count"] == 0
    assert provider_registry_mod.resolve_provider_from_alias("shared-a") is None
    # The alias map stays empty — no last-writer-wins residue.
    assert provider_registry_mod._ALIAS_MAP == {}


def test_alias_conflict_between_binary_and_alias_fails_load_closed(
    monkeypatch, _registry_state_guard
) -> None:
    """A second row's binary_name colliding with the first row's alias refuses too.

    Binaries and explicit aliases share a single lookup namespace
    (``_ALIAS_MAP``), so a binary collision carries the same silent-remap
    risk as an explicit alias collision. Both must fail closed.
    """
    rows = [
        _profile_row(
            provider_slug="acme",
            binary_name="acme-cli",
            aliases=("collide-bin",),
        ),
        _profile_row(
            provider_slug="contoso",
            binary_name="collide-bin",
            aliases=(),
        ),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    assert health["status"] == "load_failed"
    assert provider_registry_mod.resolve_provider_from_alias("collide-bin") is None
    assert provider_registry_mod._ALIAS_MAP == {}


def test_single_provider_claiming_own_binary_via_aliases_is_not_a_conflict(
    monkeypatch, _registry_state_guard
) -> None:
    """A provider may redundantly list its own binary in aliases — same claimant.

    Guards against overcorrection: set semantics should treat ``binary`` and
    an aliases entry of the same string from the SAME provider as a single
    claim, not a conflict.
    """
    rows = [
        _profile_row(
            provider_slug="acme",
            binary_name="acme-cli",
            aliases=("acme-cli", "acme"),  # binary repeated in aliases
        ),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    assert health["status"] == "loaded_from_db"
    assert provider_registry_mod.resolve_provider_from_alias("acme-cli") == "acme"
    assert provider_registry_mod.resolve_provider_from_alias("acme") == "acme"


def test_alias_conflict_error_carries_structured_conflicts() -> None:
    """Typed error exposes ``conflicts`` for observability and triage."""
    err = provider_registry_mod.ProviderRegistryAliasConflictError(
        "conflict",
        conflicts={"shared-a": ("acme", "contoso")},
    )
    assert isinstance(err, provider_registry_mod.ProviderRegistryDataError)
    assert err.conflicts == {"shared-a": ("acme", "contoso")}


def test_error_message_names_bug_and_conflicting_providers(
    monkeypatch, _registry_state_guard
) -> None:
    """The load-failure message must reference BUG-49388D90 and the slugs.

    Operators looking at the failure in ``registry_health`` or the logs need
    to find the bug pin quickly and see which rows disagree without digging
    through DB authority.
    """
    rows = [
        _profile_row(
            provider_slug="acme",
            binary_name="acme-cli",
            aliases=("shared-a",),
        ),
        _profile_row(
            provider_slug="contoso",
            binary_name="contoso-cli",
            aliases=("shared-a",),
        ),
    ]
    _install_fake_db(monkeypatch, rows)

    import logging

    caplog_records: list[logging.LogRecord] = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            caplog_records.append(record)

    handler = _CaptureHandler(level=logging.ERROR)
    provider_registry_mod.logger.addHandler(handler)
    try:
        provider_registry_mod.reload_from_db()
    finally:
        provider_registry_mod.logger.removeHandler(handler)

    messages = [rec.getMessage() for rec in caplog_records]
    joined = "\n".join(messages)
    assert "BUG-49388D90" in joined
    assert "'shared-a'" in joined
    assert "acme" in joined
    assert "contoso" in joined
