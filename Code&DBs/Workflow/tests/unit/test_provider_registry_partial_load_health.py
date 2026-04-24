"""Regression pin for BUG-F8283CC1.

Before the fix, ``registry.provider_execution_registry._load_from_db`` treated
a partial load — one or more DB rows skipped due to ``_parse_profile_row``
errors — identically to a clean authoritative load: ``_load_status`` was set
to ``LOADED_FROM_DB`` and ``authority_available`` was ``True``. The only hint
of degradation was a warning log line and a free-form string in ``_load_error``
("N row(s) skipped"). Operator dashboards and ``/health`` read
``registry_health()["status"]`` and saw ``"loaded_from_db"`` → "authoritative"
even while a provider was silently missing.

The fix introduces ``RegistryLoadStatus.LOADED_FROM_DB_PARTIAL`` and surfaces
the skipped rows in a structured ``skipped_rows`` list alongside a new
``authority_complete`` boolean. Routing for the successfully-parsed providers
keeps working, so ``authority_available`` stays ``True``, but the degradation
is now visible to operators.

These pins cover:

1. Clean load — no parse errors — reports the clean ``loaded_from_db``
   authoritative status with ``authority_complete=True`` and an empty
   ``skipped_rows``.
2. Partial load — one row skipped, one loaded — reports the new degraded
   ``loaded_from_db_partial`` status; ``authority_available`` stays ``True``
   so callers depending on it don't regress; ``authority_complete`` is
   ``False`` and ``skipped_rows`` names the offending row.
3. All rows failing continues to report ``load_failed`` with
   ``authority_available=False`` (unchanged fail-closed path).
4. The load-warning line mentions BUG-F8283CC1 and names the skipped slug so
   operators can trace the degradation without digging.
"""
from __future__ import annotations

import logging

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


def _clean_row(*, provider_slug: str, binary_name: str) -> dict[str, object]:
    row: dict[str, object] = {key: None for key in _ALL_PROFILE_COLUMNS}
    row["provider_slug"] = provider_slug
    row["binary_name"] = binary_name
    row["aliases"] = []
    row["default_timeout"] = 60
    return row


def _broken_row(provider_slug: str) -> dict[str, object]:
    """Row that survives dict access but fails _parse_profile_row.

    ``_parse_profile_row`` calls ``int(row["default_timeout"])`` unconditionally,
    so a non-numeric timeout is a reliable, narrow trigger for a parse error
    that exercises the partial-load path without also exercising some other
    validation.
    """
    row = _clean_row(provider_slug=provider_slug, binary_name=f"{provider_slug}-cli")
    row["default_timeout"] = "not-a-number"
    return row


def _install_fake_db(
    monkeypatch,
    rows: list[dict[str, object]],
    *,
    auxiliary_errors: list[str] | None = None,
) -> None:
    async def _fake_fetch(_db_url: str):
        return rows, [], [], list(auxiliary_errors or [])

    monkeypatch.setattr(provider_registry_mod, "_ASYNCPG_AVAILABLE", True)
    monkeypatch.setattr(
        provider_registry_mod, "_require_database_url", lambda: "postgresql://fake/db"
    )
    monkeypatch.setattr(provider_registry_mod, "_fetch_from_db", _fake_fetch)


@pytest.fixture
def _registry_state_guard():
    snapshot = {
        "_REGISTRY": dict(provider_registry_mod._REGISTRY),
        "_ALIAS_MAP": dict(provider_registry_mod._ALIAS_MAP),
        "_ADAPTER_CONFIG": dict(provider_registry_mod._ADAPTER_CONFIG),
        "_ADAPTER_FAILURE_MAPPINGS": dict(provider_registry_mod._ADAPTER_FAILURE_MAPPINGS),
        "_DB_LOADED": provider_registry_mod._DB_LOADED,
        "_load_status": provider_registry_mod._load_status,
        "_load_error": provider_registry_mod._load_error,
        "_load_timestamp": provider_registry_mod._load_timestamp,
        "_load_skipped_rows": provider_registry_mod._load_skipped_rows,
        "_load_auxiliary_errors": provider_registry_mod._load_auxiliary_errors,
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
        provider_registry_mod._ADAPTER_FAILURE_MAPPINGS.update(
            snapshot["_ADAPTER_FAILURE_MAPPINGS"]
        )
        provider_registry_mod._DB_LOADED = snapshot["_DB_LOADED"]
        provider_registry_mod._load_status = snapshot["_load_status"]
        provider_registry_mod._load_error = snapshot["_load_error"]
        provider_registry_mod._load_timestamp = snapshot["_load_timestamp"]
        provider_registry_mod._load_skipped_rows = snapshot["_load_skipped_rows"]
        provider_registry_mod._load_auxiliary_errors = snapshot["_load_auxiliary_errors"]


def test_clean_load_reports_authoritative_complete_status(
    monkeypatch, _registry_state_guard
) -> None:
    rows = [
        _clean_row(provider_slug="acme", binary_name="acme-cli"),
        _clean_row(provider_slug="contoso", binary_name="contoso-cli"),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    assert health["status"] == "loaded_from_db"
    assert health["authority_available"] is True
    assert health["authority_complete"] is True
    assert health["skipped_rows"] == []
    assert health["provider_count"] == 2
    assert health["error"] is None


def test_partial_load_reports_degraded_status_with_skipped_rows(
    monkeypatch, _registry_state_guard
) -> None:
    rows = [
        _clean_row(provider_slug="acme", binary_name="acme-cli"),
        _broken_row(provider_slug="contoso"),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    # New degraded status — the key BUG-F8283CC1 fix.
    assert health["status"] == "loaded_from_db_partial"
    # authority_available stays True so routing for successfully-parsed
    # providers keeps working — callers that check this boolean don't regress.
    assert health["authority_available"] is True
    # authority_complete is the new strict signal operators read to see the
    # partial state. No clean-load shortcut can hide the degradation.
    assert health["authority_complete"] is False
    assert health["provider_count"] == 1
    assert health["providers"] == ["acme"]
    # skipped_rows is structured and names the offending slug.
    assert len(health["skipped_rows"]) == 1
    assert "contoso" in health["skipped_rows"][0]
    assert "1 row(s) skipped" in (health["error"] or "")
    # The successfully-parsed provider still resolves normally.
    assert provider_registry_mod.get_profile("acme") is not None
    # The skipped provider is NOT silently installed.
    assert provider_registry_mod.get_profile("contoso") is None


def test_all_rows_failing_still_fails_closed(
    monkeypatch, _registry_state_guard
) -> None:
    """The pre-existing fail-closed path for zero surviving rows is preserved."""
    rows = [
        _broken_row(provider_slug="acme"),
        _broken_row(provider_slug="contoso"),
    ]
    _install_fake_db(monkeypatch, rows)

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    assert health["status"] == "load_failed"
    assert health["authority_available"] is False
    assert health["authority_complete"] is False
    assert health["provider_count"] == 0


def test_partial_load_warning_names_bug_and_skipped_slug(
    monkeypatch, _registry_state_guard
) -> None:
    """Operators reading the logs must find BUG-F8283CC1 and the slug.

    This mirrors the BUG-49388D90 log-message pin — the log line is part of
    the operator-visible contract when something degrades silently.
    """
    rows = [
        _clean_row(provider_slug="acme", binary_name="acme-cli"),
        _broken_row(provider_slug="contoso"),
    ]
    _install_fake_db(monkeypatch, rows)

    records: list[logging.LogRecord] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Handler(level=logging.WARNING)
    provider_registry_mod.logger.addHandler(handler)
    try:
        provider_registry_mod.reload_from_db()
    finally:
        provider_registry_mod.logger.removeHandler(handler)

    joined = "\n".join(rec.getMessage() for rec in records)
    assert "BUG-F8283CC1" in joined
    assert "contoso" in joined
    assert "partial load" in joined.lower()


def test_auxiliary_table_failure_reports_degraded_authority(
    monkeypatch, _registry_state_guard
) -> None:
    rows = [_clean_row(provider_slug="acme", binary_name="acme-cli")]
    _install_fake_db(
        monkeypatch,
        rows,
        auxiliary_errors=["adapter_failure_mappings: RuntimeError: table offline"],
    )

    provider_registry_mod.reload_from_db()
    health = provider_registry_mod.registry_health()

    assert health["status"] == "loaded_from_db_partial"
    assert health["authority_available"] is True
    assert health["authority_complete"] is False
    assert health["provider_count"] == 1
    assert health["skipped_rows"] == []
    assert health["auxiliary_errors"] == [
        "adapter_failure_mappings: RuntimeError: table offline"
    ]
    assert "1 auxiliary table(s) failed" in (health["error"] or "")
