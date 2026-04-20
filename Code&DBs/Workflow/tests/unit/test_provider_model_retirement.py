"""Unit tests for ``registry.provider_model_retirement``.

The detector has two cases (live API discovery vs ledger fallback) and a set of
safety guards that the daily heartbeat relies on. These tests pin the
guards explicitly — auto-retiring 50% of the active pool because of a partial
network response would be catastrophic.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any

import pytest

from registry import provider_model_retirement as pmr


# ---------------------------------------------------------------------------
# FakeConn
# ---------------------------------------------------------------------------


class _FakeConn:
    """asyncpg.Connection stand-in keyed on substrings inside the SQL text."""

    def __init__(
        self,
        *,
        candidates: list[dict[str, Any]] | None = None,
        profiles: dict[str, dict[str, Any]] | None = None,
        ledger: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self._candidates = list(candidates or [])
        self._profiles = dict(profiles or {})
        self._ledger = dict(ledger or {})
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM provider_model_candidates" in query:
            return list(self._candidates)
        if "FROM provider_model_retirement_ledger" in query:
            provider_slug = args[0]
            return list(self._ledger.get(provider_slug, []))
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "FROM provider_cli_profiles" in query:
            provider_slug = args[0]
            row = self._profiles.get(provider_slug)
            return dict(row) if row else None
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        # Default: pretend one row updated.
        return "UPDATE 1"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_api_discovery_findings_flags_models_missing_from_live_set() -> None:
    findings = pmr._api_discovery_findings(
        provider_slug="openai",
        registered=("gpt-5", "gpt-4o", "gpt-4o-mini"),
        discovered=("gpt-5", "gpt-4o-mini"),
    )
    assert [f.model_slug for f in findings] == ["gpt-4o"]
    assert findings[0].source == "api_discovery"
    assert findings[0].provider_slug == "openai"


def test_api_discovery_findings_returns_empty_when_all_present() -> None:
    findings = pmr._api_discovery_findings(
        provider_slug="openai",
        registered=("gpt-5",),
        discovered=("gpt-5", "gpt-4o-mini"),
    )
    assert findings == []


def test_ledger_findings_only_flags_retired_kind_past_effective_date() -> None:
    today = datetime(2026, 4, 20, tzinfo=timezone.utc)
    rows = [
        {
            "model_slug": "claude-3-haiku-20240307",
            "retirement_effective_date": date(2026, 4, 19),
            "retirement_kind": "retired",
            "notes": "Haiku 3 retired.",
        },
        {
            "model_slug": "claude-sonnet-4-20250514",
            "retirement_effective_date": date(2026, 6, 15),
            "retirement_kind": "deprecating",
            "notes": "Sonnet 4 retires later.",
        },
    ]
    findings = pmr._ledger_findings(
        provider_slug="anthropic",
        registered=("claude-3-haiku-20240307", "claude-sonnet-4-20250514"),
        ledger_rows=rows,
        today=today,
    )
    assert [f.model_slug for f in findings] == ["claude-3-haiku-20240307"]
    assert findings[0].ledger_kind == "retired"
    assert findings[0].effective_date == "2026-04-19"


def test_ledger_findings_skips_future_dated_retired_rows() -> None:
    today = datetime(2026, 4, 20, tzinfo=timezone.utc)
    rows = [
        {
            "model_slug": "claude-future",
            "retirement_effective_date": date(2026, 12, 31),
            "retirement_kind": "retired",
            "notes": None,
        },
    ]
    assert pmr._ledger_findings(
        provider_slug="anthropic",
        registered=("claude-future",),
        ledger_rows=rows,
        today=today,
    ) == []


# ---------------------------------------------------------------------------
# Per-provider scan
# ---------------------------------------------------------------------------


_FAKE_PROFILE = {
    "provider_slug": "openai",
    "default_model": "gpt-5",
    "api_endpoint": "https://api.openai.com/v1/chat/completions",
    "api_protocol_family": "openai_chat_completions",
    "api_key_env_vars": ["OPENAI_API_KEY"],
    "default_timeout": 60,
}


def _stub_discovery(monkeypatch: pytest.MonkeyPatch, models: tuple[str, ...] | Exception) -> None:
    """Replace the live discovery call with a deterministic stub."""
    def fake_discover(spec, *, env, transport_details):
        if isinstance(models, Exception):
            raise models
        return models
    monkeypatch.setattr(pmr, "_discover_api_models", fake_discover)


def test_scan_provider_api_discovery_flags_missing_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_discovery(monkeypatch, ("gpt-5", "gpt-4o-mini"))
    conn = _FakeConn(profiles={"openai": _FAKE_PROFILE})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="openai",
            registered=["gpt-5", "gpt-4o", "gpt-4o-mini"],
            env={"OPENAI_API_KEY": "sk-test"},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.discovery_mode == "api_discovery"
    assert outcome.status == "failed"
    assert outcome.live_count == 2
    assert [f.model_slug for f in outcome.findings] == ["gpt-4o"]


def test_scan_provider_api_discovery_safety_guard_skips_when_too_many_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If live discovery would retire >50% of registered models, skip."""
    # Only one of three is "discovered" — that's a 66% drop, above threshold.
    _stub_discovery(monkeypatch, ("gpt-5",))
    conn = _FakeConn(profiles={"openai": _FAKE_PROFILE})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="openai",
            registered=["gpt-5", "gpt-4o", "gpt-4o-mini"],
            env={"OPENAI_API_KEY": "sk-test"},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "degraded"
    assert outcome.findings == ()
    assert "safety threshold" in outcome.summary


def test_scan_provider_api_discovery_safety_guard_lets_50pct_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exactly 50% drop is at the threshold — must not exceed it."""
    # 2 of 4 missing = 50% — boundary, should still act (NOT exceed).
    _stub_discovery(monkeypatch, ("a", "b"))
    conn = _FakeConn(profiles={"openai": _FAKE_PROFILE})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="openai",
            registered=["a", "b", "c", "d"],
            env={"OPENAI_API_KEY": "sk-test"},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "failed"
    assert {f.model_slug for f in outcome.findings} == {"c", "d"}


def test_scan_provider_api_discovery_skips_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_discovery(monkeypatch, RuntimeError("HTTP 401: bad key"))
    conn = _FakeConn(profiles={"openai": _FAKE_PROFILE})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="openai",
            registered=["gpt-5"],
            env={"OPENAI_API_KEY": "sk-test"},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "degraded"
    assert outcome.findings == ()
    assert "HTTP 401" in (outcome.error or "")


def test_scan_provider_api_discovery_skips_on_zero_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty live set is degenerate — never auto-retire from it."""
    _stub_discovery(monkeypatch, ())
    conn = _FakeConn(profiles={"openai": _FAKE_PROFILE})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="openai",
            registered=["gpt-5"],
            env={"OPENAI_API_KEY": "sk-test"},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "degraded"
    assert outcome.findings == ()
    assert outcome.live_count == 0


def test_scan_provider_skips_when_profile_missing() -> None:
    """No provider_cli_profile → no way to probe → skipped (not failed)."""
    conn = _FakeConn(profiles={})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="some-vendor",
            registered=["model-a"],
            env={},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "skipped"
    assert outcome.discovery_mode == "skipped"


def test_scan_provider_skips_when_profile_has_no_api_metadata() -> None:
    """A profile row that exists but has no API endpoint can't be probed."""
    conn = _FakeConn(profiles={"some-vendor": {**_FAKE_PROFILE, "api_endpoint": None}})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="some-vendor",
            registered=["model-a"],
            env={},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "skipped"


def test_scan_provider_anthropic_uses_ledger_not_api() -> None:
    """Anthropic must fall through to the ledger (decision.2026-04-20.anthropic-cli-only-restored)."""
    conn = _FakeConn(
        ledger={
            "anthropic": [
                {
                    "model_slug": "claude-3-haiku-20240307",
                    "retirement_effective_date": date(2026, 4, 19),
                    "retirement_kind": "retired",
                    "notes": "Haiku 3 retired.",
                },
            ],
        },
    )
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="anthropic",
            registered=["claude-3-haiku-20240307", "claude-sonnet-4-6"],
            env={},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.discovery_mode == "ledger"
    assert outcome.status == "failed"
    assert [f.model_slug for f in outcome.findings] == ["claude-3-haiku-20240307"]


def test_scan_provider_anthropic_with_no_ledger_rows_is_degraded() -> None:
    """Ledger-only provider with an empty ledger can't judge anything — degraded."""
    conn = _FakeConn(ledger={})
    outcome = asyncio.run(
        pmr._scan_provider(
            conn,
            provider_slug="anthropic",
            registered=["claude-3-haiku-20240307"],
            env={},
            today=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert outcome.status == "degraded"
    assert outcome.findings == ()


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def test_scan_full_report_dry_run_does_not_execute_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_discovery(monkeypatch, ("gpt-5",))
    conn = _FakeConn(
        candidates=[
            {"provider_slug": "openai", "model_slug": "gpt-5"},
            {"provider_slug": "openai", "model_slug": "gpt-4o"},
        ],
        profiles={"openai": _FAKE_PROFILE},
    )
    report = asyncio.run(
        pmr.scan_provider_model_retirements(
            conn,
            env={"OPENAI_API_KEY": "sk-test"},
            dry_run=True,
            now=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert report.dry_run is True
    assert report.findings_total == 1
    assert report.retirements_applied == 0
    assert conn.executed == []  # Nothing written when dry_run.


def test_scan_full_report_apply_runs_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_discovery(monkeypatch, ("gpt-5",))
    conn = _FakeConn(
        candidates=[
            {"provider_slug": "openai", "model_slug": "gpt-5"},
            {"provider_slug": "openai", "model_slug": "gpt-4o"},
        ],
        profiles={"openai": _FAKE_PROFILE},
    )
    report = asyncio.run(
        pmr.scan_provider_model_retirements(
            conn,
            env={"OPENAI_API_KEY": "sk-test"},
            dry_run=False,
            now=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert report.findings_total == 1
    assert report.retirements_applied == 1
    # Exactly one UPDATE on provider_model_candidates with the right slug.
    update_calls = [
        (q, args) for (q, args) in conn.executed
        if "UPDATE provider_model_candidates" in q
    ]
    assert len(update_calls) == 1
    assert update_calls[0][1][0] == "openai"
    assert update_calls[0][1][1] == "gpt-4o"


def test_scan_apply_does_not_retire_advisory_ledger_rows() -> None:
    """`deprecating` and `sunset_warning` are advisory — must NOT auto-retire."""
    conn = _FakeConn(
        candidates=[
            {"provider_slug": "anthropic", "model_slug": "claude-sonnet-4-20250514"},
        ],
        ledger={
            "anthropic": [
                {
                    "model_slug": "claude-sonnet-4-20250514",
                    "retirement_effective_date": date(2026, 6, 15),
                    "retirement_kind": "deprecating",
                    "notes": "Sonnet 4 retires 2026-06-15.",
                },
            ],
        },
    )
    report = asyncio.run(
        pmr.scan_provider_model_retirements(
            conn,
            env={},
            dry_run=False,
            now=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    # Advisory only: no findings (effective date is in the future), no updates.
    assert report.findings_total == 0
    assert report.retirements_applied == 0
    assert conn.executed == []


def test_scan_apply_retires_anthropic_ledger_retired_rows() -> None:
    conn = _FakeConn(
        candidates=[
            {"provider_slug": "anthropic", "model_slug": "claude-3-haiku-20240307"},
        ],
        ledger={
            "anthropic": [
                {
                    "model_slug": "claude-3-haiku-20240307",
                    "retirement_effective_date": date(2026, 4, 19),
                    "retirement_kind": "retired",
                    "notes": "Haiku 3 retired.",
                },
            ],
        },
    )
    report = asyncio.run(
        pmr.scan_provider_model_retirements(
            conn,
            env={},
            dry_run=False,
            now=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
    )
    assert report.findings_total == 1
    assert report.retirements_applied == 1
    update_calls = [
        (q, args) for (q, args) in conn.executed
        if "UPDATE provider_model_candidates" in q
    ]
    assert len(update_calls) == 1
    assert update_calls[0][1][0] == "anthropic"
    assert update_calls[0][1][1] == "claude-3-haiku-20240307"
