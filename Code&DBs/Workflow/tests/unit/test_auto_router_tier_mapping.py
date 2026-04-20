from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import runtime.auto_router as auto_router_mod


class _FakeConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.closed = False

    async def fetch(self, _query: str, *_args: Any) -> list[dict[str, Any]]:
        return self._rows

    async def close(self) -> None:
        self.closed = True


def _install_fake_db(monkeypatch, rows: list[dict[str, Any]]) -> _FakeConn:
    fake_conn = _FakeConn(rows)

    async def _connect():
        return fake_conn

    # The import happens inside the function body; patch where it is looked up.
    import storage.postgres as storage_postgres

    monkeypatch.setattr(storage_postgres, "connect_workflow_database", _connect)
    return fake_conn


def test_load_candidates_maps_db_tier_high_medium_low_to_runtime_vocabulary_without_db_bootstrap(monkeypatch) -> None:
    rows = [
        {
            "provider_slug": "google",
            "model_slug": "gemini-3.1-pro",
            "status": "active",
            "priority": 1,
            "route_tier": "high",
        },
        {
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "status": "active",
            "priority": 2,
            "route_tier": "medium",
        },
        {
            "provider_slug": "deepseek",
            "model_slug": "deepseek-r3",
            "status": "active",
            "priority": 3,
            "route_tier": "low",
        },
    ]
    _install_fake_db(monkeypatch, rows)

    loaded = asyncio.run(auto_router_mod._load_candidates_async())

    tiers = [candidate.tier for candidate in loaded]
    assert tiers == ["frontier", "mid", "economy"]


def test_load_candidates_drops_rows_with_unknown_route_tier_without_db_bootstrap(monkeypatch, caplog) -> None:
    rows = [
        {
            "provider_slug": "mystery",
            "model_slug": "future-model",
            "status": "active",
            "priority": 1,
            "route_tier": "ultra",
        },
        {
            "provider_slug": "google",
            "model_slug": "gemini-3.1-pro",
            "status": "active",
            "priority": 1,
            "route_tier": "high",
        },
    ]
    _install_fake_db(monkeypatch, rows)

    with caplog.at_level("WARNING", logger=auto_router_mod._log.name):
        loaded = asyncio.run(auto_router_mod._load_candidates_async())

    assert [c.tier for c in loaded] == ["frontier"]
    assert any(
        "unknown route_tier" in message and "ultra" in message for message in caplog.messages
    ), caplog.messages


def test_db_route_tier_map_is_exhaustive_for_db_check_constraint_without_db_bootstrap() -> None:
    # provider_model_candidates.route_tier CHECK enforces ('high', 'medium', 'low').
    # If a new tier value is added to the DB constraint without extending this map,
    # the runtime would silently drop the new candidates. Pin the current mapping.
    assert auto_router_mod._DB_ROUTE_TIER_TO_RUNTIME == {
        "high": "frontier",
        "medium": "mid",
        "low": "economy",
    }
