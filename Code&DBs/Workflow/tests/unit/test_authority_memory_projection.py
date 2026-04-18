"""Tests for authority-to-memory projection refresher."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from runtime.authority_memory_projection import (
    AUTHORITY_CLASS,
    AuthorityMemoryProjection,
    FkProjection,
    _PROJECTIONS,
)


@dataclass
class _FakeRow(dict):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def __getitem__(self, key: str) -> Any:
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        return super().get(key, default)


@dataclass
class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


@dataclass
class _FakeConnection:
    fetch_responses: list[list[_FakeRow]] = field(default_factory=list)
    executed: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    fetched: list[str] = field(default_factory=list)

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[_FakeRow]:
        self.fetched.append(query)
        if not self.fetch_responses:
            return []
        return self.fetch_responses.pop(0)

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    async def close(self) -> None:
        return None


def test_projections_registered() -> None:
    names = {p.name for p in _PROJECTIONS}
    assert "roadmap_parent_of" in names
    assert "roadmap_resolves_bug" in names
    assert "operator_object_relations_mirror" in names
    assert "workflow_build_intent_implements_build" in names


def test_refresh_upserts_authoritative_rows_only() -> None:
    async def run() -> None:
        # For each of the 4 projections: 1 row to upsert, 0 deactivation candidates.
        fetch_responses: list[list[_FakeRow]] = []
        for _ in _PROJECTIONS:
            fetch_responses.append([
                _FakeRow(source_id="a::1", source_kind="a", source_name="n1",
                         target_id="b::2", target_kind="b", target_name="n2",
                         active=True),
            ])
            fetch_responses.append([])  # deactivation candidates

        conn = _FakeConnection(fetch_responses=fetch_responses)

        async def connect_fn(_env: Any) -> _FakeConnection:
            return conn

        projection = AuthorityMemoryProjection(connect_database=connect_fn)
        result = await projection.refresh_async(as_of=datetime(2026, 4, 18, tzinfo=timezone.utc))

        assert result.total_upserted == len(_PROJECTIONS)
        assert result.total_deactivated == 0
        for proj in _PROJECTIONS:
            assert result.by_projection[proj.name]["upserted"] == 1
        # Every INSERT uses AUTHORITY_CLASS.
        for query, args in conn.executed:
            if "INSERT INTO memory_edges" in query:
                assert AUTHORITY_CLASS in args

    asyncio.run(run())


def test_refresh_deactivates_missing_rows() -> None:
    async def run() -> None:
        fetch_responses: list[list[_FakeRow]] = []
        # Only the first projection: source has 1 row, but 2 existing active rows.
        # One of them is missing from source -> gets deactivated.
        fetch_responses.append([_FakeRow(source_id="a::1", source_kind="a", source_name="n1",
                                          target_id="b::1", target_kind="b", target_name="n2",
                                          active=True)])
        fetch_responses.append([
            _FakeRow(source_id="a::1", target_id="b::1"),
            _FakeRow(source_id="a::stale", target_id="b::stale"),
        ])
        # Remaining three projections: empty source, empty existing.
        for _ in _PROJECTIONS[1:]:
            fetch_responses.append([])
            fetch_responses.append([])

        conn = _FakeConnection(fetch_responses=fetch_responses)

        async def connect_fn(_env: Any) -> _FakeConnection:
            return conn

        projection = AuthorityMemoryProjection(connect_database=connect_fn)
        result = await projection.refresh_async()

        assert result.total_upserted == 1
        assert result.total_deactivated == 1
        assert result.by_projection["roadmap_parent_of"]["deactivated"] == 1
        update_queries = [q for q, _ in conn.executed if "UPDATE memory_edges" in q]
        assert len(update_queries) == 1

    asyncio.run(run())
