from __future__ import annotations

import asyncio

import runtime.claims as runtime_claims


class _FakeTransaction:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeTransaction":
        self._conn.transaction_entries += 1
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._conn.transaction_exits += 1


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_entries = 0
        self.transaction_exits = 0

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def execute(self, query: str, *params: object) -> str:
        self.executed.append((query, params))
        return "OK"


def test_bootstrap_schema_statements_filter_wrappers_and_authority_seed(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime_claims,
        "_schema_statements",
        lambda: (
            "-- header\nBEGIN",
            "CREATE TABLE demo (id INT)",
            "INSERT INTO workflow_claim_lifecycle_transition_authority DEFAULT VALUES",
            "COMMIT",
        ),
    )
    runtime_claims._bootstrap_schema_statements.cache_clear()
    try:
        assert runtime_claims._bootstrap_schema_statements() == ("CREATE TABLE demo (id INT)",)
    finally:
        runtime_claims._bootstrap_schema_statements.cache_clear()


def test_bootstrap_schema_serializes_on_advisory_lock(monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(
        runtime_claims,
        "_bootstrap_schema_statements",
        lambda: ("CREATE TABLE demo (id INT)", "CREATE INDEX demo_idx ON demo (id)"),
    )

    asyncio.run(runtime_claims.ClaimLeaseProposalRuntime().bootstrap_schema(conn))

    assert conn.transaction_entries == 3
    assert conn.transaction_exits == 3
    assert conn.executed[0] == (
        "SELECT pg_advisory_xact_lock($1::bigint)",
        (runtime_claims._CLAIM_RUNTIME_SCHEMA_BOOTSTRAP_LOCK_ID,),
    )
    assert [query for query, _params in conn.executed[1:]] == [
        "CREATE TABLE demo (id INT)",
        "CREATE INDEX demo_idx ON demo (id)",
    ]
