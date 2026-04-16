from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from registry.config_registry import ConfigRegistry


class _FakeConn:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((sql, args))
        return self.rows

    async def execute(self, sql: str, *args: object) -> None:
        self.execute_calls.append((sql, args))

    async def close(self) -> None:
        self.closed = True


class _FakeAsyncPGModule:
    def __init__(
        self,
        conn: _FakeConn,
        *,
        expected_dsn: str = "postgresql://test@localhost:5432/praxis_test",
    ) -> None:
        self._conn = conn
        self._expected_dsn = expected_dsn

    async def connect(self, dsn: str) -> _FakeConn:
        assert dsn == self._expected_dsn
        return self._conn


def test_get_requires_explicit_workflow_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    registry = ConfigRegistry()

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL"):
        registry.get("context.budget_ratio")


def test_get_rejects_non_postgres_workflow_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "sqlite:///tmp/workflow.db")
    registry = ConfigRegistry()

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL"):
        registry.get("context.budget_ratio")


def test_set_requires_explicit_workflow_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    registry = ConfigRegistry()

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL"):
        registry.set("context.preview_chars", 1024)


def test_seed_defaults_is_no_longer_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://test@localhost:5432/praxis_test")
    registry = ConfigRegistry()

    with pytest.raises(RuntimeError, match="no longer seeds fallback defaults"):
        registry.seed_defaults({"context.preview_chars": (1024, "context", "preview chars")})


def test_load_from_db_does_not_bootstrap_platform_config_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeConn(
        rows=[
            {
                "config_key": "context.budget_ratio",
                "config_value": "0.5",
                "value_type": "float",
                "category": "context",
                "description": "budget ratio",
                "min_value": None,
                "max_value": None,
                "updated_at": object(),
            }
        ]
    )
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://test@localhost:5432/praxis_test")
    monkeypatch.setitem(
        sys.modules,
        "asyncpg",
        _FakeAsyncPGModule(fake_conn),
    )

    registry = ConfigRegistry()
    assert registry.get("context.budget_ratio") == 0.5
    assert fake_conn.execute_calls == []
    assert len(fake_conn.fetch_calls) == 1
    assert "FROM platform_config" in fake_conn.fetch_calls[0][0]
    assert fake_conn.closed is True


def test_set_writes_only_to_existing_platform_config_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeConn()
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://test@localhost:5432/praxis_test")
    monkeypatch.setitem(sys.modules, "asyncpg", _FakeAsyncPGModule(fake_conn))

    registry = ConfigRegistry()
    registry.set("context.preview_chars", 1024)

    assert fake_conn.execute_calls
    assert all("CREATE TABLE" not in sql for sql, _args in fake_conn.execute_calls)
    assert any(sql.lstrip().startswith("INSERT INTO platform_config") for sql, _args in fake_conn.execute_calls)
    assert fake_conn.closed is True


def test_get_normalizes_missing_user_in_workflow_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeConn(
        rows=[
            {
                "config_key": "context.budget_ratio",
                "config_value": "0.5",
                "value_type": "float",
                "category": "context",
                "description": "budget ratio",
                "min_value": None,
                "max_value": None,
                "updated_at": object(),
            }
        ]
    )
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis_test")
    monkeypatch.setitem(
        sys.modules,
        "asyncpg",
        _FakeAsyncPGModule(
            fake_conn,
            expected_dsn="postgresql://localhost:5432/praxis_test",
        ),
    )

    registry = ConfigRegistry()

    assert registry.get("context.budget_ratio") == 0.5
    assert fake_conn.closed is True
