from __future__ import annotations

from datetime import datetime, timezone
from importlib import reload

import runtime.cost_tracker as cost_tracker_mod


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))
        return [
            {
                "run_id": "run-1",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "cost_usd": 1.25,
                "input_tokens": 10,
                "output_tokens": 20,
                "recorded_at": datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc),
            }
        ]


class _Result:
    run_id = "run-1"
    provider_slug = "openai"
    model_slug = "gpt-5.4"
    outputs = {"total_cost_usd": 1.25, "usage": {"input_tokens": 10, "output_tokens": 20}}


def test_get_cost_tracker_is_lazy(monkeypatch) -> None:
    tracker_module = reload(cost_tracker_mod)

    def _fail() -> object:
        raise AssertionError("connection should not be acquired during singleton construction")

    monkeypatch.setattr(tracker_module, "ensure_postgres_available", _fail)
    monkeypatch.setattr(tracker_module, "_COST_TRACKER", None)

    tracker = tracker_module.get_cost_tracker()

    assert tracker is tracker_module.get_cost_tracker()


def test_record_cost_skips_when_connection_unavailable() -> None:
    tracker = cost_tracker_mod.CostTracker(conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("db offline")))

    assert tracker.record_cost(_Result()) is None
    assert tracker.summary()["record_count"] == 0


def test_record_cost_connects_on_demand() -> None:
    conn = _Conn()
    calls: list[str] = []

    def _connect() -> _Conn:
        calls.append("connect")
        return conn

    tracker = cost_tracker_mod.CostTracker(conn_factory=_connect)

    record = tracker.record_cost(_Result())

    assert record is not None
    assert record.run_id == "run-1"
    assert calls == ["connect"]
    assert len(conn.calls) == 1
