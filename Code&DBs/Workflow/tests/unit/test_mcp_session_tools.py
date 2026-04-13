from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from surfaces.mcp.tools import session as session_tools
from surfaces.mcp.tools.session import tool_praxis_decompose as tool_dag_decompose


def test_tool_dag_decompose_returns_estimate_and_files() -> None:
    result = tool_dag_decompose(
        {
            "objective": "Fix the dashboard",
            "scope_files": ["Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"],
        }
    )

    assert "error" not in result
    assert result["total_sprints"] == 1
    assert result["sprints"][0]["estimate_minutes"] == 10
    assert result["sprints"][0]["files"] == [
        "Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"
    ]


class _HeartbeatStatusConn:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def execute_script(self, sql: str) -> None:
        self.last_schema_sql = sql

    def fetchrow(self, query: str, *args):
        del args
        normalized = " ".join(query.split())
        if "FROM heartbeat_status_current" not in normalized:
            raise AssertionError(f"unexpected query: {normalized}")
        return {
            "cycle_id": str(self.payload["cycle_id"]),
            "started_at": datetime(2026, 4, 9, 23, 26, 12, tzinfo=timezone.utc),
            "completed_at": datetime(2026, 4, 9, 23, 39, 16, tzinfo=timezone.utc),
            "total_findings": 0,
            "total_actions": 0,
            "total_errors": int(self.payload.get("errors", 0)),
            "status_payload": self.payload,
            "updated_at": datetime(2026, 4, 9, 23, 39, 16, tzinfo=timezone.utc),
        }


def test_tool_dag_heartbeat_status_returns_summary(monkeypatch) -> None:
    payload = {
        "cycle_id": "cycle-1",
        "started_at": "2026-04-09T23:26:12.783492",
        "completed_at": "2026-04-09T23:39:16.040271",
        "duration_ms": 783257.0,
        "module_count": 11,
        "errors": 0,
    }
    fake_subs = SimpleNamespace(get_pg_conn=lambda: _HeartbeatStatusConn(payload))
    monkeypatch.setattr(session_tools, "_subs", fake_subs)

    result = session_tools.tool_praxis_heartbeat({"action": "status"})

    assert result["latest_cycle"] == "cycle-1"
    assert result["summary"]["cycle_id"] == "cycle-1"
    assert result["summary"] == payload


def test_tool_dag_heartbeat_status_with_errors(monkeypatch) -> None:
    payload = {
        "cycle_id": "cycle-2",
        "started_at": "2026-04-09T23:26:12.783492",
        "completed_at": "2026-04-09T23:39:16.040271",
        "duration_ms": 783257.0,
        "module_count": 11,
        "errors": 1,
        "errored_modules": [{"module": "memory_sync", "error": "conn failed"}],
    }
    fake_subs = SimpleNamespace(get_pg_conn=lambda: _HeartbeatStatusConn(payload))
    monkeypatch.setattr(session_tools, "_subs", fake_subs)

    result = session_tools.tool_praxis_heartbeat({"action": "status"})

    assert result["summary"]["errors"] == 1
    assert result["summary"]["errored_modules"][0]["module"] == "memory_sync"
