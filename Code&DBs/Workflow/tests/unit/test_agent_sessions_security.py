"""Security boundary tests for the agent sessions API."""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import agent_sessions
from runtime.capability.sessions import hash_secret


class _FakeAgentSessionConn:
    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, object]] = {}
        self.events: list[dict[str, object]] = []
        self.mobile_sessions: dict[str, dict[str, object]] = {}

    def add_mobile_session(self, token_secret: str, *, session_id: str = "00000000-0000-4000-8000-000000000001") -> None:
        self.mobile_sessions[hash_secret(token_secret)] = {
            "session_id": session_id,
            "principal_ref": "mobile:nate",
            "device_id": "00000000-0000-4000-8000-000000000002",
            "created_at": "2026-04-23T00:00:00+00:00",
            "expires_at": "2026-04-24T00:00:00+00:00",
            "last_step_up_at": "2026-04-23T00:00:00+00:00",
            "budget_limit": 25,
            "budget_used": 0,
        }

    def execute(self, sql: str, *args):
        if "FROM mobile_sessions" in sql:
            row = self.mobile_sessions.get(args[0])
            return [] if row is None else [dict(row)]

        if "UPDATE mobile_sessions" in sql:
            session_id = args[0]
            for row in self.mobile_sessions.values():
                if row["session_id"] == session_id:
                    row["budget_used"] = int(row.get("budget_used") or 0) + int(args[1])
                    return [{"session_id": session_id, "principal_ref": row["principal_ref"], "budget_used": row["budget_used"], "budget_event_id": "budget-event"}]
            return []

        if "INSERT INTO agent_sessions" in sql:
            agent_id = args[0]
            row = {
                "session_id": agent_id,
                "external_session_id": args[5],
                "display_title": args[6],
                "principal_ref": args[7],
                "workspace_ref": args[8],
                "status": "active",
                "created_at": "2026-04-23T00:00:00+00:00",
                "last_activity_at": "2026-04-23T00:00:00+00:00",
                "heartbeat_at": "2026-04-23T00:00:00+00:00",
                "revoked_at": None,
            }
            self.sessions[agent_id] = row
            return [dict(row)]

        if "INSERT INTO agent_session_events" in sql:
            agent_id = args[0]
            row = self.sessions.get(agent_id)
            if row is None or row.get("revoked_at") is not None:
                return []
            event = {
                "event_id": len(self.events) + 1,
                "session_id": agent_id,
                "event_kind": args[2],
                "payload_json": args[3],
                "text_content": args[4],
                "created_at": "2026-04-23T00:00:00+00:00",
            }
            self.events.append(event)
            return [{"event_id": event["event_id"]}]

        if "FROM agent_sessions" in sql and "WHERE session_id = $1" in sql:
            row = self.sessions.get(args[0])
            if row is None or row.get("revoked_at") is not None:
                return []
            return [dict(row)]

        if "FROM agent_sessions" in sql and "ORDER BY last_activity_at" in sql:
            return [dict(row) for row in self.sessions.values() if row.get("revoked_at") is None]

        if "FROM agent_session_events" in sql:
            return [dict(row) for row in self.events if row["session_id"] == args[0]]

        if "UPDATE agent_sessions" in sql:
            row = self.sessions.get(args[0])
            if row is not None:
                row["status"] = "terminated"
                row["revoked_at"] = "2026-04-23T00:00:00+00:00"
                row["revoked_by"] = args[1]
                row["revoke_reason"] = args[2]
            return []

        return []


@pytest.fixture(autouse=True)
def isolated_agent_sessions(monkeypatch, tmp_path):
    fake_conn = _FakeAgentSessionConn()
    monkeypatch.setattr(agent_sessions, "AGENTS_DIR", tmp_path / "agents")
    monkeypatch.setattr(agent_sessions.app.state, "pg_conn_factory", lambda: fake_conn, raising=False)
    monkeypatch.delenv("PRAXIS_API_TOKEN", raising=False)
    agent_sessions._agent_locks.clear()
    agent_sessions._agent_queues.clear()
    agent_sessions._agent_processes.clear()
    agent_sessions._active_turns.clear()
    agent_sessions._claimed_turns.clear()
    yield fake_conn
    if hasattr(agent_sessions.app.state, "pg_conn_factory"):
        try:
            delattr(agent_sessions.app.state, "pg_conn_factory")
        except KeyError:
            pass


def _auth_headers(token: str = "session-token") -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_agent_sessions_fail_closed_without_public_api_token() -> None:
    with TestClient(agent_sessions.app) as client:
        response = client.get("/agents")

    assert response.status_code == 503
    assert response.json()["detail"]["error_code"] == "agent_sessions_auth_not_configured"


def test_agent_sessions_require_valid_bearer_token(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "session-token")

    with TestClient(agent_sessions.app) as client:
        missing = client.get("/agents")
        rejected = client.get("/agents", headers=_auth_headers("wrong-token"))
        accepted = client.get("/agents", headers=_auth_headers())

    assert missing.status_code == 401
    assert missing.json()["detail"]["error_code"] == "agent_sessions_auth_required"
    assert rejected.status_code == 403
    assert rejected.json()["detail"]["error_code"] == "agent_sessions_auth_rejected"
    assert accepted.status_code == 200
    assert accepted.json() == []


def test_create_agent_uses_server_generated_canonical_uuid(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "session-token")

    with TestClient(agent_sessions.app) as client:
        response = client.post(
            "/agents",
            headers=_auth_headers(),
            json={"title": "Security boundary"},
        )

    assert response.status_code == 200
    payload = response.json()
    agent_id = payload["agent_id"]
    assert str(UUID(agent_id, version=4)) == agent_id
    agent_dir = agent_sessions._agent_dir(agent_id)
    assert agent_dir.parent == agent_sessions.AGENTS_DIR.resolve()
    assert (agent_dir / "meta.json").exists()
    assert (agent_dir / "messages.jsonl").exists()


def test_invalid_agent_id_rejected_before_filesystem_lookup(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "session-token")

    with TestClient(agent_sessions.app) as client:
        response = client.get("/agents/not-a-uuid/messages", headers=_auth_headers())

    assert response.status_code == 400
    assert response.json()["detail"]["error_code"] == "invalid_agent_id"
    assert not agent_sessions.AGENTS_DIR.exists()

    with pytest.raises(HTTPException) as exc_info:
        agent_sessions._agent_dir("../outside")
    assert exc_info.value.status_code == 400
    assert not agent_sessions.AGENTS_DIR.parent.joinpath("outside").exists()


def test_agent_sessions_accept_mobile_session_cookie_without_public_api_token(isolated_agent_sessions) -> None:
    isolated_agent_sessions.add_mobile_session("mobile-secret")

    with TestClient(agent_sessions.app) as client:
        response = client.get(
            "/agents",
            cookies={"praxis_mobile_session": "mobile-secret"},
        )

    assert response.status_code == 200
    assert response.json() == []
