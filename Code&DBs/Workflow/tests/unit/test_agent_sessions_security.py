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


@pytest.fixture(autouse=True)
def isolated_agent_sessions(monkeypatch, tmp_path):
    monkeypatch.setattr(agent_sessions, "AGENTS_DIR", tmp_path / "agents")
    monkeypatch.delenv("PRAXIS_API_TOKEN", raising=False)
    agent_sessions._agent_locks.clear()
    agent_sessions._agent_queues.clear()
    agent_sessions._agent_processes.clear()
    agent_sessions._active_turns.clear()
    agent_sessions._claimed_turns.clear()


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
