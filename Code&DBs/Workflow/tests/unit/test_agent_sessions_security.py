"""Security boundary tests for the agent sessions API."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import agent_sessions


class _FakeAgentSessionConn:
    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, object]] = {}
        self.events: list[dict[str, object]] = []

    def execute(self, sql: str, *args):
        if "INSERT INTO agent_sessions" in sql:
            agent_id = args[0]
            row = {
                "session_id": agent_id,
                "external_session_id": args[6],
                "display_title": args[7],
                "agent_slug": args[4],
                "principal_ref": args[8],
                "workspace_ref": args[9],
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
                if "external_session_id = $2" in sql:
                    row["external_session_id"] = args[1]
                    row["agent_slug"] = args[2]
                else:
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


def _auth_headers(token: str = "session-token") -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_agent_sessions_fail_closed_without_public_api_token() -> None:
    with TestClient(agent_sessions.app) as client:
        response = client.get("/agents")

    assert response.status_code == 503
    assert response.json()["detail"]["error_code"] == "agent_sessions_auth_not_configured"


def test_agent_sessions_trust_tailscale_operator_mode_without_bearer_token(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_TRUST_TAILSCALE", "1")

    with TestClient(agent_sessions.app) as client:
        response = client.get("/agents")

    assert response.status_code == 200
    assert response.json() == []


def test_agent_sessions_trust_tailscale_client_ip_without_bearer_token() -> None:
    with TestClient(agent_sessions.app, client=("100.80.157.47", 48080)) as client:
        response = client.get("/agents")

    assert response.status_code == 200
    assert response.json() == []


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


def test_agent_session_runner_uses_explicit_session_id_and_noninteractive_permission_mode() -> None:
    cmd = agent_sessions._build_claude_command(
        "00000000-0000-4000-8000-000000000001",
        "hello",
        {"PRAXIS_AGENT_PERMISSION_MODE": "auto"},
    )

    assert "--session-id" in cmd
    assert "--resume" not in cmd
    assert cmd[cmd.index("--permission-mode") + 1] == "auto"
    assert cmd[-1] == "hello"


def test_agent_session_runner_builds_codex_resume_command() -> None:
    cmd = agent_sessions._build_codex_command(
        "019dbcd1-b64c-7013-8ee4-f1e6a099df1e",
        "hello",
        Path("/tmp/reply.txt"),
        {"PRAXIS_AGENT_CODEX_SANDBOX": "read-only"},
    )

    assert cmd[:4] == ["codex", "exec", "resume", "019dbcd1-b64c-7013-8ee4-f1e6a099df1e"]
    assert "--json" in cmd
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    assert cmd[-1] == "hello"


def test_agent_session_runner_extracts_codex_thread_id() -> None:
    assert (
        agent_sessions._thread_id_from_events(
            [{"type": "thread.started", "thread_id": "019dbcd1-b64c-7013-8ee4-f1e6a099df1e"}],
            "fallback",
        )
        == "019dbcd1-b64c-7013-8ee4-f1e6a099df1e"
    )


def test_agent_session_runner_accepts_openrouter_provider() -> None:
    assert agent_sessions._cli_provider("openrouter") == "openrouter"


def test_agent_session_openrouter_default_is_deepseek_route() -> None:
    assert agent_sessions._openrouter_model({}) == "deepseek/deepseek-v4-pro"


def test_openrouter_messages_preserve_prior_turns(isolated_agent_sessions) -> None:
    agent_id = "00000000-0000-4000-8000-000000000003"
    agent_sessions.create_interactive_agent_session(
        isolated_agent_sessions,
        agent_id=agent_id,
        cli_session_id="session",
        title="api",
        provider_slug="openrouter",
        principal_ref="operator:nate",
        workspace_ref="praxis.default",
    )
    agent_sessions.append_interactive_agent_event(
        isolated_agent_sessions,
        agent_id=agent_id,
        event_kind="user.prompt",
        text_content="first",
    )
    agent_sessions.append_interactive_agent_event(
        isolated_agent_sessions,
        agent_id=agent_id,
        event_kind="assistant.reply",
        text_content="second",
    )

    messages = agent_sessions._openrouter_messages(
        isolated_agent_sessions,
        agent_id=agent_id,
        prompt="third",
    )

    assert messages[-3:] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "second"},
        {"role": "user", "content": "third"},
    ]


def test_send_message_dispatches_to_single_together_provider(monkeypatch, isolated_agent_sessions) -> None:
    monkeypatch.setenv("PRAXIS_API_TOKEN", "session-token")
    captured: dict[str, object] = {}

    async def _fake_run_turn(
        agent_id: str,
        session_id: str,
        prompt: str,
        *,
        provider_slug: str,
        pg_conn=None,
        permission_mode=None,
    ):
        captured.update(
            {
                "agent_id": agent_id,
                "session_id": session_id,
                "prompt": prompt,
                "provider_slug": provider_slug,
                "permission_mode": permission_mode,
                "pg_conn": pg_conn,
            }
        )
        return "reply", [{"type": "assistant", "message": {"content": "reply"}}], 0, session_id

    monkeypatch.setattr(agent_sessions, "_run_turn", _fake_run_turn)

    with TestClient(agent_sessions.app) as client:
        create = client.post(
            "/agents",
            headers=_auth_headers(),
            json={"title": "DeepSeek lane", "provider": "codex"},
        )
        assert create.status_code == 200
        assert create.json()["provider"] == "together"
        agent_id = create.json()["agent_id"]

        response = client.post(
            f"/agents/{agent_id}/messages",
            headers=_auth_headers(),
            json={"prompt": "inspect Praxis", "permission_mode": "plan_only"},
        )

    assert response.status_code == 200
    assert response.json()["provider"] == "together"
    assert captured["provider_slug"] == "together"
    assert captured["prompt"] == "inspect Praxis"
    assert captured["permission_mode"] == "plan_only"
    assert captured["pg_conn"] is isolated_agent_sessions


def test_console_tool_call_protocol_extracts_json() -> None:
    call = agent_sessions._extract_praxis_tool_call(
        '{"praxis_tool_call":{"tool":"praxis_model_access_control_matrix","input":{"job_type":"chat"}}}'
    )

    assert call == {
        "tool": "praxis_model_access_control_matrix",
        "input": {"job_type": "chat"},
    }


def test_console_tool_call_rejects_unlisted_tool() -> None:
    result = agent_sessions._run_praxis_tool_for_console(
        {"tool": "praxis_operator_architecture_policy", "input": {"action": "record"}}
    )

    assert result["ok"] is False
    assert result["error_code"] == "praxis_tool_not_allowed"


def test_together_reply_cleanup_removes_timestamp_and_word_digit_junk() -> None:
    dirty = "The10 matrix is02:38:55 AM on and12 ready."

    assert agent_sessions._clean_api_reply_text(dirty, provider_slug="together") == "The matrix is on and ready."


def test_praxis_lookup_context_is_internal_not_visible_chat(monkeypatch, isolated_agent_sessions) -> None:
    monkeypatch.setattr("adapters.keychain.resolve_secret", lambda _name, env=None: "test-key")
    captured_messages: list[list[dict[str, str]]] = []

    def _fake_chat_completion_json_request(**kwargs):
        captured_messages.append(kwargs["messages"])
        if len(captured_messages) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"praxis_tool_call":{"tool":"praxis_health","input":{}}}'
                            )
                        }
                    }
                ]
            }
        return {"choices": [{"message": {"content": "Praxis is healthy."}}]}

    monkeypatch.setattr(agent_sessions, "_chat_completion_json_request", _fake_chat_completion_json_request)
    monkeypatch.setattr(
        agent_sessions,
        "_run_praxis_tool_for_console",
        lambda _call: {
            "ok": True,
            "tool": "praxis_health",
            "stdout": {
                "status": "ok",
                "projected_at": "2026-04-27 00:31:22+00:00",
                "operation_receipt": {"receipt_id": "noisy-machine-metadata"},
            },
        },
    )

    agent_id = str(uuid4())
    reply, turn_events, exit_code, _session_id = asyncio.run(
        agent_sessions._run_api_provider_turn(
            agent_id=agent_id,
            session_id="session-1",
            prompt="Check Praxis health",
            provider_slug="together",
            pg_conn=isolated_agent_sessions,
        )
    )

    assert reply == "Praxis is healthy."
    assert exit_code == 0
    event_types = [event["type"] for event in turn_events]
    assert "praxis.tool_call" in event_types
    assert "praxis.tool_result" in event_types
    assert event_types[-1] == "assistant"
    stages = [event.get("stage") for event in turn_events if event["type"] == "turn.stage"]
    assert stages == [
        "provider_key",
        "provider_request",
        "provider_response",
        "assistant_text",
        "final_provider_request",
        "final_provider_response",
        "final_assistant_text",
    ]
    assert len(captured_messages) == 2
    second_call_messages = captured_messages[1]
    assert not any(
        message["role"] == "user" and "praxis_tool_result" in message["content"]
        for message in second_call_messages
    )
    assert not any(
        message["role"] == "assistant" and "praxis_tool_call" in message["content"]
        for message in second_call_messages
    )
    internal_messages = [
        message
        for message in second_call_messages
        if message["role"] == "system"
        and "INTERNAL PRAXIS LOOKUP CONTEXT" in message["content"]
    ]
    assert internal_messages
    assert "projected_at" not in internal_messages[0]["content"]
    assert "operation_receipt" not in internal_messages[0]["content"]


def test_empty_provider_reply_is_visible_error(monkeypatch, isolated_agent_sessions) -> None:
    monkeypatch.setattr("adapters.keychain.resolve_secret", lambda _name, env=None: "test-key")
    monkeypatch.setattr(
        agent_sessions,
        "_chat_completion_json_request",
        lambda **_kwargs: {"choices": [{"message": {"content": ""}}]},
    )

    agent_id = str(uuid4())
    reply, turn_events, exit_code, _session_id = asyncio.run(
        agent_sessions._run_api_provider_turn(
            agent_id=agent_id,
            session_id="session-1",
            prompt="Hi",
            provider_slug="together",
            pg_conn=isolated_agent_sessions,
        )
    )

    assert exit_code == 1
    assert turn_events[-1]["type"] == "error"
    assert turn_events[-1]["error_code"] == "agent_provider_failed"
    assert turn_events[-1]["stage"] == "provider_response"
    stages = [event.get("stage") for event in turn_events if event["type"] == "turn.stage"]
    assert stages == ["provider_key", "provider_request", "provider_response"]
    assert "empty assistant reply" in reply


def test_praxis_context_includes_operator_chat_doctrine(isolated_agent_sessions) -> None:
    agent_sessions._PRAXIS_CONTEXT_CACHE["text"] = None
    agent_sessions._PRAXIS_CONTEXT_CACHE["expires_at"] = 0.0

    context = agent_sessions._build_praxis_context(isolated_agent_sessions)

    assert "Neo operator doctrine" in context
    assert "high-taste operator" in context
    assert "canonical operator surface is `praxis workflow`" in context
    assert "Lead with the verdict" in context


def test_agent_session_runner_strips_blank_anthropic_api_env() -> None:
    env = agent_sessions._claude_subprocess_env(
        {
            "ANTHROPIC_API_KEY": "",
            "ANTHROPIC_AUTH_TOKEN": " ",
            "CLAUDE_CODE_OAUTH_TOKEN": "oauth-token",
        }
    )

    assert "ANTHROPIC_API_KEY" not in env
    assert "ANTHROPIC_AUTH_TOKEN" not in env
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "oauth-token"
