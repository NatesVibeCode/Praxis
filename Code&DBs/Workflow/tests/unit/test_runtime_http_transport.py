from __future__ import annotations

from types import SimpleNamespace

from runtime import http_transport


def test_openai_handler_delegates_to_llm_client(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_call_llm(request):
        captured["endpoint_uri"] = request.endpoint_uri
        captured["api_key"] = request.api_key
        captured["model_slug"] = request.model_slug
        captured["messages"] = request.messages
        captured["protocol_family"] = request.protocol_family
        return SimpleNamespace(content="OPENAI_HANDLER_OK")

    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.setattr(http_transport, "call_llm", _fake_call_llm)

    result = http_transport.get_handler("openai_chat_completions")(
        "Reply with OPENAI_HANDLER_OK",
        model="gpt-4.1",
        max_tokens=64,
        timeout=30,
        api_endpoint="https://api.openai.com/v1/chat/completions",
        api_key_env="OPENAI_API_KEY",
    )

    assert result == "OPENAI_HANDLER_OK"
    assert captured["endpoint_uri"] == "https://api.openai.com/v1/chat/completions"
    assert captured["api_key"] == "openai-test-key"
    assert captured["model_slug"] == "gpt-4.1"
    assert captured["protocol_family"] == "openai_chat_completions"
    assert captured["messages"] == ({"role": "user", "content": "Reply with OPENAI_HANDLER_OK"},)


def test_cursor_background_agent_handler_uses_git_repo_context(monkeypatch) -> None:
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def _fake_run(cmd, *, capture_output, text, timeout):
        assert capture_output is True
        assert text is True
        assert timeout == 10
        joined = tuple(cmd)
        if joined[-2:] == ("rev-parse", "--show-toplevel"):
            return SimpleNamespace(returncode=0, stdout="/tmp/repo\n", stderr="")
        if joined[-3:] == ("remote", "get-url", "origin"):
            return SimpleNamespace(returncode=0, stdout="git@github.com:acme/praxis.git\n", stderr="")
        if joined[-2:] == ("--abbrev-ref", "HEAD"):
            return SimpleNamespace(returncode=0, stdout="main\n", stderr="")
        raise AssertionError(f"unexpected git command: {cmd}")

    def _fake_json_request(*, method, url, api_key, timeout_seconds, body=None):
        calls.append((method, url, body))
        assert api_key == "cursor-test-key"
        assert timeout_seconds == 60
        if url.endswith("/v0/agents"):
            return {"id": "agent_123", "status": "RUNNING"}
        if url.endswith("/v0/agents/agent_123"):
            return {
                "id": "agent_123",
                "status": "FINISHED",
                "summary": "done",
                "target": {"branchName": "cursor/main-1712345678"},
            }
        if url.endswith("/v0/agents/agent_123/conversation"):
            return {
                "messages": [
                    {"type": "assistant_message", "text": "CURSOR_BACKGROUND_AGENT_OK"}
                ]
            }
        raise AssertionError(f"unexpected cursor API url: {url}")

    monkeypatch.setenv("CURSOR_API_KEY", "cursor-test-key")
    monkeypatch.setattr(http_transport.subprocess, "run", _fake_run)
    monkeypatch.setattr(http_transport, "_json_request", _fake_json_request)
    monkeypatch.setattr(http_transport.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(http_transport.time, "time", lambda: 1712345678)

    result = http_transport.get_handler("cursor_background_agent")(
        "Add a README",
        model="auto",
        max_tokens=1024,
        timeout=60,
        api_endpoint="https://api.cursor.com/v0/agents",
        api_key_env="CURSOR_API_KEY",
        workdir="/tmp/repo/subdir",
    )

    assert result == "CURSOR_BACKGROUND_AGENT_OK"
    create_call = calls[0]
    assert create_call[0] == "POST"
    assert create_call[1] == "https://api.cursor.com/v0/agents"
    assert create_call[2] == {
        "prompt": {"text": "Add a README"},
        "source": {
            "repository": "https://github.com/acme/praxis",
            "ref": "main",
        },
        "target": {
            "autoCreatePr": False,
            "branchName": "cursor/main-1712345678",
        },
    }
