from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


_SANDBOX_CLIENT_PATH = Path(__file__).resolve().parents[2] / "bin" / "praxis_sandbox_client.py"


def _load_sandbox_client():
    spec = importlib.util.spec_from_file_location("praxis_sandbox_client", _SANDBOX_CLIENT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _install_tools_list_response(monkeypatch: pytest.MonkeyPatch, module, tools: list[dict[str, object]]) -> None:
    payload = {"jsonrpc": "2.0", "id": "1", "result": {"tools": tools}}

    def _fake_urlopen(request, timeout=0):
        del request, timeout
        return _FakeResponse(payload)

    monkeypatch.setattr(module.urllib.request, "urlopen", _fake_urlopen)


def test_workflow_tools_search_renders_matching_tool(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    module = _load_sandbox_client()
    monkeypatch.setenv(module._ENV_URL, "http://mcp.local/mcp")
    monkeypatch.setenv(module._ENV_TOKEN, "test-token")
    _install_tools_list_response(
        monkeypatch,
        module,
        [
            {
                "name": "praxis_query",
                "description": "Ask any question about the system in plain English.",
                "inputSchema": {"type": "object"},
            },
            {
                "name": "praxis_discover",
                "description": "Search for existing code by behavior before building something new.",
                "inputSchema": {"type": "object"},
            },
        ],
    )

    rc = module.main(["workflow", "tools", "search", "query"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "praxis_query" in rendered
    assert "Ask any question about the system in plain English." in rendered
    assert "praxis_discover" not in rendered


def test_workflow_tools_search_exact_mode_limits_to_direct_matches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_sandbox_client()
    monkeypatch.setenv(module._ENV_URL, "http://mcp.local/mcp")
    monkeypatch.setenv(module._ENV_TOKEN, "test-token")
    _install_tools_list_response(
        monkeypatch,
        module,
        [
            {
                "name": "praxis_query",
                "description": "Ask any question about the system in plain English.",
                "inputSchema": {"type": "object"},
            },
            {
                "name": "praxis_query_debug",
                "description": "Debug query routing with extra detail.",
                "inputSchema": {"type": "object"},
            },
        ],
    )

    rc = module.main(["workflow", "tools", "search", "praxis_query", "--exact", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert [tool["name"] for tool in payload] == ["praxis_query"]


def test_workflow_tools_describe_renders_schema(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_sandbox_client()
    monkeypatch.setenv(module._ENV_URL, "http://mcp.local/mcp")
    monkeypatch.setenv(module._ENV_TOKEN, "test-token")
    _install_tools_list_response(
        monkeypatch,
        module,
        [
            {
                "name": "praxis_query",
                "description": "Ask any question about the system in plain English.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"question": {"type": "string"}},
                    "required": ["question"],
                },
            }
        ],
    )

    rc = module.main(["workflow", "tools", "describe", "query"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "tool: praxis_query" in rendered
    assert "Ask any question about the system in plain English." in rendered
    assert '"question"' in rendered


def test_workflow_tools_describe_reports_unknown_tool(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_sandbox_client()
    monkeypatch.setenv(module._ENV_URL, "http://mcp.local/mcp")
    monkeypatch.setenv(module._ENV_TOKEN, "test-token")
    _install_tools_list_response(monkeypatch, module, [])

    with pytest.raises(SystemExit) as excinfo:
        module.main(["workflow", "tools", "describe", "missing"])

    assert excinfo.value.code == 2
    assert "unknown tool: missing" in capsys.readouterr().err


def test_help_only_teaches_canonical_workflow_tools_shape(capsys: pytest.CaptureFixture[str]) -> None:
    module = _load_sandbox_client()

    rc = module.main(["--help"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "praxis workflow tools call <tool_name>" in rendered
    assert "Short form" not in rendered
    assert "praxis submit_code_change" not in rendered
    assert "praxis query" not in rendered


def test_short_form_commands_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_sandbox_client()
    monkeypatch.setenv(module._ENV_URL, "http://mcp.local/mcp")
    monkeypatch.setenv(module._ENV_TOKEN, "test-token")

    with pytest.raises(SystemExit) as excinfo:
        module.main(["query", "what is failing?"])

    assert excinfo.value.code == 1
    assert "unknown command shape" in capsys.readouterr().err
