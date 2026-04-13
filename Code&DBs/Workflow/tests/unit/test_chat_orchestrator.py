from __future__ import annotations

from types import SimpleNamespace

import runtime.chat_orchestrator as chat_orchestrator_mod
from runtime.chat_orchestrator import ChatOrchestrator, _extract_cli_chat_text, _extract_cli_error

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


class _FakeRouter:
    def __init__(self, _pg) -> None:
        self._pg = _pg

    def resolve(self, _agent_slug: str):
        return self.result


def test_resolve_model_accepts_single_route_decision(monkeypatch) -> None:
    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    _FakeRouter.result = SimpleNamespace(provider_slug="openai", model_slug="gpt-5.4")

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    provider, model, endpoint, api_key = orchestrator._resolve_model()

    assert provider == "openai"
    assert model == "gpt-5.4"
    assert endpoint == "https://api.openai.com/v1/chat/completions"
    assert api_key == "openai-key"


def test_resolve_model_accepts_legacy_decision_list(monkeypatch) -> None:
    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    _FakeRouter.result = [SimpleNamespace(provider_slug="anthropic", model_slug="claude-sonnet-4-5")]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    provider, model, endpoint, api_key = orchestrator._resolve_model()

    assert provider == "anthropic"
    assert model == "claude-sonnet-4-5"
    assert endpoint == "https://api.anthropic.com/v1/messages"
    assert api_key == "anthropic-key"


def test_extract_cli_chat_text_reads_codex_ndjson_agent_message() -> None:
    stdout = "\n".join(
        [
            "WARN state db mismatch",
            '{"type":"thread.started","thread_id":"abc"}',
            '{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"READY"}}',
            '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}',
        ]
    )

    assert _extract_cli_chat_text(stdout) == "READY"


def test_extract_cli_error_reads_provider_json_error_payload() -> None:
    stdout = (
        '{"type":"result","subtype":"success","is_error":true,'
        '"result":"Failed to authenticate","stop_reason":"stop_sequence"}'
    )

    assert _extract_cli_error(stdout, "") == "Failed to authenticate"
