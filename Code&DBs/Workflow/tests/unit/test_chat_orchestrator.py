from __future__ import annotations

from types import SimpleNamespace

import pytest

from adapters.llm_client import LLMClientError
import runtime.chat_orchestrator as chat_orchestrator_mod
from runtime.lane_policy import ProviderLanePolicy
from runtime.chat_orchestrator import (
    ChatOrchestrator,
    ResolvedChatRoute,
    _extract_cli_chat_text,
    _extract_cli_error,
    _should_use_cli_fast_path,
)

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


def _lane_policies(*provider_specs: tuple[str, tuple[str, ...]]) -> dict[str, ProviderLanePolicy]:
    return {
        provider: ProviderLanePolicy(
            provider_slug=provider,
            allowed_adapter_types=frozenset(adapter_types),
            overridable=True,
        )
        for provider, adapter_types in provider_specs
    }


class _FakeRouter:
    def __init__(self, _pg) -> None:
        self._pg = _pg

    def resolve_failover_chain(self, _agent_slug: str):
        return self.result


class _FakeChatStore:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.title = "New conversation"
        self.updated = False

    def append_message(self, conversation_id: str, role: str, content: str, **kwargs) -> str:
        message_id = f"msg-{len(self.messages) + 1}"
        self.messages.append(
            {
                "id": message_id,
                "conversation_id": conversation_id,
                "role": role,
                "content": content,
                **kwargs,
            }
        )
        return message_id

    def get_conversation_cost(self, conversation_id: str) -> float:
        return 0.0

    def list_conversation_messages(self, conversation_id: str) -> list[dict[str, object]]:
        return []

    def get_title(self, conversation_id: str) -> str | None:
        return self.title

    def update_title(self, conversation_id: str, title: str) -> None:
        self.title = title

    def touch_updated_at(self, conversation_id: str) -> None:
        self.updated = True


def test_resolve_model_accepts_single_route_chain_entry(monkeypatch) -> None:
    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_http_endpoint",
        lambda provider, model=None: "https://api.openai.com/v1/chat/completions",
    )
    monkeypatch.setattr(
        "runtime.lane_policy.load_provider_lane_policies",
        lambda _pg: _lane_policies(("openai", ("llm_task",))),
    )
    _FakeRouter.result = [
        SimpleNamespace(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="llm_task",
        )
    ]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    provider, model, endpoint, api_key = orchestrator._resolve_model()

    assert provider == "openai"
    assert model == "gpt-5.4"
    assert endpoint == "https://api.openai.com/v1/chat/completions"
    assert api_key == "openai-key"


def test_resolve_route_chain_honors_explicit_model_override(monkeypatch) -> None:
    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_http_endpoint",
        lambda provider, model=None: f"https://{provider}.example/v1/chat",
    )
    monkeypatch.setattr(
        "runtime.lane_policy.load_provider_lane_policies",
        lambda _pg: _lane_policies(
            ("openai", ("llm_task",)),
            ("anthropic", ("llm_task",)),
        ),
    )
    _FakeRouter.result = [
        SimpleNamespace(provider_slug="openai", model_slug="gpt-5.4", adapter_type="llm_task"),
        SimpleNamespace(provider_slug="anthropic", model_slug="claude-sonnet-4-6", adapter_type="llm_task"),
    ]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    routes = orchestrator._resolve_route_chain(model_override="anthropic/claude-sonnet-4-6")

    assert [(route.provider_slug, route.model_slug) for route in routes] == [
        ("anthropic", "claude-sonnet-4-6"),
    ]


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


def test_should_use_cli_fast_path_only_when_no_http_lane_exists() -> None:
    cli_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="cli_llm",
        supports_tool_loop=False,
    )
    http_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )

    assert _should_use_cli_fast_path([cli_route]) is True
    assert _should_use_cli_fast_path([cli_route, http_route]) is False


def test_send_message_prefers_http_lane_when_cli_route_is_sticky(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    cli_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="cli_llm",
        supports_tool_loop=False,
    )
    http_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [cli_route, http_route])
    monkeypatch.setattr(
        orchestrator,
        "_resolve_model",
        lambda routes=None: ("openai", "gpt-5.4", "https://api.openai.com/v1/chat/completions", "openai-key"),
    )
    monkeypatch.setattr(
        orchestrator,
        "_send_via_cli",
        lambda routes, messages: (_ for _ in ()).throw(AssertionError("CLI fast path should be skipped")),
    )
    monkeypatch.setattr(
        chat_orchestrator_mod,
        "call_llm",
        lambda request: SimpleNamespace(
            content="HTTP lane response",
            provider_slug="openai",
            model="gpt-5.4",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=12,
            tool_calls=(),
        ),
    )

    result = orchestrator.send_message("conv-1", "Please use tools if needed")

    assert result["content"] == "HTTP lane response"
    assert result["model_used"] == "openai/gpt-5.4"
    assert [message["role"] for message in store.messages] == ["user", "assistant"]


def test_send_message_fails_over_http_route_on_rate_limit(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    openai_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )
    anthropic_route = ResolvedChatRoute(
        provider_slug="anthropic",
        model_slug="claude-sonnet-4-6",
        adapter_type="llm_task",
        endpoint_uri="https://api.anthropic.com/v1/messages",
        api_key="anthropic-key",
        supports_tool_loop=True,
    )
    calls: list[str] = []

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [openai_route, anthropic_route])
    monkeypatch.setattr(chat_orchestrator_mod, "_load_chat_tools", lambda: ([], lambda *_args: {}))
    chat_orchestrator_mod._RECENTLY_FAILED_ROUTES.clear()

    def _fake_call_llm(request):
        calls.append(f"{request.provider_slug}/{request.model_slug}")
        if request.provider_slug == "openai":
            raise LLMClientError("llm_client.rate_limited", "rate limit exhausted")
        return SimpleNamespace(
            content="Fallback response",
            provider_slug="anthropic",
            model="claude-sonnet-4-6",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=25,
            tool_calls=(),
        )

    monkeypatch.setattr(chat_orchestrator_mod, "call_llm", _fake_call_llm)

    try:
        result = orchestrator.send_message("conv-1", "Please answer")
    finally:
        chat_orchestrator_mod._RECENTLY_FAILED_ROUTES.clear()

    assert calls == ["openai/gpt-5.4", "anthropic/claude-sonnet-4-6"]
    assert result["content"] == "Fallback response"
    assert result["model_used"] == "anthropic/claude-sonnet-4-6"
    assert [message["role"] for message in store.messages] == ["user", "assistant"]


def test_send_message_leaves_output_budget_unclamped_for_moon_context(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    http_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [http_route])
    monkeypatch.setattr(chat_orchestrator_mod, "_load_chat_tools", lambda: ([], lambda *_args: {}))

    def _fake_call_llm(request):
        captured["max_tokens"] = request.max_tokens
        return SimpleNamespace(
            content="Moon reviewed it",
            provider_slug="openai",
            model="gpt-5.4",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=12,
            tool_calls=(),
        )

    monkeypatch.setattr(chat_orchestrator_mod, "call_llm", _fake_call_llm)

    orchestrator.send_message(
        "conv-1",
        "Review this build",
        selection_context=[{"kind": "moon_context", "workflow_id": "wf_123"}],
    )

    assert captured["max_tokens"] is None


def test_send_message_uses_route_max_tokens_when_route_declares_contract(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    kimi_route = ResolvedChatRoute(
        provider_slug="openrouter",
        model_slug="moonshotai/kimi-k2.6",
        adapter_type="llm_task",
        endpoint_uri="https://openrouter.ai/api/v1/chat/completions",
        api_key="openrouter-key",
        supports_tool_loop=True,
        max_tokens=32768,
        temperature=0.2,
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [kimi_route])
    monkeypatch.setattr(chat_orchestrator_mod, "_load_chat_tools", lambda: ([], lambda *_args: {}))

    def _fake_call_llm(request):
        captured["provider_slug"] = request.provider_slug
        captured["model_slug"] = request.model_slug
        captured["max_tokens"] = request.max_tokens
        captured["temperature"] = request.temperature
        return SimpleNamespace(
            content="Kimi reviewed it",
            provider_slug="openrouter",
            model="moonshotai/kimi-k2.6",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=12,
            tool_calls=(),
        )

    monkeypatch.setattr(chat_orchestrator_mod, "call_llm", _fake_call_llm)

    orchestrator.send_message("conv-1", "Review this build")

    assert captured == {
        "provider_slug": "openrouter",
        "model_slug": "moonshotai/kimi-k2.6",
        "max_tokens": 32768,
        "temperature": 0.2,
    }


def test_send_message_honors_explicit_max_tokens_over_moon_default(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    http_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [http_route])
    monkeypatch.setattr(chat_orchestrator_mod, "_load_chat_tools", lambda: ([], lambda *_args: {}))

    def _fake_call_llm(request):
        captured["max_tokens"] = request.max_tokens
        return SimpleNamespace(
            content="Custom budget",
            provider_slug="openai",
            model="gpt-5.4",
            usage={"input_tokens": 1, "output_tokens": 1},
            latency_ms=12,
            tool_calls=(),
        )

    monkeypatch.setattr(chat_orchestrator_mod, "call_llm", _fake_call_llm)

    orchestrator.send_message(
        "conv-1",
        "Review this build",
        selection_context=[{"kind": "moon_context", "workflow_id": "wf_123"}],
        max_tokens=12000,
    )

    assert captured["max_tokens"] == 12000


def test_resolve_route_chain_reads_endpoint_from_registry_without_hardcoded_fallback(monkeypatch) -> None:
    """Non-openai providers must resolve endpoints via provider_cli_profiles, not a hardcoded dict."""

    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_http_endpoint",
        lambda provider, model=None: {
            "google": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            "anthropic": "https://api.anthropic.com/v1/messages",
        }.get(provider),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator.resolve_binary",
        lambda _provider: None,
        raising=False,
    )
    monkeypatch.setattr(
        "runtime.lane_policy.load_provider_lane_policies",
        lambda _pg: _lane_policies(
            ("google", ("llm_task",)),
            ("anthropic", ("llm_task",)),
        ),
    )
    _FakeRouter.result = [
        SimpleNamespace(provider_slug="google", model_slug="gemini-2.5-pro", adapter_type="llm_task"),
        SimpleNamespace(provider_slug="anthropic", model_slug="claude-sonnet-4-5", adapter_type="llm_task"),
    ]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)
    routes = orchestrator._resolve_route_chain()

    by_provider = {route.provider_slug: route.endpoint_uri for route in routes}
    assert by_provider["google"] == "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    assert by_provider["anthropic"] == "https://api.anthropic.com/v1/messages"


def test_resolve_route_chain_rejects_provider_with_no_registered_endpoint(monkeypatch) -> None:
    """An unknown provider falls through as no_registered_endpoint instead of silently using openai."""

    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_api_key",
        lambda provider, *, required=True: f"{provider}-key",
    )
    monkeypatch.setattr(
        "runtime.chat_orchestrator._resolve_http_endpoint",
        lambda provider, model=None: None,
    )
    monkeypatch.setattr(
        "runtime.lane_policy.load_provider_lane_policies",
        lambda _pg: _lane_policies(("unregistered", ("llm_task",))),
    )
    _FakeRouter.result = [
        SimpleNamespace(provider_slug="unregistered", model_slug="model-x", adapter_type="llm_task"),
    ]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    try:
        orchestrator._resolve_route_chain()
    except RuntimeError as exc:
        assert "no_registered_endpoint" in str(exc)
    else:  # pragma: no cover - must not pass
        raise AssertionError("expected RuntimeError with no_registered_endpoint rejection")


def test_resolve_route_chain_fails_closed_when_provider_lane_policy_authority_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        chat_orchestrator_mod.importlib,
        "import_module",
        lambda _name: SimpleNamespace(TaskTypeRouter=_FakeRouter),
    )
    monkeypatch.setattr(
        "runtime.lane_policy.load_provider_lane_policies",
        lambda _pg: {},
    )
    _FakeRouter.result = [
        SimpleNamespace(provider_slug="openai", model_slug="gpt-5.4", adapter_type="llm_task"),
    ]

    orchestrator = ChatOrchestrator(object(), _REPO_ROOT)

    with pytest.raises(RuntimeError, match="provider lane policy authority returned no active rows"):
        orchestrator._resolve_route_chain()


def test_chat_orchestrator_has_no_hardcoded_endpoints_constant() -> None:
    """Guardrail: a future edit must not resurrect the hardcoded endpoint dict."""

    import inspect

    source = inspect.getsource(chat_orchestrator_mod)
    assert "_DEFAULT_ENDPOINTS" not in source, "hardcoded endpoint dict resurfaced in chat_orchestrator"
    assert "resolve_api_endpoint" in source, "registry-backed endpoint resolution must stay wired"


def test_send_message_streaming_prefers_http_lane_when_cli_route_is_sticky(monkeypatch) -> None:
    store = _FakeChatStore()
    orchestrator = ChatOrchestrator(object(), _REPO_ROOT, chat_store=store)
    cli_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="cli_llm",
        supports_tool_loop=False,
    )
    http_route = ResolvedChatRoute(
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="llm_task",
        endpoint_uri="https://api.openai.com/v1/chat/completions",
        api_key="openai-key",
        supports_tool_loop=True,
    )

    monkeypatch.setattr(orchestrator, "_resolve_route_chain", lambda **_kwargs: [cli_route, http_route])
    monkeypatch.setattr(
        orchestrator,
        "_resolve_model",
        lambda routes=None: ("openai", "gpt-5.4", "https://api.openai.com/v1/chat/completions", "openai-key"),
    )
    monkeypatch.setattr(
        orchestrator,
        "_send_via_cli",
        lambda routes, messages: (_ for _ in ()).throw(AssertionError("CLI fast path should be skipped")),
    )
    monkeypatch.setattr(
        chat_orchestrator_mod,
        "call_llm_streaming",
        lambda request: iter(
            (
                {"type": "text_delta", "text": "HTTP "},
                {"type": "text_delta", "text": "stream"},
                {"type": "message_stop", "stop_reason": "end_turn"},
            )
        ),
    )

    events = list(orchestrator.send_message_streaming("conv-1", "Please use tools if needed"))

    assert events[0] == {"event": "text_delta", "data": {"text": "HTTP "}}
    assert events[1] == {"event": "text_delta", "data": {"text": "stream"}}
    assert events[-1]["event"] == "done"
    assert events[-1]["data"]["model_used"] == "openai/gpt-5.4"
