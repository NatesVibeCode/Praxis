"""Transport registry: protocol family -> handler + telemetry parser.

Transport handlers:
    (prompt, *, model, max_tokens, timeout, api_endpoint, api_key_env) -> str

Telemetry parsers:
    (raw_json: dict) -> dict with keys: result_text, input_tokens, output_tokens,
    cache_read_tokens, cache_creation_tokens, cost_usd, model, duration_api_ms,
    num_turns, tool_use

Register new protocols with @register (transport) and @register_telemetry (parsing).
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Transport handler registry
# ---------------------------------------------------------------------------

TransportHandler = Callable[..., str]
TelemetryParser = Callable[[dict[str, Any]], dict[str, Any]]
MessageFormatter = Callable[..., list[dict[str, Any]]]

_REGISTRY: dict[str, TransportHandler] = {}
_TELEMETRY_REGISTRY: dict[str, TelemetryParser] = {}
_MESSAGE_REGISTRY: dict[str, MessageFormatter] = {}


def register(protocol_family: str) -> Callable[[TransportHandler], TransportHandler]:
    """Decorator: register a transport handler for a protocol family."""
    def decorator(fn: TransportHandler) -> TransportHandler:
        _REGISTRY[protocol_family] = fn
        return fn
    return decorator


def register_telemetry(protocol_family: str) -> Callable[[TelemetryParser], TelemetryParser]:
    """Decorator: register a telemetry parser for a protocol family."""
    def decorator(fn: TelemetryParser) -> TelemetryParser:
        _TELEMETRY_REGISTRY[protocol_family] = fn
        return fn
    return decorator


def get_handler(protocol_family: str) -> TransportHandler:
    """Look up a registered transport handler. Raises if unknown."""
    handler = _REGISTRY.get(protocol_family)
    if handler is None:
        registered = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise RuntimeError(
            f"No transport handler for protocol '{protocol_family}'. "
            f"Registered: {registered}"
        )
    return handler


def get_telemetry_parser(protocol_family: str) -> TelemetryParser | None:
    """Look up a telemetry parser. Returns None if no parser registered."""
    return _TELEMETRY_REGISTRY.get(protocol_family)


def parse_telemetry(protocol_family: str, raw_json: dict[str, Any]) -> dict[str, Any] | None:
    """Parse telemetry from raw CLI JSON output. Returns None if no parser."""
    parser = _TELEMETRY_REGISTRY.get(protocol_family)
    if parser is None:
        return None
    return parser(raw_json)


def register_message_formatter(protocol_family: str) -> Callable[[MessageFormatter], MessageFormatter]:
    """Decorator: register a tool-call message formatter for a protocol family."""
    def decorator(fn: MessageFormatter) -> MessageFormatter:
        _MESSAGE_REGISTRY[protocol_family] = fn
        return fn
    return decorator


def format_tool_messages(
    protocol_family: str,
    *,
    tool_call_id: str,
    tool_name: str,
    tool_input: Any,
    tool_result_content: str,
) -> list[dict[str, Any]]:
    """Format tool call + result as message dicts for the given protocol.

    Returns a list of messages to append to the conversation.
    Falls back to OpenAI format if no protocol-specific formatter registered.
    """
    formatter = _MESSAGE_REGISTRY.get(protocol_family)
    if formatter is None:
        formatter = _MESSAGE_REGISTRY.get("openai_chat_completions", _format_openai_tool_messages)
    return formatter(
        tool_call_id=tool_call_id,
        tool_name=tool_name,
        tool_input=tool_input,
        tool_result_content=tool_result_content,
    )


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Telemetry parsers (CLI JSON envelope → normalized telemetry dict)
# ---------------------------------------------------------------------------

@register_telemetry("anthropic_messages")
def _parse_anthropic_telemetry(data: dict[str, Any]) -> dict[str, Any]:
    usage = data.get("usage", {})
    model_usage = data.get("modelUsage", {})
    server_tool_use = usage.get("server_tool_use", {})
    model = next(iter(model_usage), "") if model_usage else ""
    return {
        "result_text": data.get("result") or "",
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "cache_read_tokens": usage.get("cache_read_input_tokens", 0),
        "cache_creation_tokens": usage.get("cache_creation_input_tokens", 0),
        "cost_usd": data.get("total_cost_usd", 0.0),
        "model": model,
        "duration_api_ms": data.get("duration_api_ms", 0),
        "num_turns": data.get("num_turns", 0),
        "tool_use": server_tool_use if server_tool_use else {},
    }


@register_telemetry("google_generate_content")
def _parse_google_telemetry(data: dict[str, Any]) -> dict[str, Any]:
    stats = data.get("stats", {})
    models_stats = stats.get("models", {})
    model_name = next(iter(models_stats), "") if models_stats else ""
    input_tokens = 0
    output_tokens = 0
    if models_stats:
        tokens = models_stats.get(model_name, {}).get("tokens", {})
        input_tokens = tokens.get("input", 0) or tokens.get("prompt", 0)
        output_tokens = tokens.get("candidates", 0) or tokens.get("output", 0)
    usage = data.get("usage", {})
    if not input_tokens:
        input_tokens = usage.get("input_tokens", 0)
    if not output_tokens:
        output_tokens = usage.get("output_tokens", 0)
    return {
        "result_text": data.get("response") or "",
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": data.get("total_cost_usd", 0.0),
        "model": model_name or data.get("model", ""),
        "num_turns": stats.get("tools", {}).get("totalCalls", 0),
    }


@register_telemetry("openai_chat_completions")
def _parse_openai_telemetry(data: dict[str, Any]) -> dict[str, Any]:
    usage = data.get("usage", {})
    return {
        "result_text": data.get("result") or data.get("text") or "",
        "input_tokens": usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0) or usage.get("completion_tokens", 0),
        "cost_usd": data.get("total_cost_usd", 0.0),
        "model": data.get("model", ""),
        "num_turns": data.get("num_turns", 0),
    }


# ---------------------------------------------------------------------------
# Message formatters (tool call + result → protocol-specific message dicts)
# ---------------------------------------------------------------------------

@register_message_formatter("anthropic_messages")
def _format_anthropic_tool_messages(
    *, tool_call_id: str, tool_name: str, tool_input: Any, tool_result_content: str,
) -> list[dict[str, Any]]:
    return [
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": tool_call_id, "name": tool_name, "input": tool_input}
        ]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": tool_call_id, "content": tool_result_content}
        ]},
    ]


@register_message_formatter("openai_chat_completions")
def _format_openai_tool_messages(
    *, tool_call_id: str, tool_name: str, tool_input: Any, tool_result_content: str,
) -> list[dict[str, Any]]:
    input_str = json.dumps(tool_input) if not isinstance(tool_input, str) else tool_input
    return [
        {"role": "assistant", "tool_calls": [
            {"id": tool_call_id, "type": "function", "function": {"name": tool_name, "arguments": input_str}}
        ]},
        {"role": "tool", "tool_call_id": tool_call_id, "content": tool_result_content},
    ]
