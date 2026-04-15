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
import re
import subprocess
import time
from typing import Any, Callable

from adapters.http_transport import HTTPTransportError, perform_http_request
from adapters.llm_client import LLMClientError, LLMRequest, call_llm

# ---------------------------------------------------------------------------
# Transport handler registry
# ---------------------------------------------------------------------------

TransportHandler = Callable[..., str]
TelemetryParser = Callable[[dict[str, Any]], dict[str, Any]]
MessageFormatter = Callable[..., list[dict[str, Any]]]

_REGISTRY: dict[str, TransportHandler] = {}
_TELEMETRY_REGISTRY: dict[str, TelemetryParser] = {}
_MESSAGE_REGISTRY: dict[str, MessageFormatter] = {}

_CURSOR_API_BASE = "https://api.cursor.com"
_CURSOR_NONTERMINAL_STATUSES = frozenset({"CREATING", "RUNNING"})
_CURSOR_SUCCESS_STATUSES = frozenset({"FINISHED"})
_CURSOR_FAILURE_STATUSES = frozenset({"ERROR", "EXPIRED"})


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


def _required_api_key(api_key_env: str) -> str:
    value = str(os.environ.get(api_key_env, "")).strip()
    if value:
        return value
    raise RuntimeError(f"missing API credential in {api_key_env}")


def _call_chat_completion_protocol(
    protocol_family: str,
    prompt: str,
    *,
    model: str,
    max_tokens: int,
    timeout: int,
    api_endpoint: str,
    api_key_env: str,
    **_: Any,
) -> str:
    try:
        response = call_llm(
            LLMRequest(
                endpoint_uri=api_endpoint,
                api_key=_required_api_key(api_key_env),
                provider_slug=protocol_family,
                model_slug=model,
                messages=({"role": "user", "content": prompt},),
                max_tokens=max_tokens,
                temperature=0.0,
                protocol_family=protocol_family,
                timeout_seconds=timeout,
                retry_attempts=0,
                retry_backoff_seconds=(),
                retryable_status_codes=(),
            )
        )
    except LLMClientError as exc:
        raise RuntimeError(f"{exc.reason_code}: {exc}") from exc
    return response.content


@register("openai_chat_completions")
def _call_openai_chat_completions(prompt: str, **kwargs: Any) -> str:
    return _call_chat_completion_protocol("openai_chat_completions", prompt, **kwargs)


@register("anthropic_messages")
def _call_anthropic_messages(prompt: str, **kwargs: Any) -> str:
    return _call_chat_completion_protocol("anthropic_messages", prompt, **kwargs)


@register("google_generate_content")
def _call_google_generate_content(prompt: str, **kwargs: Any) -> str:
    return _call_chat_completion_protocol("google_generate_content", prompt, **kwargs)


def _json_request(
    *,
    method: str,
    url: str,
    api_key: str,
    timeout_seconds: int,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {api_key}"}
    payload = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        payload = json.dumps(body).encode("utf-8")
    try:
        response = perform_http_request(
            method=method,
            url=url,
            headers=headers,
            body=payload,
            timeout_seconds=float(timeout_seconds),
        )
    except TimeoutError as exc:
        raise RuntimeError(f"request timed out: {exc}") from exc
    except HTTPTransportError as exc:
        raise RuntimeError(str(exc)) from exc

    raw_text = response.body.decode("utf-8", errors="replace")
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {raw_text[:500]}")
    if not raw_text.strip():
        return {}
    try:
        data = json.loads(raw_text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"invalid JSON response from {url}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"expected JSON object from {url}")
    return data


def _git_output(workdir: str, *args: str) -> str:
    cmd = ["git", "-C", workdir, *args]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {stderr or f'exit {proc.returncode}'}")
    return str(proc.stdout or "").strip()


def _normalize_repository_url(remote_url: str) -> str:
    normalized = remote_url.strip()
    if normalized.startswith("git@") and ":" in normalized:
        host_part, path_part = normalized.split(":", 1)
        normalized = f"https://{host_part.split('@', 1)[1]}/{path_part}"
    elif normalized.startswith("ssh://git@"):
        remainder = normalized[len("ssh://git@") :]
        if "/" in remainder:
            host_part, path_part = remainder.split("/", 1)
            normalized = f"https://{host_part}/{path_part}"
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    return normalized


def _sanitize_branch_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._/-]+", "-", value).strip("-./")
    return cleaned or "agent"


def _cursor_repo_context(workdir: str | None) -> tuple[str, str, str]:
    normalized_workdir = str(workdir or "").strip()
    if not normalized_workdir:
        raise RuntimeError("cursor background-agent transport requires --workdir")
    repo_root = _git_output(normalized_workdir, "rev-parse", "--show-toplevel")
    base_ref = _git_output(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
    if base_ref == "HEAD":
        raise RuntimeError("cursor background-agent transport requires a named git branch")
    remote_url = _git_output(repo_root, "remote", "get-url", "origin")
    repository = _normalize_repository_url(remote_url)
    branch_name = f"cursor/{_sanitize_branch_component(base_ref)}-{int(time.time())}"
    return repository, base_ref, branch_name


def _cursor_launch_body(
    *,
    prompt: str,
    model: str,
    repository: str,
    base_ref: str,
    branch_name: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "prompt": {"text": prompt},
        "source": {
            "repository": repository,
            "ref": base_ref,
        },
        "target": {
            "autoCreatePr": False,
            "branchName": branch_name,
        },
    }
    normalized_model = str(model or "").strip()
    if normalized_model and normalized_model.lower() != "auto":
        payload["model"] = normalized_model
    return payload


def _cursor_status(api_key: str, agent_id: str, *, timeout_seconds: int) -> dict[str, Any]:
    return _json_request(
        method="GET",
        url=f"{_CURSOR_API_BASE}/v0/agents/{agent_id}",
        api_key=api_key,
        timeout_seconds=timeout_seconds,
    )


def _cursor_conversation(api_key: str, agent_id: str, *, timeout_seconds: int) -> dict[str, Any]:
    return _json_request(
        method="GET",
        url=f"{_CURSOR_API_BASE}/v0/agents/{agent_id}/conversation",
        api_key=api_key,
        timeout_seconds=timeout_seconds,
    )


def _cursor_terminal_message(status_payload: dict[str, Any]) -> str:
    summary = str(status_payload.get("summary") or "").strip()
    target = status_payload.get("target")
    if isinstance(target, dict):
        branch_name = str(target.get("branchName") or "").strip()
        pr_url = str(target.get("prUrl") or "").strip()
        extras = [value for value in (summary, f"branch={branch_name}" if branch_name else "", pr_url) if value]
        if extras:
            return "\n".join(extras)
    return summary


@register("cursor_background_agent")
def _call_cursor_background_agent(
    prompt: str,
    *,
    model: str,
    timeout: int,
    api_key_env: str,
    workdir: str | None = None,
    **_: Any,
) -> str:
    api_key = _required_api_key(api_key_env)
    repository, base_ref, branch_name = _cursor_repo_context(workdir)
    created = _json_request(
        method="POST",
        url=f"{_CURSOR_API_BASE}/v0/agents",
        api_key=api_key,
        timeout_seconds=min(timeout, 60),
        body=_cursor_launch_body(
            prompt=prompt,
            model=model,
            repository=repository,
            base_ref=base_ref,
            branch_name=branch_name,
        ),
    )
    agent_id = str(created.get("id") or "").strip()
    if not agent_id:
        raise RuntimeError("cursor background-agent API response did not include an id")

    deadline = time.monotonic() + max(timeout, 30)
    status_payload = created
    status = str(status_payload.get("status") or "").strip().upper()
    while status in _CURSOR_NONTERMINAL_STATUSES:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"cursor background-agent timed out waiting for completion: {agent_id}"
            )
        time.sleep(min(5.0, max(1.0, deadline - time.monotonic())))
        status_payload = _cursor_status(api_key, agent_id, timeout_seconds=min(timeout, 60))
        status = str(status_payload.get("status") or "").strip().upper()

    if status not in _CURSOR_SUCCESS_STATUSES:
        detail = _cursor_terminal_message(status_payload)
        if not detail:
            detail = json.dumps(status_payload, sort_keys=True)
        raise RuntimeError(f"cursor background-agent {agent_id} ended with {status}: {detail}")

    try:
        conversation = _cursor_conversation(api_key, agent_id, timeout_seconds=min(timeout, 60))
    except RuntimeError:
        conversation = {}
    assistant_messages = [
        str(message.get("text") or "").strip()
        for message in conversation.get("messages", [])
        if isinstance(message, dict)
        and str(message.get("type") or "").strip() == "assistant_message"
        and str(message.get("text") or "").strip()
    ]
    if assistant_messages:
        return "\n\n".join(assistant_messages)

    detail = _cursor_terminal_message(status_payload)
    if detail:
        return detail
    return json.dumps(status_payload, sort_keys=True)


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
