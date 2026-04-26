"""Thin HTTP client for LLM chat completion APIs.

Uses stdlib transport primitives — no SDK dependency. Supports OpenAI and
Anthropic request/response shapes, including tool calling and streaming.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterator

from .http_transport import (
    HTTPResponse,
    HTTPStreamResponse,
    HTTPTransportCancelled,
    HTTPTransportError,
    open_http_stream,
    perform_http_request,
)
from registry.provider_execution_registry import resolve_adapter_config, resolve_api_protocol_family
from runtime.integrations.rate_limiter import (
    RateLimitAcquireTimeout,
    acquire_for_provider,
)

if TYPE_CHECKING:
    from .deterministic import DeterministicExecutionControl

class LLMClientError(RuntimeError):
    """Raised when an LLM API call fails."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code


# Anthropic /v1/messages REQUIRES max_tokens; for that family we fall back to
# a high ceiling when the caller doesn't set one. OpenAI / Google omit the
# field entirely when unset, so the provider uses the model's own default.
# Capping max_tokens at a low number (we used to default to 4096) silently
# truncates reasoning models like DeepSeek-V4-Pro: chain-of-thought consumes
# the budget and `content` comes back empty, which then surfaces downstream
# as a JSON parse error. Default OFF; opt in only when a caller deliberately
# wants a small-output cap.
_ANTHROPIC_FALLBACK_MAX_TOKENS = int(
    os.environ.get("PRAXIS_ANTHROPIC_FALLBACK_MAX_TOKENS", "16384")
)
_DEFAULT_HTTP_TIMEOUT = int(os.environ.get("PRAXIS_HTTP_TIMEOUT", "120"))
_HTTP_RETRIES = int(os.environ.get("PRAXIS_HTTP_RETRIES", "2"))
_HTTP_RETRY_BACKOFF = (2, 5)  # seconds between retries


@dataclass(frozen=True, slots=True)
class LLMRequest:
    """Normalized LLM chat completion request."""

    endpoint_uri: str
    api_key: str
    provider_slug: str
    model_slug: str
    messages: tuple[dict[str, Any], ...]
    # None ⇒ omit `max_tokens` from the request body (OpenAI / Google) so the
    # provider uses the model's natural ceiling. Anthropic family substitutes
    # `_ANTHROPIC_FALLBACK_MAX_TOKENS` since the API requires the field.
    max_tokens: int | None = None
    temperature: float = 0.0
    tools: tuple[dict[str, Any], ...] | None = None
    system_prompt: str | None = None
    protocol_family: str | None = None
    timeout_seconds: int | None = None
    retry_attempts: int | None = None
    retry_backoff_seconds: tuple[int, ...] | None = None
    retryable_status_codes: tuple[int, ...] | None = None
    execution_control: DeterministicExecutionControl | None = None
    cache_static_prefix: bool = False


@dataclass(frozen=True, slots=True)
class ToolCall:
    """A single tool call from the LLM."""
    id: str
    name: str
    input: dict[str, Any]


@dataclass(frozen=True, slots=True)
class LLMResponse:
    """Normalized LLM chat completion response."""

    content: str
    model: str
    provider_slug: str
    usage: dict[str, int]
    raw_response: dict[str, Any]
    latency_ms: int
    status_code: int
    tool_calls: tuple[ToolCall, ...] = ()
    stop_reason: str | None = None


_OPENAI_CHAT_FAMILY = "openai_chat_completions"
_ANTHROPIC_MESSAGES_FAMILY = "anthropic_messages"
_GOOGLE_GENERATE_CONTENT_FAMILY = "google_generate_content"


# ---------------------------------------------------------------------------
# Request body builders
# ---------------------------------------------------------------------------

def _build_openai_body(request: LLMRequest) -> dict[str, Any]:
    messages = list(request.messages)

    # Inject system prompt as first message if provided
    if request.system_prompt:
        messages = [{"role": "system", "content": request.system_prompt}] + [
            m for m in messages if m.get("role") != "system"
        ]

    body: dict[str, Any] = {
        "model": request.model_slug,
        "messages": messages,
        "temperature": request.temperature,
    }
    # OpenAI / Together / DeepSeek omit max_tokens when unset, so the provider
    # uses the model's natural ceiling. Capping at 4096 truncated DeepSeek-V4
    # reasoning into empty `content`.
    if request.max_tokens is not None:
        body["max_tokens"] = request.max_tokens

    if request.tools:
        body["tools"] = [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("input_schema", t.get("parameters", {})),
                },
            }
            for t in request.tools
        ]

    return body


def _build_anthropic_body(request: LLMRequest) -> dict[str, Any]:
    # Anthropic uses top-level `system`, not a system message in messages array
    messages = [m for m in request.messages if m.get("role") != "system"]
    system_parts = [m["content"] for m in request.messages if m.get("role") == "system"]
    if request.system_prompt:
        system_parts.insert(0, request.system_prompt)

    # Anthropic /v1/messages REQUIRES max_tokens, so substitute a high fallback
    # when the caller hasn't picked one.
    anthropic_max_tokens = (
        request.max_tokens
        if request.max_tokens is not None
        else _ANTHROPIC_FALLBACK_MAX_TOKENS
    )
    body: dict[str, Any] = {
        "model": request.model_slug,
        "messages": messages,
        "max_tokens": anthropic_max_tokens,
        "temperature": request.temperature,
    }

    if system_parts:
        joined_system = "\n\n".join(system_parts)
        if request.cache_static_prefix:
            body["system"] = [
                {
                    "type": "text",
                    "text": joined_system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            body["system"] = joined_system

    if request.tools:
        tools_list = [
            {
                "name": t["name"],
                "description": t.get("description", ""),
                "input_schema": t.get("input_schema", t.get("parameters", {})),
            }
            for t in request.tools
        ]
        if request.cache_static_prefix and not system_parts and tools_list:
            tools_list[-1]["cache_control"] = {"type": "ephemeral"}
        body["tools"] = tools_list

    return body


def _build_google_body(request: LLMRequest) -> dict[str, Any]:
    contents: list[dict[str, Any]] = []
    for message in request.messages:
        role = str(message.get("role") or "user")
        if role == "system":
            continue
        contents.append(
            {
                "role": "model" if role == "assistant" else "user",
                "parts": [{"text": str(message.get("content") or "")}],
            }
        )

    generation_config: dict[str, Any] = {"temperature": request.temperature}
    if request.max_tokens is not None:
        generation_config["maxOutputTokens"] = request.max_tokens
    body: dict[str, Any] = {
        "contents": contents,
        "generationConfig": generation_config,
    }

    if request.system_prompt:
        body["systemInstruction"] = {"parts": [{"text": request.system_prompt}]}

    if request.tools:
        body["tools"] = [
            {
                "functionDeclarations": [
                    {
                        "name": t["name"],
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", t.get("parameters", {})),
                    }
                    for t in request.tools
                ]
            }
        ]

    return body


def _request_protocol_family(request: LLMRequest) -> str | None:
    family = (request.protocol_family or resolve_api_protocol_family(request.provider_slug) or "").strip()
    if family:
        return family
    return None


def _require_protocol_family(request: LLMRequest) -> str:
    family = _request_protocol_family(request)
    if family:
        return family
    raise LLMClientError(
        "llm_client.unsupported_protocol_family",
        f"Provider '{request.provider_slug}' has no configured protocol family",
    )


def _build_headers(request: LLMRequest) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    family = _require_protocol_family(request)
    if family == _ANTHROPIC_MESSAGES_FAMILY:
        headers["x-api-key"] = request.api_key
        headers["anthropic-version"] = "2023-06-01"
    elif family == _GOOGLE_GENERATE_CONTENT_FAMILY:
        headers["x-goog-api-key"] = request.api_key
    elif family == _OPENAI_CHAT_FAMILY:
        headers["Authorization"] = f"Bearer {request.api_key}"
    else:
        raise LLMClientError(
            "llm_client.unsupported_protocol_family",
            f"unsupported protocol family: {family}",
        )
    return headers


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

def _parse_openai_response(data: dict[str, Any]) -> tuple[str, dict[str, int], tuple[ToolCall, ...], str | None]:
    choices = data.get("choices", [])
    if not choices:
        raise LLMClientError(
            "llm_client.response_parse_error",
            "OpenAI response contained no choices",
        )

    message = choices[0].get("message", {})
    content = message.get("content") or ""
    finish_reason = choices[0].get("finish_reason")

    # Parse tool calls
    tool_calls: list[ToolCall] = []
    raw_tool_calls = message.get("tool_calls", [])
    for tc in raw_tool_calls:
        try:
            args = json.loads(tc["function"]["arguments"]) if isinstance(tc["function"]["arguments"], str) else tc["function"]["arguments"]
        except (json.JSONDecodeError, KeyError):
            args = {}
        tool_calls.append(ToolCall(
            id=tc.get("id", ""),
            name=tc["function"]["name"],
            input=args,
        ))

    usage = data.get("usage", {})
    details = usage.get("prompt_tokens_details") or {}
    parsed_usage = {
        "prompt_tokens": usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
        # Cached prefix tokens (OpenAI / Together / DeepSeek prompt cache).
        # 0 when the provider doesn't report cache hits or the prefix didn't match.
        "cached_tokens": int(details.get("cached_tokens") or 0),
    }

    stop_reason = "tool_use" if finish_reason == "tool_calls" else "end_turn" if finish_reason == "stop" else finish_reason
    return content, parsed_usage, tuple(tool_calls), stop_reason


def _parse_anthropic_response(data: dict[str, Any]) -> tuple[str, dict[str, int], tuple[ToolCall, ...], str | None]:
    content_blocks = data.get("content", [])
    if not content_blocks:
        raise LLMClientError(
            "llm_client.response_parse_error",
            "Anthropic response contained no content blocks",
        )

    # Anthropic can return mixed content: text blocks AND tool_use blocks
    text_parts: list[str] = []
    tool_calls: list[ToolCall] = []

    for block in content_blocks:
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
        elif block.get("type") == "tool_use":
            tool_calls.append(ToolCall(
                id=block.get("id", ""),
                name=block.get("name", ""),
                input=block.get("input", {}),
            ))

    content = "\n".join(text_parts)
    usage = data.get("usage", {})
    parsed_usage = {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
    }

    stop_reason = data.get("stop_reason")  # "end_turn" or "tool_use"
    return content, parsed_usage, tuple(tool_calls), stop_reason


def _parse_google_response(data: dict[str, Any]) -> tuple[str, dict[str, int], tuple[ToolCall, ...], str | None]:
    candidates = data.get("candidates", [])
    if not candidates:
        raise LLMClientError(
            "llm_client.response_parse_error",
            "Google response contained no candidates",
        )

    first_candidate = candidates[0]
    content = first_candidate.get("content", {})
    parts = content.get("parts", []) if isinstance(content, dict) else []
    text_parts: list[str] = []
    tool_calls: list[ToolCall] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        if "text" in part:
            text_parts.append(str(part.get("text") or ""))
        function_call = part.get("functionCall")
        if isinstance(function_call, dict):
            tool_calls.append(
                ToolCall(
                    id=str(function_call.get("name") or ""),
                    name=str(function_call.get("name") or ""),
                    input=dict(function_call.get("args") or {}),
                )
            )

    usage = data.get("usageMetadata", {})
    parsed_usage = {
        "prompt_tokens": usage.get("promptTokenCount", 0),
        "completion_tokens": usage.get("candidatesTokenCount", 0),
        "total_tokens": usage.get("totalTokenCount", 0),
    }
    stop_reason = first_candidate.get("finishReason")
    return "\n".join(text_parts), parsed_usage, tuple(tool_calls), stop_reason


# ---------------------------------------------------------------------------
# Main API call
# ---------------------------------------------------------------------------

def _request_timeout_seconds(request: LLMRequest) -> int:
    timeout = request.timeout_seconds
    if timeout is None or timeout <= 0:
        return _DEFAULT_HTTP_TIMEOUT
    return timeout


def _request_retry_attempts(request: LLMRequest) -> int:
    retries = request.retry_attempts
    if retries is None or retries < 0:
        return _HTTP_RETRIES
    return retries


def _request_retry_backoff_seconds(request: LLMRequest) -> tuple[int, ...]:
    if not request.retry_backoff_seconds:
        return _HTTP_RETRY_BACKOFF
    return request.retry_backoff_seconds


def _request_retryable_status_codes(request: LLMRequest) -> tuple[int, ...]:
    if request.retryable_status_codes is None:
        codes = resolve_adapter_config("llm_http.retryable_status_codes", [408, 429, 500, 502, 503, 504])
        return tuple(int(c) for c in codes)
    return request.retryable_status_codes


def _is_timeout_error(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    text = str(exc).lower()
    if "timed out" in text or "timeout" in text:
        return True
    reason = getattr(exc, "reason", None)
    if reason is not None:
        reason_text = str(reason).lower()
        if "timed out" in reason_text or "timeout" in reason_text:
            return True
    return False


def _cancelled_client_error() -> LLMClientError:
    return LLMClientError("llm_client.cancelled", "request cancelled")


def _throttle_for_provider(request: LLMRequest, timeout_seconds: int) -> None:
    """Wait on the provider's rate-limit bucket before dispatching the request.

    No-op when no global throttle registry is installed or when the provider
    has no rate_limit_configs row. Raises LLMClientError("llm_client.rate_limited")
    on timeout so callers see a clean reason_code instead of a bucket error.
    """
    try:
        acquire_for_provider(
            request.provider_slug,
            max_wait_seconds=min(float(timeout_seconds), 30.0),
        )
    except RateLimitAcquireTimeout as exc:
        raise LLMClientError(
            "llm_client.rate_limited",
            str(exc),
        ) from exc


def _perform_http_request(
    *,
    request: LLMRequest,
    body_bytes: bytes,
    headers: dict[str, str],
    timeout_seconds: int,
) -> HTTPResponse:
    _throttle_for_provider(request, timeout_seconds)
    return perform_http_request(
        method="POST",
        url=request.endpoint_uri,
        headers=headers,
        body=body_bytes,
        timeout_seconds=timeout_seconds,
        execution_control=request.execution_control,
    )


def _open_streaming_http_request(
    *,
    request: LLMRequest,
    body_bytes: bytes,
    headers: dict[str, str],
    timeout_seconds: int,
) -> HTTPStreamResponse:
    _throttle_for_provider(request, timeout_seconds)
    return open_http_stream(
        method="POST",
        url=request.endpoint_uri,
        headers=headers,
        body=body_bytes,
        timeout_seconds=timeout_seconds,
        execution_control=request.execution_control,
    )


def _wait_backoff_or_cancel(
    *,
    request: LLMRequest,
    backoff_seconds: int,
) -> None:
    if backoff_seconds <= 0:
        return
    if request.execution_control is None:
        time.sleep(backoff_seconds)
        return
    if request.execution_control.wait_for_cancel(timeout=backoff_seconds):
        raise _cancelled_client_error()

def call_llm(request: LLMRequest) -> LLMResponse:
    """Call an LLM chat completion API and return the response.

    Supports Anthropic and OpenAI, including tool calling.
    """

    family = _require_protocol_family(request)
    if family == _ANTHROPIC_MESSAGES_FAMILY:
        body = _build_anthropic_body(request)
    elif family == _GOOGLE_GENERATE_CONTENT_FAMILY:
        body = _build_google_body(request)
    elif family == _OPENAI_CHAT_FAMILY:
        body = _build_openai_body(request)
    else:
        raise LLMClientError(
            "llm_client.unsupported_protocol_family",
            f"unsupported protocol family: {family}",
        )

    headers = _build_headers(request)
    body_bytes = json.dumps(body).encode("utf-8")
    timeout_seconds = _request_timeout_seconds(request)
    retry_attempts = _request_retry_attempts(request)
    retry_backoff_seconds = _request_retry_backoff_seconds(request)
    retryable_status_codes = set(_request_retryable_status_codes(request))

    start_ns = time.monotonic_ns()
    last_exc: Exception | None = None
    status_code: int = 0
    response_bytes: bytes = b""
    for attempt in range(retry_attempts + 1):
        if request.execution_control is not None and request.execution_control.cancel_requested():
            raise _cancelled_client_error()
        try:
            response = _perform_http_request(
                request=request,
                body_bytes=body_bytes,
                headers=headers,
                timeout_seconds=timeout_seconds,
            )
            status_code = response.status_code
            response_bytes = response.body
            if status_code >= 400:
                error_body = response_bytes.decode("utf-8", errors="replace")
                if status_code not in retryable_status_codes and 400 <= status_code < 500:
                    raise LLMClientError(
                        "llm_client.http_error",
                        f"HTTP {status_code}: {error_body[:500]}",
                        status_code=status_code,
                    )
                last_exc = LLMClientError(
                    "llm_client.http_error",
                    f"HTTP {status_code}: {error_body[:500]}",
                    status_code=status_code,
                )
            else:
                last_exc = None
                break
        except HTTPTransportCancelled:
            raise _cancelled_client_error()
        except TimeoutError as exc:
            last_exc = LLMClientError(
                "llm_client.timeout",
                f"request timed out: {exc}",
            )
            last_exc.__cause__ = exc
        except HTTPTransportError as exc:
            if _is_timeout_error(exc):
                last_exc = LLMClientError(
                    "llm_client.timeout",
                    f"request timed out: {exc}",
                )
            else:
                last_exc = LLMClientError(
                    "llm_client.network_error",
                    f"network error: {exc}",
                )
            last_exc.__cause__ = exc
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            # Don't retry client errors (4xx) unless the contract explicitly allows them.
            if exc.code not in retryable_status_codes and 400 <= exc.code < 500:
                raise LLMClientError(
                    "llm_client.http_error",
                    f"HTTP {exc.code}: {error_body[:500]}",
                    status_code=exc.code,
                ) from exc
            last_exc = LLMClientError(
                "llm_client.http_error",
                f"HTTP {exc.code}: {error_body[:500]}",
                status_code=exc.code,
            )
            last_exc.__cause__ = exc
        except (urllib.error.URLError, OSError) as exc:
            if _is_timeout_error(exc):
                last_exc = LLMClientError(
                    "llm_client.timeout",
                    f"request timed out: {exc}",
                )
            else:
                last_exc = LLMClientError(
                    "llm_client.network_error",
                    f"network error: {exc}",
                )
            last_exc.__cause__ = exc

        if attempt < retry_attempts:
            backoff = retry_backoff_seconds[min(attempt, len(retry_backoff_seconds) - 1)]
            _wait_backoff_or_cancel(request=request, backoff_seconds=backoff)

    if last_exc is not None:
        raise last_exc

    latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000

    try:
        data = json.loads(response_bytes)
    except (json.JSONDecodeError, ValueError) as exc:
        raise LLMClientError(
            "llm_client.response_parse_error",
            f"invalid JSON response: {exc}",
            status_code=status_code,
        ) from exc

    if family == _ANTHROPIC_MESSAGES_FAMILY:
        content, usage, tool_calls, stop_reason = _parse_anthropic_response(data)
    elif family == _GOOGLE_GENERATE_CONTENT_FAMILY:
        content, usage, tool_calls, stop_reason = _parse_google_response(data)
    else:
        content, usage, tool_calls, stop_reason = _parse_openai_response(data)

    return LLMResponse(
        content=content,
        model=data.get("model", request.model_slug),
        provider_slug=request.provider_slug,
        usage=usage,
        raw_response=data,
        latency_ms=latency_ms,
        status_code=status_code,
        tool_calls=tool_calls,
        stop_reason=stop_reason,
    )


# ---------------------------------------------------------------------------
# Streaming API call
# ---------------------------------------------------------------------------

def call_llm_streaming(request: LLMRequest) -> Iterator[dict[str, Any]]:
    """Call an LLM API with streaming enabled. Yields event dicts.

    Event types:
      {"type": "text_delta", "text": "..."}
      {"type": "tool_call_start", "id": "...", "name": "..."}
      {"type": "tool_call_delta", "id": "...", "input_json": "..."}
      {"type": "tool_call_end", "id": "...", "input": {...}}
      {"type": "message_stop", "stop_reason": "...", "usage": {...}}
      {"type": "error", "message": "..."}
    """

    family = _request_protocol_family(request)
    if family == _ANTHROPIC_MESSAGES_FAMILY:
        body = _build_anthropic_body(request)
        event_parser = _parse_anthropic_stream_event
    elif family == _OPENAI_CHAT_FAMILY:
        body = _build_openai_body(request)
        event_parser = _parse_openai_stream_event
    else:
        yield {
            "type": "error",
            "message": f"streaming unsupported for protocol family: {family or 'unknown'}",
        }
        return

    body["stream"] = True
    headers = _build_headers(request)
    body_bytes = json.dumps(body).encode("utf-8")
    timeout_seconds = _request_timeout_seconds(request)
    if request.execution_control is not None and request.execution_control.cancel_requested():
        yield {"type": "error", "message": "cancelled"}
        return

    try:
        response = _open_streaming_http_request(
            request=request,
            body_bytes=body_bytes,
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
    except HTTPTransportCancelled:
        yield {"type": "error", "message": "cancelled"}
        return
    except TimeoutError as exc:
        yield {"type": "error", "message": f"timeout: {exc}"}
        return
    except HTTPTransportError as exc:
        if _is_timeout_error(exc):
            yield {"type": "error", "message": f"timeout: {exc}"}
        else:
            yield {"type": "error", "message": f"network error: {exc}"}
        return

    # Parse SSE stream
    tool_call_buffers: dict[str, dict] = {}  # id -> {name, input_json_parts}

    try:
        if response.status_code >= 400:
            error_body = b""
            try:
                error_body = b"".join(response.iter_chunks())
            except HTTPTransportCancelled:
                yield {"type": "error", "message": "cancelled"}
                return
            except TimeoutError as exc:
                yield {"type": "error", "message": f"timeout: {exc}"}
                return
            except HTTPTransportError as exc:
                if _is_timeout_error(exc):
                    yield {"type": "error", "message": f"timeout: {exc}"}
                else:
                    yield {"type": "error", "message": f"network error: {exc}"}
                return
            error_text = error_body.decode("utf-8", errors="replace")
            if response.status_code == 408:
                yield {"type": "error", "message": f"timeout: HTTP {response.status_code}: {error_text[:500]}"}
            else:
                yield {"type": "error", "message": f"HTTP {response.status_code}: {error_text[:500]}"}
            return

        for line in response.iter_lines():
            line_str = line.decode("utf-8", errors="replace").strip()

            if not line_str or line_str.startswith(":"):
                continue

            if line_str.startswith("data: "):
                data_str = line_str[6:]
                if data_str == "[DONE]":
                    # Flush any pending tool calls
                    for tc_id, tc_buf in tool_call_buffers.items():
                        try:
                            input_obj = json.loads("".join(tc_buf["input_json_parts"]))
                        except json.JSONDecodeError:
                            input_obj = {}
                        yield {"type": "tool_call_end", "id": tc_id, "name": tc_buf["name"], "input": input_obj}
                    yield {"type": "message_stop", "stop_reason": "end_turn", "usage": {}}
                    return

                try:
                    event = json.loads(data_str)
                except json.JSONDecodeError:
                    continue

                yield from event_parser(event, tool_call_buffers)
    except HTTPTransportCancelled:
        yield {"type": "error", "message": "cancelled"}
        return
    except TimeoutError as exc:
        yield {"type": "error", "message": f"timeout: {exc}"}
        return
    except HTTPTransportError as exc:
        if _is_timeout_error(exc):
            yield {"type": "error", "message": f"timeout: {exc}"}
        else:
            yield {"type": "error", "message": f"network error: {exc}"}
        return
    finally:
        response.close()


def _parse_anthropic_stream_event(event: dict, tool_buffers: dict) -> Iterator[dict]:
    """Parse a single Anthropic SSE event."""
    event_type = event.get("type", "")

    if event_type == "content_block_start":
        block = event.get("content_block", {})
        if block.get("type") == "tool_use":
            tc_id = block.get("id", "")
            tool_buffers[tc_id] = {"name": block.get("name", ""), "input_json_parts": []}
            yield {"type": "tool_call_start", "id": tc_id, "name": block.get("name", "")}

    elif event_type == "content_block_delta":
        delta = event.get("delta", {})
        if delta.get("type") == "text_delta":
            yield {"type": "text_delta", "text": delta.get("text", "")}
        elif delta.get("type") == "input_json_delta":
            # Tool call input streaming
            partial = delta.get("partial_json", "")
            # Find which tool call this belongs to (last started)
            for tc_id in reversed(list(tool_buffers.keys())):
                tool_buffers[tc_id]["input_json_parts"].append(partial)
                yield {"type": "tool_call_delta", "id": tc_id, "input_json": partial}
                break

    elif event_type == "content_block_stop":
        idx = event.get("index", 0)
        # Check if this was a tool call block
        for tc_id, buf in list(tool_buffers.items()):
            if buf["input_json_parts"]:
                try:
                    input_obj = json.loads("".join(buf["input_json_parts"]))
                except json.JSONDecodeError:
                    input_obj = {}
                yield {"type": "tool_call_end", "id": tc_id, "name": buf["name"], "input": input_obj}
                del tool_buffers[tc_id]
                break

    elif event_type == "message_stop":
        yield {"type": "message_stop", "stop_reason": event.get("message", {}).get("stop_reason", "end_turn"), "usage": event.get("message", {}).get("usage", {})}

    elif event_type == "message_delta":
        delta = event.get("delta", {})
        stop_reason = delta.get("stop_reason")
        usage = event.get("usage", {})
        if stop_reason:
            yield {"type": "message_stop", "stop_reason": stop_reason, "usage": usage}


def _parse_openai_stream_event(event: dict, tool_buffers: dict) -> Iterator[dict]:
    """Parse a single OpenAI SSE event."""
    choices = event.get("choices", [])
    if not choices:
        return

    delta = choices[0].get("delta", {})
    finish_reason = choices[0].get("finish_reason")

    # Text content
    if delta.get("content"):
        yield {"type": "text_delta", "text": delta["content"]}

    # Tool calls
    for tc in delta.get("tool_calls", []):
        tc_idx = str(tc.get("index", 0))
        if tc.get("function", {}).get("name"):
            # New tool call
            tc_id = tc.get("id", tc_idx)
            tool_buffers[tc_id] = {"name": tc["function"]["name"], "input_json_parts": []}
            yield {"type": "tool_call_start", "id": tc_id, "name": tc["function"]["name"]}
        if tc.get("function", {}).get("arguments"):
            # Streaming arguments
            tc_id = tc.get("id", tc_idx)
            if tc_id not in tool_buffers:
                # Find by index
                for k in tool_buffers:
                    tc_id = k
                    break
            if tc_id in tool_buffers:
                tool_buffers[tc_id]["input_json_parts"].append(tc["function"]["arguments"])
                yield {"type": "tool_call_delta", "id": tc_id, "input_json": tc["function"]["arguments"]}

    # Finish
    if finish_reason:
        # Flush pending tool calls
        for tc_id, buf in list(tool_buffers.items()):
            try:
                input_obj = json.loads("".join(buf["input_json_parts"]))
            except json.JSONDecodeError:
                input_obj = {}
            yield {"type": "tool_call_end", "id": tc_id, "name": buf["name"], "input": input_obj}
        tool_buffers.clear()

        stop_reason = "tool_use" if finish_reason == "tool_calls" else "end_turn" if finish_reason == "stop" else finish_reason
        usage = event.get("usage", {})
        yield {"type": "message_stop", "stop_reason": stop_reason, "usage": usage}
