"""Strict OpenRouter client for Model Eval API evals."""

from __future__ import annotations

import json
import signal
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from adapters.keychain import resolve_secret


OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"

BLOCKED_PROVIDER_SLUGS: tuple[str, ...] = (
    "moonshot",
    "moonshotai",
    "kimi",
    "deepseek",
    "deepseek-ai",
    "baidu",
    "alibaba",
    "tencent",
    "zhipu",
    "z-ai",
)

LAB_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "model_eval_response",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id", "answer", "artifacts"],
            "properties": {
                "task_id": {"type": "string"},
                "answer": {"type": "string"},
                "artifacts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["path", "media_type", "content"],
                        "properties": {
                            "path": {"type": "string"},
                            "media_type": {"type": "string"},
                            "content": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
}


class OpenRouterError(RuntimeError):
    """Raised for OpenRouter transport failures."""


def strict_provider_policy(provider_order: list[str] | None = None) -> dict[str, Any]:
    ordered = [str(item).strip() for item in provider_order or [] if str(item).strip()]
    if not ordered:
        raise OpenRouterError("explicit provider_order is required for Model Eval routes")
    blocked = {item.lower() for item in BLOCKED_PROVIDER_SLUGS}
    disallowed = [item for item in ordered if item.lower() in blocked]
    if disallowed:
        raise OpenRouterError(f"disallowed provider_order entries: {', '.join(disallowed)}")
    policy: dict[str, Any] = {
        "allow_fallbacks": False,
        "require_parameters": True,
        "data_collection": "deny",
        "zdr": True,
        "ignore": list(BLOCKED_PROVIDER_SLUGS),
    }
    if ordered:
        policy["order"] = ordered
    return policy


def _api_key() -> str:
    key = resolve_secret("OPENROUTER_API_KEY")
    if not key:
        raise OpenRouterError("OPENROUTER_API_KEY is missing")
    return key


def chat_completion(payload: dict[str, Any], *, timeout_seconds: int = 90) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        OPENROUTER_CHAT_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://praxis.local/model-eval",
            "X-Title": "Praxis Model Eval",
        },
        method="POST",
    )
    started = time.perf_counter()

    def _execute() -> dict[str, Any]:
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            try:
                parsed_detail: Any = json.loads(detail)
            except json.JSONDecodeError:
                parsed_detail = detail
            return {
                "ok": False,
                "status": exc.code,
                "duration_s": round(time.perf_counter() - started, 3),
                "error": parsed_detail,
            }
        except (OSError, TimeoutError, json.JSONDecodeError) as exc:
            return {
                "ok": False,
                "status": None,
                "duration_s": round(time.perf_counter() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
        parsed["ok"] = "error" not in parsed
        parsed["status"] = 200
        parsed["duration_s"] = round(time.perf_counter() - started, 3)
        return parsed

    if timeout_seconds <= 0 or threading.current_thread() is not threading.main_thread():
        return _execute()

    def _handle_timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError(f"OpenRouter call exceeded {timeout_seconds}s wall-clock deadline")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
    try:
        return _execute()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


def build_lab_request(
    *,
    model_slug: str,
    provider_order: list[str] | None,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float | None = 0.1,
    reasoning_effort: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    if ":free" in model_slug:
        raise OpenRouterError("free OpenRouter model routes are not admitted by Model Eval")
    body: dict[str, Any] = {
        "model": model_slug,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "provider": strict_provider_policy(provider_order),
    }
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"
    else:
        body["response_format"] = LAB_RESPONSE_SCHEMA
    if model_slug.startswith("openai/gpt-5.4"):
        body["max_completion_tokens"] = int(max_tokens)
    else:
        body["max_tokens"] = int(max_tokens)
    if temperature is not None and not model_slug.startswith("openai/gpt-5.4"):
        body["temperature"] = float(temperature)
    if reasoning_effort:
        body["reasoning"] = {"effort": reasoning_effort}
    if seed is not None:
        body["seed"] = int(seed)
    return body


__all__ = [
    "BLOCKED_PROVIDER_SLUGS",
    "LAB_RESPONSE_SCHEMA",
    "OpenRouterError",
    "build_lab_request",
    "chat_completion",
    "strict_provider_policy",
]
