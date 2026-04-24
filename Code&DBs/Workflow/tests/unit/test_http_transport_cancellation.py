from __future__ import annotations

import http.server
import json
import threading
import time
from contextlib import contextmanager
from typing import Any

import adapters.llm_client as llm_client_mod
import adapters.llm_task as llm_task_mod
import pytest

from adapters import provider_transport
from adapters.api_task import APITaskAdapter
from adapters.deterministic import DeterministicExecutionControl, DeterministicTaskRequest
from adapters.http_transport import HTTPTransportCancelled
from adapters.llm_client import LLMClientError, LLMRequest, call_llm, call_llm_streaming
from adapters.llm_task import LLMTaskAdapter


@contextmanager
def _slow_http_server(
    *,
    response_body: bytes,
    content_type: str = "application/json",
    sleep_s: float = 5.0,
):
    started = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            started.set()
            time.sleep(sleep_s)
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            try:
                self.wfile.write(response_body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args
            return

    try:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    except PermissionError as exc:
        pytest.skip(f"loopback HTTP bind unavailable in this sandbox: {exc}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield (
            f"http://127.0.0.1:{server.server_port}/v1/test",
            started,
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


@contextmanager
def _slow_streaming_http_server(
    *,
    first_chunk: bytes,
    trailing_chunk: bytes,
    delay_after_first_chunk_s: float = 5.0,
):
    started = threading.Event()
    first_chunk_sent = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            started.set()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            try:
                self.wfile.write(first_chunk)
                self.wfile.flush()
                first_chunk_sent.set()
                time.sleep(delay_after_first_chunk_s)
                self.wfile.write(trailing_chunk)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args
            return

    try:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    except PermissionError as exc:
        pytest.skip(f"loopback HTTP bind unavailable in this sandbox: {exc}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield (
            f"http://127.0.0.1:{server.server_port}/v1/test",
            started,
            first_chunk_sent,
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_call_llm_interrupts_inflight_http_request_when_cancelled() -> None:
    response_body = json.dumps(
        {
            "model": "gpt-5.4",
            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }
    ).encode("utf-8")
    control = DeterministicExecutionControl()

    with _slow_http_server(response_body=response_body) as (url, started):
        request = LLMRequest(
            endpoint_uri=url,
            api_key="sk-test",
            provider_slug="openai",
            model_slug="gpt-5.4",
            messages=({"role": "user", "content": "say hello"},),
            protocol_family="openai_chat_completions",
            timeout_seconds=30,
            execution_control=control,
        )
        result_box: dict[str, Any] = {}

        def _run() -> None:
            try:
                result_box["response"] = call_llm(request)
            except Exception as exc:  # pragma: no cover - thread boundary
                result_box["error"] = exc

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        assert started.wait(timeout=2)
        control.request_cancel()
        worker.join(timeout=5)

        assert not worker.is_alive()
        assert "response" not in result_box
        assert isinstance(result_box.get("error"), LLMClientError)
        assert result_box["error"].reason_code == "llm_client.cancelled"


def test_api_task_adapter_returns_cancelled_when_http_request_is_interrupted() -> None:
    control = DeterministicExecutionControl()
    adapter = APITaskAdapter()

    with _slow_http_server(response_body=b'{"ok": true}') as (url, started):
        request = DeterministicTaskRequest(
            node_id="api_node",
            task_name="api_node",
            input_payload={
                "url": url,
                "method": "POST",
                "body": {"hello": "world"},
                "timeout": 30,
            },
            expected_outputs={},
            dependency_inputs={},
            execution_boundary_ref="workspace:test",
            execution_control=control,
        )
        result_box: dict[str, Any] = {}

        def _run() -> None:
            result_box["result"] = adapter.execute(request=request)

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        assert started.wait(timeout=2)
        control.request_cancel()
        worker.join(timeout=5)

        assert not worker.is_alive()
        result = result_box["result"]
        assert result.status == "cancelled"
        assert result.failure_code == "workflow_cancelled"


def test_llm_task_adapter_returns_cancelled_when_http_request_is_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response_body = json.dumps(
        {
            "model": "gpt-5.4",
            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }
    ).encode("utf-8")
    control = DeterministicExecutionControl()
    profiles = {profile.provider_slug: profile for profile in provider_transport.BUILTIN_PROVIDER_PROFILES}

    with _slow_http_server(response_body=response_body) as (url, started):
        monkeypatch.setattr(llm_task_mod, "resolve_api_endpoint", lambda *_args, **_kwargs: url)
        monkeypatch.setattr(
            llm_task_mod,
            "resolve_adapter_contract",
            lambda provider_slug, adapter_type: provider_transport.resolve_adapter_contract(
                provider_slug,
                adapter_type,
                profiles=profiles,
                adapter_config={},
                failure_mappings={},
            ),
        )
        monkeypatch.setattr(
            llm_task_mod,
            "resolve_api_protocol_family",
            lambda provider_slug: provider_transport.resolve_api_protocol_family(
                provider_slug,
                profiles=profiles,
            ),
        )
        monkeypatch.setattr(
            llm_task_mod,
            "supports_adapter",
            lambda provider_slug, adapter_type: True,
        )
        monkeypatch.setattr(
            llm_task_mod,
            "resolve_credential",
            lambda auth_ref, env=None: type(
                "_Cred",
                (),
                {
                    "auth_ref": auth_ref,
                    "api_key": "sk-test",
                    "provider_hint": "openai",
                },
            )(),
        )
        adapter = LLMTaskAdapter(
            default_provider="openai",
            default_model="gpt-5.4",
            credential_env={"OPENAI_API_KEY": "sk-test"},
        )
        request = DeterministicTaskRequest(
            node_id="llm_node",
            task_name="llm_node",
            input_payload={
                "prompt": "say hello",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "timeout_seconds": 30,
            },
            expected_outputs={},
            dependency_inputs={},
            execution_boundary_ref="workspace:test",
            execution_control=control,
        )
        result_box: dict[str, Any] = {}

        def _run() -> None:
            try:
                result_box["result"] = adapter.execute(request=request)
            except Exception as exc:  # pragma: no cover - thread boundary
                result_box["error"] = exc

        worker = threading.Thread(target=_run, daemon=True)
        worker.start()
        assert started.wait(timeout=2)
        control.request_cancel()
        worker.join(timeout=5)

        assert not worker.is_alive()
        assert "error" not in result_box, result_box.get("error")
        result = result_box["result"]
        assert result.status == "cancelled"
        assert result.failure_code == "workflow_cancelled"


def test_call_llm_streaming_interrupts_mid_stream_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = DeterministicExecutionControl()
    first_delta_seen = threading.Event()
    events: list[dict[str, Any]] = []
    result_box: dict[str, Any] = {}

    class _BlockingStreamResponse:
        status_code = 200

        def iter_lines(self, max_line_bytes: int = 65_536):
            del max_line_bytes
            yield b'data: {"choices":[{"delta":{"content":"hel"}}]}\n'
            if control.wait_for_cancel(timeout=5.0):
                raise HTTPTransportCancelled()
            yield b'data: {"choices":[{"delta":{"content":"lo"}}]}\n'
            yield b"data: [DONE]\n"

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        llm_client_mod,
        "_open_streaming_http_request",
        lambda **_kwargs: _BlockingStreamResponse(),
    )

    request = LLMRequest(
        endpoint_uri="https://example.invalid/v1/test",
        api_key="sk-test",
        provider_slug="openai",
        model_slug="gpt-5.4",
        messages=({"role": "user", "content": "say hello"},),
        protocol_family="openai_chat_completions",
        timeout_seconds=30,
        execution_control=control,
    )

    def _consume() -> None:
        try:
            for event in call_llm_streaming(request):
                events.append(event)
                if event.get("type") == "text_delta":
                    first_delta_seen.set()
        except Exception as exc:  # pragma: no cover - thread boundary
            result_box["error"] = exc

    worker = threading.Thread(target=_consume, daemon=True)
    worker.start()
    assert first_delta_seen.wait(timeout=2)
    control.request_cancel()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert "error" not in result_box, result_box.get("error")
    assert any(event.get("type") == "text_delta" and event.get("text") == "hel" for event in events)
    assert events[-1] == {"type": "error", "message": "cancelled"}
