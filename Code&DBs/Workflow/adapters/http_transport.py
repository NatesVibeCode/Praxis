"""Interruptible HTTP transport helpers for workflow adapters."""

from __future__ import annotations

import http.client
import select
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import SplitResult, urlsplit

if TYPE_CHECKING:
    from .deterministic import DeterministicExecutionControl


class HTTPTransportError(RuntimeError):
    """Raised when the low-level HTTP transport cannot complete a request."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class HTTPTransportCancelled(HTTPTransportError):
    """Raised when workflow cancellation interrupts an in-flight HTTP request."""

    def __init__(self) -> None:
        super().__init__("http_transport.cancelled", "request cancelled")


@dataclass(frozen=True, slots=True)
class HTTPResponse:
    """Normalized HTTP response payload."""

    status_code: int
    headers: dict[str, str]
    body: bytes


class HTTPStreamResponse:
    """Normalized interruptible streaming HTTP response."""

    def __init__(
        self,
        *,
        status_code: int,
        headers: dict[str, str],
        response: http.client.HTTPResponse,
        connection: http.client.HTTPConnection,
        cancel_requested: threading.Event,
        timeout_seconds: float,
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self._response = response
        self._connection = connection
        self._cancel_requested = cancel_requested
        self._deadline = time.monotonic() + timeout_seconds

    def _response_reader(self):
        return getattr(self._response, "fp", None)

    def _response_socket(self):
        sock = getattr(self._connection, "sock", None)
        if sock is not None:
            return sock
        reader = self._response_reader()
        raw = getattr(reader, "raw", None)
        return getattr(raw, "_sock", None)

    def _remaining_timeout(self) -> float:
        return self._deadline - time.monotonic()

    def _read_available(self, read_chunk_size: int) -> bytes | None:
        if self._cancel_requested.is_set():
            raise HTTPTransportCancelled()
        remaining = self._remaining_timeout()
        if remaining <= 0:
            raise TimeoutError("request timed out")
        sock = self._response_socket()
        if sock is not None:
            try:
                ready, _, _ = select.select([sock], [], [], min(0.2, max(remaining, 0.01)))
            except (OSError, ValueError) as exc:
                if self._cancel_requested.is_set():
                    raise HTTPTransportCancelled() from exc
                raise HTTPTransportError("http_transport.network_error", str(exc)) from exc
            if not ready:
                return None
        try:
            if hasattr(self._response, "read1"):
                return self._response.read1(read_chunk_size)
            return self._response.read(read_chunk_size)
        except socket.timeout:
            return None
        except TimeoutError as exc:
            if self._cancel_requested.is_set():
                raise HTTPTransportCancelled() from exc
            if self._remaining_timeout() <= 0:
                raise TimeoutError("request timed out") from exc
            return None
        except (OSError, ValueError, http.client.HTTPException, ssl.SSLError) as exc:
            if self._cancel_requested.is_set():
                raise HTTPTransportCancelled() from exc
            raise HTTPTransportError("http_transport.network_error", str(exc)) from exc

    def iter_chunks(self, read_chunk_size: int = 4096):
        while True:
            chunk = self._read_available(read_chunk_size)
            if chunk is None:
                continue
            if not chunk:
                break
            yield chunk

    def iter_lines(self, max_line_bytes: int = 65_536):
        buffer = b""
        while True:
            newline_index = buffer.find(b"\n")
            if newline_index >= 0:
                line = buffer[: newline_index + 1]
                buffer = buffer[newline_index + 1 :]
                yield line
                continue
            chunk = self._read_available(max_line_bytes)
            if chunk is None:
                continue
            if not chunk:
                if buffer:
                    yield buffer
                break
            buffer += chunk

    def close(self) -> None:
        self._cancel_requested.set()
        try:
            self._response.close()
        except Exception:
            pass
        reader = self._response_reader()
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        try:
            self._connection.close()
        except Exception:
            pass


def _parsed_url(url: str) -> SplitResult:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HTTPTransportError("http_transport.invalid_url", f"invalid URL: {url!r}")
    return parsed


def _request_target(parsed: SplitResult) -> str:
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    return target


def _close_live_response(response: http.client.HTTPResponse | None) -> None:
    if response is None:
        return
    try:
        response.close()
    except Exception:
        pass
    reader = getattr(response, "fp", None)
    if reader is not None:
        try:
            reader.close()
        except Exception:
            pass
        raw = getattr(reader, "raw", None)
        if raw is not None:
            sock = getattr(raw, "_sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass


def perform_http_request(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout_seconds: float,
    execution_control: DeterministicExecutionControl | None = None,
    read_chunk_size: int = 4096,
) -> HTTPResponse:
    """Perform one interruptible HTTP request."""

    if execution_control is not None and execution_control.cancel_requested():
        raise HTTPTransportCancelled()

    parsed = _parsed_url(url)
    connection_class = (
        http.client.HTTPSConnection
        if parsed.scheme == "https"
        else http.client.HTTPConnection
    )
    connection = connection_class(
        host=parsed.hostname,
        port=parsed.port,
        timeout=timeout_seconds,
    )
    cancel_requested = threading.Event()
    response_holder: list[http.client.HTTPResponse] = []

    if execution_control is not None:
        def _interrupt() -> None:
            cancel_requested.set()
            if response_holder:
                try:
                    response_holder[0].close()
                except Exception:
                    pass
                reader = getattr(response_holder[0], "fp", None)
                if reader is not None:
                    try:
                        reader.close()
                    except Exception:
                        pass
            sock = getattr(connection, "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
            try:
                connection.close()
            except Exception:
                return

        execution_control.register_interrupt(_interrupt)

    try:
        if cancel_requested.is_set():
            raise HTTPTransportCancelled()
        connection.request(
            method.upper(),
            _request_target(parsed),
            body=body,
            headers=headers,
        )
        response = connection.getresponse()
        response_holder[:] = [response]
        response_headers = {key: value for key, value in response.getheaders()}
        chunks: list[bytes] = []
        while True:
            if cancel_requested.is_set():
                raise HTTPTransportCancelled()
            chunk = response.read(read_chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
        return HTTPResponse(
            status_code=response.status,
            headers=response_headers,
            body=b"".join(chunks),
        )
    except TimeoutError as exc:
        if cancel_requested.is_set():
            raise HTTPTransportCancelled() from exc
        raise
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        if cancel_requested.is_set():
            raise HTTPTransportCancelled() from exc
        raise HTTPTransportError("http_transport.network_error", str(exc)) from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass


def open_http_stream(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout_seconds: float,
    execution_control: DeterministicExecutionControl | None = None,
) -> HTTPStreamResponse:
    """Open one interruptible streaming HTTP request."""

    if execution_control is not None and execution_control.cancel_requested():
        raise HTTPTransportCancelled()

    parsed = _parsed_url(url)
    connection_class = (
        http.client.HTTPSConnection
        if parsed.scheme == "https"
        else http.client.HTTPConnection
    )
    connection = connection_class(
        host=parsed.hostname,
        port=parsed.port,
        timeout=timeout_seconds,
    )
    cancel_requested = threading.Event()
    response_holder: list[http.client.HTTPResponse] = []

    if execution_control is not None:
        def _interrupt() -> None:
            cancel_requested.set()
            _close_live_response(response_holder[0] if response_holder else None)
            sock = getattr(connection, "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
            try:
                connection.close()
            except Exception:
                return

        execution_control.register_interrupt(_interrupt)

    try:
        if cancel_requested.is_set():
            raise HTTPTransportCancelled()
        connection.request(
            method.upper(),
            _request_target(parsed),
            body=body,
            headers=headers,
        )
        response = connection.getresponse()
        response_holder[:] = [response]
        return HTTPStreamResponse(
            status_code=response.status,
            headers={key: value for key, value in response.getheaders()},
            response=response,
            connection=connection,
            cancel_requested=cancel_requested,
            timeout_seconds=timeout_seconds,
        )
    except TimeoutError as exc:
        try:
            connection.close()
        except Exception:
            pass
        if cancel_requested.is_set():
            raise HTTPTransportCancelled() from exc
        raise
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        try:
            connection.close()
        except Exception:
            pass
        if cancel_requested.is_set():
            raise HTTPTransportCancelled() from exc
        raise HTTPTransportError("http_transport.network_error", str(exc)) from exc


__all__ = [
    "HTTPResponse",
    "HTTPStreamResponse",
    "HTTPTransportCancelled",
    "HTTPTransportError",
    "open_http_stream",
    "perform_http_request",
]
