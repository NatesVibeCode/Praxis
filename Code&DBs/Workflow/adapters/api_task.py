"""HTTP API task adapter for workflow external service integration.

Makes synchronous HTTP API calls for data fetching, webhook triggers, and
external service integration. No LLM involved — just HTTP requests with
optional authentication.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any

from .credentials import CredentialResolutionError, resolve_credential
from .provider_registry import resolve_adapter_config as _resolve_adapter_config
from .deterministic import (
    DeterministicTaskRequest,
    DeterministicTaskResult,
    _utc_now,
    cancelled_task_result,
)
from .http_transport import HTTPTransportCancelled, HTTPTransportError, perform_http_request


class APITaskError(RuntimeError):
    """Raised when an API task execution fails."""

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


def _default_timeout() -> int:
    return int(_resolve_adapter_config("api_task.timeout_seconds", 30))


def _default_expected_status() -> int:
    return int(_resolve_adapter_config("api_task.expected_status", 200))


def _default_user_agent() -> str:
    return str(_resolve_adapter_config("api_task.user_agent", "DAG-APITaskAdapter/1.0"))


def _is_non_empty_text(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_mapping(value: object) -> bool:
    return isinstance(value, Mapping)


def _resolve_auth_header(
    auth_ref: str | None,
    *,
    env: dict[str, str] | None = None,
) -> dict[str, str]:
    """Resolve auth_ref to an Authorization header if provided."""

    if auth_ref is None or not _is_non_empty_text(auth_ref):
        return {}

    try:
        credential = resolve_credential(auth_ref, env=env)
    except CredentialResolutionError as exc:
        raise APITaskError(
            f"api_task.{exc.reason_code}",
            f"credential resolution failed: {exc}",
        ) from exc

    return {"Authorization": f"Bearer {credential.api_key}"}


def _build_request_body(body_input: Any) -> str | bytes:
    """Convert body input to request bytes."""

    if body_input is None:
        return b""

    if isinstance(body_input, str):
        return body_input.encode("utf-8")

    if isinstance(body_input, bytes):
        return body_input

    if isinstance(body_input, Mapping):
        return json.dumps(dict(body_input)).encode("utf-8")

    # Try to JSON-serialize other types
    try:
        return json.dumps(body_input).encode("utf-8")
    except (TypeError, ValueError):
        raise APITaskError(
            "api_task.body_serialization_error",
            f"cannot serialize body of type {type(body_input).__name__}",
        )


def _parse_response_body(response_bytes: bytes) -> dict[str, Any] | None:
    """Try to parse response as JSON; return None if not valid JSON."""

    try:
        return json.loads(response_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None


def _build_headers(
    url: str,
    method: str,
    headers_input: Mapping[str, Any] | None,
    body: str | bytes,
    auth_header: dict[str, str],
) -> dict[str, str]:
    """Build final request headers."""

    headers = {"User-Agent": _default_user_agent()}

    # Add auth if provided
    headers.update(auth_header)

    # Add user-supplied headers
    if headers_input is not None:
        for key, value in headers_input.items():
            if _is_non_empty_text(key):
                headers[str(key)] = str(value)

    # Set Content-Type if body is present and not already set
    if body and "Content-Type" not in headers:
        headers["Content-Type"] = "application/json"

    return headers


class APITaskAdapter:
    """HTTP API task adapter for external service calls."""

    executor_type = "adapter.api_task"

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        """Execute an HTTP API call.

        Reads from input_payload:
        - url (required): endpoint to call
        - method (default "GET"): HTTP method
        - headers (optional dict): request headers
        - body (optional dict/str): request body (JSON-serialized if dict)
        - auth_ref (optional): credential reference for Authorization header
        - timeout (default 30): request timeout in seconds
        - expected_status (default 200): expected HTTP status code

        On success: outputs include status_code, response_body, response_json (if
        parseable), latency_ms, and response headers.

        On failure: failure_code indicates api_task.http_error, network_error, or
        timeout.
        """

        started_at = _utc_now()
        normalized_inputs = {
            "task_name": request.task_name,
            "input_payload": dict(request.input_payload),
            "dependency_inputs": dict(request.dependency_inputs),
            "execution_boundary_ref": request.execution_boundary_ref,
        }

        if request.execution_control is not None and request.execution_control.cancel_requested():
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=normalized_inputs,
            )

        # Validate basic request shape
        if (
            not _is_non_empty_text(request.node_id)
            or not _is_non_empty_text(request.task_name)
            or not _is_non_empty_text(request.execution_boundary_ref)
            or not _is_mapping(request.input_payload)
            or not _is_mapping(request.expected_outputs)
            or not _is_mapping(request.dependency_inputs)
        ):
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.input_invalid",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.input_invalid",
            )

        input_payload = dict(request.input_payload)
        url = input_payload.get("url")
        method = input_payload.get("method", "GET")
        headers_input = input_payload.get("headers")
        body_input = input_payload.get("body")
        auth_ref = input_payload.get("auth_ref")
        timeout = input_payload.get("timeout", _default_timeout())
        expected_status = input_payload.get("expected_status", _default_expected_status())

        # Validate required parameters
        if not _is_non_empty_text(url):
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.url_required",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.url_required",
            )

        if not _is_non_empty_text(method):
            method = "GET"

        # Validate method
        method_upper = str(method).upper()
        valid_methods = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        if method_upper not in valid_methods:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.method_invalid",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.method_invalid",
            )

        # Validate timeout
        try:
            timeout_float = float(timeout)
            if timeout_float <= 0:
                timeout_float = _default_timeout()
        except (TypeError, ValueError):
            timeout_float = _default_timeout()

        # Validate expected_status
        try:
            expected_status_int = int(expected_status)
        except (TypeError, ValueError):
            expected_status_int = _default_expected_status()

        # Resolve auth if provided
        try:
            auth_header = _resolve_auth_header(auth_ref)
        except APITaskError as exc:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code=exc.reason_code,
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code=exc.reason_code,
            )

        # Build request body
        try:
            body_bytes = _build_request_body(body_input)
        except APITaskError as exc:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code=exc.reason_code,
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code=exc.reason_code,
            )

        # Build headers
        headers = _build_headers(
            url,
            method_upper,
            headers_input,
            body_bytes,
            auth_header,
        )

        if request.execution_control is not None and request.execution_control.cancel_requested():
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=normalized_inputs,
            )

        start_ns = time.monotonic_ns()
        try:
            response = perform_http_request(
                method=method_upper,
                url=str(url),
                headers=headers,
                body=body_bytes if body_bytes else None,
                timeout_seconds=timeout_float,
                execution_control=request.execution_control,
            )
        except HTTPTransportCancelled:
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=normalized_inputs,
            )
        except TimeoutError:
            latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="api_task.timeout",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="api_task.timeout",
            )
        except HTTPTransportError:
            latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="api_task.network_error",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="api_task.network_error",
            )

        latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
        status_code = response.status_code
        response_headers = response.headers
        response_bytes = response.body
        response_json = _parse_response_body(response_bytes)
        response_text = response_bytes.decode("utf-8", errors="replace")

        if status_code != expected_status_int:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="api_task.http_error",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={
                    "status_code": status_code,
                    "response_body": response_text,
                    "response_json": response_json,
                    "latency_ms": latency_ms,
                    "headers": response_headers,
                },
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="api_task.http_error",
            )

        # Success
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs=normalized_inputs,
            outputs={
                "status_code": status_code,
                "response_body": response_text,
                "response_json": response_json,
                "latency_ms": latency_ms,
                "headers": response_headers,
            },
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code=None,
        )


__all__ = [
    "APITaskAdapter",
    "APITaskError",
]
