"""MCP task adapter for calling external MCP servers as workflow nodes.

This adapter supports two execution modes:

- local subprocess transport (legacy mode)
- authority-backed protocol transport (streamable_http for MCP tools/call)

In protocol mode the task payload includes a protocol envelope plus authority
selectors. Those selectors are resolved through Postgres endpoint authority and
the request is then sent over HTTP with Bearer authentication from the bound
auth_ref.
"""

from __future__ import annotations

import json
import select
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _fail
from storage.postgres.connection import _run_sync

from .credentials import CredentialResolutionError, resolve_credential
from .http_transport import HTTPTransportCancelled, HTTPTransportError, perform_http_request
from .protocol_endpoint_runtime import (
    MCPProtocolEndpointRequest,
    ProtocolEndpointRuntimeError,
    resolve_mcp_protocol_endpoint,
)
from .protocol_events import ProtocolMessage, ProtocolMetadata, ProtocolReplyTarget
from storage.postgres import ensure_postgres_available
from .deterministic import DeterministicTaskRequest, DeterministicTaskResult, cancelled_task_result

_DEFAULT_TIMEOUT = 30
_MCP_JSONRPC_VERSION = "2.0"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class _MCPTaskCancelled(RuntimeError):
    """Raised when workflow cancellation interrupts an MCP task."""

class MCPTaskAdapter:
    """Adapter that calls external MCP servers as workflow nodes."""

    executor_type = "adapter.mcp_task"

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        """Execute a task by calling an MCP server tool."""
        started_at = _utc_now()
        payload = dict(request.input_payload)
        inputs = {
            "task_name": request.task_name,
            "input_payload": payload,
            "execution_boundary_ref": request.execution_boundary_ref,
        }

        uses_protocol_mode = self._uses_protocol_mode(payload=payload)
        if uses_protocol_mode:
            return self._execute_protocol_mode(
                request=request,
                payload=payload,
                inputs=inputs,
                started_at=started_at,
            )

        # Validate required fields
        server_command = payload.get("server_command")
        tool_name = payload.get("tool_name")
        if not isinstance(server_command, str) or not server_command.strip():
            return _fail(
                request=request,
                reason_code="adapter.input_invalid",
                failure_code="mcp_task.missing_server_command",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )

        if not isinstance(tool_name, str) or not tool_name.strip():
            return _fail(
                request=request,
                reason_code="adapter.input_invalid",
                failure_code="mcp_task.missing_tool_name",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )

        arguments = payload.get("arguments", {})
        if not isinstance(arguments, Mapping):
            arguments = {}

        timeout = payload.get("timeout", _DEFAULT_TIMEOUT)
        try:
            timeout = int(timeout)
        except (TypeError, ValueError):
            timeout = _DEFAULT_TIMEOUT

        # Execute the MCP call
        try:
            result = self._call_mcp_server(
                server_command=server_command.strip(),
                tool_name=tool_name.strip(),
                arguments=dict(arguments),
                timeout=timeout,
                execution_control=request.execution_control,
            )

            latency_ms = int(((_utc_now() - started_at).total_seconds()) * 1000)

            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs=inputs,
                outputs={
                    "tool_result": result,
                    "latency_ms": latency_ms,
                },
                started_at=started_at,
                finished_at=_utc_now(),
            )

        except _MCPTaskCancelled:
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
                outputs={},
            )

        except TimeoutError:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code="mcp_task.timeout",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )

        except Exception as exc:
            # Classify the error
            error_msg = str(exc)
            if "tool" in error_msg.lower():
                failure_code = "mcp_task.tool_error"
            else:
                failure_code = "mcp_task.server_error"

            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code=failure_code,
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )

    def _execute_protocol_mode(
        self,
        *,
        request: DeterministicTaskRequest,
        payload: dict[str, Any],
        inputs: dict[str, Any],
        started_at: datetime,
    ) -> DeterministicTaskResult:
        try:
            protocol_message = self._normalize_protocol_message(
                payload.get("protocol_message"),
            )
            provider_policy_id = self._require_text(
                payload.get("provider_policy_id"),
                field_name="provider_policy_id",
            )
            candidate_ref = self._require_text(
                payload.get("candidate_ref"),
                field_name="candidate_ref",
            )
            as_of = self._normalize_as_of(payload.get("as_of"))
        except ValueError as exc:
            return _fail(
                request=request,
                reason_code="adapter.input_invalid",
                failure_code="mcp_task.protocol_input_invalid",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
                outputs={
                    "failure_reason": str(exc),
                },
            )

        resolution_request = MCPProtocolEndpointRequest(
            provider_policy_id=provider_policy_id,
            candidate_ref=candidate_ref,
            message=protocol_message,
        )

        try:
            # Make sure Postgres authority tables are available and resolve
            # the bounded MCP endpoint.
            ensure_postgres_available()
            resolution = self._resolve_protocol_endpoint(
                request=resolution_request,
                as_of=as_of,
            )
        except ProtocolEndpointRuntimeError as exc:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code=f"mcp_task.{exc.reason_code}",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
                outputs={"failure_reason": str(exc)},
            )
        except Exception as exc:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code="mcp_task.endpoint_resolution_failed",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
                outputs={"failure_reason": str(exc)},
            )

        timeout = self._normalize_timeout(
            payload.get("timeout"),
            request_policy=resolution.provider_endpoint_binding.request_policy,
            request_default=_DEFAULT_TIMEOUT,
        )

        try:
            auth_header = _resolve_auth_header(resolution.auth_ref)
            result = self._call_mcp_protocol(
                endpoint_uri=resolution.endpoint_uri,
                protocol_message=protocol_message,
                timeout=timeout,
                auth_header=auth_header,
                execution_control=request.execution_control,
            )

            latency_ms = int(((_utc_now() - started_at).total_seconds()) * 1000)
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs=inputs,
                outputs={
                    "tool_result": result,
                    "latency_ms": latency_ms,
                },
                started_at=started_at,
                finished_at=_utc_now(),
            )

        except _MCPTaskCancelled:
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
                outputs={},
            )
        except HTTPTransportCancelled:
            return cancelled_task_result(
                request=request,
                executor_type=self.executor_type,
                started_at=started_at,
                inputs=inputs,
                outputs={},
            )
        except HTTPTransportError:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code="mcp_task.mcp_http_error",
                started_at=started_at,
                executor_type=self.executor_type,
                inputs=inputs,
                outputs={},
            )
        except TimeoutError:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code="mcp_task.timeout",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )
        except CredentialResolutionError as exc:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code=f"mcp_task.{exc.reason_code}",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
                outputs={"failure_reason": str(exc)},
            )
        except ValueError as exc:
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code="mcp_task.protocol_response_invalid",
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
                outputs={"failure_reason": str(exc)},
            )
        except Exception as exc:
            error_msg = str(exc)
            if "tool" in error_msg.lower():
                failure_code = "mcp_task.tool_error"
            elif "http" in error_msg.lower():
                failure_code = "mcp_task.http_error"
            else:
                failure_code = "mcp_task.server_error"
            return _fail(
                request=request,
                reason_code="adapter.execution_failed",
                failure_code=failure_code,
                started_at=started_at,
                executor_type=MCPTaskAdapter.executor_type,
                inputs=inputs,
            )

    @staticmethod
    def _resolve_protocol_endpoint(
        *,
        request: MCPProtocolEndpointRequest,
        as_of: datetime,
    ):
        from storage.postgres.connection import get_workflow_pool

        async def _resolve():
            async with get_workflow_pool().acquire() as conn:
                return await resolve_mcp_protocol_endpoint(
                    conn,
                    request=request,
                    as_of=as_of,
                )

        return _run_sync(_resolve())

    @staticmethod
    def _uses_protocol_mode(
        *,
        payload: dict[str, Any],
    ) -> bool:
        return (
            payload.get("protocol_message") is not None
            or payload.get("provider_policy_id") is not None
            or payload.get("candidate_ref") is not None
            or payload.get("as_of") is not None
        )

    @staticmethod
    def _require_text(
        value: object,
        *,
        field_name: str,
    ) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")
        return value.strip()

    @staticmethod
    def _normalize_as_of(value: object) -> datetime:
        if value is None:
            return _utc_now()
        if isinstance(value, datetime):
            as_of = value
        elif isinstance(value, str):
            normalized = value.replace("Z", "+00:00")
            as_of = datetime.fromisoformat(normalized)
        else:
            raise ValueError("as_of must be an ISO8601 datetime string or datetime")
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        return as_of

    @staticmethod
    def _normalize_timeout(
        value: object,
        *,
        request_policy: Mapping[str, Any],
        request_default: int,
    ) -> int:
        if value is None:
            if not isinstance(request_policy, Mapping):
                return request_default
            raw_timeout = request_policy.get("timeout_ms")
            if raw_timeout is None:
                return request_default
            try:
                timeout_ms = int(raw_timeout)
            except (TypeError, ValueError):
                return request_default
            if timeout_ms <= 0:
                return request_default
            # `request_policy.timeout_ms` is specified in milliseconds.
            return max(1, (timeout_ms + 999) // 1000)
        try:
            timeout = int(value)
        except (TypeError, ValueError):
            return request_default
        if timeout <= 0:
            return request_default
        return max(1, timeout)

    @staticmethod
    def _normalize_protocol_message(
        value: object,
    ) -> ProtocolMessage:
        if isinstance(value, ProtocolMessage):
            return value

        if not isinstance(value, Mapping):
            raise ValueError("protocol_message must be a mapping")
        message = dict(value)
        body = message.get("body")
        if not isinstance(body, Mapping):
            raise ValueError("protocol_message.body must be a mapping")

        metadata = message.get("metadata")
        if not isinstance(metadata, Mapping):
            raise ValueError("protocol_message.metadata must be a mapping")

        raw_reply_target = metadata.get("reply_target")
        if raw_reply_target is None:
            reply_target = None
        elif not isinstance(raw_reply_target, Mapping):
            raise ValueError("protocol_message.metadata.reply_target must be a mapping")
        else:
            reply_target = ProtocolReplyTarget(
                target_kind=MCPTaskAdapter._require_text(
                    raw_reply_target.get("target_kind"),
                    field_name="protocol_message.metadata.reply_target.target_kind",
                ),
                transport_kind=MCPTaskAdapter._require_text(
                    raw_reply_target.get("transport_kind"),
                    field_name="protocol_message.metadata.reply_target.transport_kind",
                ),
                target_ref=MCPTaskAdapter._require_text(
                    raw_reply_target.get("target_ref"),
                    field_name="protocol_message.metadata.reply_target.target_ref",
                ),
            )

        raw_correlation_ids = metadata.get("correlation_ids")
        if raw_correlation_ids is None:
            correlation_ids = {}
        elif not isinstance(raw_correlation_ids, Mapping):
            raise ValueError("protocol_message.metadata.correlation_ids must be a mapping")
        else:
            correlation_ids = {}
            for key, val in dict(raw_correlation_ids).items():
                key_text = MCPTaskAdapter._require_text(key, field_name="protocol_message.metadata.correlation_ids key")
                val_text = MCPTaskAdapter._require_text(
                    val,
                    field_name=f"protocol_message.metadata.correlation_ids[{key_text}]",
                )
                correlation_ids[key_text] = val_text

        return ProtocolMessage(
            direction=MCPTaskAdapter._require_text(
                message.get("direction"),
                field_name="protocol_message.direction",
            ),
            metadata=ProtocolMetadata(
                protocol_kind=MCPTaskAdapter._require_text(
                    metadata.get("protocol_kind"),
                    field_name="protocol_message.metadata.protocol_kind",
                ),
                transport_kind=MCPTaskAdapter._require_text(
                    metadata.get("transport_kind"),
                    field_name="protocol_message.metadata.transport_kind",
                ),
                correlation_ids=correlation_ids,
                reply_target=reply_target,
            ),
            body=dict(body),
        )

    def _call_mcp_protocol(
        self,
        *,
        endpoint_uri: str,
        protocol_message: ProtocolMessage,
        timeout: int,
        auth_header: dict[str, str],
        execution_control,
    ) -> Any:
        message_body = dict(protocol_message.body)
        method = self._require_text(
            message_body.get("method"),
            field_name="protocol_message.body.method",
        )
        params = message_body.get("params")
        if params is None:
            params = {}
        elif not isinstance(params, Mapping):
            raise ValueError("protocol_message.body.params must be a mapping")

        rpc_payload = {
            "jsonrpc": _MCP_JSONRPC_VERSION,
            "id": "praxis-protocol",
            "method": method,
            "params": dict(params),
        }
        headers = {
            "Content-Type": "application/json",
            **auth_header,
        }

        response = perform_http_request(
            method="POST",
            url=endpoint_uri,
            headers=headers,
            body=json.dumps(rpc_payload).encode("utf-8"),
            timeout_seconds=timeout,
            execution_control=execution_control,
        )
        response_body = response.body.decode("utf-8", errors="replace")

        try:
            message = json.loads(response_body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError("MCP protocol response was not JSON")

        if not isinstance(message, Mapping):
            raise ValueError("MCP protocol response must be a mapping")

        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                f"MCP protocol request failed with status={response.status_code}: "
                f"{response_body[:500]}"
            )

        if message.get("error") is not None:
            raise RuntimeError(
                f"MCP protocol tool call failed: {message.get('error')}"
            )

        if "result" not in message:
            raise RuntimeError("MCP protocol response did not contain result")
        return message["result"]

    def _call_mcp_server(
        self,
        *,
        server_command: str,
        tool_name: str,
        arguments: dict[str, Any],
        timeout: int,
        execution_control=None,
    ) -> Any:
        """Call an MCP server tool via JSON-RPC stdin/stdout.

        Protocol:
        1. Start server as subprocess with shell=True
        2. Send initialize request
        3. Send tools/call request
        4. Read and parse response
        5. Terminate subprocess
        """
        proc = None
        try:
            # Start the MCP server
            proc = subprocess.Popen(
                shlex.split(server_command),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=False,
                bufsize=1,  # Line buffered
            )
            cancel_requested = threading.Event()

            if execution_control is not None:
                def _interrupt() -> None:
                    if proc.poll() is not None:
                        return
                    cancel_requested.set()
                    try:
                        proc.terminate()
                        proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()

                execution_control.register_interrupt(_interrupt)

            if cancel_requested.is_set():
                raise _MCPTaskCancelled()

            # Send initialize request
            initialize_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "dag-workflow-adapter",
                        "version": "1.0.0",
                    },
                },
            }

            self._send_json_rpc(proc, initialize_msg, timeout, cancel_requested)

            # Read initialize response
            init_response = self._read_json_rpc(proc, timeout, cancel_requested)
            if init_response.get("error"):
                raise RuntimeError(
                    f"initialize failed: {init_response['error'].get('message', 'unknown')}"
                )

            # Send tools/call request
            call_msg = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "input": arguments,
                },
            }

            self._send_json_rpc(proc, call_msg, timeout, cancel_requested)

            # Read tool response
            response = self._read_json_rpc(proc, timeout, cancel_requested)

            if response.get("error"):
                raise RuntimeError(
                    f"tool {tool_name} failed: {response['error'].get('message', 'unknown')}"
                )

            return response.get("result")

        finally:
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    @staticmethod
    def _send_json_rpc(
        proc: subprocess.Popen,
        msg: dict[str, Any],
        timeout: int,
        cancel_requested: threading.Event,
    ) -> None:
        """Send a JSON-RPC message to the MCP server."""
        if cancel_requested.is_set():
            raise _MCPTaskCancelled()
        try:
            json_line = json.dumps(msg) + "\n"
            proc.stdin.write(json_line)
            proc.stdin.flush()
        except Exception as exc:
            if cancel_requested.is_set():
                raise _MCPTaskCancelled()
            raise RuntimeError(f"Failed to send JSON-RPC message: {exc}")

    @staticmethod
    def _read_json_rpc(
        proc: subprocess.Popen,
        timeout: int,
        cancel_requested: threading.Event,
    ) -> dict[str, Any]:
        """Read and parse a JSON-RPC response from the MCP server."""
        try:
            deadline = time.monotonic() + timeout
            while True:
                if cancel_requested.is_set():
                    raise _MCPTaskCancelled()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"MCP server did not respond within {timeout}s")
                ready, _, _ = select.select([proc.stdout], [], [], min(0.1, remaining))
                if ready:
                    break

            line = proc.stdout.readline()

            if not line:
                if cancel_requested.is_set():
                    raise _MCPTaskCancelled()
                raise RuntimeError("MCP server closed connection unexpectedly")

            return json.loads(line.strip())

        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse JSON-RPC response: {exc}")
        except Exception as exc:
            raise RuntimeError(f"Failed to read JSON-RPC response: {exc}")


def _resolve_auth_header(auth_ref: str) -> dict[str, str]:
    credential = resolve_credential(auth_ref)
    return {"Authorization": f"Bearer {credential.api_key}"}


__all__ = ["MCPTaskAdapter"]
