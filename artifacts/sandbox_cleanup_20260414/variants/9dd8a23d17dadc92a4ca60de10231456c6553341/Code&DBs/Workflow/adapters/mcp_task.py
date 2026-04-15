"""MCP task adapter for calling external MCP servers as workflow nodes.

This adapter enables workflow nodes to call external MCP (Model Context Protocol)
servers' tools. It handles:
- Starting the MCP server as a subprocess (stdin/stdout JSON-RPC)
- Initializing the MCP connection
- Calling tools with arguments
- Parsing and returning results

The adapter implements the TaskAdapter protocol for transparent integration with
the deterministic task execution system.
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
from .deterministic import DeterministicTaskRequest, DeterministicTaskResult, cancelled_task_result

_DEFAULT_TIMEOUT = 30


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
        """Execute a task by calling an MCP server tool.

        Input payload shape:
        {
            "server_command": "python3 -m my_mcp_server",  # required
            "tool_name": "tool_to_call",                    # required
            "arguments": {...},                             # optional
            "timeout": 30                                   # optional, default 30
        }

        Returns DeterministicTaskResult with outputs:
        {
            "tool_result": {...},
            "latency_ms": N
        }
        """
        started_at = _utc_now()
        payload = dict(request.input_payload)
        inputs = {
            "task_name": request.task_name,
            "input_payload": payload,
            "execution_boundary_ref": request.execution_boundary_ref,
        }

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


__all__ = ["MCPTaskAdapter"]
