"""Deterministic task adapter for the first runnable workflow slice.

Adapters translate a normalized execution request into a typed result. They do
not derive runtime transitions, dependency order, or lifecycle truth.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import importlib
import logging
import threading
from typing import Any, Callable, Protocol

from runtime.workspace_paths import authority_workspace_roots, container_workspace_root

_log = logging.getLogger(__name__)


def _translate_host_path_to_container(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return value
    path = Path(value)
    container_root = container_workspace_root()
    for host_root in authority_workspace_roots():
        try:
            relative = path.relative_to(host_root)
        except ValueError:
            continue
        if host_root.exists() or not container_root.exists():
            return value
        return str(container_root / relative)
    return value


def _normalize_payload_for_container(payload: dict[str, Any]) -> dict[str, Any]:
    """Translate host-view workspace paths to container-view paths in place.

    The deterministic adapter runs in the worker container; admission on the
    host encodes paths from its filesystem view. Only `workspace_root` is
    translated — other keys are resolved relative to it by the builder.
    """

    value = payload.get("workspace_root")
    translated = _translate_host_path_to_container(value)
    if translated != value:
        payload["workspace_root"] = translated
    return payload


class AdapterResolutionError(RuntimeError):
    """Raised when runtime requests an unsupported adapter type."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class DeterministicTaskRequest:
    """Normalized deterministic task request."""

    node_id: str
    task_name: str
    input_payload: Mapping[str, Any]
    expected_outputs: Mapping[str, Any]
    dependency_inputs: Mapping[str, Any]
    execution_boundary_ref: str
    execution_control: DeterministicExecutionControl | None = None


@dataclass(frozen=True, slots=True)
class DeterministicTaskResult:
    """Typed deterministic task execution result."""

    node_id: str
    task_name: str
    status: str
    reason_code: str
    executor_type: str
    inputs: Mapping[str, Any]
    outputs: Mapping[str, Any]
    started_at: datetime
    finished_at: datetime
    failure_code: str | None = None


class DeterministicExecutionControl:
    """Shared cancellation authority for one in-flight deterministic task."""

    def __init__(self) -> None:
        self._cancel_event = threading.Event()
        self._callbacks: list[Callable[[], None]] = []
        self._lock = threading.Lock()

    def cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    def wait_for_cancel(self, timeout: float | None = None) -> bool:
        return self._cancel_event.wait(timeout=timeout)

    def register_interrupt(self, callback: Callable[[], None]) -> None:
        invoke_immediately = False
        with self._lock:
            if self._cancel_event.is_set():
                invoke_immediately = True
            else:
                self._callbacks.append(callback)
        if invoke_immediately:
            self._invoke_callback(callback)

    def request_cancel(self) -> bool:
        callbacks: tuple[Callable[[], None], ...] = ()
        with self._lock:
            if self._cancel_event.is_set():
                return False
            self._cancel_event.set()
            callbacks = tuple(self._callbacks)
            self._callbacks.clear()
        for callback in callbacks:
            self._invoke_callback(callback)
        return True

    @staticmethod
    def _invoke_callback(callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception:
            return


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _deterministic_builder_outputs(input_payload: Mapping[str, Any]) -> dict[str, Any] | None:
    builder_ref = input_payload.get("deterministic_builder")
    if not _is_non_empty_text(builder_ref):
        return None
    builder_path = str(builder_ref).strip()
    module_name, _, function_name = builder_path.rpartition(".")
    if not module_name or not function_name:
        raise ValueError(
            "input_payload.deterministic_builder must be a dotted module path like "
            "'runtime.workflow_eval.build_runtime_regression_probe_review'"
        )
    module = importlib.import_module(module_name)
    builder = getattr(module, function_name, None)
    if not callable(builder):
        raise ValueError(f"deterministic builder not found: {builder_path}")
    outputs = builder(_normalize_payload_for_container(dict(input_payload)))
    if not isinstance(outputs, Mapping):
        raise ValueError(f"deterministic builder must return a mapping: {builder_path}")
    return dict(outputs)


def cancelled_task_result(
    *,
    request: DeterministicTaskRequest,
    executor_type: str,
    started_at: datetime,
    inputs: Mapping[str, Any],
    outputs: Mapping[str, Any] | None = None,
    reason_code: str = "workflow_cancelled",
) -> DeterministicTaskResult:
    return DeterministicTaskResult(
        node_id=request.node_id,
        task_name=request.task_name,
        status="cancelled",
        reason_code=reason_code,
        executor_type=executor_type,
        inputs=dict(inputs),
        outputs=dict(outputs or {}),
        started_at=started_at,
        finished_at=_utc_now(),
        failure_code=reason_code,
    )


def _is_non_empty_text(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_mapping(value: object) -> bool:
    return isinstance(value, Mapping)


def _allows_passthrough_echo(input_payload: Mapping[str, Any]) -> bool:
    return input_payload.get("allow_passthrough_echo") is True


class DeterministicTaskAdapter:
    """Single boring deterministic adapter for the minimal workflow slice."""

    executor_type = "adapter.deterministic_task"

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        normalized_inputs = {
            "task_name": request.task_name,
            "input_payload": dict(request.input_payload),
            "dependency_inputs": dict(request.dependency_inputs),
            "execution_boundary_ref": request.execution_boundary_ref,
        }
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
        if request.dependency_inputs:
            for dependency_key, dependency_value in request.dependency_inputs.items():
                if isinstance(dependency_value, Mapping):
                    input_payload[dependency_key] = dict(dependency_value)
                else:
                    input_payload[dependency_key] = dependency_value
        if input_payload.get("force_failure") is True:
            failure_code = input_payload.get("failure_code")
            if not _is_non_empty_text(failure_code):
                failure_code = "adapter.command_failed"
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code=str(failure_code),
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code=str(failure_code),
            )

        try:
            outputs = _deterministic_builder_outputs(input_payload)
        except Exception as exc:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="failed",
                reason_code="adapter.execution_failed",
                executor_type=self.executor_type,
                inputs=normalized_inputs,
                outputs={"failure_reason": str(exc)},
                started_at=started_at,
                finished_at=_utc_now(),
                failure_code="adapter.deterministic_builder_failed",
            )

        # Passthrough-echo path: no `deterministic_builder` provided. This is
        # only legal for explicit smoke specs that opt into it; otherwise the
        # adapter fails closed instead of minting green receipts over phantom
        # work. Explicit passthrough still surfaces three signals:
        #   1. A distinct reason_code so receipts can be queried/filtered.
        #   2. An output annotation (`passthrough_echo: true`) so downstream
        #      adapters, verifiers, and UIs can detect the echo.
        #   3. A WARN log including node_id + task_name + a hint to register
        #      a builder. Operators tailing worker logs see this immediately.
        passthrough_echo = outputs is None
        if passthrough_echo:
            if not _allows_passthrough_echo(input_payload):
                return DeterministicTaskResult(
                    node_id=request.node_id,
                    task_name=request.task_name,
                    status="failed",
                    reason_code="adapter.deterministic_builder_missing",
                    executor_type=self.executor_type,
                    inputs=normalized_inputs,
                    outputs={
                        "failure_reason": (
                            "deterministic_task requires input_payload.deterministic_builder "
                            "unless allow_passthrough_echo is true"
                        )
                    },
                    started_at=started_at,
                    finished_at=_utc_now(),
                    failure_code="adapter.deterministic_builder_missing",
                )
            outputs = dict(request.expected_outputs)
            outputs.setdefault("passthrough_echo", True)
            _log.warning(
                "deterministic_task passthrough-echo: node_id=%s task_name=%s "
                "has no 'deterministic_builder' in input_payload; echoing "
                "expected_outputs back as the task result. This is almost "
                "always a spec error — register a builder or rename the task "
                "to make the passthrough explicit.",
                request.node_id,
                request.task_name,
            )

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code=(
                "adapter.execution_passthrough_echo"
                if passthrough_echo
                else "adapter.execution_succeeded"
            ),
            executor_type=self.executor_type,
            inputs=normalized_inputs,
            outputs=outputs,
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code=None,
        )


class ControlOperatorAdapter:
    """Placeholder adapter registration for graph-native control operators.

    The deterministic runtime owns control-operator expansion directly. This
    adapter exists so the admitted contract can resolve `adapter_type` without
    falling back to an unknown-adapter error in older call sites.
    """

    executor_type = "adapter.control_operator"

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="failed",
            reason_code="adapter.control_operator_direct_execution_forbidden",
            executor_type=self.executor_type,
            inputs={
                "task_name": request.task_name,
                "input_payload": dict(request.input_payload),
                "dependency_inputs": dict(request.dependency_inputs),
                "execution_boundary_ref": request.execution_boundary_ref,
            },
            outputs={},
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code="adapter.control_operator_direct_execution_forbidden",
        )


class TaskAdapter(Protocol):
    """Protocol that all task adapters must satisfy."""

    executor_type: str

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult: ...


class BaseNodeAdapter:
    """Base class for workflow node adapters.

    Provides the common dependency_inputs merge that every adapter needs.
    Subclasses implement _execute(payload, request) instead of execute().
    """

    executor_type: str = "adapter.base"

    def _merge_inputs(self, request: DeterministicTaskRequest) -> dict[str, Any]:
        """Merge dependency_inputs into input_payload.

        Upstream node outputs (passed via edge payload_mapping) override
        static node inputs. This is the standard pattern for all adapters.
        """
        payload = dict(request.input_payload)
        if request.dependency_inputs:
            for dep_key, dep_val in request.dependency_inputs.items():
                if isinstance(dep_val, Mapping):
                    payload.update(dep_val)
                else:
                    payload[dep_key] = dep_val
        return payload


class AdapterRegistry:
    """Open adapter registry for workflow task execution.

    Adapters are registered by adapter_type string. New adapter types
    can be added at any time via register() — no code changes to this class.
    """

    def __init__(
        self,
        *,
        # Legacy named params for backward compat
        deterministic_task_adapter: DeterministicTaskAdapter | None = None,
        llm_task_adapter: TaskAdapter | None = None,
        cli_llm_adapter: TaskAdapter | None = None,
        mcp_task_adapter: TaskAdapter | None = None,
        api_task_adapter: TaskAdapter | None = None,
    ) -> None:
        self._registry: dict[str, TaskAdapter] = {}
        # Always register the deterministic fallback
        self.register("deterministic_task", deterministic_task_adapter or DeterministicTaskAdapter())
        self.register("control_operator", ControlOperatorAdapter())
        # Register legacy named adapters if provided
        if llm_task_adapter:
            self.register("llm_task", llm_task_adapter)
        if cli_llm_adapter:
            self.register("cli_llm", cli_llm_adapter)
        if mcp_task_adapter:
            self.register("mcp_task", mcp_task_adapter)
        if api_task_adapter:
            self.register("api_task", api_task_adapter)

    def register(self, adapter_type: str, adapter: TaskAdapter) -> None:
        """Register an adapter for a given type. Overwrites if already registered.

        Each adapter is composed with the default middleware chain at
        registration so cross-cutting behavior (entry cancellation guard,
        etc.) applies uniformly without the dispatcher needing to know.
        """

        from .middleware import compose_middleware

        self._registry[adapter_type] = compose_middleware(adapter)

    def resolve(self, *, adapter_type: str) -> TaskAdapter:
        adapter = self._registry.get(adapter_type)
        if adapter is not None:
            return adapter
        raise AdapterResolutionError(
            "adapter.type_unknown",
            f"no adapter registered for adapter_type={adapter_type!r}; "
            f"registered: {sorted(self._registry)}",
        )


__all__ = [
    "AdapterRegistry",
    "AdapterResolutionError",
    "ControlOperatorAdapter",
    "DeterministicTaskAdapter",
    "DeterministicTaskRequest",
    "DeterministicTaskResult",
    "TaskAdapter",
]
