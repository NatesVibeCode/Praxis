"""Unit tests for :mod:`adapters.middleware`.

Covers:
  - ``entry_cancellation_guard`` short-circuits to a ``cancelled`` result
    without calling the underlying adapter when cancel was already requested.
  - The guard passes through to the adapter when cancellation is not set.
  - ``compose_middleware`` chains middlewares in order — the first in the
    sequence is the outermost layer.
  - ``AdapterRegistry.register`` applies the default chain, so a cancelled
    request never reaches the registered adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from adapters.deterministic import (
    AdapterRegistry,
    DeterministicExecutionControl,
    DeterministicTaskRequest,
    DeterministicTaskResult,
)
from adapters.middleware import (
    compose_middleware,
    entry_cancellation_guard,
)


_BASE_NOW = datetime(2026, 4, 17, 15, 0, 0, tzinfo=timezone.utc)


def _request(
    *,
    execution_control: DeterministicExecutionControl | None = None,
) -> DeterministicTaskRequest:
    return DeterministicTaskRequest(
        node_id="node-a",
        task_name="task-a",
        input_payload={"k": "v"},
        expected_outputs={},
        dependency_inputs={},
        execution_boundary_ref="boundary-a",
        execution_control=execution_control,
    )


@dataclass
class _RecordingAdapter:
    executor_type: str = "adapter.recording"
    calls: list[DeterministicTaskRequest] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.calls = []

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        self.calls.append(request)
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="ok",
            executor_type=self.executor_type,
            inputs={"task_name": request.task_name},
            outputs={"ran": True},
            started_at=_BASE_NOW,
            finished_at=_BASE_NOW,
        )


def test_entry_cancellation_guard_short_circuits_when_cancelled() -> None:
    control = DeterministicExecutionControl()
    control.request_cancel()
    inner = _RecordingAdapter()

    wrapped = compose_middleware(inner)
    result = wrapped.execute(request=_request(execution_control=control))

    assert inner.calls == []
    assert result.status == "cancelled"
    assert result.failure_code == "workflow_cancelled"
    assert result.executor_type == "adapter.recording"


def test_entry_cancellation_guard_passes_through_when_not_cancelled() -> None:
    inner = _RecordingAdapter()
    wrapped = compose_middleware(inner)

    result = wrapped.execute(request=_request())

    assert len(inner.calls) == 1
    assert result.status == "succeeded"


def test_compose_middleware_runs_middlewares_in_given_order() -> None:
    inner = _RecordingAdapter()
    order: list[str] = []

    def _outer(request, nxt, *, executor_type):
        order.append("outer-pre")
        result = nxt(request)
        order.append("outer-post")
        return result

    def _inner_mw(request, nxt, *, executor_type):
        order.append("inner-pre")
        result = nxt(request)
        order.append("inner-post")
        return result

    wrapped = compose_middleware(inner, (_outer, _inner_mw))
    wrapped.execute(request=_request())

    assert order == ["outer-pre", "inner-pre", "inner-post", "outer-post"]


def test_compose_middleware_with_empty_chain_returns_adapter_unchanged() -> None:
    inner = _RecordingAdapter()
    wrapped = compose_middleware(inner, ())

    assert wrapped is inner


def test_compose_middleware_avoids_double_wrapping() -> None:
    inner = _RecordingAdapter()
    once = compose_middleware(inner)
    twice = compose_middleware(once)

    control = DeterministicExecutionControl()
    control.request_cancel()
    result = twice.execute(request=_request(execution_control=control))

    assert result.status == "cancelled"
    assert inner.calls == []


def test_wrapped_adapter_forwards_attribute_access() -> None:
    @dataclass
    class _AdapterWithExtras:
        executor_type: str = "adapter.extras"
        marker: str = "hello"

        def execute(self, *, request):  # type: ignore[no-untyped-def]
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="ok",
                executor_type=self.executor_type,
                inputs={},
                outputs={},
                started_at=_BASE_NOW,
                finished_at=_BASE_NOW,
            )

    inner = _AdapterWithExtras()
    wrapped = compose_middleware(inner)

    assert wrapped.executor_type == "adapter.extras"
    assert wrapped.marker == "hello"


def test_adapter_registry_wraps_registered_adapter_with_default_chain() -> None:
    inner = _RecordingAdapter()
    registry = AdapterRegistry()
    registry.register("recording", inner)

    resolved = registry.resolve(adapter_type="recording")
    assert resolved is not inner

    control = DeterministicExecutionControl()
    control.request_cancel()
    result = resolved.execute(request=_request(execution_control=control))

    assert result.status == "cancelled"
    assert inner.calls == []


def test_entry_cancellation_guard_directly_invokable() -> None:
    control = DeterministicExecutionControl()
    control.request_cancel()

    def _nxt(_req: DeterministicTaskRequest) -> DeterministicTaskResult:
        raise AssertionError("adapter should not be called when cancelled")

    result = entry_cancellation_guard(
        _request(execution_control=control),
        _nxt,
        executor_type="adapter.test",
    )

    assert result.status == "cancelled"
    assert result.executor_type == "adapter.test"
