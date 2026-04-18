"""Adapter middleware protocol and built-in pipeline.

Every workflow adapter implements ``execute(request) -> DeterministicTaskResult``.
A middleware wraps that call so cross-cutting behavior — entry cancellation,
timing, future audit hooks — lives in one place rather than duplicated inside
each adapter.

Usage:

    adapter = compose_middleware(real_adapter, _DEFAULT_MIDDLEWARES)
    adapter.execute(request=...)

:class:`AdapterRegistry` wraps every registered adapter with the default
chain at registration time, so the dispatcher (``pool.submit(adapter.execute,
...)``) does not need to know middleware exists.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Callable, Protocol

from .deterministic import (
    DeterministicTaskRequest,
    DeterministicTaskResult,
    TaskAdapter,
    cancelled_task_result,
)


AdapterCall = Callable[[DeterministicTaskRequest], DeterministicTaskResult]


class AdapterMiddleware(Protocol):
    """A middleware wraps one adapter call.

    ``nxt`` is the next stage in the chain — either another middleware or the
    real adapter's ``execute``. Implementations may short-circuit (return
    without calling ``nxt``) or delegate and transform the result.
    """

    def __call__(
        self,
        request: DeterministicTaskRequest,
        nxt: AdapterCall,
        *,
        executor_type: str,
    ) -> DeterministicTaskResult: ...


def entry_cancellation_guard(
    request: DeterministicTaskRequest,
    nxt: AdapterCall,
    *,
    executor_type: str,
) -> DeterministicTaskResult:
    """Short-circuit to a cancelled result when cancel was already requested.

    Checked once before the adapter runs. Adapters that need to observe
    cancellation mid-flight (inside a retry loop, between HTTP calls) still
    call :meth:`DeterministicExecutionControl.cancel_requested` themselves —
    this guard only covers the entry window.
    """

    control = request.execution_control
    if control is not None and control.cancel_requested():
        return cancelled_task_result(
            request=request,
            executor_type=executor_type,
            started_at=datetime.now(timezone.utc),
            inputs={
                "task_name": request.task_name,
                "input_payload": dict(request.input_payload),
                "execution_boundary_ref": request.execution_boundary_ref,
            },
        )
    return nxt(request)


_DEFAULT_MIDDLEWARES: tuple[AdapterMiddleware, ...] = (
    entry_cancellation_guard,
)


class _WrappedAdapter:
    """Adapter whose ``execute`` runs through a middleware chain."""

    __slots__ = ("_inner", "_call", "executor_type")

    def __init__(self, inner: TaskAdapter, call: AdapterCall) -> None:
        self._inner = inner
        self._call = call
        self.executor_type = inner.executor_type

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        return self._call(request)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


def compose_middleware(
    adapter: TaskAdapter,
    middlewares: Sequence[AdapterMiddleware] = _DEFAULT_MIDDLEWARES,
) -> TaskAdapter:
    """Wrap ``adapter`` so ``middlewares`` run before its ``execute``.

    Middlewares run in the order given: the first middleware in the sequence
    is the outermost layer. If ``middlewares`` is empty, ``adapter`` is
    returned unchanged.
    """

    if not middlewares:
        return adapter
    if isinstance(adapter, _WrappedAdapter):
        adapter = adapter._inner  # avoid double-wrapping
    executor_type = adapter.executor_type

    def _inner_call(req: DeterministicTaskRequest) -> DeterministicTaskResult:
        return adapter.execute(request=req)

    chain: AdapterCall = _inner_call
    for middleware in reversed(middlewares):
        prev = chain

        def _stage(
            req: DeterministicTaskRequest,
            _mw: AdapterMiddleware = middleware,
            _nxt: AdapterCall = prev,
        ) -> DeterministicTaskResult:
            return _mw(req, _nxt, executor_type=executor_type)

        chain = _stage
    return _WrappedAdapter(adapter, chain)


__all__ = [
    "AdapterCall",
    "AdapterMiddleware",
    "compose_middleware",
    "entry_cancellation_guard",
]
