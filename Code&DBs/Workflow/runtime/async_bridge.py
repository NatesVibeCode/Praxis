"""Sync→async bridge that's safe to call from inside a running event loop.

Why this exists:
    Many runtime helpers, MCP wizards, and CQRS command handlers are
    synchronous functions that internally need to drive an async coroutine
    (asyncpg connection, async repository, etc.). The historical pattern
    was a bare ``asyncio.run(coro)`` at the top of the helper. That works
    when invoked from a CLI script (no parent loop), but RuntimeErrors
    with ``cannot be called from a running event loop`` whenever the same
    helper is reached from an MCP HTTP handler (api-server runs an
    asyncio loop) or any other async context.

    Symptoms operators saw:
      * ``praxis_provider_onboard`` returning ``RuntimeError: asyncio.run()
        cannot be called from a running event loop``
      * ``praxis_operator_roadmap_view`` returning
        ``operator_query.async_boundary_required``

    Both classes of error trace back to the same anti-pattern: bare
    ``asyncio.run`` in a code path that is reachable from both sync CLI
    and async MCP entrypoints.

What this provides:
    ``run_sync_safe(coro)`` — runs the coroutine to completion and returns
    its result. Detects whether a parent event loop is running on the
    current thread and picks the right strategy:

      * No running loop: uses ``asyncio.run`` directly.
      * Running loop: spawns a short-lived worker thread, runs
        ``asyncio.run(coro)`` on the worker's own loop, blocks the calling
        thread until it completes, and propagates the result/exception.

    The worker-thread strategy works because each thread can host its own
    event loop independently. Spawning a thread per call is fine for the
    targeted use cases (wizard handlers, one-shot CQRS commands) where
    the surrounding work already takes orders of magnitude longer than
    thread setup (~50–100µs).

    For high-frequency hot-path code that needs a long-lived bridge thread
    with persistent asyncpg connections, see ``_SyncAsyncBridge`` in
    ``persistent_evidence.py`` instead.

Standing-order references:
    architecture-policy::agent-behavior::no-bare-asyncio-run-in-mcp-paths
    BUG class: wizard async-boundary footgun across MCP entrypoints
"""

from __future__ import annotations

import asyncio
import threading
from typing import Awaitable, TypeVar

T = TypeVar("T")


def run_sync_safe(coro: Awaitable[T]) -> T:
    """Run ``coro`` to completion from sync code, even if a parent loop is running.

    Use this anywhere a synchronous function needs to drive a coroutine and
    might be invoked from either CLI (no loop) or MCP/HTTP (loop running).
    Replaces the ``asyncio.run(coro)`` anti-pattern.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)  # type: ignore[arg-type]

    result_box: list[T] = []
    exc_box: list[BaseException] = []

    def _runner() -> None:
        try:
            result_box.append(asyncio.run(coro))  # type: ignore[arg-type]
        except BaseException as exc:
            exc_box.append(exc)

    worker = threading.Thread(target=_runner, name="praxis-async-bridge", daemon=True)
    worker.start()
    worker.join()
    if exc_box:
        raise exc_box[0]
    return result_box[0]


__all__ = ["run_sync_safe"]
