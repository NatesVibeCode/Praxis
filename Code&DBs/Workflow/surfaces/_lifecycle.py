"""Explicit lifecycle management for background subsystem threads.

Owns the heartbeat loop and any other daemon threads that surfaces need.
Call ``start()`` after boot; call ``stop()`` on shutdown.
"""
from __future__ import annotations

import threading
from typing import Any


class LifecycleManager:
    """Manages background threads that used to start as __init__ side effects."""

    def __init__(self) -> None:
        self._heartbeat_thread: threading.Thread | None = None
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    def start_heartbeat(
        self,
        runner: Any,
        *,
        interval_seconds: int = 300,
    ) -> None:
        """Start the heartbeat background loop if not already running."""
        if self._heartbeat_thread is not None:
            return
        thread = threading.Thread(
            target=runner.run_loop,
            kwargs={"interval_seconds": interval_seconds},
            daemon=True,
            name="heartbeat-loop",
        )
        thread.start()
        self._heartbeat_thread = thread
        self._started = True

    def stop(self) -> None:
        """Signal stop. Daemon threads die with the process, but this marks intent."""
        self._started = False
        self._heartbeat_thread = None


__all__ = ["LifecycleManager"]
