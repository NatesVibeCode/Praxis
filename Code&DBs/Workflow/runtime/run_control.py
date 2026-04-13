"""Run cancellation control for in-flight dispatches.

Manages cancellation events for active dispatch runs. This module tracks
all in-flight dispatches and allows callers to request cancellation of
a specific run via its run_id.

Module-level singleton: use get_run_control() to access.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class RunControl:
    """Thread-safe registry of in-flight run cancellation events.

    Maps run_id to a threading.Event that signals cancellation.
    When cancel(run_id) is called, the event is set and the run should
    check is_cancelled() and exit gracefully.
    """

    __slots__ = ("_active_runs", "_lock")

    def __init__(self) -> None:
        """Initialize the run control registry."""
        self._active_runs: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def register(self, run_id: str) -> threading.Event:
        """Register a new run and return its cancellation event.

        Args:
            run_id: Unique identifier for the dispatch run

        Returns:
            A threading.Event that will be set when cancellation is requested
        """
        with self._lock:
            event = threading.Event()
            self._active_runs[run_id] = event
            return event

    def cancel(self, run_id: str) -> bool:
        """Request cancellation of a run by run_id.

        Args:
            run_id: Identifier of the run to cancel

        Returns:
            True if the run was found and cancellation was requested,
            False if the run_id was not active
        """
        with self._lock:
            event = self._active_runs.get(run_id)
            if event is None:
                return False
            event.set()
            return True

    def is_cancelled(self, run_id: str) -> bool:
        """Check if a run has been requested to cancel.

        Args:
            run_id: Identifier of the run to check

        Returns:
            True if the cancellation event is set, False otherwise.
            Returns False if the run_id is not registered.
        """
        with self._lock:
            event = self._active_runs.get(run_id)
            if event is None:
                return False
            return event.is_set()

    def unregister(self, run_id: str) -> None:
        """Clean up a run from the registry after dispatch completes.

        Args:
            run_id: Identifier of the run to unregister
        """
        with self._lock:
            self._active_runs.pop(run_id, None)

    def active_run_ids(self) -> list[str]:
        """Return a list of currently in-flight run IDs.

        Returns:
            List of active run_ids, not in any particular order
        """
        with self._lock:
            return list(self._active_runs.keys())


_RUN_CONTROL = RunControl()


def get_run_control() -> RunControl:
    """Return the module-level RunControl singleton."""
    return _RUN_CONTROL
