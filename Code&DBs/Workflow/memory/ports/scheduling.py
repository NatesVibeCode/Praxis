"""Port for heartbeat/scheduling integration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class SchedulingResult:
    """Result of a scheduled memory maintenance cycle."""

    module_name: str
    findings: tuple[str, ...]
    actions_taken: int
    errors: tuple[str, ...]
    duration_ms: float


@runtime_checkable
class SchedulingPort(Protocol):
    """Interface for memory modules that run on a heartbeat schedule."""

    @property
    def name(self) -> str: ...

    def run(self) -> SchedulingResult: ...
