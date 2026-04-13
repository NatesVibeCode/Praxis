"""Runtime-owned control-operator frame state.

Control operators expand dynamic item / iteration frames that need one obvious
runtime authority. The deterministic runtime defaults to an in-memory
repository, while canonical persisted execution can inject a Postgres-backed
authority explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class RunOperatorFrame:
    """One runtime-owned item / iteration frame for a control operator."""

    operator_frame_id: str
    run_id: str
    node_id: str
    operator_kind: str
    frame_state: str
    item_index: int | None
    iteration_index: int | None
    source_snapshot: dict[str, Any]
    aggregate_outputs: dict[str, Any]
    active_count: int
    stop_reason: str | None
    started_at: datetime
    finished_at: datetime | None = None


class OperatorFrameRepository(Protocol):
    """Runtime authority required for dynamic control-operator frame state."""

    def create_frame(
        self,
        *,
        operator_frame_id: str,
        run_id: str,
        node_id: str,
        operator_kind: str,
        item_index: int | None,
        iteration_index: int | None,
        source_snapshot: dict[str, Any] | None = None,
        active_count: int = 0,
    ) -> RunOperatorFrame:
        """Persist one newly created frame."""

    def mark_running(
        self,
        *,
        operator_frame_id: str,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        """Mark a frame as actively executing."""

    def mark_succeeded(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        """Persist one successful frame terminal state."""

    def mark_failed(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        """Persist one failed frame terminal state."""

    def mark_cancelled(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        """Persist one cancelled frame terminal state."""

    def list_for_node(self, *, run_id: str, node_id: str) -> tuple[RunOperatorFrame, ...]:
        """Return all persisted frames for one operator node in stable order."""

    def list_for_run(self, *, run_id: str) -> tuple[RunOperatorFrame, ...]:
        """Return all persisted frames for one run in stable order."""

    def clear_for_run(self, *, run_id: str) -> int:
        """Delete any existing frames for one run before a fresh retry/replay attempt."""


class InMemoryOperatorFrameRepository:
    """Small in-memory authority for deterministic control-operator frames."""

    def __init__(self) -> None:
        self._frames: dict[str, RunOperatorFrame] = {}

    def create_frame(
        self,
        *,
        operator_frame_id: str,
        run_id: str,
        node_id: str,
        operator_kind: str,
        item_index: int | None,
        iteration_index: int | None,
        source_snapshot: dict[str, Any] | None = None,
        active_count: int = 0,
    ) -> RunOperatorFrame:
        frame = RunOperatorFrame(
            operator_frame_id=operator_frame_id,
            run_id=run_id,
            node_id=node_id,
            operator_kind=operator_kind,
            frame_state="created",
            item_index=item_index,
            iteration_index=iteration_index,
            source_snapshot=dict(source_snapshot or {}),
            aggregate_outputs={},
            active_count=max(0, int(active_count)),
            stop_reason=None,
            started_at=_utc_now(),
            finished_at=None,
        )
        self._frames[operator_frame_id] = frame
        return frame

    def mark_running(
        self,
        *,
        operator_frame_id: str,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        frame = self._require(operator_frame_id)
        updated = replace(
            frame,
            frame_state="running",
            active_count=frame.active_count if active_count is None else max(0, int(active_count)),
        )
        self._frames[operator_frame_id] = updated
        return updated

    def mark_succeeded(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        frame = self._require(operator_frame_id)
        updated = replace(
            frame,
            frame_state="succeeded",
            aggregate_outputs=dict(aggregate_outputs or {}),
            stop_reason=stop_reason,
            active_count=frame.active_count if active_count is None else max(0, int(active_count)),
            finished_at=_utc_now(),
        )
        self._frames[operator_frame_id] = updated
        return updated

    def mark_failed(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        frame = self._require(operator_frame_id)
        updated = replace(
            frame,
            frame_state="failed",
            aggregate_outputs=dict(aggregate_outputs or {}),
            stop_reason=stop_reason,
            active_count=frame.active_count if active_count is None else max(0, int(active_count)),
            finished_at=_utc_now(),
        )
        self._frames[operator_frame_id] = updated
        return updated

    def mark_cancelled(
        self,
        *,
        operator_frame_id: str,
        aggregate_outputs: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        active_count: int | None = None,
    ) -> RunOperatorFrame:
        frame = self._require(operator_frame_id)
        updated = replace(
            frame,
            frame_state="cancelled",
            aggregate_outputs=dict(aggregate_outputs or {}),
            stop_reason=stop_reason,
            active_count=frame.active_count if active_count is None else max(0, int(active_count)),
            finished_at=_utc_now(),
        )
        self._frames[operator_frame_id] = updated
        return updated

    def list_for_node(self, *, run_id: str, node_id: str) -> tuple[RunOperatorFrame, ...]:
        frames = [
            frame
            for frame in self._frames.values()
            if frame.run_id == run_id and frame.node_id == node_id
        ]
        frames.sort(
            key=lambda frame: (
                frame.item_index if frame.item_index is not None else -1,
                frame.iteration_index if frame.iteration_index is not None else -1,
                frame.operator_frame_id,
            )
        )
        return tuple(frames)

    def list_for_run(self, *, run_id: str) -> tuple[RunOperatorFrame, ...]:
        frames = [
            frame
            for frame in self._frames.values()
            if frame.run_id == run_id
        ]
        frames.sort(
            key=lambda frame: (
                frame.node_id,
                frame.item_index if frame.item_index is not None else -1,
                frame.iteration_index if frame.iteration_index is not None else -1,
                frame.operator_frame_id,
            )
        )
        return tuple(frames)

    def clear_for_run(self, *, run_id: str) -> int:
        frame_ids = [
            frame.operator_frame_id
            for frame in self._frames.values()
            if frame.run_id == run_id
        ]
        for operator_frame_id in frame_ids:
            del self._frames[operator_frame_id]
        return len(frame_ids)

    def _require(self, operator_frame_id: str) -> RunOperatorFrame:
        frame = self._frames.get(operator_frame_id)
        if frame is None:
            raise KeyError(f"unknown operator_frame_id={operator_frame_id!r}")
        return frame

__all__ = [
    "OperatorFrameRepository",
    "InMemoryOperatorFrameRepository",
    "RunOperatorFrame",
]
