"""Stub: native primary cutover gate — not yet implemented.

Provides the interface that operator_write.py imports so the API server boots.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol


@dataclass
class NativePrimaryCutoverGateRecord:
    gate_id: str = ""
    roadmap_item_id: str | None = None
    workflow_class_id: str | None = None
    decided_by: str = ""
    decision_source: str = ""
    rationale: str = ""
    status: str = "stub"

    def to_json(self) -> dict[str, Any]:
        return {
            "gate_id": self.gate_id,
            "roadmap_item_id": self.roadmap_item_id,
            "workflow_class_id": self.workflow_class_id,
            "decided_by": self.decided_by,
            "status": self.status,
        }


class NativePrimaryCutoverRepository(Protocol):
    pass


class PostgresNativePrimaryCutoverRepository:
    def __init__(self, conn: Any) -> None:
        self._conn = conn


class NativePrimaryCutoverRuntime:
    def __init__(self, *, repository: Any) -> None:
        self._repository = repository

    async def admit_gate(self, **kwargs: Any) -> NativePrimaryCutoverGateRecord:
        return NativePrimaryCutoverGateRecord(
            gate_id="stub",
            decided_by=str(kwargs.get("decided_by", "")),
            rationale=str(kwargs.get("rationale", "")),
        )
