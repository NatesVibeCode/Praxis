"""Cross-profile bridge queries and story composition.

Enables searching across profile-scoped entity views and composing
narrative story lines from the memory graph.
"""
from __future__ import annotations

import hashlib
import enum
from dataclasses import dataclass
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from memory.types import EntityType, RelationType
from memory.retrieval_telemetry import RetrievalMetric, TelemetryStore

if TYPE_CHECKING:
    from memory.engine import MemoryEngine


# ---------------------------------------------------------------------------
# Profile views
# ---------------------------------------------------------------------------

class ProfileView(enum.Enum):
    DEVELOPER = "developer"
    STRATEGIST = "strategist"
    OPERATOR = "operator"


_PROFILE_ENTITY_TYPES: dict[ProfileView, tuple[EntityType, ...]] = {
    ProfileView.DEVELOPER: (
        EntityType.module,
        EntityType.code_unit,
        EntityType.table,
        EntityType.tool,
        EntityType.pattern,
        EntityType.constraint,
    ),
    ProfileView.STRATEGIST: (
        EntityType.decision,
        EntityType.workstream,
        EntityType.task,
        EntityType.lesson,
    ),
    ProfileView.OPERATOR: (
        EntityType.fact,
        EntityType.document,
        EntityType.action,
    ),
}


# ---------------------------------------------------------------------------
# Data envelopes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BridgeEnvelope:
    query: str
    source_profile: ProfileView
    target_profile: ProfileView
    results: tuple[dict, ...]
    confidence: float
    provenance: str


@dataclass(frozen=True)
class StoryLine:
    entity_a: str
    entity_b: str
    relation: str
    narrative: str
    strength: float


# ---------------------------------------------------------------------------
# Narrative templates
# ---------------------------------------------------------------------------

_FORWARD_TEMPLATES: dict[str, str] = {
    "depends_on": "{a} depends on {b}",
    "implements": "{a} implements {b}",
    "constrains": "{a} constrains {b}",
    "supersedes": "{a} supersedes {b}",
}

_REVERSE_TEMPLATES: dict[str, str] = {
    "depends_on": "{b} is depended on by {a}",
    "implements": "{b} is implemented by {a}",
    "constrains": "{b} is constrained by {a}",
    "supersedes": "{b} is superseded by {a}",
}


# ---------------------------------------------------------------------------
# BridgeQueryEngine
# ---------------------------------------------------------------------------

class BridgeQueryEngine:
    """Search across profile-scoped entity views."""

    def __init__(self, engine: MemoryEngine) -> None:
        self._engine = engine
        self._telemetry_store: TelemetryStore | None = None

    def cross_profile_search(
        self,
        query: str,
        source: ProfileView,
        target: ProfileView,
        limit: int = 10,
    ) -> BridgeEnvelope:
        started_at = time.monotonic()
        target_types = _PROFILE_ENTITY_TYPES[target]
        hits: list[dict] = []
        for et in target_types:
            results = self._engine.search(query, entity_type=et, limit=limit)
            for ent in results:
                hits.append({
                    "id": ent.id,
                    "name": ent.name,
                    "entity_type": ent.entity_type.value,
                    "content": ent.content,
                    "confidence": ent.confidence,
                })
            if len(hits) >= limit:
                break

        hits = hits[:limit]
        avg_conf = (
            sum(h["confidence"] for h in hits) / len(hits) if hits else 0.0
        )
        self._record_telemetry(
            query=query,
            pattern_name="bridge.cross_profile_search",
            result_count=len(hits),
            started_at=started_at,
        )
        return BridgeEnvelope(
            query=query,
            source_profile=source,
            target_profile=target,
            results=tuple(hits),
            confidence=round(avg_conf, 4),
            provenance=f"{source.value}->{target.value}",
        )

    def _record_telemetry(
        self,
        *,
        query: str,
        pattern_name: str,
        result_count: int,
        started_at: float,
    ) -> None:
        try:
            store = self._telemetry_store
            if store is None:
                store = TelemetryStore(self._engine._connect())
                self._telemetry_store = store
            store.record(
                RetrievalMetric(
                    query_fingerprint=hashlib.sha256(query.encode()).hexdigest()[:8],
                    pattern_name=pattern_name,
                    result_count=result_count,
                    score_min=0.0,
                    score_max=0.0,
                    score_mean=0.0,
                    score_stddev=0.0,
                    tie_break_count=0,
                    latency_ms=(time.monotonic() - started_at) * 1000.0,
                    timestamp=datetime.now(timezone.utc),
                )
            )
        except Exception:
            pass

    def explain_relationship(
        self, entity_a_id: str, entity_b_id: str
    ) -> StoryLine | None:
        edges = self._engine.get_edges(entity_a_id, direction="both")
        for edge in edges:
            if edge.source_id == entity_a_id and edge.target_id == entity_b_id:
                rel = edge.relation_type.value
                tpl = _FORWARD_TEMPLATES.get(rel, "{a} is related to {b}")
                narrative = tpl.format(a=entity_a_id, b=entity_b_id)
                return StoryLine(
                    entity_a=entity_a_id,
                    entity_b=entity_b_id,
                    relation=rel,
                    narrative=narrative,
                    strength=edge.weight,
                )
            if edge.source_id == entity_b_id and edge.target_id == entity_a_id:
                rel = edge.relation_type.value
                tpl = _REVERSE_TEMPLATES.get(rel, "{a} is related to {b}")
                narrative = tpl.format(a=entity_b_id, b=entity_a_id)
                return StoryLine(
                    entity_a=entity_a_id,
                    entity_b=entity_b_id,
                    relation=rel,
                    narrative=narrative,
                    strength=edge.weight,
                )
        return None


# ---------------------------------------------------------------------------
# StoryComposer
# ---------------------------------------------------------------------------

class StoryComposer:
    """Compose narrative story lines for an entity from its graph edges."""

    def __init__(self, engine: MemoryEngine) -> None:
        self._engine = engine

    def compose(self, entity_id: str, max_lines: int = 5) -> list[StoryLine]:
        edges = self._engine.get_edges(entity_id, direction="both")
        lines: list[StoryLine] = []
        for edge in edges:
            rel = edge.relation_type.value
            if edge.source_id == entity_id:
                tpl = _FORWARD_TEMPLATES.get(rel, "{a} is related to {b}")
                narrative = tpl.format(a=entity_id, b=edge.target_id)
                peer = edge.target_id
            else:
                tpl = _REVERSE_TEMPLATES.get(rel, "{a} is related to {b}")
                narrative = tpl.format(a=edge.source_id, b=entity_id)
                peer = edge.source_id
            lines.append(
                StoryLine(
                    entity_a=entity_id,
                    entity_b=peer,
                    relation=rel,
                    narrative=narrative,
                    strength=edge.weight,
                )
            )
        lines.sort(key=lambda s: s.strength, reverse=True)
        return lines[:max_lines]
