"""Ownership seam for canonical memory-graph mutations."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from typing import Protocol

if TYPE_CHECKING:
    from memory.engine import MemoryEngine


@dataclass(frozen=True)
class MemoryEdgeRef:
    """Stable identifier for one canonical memory edge."""

    source_id: str
    target_id: str
    relation_type: str


class MemoryGraphMutationRepository(Protocol):
    """Owns canonical memory-graph mutations on behalf of runtime modules."""

    def archive_entities(self, *, entity_ids: Sequence[str]) -> tuple[str, ...]:
        """Archive the addressed entities and return the ids actually mutated."""

    def delete_edges(self, *, edges: Sequence[MemoryEdgeRef]) -> tuple[MemoryEdgeRef, ...]:
        """Delete the addressed edges and return the edges actually removed."""

    def mark_entity_embedding_failed(self, *, entity_id: str) -> bool:
        """Mark one entity as failed for embedding maintenance."""

    def mark_entity_embedding_ready(self, *, entity_id: str, embedding_model: str) -> bool:
        """Mark one entity as embedding-ready after the vector write succeeds."""

    def touch_entity_maintenance(self, *, entity_id: str) -> bool:
        """Refresh maintenance timestamps for one entity."""

    def absorb_exact_duplicate_entities(
        self,
        *,
        canonical_entity_id: str,
        duplicate_entity_ids: Sequence[str],
    ) -> dict[str, object]:
        """Collapse exact-duplicate entities into the canonical survivor."""

    def replace_vector_neighbor_projection(
        self,
        *,
        source_entity_id: str,
        neighbors: Sequence[Mapping[str, object]],
        policy_key: str,
        embedding_version: int,
    ) -> int:
        """Replace the vector-neighbor projection for one source entity."""


def resolve_memory_graph_mutation_repository(
    engine: "MemoryEngine",
) -> MemoryGraphMutationRepository:
    """Resolve the canonical mutation owner for memory-graph maintenance flows."""

    from storage.postgres.memory_graph_repository import PostgresMemoryGraphRepository

    return PostgresMemoryGraphRepository(engine._connect())
