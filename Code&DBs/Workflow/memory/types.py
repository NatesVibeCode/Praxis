from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class EntityType(enum.Enum):
    person = "person"
    topic = "topic"
    decision = "decision"
    preference = "preference"
    constraint = "constraint"
    fact = "fact"
    task = "task"
    document = "document"
    workstream = "workstream"
    action = "action"
    lesson = "lesson"
    pattern = "pattern"
    module = "module"
    table = "table"
    code_unit = "code_unit"
    tool = "tool"
    metric = "metric"


class RelationType(enum.Enum):
    depends_on = "depends_on"
    constrains = "constrains"
    implements = "implements"
    supersedes = "supersedes"
    related_to = "related_to"
    derived_from = "derived_from"
    caused_by = "caused_by"
    correlates_with = "correlates_with"
    produced = "produced"
    triggered = "triggered"
    covers = "covers"
    verified_by = "verified_by"
    recorded_in = "recorded_in"
    regressed_from = "regressed_from"
    semantic_neighbor = "semantic_neighbor"


class EdgeAuthorityClass(enum.Enum):
    canonical = "canonical"
    enrichment = "enrichment"


class EdgeProvenanceKind(enum.Enum):
    legacy_unspecified = "legacy_unspecified"
    structured_ingest = "structured_ingest"
    conversation_extraction = "conversation_extraction"
    receipt_projection = "receipt_projection"
    verification_projection = "verification_projection"
    failure_projection = "failure_projection"
    constraint_projection = "constraint_projection"
    friction_projection = "friction_projection"
    schema_projection = "schema_projection"
    import_graph_projection = "import_graph_projection"
    heuristic_extraction = "heuristic_extraction"
    relationship_mining = "relationship_mining"
    rollup_inference = "rollup_inference"


@dataclass(frozen=True)
class Entity:
    id: str
    entity_type: EntityType
    name: str
    content: str
    metadata: dict
    created_at: datetime
    updated_at: datetime
    source: str
    confidence: float  # 0-1


@dataclass(frozen=True)
class Edge:
    source_id: str
    target_id: str
    relation_type: RelationType
    weight: float  # 0-1
    metadata: dict
    created_at: datetime
    authority_class: EdgeAuthorityClass = EdgeAuthorityClass.enrichment
    provenance_kind: EdgeProvenanceKind = EdgeProvenanceKind.legacy_unspecified
    provenance_ref: str | None = None


def canonical_edge(
    *,
    source_id: str,
    target_id: str,
    relation_type: RelationType,
    weight: float,
    metadata: dict,
    created_at: datetime,
    provenance_kind: EdgeProvenanceKind,
    provenance_ref: str | None = None,
) -> Edge:
    return Edge(
        source_id=source_id,
        target_id=target_id,
        relation_type=relation_type,
        weight=weight,
        metadata=metadata,
        created_at=created_at,
        authority_class=EdgeAuthorityClass.canonical,
        provenance_kind=provenance_kind,
        provenance_ref=provenance_ref,
    )


def enrichment_edge(
    *,
    source_id: str,
    target_id: str,
    relation_type: RelationType,
    weight: float,
    metadata: dict,
    created_at: datetime,
    provenance_kind: EdgeProvenanceKind,
    provenance_ref: str | None = None,
) -> Edge:
    return Edge(
        source_id=source_id,
        target_id=target_id,
        relation_type=relation_type,
        weight=weight,
        metadata=metadata,
        created_at=created_at,
        authority_class=EdgeAuthorityClass.enrichment,
        provenance_kind=provenance_kind,
        provenance_ref=provenance_ref,
    )


@dataclass
class ChangeSet:
    inserts: list[Entity] = field(default_factory=list)
    updates: list[Entity] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    edges_add: list[Edge] = field(default_factory=list)
    edges_remove: list[tuple[str, str, RelationType]] = field(default_factory=list)
