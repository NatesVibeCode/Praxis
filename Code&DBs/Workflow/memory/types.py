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


@dataclass
class ChangeSet:
    inserts: list[Entity] = field(default_factory=list)
    updates: list[Entity] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    edges_add: list[Edge] = field(default_factory=list)
    edges_remove: list[tuple[str, str, RelationType]] = field(default_factory=list)
