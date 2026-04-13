"""Deterministic ingestion pipeline for the memory graph.

Provides a single entry-point (IngestRouter.ingest) that validates,
redacts secrets, deduplicates, and routes payloads into the memory
engine by kind.
"""

from __future__ import annotations

import enum
import hashlib
import json
import re
import signal
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Tuple

from memory.types import Edge, Entity, EntityType, RelationType


# ---------------------------------------------------------------------------
# Enums / value objects
# ---------------------------------------------------------------------------

class IngestKind(enum.Enum):
    BUILD_EVENT = "build_event"
    EXTRACTION = "extraction"
    IMPORT = "import"
    CONVERSATION = "conversation"
    DOCUMENT = "document"


@dataclass(frozen=True)
class IngestPayload:
    kind: IngestKind
    content: str
    source: str
    metadata: dict
    timestamp: datetime
    idempotency_key: str | None = None


@dataclass(frozen=True)
class IngestResult:
    accepted: bool
    entities_created: int
    edges_created: int
    duplicates_skipped: int
    errors: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------

class SecretRedactor:
    """Deterministic regex-based secret redaction."""

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        # API keys: sk-..., AKIA..., ghp_..., ghs_...
        (re.compile(r'\bsk-(?:proj-|ant-|live-|test-)?[A-Za-z0-9]{20,}\b'), '[REDACTED_API_KEY]'),
        (re.compile(r'\bAKIA[A-Z0-9]{12,}\b'), '[REDACTED_API_KEY]'),
        (re.compile(r'\bghp_[A-Za-z0-9]{20,}\b'), '[REDACTED_API_KEY]'),
        (re.compile(r'\bghs_[A-Za-z0-9]{20,}\b'), '[REDACTED_API_KEY]'),
        # Bearer tokens
        (re.compile(r'Bearer\s+[A-Za-z0-9._\-]{20,}', re.IGNORECASE), '[REDACTED_BEARER]'),
        # password/secret in key=value (handles quotes)
        (re.compile(
            r'((?:password|secret|token|api_key|apikey|auth))\s*=\s*["\']?[^\s"\']+',
            re.IGNORECASE,
        ), r'\1=[REDACTED]'),
        # Generic long base64 blobs (40+ chars, no spaces)
        (re.compile(r'(?<![A-Za-z0-9+/=._\-])[A-Za-z0-9+/]{40,}={0,2}(?![A-Za-z0-9+/=._\-])'), '[REDACTED_TOKEN]'),
    ]

    def redact(self, text: str) -> str:
        for pattern, replacement in self._PATTERNS:
            text = pattern.sub(replacement, text)
        return text


# ---------------------------------------------------------------------------
# Timeout helper
# ---------------------------------------------------------------------------

class _IngestTimeout(Exception):
    pass


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

_DEFAULT_MAX_CONTENT_BYTES = 100 * 1024  # 100 KB


class IngestRouter:
    """Single deterministic ingress into the memory engine."""

    def __init__(
        self,
        engine,  # MemoryEngine
        *,
        max_content_bytes: int = _DEFAULT_MAX_CONTENT_BYTES,
        timeout_seconds: int = 30,
    ) -> None:
        self._engine = engine
        self._max_content_bytes = max_content_bytes
        self._timeout_seconds = timeout_seconds
        self._seen_keys: set[str] = set()
        self._redactor = SecretRedactor()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, payload: IngestPayload) -> IngestResult:
        # Idempotency check
        if payload.idempotency_key is not None:
            if payload.idempotency_key in self._seen_keys:
                return IngestResult(
                    accepted=False,
                    entities_created=0,
                    edges_created=0,
                    duplicates_skipped=1,
                    errors=(),
                )
            self._seen_keys.add(payload.idempotency_key)

        # Size check
        if len(payload.content.encode("utf-8")) > self._max_content_bytes:
            return IngestResult(
                accepted=False,
                entities_created=0,
                edges_created=0,
                duplicates_skipped=0,
                errors=(
                    f"Content exceeds max size of {self._max_content_bytes} bytes",
                ),
            )

        # Redact secrets
        clean_content = self._redactor.redact(payload.content)

        # Route by kind
        try:
            entities, edges, errors = self._route(payload.kind, clean_content, payload.source, payload.metadata)
        except Exception as exc:
            return IngestResult(
                accepted=False,
                entities_created=0,
                edges_created=0,
                duplicates_skipped=0,
                errors=(str(exc),),
            )

        # If routing produced no entities/edges and only errors, reject
        if not entities and not edges and errors:
            return IngestResult(
                accepted=False,
                entities_created=0,
                edges_created=0,
                duplicates_skipped=0,
                errors=tuple(errors),
            )

        # Insert into engine
        entity_count = 0
        edge_count = 0
        insert_errors: list[str] = list(errors)
        for ent in entities:
            try:
                self._engine.insert(ent)
                entity_count += 1
            except Exception as exc:
                insert_errors.append(f"entity insert failed ({ent.id}): {exc}")

        for edg in edges:
            try:
                self._engine.add_edge(edg)
                edge_count += 1
            except Exception as exc:
                insert_errors.append(f"edge insert failed: {exc}")

        return IngestResult(
            accepted=True,
            entities_created=entity_count,
            edges_created=edge_count,
            duplicates_skipped=0,
            errors=tuple(insert_errors),
        )

    # ------------------------------------------------------------------
    # Internal routing
    # ------------------------------------------------------------------

    def _route(
        self,
        kind: IngestKind,
        content: str,
        source: str,
        metadata: dict,
    ) -> Tuple[List[Entity], List[Edge], List[str]]:
        if kind is IngestKind.BUILD_EVENT:
            ents, edgs = self._extract_build_event_entities(content, source)
            return ents, edgs, []
        if kind is IngestKind.CONVERSATION:
            ents, edgs = self._extract_conversation_entities(content, source)
            return ents, edgs, []
        if kind is IngestKind.DOCUMENT:
            return self._build_document(content, source, metadata)
        if kind is IngestKind.EXTRACTION:
            return self._handle_extraction(content, source)
        if kind is IngestKind.IMPORT:
            return self._handle_import(content, source)
        return [], [], [f"Unknown kind: {kind}"]

    # ------------------------------------------------------------------
    # Kind-specific handlers
    # ------------------------------------------------------------------

    def _extract_build_event_entities(
        self, content: str, source: str
    ) -> Tuple[List[Entity], List[Edge]]:
        """Extract task/decision entities from structured build-event content."""
        now = datetime.now(timezone.utc)
        entities: list[Entity] = []
        edges: list[Edge] = []

        # Try JSON first
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            data = None

        if isinstance(data, dict):
            # Structured build event
            event_type = data.get("type", "task")
            etype = EntityType.decision if event_type == "decision" else EntityType.task
            eid = data.get("id", _make_id(content))
            name = data.get("name", data.get("summary", f"{etype.value}:{eid[:8]}"))
            ent = Entity(
                id=eid,
                entity_type=etype,
                name=name,
                content=content,
                metadata=data.get("metadata", {}),
                created_at=now,
                updated_at=now,
                source=source,
                confidence=0.8,
            )
            entities.append(ent)
            # Link parent if present
            parent = data.get("parent_id")
            if parent:
                edges.append(Edge(
                    source_id=eid,
                    target_id=parent,
                    relation_type=RelationType.depends_on,
                    weight=1.0,
                    metadata={},
                    created_at=now,
                ))
        else:
            # Plain-text fallback: create a single task entity
            eid = _make_id(content)
            entities.append(Entity(
                id=eid,
                entity_type=EntityType.task,
                name=f"build_event:{eid[:8]}",
                content=content,
                metadata={},
                created_at=now,
                updated_at=now,
                source=source,
                confidence=0.6,
            ))

        return entities, edges

    def _extract_conversation_entities(
        self, content: str, source: str
    ) -> Tuple[List[Entity], List[Edge]]:
        """Split conversation into messages and extract mentioned entities."""
        now = datetime.now(timezone.utc)
        entities: list[Entity] = []
        edges: list[Edge] = []

        lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
        if not lines:
            return entities, edges

        # Create a top-level conversation entity
        conv_id = _make_id(content)
        entities.append(Entity(
            id=conv_id,
            entity_type=EntityType.document,
            name=f"conversation:{conv_id[:8]}",
            content=content,
            metadata={"message_count": len(lines)},
            created_at=now,
            updated_at=now,
            source=source,
            confidence=0.7,
        ))

        # Each non-empty line is a message; extract @mentions as person entities
        mention_re = re.compile(r'@(\w+)')
        for line in lines:
            for match in mention_re.finditer(line):
                name = match.group(1)
                person_id = _make_id(f"person:{name}")
                entities.append(Entity(
                    id=person_id,
                    entity_type=EntityType.person,
                    name=name,
                    content="",
                    metadata={},
                    created_at=now,
                    updated_at=now,
                    source=source,
                    confidence=0.5,
                ))
                edges.append(Edge(
                    source_id=conv_id,
                    target_id=person_id,
                    relation_type=RelationType.related_to,
                    weight=0.5,
                    metadata={},
                    created_at=now,
                ))

        return entities, edges

    def _build_document(
        self, content: str, source: str, metadata: dict
    ) -> Tuple[List[Entity], List[Edge], List[str]]:
        now = datetime.now(timezone.utc)
        eid = _make_id(source) if source else _make_id(content)
        ent = Entity(
            id=eid,
            entity_type=EntityType.document,
            name=metadata.get("title", f"doc:{eid[:8]}"),
            content=content,
            metadata=metadata,
            created_at=now,
            updated_at=now,
            source=source,
            confidence=0.9,
        )
        return [ent], [], []

    def _handle_extraction(
        self, content: str, source: str
    ) -> Tuple[List[Entity], List[Edge], List[str]]:
        """Expect JSON with {entities: [...], edges: [...]}."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError) as exc:
            return [], [], [f"EXTRACTION requires valid JSON: {exc}"]

        if not isinstance(data, dict):
            return [], [], ["EXTRACTION payload must be a JSON object"]

        now = datetime.now(timezone.utc)
        entities: list[Entity] = []
        edges: list[Edge] = []
        errors: list[str] = []

        for raw in data.get("entities", []):
            try:
                ent = Entity(
                    id=raw.get("id", _make_id(json.dumps(raw))),
                    entity_type=EntityType(raw["entity_type"]),
                    name=raw.get("name", ""),
                    content=raw.get("content", ""),
                    metadata=raw.get("metadata", {}),
                    created_at=now,
                    updated_at=now,
                    source=source,
                    confidence=float(raw.get("confidence", 0.7)),
                )
                entities.append(ent)
            except Exception as exc:
                errors.append(f"Bad entity: {exc}")

        for raw in data.get("edges", []):
            try:
                edg = Edge(
                    source_id=raw["source_id"],
                    target_id=raw["target_id"],
                    relation_type=RelationType(raw["relation_type"]),
                    weight=float(raw.get("weight", 0.5)),
                    metadata=raw.get("metadata", {}),
                    created_at=now,
                )
                edges.append(edg)
            except Exception as exc:
                errors.append(f"Bad edge: {exc}")

        return entities, edges, errors

    def _handle_import(
        self, content: str, source: str
    ) -> Tuple[List[Entity], List[Edge], List[str]]:
        """Bulk insert entities from a JSON array."""
        try:
            items = json.loads(content)
        except (json.JSONDecodeError, TypeError) as exc:
            return [], [], [f"IMPORT requires valid JSON array: {exc}"]

        if not isinstance(items, list):
            return [], [], ["IMPORT payload must be a JSON array"]

        now = datetime.now(timezone.utc)
        entities: list[Entity] = []
        errors: list[str] = []

        for raw in items:
            try:
                ent = Entity(
                    id=raw.get("id", _make_id(json.dumps(raw))),
                    entity_type=EntityType(raw["entity_type"]),
                    name=raw.get("name", ""),
                    content=raw.get("content", ""),
                    metadata=raw.get("metadata", {}),
                    created_at=now,
                    updated_at=now,
                    source=source,
                    confidence=float(raw.get("confidence", 0.7)),
                )
                entities.append(ent)
            except Exception as exc:
                errors.append(f"Bad import entity: {exc}")

        return entities, [], errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()[:16]
