"""Advanced heartbeat data-quality scanners.

Complements the basic scanners in heartbeat.py (StaleEntityDetector,
DuplicateScanner, OrphanEdgeCleanup, GapScanner) with deeper
relationship-integrity, schema-consistency, and content-quality checks.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from memory.engine import MemoryEngine
from memory.types import EntityType, RelationType
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_datetime(value: object) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# RelationshipIntegrityScanner
# ---------------------------------------------------------------------------

class RelationshipIntegrityScanner(HeartbeatModule):
    """Checks the edges table for structural violations:
    - relation_type not in the valid RelationType enum values
    - weight outside [0, 1]
    - self-referential edges (source_id == target_id)
    """

    def __init__(self, engine: MemoryEngine) -> None:
        self._engine = engine

    @property
    def name(self) -> str:
        return "relationship_integrity_scanner"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        findings: list[str] = []
        valid_relations = {rt.value for rt in RelationType}
        conn = self._engine._connect()
        rows = conn.execute(
            "SELECT source_id, target_id, relation_type, weight FROM memory_edges"
        )
        for row in rows:
            src = row["source_id"]
            tgt = row["target_id"]
            rel = row["relation_type"]
            weight = row["weight"]

            if rel not in valid_relations:
                findings.append(f"invalid_relation:{src}->{tgt}:{rel}")
            if weight is not None and (weight < 0 or weight > 1):
                findings.append(f"bad_weight:{src}->{tgt}:{weight}")
            if src == tgt:
                findings.append(f"self_ref:{src}")
        elapsed = (time.monotonic() - t0) * 1000
        return HeartbeatModuleResult(
            module_name=self.name,
            findings=tuple(findings),
            actions_taken=0,
            errors=(),
            duration_ms=elapsed,
        )


# ---------------------------------------------------------------------------
# SchemaConsistencyScanner
# ---------------------------------------------------------------------------

class SchemaConsistencyScanner(HeartbeatModule):
    """For each entity-type table, flags:
    - future created_at dates
    - updated_at < created_at
    - confidence outside [0, 1]
    """

    def __init__(self, engine: MemoryEngine) -> None:
        self._engine = engine

    @property
    def name(self) -> str:
        return "schema_consistency_scanner"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        findings: list[str] = []
        now = _utcnow()
        conn = self._engine._connect()
        rows = conn.execute(
            "SELECT id, created_at, updated_at, confidence FROM memory_entities"
        )
        for row in rows:
            eid = row["id"]
            created = _normalize_datetime(row["created_at"])
            updated = _normalize_datetime(row["updated_at"])
            confidence = row["confidence"]
            if created is not None and created > now:
                findings.append(f"future_created:{eid}")
            if created is not None and updated is not None:
                if updated < created:
                    findings.append(f"updated_before_created:{eid}")
            if confidence is not None and (confidence < 0 or confidence > 1):
                findings.append(f"bad_confidence:{eid}")
        elapsed = (time.monotonic() - t0) * 1000
        return HeartbeatModuleResult(
            module_name=self.name,
            findings=tuple(findings),
            actions_taken=0,
            errors=(),
            duration_ms=elapsed,
        )


# ---------------------------------------------------------------------------
# ContentQualityScanner
# ---------------------------------------------------------------------------

class ContentQualityScanner(HeartbeatModule):
    """Flags entities where:
    - content is just whitespace
    - name is a single character
    - content is an exact duplicate of another entity's content (same type)
    - metadata is not valid JSON
    """

    def __init__(self, engine: MemoryEngine) -> None:
        self._engine = engine

    @property
    def name(self) -> str:
        return "content_quality_scanner"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        findings: list[str] = []
        conn = self._engine._connect()

        for et in EntityType:
            rows = conn.execute(
                "SELECT id, name, content, metadata FROM memory_entities "
                "WHERE entity_type = $1 AND NOT archived",
                et.value,
            )

            # Track content for duplicate detection within this type
            seen_content: dict[str, str] = {}  # content -> first entity id

            for row in rows:
                eid = row["id"]
                name = row["name"] or ""
                content = row["content"] or ""
                metadata_raw = row["metadata"]

                # Whitespace-only content
                if content and not content.strip():
                    findings.append(f"whitespace_content:{eid}")

                # Single-character name
                if len(name) == 1:
                    findings.append(f"short_name:{eid}")

                # Exact duplicate content within same type
                if content.strip():
                    if content in seen_content:
                        findings.append(
                            f"dup_content:{eid}~{seen_content[content]}"
                        )
                    else:
                        seen_content[content] = eid

                # Invalid JSON metadata
                if metadata_raw is not None:
                    if isinstance(metadata_raw, (dict, list)):
                        pass  # already parsed (Postgres jsonb)
                    else:
                        try:
                            json.loads(metadata_raw)
                        except (json.JSONDecodeError, TypeError):
                            findings.append(f"bad_metadata:{eid}")

        elapsed = (time.monotonic() - t0) * 1000
        return HeartbeatModuleResult(
            module_name=self.name,
            findings=tuple(findings),
            actions_taken=0,
            errors=(),
            duration_ms=elapsed,
        )
