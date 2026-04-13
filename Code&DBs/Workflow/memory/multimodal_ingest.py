"""Multimodal partner-data ingest with posture-gated writes."""

from __future__ import annotations

import csv
import io
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from storage.postgres import SyncPostgresConnection, ensure_postgres_available


class PostureMode(Enum):
    """Controls write behaviour of the ingester."""

    OBSERVE = "observe"
    OPERATE = "operate"
    BUILD = "build"


class IngestSource(Enum):
    """Supported ingest source types."""

    MEETING_TRANSCRIPT = "meeting_transcript"
    CRM_EXPORT = "crm_export"
    PROFILE_DOCUMENT = "profile_document"
    GENERIC_STRUCTURED = "generic_structured"


@dataclass(frozen=True)
class MultimodalPayload:
    source_type: IngestSource
    content: str
    metadata: dict = field(default_factory=dict)
    posture: PostureMode = PostureMode.OBSERVE
    dry_run: bool = False


@dataclass(frozen=True)
class IngestReceipt:
    payload_source: str
    entities_classified: int
    entities_written: int
    skipped_reason: str | None
    timestamp: datetime


class PostgresMultimodalIngestStore:
    """Persist multimodal ingest entities to Postgres staging rows."""

    def __init__(self, conn: SyncPostgresConnection | None = None) -> None:
        self._conn = conn or ensure_postgres_available()

    def write_entities(
        self,
        *,
        source_type: IngestSource,
        posture: PostureMode,
        entities: list[dict],
        metadata: dict[str, Any] | None = None,
    ) -> int:
        if not entities:
            return 0
        now = datetime.now(timezone.utc)
        rows = [
            (
                f"mmi:{uuid.uuid4().hex}",
                source_type.value,
                posture.value,
                str(entity.get("type") or "unknown"),
                json.dumps({"entity": entity, "metadata": metadata or {}}, sort_keys=True, default=str),
                now,
            )
            for entity in entities
        ]
        self._conn.execute_many(
            """
            INSERT INTO multimodal_ingest_staging (
                staging_id, source_type, posture, entity_type, entity_data, recorded_at
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            """,
            rows,
        )
        return len(rows)


class MultimodalIngester:
    """Classifies and optionally writes entities from multimodal payloads."""

    def __init__(self, engine: Any | None = None) -> None:
        self._engine = engine

    def ingest(self, payload: MultimodalPayload) -> IngestReceipt:
        entities = self.classify(payload.content, payload.source_type)
        classified = len(entities)
        now = datetime.now(timezone.utc)

        effective_posture = PostureMode.OBSERVE if payload.dry_run else payload.posture

        if effective_posture == PostureMode.OBSERVE:
            return IngestReceipt(
                payload_source=payload.source_type.value,
                entities_classified=classified,
                entities_written=0,
                skipped_reason="observe-only mode" if not payload.dry_run else "dry_run",
                timestamp=now,
            )

        written = self._write_entities(payload, entities, effective_posture)
        return IngestReceipt(
            payload_source=payload.source_type.value,
            entities_classified=classified,
            entities_written=written,
            skipped_reason=None,
            timestamp=now,
        )

    def classify(self, content: str, source_type: IngestSource) -> list[dict]:
        dispatch = {
            IngestSource.MEETING_TRANSCRIPT: self._classify_transcript,
            IngestSource.CRM_EXPORT: self._classify_crm,
            IngestSource.PROFILE_DOCUMENT: self._classify_profile,
            IngestSource.GENERIC_STRUCTURED: self._classify_generic,
        }
        return dispatch[source_type](content)

    @staticmethod
    def _classify_transcript(content: str) -> list[dict]:
        entities: list[dict] = []
        lines = content.splitlines()
        current_speaker: str | None = None
        speaker_re = re.compile(r"^([A-Za-z][\w\s]*?):\s*(.*)")

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            speaker_match = speaker_re.match(stripped)
            if speaker_match:
                current_speaker = speaker_match.group(1).strip()
                text = speaker_match.group(2).strip()
            else:
                text = stripped

            upper = text.upper()
            if "TODO" in upper or "ACTION" in upper:
                entities.append({"type": "action_item", "speaker": current_speaker, "text": text})
            elif "DECISION" in upper or "DECIDED" in upper:
                entities.append({"type": "decision", "speaker": current_speaker, "text": text})

        return entities

    @staticmethod
    def _classify_crm(content: str) -> list[dict]:
        entities: list[dict] = []
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            lower_keys = {k.lower(): v for k, v in row.items()}
            name = lower_keys.get("name", "").strip()
            org = lower_keys.get("organization", lower_keys.get("company", "")).strip()
            if name:
                entities.append({"type": "person", "name": name, "organization": org})
            if org:
                entities.append({"type": "organization", "name": org})
        return entities

    @staticmethod
    def _classify_profile(content: str) -> list[dict]:
        data: dict[str, str] = {}
        for line in content.splitlines():
            if ":" in line:
                key, _, value = line.partition(":")
                data[key.strip().lower()] = value.strip()
        name = data.get("name", "")
        if not name:
            return []
        return [{
            "type": "person",
            "name": name,
            "role": data.get("role", data.get("title", "")),
            "organization": data.get("organization", data.get("org", data.get("company", ""))),
        }]

    @staticmethod
    def _classify_generic(content: str) -> list[dict]:
        try:
            parsed = json.loads(content)
            if isinstance(parsed, list):
                return [
                    {"type": "structured", **item} if isinstance(item, dict)
                    else {"type": "structured", "value": item}
                    for item in parsed
                ]
            if isinstance(parsed, dict):
                return [{"type": "structured", **parsed}]
        except (json.JSONDecodeError, TypeError):
            pass

        entities: list[dict] = []
        for line in content.splitlines():
            stripped = line.strip()
            if stripped:
                entities.append({"type": "line", "text": stripped})
        return entities

    def _write_entities(
        self,
        payload: MultimodalPayload,
        entities: list[dict],
        posture: PostureMode,
    ) -> int:
        if not entities:
            return 0
        writer = getattr(self._resolved_engine(), "write_entities", None)
        if callable(writer):
            try:
                return int(
                    writer(
                        source_type=payload.source_type,
                        posture=posture,
                        entities=entities,
                        metadata=dict(payload.metadata),
                    )
                )
            except TypeError:
                return int(writer(entities, posture))
        raise TypeError("MultimodalIngester engine must expose write_entities(...)")

    def _resolved_engine(self) -> Any:
        if self._engine is None:
            self._engine = PostgresMultimodalIngestStore()
        return self._engine
