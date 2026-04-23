"""Friction Ledger — tracks guardrail bounces, warnings, and hard failures."""

from __future__ import annotations

import enum
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection
    from runtime.embedding_service import EmbeddingService
    from storage.postgres.friction_repository import PostgresFrictionRepository

from storage.postgres.vector_store import PostgresVectorStore, decode_vector_value


class FrictionType(enum.Enum):
    GUARDRAIL_BOUNCE = "guardrail_bounce"
    WARN_ONLY = "warn_only"
    HARD_FAILURE = "hard_failure"


@dataclass(frozen=True)
class FrictionEvent:
    event_id: str
    friction_type: FrictionType
    source: str
    job_label: str
    message: str
    timestamp: datetime
    is_test: bool = False


@dataclass(frozen=True)
class FrictionStats:
    total: int
    by_type: dict
    by_source: dict
    bounce_rate: float


@dataclass(frozen=True)
class FrictionPattern:
    """Deterministic grouping of repeated friction events."""

    fingerprint: str
    count: int
    sources: tuple[str, ...]
    job_labels: tuple[str, ...]
    reason_code: str
    command: str
    sample: str
    first_seen: datetime
    last_seen: datetime
    event_ids: tuple[str, ...]
    promotion_candidate: bool

    def to_json(self) -> dict:
        return {
            "fingerprint": self.fingerprint,
            "count": self.count,
            "sources": list(self.sources),
            "job_labels": list(self.job_labels),
            "reason_code": self.reason_code,
            "command": self.command,
            "sample": self.sample,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "event_ids": list(self.event_ids),
            "promotion_candidate": self.promotion_candidate,
        }


class FrictionLedger:
    """Postgres-backed ledger for friction events."""

    def __init__(
        self,
        conn: "SyncPostgresConnection",
        embedder: Optional["EmbeddingService"] = None,
    ) -> None:
        self._conn = conn
        self._embedder = embedder
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )

    @property
    def _repository(self) -> "PostgresFrictionRepository":
        from storage.postgres.friction_repository import PostgresFrictionRepository

        return PostgresFrictionRepository(self._conn)

    def record(
        self,
        friction_type: FrictionType,
        source: str,
        job_label: str,
        message: str,
        is_test: bool = False,
    ) -> FrictionEvent:
        event_id = uuid.uuid4().hex[:12]
        ts = datetime.now(timezone.utc)

        vector_query = None
        if self._vector_store is not None:
            try:
                embed_text = friction_type.value + " " + message
                vector_query = self._vector_store.prepare(embed_text)
            except Exception:
                vector_query = None

        self._repository.record_friction_event(
            event_id=event_id,
            friction_type=friction_type.value,
            source=source,
            job_label=job_label,
            message=message,
            timestamp=ts,
            is_test=is_test,
        )
        if vector_query is not None:
            vector_query.set_embedding("friction_events", "event_id", event_id)
        return FrictionEvent(
            event_id=event_id, friction_type=friction_type,
            source=source, job_label=job_label, message=message, timestamp=ts,
            is_test=is_test,
        )

    def cluster_patterns(
        self,
        since_hours: int = 24,
        threshold: float = 0.82,
        limit: int = 10,
    ) -> list[dict]:
        """Group recent friction events by semantic similarity.

        Returns clusters like:
        [{'pattern': 'representative message', 'count': 15, 'sources': ['a', 'b'], 'event_ids': [...]}]
        """
        if self._embedder is None:
            return []

        try:
            import numpy as np
        except ImportError:
            return []

        rows = self._repository.list_embedded_events_since(
            since=datetime.now(timezone.utc) - timedelta(hours=since_hours),
        )
        if not rows:
            return []

        # Parse embedding strings to numpy arrays
        events: list[dict] = []
        for r in rows:
            raw = r["embedding"]
            if raw is None:
                continue
            vec = np.array(decode_vector_value(raw), dtype=np.float32)
            events.append({
                "event_id": r["event_id"],
                "source": r["source"],
                "message": r["message"],
                "vec": vec,
            })

        if not events:
            return []

        # Greedy clustering: compare each event against cluster centroids
        clusters: list[dict] = []  # {centroid, messages, sources, event_ids, count}

        for ev in events:
            best_idx = -1
            best_sim = -1.0
            for i, cl in enumerate(clusters):
                c = cl["centroid"]
                sim = float(np.dot(ev["vec"], c) / (np.linalg.norm(ev["vec"]) * np.linalg.norm(c) + 1e-10))
                if sim > best_sim:
                    best_sim = sim
                    best_idx = i

            if best_sim >= threshold and best_idx >= 0:
                cl = clusters[best_idx]
                cl["count"] += 1
                cl["event_ids"].append(ev["event_id"])
                cl["sources"].add(ev["source"])
                # Update centroid as running mean
                n = cl["count"]
                cl["centroid"] = (cl["centroid"] * (n - 1) + ev["vec"]) / n
            else:
                clusters.append({
                    "centroid": ev["vec"].copy(),
                    "pattern": ev["message"],
                    "count": 1,
                    "event_ids": [ev["event_id"]],
                    "sources": {ev["source"]},
                })

        clusters.sort(key=lambda c: c["count"], reverse=True)
        return [
            {
                "pattern": cl["pattern"][:200],
                "count": cl["count"],
                "sources": sorted(cl["sources"]),
                "event_ids": cl["event_ids"],
            }
            for cl in clusters[:limit]
        ]

    def list_events(
        self,
        friction_type: Optional[FrictionType] = None,
        source: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 50,
        include_test: bool = False,
    ) -> list[FrictionEvent]:
        rows = self._repository.list_friction_events(
            friction_type=friction_type.value if friction_type is not None else None,
            source=source,
            since=since,
            limit=limit,
            include_test=include_test,
        )
        return [self._row_to_event(r) for r in rows]

    def stats(self, include_test: bool = False) -> FrictionStats:
        rows = self._repository.list_type_source_rows(include_test=include_test)
        total = len(rows)
        by_type: dict[str, int] = {}
        by_source: dict[str, int] = {}
        bounces = 0
        for r in rows:
            ft, src = r["friction_type"], r["source"]
            by_type[ft] = by_type.get(ft, 0) + 1
            by_source[src] = by_source.get(src, 0) + 1
            if ft == FrictionType.GUARDRAIL_BOUNCE.value:
                bounces += 1
        br = bounces / total if total else 0.0
        return FrictionStats(total=total, by_type=by_type, by_source=by_source, bounce_rate=br)

    def patterns(
        self,
        friction_type: Optional[FrictionType] = None,
        source: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 20,
        scan_limit: int = 500,
        include_test: bool = False,
        promotion_threshold: int = 3,
    ) -> list[FrictionPattern]:
        """Return deterministic repeated-friction groups.

        Structured event messages may supply a fingerprint, command, and
        reason_code. Older plain-text rows still group by a stable fallback
        hash over source, job label, type, and normalized message.
        """

        rows = self.list_events(
            friction_type=friction_type,
            source=source,
            since=since,
            limit=max(limit, scan_limit),
            include_test=include_test,
        )
        groups: dict[str, dict] = {}
        for event in rows:
            payload = _structured_message(event.message)
            fingerprint = _pattern_fingerprint(event, payload)
            command = _payload_text(payload, "command") or event.job_label
            reason_code = (
                _payload_text(payload, "reason_code")
                or _payload_text(payload, "code")
                or event.friction_type.value
            )
            sample = (
                _payload_text(payload, "output")
                or _payload_text(payload, "message")
                or event.message
            )
            group = groups.setdefault(
                fingerprint,
                {
                    "fingerprint": fingerprint,
                    "count": 0,
                    "sources": set(),
                    "job_labels": set(),
                    "reason_code": reason_code,
                    "command": command,
                    "sample": sample,
                    "first_seen": event.timestamp,
                    "last_seen": event.timestamp,
                    "event_ids": [],
                },
            )
            group["count"] += 1
            group["sources"].add(event.source)
            group["job_labels"].add(event.job_label)
            group["event_ids"].append(event.event_id)
            if event.timestamp < group["first_seen"]:
                group["first_seen"] = event.timestamp
            if event.timestamp > group["last_seen"]:
                group["last_seen"] = event.timestamp
                group["reason_code"] = reason_code
                group["command"] = command
                group["sample"] = sample

        patterns = [
            FrictionPattern(
                fingerprint=str(group["fingerprint"]),
                count=int(group["count"]),
                sources=tuple(sorted(group["sources"])),
                job_labels=tuple(sorted(group["job_labels"])),
                reason_code=str(group["reason_code"]),
                command=str(group["command"]),
                sample=str(group["sample"])[:700],
                first_seen=group["first_seen"],
                last_seen=group["last_seen"],
                event_ids=tuple(str(event_id) for event_id in group["event_ids"][:10]),
                promotion_candidate=int(group["count"]) >= promotion_threshold,
            )
            for group in groups.values()
        ]
        patterns.sort(key=lambda pattern: (pattern.count, pattern.last_seen), reverse=True)
        return patterns[:limit]

    def bounce_rate(self, since_hours: int = 24, include_test: bool = False) -> float:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        rows = self._repository.list_type_rows_since(
            since=since,
            include_test=include_test,
        )
        if not rows:
            return 0.0
        bounces = sum(1 for r in rows if r["friction_type"] == FrictionType.GUARDRAIL_BOUNCE.value)
        return bounces / len(rows)

    @staticmethod
    def is_guardrail(event: FrictionEvent) -> bool:
        return event.friction_type in (FrictionType.GUARDRAIL_BOUNCE, FrictionType.WARN_ONLY)

    @staticmethod
    def _row_to_event(row) -> FrictionEvent:
        return FrictionEvent(
            event_id=row["event_id"],
            friction_type=FrictionType(row["friction_type"]),
            source=row["source"],
            job_label=row["job_label"],
            message=row["message"],
            timestamp=row["timestamp"] if isinstance(row["timestamp"], datetime) else datetime.fromisoformat(row["timestamp"]),
            is_test=row.get("is_test", False) if isinstance(row, dict) else getattr(row, "is_test", False),
        )


def _structured_message(message: str) -> dict[str, object] | None:
    text = str(message or "").strip()
    if not text.startswith("{"):
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _payload_text(payload: dict[str, object] | None, key: str) -> str:
    if payload is None:
        return ""
    value = payload.get(key)
    if value is None:
        return ""
    return " ".join(str(value).split())


def _pattern_fingerprint(event: FrictionEvent, payload: dict[str, object] | None) -> str:
    explicit = _payload_text(payload, "fingerprint")
    if explicit:
        return explicit
    basis = json.dumps(
        {
            "friction_type": event.friction_type.value,
            "source": event.source,
            "job_label": event.job_label,
            "message": " ".join(event.message.split())[:700],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.blake2s(basis.encode("utf-8"), digest_size=8).hexdigest()
