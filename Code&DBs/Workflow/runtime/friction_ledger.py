"""Friction Ledger — tracks guardrail bounces, warnings, and hard failures."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection
    from runtime.embedding_service import EmbeddingService

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


_TABLE = "friction_events"


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

        self._conn.execute(
            f"INSERT INTO {_TABLE} (event_id, friction_type, source, job_label, message, timestamp, is_test) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            event_id, friction_type.value, source, job_label, message, ts, is_test,
        )
        if vector_query is not None:
            vector_query.set_embedding(_TABLE, "event_id", event_id)
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

        rows = self._conn.execute(
            f"SELECT event_id, friction_type, source, message, embedding "
            f"FROM {_TABLE} "
            f"WHERE embedding IS NOT NULL AND timestamp >= NOW() - interval '{since_hours} hours'"
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
        clauses: list[str] = []
        params: list = []
        idx = 1
        if not include_test:
            clauses.append(f"is_test = ${idx}")
            params.append(False)
            idx += 1
        if friction_type is not None:
            clauses.append(f"friction_type = ${idx}")
            params.append(friction_type.value)
            idx += 1
        if source is not None:
            clauses.append(f"source = ${idx}")
            params.append(source)
            idx += 1
        if since is not None:
            clauses.append(f"timestamp >= ${idx}")
            params.append(since)
            idx += 1

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT event_id, friction_type, source, job_label, message, timestamp, is_test FROM {_TABLE}{where} ORDER BY timestamp DESC LIMIT ${idx}"
        params.append(limit)

        rows = self._conn.execute(sql, *params)
        return [self._row_to_event(r) for r in rows]

    def stats(self, include_test: bool = False) -> FrictionStats:
        if include_test:
            rows = self._conn.execute(
                f"SELECT friction_type, source FROM {_TABLE}"
            )
        else:
            rows = self._conn.execute(
                f"SELECT friction_type, source FROM {_TABLE} WHERE is_test = false"
            )
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

    def bounce_rate(self, since_hours: int = 24, include_test: bool = False) -> float:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        if include_test:
            rows = self._conn.execute(
                f"SELECT friction_type FROM {_TABLE} WHERE timestamp >= $1",
                since,
            )
        else:
            rows = self._conn.execute(
                f"SELECT friction_type FROM {_TABLE} WHERE timestamp >= $1 AND is_test = false",
                since,
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
