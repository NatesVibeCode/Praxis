"""Retrieval quality telemetry: metrics collection, storage, and health checks."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


@dataclass(frozen=True)
class RetrievalMetric:
    query_fingerprint: str  # first 8 chars of SHA256 of query
    pattern_name: str
    result_count: int
    score_min: float
    score_max: float
    score_mean: float
    score_stddev: float
    tie_break_count: int  # how many results had identical scores
    latency_ms: float
    timestamp: datetime


@dataclass(frozen=True)
class TelemetrySummary:
    total_queries: int
    avg_result_count: float
    avg_latency_ms: float
    avg_score_mean: float
    avg_tie_break_ratio: float
    patterns_seen: tuple[str, ...]


_DDL = """\
CREATE TABLE IF NOT EXISTS retrieval_metrics (
    id SERIAL PRIMARY KEY,
    query_fingerprint TEXT NOT NULL,
    pattern_name TEXT NOT NULL,
    result_count INTEGER NOT NULL,
    score_min DOUBLE PRECISION NOT NULL,
    score_max DOUBLE PRECISION NOT NULL,
    score_mean DOUBLE PRECISION NOT NULL,
    score_stddev DOUBLE PRECISION NOT NULL,
    tie_break_count INTEGER NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
)
"""


class TelemetryStore:
    """Postgres-backed telemetry store for retrieval metrics."""

    def __init__(self, conn: "SyncPostgresConnection", max_entries: int = 10_000) -> None:
        self._conn = conn
        self._max_entries = max_entries
        self._conn.execute(_DDL)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(self, metric: RetrievalMetric) -> None:
        """Append a metric to the store, auto-pruning if needed."""
        self._conn.execute(
            """INSERT INTO retrieval_metrics
               (query_fingerprint, pattern_name, result_count,
                score_min, score_max, score_mean, score_stddev,
                tie_break_count, latency_ms, timestamp)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
            metric.query_fingerprint,
            metric.pattern_name,
            metric.result_count,
            metric.score_min,
            metric.score_max,
            metric.score_mean,
            metric.score_stddev,
            metric.tie_break_count,
            metric.latency_ms,
            metric.timestamp,
        )
        self._auto_prune()

    def query_metrics(
        self,
        pattern_name: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[RetrievalMetric]:
        """Query stored metrics with optional filters."""
        clauses: list[str] = []
        params: list = []
        param_idx = 1
        if pattern_name is not None:
            clauses.append(f"pattern_name = ${param_idx}")
            params.append(pattern_name)
            param_idx += 1
        if since is not None:
            clauses.append(f"timestamp >= ${param_idx}")
            params.append(since)
            param_idx += 1

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM retrieval_metrics {where} ORDER BY id DESC LIMIT ${param_idx}",
            *params,
        )

        return [self._row_to_metric(r) for r in rows]

    def summary(self, pattern_name: str | None = None) -> TelemetrySummary:
        """Aggregate stats across stored metrics."""
        clauses: list[str] = []
        params: list = []
        param_idx = 1
        if pattern_name is not None:
            clauses.append(f"pattern_name = ${param_idx}")
            params.append(pattern_name)
            param_idx += 1

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        rows = self._conn.execute(
            f"""SELECT
                    COUNT(*) as total,
                    AVG(result_count) as avg_rc,
                    AVG(latency_ms) as avg_lat,
                    AVG(score_mean) as avg_sm,
                    AVG(CASE WHEN result_count > 0
                         THEN CAST(tie_break_count AS DOUBLE PRECISION) / result_count
                         ELSE 0 END) as avg_tbr
                FROM retrieval_metrics {where}""",
            *params,
        )
        row = next(iter(rows), None)

        patterns_rows = self._conn.execute(
            f"SELECT DISTINCT pattern_name FROM retrieval_metrics {where}",
            *params,
        )

        total = row["total"] if row and row["total"] else 0
        return TelemetrySummary(
            total_queries=total,
            avg_result_count=float(row["avg_rc"] or 0) if row else 0.0,
            avg_latency_ms=float(row["avg_lat"] or 0) if row else 0.0,
            avg_score_mean=float(row["avg_sm"] or 0) if row else 0.0,
            avg_tie_break_ratio=float(row["avg_tbr"] or 0) if row else 0.0,
            patterns_seen=tuple(r["pattern_name"] for r in patterns_rows),
        )

    def prune(self) -> None:
        """Remove oldest entries beyond max_entries."""
        rows = self._conn.execute(
            "SELECT COUNT(*) as c FROM retrieval_metrics"
        )
        row = next(iter(rows), None)
        count = row["c"] if row else 0
        if count > self._max_entries:
            excess = count - self._max_entries
            self._conn.execute(
                """DELETE FROM retrieval_metrics WHERE id IN
                   (SELECT id FROM retrieval_metrics ORDER BY id ASC LIMIT $1)""",
                excess,
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _auto_prune(self) -> None:
        """Prune if over limit -- called after each record()."""
        self.prune()

    @staticmethod
    def _row_to_metric(row) -> RetrievalMetric:
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        return RetrievalMetric(
            query_fingerprint=row["query_fingerprint"],
            pattern_name=row["pattern_name"],
            result_count=row["result_count"],
            score_min=row["score_min"],
            score_max=row["score_max"],
            score_mean=row["score_mean"],
            score_stddev=row["score_stddev"],
            tie_break_count=row["tie_break_count"],
            latency_ms=row["latency_ms"],
            timestamp=ts,
        )


class RetrievalInstrumenter:
    """Compute and record retrieval metrics from raw query results."""

    def __init__(self, store: TelemetryStore) -> None:
        self._store = store

    def instrument(
        self,
        query: str,
        pattern_name: str,
        results: list,
        latency_ms: float,
    ) -> RetrievalMetric:
        """Compute metrics from results, record, and return the metric.

        Each item in *results* should either be a numeric score or an object
        with a ``score`` attribute.
        """
        fingerprint = hashlib.sha256(query.encode()).hexdigest()[:8]

        scores = self._extract_scores(results)
        n = len(scores)

        if n == 0:
            metric = RetrievalMetric(
                query_fingerprint=fingerprint,
                pattern_name=pattern_name,
                result_count=0,
                score_min=0.0,
                score_max=0.0,
                score_mean=0.0,
                score_stddev=0.0,
                tie_break_count=0,
                latency_ms=latency_ms,
                timestamp=datetime.now(timezone.utc),
            )
        else:
            mean = sum(scores) / n
            variance = sum((s - mean) ** 2 for s in scores) / n
            stddev = math.sqrt(variance)

            from collections import Counter

            counts = Counter(scores)
            tie_break = sum(c for c in counts.values() if c > 1)

            metric = RetrievalMetric(
                query_fingerprint=fingerprint,
                pattern_name=pattern_name,
                result_count=n,
                score_min=min(scores),
                score_max=max(scores),
                score_mean=mean,
                score_stddev=stddev,
                tie_break_count=tie_break,
                latency_ms=latency_ms,
                timestamp=datetime.now(timezone.utc),
            )

        self._store.record(metric)
        return metric

    def health_check(self) -> dict:
        """Return summary stats and whether avg latency is acceptable (< 500ms)."""
        s = self._store.summary()
        return {
            "total_queries": s.total_queries,
            "avg_result_count": s.avg_result_count,
            "avg_latency_ms": s.avg_latency_ms,
            "avg_score_mean": s.avg_score_mean,
            "avg_tie_break_ratio": s.avg_tie_break_ratio,
            "patterns_seen": list(s.patterns_seen),
            "latency_ok": s.avg_latency_ms < 500.0,
        }

    @staticmethod
    def _extract_scores(results: list) -> list[float]:
        scores: list[float] = []
        for r in results:
            if isinstance(r, (int, float)):
                scores.append(float(r))
            elif hasattr(r, "score"):
                scores.append(float(r.score))
            elif isinstance(r, dict) and "score" in r:
                scores.append(float(r["score"]))
        return scores
