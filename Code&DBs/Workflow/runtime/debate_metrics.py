"""Debate metrics: round-level quality scoring and consensus tracking."""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

_log = logging.getLogger(__name__)

@dataclass(frozen=True)
class DebateRoundMetrics:
    round_id: str
    debate_id: str
    persona: str
    word_count: int
    claim_count: int
    evidence_citations: int
    quality_score: float
    duration_seconds: float


@dataclass(frozen=True)
class DebateConsensus:
    debate_id: str
    total_rounds: int
    consensus_points: tuple[str, ...]
    disagreements: tuple[str, ...]
    avg_quality: float
    synthesis_quality: Optional[float]


# Assertive sentence starters that signal a claim
_ASSERTIVE_STARTS = re.compile(
    r"^\s*(?:I\s+(?:believe|argue|contend|assert|claim|propose|maintain|suggest|think|recommend)"
    r"|(?:This|That|It|The\s+\w+)\s+(?:is|are|was|were|shows?|demonstrates?|proves?|indicates?|requires?|means?|should|must|will|can)"
    r"|(?:We|They)\s+(?:should|must|need|can|will|have)"
    r"|(?:Clearly|Obviously|Evidently|Indeed|Therefore|Thus|Hence|Consequently|Furthermore|Moreover))",
    re.IGNORECASE,
)

# Patterns that count as evidence citations
_CITATION_PATTERNS = [
    re.compile(r'"[^"]{4,}"'),           # quoted text (4+ chars)
    re.compile(r"'[^']{4,}'"),           # single-quoted text
    re.compile(r"\[[^\]]*\d+[^\]]*\]"),  # bracketed references like [1] or [Smith 2024]
    re.compile(r"(?:see|cf\.?|per|from|according to)\s", re.IGNORECASE),  # citation phrases
    re.compile(r"\b\w+\.\w{1,4}(?::\d+)?"),  # file paths like foo.py or foo.py:42
    re.compile(r"https?://\S+"),         # URLs
]

# Specificity signals: numbers, file paths, code-like references
_SPECIFICITY_PATTERNS = [
    re.compile(r"\b\d+(?:\.\d+)?%?\b"),                   # numbers / percentages
    re.compile(r"\b\w+(?:/\w+)+(?:\.\w+)?\b"),             # paths like src/foo/bar.py
    re.compile(r"`[^`]+`"),                                 # inline code
    re.compile(r"\b(?:function|class|def|import|return)\b"),# code keywords
]


class DebateMetricsCollector:
    """Collects per-round debate metrics and consensus summaries."""

    def __init__(
        self,
        conn: "SyncPostgresConnection | None" = None,
    ) -> None:
        self._rounds: dict[str, list[DebateRoundMetrics]] = {}
        self._consensus: dict[str, DebateConsensus] = {}
        self._round_positions: dict[str, list[tuple[int, int]]] = {}
        self._conn = conn
        self._debate_run_id = uuid.uuid4().hex[:16]
        self._persistence_disabled = False
        self._persisted_round_keys: set[tuple[str, int, int]] = set()
        self._persisted_consensus_runs: set[str] = set()

    def _require_positive_int(self, value: int | None, *, field_name: str) -> int:
        if value is None:
            return 1 if field_name == "round_number" else 0
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{field_name} must be a non-negative integer")
        if field_name == "round_number" and value == 0:
            raise ValueError("round_number must be a positive integer")
        return value

    def _get_conn(self) -> "SyncPostgresConnection | None":
        if self._persistence_disabled:
            return None
        if self._conn is not None:
            return self._conn
        try:
            from storage.postgres import ensure_postgres_available

            self._conn = ensure_postgres_available()
            return self._conn
        except Exception as exc:
            self._persistence_disabled = True
            _log.warning("debate metrics persistence disabled: %s", exc)
            return None

    def _repository(self, conn: "SyncPostgresConnection"):
        from storage.postgres import PostgresDebateMetricsRepository

        return PostgresDebateMetricsRepository(conn)

    def _persist_round(
        self,
        *,
        metric: DebateRoundMetrics,
        round_number: int,
        persona_position: int,
        conn: "SyncPostgresConnection | None" = None,
    ) -> None:
        use_shared_conn = conn is None
        if use_shared_conn:
            conn = self._get_conn()
        if conn is None:
            return
        try:
            self._repository(conn).upsert_round_metric(
                debate_run_id=self._debate_run_id,
                debate_id=metric.debate_id,
                round_number=round_number,
                persona_position=persona_position,
                round_id=metric.round_id,
                persona=metric.persona,
                word_count=metric.word_count,
                claim_count=metric.claim_count,
                evidence_citations=metric.evidence_citations,
                quality_score=metric.quality_score,
                duration_seconds=metric.duration_seconds,
            )
            self._persisted_round_keys.add((self._debate_run_id, round_number, persona_position))
        except Exception as exc:
            if use_shared_conn:
                self._persistence_disabled = True
            _log.warning("failed to persist debate round metrics: %s", exc)

    def _persist_consensus_with_conn(
        self,
        metric: DebateConsensus,
        conn: "SyncPostgresConnection | None" = None,
    ) -> None:
        use_shared_conn = conn is None
        if use_shared_conn:
            conn = self._get_conn()
        if conn is None:
            return
        try:
            self._repository(conn).upsert_consensus_metric(
                debate_run_id=self._debate_run_id,
                debate_id=metric.debate_id,
                total_rounds=metric.total_rounds,
                consensus_points=metric.consensus_points,
                disagreements=metric.disagreements,
                avg_quality=metric.avg_quality,
                synthesis_quality=metric.synthesis_quality,
            )
            self._persisted_consensus_runs.add(self._debate_run_id)
        except Exception as exc:
            if use_shared_conn:
                self._persistence_disabled = True
            _log.warning("failed to persist debate consensus metrics: %s", exc)

    def _persist_consensus(self, metric: DebateConsensus) -> None:
        """Persist consensus using the collector's default connection path."""
        self._persist_consensus_with_conn(metric=metric, conn=None)

    def flush(
        self,
        conn: "SyncPostgresConnection | None" = None,
    ) -> None:
        """Flush in-memory metrics to Postgres.

        The in-memory round and consensus caches remain the source of truth for the
        current process. `flush()` provides durable evidence at the end of execution
        with conflict-safe upserts so repeated flush attempts stay idempotent.
        """
        target_conn = conn
        if target_conn is None:
            target_conn = self._get_conn()
        if target_conn is None:
            return

        try:
            for debate_id, rounds in self._rounds.items():
                positions = self._round_positions.get(debate_id, [])
                for metric, position in zip(rounds, positions):
                    round_number, persona_position = position
                    key = (self._debate_run_id, round_number, persona_position)
                    if key in self._persisted_round_keys:
                        continue
                    self._persist_round(
                        metric=metric,
                        round_number=round_number,
                        persona_position=persona_position,
                        conn=target_conn,
                    )

            if self._debate_run_id not in self._persisted_consensus_runs:
                for metric in self._consensus.values():
                    self._persist_consensus_with_conn(metric=metric, conn=target_conn)
        except Exception as exc:
            _log.warning("failed to flush debate metrics: %s", exc)

    def quality_score(self, text: str) -> float:
        """Heuristic quality score for debate text, 0.0 - 1.0."""
        words = text.split()
        wc = len(words)

        if wc == 0:
            return 0.0

        score = 0.0

        # Length component: reward substance, penalise very short
        if wc < 20:
            score += 0.1
        elif wc < 50:
            score += 0.25
        elif wc < 200:
            score += 0.35
        else:
            score += 0.4

        # Evidence component
        citation_hits = sum(
            len(pat.findall(text)) for pat in _CITATION_PATTERNS
        )
        score += min(citation_hits * 0.05, 0.3)

        # Specificity component
        specificity_hits = sum(
            len(pat.findall(text)) for pat in _SPECIFICITY_PATTERNS
        )
        score += min(specificity_hits * 0.04, 0.2)

        # Claim density (moderate is good)
        sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
        claim_count = sum(1 for s in sentences if _ASSERTIVE_STARTS.match(s))
        if len(sentences) > 0:
            claim_ratio = claim_count / len(sentences)
            if 0.2 <= claim_ratio <= 0.7:
                score += 0.1

        return round(min(score, 1.0), 4)

    def record_round(
        self,
        debate_id: str,
        persona: str,
        text: str,
        duration_seconds: float,
        *,
        round_number: int | None = None,
        persona_position: int | None = None,
    ) -> DebateRoundMetrics:
        words = text.split()
        wc = len(words)
        sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
        claim_count = sum(1 for s in sentences if _ASSERTIVE_STARTS.match(s))
        evidence_citations = sum(
            len(pat.findall(text)) for pat in _CITATION_PATTERNS
        )

        m = DebateRoundMetrics(
            round_id=uuid.uuid4().hex[:12],
            debate_id=debate_id,
            persona=persona,
            word_count=wc,
            claim_count=claim_count,
            evidence_citations=evidence_citations,
            quality_score=self.quality_score(text),
            duration_seconds=duration_seconds,
        )
        self._rounds.setdefault(debate_id, []).append(m)
        round_number = self._require_positive_int(round_number, field_name="round_number")
        persisted_persona_position = (
            self._require_positive_int(persona_position, field_name="persona_position")
            if persona_position is not None
            else len(self._rounds.get(debate_id, [])) - 1
        )
        self._round_positions.setdefault(debate_id, []).append((round_number, persisted_persona_position))
        self._persist_round(
            metric=m,
            round_number=round_number,
            persona_position=persisted_persona_position,
        )
        return m

    def record_synthesis(
        self,
        debate_id: str,
        consensus_points: list[str],
        disagreements: list[str],
        synthesis_text: str,
    ) -> DebateConsensus:
        rounds = self._rounds.get(debate_id, [])
        avg_q = (
            sum(r.quality_score for r in rounds) / len(rounds)
            if rounds
            else 0.0
        )
        syn_q = self.quality_score(synthesis_text) if synthesis_text else None

        c = DebateConsensus(
            debate_id=debate_id,
            total_rounds=len(rounds),
            consensus_points=tuple(consensus_points),
            disagreements=tuple(disagreements),
            avg_quality=round(avg_q, 4),
            synthesis_quality=round(syn_q, 4) if syn_q is not None else None,
        )
        self._consensus[debate_id] = c
        self._persist_consensus(c)
        return c

    def get_debate(
        self, debate_id: str
    ) -> tuple[list[DebateRoundMetrics], Optional[DebateConsensus]]:
        return (
            list(self._rounds.get(debate_id, [])),
            self._consensus.get(debate_id),
        )
