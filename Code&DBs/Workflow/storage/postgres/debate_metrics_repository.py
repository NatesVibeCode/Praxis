"""Explicit sync Postgres repository for debate metrics persistence."""

from __future__ import annotations

import json
from typing import Any

from .validators import _require_text


class PostgresDebateMetricsRepository:
    """Owns canonical debate metrics writes."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def upsert_round_metric(
        self,
        *,
        debate_run_id: str,
        debate_id: str,
        round_number: int,
        persona_position: int,
        round_id: str,
        persona: str,
        word_count: int,
        claim_count: int,
        evidence_citations: int,
        quality_score: float,
        duration_seconds: float,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO debate_round_metrics (
                debate_run_id, debate_id, round_number, persona_position,
                round_id, persona, word_count, claim_count,
                evidence_citations, quality_score, duration_seconds, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            ON CONFLICT ON CONSTRAINT debate_round_metrics_run_round_position_uniq
            DO UPDATE SET
                debate_id = EXCLUDED.debate_id,
                round_id = EXCLUDED.round_id,
                persona = EXCLUDED.persona,
                word_count = EXCLUDED.word_count,
                claim_count = EXCLUDED.claim_count,
                evidence_citations = EXCLUDED.evidence_citations,
                quality_score = EXCLUDED.quality_score,
                duration_seconds = EXCLUDED.duration_seconds,
                created_at = NOW()
            """,
            _require_text(debate_run_id, field_name="debate_run_id"),
            _require_text(debate_id, field_name="debate_id"),
            round_number,
            persona_position,
            _require_text(round_id, field_name="round_id"),
            _require_text(persona, field_name="persona"),
            word_count,
            claim_count,
            evidence_citations,
            quality_score,
            duration_seconds,
        )

    def upsert_consensus_metric(
        self,
        *,
        debate_run_id: str,
        debate_id: str,
        total_rounds: int,
        consensus_points: tuple[str, ...],
        disagreements: tuple[str, ...],
        avg_quality: float,
        synthesis_quality: float | None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO debate_consensus (
                debate_run_id, debate_id, total_rounds,
                consensus_points, disagreements, avg_quality,
                synthesis_quality, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, NOW(), NOW())
            ON CONFLICT (debate_run_id) DO UPDATE SET
                debate_id = EXCLUDED.debate_id,
                total_rounds = EXCLUDED.total_rounds,
                consensus_points = EXCLUDED.consensus_points,
                disagreements = EXCLUDED.disagreements,
                avg_quality = EXCLUDED.avg_quality,
                synthesis_quality = EXCLUDED.synthesis_quality,
                updated_at = NOW()
            """,
            _require_text(debate_run_id, field_name="debate_run_id"),
            _require_text(debate_id, field_name="debate_id"),
            total_rounds,
            json.dumps(list(consensus_points)),
            json.dumps(list(disagreements)),
            avg_quality,
            synthesis_quality,
        )


__all__ = ["PostgresDebateMetricsRepository"]
