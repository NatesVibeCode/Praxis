from __future__ import annotations

import json

import pytest

from runtime.risk_scoring import RiskScorer
from runtime.trust_scoring import TrustScorer


def test_trust_scorer_rejects_file_persistence_path(tmp_path) -> None:
    with pytest.raises(ValueError, match="no longer accepts a file persistence path"):
        TrustScorer(persistence_path=str(tmp_path / "trust_scores.json"))


def test_trust_scorer_update_is_ephemeral_not_file_backed(tmp_path) -> None:
    artifact_path = tmp_path / "trust_scores.json"
    scorer = TrustScorer()

    score = scorer.update("openai", "gpt-5.4", True)

    assert score.provider_slug == "openai"
    assert scorer.score("openai", "gpt-5.4") is score
    assert not artifact_path.exists()


def test_risk_scorer_requires_explicit_export_path() -> None:
    scorer = RiskScorer()

    with pytest.raises(ValueError, match="explicit path"):
        scorer.persist()

    with pytest.raises(ValueError, match="explicit path"):
        RiskScorer.load()


def test_risk_scorer_explicit_export_is_diagnostic_only(tmp_path) -> None:
    output_path = tmp_path / "risk_scores.json"
    scorer = RiskScorer()

    written_path = scorer.persist(str(output_path))

    assert written_path == str(output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["kind"] == "risk_scores"
    assert payload["scores"] == []
    assert RiskScorer.load(str(output_path)) == payload
