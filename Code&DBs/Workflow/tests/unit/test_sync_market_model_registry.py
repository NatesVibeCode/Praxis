from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

_spec = importlib.util.spec_from_file_location(
    "scripts.sync_market_model_registry",
    Path(__file__).resolve().parents[2] / "scripts" / "sync_market_model_registry.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["scripts.sync_market_model_registry"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


def _source_config() -> dict[str, object]:
    return {
        "source_slug": "artificial_analysis",
        "modality": "llm",
        "creator_slug_aliases": {
            "anthropic": "anthropic",
            "google": "google",
            "google-deepmind": "google",
            "openai": "openai",
        },
        "common_metric_paths": {
            "artificial_analysis_intelligence_index": "evaluations.artificial_analysis_intelligence_index",
            "artificial_analysis_coding_index": "evaluations.artificial_analysis_coding_index",
            "artificial_analysis_math_index": "evaluations.artificial_analysis_math_index",
            "price_1m_blended_3_to_1": "pricing.price_1m_blended_3_to_1",
            "price_1m_input_tokens": "pricing.price_1m_input_tokens",
            "price_1m_output_tokens": "pricing.price_1m_output_tokens",
            "median_output_tokens_per_second": "median_output_tokens_per_second",
            "median_time_to_first_token_seconds": "median_time_to_first_token_seconds",
            "median_time_to_first_answer_token": "median_time_to_first_answer_token",
        },
    }


def _match_rule(**overrides: object) -> dict[str, object]:
    rule = {
        "provider_model_market_match_rule_id": "rule.test.openai.gpt-5.4",
        "source_slug": "artificial_analysis",
        "provider_slug": "openai",
        "candidate_model_slug": "gpt-5.4",
        "target_creator_slug": "openai",
        "target_source_model_slug": "gpt-5-4",
        "match_kind": "normalized_slug_alias",
        "binding_confidence": 0.99,
        "selection_metadata": {"reason": "punctuation only"},
        "decision_ref": "decision.market-model-match-rules.test",
        "enabled": True,
    }
    rule.update(overrides)
    return rule


def test_normalize_market_row_extracts_common_fields() -> None:
    row = _mod._normalize_market_row(
        {
            "id": "model-123",
            "name": "GPT-5.4",
            "slug": "gpt-5.4",
            "model_creator": {
                "id": "creator-1",
                "name": "OpenAI",
                "slug": "openai",
            },
            "evaluations": {
                "artificial_analysis_intelligence_index": 70.1,
                "artificial_analysis_coding_index": 65.4,
            },
            "pricing": {
                "price_1m_blended_3_to_1": 12.3,
            },
            "median_output_tokens_per_second": 98.2,
            "median_time_to_first_token_seconds": 1.9,
            "median_time_to_first_answer_token": 1.9,
        },
        source_config=_source_config(),
        prompt_options={"prompt_length": "medium"},
        decision_ref="decision.market-model-sync.test",
        synced_at=_mod._utc_now(),
    )

    assert row["market_model_ref"] == "market_model.artificial_analysis.llm.model-123"
    assert row["creator_slug"] == "openai"
    assert row["speed_metrics"]["median_output_tokens_per_second"] == 98.2


def test_normalize_provider_slug_maps_google_deepmind_to_google() -> None:
    assert _mod._normalize_provider_slug(
        "google-deepmind",
        aliases=dict(_source_config()["creator_slug_aliases"]),
    ) == "google"


def test_common_metrics_selects_general_market_fields() -> None:
    metrics = _mod._common_metrics(
        {
            "evaluations": {
                "artificial_analysis_intelligence_index": 71.0,
                "artificial_analysis_coding_index": 60.5,
                "artificial_analysis_math_index": 81.2,
            },
            "pricing": {
                "price_1m_blended_3_to_1": 5.4,
                "price_1m_input_tokens": 2.0,
                "price_1m_output_tokens": 12.0,
            },
            "median_output_tokens_per_second": 140.0,
            "median_time_to_first_token_seconds": 0.8,
            "median_time_to_first_answer_token": 0.8,
        },
        metric_paths=dict(_source_config()["common_metric_paths"]),
    )

    assert metrics["artificial_analysis_intelligence_index"] == 71.0
    assert metrics["price_1m_blended_3_to_1"] == 5.4
    assert metrics["median_time_to_first_token_seconds"] == 0.8


def test_market_benchmark_payload_carries_binding_metadata() -> None:
    synced_at = _mod._utc_now()
    market_row = _mod._normalize_market_row(
        {
            "id": "model-123",
            "name": "GPT-5.4",
            "slug": "gpt-5-4",
            "model_creator": {
                "id": "creator-1",
                "name": "OpenAI",
                "slug": "openai",
            },
            "evaluations": {
                "artificial_analysis_intelligence_index": 70.1,
            },
            "pricing": {
                "price_1m_blended_3_to_1": 12.3,
            },
            "median_output_tokens_per_second": 98.2,
            "median_time_to_first_token_seconds": 1.9,
            "median_time_to_first_answer_token": 1.9,
        },
        source_config=_source_config(),
        prompt_options={"prompt_length": "medium"},
        decision_ref="decision.market-model-sync.test",
        synced_at=synced_at,
    )

    payload = _mod._market_benchmark_payload(
        market_row=market_row,
        source_config=_source_config(),
        rule=_match_rule(),
    )

    assert payload["coverage_status"] == "bound"
    assert payload["source_model_slug"] == "gpt-5-4"
    assert payload["binding_kind"] == "normalized_slug_alias"
    assert payload["binding_rule_ref"] == "rule.test.openai.gpt-5.4"
    assert payload["binding_decision_ref"] == "decision.market-model-match-rules.test"
    assert payload["common_metrics"]["artificial_analysis_intelligence_index"] == 70.1


def test_market_benchmark_gap_payload_marks_unavailable_rows() -> None:
    payload = _mod._market_benchmark_gap_payload(
        source_slug="artificial_analysis",
        rule=_match_rule(
            provider_model_market_match_rule_id="rule.test.google.gemini-live",
            provider_slug="google",
            candidate_model_slug="gemini-live-2.5-flash-native-audio",
            target_source_model_slug=None,
            match_kind="source_unavailable",
            binding_confidence=0.0,
            selection_metadata={"surface_gap": "live_native_audio"},
        ),
        synced_at=_mod._utc_now(),
    )

    assert payload["coverage_status"] == "source_unavailable"
    assert payload["binding_kind"] == "source_unavailable"
    assert payload["selection_metadata"]["surface_gap"] == "live_native_audio"


def test_validate_candidate_rule_coverage_rejects_missing_rows() -> None:
    candidate_lookup = {
        ("openai", "gpt-5.4"): ("candidate.openai.gpt-5.4", {}),
        ("openai", "gpt-5.4-mini"): ("candidate.openai.gpt-5.4-mini", {}),
    }
    match_rules = {
        ("openai", "gpt-5.4"): _match_rule(),
    }

    try:
        _mod._validate_candidate_rule_coverage(
            candidate_lookup=candidate_lookup,
            match_rules=match_rules,
            source_slug="artificial_analysis",
        )
    except RuntimeError as exc:
        assert "openai/gpt-5.4-mini" in str(exc)
    else:
        raise AssertionError("expected missing candidate coverage to raise")
