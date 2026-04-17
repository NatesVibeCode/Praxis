from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

_spec = importlib.util.spec_from_file_location(
    "scripts.sync_provider_model_catalog",
    Path(__file__).resolve().parents[2] / "scripts" / "sync_provider_model_catalog.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["scripts.sync_provider_model_catalog"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


def _profile_authority_fixture() -> dict[str, object]:
    rows = [
        {
            "provider_slug": "openai",
            "model_slug": "gpt-5.4-mini",
            "route_tier": "medium",
            "route_tier_rank": 1,
            "latency_class": "instant",
            "latency_rank": 2,
            "reasoning_control": {
                "default": "medium",
                "kind": "openai_reasoning_effort",
                "supported_values": ["low", "medium", "high", "xhigh"],
            },
            "task_affinities": {
                "primary": ["build", "wiring", "subagents", "computer-use"],
                "secondary": ["review", "chat", "analysis"],
                "specialized": [],
                "avoid": [],
            },
            "benchmark_profile": {
                "positioning": "OpenAI's strongest mini model for coding, computer use, and subagents.",
                "source_refs": ["openai_all", "openai_gpt54_mini_release"],
                "evidence_level": "vendor_plus_secondary",
                "benchmark_notes": [
                    "Official release positions GPT-5.4-mini as the strongest mini in the family."
                ],
            },
        },
        {
            "provider_slug": "anthropic",
            "model_slug": "claude-sonnet-4-6",
            "route_tier": "medium",
            "route_tier_rank": 1,
            "latency_class": "instant",
            "latency_rank": 3,
            "reasoning_control": {
                "default": "adaptive",
                "kind": "anthropic_thinking",
                "supported_values": ["adaptive", "extended"],
            },
            "task_affinities": {
                "primary": ["review", "build", "chat", "analysis"],
                "secondary": ["research", "architecture"],
                "specialized": [],
                "avoid": [],
            },
            "benchmark_profile": {
                "positioning": "Balanced Claude route with the best blend of speed and intelligence.",
                "source_refs": ["anthropic_models", "anthropic_choosing"],
                "evidence_level": "vendor_plus_secondary",
                "benchmark_notes": [
                    "Vendor docs describe Sonnet as the best speed-intelligence balance."
                ],
            },
        },
    ]
    return _mod._normalize_profile_authority_rows(rows)


def test_normalize_profile_authority_rows_builds_index() -> None:
    authority = _profile_authority_fixture()

    assert set(authority["index"]) == {
        ("openai", "gpt-5.4-mini"),
        ("anthropic", "claude-sonnet-4-6"),
    }
    openai_profile = authority["index"][("openai", "gpt-5.4-mini")]
    assert openai_profile["profile_id"] == "profile.openai.gpt-5.4-mini"
    assert openai_profile["models"] == ["gpt-5.4-mini"]


def test_model_profile_lookup_is_strict_for_unknown_models() -> None:
    authority = _profile_authority_fixture()

    with pytest.raises(RuntimeError, match="missing model classification authority"):
        _mod._model_profile(authority, "openai", "gpt-9-imaginary")


def test_capability_tags_include_legacy_bucket_and_affinities() -> None:
    authority = _profile_authority_fixture()
    profile = _mod._model_profile(authority, "openai", "gpt-5.4-mini")

    tags = _mod._capability_tags(profile, source_tag="unit-test")

    assert "mid" in tags
    assert "instant" in tags
    assert "subagents" in tags
    assert "computer-use" in tags
    assert "unit-test" in tags


def test_load_anthropic_inventory_uses_db_sync_config_authority() -> None:
    models = _mod.load_anthropic_inventory(
        {
            "anthropic": {
                "doc_model_ids": ("claude-opus-4-6", "claude-sonnet-4-6"),
                "migration_rules": {"claude-sonnet-4-5": "claude-sonnet-4-6"},
            }
        }
    )

    assert models == ("claude-opus-4-6", "claude-sonnet-4-6")
