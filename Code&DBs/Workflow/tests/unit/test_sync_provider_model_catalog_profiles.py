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


def test_model_profile_authority_covers_current_active_catalog() -> None:
    authority = _mod._load_model_profile_authority()
    index = authority["index"]

    expected = {
        ("anthropic", "claude-haiku-4-5-20251001"),
        ("anthropic", "claude-opus-4-6"),
        ("anthropic", "claude-sonnet-4-6"),
        ("google", "gemini-1.5-pro-002"),
        ("google", "gemini-2.0-flash"),
        ("google", "gemini-2.0-flash-001"),
        ("google", "gemini-2.0-flash-lite-001"),
        ("google", "gemini-2.5-flash"),
        ("google", "gemini-2.5-flash-lite"),
        ("google", "gemini-2.5-flash-preview-04-17"),
        ("google", "gemini-2.5-flash-tts"),
        ("google", "gemini-2.5-pro"),
        ("google", "gemini-2.5-pro-exp-03-25"),
        ("google", "gemini-2.5-pro-tts"),
        ("google", "gemini-3-flash-preview"),
        ("google", "gemini-3.1-flash-image-preview"),
        ("google", "gemini-3.1-flash-lite-preview"),
        ("google", "gemini-3.1-pro-preview"),
        ("google", "gemini-live-2.5-flash-native-audio"),
        ("openai", "gpt-5"),
        ("openai", "gpt-5-codex"),
        ("openai", "gpt-5-codex-mini"),
        ("openai", "gpt-5.1"),
        ("openai", "gpt-5.1-codex"),
        ("openai", "gpt-5.1-codex-max"),
        ("openai", "gpt-5.1-codex-mini"),
        ("openai", "gpt-5.2"),
        ("openai", "gpt-5.2-codex"),
        ("openai", "gpt-5.3-codex"),
        ("openai", "gpt-5.3-codex-spark"),
        ("openai", "gpt-5.4"),
        ("openai", "gpt-5.4-mini"),
    }

    assert set(index) == expected


def test_model_profile_lookup_is_strict_for_unknown_models() -> None:
    with pytest.raises(RuntimeError, match="missing model classification authority"):
        _mod._model_profile("openai", "gpt-9-imaginary")


def test_capability_tags_include_legacy_bucket_and_affinities() -> None:
    profile = _mod._model_profile("openai", "gpt-5.4-mini")

    tags = _mod._capability_tags_for(profile, source_tag="unit-test")

    assert "mid" in tags
    assert "instant" in tags
    assert "subagents" in tags
    assert "computer-use" in tags
    assert "unit-test" in tags
