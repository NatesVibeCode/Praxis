from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime.intent_decomposition import (
    DecomposedIntent,
    DecompositionRequiresLLMError,
    StepIntent,
    decompose_intent,
)


def test_numbered_list_decomposes_in_order() -> None:
    intent = (
        "1. Add a timezone column to users.\n"
        "2. Backfill existing rows with UTC.\n"
        "3. Update the profile UI to expose the field."
    )
    result = decompose_intent(intent)
    assert result.detection_mode == "numbered_list"
    assert len(result.steps) == 3
    assert result.steps[0].text == "Add a timezone column to users."
    assert result.steps[0].raw_marker == "1"
    assert result.steps[0].stage_hint == "build"  # 'Add' → build
    # 'Backfill' and 'Update' aren't in the conservative verb map, so
    # stage_hint is None — that's the honest default; caller fills.
    assert result.steps[1].stage_hint is None
    assert result.steps[2].stage_hint is None


def test_numbered_list_handles_paren_and_dash_markers() -> None:
    intent = "1) First thing\n2- Second thing"
    result = decompose_intent(intent)
    assert result.detection_mode == "numbered_list"
    assert [step.text for step in result.steps] == ["First thing", "Second thing"]


def test_bulleted_list_decomposes() -> None:
    intent = (
        "Things to do:\n"
        "- Fix the dispatcher race\n"
        "- Write a test for the race\n"
        "- Review the patch"
    )
    result = decompose_intent(intent)
    assert result.detection_mode == "bulleted_list"
    assert len(result.steps) == 3
    assert result.steps[0].raw_marker == "-"
    assert result.steps[0].stage_hint == "fix"
    assert result.steps[1].stage_hint == "build"  # 'Write' maps to build
    assert result.steps[2].stage_hint == "review"


def test_ordered_phrases_decompose() -> None:
    intent = (
        "First investigate the leak, then patch it, finally verify with a run."
    )
    result = decompose_intent(intent)
    assert result.detection_mode == "ordered_phrases"
    assert len(result.steps) == 3
    # Marker words stripped from step text.
    assert result.steps[0].text.startswith("investigate the leak")
    assert result.steps[0].raw_marker == "first"
    assert result.steps[0].stage_hint == "research"
    assert result.steps[1].raw_marker == "then"
    assert result.steps[1].stage_hint == "fix"
    assert result.steps[2].stage_hint == "test"


def test_free_prose_raises_unless_allow_single_step() -> None:
    intent = "Make the dashboard faster by reducing the number of API calls on load."
    with pytest.raises(DecompositionRequiresLLMError, match="no explicit step markers"):
        decompose_intent(intent)


def test_free_prose_with_allow_single_step_returns_one_step() -> None:
    intent = "Investigate why the checkout fails in staging and write up findings."
    result = decompose_intent(intent, allow_single_step=True)
    assert result.detection_mode == "single_step"
    assert len(result.steps) == 1
    assert result.steps[0].text == intent
    assert result.steps[0].raw_marker is None
    # First verb 'Investigate' → research.
    assert result.steps[0].stage_hint == "research"


def test_single_numbered_item_is_not_enough_to_detect() -> None:
    """One numbered item alone isn't a list — falls through to free-prose path."""
    intent = "1. Just do this one thing and call it done"
    with pytest.raises(DecompositionRequiresLLMError):
        decompose_intent(intent)


def test_empty_intent_rejected() -> None:
    with pytest.raises(ValueError, match="intent is empty"):
        decompose_intent("   ")


def test_version_strings_do_not_count_as_numbered_lists() -> None:
    """A sentence containing '1.0.2' shouldn't accidentally match the numbered-list pattern."""
    intent = "Upgrade Vite to 1.0.2. Then run the tests."
    # No numbered markers at line start; 'then' alone isn't enough; fail closed.
    with pytest.raises(DecompositionRequiresLLMError):
        decompose_intent(intent)


def test_mixed_markers_prefer_numbered_over_bulleted() -> None:
    """When both numbered and bulleted markers appear, numbered wins (more structured)."""
    intent = (
        "1. Step one\n"
        "2. Step two\n"
        "- Side note\n"
        "- Another side note"
    )
    result = decompose_intent(intent)
    assert result.detection_mode == "numbered_list"
    assert len(result.steps) == 2


def test_decomposed_intent_to_dict_round_trips() -> None:
    result = decompose_intent("1. do thing\n2. verify thing")
    payload = result.to_dict()
    assert payload["detection_mode"] == "numbered_list"
    assert payload["steps"][0]["index"] == 0
    assert payload["steps"][0]["stage_hint"] is None  # 'do' not in verb map
    assert payload["steps"][1]["stage_hint"] == "test"  # 'verify' → test
