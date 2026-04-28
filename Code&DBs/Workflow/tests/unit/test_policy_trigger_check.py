"""Unit tests for surfaces.policy.trigger_check.

Covers:
  - _normalize_tool_name aliases (Gemini/Codex native names → registry canonical)
  - check() finds matches via tool-name aliasing + native names equally
  - the matcher integrates correctly when called from a harness with
    Gemini-style or Codex-style tool input shapes

The trigger registry path is mocked by writing a tiny fixture file and
pointing PRAXIS_TRIGGER_REGISTRY at it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from surfaces.policy import trigger_check


# Reset the lru_cache between tests since the registry path varies per test.
@pytest.fixture(autouse=True)
def _clear_registry_cache():
    trigger_check._load_registry_cached.cache_clear()
    yield
    trigger_check._load_registry_cached.cache_clear()


def _write_registry(tmp_path: Path, triggers: list[dict]) -> Path:
    path = tmp_path / "operator-decision-triggers.json"
    path.write_text(
        json.dumps({"$schema_version": 1, "triggers": triggers}),
        encoding="utf-8",
    )
    return path


def test_normalize_tool_name_gemini_aliases() -> None:
    assert trigger_check._normalize_tool_name("run_shell_command") == "Bash"
    assert trigger_check._normalize_tool_name("ShellTool") == "Bash"
    assert trigger_check._normalize_tool_name("replace") == "Edit"
    assert trigger_check._normalize_tool_name("write_file") == "Write"
    assert trigger_check._normalize_tool_name("read_file") == "Read"


def test_normalize_tool_name_codex_aliases() -> None:
    assert trigger_check._normalize_tool_name("local_shell") == "Bash"
    assert trigger_check._normalize_tool_name("shell") == "Bash"
    assert trigger_check._normalize_tool_name("apply_patch") == "Edit"


def test_normalize_tool_name_canonical_passthrough() -> None:
    """Canonical Claude-Code names normalize to themselves (identity)."""
    for name in ("Bash", "Edit", "MultiEdit", "Write", "Read"):
        assert trigger_check._normalize_tool_name(name) == name


def test_normalize_tool_name_unknown_passthrough() -> None:
    """Unknown tool names pass through unchanged — no normalization."""
    for name in ("praxis_search", "some_random_tool", ""):
        assert trigger_check._normalize_tool_name(name) == name


def test_check_matches_via_canonical_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::docker-restart",
                "title": "docker restart blocked",
                "match": [{"tool": "Bash", "regex": r"^\s*docker\s+restart"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "docker restart praxis-x"})
    assert len(matches) == 1
    assert matches[0].decision_key == "test::docker-restart"


def test_check_matches_via_gemini_alias(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Gemini's run_shell_command should fire the same Bash trigger."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::docker-restart",
                "title": "docker restart blocked",
                "match": [{"tool": "Bash", "regex": r"^\s*docker\s+restart"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("run_shell_command", {"command": "docker restart praxis-x"})
    assert len(matches) == 1
    assert matches[0].decision_key == "test::docker-restart"


def test_check_matches_via_codex_alias(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Codex's local_shell should fire the same Bash trigger."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::docker-restart",
                "title": "docker restart blocked",
                "match": [{"tool": "Bash", "regex": r"^\s*docker\s+restart"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("local_shell", {"command": "docker restart praxis-x"})
    assert len(matches) == 1


def test_check_file_glob_via_gemini_write_file_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gemini's write_file matches a file_glob trigger registered against Write."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::dockerfile-edits",
                "title": "Dockerfile edits surface a warning",
                "match": [{"tool": "Write", "file_glob": "**/*.Dockerfile"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check(
        "write_file",
        {"file_path": "/repo/sandboxes/praxis-claude.Dockerfile", "content": "..."},
    )
    assert len(matches) == 1


def test_check_no_match_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::never-fires",
                "title": "never fires",
                "match": [{"tool": "Bash", "regex": r"^never_appears$"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    assert trigger_check.check("Bash", {"command": "ls"}) == []


def test_check_empty_registry_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry_path = _write_registry(tmp_path, [])
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    assert trigger_check.check("Bash", {"command": "anything"}) == []


def test_check_missing_registry_returns_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Registry path doesn't exist → degrade gracefully, return empty list."""
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(tmp_path / "does-not-exist.json"))

    assert trigger_check.check("Bash", {"command": "anything"}) == []


def test_check_corrupt_registry_returns_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Malformed JSON → degrade gracefully."""
    bad = tmp_path / "operator-decision-triggers.json"
    bad.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(bad))

    assert trigger_check.check("Bash", {"command": "anything"}) == []


def test_check_advisory_only_flag_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """advisory_only on the matched condition surfaces in the TriggerMatch."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::soft-warn",
                "title": "soft warning",
                "match": [
                    {"tool": "Bash", "regex": r"^echo", "advisory_only": True}
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "echo hi"})
    assert len(matches) == 1
    assert matches[0].advisory_only is True


def test_check_multiple_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A command matching two distinct decisions returns one match per decision."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::has-docker",
                "title": "docker present",
                "match": [{"tool": "Bash", "regex": r"docker"}],
            },
            {
                "decision_key": "test::has-restart",
                "title": "restart present",
                "match": [{"tool": "Bash", "regex": r"restart"}],
            },
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "docker restart x"})
    assert len(matches) == 2
    assert {m.decision_key for m in matches} == {
        "test::has-docker",
        "test::has-restart",
    }


def test_check_string_match_in_write_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """string_match should fire on Write content, regardless of harness."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::no-deepseek",
                "title": "no deepseek",
                "match": [{"tool": "Write", "string_match": r"(?i)deepseek"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check(
        "write_file",
        {"file_path": "/repo/foo.py", "content": "model = 'DeepSeek-R1'"},
    )
    assert len(matches) == 1


def test_check_invalid_regex_skipped_not_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bad regex in the registry must not crash check() — log and skip."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::bad-regex",
                "title": "bad regex",
                "match": [{"tool": "Bash", "regex": r"["}],  # unterminated
            },
            {
                "decision_key": "test::good-regex",
                "title": "good regex",
                "match": [{"tool": "Bash", "regex": r"hello"}],
            },
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "hello world"})
    keys = {m.decision_key for m in matches}
    # The bad regex one is skipped; the good one fires.
    assert "test::good-regex" in keys
    assert "test::bad-regex" not in keys


def test_render_additional_context_format(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rendered surface includes the standing-order title + decision_key + trigger summary."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::demo",
                "title": "demo standing order",
                "match": [{"tool": "Bash", "regex": r"^echo"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "echo hi"})
    out = trigger_check.render_additional_context(matches, "Bash")
    assert "STANDING ORDER MATCH" in out
    assert "demo standing order" in out
    assert "test::demo" in out
    assert "Bash" in out


def test_render_additional_context_empty_returns_empty_string() -> None:
    assert trigger_check.render_additional_context([], "Bash") == ""
