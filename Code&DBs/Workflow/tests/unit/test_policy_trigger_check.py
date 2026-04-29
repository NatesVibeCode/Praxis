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


def test_infer_harness_from_native_tool_names() -> None:
    assert trigger_check._infer_harness("Bash") == "claude_code"
    assert trigger_check._infer_harness("local_shell") == "codex_cli"
    assert trigger_check._infer_harness("run_shell_command") == "gemini_cli"
    assert trigger_check._infer_harness("praxis_search") == ""


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
    assert matches[0].advisory_only is True


def test_explicit_trigger_is_not_advisory_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::explicit-docker-restart",
                "title": "docker restart blocked",
                "decision_provenance": "explicit",
                "match": [{"tool": "Bash", "regex": r"^\s*docker\s+restart"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    matches = trigger_check.check("Bash", {"command": "docker restart praxis-x"})
    assert len(matches) == 1
    assert matches[0].advisory_only is False


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


def test_codex_apply_patch_extracts_file_paths_for_edit_triggers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::cqrs-wizard",
                "title": "use cqrs wizard",
                "match": [
                    {
                        "tool": "Edit",
                        "file_glob": "**/runtime/operations/**/*.py",
                        "advisory_only": True,
                    }
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    patch = """*** Begin Patch
*** Update File: /Users/nate/Praxis/Code&DBs/Workflow/runtime/operations/queries/platform_patterns.py
@@
+    include_hydration: bool = False
*** End Patch
"""

    matches = trigger_check.check("apply_patch", {"patch": patch})
    assert len(matches) == 1
    assert matches[0].decision_key == "test::cqrs-wizard"


def test_repo_policy_contract_rules_materialize_into_existing_hook_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = _write_registry(tmp_path, [])
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))
    monkeypatch.setattr(
        trigger_check,
        "_load_repo_policy_contract_payload",
        lambda: {
            "repo_policy_contract_id": "repo_policy_contract.test",
            "repo_policy_sections": {
                "forbidden_action_rules": [
                    {
                        "rule_id": "forbidden_action_rule.test",
                        "raw_text": "delete migrations/*",
                        "action": "delete",
                        "path_glob": "migrations/*",
                        "path_substring": None,
                        "enforcement_level": "hard",
                        "machine_enforceable": True,
                    }
                ]
            },
        },
    )

    patch = """*** Begin Patch
*** Delete File: migrations/001_old.sql
*** End Patch
"""

    matches = trigger_check.check("apply_patch", {"patch": patch})

    assert len(matches) == 1
    assert matches[0].decision_key == (
        "repo-policy::repo_policy_contract.test::forbidden_action_rule.test"
    )
    assert matches[0].advisory_only is False
    assert matches[0].condition["matched_operation"] == {
        "action": "delete",
        "path": "migrations/001_old.sql",
    }


def test_codex_apply_patch_string_match_sees_patch_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::cqrs-registry",
                "title": "use cqrs wizard",
                "match": [
                    {
                        "tool": "Edit",
                        "string_match": "operation_catalog_registry",
                    }
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    patch = """*** Begin Patch
*** Update File: /repo/migrations/workflow/999_demo.sql
@@
+INSERT INTO operation_catalog_registry (operation_name) VALUES ('demo');
*** End Patch
"""

    matches = trigger_check.check("apply_patch", {"content": patch})
    assert len(matches) == 1
    assert matches[0].decision_key == "test::cqrs-registry"


def test_harness_scoped_trigger_matches_only_declared_harness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::claude-only",
                "title": "Claude-only standing order",
                "match": [
                    {
                        "harness": "claude_code",
                        "tool": "Bash",
                        "regex": r"^\s*praxis\s+workflow\b",
                    }
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    assert len(trigger_check.check("Bash", {"command": "praxis workflow bugs list"})) == 1
    assert trigger_check.check("local_shell", {"command": "praxis workflow bugs list"}) == []
    assert trigger_check.check("run_shell_command", {"command": "praxis workflow bugs list"}) == []


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


# ─── Session cooldown (BUG-3E9820C4) ──────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_cooldown_cache():
    """The cooldown set is module-global; reset between tests so dedupe
    behavior is deterministic per test."""
    trigger_check._ADVISORY_FIRED.clear()
    yield
    trigger_check._ADVISORY_FIRED.clear()


def test_advisory_trigger_dedupes_consecutive_same_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same advisory + same file fires once per session, not on every edit."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::advisory",
                "title": "advisory",
                "match": [
                    {"tool": "Edit", "file_glob": "**/foo.py", "advisory_only": True}
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    target = {"file_path": "/tmp/foo.py", "old_string": "x", "new_string": "y"}
    first = trigger_check.check("Edit", target)
    second = trigger_check.check("Edit", target)
    assert len(first) == 1
    assert len(second) == 0  # deduped


def test_advisory_trigger_refires_on_different_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Different file = different cooldown key — fires again."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::advisory",
                "title": "advisory",
                "match": [
                    {"tool": "Edit", "file_glob": "**/*.py", "advisory_only": True}
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    a = trigger_check.check("Edit", {"file_path": "/tmp/foo.py", "old_string": "x", "new_string": "y"})
    b = trigger_check.check("Edit", {"file_path": "/tmp/bar.py", "old_string": "x", "new_string": "y"})
    assert len(a) == 1
    assert len(b) == 1  # different file → different cooldown key


def test_explicit_trigger_never_dedupes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Operator-binding explicit triggers always fire — never silenced."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::explicit",
                "title": "explicit",
                "decision_provenance": "explicit",
                "match": [
                    # Explicit provenance defaults to binding.
                    {"tool": "Bash", "regex": r"^echo"},
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    a = trigger_check.check("Bash", {"command": "echo hi"})
    b = trigger_check.check("Bash", {"command": "echo hi"})
    assert len(a) == 1
    assert len(b) == 1  # explicit triggers ALWAYS fire


def test_inferred_trigger_without_flag_dedupes_as_advisory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Old registry rows without provenance are advisory and cooldowned."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::legacy-inferred",
                "title": "legacy inferred row",
                "match": [{"tool": "Bash", "regex": r"^echo"}],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))

    a = trigger_check.check("Bash", {"command": "echo hi"})
    b = trigger_check.check("Bash", {"command": "echo hi"})
    assert len(a) == 1
    assert b == []


def test_cross_subprocess_cooldown_via_marker_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When PRAXIS_SESSION_COOLDOWN_DIR is set, marker files persist
    fired pairs across subprocess invocations. Simulated by clearing the
    in-process cache between calls — the marker file should still dedupe."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::cross-subprocess",
                "title": "cross-subprocess advisory",
                "match": [
                    {"tool": "Edit", "file_glob": "**/foo.py", "advisory_only": True}
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))
    cooldown_dir = tmp_path / "cooldown"
    monkeypatch.setenv("PRAXIS_SESSION_COOLDOWN_DIR", str(cooldown_dir))

    target = {"file_path": "/tmp/foo.py", "old_string": "x", "new_string": "y"}
    first = trigger_check.check("Edit", target)
    assert len(first) == 1
    # Marker file written under the cooldown dir.
    assert cooldown_dir.exists()
    assert any(cooldown_dir.iterdir())

    # Simulate a second subprocess: clear the in-process cache so only
    # the marker file can drive dedupe.
    trigger_check._ADVISORY_FIRED.clear()
    second = trigger_check.check("Edit", target)
    assert len(second) == 0  # marker file caused dedupe across "subprocess"


def test_cooldown_disabled_when_marker_dir_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without PRAXIS_SESSION_COOLDOWN_DIR, only in-process cache dedupes;
    a fresh process (simulated by clearing the set) re-fires."""
    registry_path = _write_registry(
        tmp_path,
        [
            {
                "decision_key": "test::no-marker",
                "title": "advisory",
                "match": [
                    {"tool": "Edit", "file_glob": "**/foo.py", "advisory_only": True}
                ],
            }
        ],
    )
    monkeypatch.setenv("PRAXIS_TRIGGER_REGISTRY", str(registry_path))
    monkeypatch.delenv("PRAXIS_SESSION_COOLDOWN_DIR", raising=False)

    target = {"file_path": "/tmp/foo.py", "old_string": "x", "new_string": "y"}
    first = trigger_check.check("Edit", target)
    assert len(first) == 1
    # Same process — in-process cache dedupes.
    assert len(trigger_check.check("Edit", target)) == 0
    # Simulate fresh process (clear in-process cache, no marker dir to fall back on).
    trigger_check._ADVISORY_FIRED.clear()
    third = trigger_check.check("Edit", target)
    assert len(third) == 1  # re-fires — no on-disk marker
