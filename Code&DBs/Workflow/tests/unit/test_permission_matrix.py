"""Permission matrix contract and translation tests.

Covers the five normalized permission modes across all supported CLI
providers (claude, codex, gemini) and asserts the agent_sessions
subprocess builders honor the matrix.
"""

from __future__ import annotations

import pytest

from adapters.permission_matrix import (
    ALLOWED_PERMISSION_MODES,
    DEFAULT_PERMISSION_MODE,
    PermissionMatrixError,
    SUPPORTED_CLI_PROVIDERS,
    translate_permission_flags,
)


# --- Contract tests --------------------------------------------------------


def test_allowed_modes_are_five_and_ordered_least_to_most_privileged() -> None:
    assert ALLOWED_PERMISSION_MODES == (
        "read_only",
        "plan_only",
        "propose_edits",
        "auto_edits",
        "full_autonomy",
    )


def test_default_mode_is_propose_edits() -> None:
    # Default must be safer than full_autonomy for the operator console.
    # propose_edits is the "every action gated" mode.
    assert DEFAULT_PERMISSION_MODE == "propose_edits"
    assert DEFAULT_PERMISSION_MODE in ALLOWED_PERMISSION_MODES


def test_supported_providers_are_three_cli_providers() -> None:
    assert SUPPORTED_CLI_PROVIDERS == frozenset({"claude", "codex", "gemini"})


def test_every_provider_has_every_mode_mapped() -> None:
    # Contract: no (provider, mode) pair may be missing from the matrix.
    for provider in SUPPORTED_CLI_PROVIDERS:
        for mode in ALLOWED_PERMISSION_MODES:
            # Empty tuple is a valid answer; missing key is not.
            flags = translate_permission_flags(provider, mode)
            assert isinstance(flags, tuple)


# --- Claude translation ----------------------------------------------------


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("read_only",     ("--permission-mode", "plan")),
        ("plan_only",     ("--permission-mode", "plan")),
        ("propose_edits", ("--permission-mode", "default")),
        ("auto_edits",    ("--permission-mode", "acceptEdits")),
        ("full_autonomy", ("--permission-mode", "dontAsk")),
    ],
)
def test_claude_matrix_translation(mode: str, expected: tuple[str, ...]) -> None:
    assert translate_permission_flags("claude", mode) == expected  # type: ignore[arg-type]


# --- Codex translation -----------------------------------------------------


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("read_only",     ("--sandbox", "read-only",       "--approval-mode", "never")),
        ("plan_only",     ("--sandbox", "read-only",       "--approval-mode", "on-request")),
        ("propose_edits", ("--sandbox", "workspace-write", "--approval-mode", "on-request")),
        ("auto_edits",    ("--sandbox", "workspace-write", "--approval-mode", "on-failure")),
        ("full_autonomy", ("--sandbox", "workspace-write", "--approval-mode", "never")),
    ],
)
def test_codex_matrix_translation(mode: str, expected: tuple[str, ...]) -> None:
    assert translate_permission_flags("codex", mode) == expected  # type: ignore[arg-type]


# --- Gemini translation ----------------------------------------------------


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("read_only",     ("--approval-mode", "plan")),
        ("plan_only",     ("--approval-mode", "plan")),
        ("propose_edits", ("--approval-mode", "default")),
        ("auto_edits",    ("--approval-mode", "auto_edit")),
        ("full_autonomy", ("--approval-mode", "yolo")),
    ],
)
def test_gemini_matrix_translation(mode: str, expected: tuple[str, ...]) -> None:
    assert translate_permission_flags("gemini", mode) == expected  # type: ignore[arg-type]


# --- Error paths -----------------------------------------------------------


def test_unknown_provider_raises() -> None:
    with pytest.raises(PermissionMatrixError) as exc:
        translate_permission_flags("unknown_cli", "propose_edits")
    message = str(exc.value)
    assert "unknown_cli" in message
    assert "claude" in message and "codex" in message and "gemini" in message


def test_unknown_mode_raises() -> None:
    with pytest.raises(PermissionMatrixError) as exc:
        translate_permission_flags("claude", "unknown_mode")  # type: ignore[arg-type]
    assert "unknown_mode" in str(exc.value)


def test_provider_slug_is_case_and_whitespace_tolerant() -> None:
    assert translate_permission_flags("  CLAUDE  ", "plan_only") == (
        "--permission-mode",
        "plan",
    )


# --- Agent-sessions builder integration ------------------------------------


def test_claude_builder_uses_matrix_when_permission_mode_supplied() -> None:
    from surfaces.api.agent_sessions import _build_claude_command

    cmd = _build_claude_command(
        "session-abc",
        "hello",
        permission_mode="plan_only",
    )
    assert cmd == [
        "claude",
        "-p",
        "--session-id",
        "session-abc",
        "--output-format",
        "stream-json",
        "--permission-mode",
        "plan",
        "hello",
    ]


def test_claude_builder_falls_back_to_env_default_when_permission_mode_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.api.agent_sessions import _build_claude_command

    monkeypatch.setenv("PRAXIS_AGENT_PERMISSION_MODE", "dontAsk")
    cmd = _build_claude_command("session-xyz", "hi")
    assert "--permission-mode" in cmd
    idx = cmd.index("--permission-mode")
    assert cmd[idx + 1] == "dontAsk"


def test_codex_builder_uses_matrix_when_permission_mode_supplied(tmp_path) -> None:
    from surfaces.api.agent_sessions import _build_codex_command

    reply_path = tmp_path / "reply.txt"
    cmd = _build_codex_command(
        "019abcdef0123456789012345678901234567",
        "do the thing",
        reply_path,
        permission_mode="read_only",
    )
    assert "--sandbox" in cmd
    assert "read-only" in cmd
    assert "--approval-mode" in cmd
    assert "never" in cmd
    assert cmd[-1] == "do the thing"


def test_codex_builder_falls_back_to_env_default_when_permission_mode_absent(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.api.agent_sessions import _build_codex_command

    monkeypatch.setenv("PRAXIS_AGENT_CODEX_SANDBOX", "read-only")
    cmd = _build_codex_command("session-1", "prompt", tmp_path / "out.txt")
    assert "--sandbox" in cmd
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    # When no matrix override, codex gets --sandbox only (existing behavior).
    assert "--approval-mode" not in cmd


# --- Gemini builder + provider acceptance ----------------------------------


def test_gemini_builder_uses_matrix_when_permission_mode_supplied() -> None:
    from surfaces.api.agent_sessions import _build_gemini_command

    cmd = _build_gemini_command("session-gemini", "write a haiku", permission_mode="full_autonomy")
    assert cmd[0] == "gemini"
    assert cmd[1] == "-p"
    assert cmd[2] == "write a haiku"
    assert "--approval-mode" in cmd
    assert cmd[cmd.index("--approval-mode") + 1] == "yolo"


def test_gemini_builder_without_permission_mode_omits_approval_flag() -> None:
    from surfaces.api.agent_sessions import _build_gemini_command

    cmd = _build_gemini_command("session-gemini", "hello")
    assert "--approval-mode" not in cmd
    assert "-o" in cmd
    assert cmd[cmd.index("-o") + 1] == "stream-json"


def test_gemini_builder_honors_model_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api.agent_sessions import _build_gemini_command

    monkeypatch.setenv("PRAXIS_AGENT_GEMINI_MODEL", "gemini-2.5-pro")
    cmd = _build_gemini_command("session-gemini", "hi")
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "gemini-2.5-pro"


def test_cli_provider_accepts_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api.agent_sessions import _cli_provider

    assert _cli_provider("gemini") == "gemini"


def test_cli_provider_rejects_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import HTTPException
    from surfaces.api.agent_sessions import _cli_provider

    with pytest.raises(HTTPException) as exc:
        _cli_provider("llama")
    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert "claude" in detail["message"]
    assert "codex" in detail["message"]
    assert "gemini" in detail["message"]


# --- API validation --------------------------------------------------------


def test_invalid_permission_mode_rejected_at_api_boundary() -> None:
    from fastapi import HTTPException
    from surfaces.api.agent_sessions import _validate_permission_mode

    with pytest.raises(HTTPException) as exc:
        _validate_permission_mode("full-autonomy")  # hyphen not underscore
    assert exc.value.status_code == 400
    detail = exc.value.detail
    assert isinstance(detail, dict)
    assert detail["error_code"] == "agent_sessions_invalid_permission_mode"


def test_none_permission_mode_passes_validation_as_none() -> None:
    from surfaces.api.agent_sessions import _validate_permission_mode

    assert _validate_permission_mode(None) is None


def test_valid_permission_mode_returned_normalized() -> None:
    from surfaces.api.agent_sessions import _validate_permission_mode

    assert _validate_permission_mode("propose_edits") == "propose_edits"
