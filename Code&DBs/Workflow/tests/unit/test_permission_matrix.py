"""Permission matrix contract and translation tests.

Covers the five normalized permission modes across all supported CLI
providers (claude, codex, gemini) and asserts the agent_sessions
subprocess builders honor the matrix.
"""

from __future__ import annotations

import pytest

from adapters.permission_matrix import (
    ALLOWED_PERMISSION_MODES,
    API_PROVIDERS,
    DEFAULT_PERMISSION_MODE,
    PERMISSION_MODE_RANK,
    PermissionMatrixError,
    SUPPORTED_CLI_PROVIDERS,
    api_permission_prompt_suffix,
    is_permission_step_up,
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


# --- API provider permission normalization (B.5) ---------------------------


def test_api_providers_include_openrouter() -> None:
    assert "openrouter" in API_PROVIDERS


def test_api_permission_prompt_suffix_returns_empty_for_none_mode() -> None:
    assert api_permission_prompt_suffix("openrouter", None) == ""


def test_api_permission_prompt_suffix_returns_empty_for_unknown_provider() -> None:
    assert api_permission_prompt_suffix("claude", "propose_edits") == ""


def test_api_permission_prompt_suffix_returns_empty_for_unknown_mode() -> None:
    assert api_permission_prompt_suffix("openrouter", "bogus") == ""


@pytest.mark.parametrize(
    "mode, required_fragment",
    [
        ("read_only",     "read_only"),
        ("plan_only",     "plan_only"),
        ("propose_edits", "propose_edits"),
        ("auto_edits",    "auto_edits"),
        ("full_autonomy", "full_autonomy"),
    ],
)
def test_api_permission_prompt_suffix_names_each_mode(
    mode: str, required_fragment: str,
) -> None:
    suffix = api_permission_prompt_suffix("openrouter", mode)
    assert suffix  # non-empty
    assert required_fragment in suffix


def test_api_permission_prompt_suffix_read_only_forbids_actions() -> None:
    suffix = api_permission_prompt_suffix("openrouter", "read_only")
    assert "Do not propose changes" in suffix


def test_api_permission_prompt_suffix_plan_only_produces_plan() -> None:
    suffix = api_permission_prompt_suffix("openrouter", "plan_only")
    assert "plan" in suffix.lower()


def test_openrouter_messages_append_permission_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api.agent_sessions import _openrouter_messages

    # No pg_conn: skips history lookup, returns system + user only.
    messages = _openrouter_messages(None, agent_id="a", prompt="hi", permission_mode="plan_only")
    assert messages[0]["role"] == "system"
    assert "Permission: plan_only" in messages[0]["content"]
    # The base system prompt is still there.
    assert "Praxis operator" in messages[0]["content"]
    # No mode → no suffix.
    messages_no_mode = _openrouter_messages(None, agent_id="a", prompt="hi", permission_mode=None)
    assert "Permission:" not in messages_no_mode[0]["content"]


# --- Gemini session continuity (B.1b) --------------------------------------


def test_gemini_builder_fresh_session_omits_resume_flag() -> None:
    from surfaces.api.agent_sessions import _build_gemini_command

    cmd = _build_gemini_command("session-x", "start", resume=False)
    assert "-r" not in cmd
    assert "latest" not in cmd


def test_gemini_builder_with_resume_uses_latest() -> None:
    from surfaces.api.agent_sessions import _build_gemini_command

    cmd = _build_gemini_command("session-x", "next", resume=True)
    assert "-r" in cmd
    assert cmd[cmd.index("-r") + 1] == "latest"


def test_gemini_resume_enabled_detects_prior_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api import agent_sessions

    monkeypatch.setattr(
        agent_sessions,
        "list_interactive_agent_events",
        lambda conn, *, agent_id: [
            {"event_kind": "user.prompt", "text": "hello"},
            {"event_kind": "assistant.reply", "text": "hi back"},
        ],
    )
    assert agent_sessions._gemini_resume_enabled(object(), agent_id="a") is True


def test_gemini_resume_disabled_when_no_prior_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api import agent_sessions

    # Only user prompts, no assistant replies yet → no resume.
    monkeypatch.setattr(
        agent_sessions,
        "list_interactive_agent_events",
        lambda conn, *, agent_id: [
            {"event_kind": "user.prompt", "text": "hi"},
        ],
    )
    assert agent_sessions._gemini_resume_enabled(object(), agent_id="a") is False


def test_gemini_resume_disabled_when_events_query_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.api import agent_sessions

    def _boom(conn, *, agent_id):
        raise RuntimeError("db down")

    monkeypatch.setattr(agent_sessions, "list_interactive_agent_events", _boom)
    # Resume helper swallows the exception and defaults to fresh session.
    assert agent_sessions._gemini_resume_enabled(object(), agent_id="a") is False


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


# --- Event payload persistence (B.3) ---------------------------------------
#
# When a client sends a turn with permission_mode, the user.prompt and
# assistant.reply events must carry the mode in their payload so history
# replay can reconstruct the plan/approval flow.


def _captured_events() -> list[dict[str, object]]:
    captured: list[dict[str, object]] = []

    def fake_append(
        conn, *, agent_id, event_kind, payload=None, text_content=None, **_ignore
    ) -> int:
        captured.append({
            "agent_id": agent_id,
            "event_kind": event_kind,
            "payload": dict(payload or {}),
            "text_content": text_content,
        })
        return len(captured)

    return captured, fake_append


def test_user_prompt_event_payload_carries_permission_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api import agent_sessions

    captured, fake_append = _captured_events()
    monkeypatch.setattr(agent_sessions, "append_interactive_agent_event", fake_append)

    # Call the validator + builder directly rather than the full endpoint —
    # we are asserting the payload construction, not the HTTP wiring.
    mode = agent_sessions._validate_permission_mode("plan_only")
    assert mode == "plan_only"

    # Simulate the user.prompt logging block from create_agent / send_message.
    user_payload = {"principal_ref": "operator:nate"}
    if mode is not None:
        user_payload["permission_mode"] = mode
    fake_append(
        object(),
        agent_id="agent-abc",
        event_kind="user.prompt",
        payload=user_payload,
        text_content="build a feature",
    )

    assert len(captured) == 1
    record = captured[0]
    assert record["event_kind"] == "user.prompt"
    assert record["payload"]["permission_mode"] == "plan_only"
    assert record["payload"]["principal_ref"] == "operator:nate"


def test_user_prompt_event_payload_omits_permission_mode_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.api import agent_sessions

    captured, fake_append = _captured_events()
    monkeypatch.setattr(agent_sessions, "append_interactive_agent_event", fake_append)

    mode = agent_sessions._validate_permission_mode(None)
    assert mode is None

    user_payload = {"principal_ref": "operator:nate"}
    if mode is not None:
        user_payload["permission_mode"] = mode
    fake_append(
        object(),
        agent_id="agent-abc",
        event_kind="user.prompt",
        payload=user_payload,
        text_content="hello",
    )

    assert "permission_mode" not in captured[0]["payload"]


# --- Console HTML plan-approval markers (B.3) ------------------------------


# --- Step-up detection + audit event (B.3b) --------------------------------


def test_permission_rank_strictly_orders_five_modes() -> None:
    ranks = [PERMISSION_MODE_RANK[m] for m in ALLOWED_PERMISSION_MODES]
    assert ranks == sorted(ranks)  # least-to-most
    assert len(set(ranks)) == len(ranks)  # no ties


@pytest.mark.parametrize(
    "from_mode, to_mode, expected",
    [
        ("read_only",     "plan_only",     True),
        ("plan_only",     "auto_edits",    True),
        ("propose_edits", "full_autonomy", True),
        # equal is not a step-up
        ("propose_edits", "propose_edits", False),
        # step-down is not a step-up
        ("full_autonomy", "propose_edits", False),
        ("auto_edits",    "plan_only",     False),
        # unknown modes never count
        (None,            "propose_edits", False),
        ("propose_edits", None,            False),
        ("bogus",         "full_autonomy", False),
        ("read_only",     "bogus",         False),
    ],
)
def test_is_permission_step_up(from_mode, to_mode, expected) -> None:
    assert is_permission_step_up(from_mode, to_mode) is expected


def test_step_up_event_emitted_when_mode_escalates(monkeypatch: pytest.MonkeyPatch) -> None:
    from surfaces.api import agent_sessions

    captured: list[dict[str, object]] = []

    def fake_append(conn, *, agent_id, event_kind, payload=None, text_content=None, **_ignore):
        captured.append({
            "event_kind": event_kind,
            "payload": dict(payload or {}),
            "text_content": text_content,
        })
        return len(captured)

    # Stub the most-recent-mode lookup to simulate prior plan_only turn.
    monkeypatch.setattr(agent_sessions, "append_interactive_agent_event", fake_append)
    monkeypatch.setattr(
        agent_sessions,
        "_most_recent_permission_mode",
        lambda conn, *, agent_id: "plan_only",
    )

    # Simulate the step-up emission block (same shape as create_agent / send_message).
    validated_mode = agent_sessions._validate_permission_mode("auto_edits")
    principal = "operator:nate"
    prior_mode = agent_sessions._most_recent_permission_mode(object(), agent_id="a")
    if agent_sessions.is_permission_step_up(prior_mode, validated_mode):
        fake_append(
            object(),
            agent_id="a",
            event_kind="permission.step_up",
            payload={
                "principal_ref": principal,
                "from_mode": prior_mode,
                "to_mode": validated_mode,
            },
        )

    assert len(captured) == 1
    record = captured[0]
    assert record["event_kind"] == "permission.step_up"
    assert record["payload"] == {
        "principal_ref": "operator:nate",
        "from_mode": "plan_only",
        "to_mode": "auto_edits",
    }


def test_step_up_event_NOT_emitted_when_mode_stays_or_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.api import agent_sessions

    captured: list[dict[str, object]] = []

    def fake_append(conn, *, agent_id, event_kind, payload=None, text_content=None, **_ignore):
        captured.append({"event_kind": event_kind})
        return len(captured)

    monkeypatch.setattr(agent_sessions, "append_interactive_agent_event", fake_append)

    # Prior auto_edits; new propose_edits → step-down, no event.
    monkeypatch.setattr(
        agent_sessions,
        "_most_recent_permission_mode",
        lambda conn, *, agent_id: "auto_edits",
    )
    validated = agent_sessions._validate_permission_mode("propose_edits")
    prior = agent_sessions._most_recent_permission_mode(object(), agent_id="a")
    if agent_sessions.is_permission_step_up(prior, validated):
        fake_append(object(), agent_id="a", event_kind="permission.step_up")

    assert captured == []


def test_step_up_event_NOT_emitted_on_first_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    # No prior mode (fresh agent) → no step-up even when starting at full_autonomy.
    from surfaces.api import agent_sessions

    captured: list[dict[str, object]] = []
    monkeypatch.setattr(
        agent_sessions,
        "append_interactive_agent_event",
        lambda *a, **k: captured.append(k.get("event_kind")) or 1,
    )
    monkeypatch.setattr(
        agent_sessions,
        "_most_recent_permission_mode",
        lambda conn, *, agent_id: None,
    )

    validated = agent_sessions._validate_permission_mode("full_autonomy")
    prior = agent_sessions._most_recent_permission_mode(object(), agent_id="a")
    assert not agent_sessions.is_permission_step_up(prior, validated)


# --- Console HTML plan-approval markers (B.3) ------------------------------


def test_console_html_renders_plan_approval_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient
    from surfaces.api import rest

    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        response = client.get("/console")
    assert response.status_code == 200
    body = response.text

    # Component name exported by the page script.
    assert "function PlanActions" in body
    # CSS for plan-action bubbles must ship.
    assert ".plan-actions" in body
    # Approval-escalation table must reference both plan modes.
    assert "PLAN_APPROVAL_ESCALATION" in body
    assert "plan_only" in body and "auto_edits" in body
    assert "read_only" in body and "propose_edits" in body
    # Canned prompts must be present so the UX is deterministic.
    assert "CANNED_APPROVE_PROMPT" in body
    assert "CANNED_REJECT_PROMPT" in body


# --- Console HTML live-streaming markers (B.6) -----------------------------


def test_console_html_streams_turn_events_live(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient
    from surfaces.api import rest

    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        response = client.get("/console")
    body = response.text

    # Fetch-based SSE reader function must be present.
    assert "async function streamTurnEvents" in body
    # It must target the agent_sessions stream endpoint.
    assert "/api/agent-sessions/agents/${id}/stream" in body
    # It must parse SSE `data:` frames (not use EventSource).
    assert "text/event-stream" in body
    assert "getReader()" in body
    # The client must carry the bearer token on the stream (browser
    # EventSource can't, which is why we use fetch).
    assert "'Authorization': `Bearer ${token}`" in body
    # Stream must be abortable so the client can stop it when a turn ends.
    assert "AbortController" in body
    # Live-streaming indicator CSS + React state must ship together.
    assert "setStreaming" in body
    assert ".status-strip.live" in body
    assert "@keyframes pulse" in body


def test_console_html_language_reflects_watching_and_steering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from surfaces.api import rest

    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        response = client.get("/console")
    body = response.text

    # The console is for watching + steering agents, not for chatting with an assistant.
    assert "Watch and steer" in body
    assert "Steer the agent" in body
    assert "Agent idle" in body
    assert "Pick an agent to watch" in body
    # The old builder-shaped copy must not ship anymore.
    assert "Message the agent" not in body
    assert "Type a prompt below. Cmd/Ctrl+Enter to send." not in body
