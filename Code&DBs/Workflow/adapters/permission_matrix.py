"""Normalized permission matrix for CLI agent providers.

Collapses the per-provider permission vocabulary (Claude Code, Codex,
Gemini CLI each have their own flags) into five common modes so the
operator console and agent_sessions API can speak one permission
vocabulary regardless of which CLI is running underneath.

Modes, ordered least-privileged to most-privileged:

    read_only      — observe only, no mutations, no command execution
    plan_only      — produce a plan document, no execution
    propose_edits  — suggest edits and commands; every action approved inline
    auto_edits     — apply edits automatically, command execution still approved
    full_autonomy  — apply edits and run commands without prompting

The matrix lives here (not in ProviderCLIProfile) so translation has one
home. If a provider's native permission model grows richer, extend the
matrix entry — not the consumer code.

Gemini CLI spawn support is not yet wired into
``surfaces.api.agent_sessions``; the matrix row is ready for the follow-up
packet that adds the gemini subprocess builder. Today's consumers
(claude, codex) are fully covered.
"""

from __future__ import annotations

from typing import Literal

__all__ = [
    "ALLOWED_PERMISSION_MODES",
    "API_PROVIDERS",
    "DEFAULT_PERMISSION_MODE",
    "NormalizedPermissionMode",
    "PERMISSION_MODE_RANK",
    "PermissionMatrixError",
    "SUPPORTED_CLI_PROVIDERS",
    "api_permission_prompt_suffix",
    "is_permission_step_up",
    "translate_permission_flags",
]


NormalizedPermissionMode = Literal[
    "read_only",
    "plan_only",
    "propose_edits",
    "auto_edits",
    "full_autonomy",
]


ALLOWED_PERMISSION_MODES: tuple[NormalizedPermissionMode, ...] = (
    "read_only",
    "plan_only",
    "propose_edits",
    "auto_edits",
    "full_autonomy",
)


DEFAULT_PERMISSION_MODE: NormalizedPermissionMode = "propose_edits"


# Strict ordinal ranking of permission modes. Used for step-up detection:
# a transition is a step-up iff RANK[to] > RANK[from].
PERMISSION_MODE_RANK: dict[NormalizedPermissionMode, int] = {
    "read_only":     0,
    "plan_only":     1,
    "propose_edits": 2,
    "auto_edits":    3,
    "full_autonomy": 4,
}


SUPPORTED_CLI_PROVIDERS: frozenset[str] = frozenset({"claude", "codex", "gemini"})


# API-backed providers that talk to an HTTP chat-completions endpoint rather
# than spawning a local CLI. Permission modes for these do not map to argv
# flags — the API has no tool sandbox to toggle, no approval prompts to
# route — so the matrix expresses them as system-prompt suffixes that
# constrain what the model is asked to produce. Honest: this is softer than
# the CLI flag path. An API provider that grows tool-use will need its own
# capability layer on top of this vocabulary.
API_PROVIDERS: frozenset[str] = frozenset({"openrouter"})


_API_PERMISSION_PROMPT_SUFFIX: dict[NormalizedPermissionMode, str] = {
    "read_only": (
        "\n\nPermission: read_only. Answer questions about the workspace and "
        "describe what you observe. Do not propose changes, commands, or actions."
    ),
    "plan_only": (
        "\n\nPermission: plan_only. Produce a structured plan of actions the "
        "operator could take. Do not describe actions as executed or imply "
        "execution is underway."
    ),
    "propose_edits": (
        "\n\nPermission: propose_edits. You may propose specific edits and "
        "commands for the operator to apply. Do not claim to have executed "
        "anything — the API transport cannot run your suggestions."
    ),
    "auto_edits": (
        "\n\nPermission: auto_edits. Propose precise edits the operator's "
        "environment will apply automatically. Be specific about file paths "
        "and exact text."
    ),
    "full_autonomy": (
        "\n\nPermission: full_autonomy. The operator has delegated broad "
        "authority for this turn. Propose edits and commands freely while "
        "staying focused on the task."
    ),
}


class PermissionMatrixError(ValueError):
    """Raised when the matrix cannot translate a provider/mode combination."""


# --- Per-provider tables ---------------------------------------------------
#
# Claude Code (`claude`)
#   --permission-mode values: plan | default | acceptEdits | dontAsk
#   Sandbox is external (Docker wrapper); no CLI flag.
#   read_only and plan_only both resolve to `plan`: the CLI has a single
#   read/plan mode. The distinction between "observe" and "produce a plan
#   I can approve" lives in how the caller interprets the assistant output,
#   not in the flag.

_CLAUDE_MATRIX: dict[NormalizedPermissionMode, tuple[str, ...]] = {
    "read_only":     ("--permission-mode", "plan"),
    "plan_only":     ("--permission-mode", "plan"),
    "propose_edits": ("--permission-mode", "default"),
    "auto_edits":    ("--permission-mode", "acceptEdits"),
    "full_autonomy": ("--permission-mode", "dontAsk"),
}


# Codex (`codex exec`)
#   --sandbox values: disabled | read-only | workspace-write
#   --approval-mode values: untrusted | on-request | on-failure | never
#   read_only pins sandbox=read-only + approval=never (no prompts because
#   no action can escape the sandbox). propose_edits keeps write sandbox
#   but approval=on-request so every action surfaces a prompt. full_autonomy
#   is write sandbox + approval=never.

_CODEX_MATRIX: dict[NormalizedPermissionMode, tuple[str, ...]] = {
    "read_only":     ("--sandbox", "read-only",       "--approval-mode", "never"),
    "plan_only":     ("--sandbox", "read-only",       "--approval-mode", "on-request"),
    "propose_edits": ("--sandbox", "workspace-write", "--approval-mode", "on-request"),
    "auto_edits":    ("--sandbox", "workspace-write", "--approval-mode", "on-failure"),
    "full_autonomy": ("--sandbox", "workspace-write", "--approval-mode", "never"),
}


# Gemini CLI (`gemini`)
#   --approval-mode values: default | auto_edit | yolo | plan
#   plan is read-only, produces a plan; default prompts per-action;
#   auto_edit auto-approves edit tools but prompts for commands; yolo
#   auto-approves everything. The matrix maps both read_only and
#   plan_only to `plan` because gemini's CLI exposes one read-only flag.
#   The distinction between the two lives in how the caller frames the
#   prompt, not in the flag.

_GEMINI_MATRIX: dict[NormalizedPermissionMode, tuple[str, ...]] = {
    "read_only":     ("--approval-mode", "plan"),
    "plan_only":     ("--approval-mode", "plan"),
    "propose_edits": ("--approval-mode", "default"),
    "auto_edits":    ("--approval-mode", "auto_edit"),
    "full_autonomy": ("--approval-mode", "yolo"),
}


_MATRIX: dict[str, dict[NormalizedPermissionMode, tuple[str, ...]]] = {
    "claude": _CLAUDE_MATRIX,
    "codex":  _CODEX_MATRIX,
    "gemini": _GEMINI_MATRIX,
}


def api_permission_prompt_suffix(
    provider_slug: str,
    mode: NormalizedPermissionMode | str | None,
) -> str:
    """Return a system-prompt suffix for an API provider at the given mode.

    Empty string when ``mode`` is None or the provider does not participate
    in the API-prompt-suffix scheme. Unknown modes also return empty —
    typo'd modes do not leak into the system prompt.
    """
    if mode is None:
        return ""
    provider = provider_slug.strip().lower()
    if provider not in API_PROVIDERS:
        return ""
    return _API_PERMISSION_PROMPT_SUFFIX.get(mode, "")  # type: ignore[arg-type]


def is_permission_step_up(
    from_mode: NormalizedPermissionMode | str | None,
    to_mode: NormalizedPermissionMode | str | None,
) -> bool:
    """True iff ``to_mode`` is strictly more privileged than ``from_mode``.

    Returns False when either mode is unknown, when they are equal, or when
    ``to_mode`` is less privileged (a step-down). Unknown modes never count
    as step-ups — they are silently ignored so an audit stream cannot be
    poisoned by typo'd modes.
    """
    if from_mode is None or to_mode is None:
        return False
    from_rank = PERMISSION_MODE_RANK.get(from_mode)  # type: ignore[arg-type]
    to_rank = PERMISSION_MODE_RANK.get(to_mode)  # type: ignore[arg-type]
    if from_rank is None or to_rank is None:
        return False
    return to_rank > from_rank


def translate_permission_flags(
    provider_slug: str,
    mode: NormalizedPermissionMode,
) -> tuple[str, ...]:
    """Return the argv flags for ``(provider_slug, mode)``.

    Raises :class:`PermissionMatrixError` on unsupported provider or
    unknown mode. An empty tuple is a legitimate answer for some
    (provider, mode) pairs (see Gemini's read_only); the caller should
    not treat empty as error.
    """
    provider = provider_slug.strip().lower()
    if provider not in _MATRIX:
        raise PermissionMatrixError(
            f"no permission matrix for provider {provider_slug!r}; "
            f"supported: {sorted(SUPPORTED_CLI_PROVIDERS)}"
        )
    if mode not in ALLOWED_PERMISSION_MODES:
        raise PermissionMatrixError(
            f"unknown permission mode {mode!r}; "
            f"allowed: {list(ALLOWED_PERMISSION_MODES)}"
        )
    return _MATRIX[provider][mode]
