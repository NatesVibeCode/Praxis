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
    "DEFAULT_PERMISSION_MODE",
    "NormalizedPermissionMode",
    "PermissionMatrixError",
    "SUPPORTED_CLI_PROVIDERS",
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


SUPPORTED_CLI_PROVIDERS: frozenset[str] = frozenset({"claude", "codex", "gemini"})


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
#   Permission granularity is coarser than Claude/Codex: `--yolo` auto-
#   accepts everything; its absence prompts. Finer-grained modes
#   (read_only, plan_only, propose_edits) cannot be expressed purely via
#   flags and must be enforced by:
#     - read_only:   mount the workspace read-only at the Docker boundary
#                    and rely on gemini's default prompting to deny writes
#     - plan_only:   prompt-engineer the assistant to emit a plan, no exec
#     - propose_edits: default (no --yolo), rely on prompts
#   auto_edits and full_autonomy collapse to the same flag (--yolo); the
#   distinction between them is enforced by the sandbox, not by gemini.

_GEMINI_MATRIX: dict[NormalizedPermissionMode, tuple[str, ...]] = {
    "read_only":     (),
    "plan_only":     (),
    "propose_edits": (),
    "auto_edits":    ("--yolo",),
    "full_autonomy": ("--yolo",),
}


_MATRIX: dict[str, dict[NormalizedPermissionMode, tuple[str, ...]]] = {
    "claude": _CLAUDE_MATRIX,
    "codex":  _CODEX_MATRIX,
    "gemini": _GEMINI_MATRIX,
}


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
