"""Canonical raw-agent action fingerprint shaping and persistence.

This module turns raw harness tool calls (shell/edit/write/read) into the
shape-only rows stored in ``action_fingerprints``. The host-side hooks send the
raw tool payload through a receipt-backed gateway command; the handler uses
this module so the database, not the hook script, decides what gets stored.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import shlex
from typing import Any

_TOOL_NAME_ALIASES: dict[str, str] = {
    "run_shell_command": "Bash",
    "ShellTool": "Bash",
    "replace": "Edit",
    "write_file": "Write",
    "read_file": "Read",
    "local_shell": "Bash",
    "shell": "Bash",
    "apply_patch": "Edit",
}

_SESSION_ENV_KEYS = (
    "CLAUDE_SESSION_ID",
    "CODEX_SESSION_ID",
    "GEMINI_SESSION_ID",
    "AGENT_SESSION_ID",
    "SESSION_ID",
)

_SAFE_COMMAND_WORDS = {
    "bash",
    "cat",
    "cd",
    "curl",
    "docker",
    "find",
    "git",
    "head",
    "jq",
    "ls",
    "mv",
    "node",
    "npm",
    "pnpm",
    "praxis",
    "psql",
    "py_compile",
    "pytest",
    "python",
    "python3",
    "rg",
    "run",
    "search",
    "sed",
    "sh",
    "show",
    "status",
    "tail",
    "test",
    "tools",
    "uv",
    "workflow",
    "write",
    "zsh",
}

_URL_RE = re.compile(r"^[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f-]{27}$", re.IGNORECASE)
_HEX_RE = re.compile(r"^[0-9a-f]{12,}$", re.IGNORECASE)
_NUMERIC_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
_ENV_ASSIGNMENT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.+)$")


@dataclass(frozen=True, slots=True)
class ActionFingerprintRecord:
    source_surface: str
    action_kind: str
    shape_hash: str
    session_ref: str | None
    payload_meta: dict[str, Any]
    normalized_command: str | None = None
    path_shape: str | None = None


def normalize_tool_name(tool_name: str) -> str:
    return _TOOL_NAME_ALIASES.get(tool_name, tool_name)


def infer_session_ref(env: dict[str, str] | None = None) -> str | None:
    source = env or os.environ
    for key in _SESSION_ENV_KEYS:
        value = str(source.get(key) or "").strip()
        if value:
            return value[:200]
    return None


def build_action_fingerprint_record(
    *,
    tool_name: str,
    tool_input: dict[str, Any],
    source_surface: str,
    session_ref: str | None = None,
    payload_meta: dict[str, Any] | None = None,
    repo_root: str | None = None,
) -> ActionFingerprintRecord | None:
    raw_tool_name = str(tool_name or "").strip()
    if not raw_tool_name or not isinstance(tool_input, dict):
        return None

    canonical_tool_name = normalize_tool_name(raw_tool_name)
    meta = dict(payload_meta or {})
    meta.setdefault("raw_tool_name", raw_tool_name)
    meta.setdefault("tool_name", canonical_tool_name)

    if canonical_tool_name == "Bash":
        command_text = _command_text(tool_input.get("command"))
        normalized_command = _shape_shell_command(command_text, repo_root=repo_root)
        if not normalized_command:
            return None
        shape_input = f"shell|{normalized_command}"
        meta["shape_input"] = shape_input
        return ActionFingerprintRecord(
            source_surface=str(source_surface).strip(),
            action_kind="shell",
            normalized_command=normalized_command,
            path_shape=None,
            shape_hash=hashlib.md5(shape_input.encode("utf-8")).hexdigest(),
            session_ref=_normalized_session_ref(session_ref),
            payload_meta=meta,
        )

    path_shapes = _path_shapes_for_tool(
        raw_tool_name=raw_tool_name,
        canonical_tool_name=canonical_tool_name,
        tool_input=tool_input,
        repo_root=repo_root,
    )
    if not path_shapes:
        return None

    action_kind = _action_kind_for_tool(
        raw_tool_name=raw_tool_name,
        canonical_tool_name=canonical_tool_name,
        path_count=len(path_shapes),
    )
    path_shape = "\n".join(path_shapes)
    shape_input = f"{action_kind}|{path_shape}"
    meta["shape_input"] = shape_input
    meta["path_count"] = len(path_shapes)
    return ActionFingerprintRecord(
        source_surface=str(source_surface).strip(),
        action_kind=action_kind,
        normalized_command=None,
        path_shape=path_shape,
        shape_hash=hashlib.md5(shape_input.encode("utf-8")).hexdigest(),
        session_ref=_normalized_session_ref(session_ref),
        payload_meta=meta,
    )


def record_action_fingerprint(conn: Any, record: ActionFingerprintRecord) -> None:
    conn.execute(
        """
        INSERT INTO action_fingerprints (
            source_surface,
            action_kind,
            normalized_command,
            path_shape,
            shape_hash,
            session_ref,
            payload_meta
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        record.source_surface,
        record.action_kind,
        record.normalized_command,
        record.path_shape,
        record.shape_hash,
        record.session_ref,
        json.dumps(record.payload_meta, sort_keys=True, default=str),
    )


def _normalized_session_ref(session_ref: str | None) -> str | None:
    value = str(session_ref or "").strip()
    return value[:200] if value else None


def _command_text(command: Any) -> str:
    if isinstance(command, list):
        return " ".join(str(part) for part in command)
    return str(command or "")


def _shape_shell_command(command: str, *, repo_root: str | None) -> str:
    text = " ".join(command.strip().split())
    if not text:
        return ""
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    if not tokens:
        return ""

    normalized: list[str] = []
    preserved_words = 0
    for token in tokens:
        shaped = _shape_shell_token(
            token,
            repo_root=repo_root,
            preserved_words=preserved_words,
        )
        normalized.append(shaped)
        if shaped == token.lower():
            preserved_words += 1
    return " ".join(normalized)


def _shape_shell_token(token: str, *, repo_root: str | None, preserved_words: int) -> str:
    stripped = str(token or "").strip()
    if not stripped:
        return "<arg>"
    if stripped.startswith("-"):
        return stripped
    if _URL_RE.match(stripped):
        return "<url>"
    env_match = _ENV_ASSIGNMENT_RE.match(stripped)
    if env_match:
        return f"{env_match.group(1)}=<value>"
    if stripped.startswith("$"):
        return "$VAR"
    if _looks_like_path(stripped):
        return _shape_path(stripped, repo_root=repo_root)
    if _NUMERIC_RE.match(stripped):
        return "<num>"
    if _UUID_RE.match(stripped) or _HEX_RE.match(stripped):
        return "<id>"
    lowered = stripped.lower()
    if lowered in _SAFE_COMMAND_WORDS or (preserved_words < 3 and _is_safe_word(lowered)):
        return lowered
    return "<arg>"


def _is_safe_word(value: str) -> bool:
    return bool(re.match(r"^[a-z][a-z0-9._:-]{0,31}$", value))


def _path_shapes_for_tool(
    *,
    raw_tool_name: str,
    canonical_tool_name: str,
    tool_input: dict[str, Any],
    repo_root: str | None,
) -> tuple[str, ...]:
    if raw_tool_name == "apply_patch":
        return _shape_paths(_patch_file_paths(_first_text(tool_input, ("patch", "input", "content", "text", "command"))), repo_root=repo_root)

    path_values: list[str] = []
    if canonical_tool_name in {"Edit", "Write", "Read"}:
        primary = str(
            tool_input.get("file_path")
            or tool_input.get("path")
            or tool_input.get("target_file")
            or ""
        ).strip()
        if primary:
            path_values.append(primary)
    return _shape_paths(path_values, repo_root=repo_root)


def _shape_paths(paths: list[str] | tuple[str, ...], *, repo_root: str | None) -> tuple[str, ...]:
    shapes: list[str] = []
    for path in paths:
        shaped = _shape_path(path, repo_root=repo_root)
        if shaped and shaped not in shapes:
            shapes.append(shaped)
    return tuple(sorted(shapes))


def _shape_path(path_text: str, *, repo_root: str | None) -> str:
    raw = str(path_text or "").strip().strip("\"'")
    if not raw:
        return ""
    if _URL_RE.match(raw):
        return "<url>"

    normalized = raw.replace("\\", "/")
    rel = normalized
    absolute = normalized.startswith("/")
    if repo_root:
        repo = repo_root.rstrip("/").replace("\\", "/")
        if normalized == repo:
            rel = "."
            absolute = False
        elif normalized.startswith(repo + "/"):
            rel = normalized[len(repo) + 1 :]
            absolute = False

    path = PurePosixPath(rel)
    parts = [part for part in path.parts if part not in ("", ".")]
    if not parts:
        return "<path>"

    filename = parts[-1]
    suffixes = PurePosixPath(filename).suffixes
    filename_shape = "*" + "".join(suffixes[-3:]) if suffixes else "<file>"

    prefix_parts = parts[:-1]
    if absolute:
        if len(prefix_parts) >= 2:
            prefix_parts = ["<abs>", prefix_parts[-1]]
        elif prefix_parts:
            prefix_parts = ["<abs>", prefix_parts[0]]
        else:
            prefix_parts = ["<abs>"]
    elif len(prefix_parts) > 3:
        prefix_parts = [prefix_parts[0], prefix_parts[1], "**", prefix_parts[-1]]

    return "/".join([*prefix_parts, filename_shape])


def _looks_like_path(token: str) -> bool:
    return (
        "/" in token
        or token.startswith(".")
        or token.startswith("~")
        or bool(re.search(r"\.[A-Za-z0-9]{1,8}$", token))
    )


def _action_kind_for_tool(
    *,
    raw_tool_name: str,
    canonical_tool_name: str,
    path_count: int,
) -> str:
    if canonical_tool_name == "Write":
        return "write"
    if canonical_tool_name == "Read":
        return "read"
    if canonical_tool_name == "Edit":
        return "multi_edit" if raw_tool_name == "apply_patch" and path_count > 1 else "edit"
    return "edit"


def _first_text(tool_input: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _patch_file_paths(patch_text: str) -> list[str]:
    paths: list[str] = []
    for line in patch_text.splitlines():
        match = re.match(r"^\*\*\* (?:Add|Update|Delete) File: (.+)$", line)
        if not match:
            continue
        path = match.group(1).strip()
        if path and path not in paths:
            paths.append(path)
    return paths


__all__ = [
    "ActionFingerprintRecord",
    "build_action_fingerprint_record",
    "infer_session_ref",
    "normalize_tool_name",
    "record_action_fingerprint",
]
