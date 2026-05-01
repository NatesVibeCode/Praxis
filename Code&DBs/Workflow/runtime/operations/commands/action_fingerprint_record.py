"""Gateway command for persisting raw shell/edit/write/read fingerprints."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.agent_action_fingerprints import (
    build_action_fingerprint_record,
    record_action_fingerprint,
)
from runtime.workspace_paths import repo_root as workspace_repo_root


class ActionFingerprintRecordInput(BaseModel):
    """Input contract for one raw-agent action observation."""

    tool_name: str = Field(
        ...,
        description="Raw harness tool name, e.g. local_shell, Bash, apply_patch, read_file.",
    )
    tool_input: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw tool payload from the harness hook.",
    )
    source_surface: str = Field(
        ...,
        description="Origin tag, e.g. codex:host, claude-code:host, gemini:host.",
    )
    session_ref: str | None = Field(
        default=None,
        description="Optional harness session identifier when available.",
    )
    payload_meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional bounded metadata to store alongside the shape row.",
    )

    @field_validator("tool_name", "source_surface")
    @classmethod
    def _require_nonblank(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field must be non-empty")
        return text


def handle_action_fingerprint_record(
    command: ActionFingerprintRecordInput,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    fingerprint = build_action_fingerprint_record(
        tool_name=command.tool_name,
        tool_input=dict(command.tool_input or {}),
        source_surface=command.source_surface,
        session_ref=command.session_ref,
        payload_meta=dict(command.payload_meta or {}),
        repo_root=str(workspace_repo_root()),
    )
    if fingerprint is None:
        return {
            "ok": True,
            "recorded": False,
            "reason": "unsupported_or_unshapeable_tool_input",
            "source_surface": command.source_surface,
            "tool_name": command.tool_name,
            "event_payload": {
                "source_surface": command.source_surface,
                "tool_name": command.tool_name,
                "recorded": False,
                "reason": "unsupported_or_unshapeable_tool_input",
                "session_ref": command.session_ref,
            },
        }

    record_action_fingerprint(conn, fingerprint)
    return {
        "ok": True,
        "recorded": True,
        "source_surface": fingerprint.source_surface,
        "action_kind": fingerprint.action_kind,
        "shape_hash": fingerprint.shape_hash,
        "normalized_command": fingerprint.normalized_command,
        "path_shape": fingerprint.path_shape,
        "session_ref": fingerprint.session_ref,
        "event_payload": {
            "source_surface": fingerprint.source_surface,
            "action_kind": fingerprint.action_kind,
            "shape_hash": fingerprint.shape_hash,
            "normalized_command": fingerprint.normalized_command,
            "path_shape": fingerprint.path_shape,
            "session_ref": fingerprint.session_ref,
            "recorded": True,
        },
    }


__all__ = [
    "ActionFingerprintRecordInput",
    "handle_action_fingerprint_record",
]
