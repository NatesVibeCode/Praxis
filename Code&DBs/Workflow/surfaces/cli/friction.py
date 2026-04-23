"""Automatic friction recording for the workflow CLI frontdoor."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

from runtime.friction_ledger import FrictionLedger, FrictionType
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
from surfaces._workflow_database import workflow_database_env_for_repo

_MAX_CAPTURE_CHARS = 8000
_MAX_MESSAGE_CHARS = 1800
_DB_UNAVAILABLE_REASON_CODES = {
    "workflow_records.db_authority_unavailable",
    "verifier.db_authority_unavailable",
    "postgres.authority_unavailable",
    "postgres.config_missing",
    "postgres.config_invalid",
}


class TrackingStdout:
    """Tee stdout while retaining a bounded command output summary."""

    def __init__(self, target: TextIO, *, max_chars: int = _MAX_CAPTURE_CHARS) -> None:
        self._target = target
        self._max_chars = max_chars
        self._chunks: list[str] = []
        self._captured_chars = 0
        self.truncated = False

    def write(self, value: str) -> int:
        written = self._target.write(value)
        if self._captured_chars < self._max_chars:
            remaining = self._max_chars - self._captured_chars
            captured = value[:remaining]
            self._chunks.append(captured)
            self._captured_chars += len(captured)
            if len(value) > remaining:
                self.truncated = True
        elif value:
            self.truncated = True
        return written

    def flush(self) -> None:
        self._target.flush()

    def getvalue(self) -> str:
        return self._target.getvalue()  # type: ignore[attr-defined]

    def captured_output(self) -> str:
        return "".join(self._chunks)

    def __getattr__(self, name: str):
        return getattr(self._target, name)


def cli_failure_fingerprint(args: Sequence[str], *, reason_code: str | None = None) -> str:
    command = _command_label(args)
    basis = json.dumps(
        {"command": command, "reason_code": reason_code or ""},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.blake2s(basis.encode("utf-8"), digest_size=8).hexdigest()


def record_cli_command_failure(
    *,
    args: Sequence[str],
    exit_code: int,
    output_text: str,
    output_truncated: bool = False,
    env: Mapping[str, str] | None = None,
) -> bool:
    """Persist a failed CLI command into the canonical friction ledger.

    The original command result remains authoritative. Telemetry recording is
    best-effort and must never make the operator command fail harder.
    """

    if exit_code == 0 or not args:
        return False
    if _recording_disabled(env):
        return False

    output_payload = _extract_output_payload(output_text)
    reason_code = _extract_reason_code(output_payload, output_text)
    if reason_code in _DB_UNAVAILABLE_REASON_CODES:
        return False

    try:
        repo_root = Path(__file__).resolve().parents[4]
        db_env = (
            dict(env)
            if env is not None and env.get("WORKFLOW_DATABASE_URL")
            else workflow_database_env_for_repo(repo_root, env=env or os.environ)
        )
        conn = SyncPostgresConnection(get_workflow_pool(env=db_env))
        ledger = FrictionLedger(conn)
        fingerprint = cli_failure_fingerprint(args, reason_code=reason_code)
        ledger.record(
            friction_type=FrictionType.HARD_FAILURE,
            source="cli.workflow",
            job_label=_command_label(args),
            message=_failure_message(
                args=args,
                exit_code=exit_code,
                fingerprint=fingerprint,
                reason_code=reason_code,
                output_payload=output_payload,
                output_text=output_text,
                output_truncated=output_truncated,
            ),
        )
        return True
    except Exception:
        return False


def _recording_disabled(env: Mapping[str, str] | None) -> bool:
    source = env if env is not None else os.environ
    value = str(source.get("PRAXIS_CLI_FRICTION_RECORDING", "")).strip().lower()
    return value in {"0", "false", "off", "no"}


def _command_label(args: Sequence[str]) -> str:
    if not args:
        return "workflow"
    command = args[0]
    if command == "help" and len(args) > 1:
        return f"workflow help {args[1]}"
    return f"workflow {command}"


def _extract_output_payload(output_text: str) -> dict[str, object] | None:
    text = output_text.strip()
    if not text or not text.startswith("{"):
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_reason_code(
    output_payload: dict[str, object] | None,
    output_text: str,
) -> str | None:
    if output_payload is not None:
        raw = output_payload.get("reason_code") or output_payload.get("code")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    lower_output = output_text.lower()
    if "unknown command:" in lower_output:
        return "cli.unknown_command"
    if "unknown help topic:" in lower_output:
        return "cli.unknown_help_topic"
    if "does not support arguments" in lower_output:
        return "cli.unsupported_arguments"
    return None


def _failure_message(
    *,
    args: Sequence[str],
    exit_code: int,
    fingerprint: str,
    reason_code: str | None,
    output_payload: dict[str, object] | None,
    output_text: str,
    output_truncated: bool,
) -> str:
    summary = _output_summary(output_payload, output_text)
    payload = {
        "event": "cli_command_failure",
        "fingerprint": fingerprint,
        "exit_code": exit_code,
        "command": " ".join(shlex.quote(arg) for arg in args),
        "reason_code": reason_code or "cli.command_failed",
        "output": summary,
        "output_truncated": output_truncated,
    }
    message = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(message) <= _MAX_MESSAGE_CHARS:
        return message
    payload["output"] = str(summary)[:240] + "...[truncated]"
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))[:_MAX_MESSAGE_CHARS]


def _output_summary(
    output_payload: dict[str, object] | None,
    output_text: str,
) -> str:
    if output_payload is not None:
        for key in ("message", "error", "detail", "status"):
            value = output_payload.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())[:700]
    return " ".join(output_text.split())[:700]


__all__ = [
    "TrackingStdout",
    "cli_failure_fingerprint",
    "record_cli_command_failure",
]
