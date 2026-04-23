"""Automatic friction recording for the workflow CLI frontdoor."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

_MAX_CAPTURE_CHARS = 8000
_MAX_MESSAGE_CHARS = 1800
_DEFAULT_AUTO_BUG_THRESHOLD = 3
_DB_UNAVAILABLE_REASON_CODES = {
    "workflow_records.db_authority_unavailable",
    "verifier.db_authority_unavailable",
    "postgres.authority_unavailable",
    "postgres.config_missing",
    "postgres.config_invalid",
}
_NON_PROMOTABLE_REASON_CODES = {
    "cli.unknown_command",
    "cli.unknown_help_topic",
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

    return _record_command_failure(
        args=args,
        exit_code=exit_code,
        output_text=output_text,
        output_truncated=output_truncated,
        env=env,
        source="cli.workflow",
        command_label_prefix="workflow",
        allow_empty_args=False,
    )


def record_shell_command_failure(
    *,
    args: Sequence[str],
    exit_code: int,
    output_text: str,
    output_truncated: bool = False,
    env: Mapping[str, str] | None = None,
    source: str = "cli.praxis",
    command_label_prefix: str = "praxis",
) -> bool:
    """Persist a shell-frontdoor failure into the same friction ledger path.

    Shell launchers should use this for failures that happen before the Python
    CLI frontdoor starts, so pre-main exits still become durable telemetry.
    """

    return _record_command_failure(
        args=args,
        exit_code=exit_code,
        output_text=output_text,
        output_truncated=output_truncated,
        env=env,
        source=source,
        command_label_prefix=command_label_prefix,
        allow_empty_args=True,
    )


def _record_command_failure(
    *,
    args: Sequence[str],
    exit_code: int,
    output_text: str,
    output_truncated: bool,
    env: Mapping[str, str] | None,
    source: str,
    command_label_prefix: str,
    allow_empty_args: bool,
) -> bool:
    if exit_code == 0 or (not args and not allow_empty_args):
        return False
    if _recording_disabled(env):
        return False

    output_payload = _extract_output_payload(output_text)
    reason_code = _extract_reason_code(output_payload, output_text)
    if reason_code in _DB_UNAVAILABLE_REASON_CODES:
        return False

    try:
        (
            friction_ledger_cls,
            friction_type_cls,
            sync_connection_cls,
            workflow_pool_factory,
            database_env_resolver,
        ) = _recording_dependencies()

        repo_root = Path(__file__).resolve().parents[4]
        db_env = (
            dict(env)
            if env is not None and env.get("WORKFLOW_DATABASE_URL")
            else database_env_resolver(repo_root, env=env or os.environ)
        )
        conn = sync_connection_cls(workflow_pool_factory(env=db_env))
        ledger = friction_ledger_cls(conn)
        command_args = list(args) if args else [command_label_prefix]
        fingerprint = cli_failure_fingerprint(command_args, reason_code=reason_code)
        ledger.record(
            friction_type=friction_type_cls.HARD_FAILURE,
            source=source,
            job_label=_command_label(command_args, prefix=command_label_prefix),
            message=_failure_message(
                args=command_args,
                exit_code=exit_code,
                fingerprint=fingerprint,
                reason_code=reason_code,
                output_payload=output_payload,
                output_text=output_text,
                output_truncated=output_truncated,
            ),
        )
        _maybe_promote_command_failure(
            conn=conn,
            ledger=ledger,
            args=command_args,
            fingerprint=fingerprint,
            reason_code=reason_code or "cli.command_failed",
            source=source,
            command_label_prefix=command_label_prefix,
            env=env,
        )
        return True
    except Exception:
        return False


def _recording_dependencies():
    from runtime.friction_ledger import FrictionLedger, FrictionType
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
    from surfaces._workflow_database import workflow_database_env_for_repo

    return (
        FrictionLedger,
        FrictionType,
        SyncPostgresConnection,
        get_workflow_pool,
        workflow_database_env_for_repo,
    )


def _bug_tracker_dependencies():
    from runtime.bug_tracker import BugCategory, BugSeverity, BugTracker

    return BugTracker, BugSeverity, BugCategory


def _recording_disabled(env: Mapping[str, str] | None) -> bool:
    source = env if env is not None else os.environ
    value = str(source.get("PRAXIS_CLI_FRICTION_RECORDING", "")).strip().lower()
    if value in {"1", "true", "on", "yes"}:
        return False
    if value in {"0", "false", "off", "no"}:
        return True
    return bool(source.get("PYTEST_CURRENT_TEST") or os.environ.get("PYTEST_CURRENT_TEST"))


def _auto_bug_disabled(env: Mapping[str, str] | None) -> bool:
    source = env if env is not None else os.environ
    value = str(source.get("PRAXIS_CLI_FRICTION_AUTO_BUGS", "")).strip().lower()
    return value in {"0", "false", "off", "no"}


def _auto_bug_threshold(env: Mapping[str, str] | None) -> int:
    source = env if env is not None else os.environ
    raw_value = source.get("PRAXIS_CLI_FRICTION_AUTO_BUG_THRESHOLD")
    try:
        threshold = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return _DEFAULT_AUTO_BUG_THRESHOLD
    return threshold if threshold > 1 else _DEFAULT_AUTO_BUG_THRESHOLD


def _maybe_promote_command_failure(
    *,
    conn: object,
    ledger: object,
    args: Sequence[str],
    fingerprint: str,
    reason_code: str,
    source: str,
    command_label_prefix: str,
    env: Mapping[str, str] | None,
) -> bool:
    if _auto_bug_disabled(env) or not _promotable_failure(args, reason_code):
        return False
    threshold = _auto_bug_threshold(env)
    try:
        patterns = ledger.patterns(  # type: ignore[attr-defined]
            source=source,
            limit=20,
            scan_limit=500,
            promotion_threshold=threshold,
        )
        pattern = next(
            (
                item
                for item in patterns
                if getattr(item, "fingerprint", "") == fingerprint
            ),
            None,
        )
        if pattern is None or not bool(getattr(pattern, "promotion_candidate", False)):
            return False

        source_issue_id = f"{source}-friction:{fingerprint}"
        bug_tracker_cls, severity_cls, category_cls = _bug_tracker_dependencies()
        tracker = bug_tracker_cls(conn)
        existing = tracker.list_bugs(
            source_issue_id=source_issue_id,
            open_only=False,
            limit=1,
        )
        if existing:
            return False

        command = str(getattr(pattern, "command", "") or _command_label(args, prefix=command_label_prefix))
        count = int(getattr(pattern, "count", 0) or 0)
        sample = str(getattr(pattern, "sample", "") or "")
        event_ids = list(getattr(pattern, "event_ids", ()) or ())
        tracker.file_bug(
            title=_truncate_text(f"[{source}] {command} repeats with {reason_code}", 180),
            severity=severity_cls.P2,
            category=category_cls.RUNTIME,
            description=(
                "Repeated command friction crossed the promotion threshold. "
                f"source={source}; fingerprint={fingerprint}; command={command}; "
                f"reason_code={reason_code}; count={count}; sample={sample}"
            ),
            filed_by="cli_friction_auto_promoter",
            source_kind="friction_ledger",
            source_issue_id=source_issue_id,
            tags=("auto_cli_friction", source),
            resume_context={
                "fingerprint": fingerprint,
                "source": source,
                "reason_code": reason_code,
                "command": command,
                "count": count,
                "event_ids": event_ids,
                "promotion_threshold": threshold,
            },
        )
        return True
    except Exception:
        return False


def _promotable_failure(args: Sequence[str], reason_code: str) -> bool:
    if reason_code in _NON_PROMOTABLE_REASON_CODES:
        return False
    if not args:
        return False
    command = " ".join(str(arg) for arg in args[:2])
    return not (args[0] == "bugs" or command == "workflow bugs")


def _command_label(args: Sequence[str], *, prefix: str = "workflow") -> str:
    if not args:
        return prefix
    command = args[0]
    if command == "help" and len(args) > 1:
        return f"{prefix} help {args[1]}"
    return f"{prefix} {command}"


def _truncate_text(value: str, max_chars: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 14].rstrip() + "...[truncated]"


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
    "record_shell_command_failure",
]
