"""Post-dispatch verification backed by verification_registry authority."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection
    from storage.postgres.verification_repository import PostgresVerificationRepository


_DEFAULT_VERIFY_TIMEOUT = int(os.environ.get("PRAXIS_VERIFY_TIMEOUT", "60"))
_MAX_OUTPUT_LEN = int(os.environ.get("PRAXIS_VERIFY_OUTPUT_LIMIT", "2000"))
_FILE_NON_EMPTY_VERIFICATION_REF = "verification.file.non_empty"
_LEGACY_VERIFY_COMMAND_DECISION_REF = "decision.verify_refs.legacy_verify_command_bridge.20260428"


class VerificationAuthorityError(RuntimeError):
    """Raised when verification authority rows are missing or malformed."""


@dataclass(frozen=True, slots=True)
class VerificationBinding:
    verification_ref: str
    inputs: dict[str, Any]
    label: str | None = None
    timeout: int | None = None


@dataclass(frozen=True, slots=True)
class VerifyCommand:
    verification_ref: str
    argv: tuple[str, ...]
    label: str
    timeout: int = _DEFAULT_VERIFY_TIMEOUT

    def to_json(self) -> dict[str, Any]:
        return {
            "verification_ref": self.verification_ref,
            "argv": list(self.argv),
            "label": self.label,
            "timeout": self.timeout,
            "command": shlex.join(self.argv),
        }


@dataclass(frozen=True, slots=True)
class VerifyResult:
    label: str
    command: str
    passed: bool
    exit_code: int
    stdout: str
    stderr: str
    latency_ms: int
    verification_ref: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "command": self.command,
            "passed": self.passed,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "latency_ms": self.latency_ms,
            "verification_ref": self.verification_ref,
        }


@dataclass(frozen=True, slots=True)
class VerificationSummary:
    total: int
    passed: int
    failed: int
    results: tuple[VerifyResult, ...]
    all_passed: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "all_passed": self.all_passed,
            "results": [r.to_json() for r in self.results],
        }


def _truncate(text: str) -> str:
    if len(text) <= _MAX_OUTPUT_LEN:
        return text
    return text[: _MAX_OUTPUT_LEN - 15] + "\n...[truncated]"


def _json_array(value: object, *, field_name: str) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise VerificationAuthorityError(f"{field_name} must decode to a JSON array") from exc
    if not isinstance(value, list):
        raise VerificationAuthorityError(f"{field_name} must be a JSON array")
    return list(value)


def _verify_ref_to_binding(
    conn: "SyncPostgresConnection",
    verify_ref: str,
) -> VerificationBinding:
    row = _repository(conn).load_verify_ref(verify_ref=verify_ref)
    if row is None:
        raise VerificationAuthorityError(f"verify_refs missing {verify_ref}")
    if not bool(row.get("enabled")):
        raise VerificationAuthorityError(f"verify_refs row {verify_ref} is disabled")

    inputs = row.get("inputs") or {}
    if isinstance(inputs, str):
        try:
            inputs = json.loads(inputs)
        except json.JSONDecodeError as exc:
            raise VerificationAuthorityError(f"verify_refs row {verify_ref} has invalid inputs JSON") from exc
    if not isinstance(inputs, dict):
        raise VerificationAuthorityError(f"verify_refs row {verify_ref} inputs must be an object")

    verification_ref = str(row.get("verification_ref") or "").strip()
    if not verification_ref:
        raise VerificationAuthorityError(f"verify_refs row {verify_ref} is missing verification_ref")

    label = row.get("label")
    return VerificationBinding(
        verification_ref=verification_ref,
        inputs=dict(inputs),
        label=str(label).strip() if isinstance(label, str) and label.strip() else None,
    )


def _verify_ref_row_to_binding(row: dict[str, Any]) -> VerificationBinding:
    inputs = row.get("inputs") or {}
    if isinstance(inputs, str):
        try:
            inputs = json.loads(inputs)
        except json.JSONDecodeError as exc:
            raise VerificationAuthorityError(
                f"verify_refs row {row.get('verify_ref')} has invalid inputs JSON",
            ) from exc
    if not isinstance(inputs, dict):
        raise VerificationAuthorityError(f"verify_refs row {row.get('verify_ref')} inputs must be an object")

    verification_ref = str(row.get("verification_ref") or "").strip()
    if not verification_ref:
        raise VerificationAuthorityError(f"verify_refs row {row.get('verify_ref')} is missing verification_ref")
    label = row.get("label")
    return VerificationBinding(
        verification_ref=verification_ref,
        inputs=dict(inputs),
        label=str(label).strip() if isinstance(label, str) and label.strip() else None,
    )


def _render_argv(argv_template: list[Any], *, inputs: dict[str, Any], verification_ref: str) -> tuple[str, ...]:
    rendered: list[str] = []
    rendered_inputs = {key: str(value) for key, value in inputs.items()}
    for index, part in enumerate(argv_template):
        if not isinstance(part, str):
            raise VerificationAuthorityError(
                f"{verification_ref} argv_template[{index}] must be a string",
            )
        try:
            rendered.append(part.format(**rendered_inputs))
        except KeyError as exc:
            missing = str(exc).strip("'")
            raise VerificationAuthorityError(
                f"{verification_ref} missing required verification input '{missing}'",
            ) from exc
    if not rendered:
        raise VerificationAuthorityError(f"{verification_ref} argv_template cannot be empty")
    return tuple(rendered)


def resolve_verification_bindings(
    conn: "SyncPostgresConnection",
    bindings: list[VerificationBinding] | None,
) -> list[VerifyCommand]:
    bindings = list(bindings or [])
    if not bindings:
        return []

    rows = _repository(conn).list_verification_registry_rows(
        verification_refs=[binding.verification_ref for binding in bindings],
    )
    authority_rows = {str(row["verification_ref"]): dict(row) for row in rows or []}

    commands: list[VerifyCommand] = []
    for index, binding in enumerate(bindings):
        if not isinstance(binding, VerificationBinding):
            raise VerificationAuthorityError(
                f"verification_bindings[{index}] must be a VerificationBinding",
            )
        row = authority_rows.get(binding.verification_ref)
        if row is None:
            raise VerificationAuthorityError(
                f"verification_registry missing {binding.verification_ref}",
            )
        if not bool(row.get("enabled")):
            raise VerificationAuthorityError(
                f"verification_registry row {binding.verification_ref} is disabled",
            )
        if str(row.get("executor_kind") or "").strip() != "argv":
            raise VerificationAuthorityError(
                f"{binding.verification_ref} executor_kind must be 'argv'",
            )
        required_inputs = [
            str(item)
            for item in _json_array(row.get("template_inputs"), field_name="template_inputs")
        ]
        for required_input in required_inputs:
            if required_input not in binding.inputs:
                raise VerificationAuthorityError(
                    f"{binding.verification_ref} requires verify input '{required_input}'",
                )
        argv_template = _json_array(row.get("argv_template"), field_name="argv_template")
        argv = _render_argv(
            argv_template,
            inputs=binding.inputs,
            verification_ref=binding.verification_ref,
        )
        commands.append(
            VerifyCommand(
                verification_ref=binding.verification_ref,
                argv=argv,
                label=binding.label or str(row.get("display_name") or binding.verification_ref),
                timeout=int(binding.timeout or row.get("default_timeout_seconds") or _DEFAULT_VERIFY_TIMEOUT),
            )
        )
    return commands


def resolve_verify_commands(
    conn: "SyncPostgresConnection",
    raw_bindings: list[object] | None,
) -> list[VerifyCommand]:
    raw_bindings = list(raw_bindings or [])
    if not raw_bindings:
        return []

    refs: list[str] = []
    for index, raw in enumerate(raw_bindings):
        if not isinstance(raw, str):
            raise VerificationAuthorityError(
                f"verify_refs[{index}] must be a non-empty string resolved through verify_refs",
            )
        verify_ref = raw.strip()
        if not verify_ref:
            raise VerificationAuthorityError(f"verify_refs[{index}] must be a non-empty string")
        refs.append(verify_ref)

    bindings = [_verify_ref_to_binding(conn, verify_ref) for verify_ref in refs]
    return resolve_verification_bindings(conn, bindings)


def sync_verify_refs(
    conn: "SyncPostgresConnection",
    *,
    verify_refs: list[dict[str, Any]] | None = None,
) -> int:
    """Best-effort upsert of canonical verify_ref authority rows."""
    if conn is None:
        return 0
    if not verify_refs:
        return 0

    try:
        return _repository(conn).upsert_verify_refs(verify_refs=verify_refs)
    except Exception as exc:
        raise VerificationAuthorityError(f"failed to persist verify_refs authority rows: {exc}") from exc

def sync_verification_registry(
    conn: "SyncPostgresConnection",
    *,
    verification_registry: list[dict[str, Any]] | None = None,
) -> int:
    """Best-effort upsert of canonical verification executor rows."""
    if conn is None:
        return 0
    if not verification_registry:
        return 0

    try:
        return _repository(conn).upsert_verification_registry(
            verification_registry=verification_registry,
        )
    except Exception as exc:
        raise VerificationAuthorityError(
            f"failed to persist verification_registry authority rows: {exc}",
        ) from exc


def _file_non_empty_verification_registry_row() -> dict[str, Any]:
    return {
        "verification_ref": _FILE_NON_EMPTY_VERIFICATION_REF,
        "display_name": "Non-empty File",
        "description": "Verify that a declared artifact path exists and is non-empty.",
        "executor_kind": "argv",
        "argv_template": ["test", "-s", "{path}"],
        "template_inputs": ["path"],
        "default_timeout_seconds": 30,
        "workdir_policy": "job",
        "enabled": True,
        "decision_ref": _LEGACY_VERIFY_COMMAND_DECISION_REF,
    }


def _file_non_empty_path_from_legacy_verify_command(verify_command: str) -> str | None:
    command = str(verify_command or "").strip()
    if not command:
        return None
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    if len(argv) == 3 and argv[0] == "test" and argv[1] == "-s" and argv[2].strip():
        return argv[2].strip()
    if (
        len(argv) == 4
        and argv[0] == "["
        and argv[1] == "-s"
        and argv[3] == "]"
        and argv[2].strip()
    ):
        return argv[2].strip()
    return None


def sync_verify_command_refs(
    conn: "SyncPostgresConnection",
    *,
    verify_refs: list[str] | None,
    verify_command: str | None,
    label: str | None = None,
) -> int:
    """Bridge legacy safe artifact checks into DB-backed verify_ref rows.

    Only the generated artifact contract ``test -s <path>`` is admitted. Other
    legacy commands remain unregistered so verification still fails closed.
    """
    if conn is None:
        return 0
    refs = [
        item.strip()
        for item in list(verify_refs or [])
        if isinstance(item, str) and item.strip() and item.strip().startswith("verify.")
    ]
    if not refs:
        return 0
    path = _file_non_empty_path_from_legacy_verify_command(str(verify_command or ""))
    if not path:
        return 0

    repo = _repository(conn)
    missing_refs = [verify_ref for verify_ref in refs if repo.load_verify_ref(verify_ref=verify_ref) is None]
    if not missing_refs:
        return 0

    registry_count = sync_verification_registry(
        conn,
        verification_registry=[_file_non_empty_verification_registry_row()],
    )
    verify_rows = []
    label_text = str(label or "").strip() or "artifact"
    for verify_ref in missing_refs:
        digest = sha256(f"{verify_ref}|{path}".encode("utf-8")).hexdigest()[:12]
        verify_rows.append(
            {
                "verify_ref": verify_ref,
                "verification_ref": _FILE_NON_EMPTY_VERIFICATION_REF,
                "label": f"Non-empty artifact for {label_text}",
                "description": f"Verify generated artifact {path}",
                "inputs": {"path": path},
                "enabled": True,
                "binding_revision": f"binding.legacy_verify_command.{digest}",
                "decision_ref": _LEGACY_VERIFY_COMMAND_DECISION_REF,
            }
        )
    return registry_count + sync_verify_refs(conn, verify_refs=verify_rows)


def _repository(conn: "SyncPostgresConnection") -> "PostgresVerificationRepository":
    from storage.postgres.verification_repository import PostgresVerificationRepository

    return PostgresVerificationRepository(conn)


def run_verify(
    commands: list[VerifyCommand],
    *,
    workdir: str | None = None,
) -> tuple[VerifyResult, ...]:
    results: list[VerifyResult] = []

    for cmd in commands:
        start_ns = time.monotonic_ns()
        command_text = shlex.join(cmd.argv)
        try:
            proc = subprocess.run(
                list(cmd.argv),
                shell=False,
                capture_output=True,
                text=True,
                timeout=cmd.timeout,
                cwd=workdir,
            )
            latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            results.append(
                VerifyResult(
                    label=cmd.label,
                    command=command_text,
                    passed=proc.returncode == 0,
                    exit_code=proc.returncode,
                    stdout=_truncate(proc.stdout),
                    stderr=_truncate(proc.stderr),
                    latency_ms=latency_ms,
                    verification_ref=cmd.verification_ref,
                )
            )
        except subprocess.TimeoutExpired:
            latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            results.append(
                VerifyResult(
                    label=cmd.label,
                    command=command_text,
                    passed=False,
                    exit_code=-1,
                    stdout="",
                    stderr=f"timed out after {cmd.timeout}s",
                    latency_ms=latency_ms,
                    verification_ref=cmd.verification_ref,
                )
            )
        except OSError as exc:
            latency_ms = (time.monotonic_ns() - start_ns) // 1_000_000
            results.append(
                VerifyResult(
                    label=cmd.label,
                    command=command_text,
                    passed=False,
                    exit_code=-1,
                    stdout="",
                    stderr=f"execution error: {exc}",
                    latency_ms=latency_ms,
                    verification_ref=cmd.verification_ref,
                )
            )

    return tuple(results)


def summarize_verification(results: tuple[VerifyResult, ...]) -> VerificationSummary:
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    return VerificationSummary(
        total=len(results),
        passed=passed,
        failed=failed,
        results=results,
        all_passed=failed == 0,
    )
