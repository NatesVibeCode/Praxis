"""Post-dispatch verification backed by verification_registry authority."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


_DEFAULT_VERIFY_TIMEOUT = int(os.environ.get("PRAXIS_VERIFY_TIMEOUT", "60"))
_MAX_OUTPUT_LEN = int(os.environ.get("PRAXIS_VERIFY_OUTPUT_LIMIT", "2000"))


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
    rows = conn.execute(
        """
        SELECT verify_ref,
               verification_ref,
               label,
               description,
               inputs,
               enabled,
               binding_revision,
               decision_ref
          FROM verify_refs
         WHERE verify_ref = $1
        """,
        verify_ref,
    )
    row = dict(rows[0]) if rows else None
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
    rows = conn.execute(
        """
        SELECT verification_ref,
               display_name,
               executor_kind,
               argv_template,
               template_inputs,
               default_timeout_seconds,
               enabled
          FROM verification_registry
         WHERE verification_ref = ANY($1::text[])
        """,
        refs,
    )
    authority_rows = {str(row["verification_ref"]): dict(row) for row in rows or []}

    commands: list[VerifyCommand] = []
    for binding in bindings:
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
        required_inputs = [str(item) for item in _json_array(row.get("template_inputs"), field_name="template_inputs")]
        for required_input in required_inputs:
            if required_input not in binding.inputs:
                raise VerificationAuthorityError(
                    f"{binding.verification_ref} requires verify input '{required_input}'",
                )
        argv_template = _json_array(row.get("argv_template"), field_name="argv_template")
        argv = _render_argv(argv_template, inputs=binding.inputs, verification_ref=binding.verification_ref)
        commands.append(
            VerifyCommand(
                verification_ref=binding.verification_ref,
                argv=argv,
                label=binding.label or str(row.get("display_name") or binding.verification_ref),
                timeout=int(binding.timeout or row.get("default_timeout_seconds") or _DEFAULT_VERIFY_TIMEOUT),
            )
        )
    return commands


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
        conn.execute_many(
            """
            INSERT INTO verify_refs (
                verify_ref,
                verification_ref,
                label,
                description,
                inputs,
                enabled,
                binding_revision,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6, $7, $8
            )
            ON CONFLICT (verify_ref) DO UPDATE SET
                verification_ref = EXCLUDED.verification_ref,
                label = EXCLUDED.label,
                description = EXCLUDED.description,
                inputs = EXCLUDED.inputs,
                enabled = EXCLUDED.enabled,
                binding_revision = EXCLUDED.binding_revision,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            [
                (
                    row["verify_ref"],
                    row["verification_ref"],
                    row["label"],
                    row.get("description", ""),
                    json.dumps(row.get("inputs") or {}),
                    bool(row.get("enabled", True)),
                    row["binding_revision"],
                    row["decision_ref"],
                )
                for row in verify_refs
            ],
        )
    except Exception as exc:
        raise VerificationAuthorityError(f"failed to persist verify_refs authority rows: {exc}") from exc

    return len(verify_refs)


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
