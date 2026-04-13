"""Verification runtime helpers for the unified workflow worker.

This module owns post-execution verification binding resolution and status
mapping so the worker orchestrator does not also own proof execution details.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Callable

from ._shared import _json_loads_maybe, _workflow_run_envelope

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


def get_verify_bindings(
    conn: "SyncPostgresConnection",
    run_id: str,
) -> list[str] | None:
    """Extract verify bindings from a workflow run request envelope."""
    rows = conn.execute(
        """SELECT request_envelope
           FROM workflow_runs WHERE run_id = $1""",
        run_id,
    )
    if not rows:
        return None

    envelope = _workflow_run_envelope(dict(rows[0]))
    snapshot = envelope.get("spec_snapshot") or envelope
    raw = snapshot.get("verify_refs")
    if not raw:
        return None
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, list) or not parsed:
        return None
    result = [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
    return result or None


def extract_verification_paths(bindings: list[dict[str, Any]] | None) -> list[str]:
    """Collect explicit verification coverage paths from verifier bindings."""
    paths: set[str] = set()
    if not bindings:
        return []
    singular_keys = ("path", "file", "target", "module")
    plural_keys = ("paths", "files", "targets", "write_scope", "file_paths", "modules")
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        inputs = binding.get("inputs")
        if not isinstance(inputs, dict):
            continue
        for key in singular_keys:
            value = inputs.get(key)
            if isinstance(value, str) and value.strip():
                paths.add(value.strip())
        for key in plural_keys:
            value = inputs.get(key)
            if isinstance(value, str) and value.strip():
                paths.add(value.strip())
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        paths.add(item.strip())
    return sorted(paths)


def run_post_execution_verification(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    job_id: int,
    label: str,
    repo_root: str,
    result: dict[str, Any],
    initial_status: str,
    initial_error_code: str,
    binding_loader: Callable[["SyncPostgresConnection", str], list[str] | None] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run DB-backed verification after execution and return receipt-ready state."""
    final_status = initial_status
    final_error_code = initial_error_code
    verification_summary = None
    verification_bindings = None
    verification_error = None
    bound_loader = binding_loader or get_verify_bindings
    log = logger or logging.getLogger(__name__)

    if final_status == "succeeded":
        try:
            verification_bindings = bound_loader(conn, run_id)
            if verification_bindings:
                from runtime.verification import (
                    resolve_verify_commands,
                    run_verify,
                    summarize_verification,
                )

                verify_results = run_verify(
                    resolve_verify_commands(conn, verification_bindings),
                    workdir=repo_root,
                )
                verification_summary = summarize_verification(verify_results)

                if not verification_summary.all_passed:
                    final_status = "failed"
                    final_error_code = "verification.failed"
                    failed_labels = [
                        item.label for item in verification_summary.results if not item.passed
                    ]
                    log.warning(
                        "Job %d (%s) verification FAILED: %d/%d checks passed. Failed: %s",
                        job_id,
                        label,
                        verification_summary.passed,
                        verification_summary.total,
                        ", ".join(failed_labels),
                    )
                    verify_stderr = "\n".join(
                        f"[VERIFY FAIL] {item.label}: exit={item.exit_code} stderr={item.stderr[:200]}"
                        for item in verification_summary.results
                        if not item.passed
                    )
                    result["stderr"] = (result.get("stderr", "") + "\n" + verify_stderr).strip()
                else:
                    log.info(
                        "Job %d (%s) verification passed: %d/%d checks",
                        job_id,
                        label,
                        verification_summary.passed,
                        verification_summary.total,
                    )
        except Exception as exc:
            verification_error = str(exc)
            final_status = "failed"
            final_error_code = "verification.error"
            log.warning("Verification failed to run for job %d: %s", job_id, exc)

    return {
        "result": result,
        "final_status": final_status,
        "final_error_code": final_error_code,
        "verification_summary": verification_summary,
        "verification_bindings": verification_bindings,
        "verification_error": verification_error,
    }
