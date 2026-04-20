"""Verifier/healer authority backed by Postgres registry tables.

This module turns platform verification and repair into explicit control-plane
objects instead of ad hoc scripts. Verifiers and healers are loaded from one
authority path, executed through a small runtime, and recorded durably.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

import runtime.verifier_bug_bridge as _verifier_bug_bridge
import runtime.verifier_builtins as _verifier_builtins
from runtime.bug_evidence import (
    EVIDENCE_ROLE_ATTEMPTED_FIX,
    EVIDENCE_ROLE_OBSERVED_IN,
    EVIDENCE_ROLE_VALIDATES_FIX,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection
    from storage.postgres.verification_repository import PostgresVerificationRepository


class VerifierAuthorityError(RuntimeError):
    """Raised when verifier/healer authority rows are missing or malformed."""


_CONTROL_PLANE_AUTO_BUG_THRESHOLD = 3


@dataclass(frozen=True, slots=True)
class VerifierDefinition:
    verifier_ref: str
    display_name: str
    description: str
    verifier_kind: str
    verification_ref: str | None
    builtin_ref: str | None
    default_inputs: dict[str, Any]
    enabled: bool
    decision_ref: str

    def to_json(self) -> dict[str, Any]:
        return {
            "verifier_ref": self.verifier_ref,
            "display_name": self.display_name,
            "description": self.description,
            "verifier_kind": self.verifier_kind,
            "verification_ref": self.verification_ref,
            "builtin_ref": self.builtin_ref,
            "default_inputs": dict(self.default_inputs),
            "enabled": self.enabled,
            "decision_ref": self.decision_ref,
        }


@dataclass(frozen=True, slots=True)
class HealerDefinition:
    healer_ref: str
    display_name: str
    description: str
    executor_kind: str
    action_ref: str
    auto_mode: str
    safety_mode: str
    enabled: bool
    decision_ref: str

    def to_json(self) -> dict[str, Any]:
        return {
            "healer_ref": self.healer_ref,
            "display_name": self.display_name,
            "description": self.description,
            "executor_kind": self.executor_kind,
            "action_ref": self.action_ref,
            "auto_mode": self.auto_mode,
            "safety_mode": self.safety_mode,
            "enabled": self.enabled,
            "decision_ref": self.decision_ref,
        }


@dataclass(frozen=True, slots=True)
class VerifierHealerBinding:
    binding_ref: str
    verifier_ref: str
    healer_ref: str
    enabled: bool
    binding_revision: str
    decision_ref: str

    def to_json(self) -> dict[str, Any]:
        return {
            "binding_ref": self.binding_ref,
            "verifier_ref": self.verifier_ref,
            "healer_ref": self.healer_ref,
            "enabled": self.enabled,
            "binding_revision": self.binding_revision,
            "decision_ref": self.decision_ref,
        }


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def _connection(conn: "SyncPostgresConnection | None" = None) -> "SyncPostgresConnection":
    if conn is not None:
        return conn
    from storage.postgres.connection import ensure_postgres_available

    return ensure_postgres_available()


def _optional_connection(conn: "SyncPostgresConnection | None" = None) -> "SyncPostgresConnection | None":
    if conn is not None:
        return conn
    try:
        return _connection()
    except Exception:
        return None


def _verification_repository(
    conn: "SyncPostgresConnection | None" = None,
) -> "PostgresVerificationRepository":
    from storage.postgres.verification_repository import PostgresVerificationRepository

    return PostgresVerificationRepository(_connection(conn))


def _verifier_from_row(row: dict[str, Any]) -> VerifierDefinition:
    verifier_ref = str(row.get("verifier_ref") or "").strip()
    if not verifier_ref:
        raise VerifierAuthorityError("verifier_registry row missing verifier_ref")
    verifier_kind = str(row.get("verifier_kind") or "").strip()
    if verifier_kind not in {"verification_ref", "builtin"}:
        raise VerifierAuthorityError(f"invalid verifier_kind for {verifier_ref}: {verifier_kind}")
    return VerifierDefinition(
        verifier_ref=verifier_ref,
        display_name=str(row.get("display_name") or verifier_ref).strip(),
        description=str(row.get("description") or "").strip(),
        verifier_kind=verifier_kind,
        verification_ref=str(row.get("verification_ref") or "").strip() or None,
        builtin_ref=str(row.get("builtin_ref") or "").strip() or None,
        default_inputs=_json_object(row.get("default_inputs")),
        enabled=bool(row.get("enabled")),
        decision_ref=str(row.get("decision_ref") or "").strip(),
    )


def _healer_from_row(row: dict[str, Any]) -> HealerDefinition:
    healer_ref = str(row.get("healer_ref") or "").strip()
    if not healer_ref:
        raise VerifierAuthorityError("healer_registry row missing healer_ref")
    return HealerDefinition(
        healer_ref=healer_ref,
        display_name=str(row.get("display_name") or healer_ref).strip(),
        description=str(row.get("description") or "").strip(),
        executor_kind=str(row.get("executor_kind") or "").strip(),
        action_ref=str(row.get("action_ref") or "").strip(),
        auto_mode=str(row.get("auto_mode") or "manual").strip(),
        safety_mode=str(row.get("safety_mode") or "guarded").strip(),
        enabled=bool(row.get("enabled")),
        decision_ref=str(row.get("decision_ref") or "").strip(),
    )


def _binding_from_row(row: dict[str, Any]) -> VerifierHealerBinding:
    binding_ref = str(row.get("binding_ref") or "").strip()
    if not binding_ref:
        raise VerifierAuthorityError("verifier_healer_bindings row missing binding_ref")
    return VerifierHealerBinding(
        binding_ref=binding_ref,
        verifier_ref=str(row.get("verifier_ref") or "").strip(),
        healer_ref=str(row.get("healer_ref") or "").strip(),
        enabled=bool(row.get("enabled")),
        binding_revision=str(row.get("binding_revision") or "").strip(),
        decision_ref=str(row.get("decision_ref") or "").strip(),
    )


def list_registered_verifiers(
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[VerifierDefinition, ...]:
    rows = _verification_repository(conn).list_registered_verifiers()
    return tuple(_verifier_from_row(dict(row)) for row in rows or [])


def list_registered_healers(
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[HealerDefinition, ...]:
    rows = _verification_repository(conn).list_registered_healers()
    return tuple(_healer_from_row(dict(row)) for row in rows or [])


def list_verifier_healer_bindings(
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[VerifierHealerBinding, ...]:
    rows = _verification_repository(conn).list_verifier_healer_bindings()
    return tuple(_binding_from_row(dict(row)) for row in rows or [])


def _load_verifier(
    verifier_ref: str,
    *,
    conn: "SyncPostgresConnection | None" = None,
) -> VerifierDefinition:
    row = _verification_repository(conn).load_verifier(verifier_ref=verifier_ref)
    if row is None:
        raise VerifierAuthorityError(f"verifier_registry missing {verifier_ref}")
    definition = _verifier_from_row(row)
    if not definition.enabled:
        raise VerifierAuthorityError(f"verifier_registry row {verifier_ref} is disabled")
    return definition


def _load_healer(
    healer_ref: str,
    *,
    conn: "SyncPostgresConnection | None" = None,
) -> HealerDefinition:
    row = _verification_repository(conn).load_healer(healer_ref=healer_ref)
    if row is None:
        raise VerifierAuthorityError(f"healer_registry missing {healer_ref}")
    definition = _healer_from_row(row)
    if not definition.enabled:
        raise VerifierAuthorityError(f"healer_registry row {healer_ref} is disabled")
    return definition


def _bound_healer_refs(
    verifier_ref: str,
    *,
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, ...]:
    return _verification_repository(conn).list_bound_healer_refs(
        verifier_ref=verifier_ref,
    )


def _first_bound_healer_ref(
    verifier_ref: str,
    *,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    rows = _verification_repository(conn).list_bound_healer_refs(
        verifier_ref=verifier_ref,
        limit=1,
    )
    return rows[0] if rows else None


def registry_snapshot(
    conn: "SyncPostgresConnection | None" = None,
) -> dict[str, Any]:
    db = _connection(conn)
    verifiers = [item.to_json() for item in list_registered_verifiers(db)]
    healers = [item.to_json() for item in list_registered_healers(db)]
    bindings = [item.to_json() for item in list_verifier_healer_bindings(db)]
    return {
        "verifiers": verifiers,
        "healers": healers,
        "bindings": bindings,
    }


def _record_verification_run(
    *,
    verifier: VerifierDefinition,
    target_kind: str,
    target_ref: str,
    status: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    suggested_healer_ref: str | None,
    healing_candidate: bool,
    duration_ms: int,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    db = _optional_connection(conn)
    if db is None:
        return None
    verification_run_id = f"verification_run:{uuid.uuid4().hex}"
    _verification_repository(db).record_verification_run(
        verification_run_id=verification_run_id,
        verifier_ref=verifier.verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        inputs=inputs,
        outputs=outputs,
        suggested_healer_ref=suggested_healer_ref,
        healing_candidate=healing_candidate,
        decision_ref=verifier.decision_ref,
        duration_ms=max(duration_ms, 0),
    )
    return verification_run_id


def _record_healing_run(
    *,
    healer: HealerDefinition,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    status: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    duration_ms: int,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    db = _optional_connection(conn)
    if db is None:
        return None
    healing_run_id = f"healing_run:{uuid.uuid4().hex}"
    _verification_repository(db).record_healing_run(
        healing_run_id=healing_run_id,
        healer_ref=healer.healer_ref,
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        inputs=inputs,
        outputs=outputs,
        decision_ref=healer.decision_ref,
        duration_ms=max(duration_ms, 0),
    )
    return healing_run_id


def _normalized_target_ref(
    *,
    target_kind: str,
    target_ref: str,
    fallback_ref: str,
    inputs: dict[str, Any] | None = None,
) -> str:
    normalized_target_ref = str(target_ref or "").strip()
    if normalized_target_ref:
        return normalized_target_ref
    if target_kind == "path":
        for key in ("path", "file", "target", "module"):
            candidate = inputs.get(key) if isinstance(inputs, dict) else None
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return str(fallback_ref or "").strip()


def _annotate_control_plane_outputs(
    *,
    kind: str,
    primary_ref: str,
    target_kind: str,
    target_ref: str,
    status: str,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    return _verifier_bug_bridge.annotate_control_plane_outputs(
        kind=kind,
        primary_ref=primary_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        outputs=outputs,
    )


def _error_outputs(exc: BaseException) -> dict[str, Any]:
    return {
        "error": str(exc),
        "exception_type": type(exc).__name__,
    }


def _recent_verification_failure_count(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
) -> int:
    return _verifier_bug_bridge._recent_verification_failure_count(
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        fingerprint=fingerprint,
        conn=conn,
    )


def _recent_healing_failure_count(
    *,
    healer_ref: str,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
) -> int:
    return _verifier_bug_bridge._recent_healing_failure_count(
        healer_ref=healer_ref,
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        fingerprint=fingerprint,
        conn=conn,
    )


def _load_open_bug_by_fingerprint(
    *,
    fingerprint: str,
    conn: "SyncPostgresConnection | None" = None,
):
    return _verifier_bug_bridge._load_open_bug_by_fingerprint(
        fingerprint=fingerprint,
        conn=conn,
    )


def _link_bug_evidence(
    *,
    bug_id: str,
    evidence_kind: str,
    evidence_ref: str,
    evidence_role: str,
    notes: str,
    conn: "SyncPostgresConnection | None" = None,
) -> None:
    _verifier_bug_bridge._link_bug_evidence(
        bug_id=bug_id,
        evidence_kind=evidence_kind,
        evidence_ref=evidence_ref,
        evidence_role=evidence_role,
        notes=notes,
        conn=conn,
    )


def _file_control_plane_bug(
    *,
    kind: str,
    primary_ref: str,
    primary_display_name: str,
    status: str,
    target_kind: str,
    target_ref: str,
    fingerprint: str,
    recent_failures: int,
    outputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
):
    return _verifier_bug_bridge._file_control_plane_bug(
        kind=kind,
        primary_ref=primary_ref,
        primary_display_name=primary_display_name,
        status=status,
        target_kind=target_kind,
        target_ref=target_ref,
        fingerprint=fingerprint,
        recent_failures=recent_failures,
        outputs=outputs,
        conn=conn,
    )


def _latest_failed_verification_fingerprint(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    return _verifier_bug_bridge._latest_failed_verification_fingerprint(
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        conn=conn,
    )


def _maybe_promote_verifier_bug(
    *,
    verifier: VerifierDefinition,
    target_kind: str,
    target_ref: str,
    status: str,
    outputs: dict[str, Any],
    verification_run_id: str | None,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    if not verification_run_id or status == "passed":
        return None
    fingerprint = str(outputs.get("control_plane_bug_fingerprint") or "").strip()
    if not fingerprint:
        return None
    bug = _load_open_bug_by_fingerprint(fingerprint=fingerprint, conn=conn)
    recent_failures = _recent_verification_failure_count(
        verifier_ref=verifier.verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        fingerprint=fingerprint,
        conn=conn,
    )
    if bug is None and status != "error" and recent_failures < _CONTROL_PLANE_AUTO_BUG_THRESHOLD:
        return None
    if bug is None:
        bug = _file_control_plane_bug(
            kind="verification",
            primary_ref=verifier.verifier_ref,
            primary_display_name=verifier.display_name,
            status=status,
            target_kind=target_kind,
            target_ref=target_ref,
            fingerprint=fingerprint,
            recent_failures=recent_failures,
            outputs=outputs,
            conn=conn,
        )
    if bug is None:
        return None
    _link_bug_evidence(
        bug_id=bug.bug_id,
        evidence_kind="verification_run",
        evidence_ref=verification_run_id,
        evidence_role=EVIDENCE_ROLE_OBSERVED_IN,
        notes=f"Verifier {verifier.verifier_ref} reported {status}.",
        conn=conn,
    )
    return bug.bug_id


def _maybe_promote_healer_bug(
    *,
    healer: HealerDefinition,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    status: str,
    outputs: dict[str, Any],
    healing_run_id: str | None,
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    if not healing_run_id or status == "succeeded":
        return None
    fingerprint = str(outputs.get("control_plane_bug_fingerprint") or "").strip()
    if not fingerprint:
        return None
    bug = _load_open_bug_by_fingerprint(fingerprint=fingerprint, conn=conn)
    recent_failures = _recent_healing_failure_count(
        healer_ref=healer.healer_ref,
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        fingerprint=fingerprint,
        conn=conn,
    )
    if bug is None and status != "error" and recent_failures < _CONTROL_PLANE_AUTO_BUG_THRESHOLD:
        return None
    if bug is None:
        bug = _file_control_plane_bug(
            kind="healing",
            primary_ref=healer.healer_ref,
            primary_display_name=healer.display_name,
            status=status,
            target_kind=target_kind,
            target_ref=target_ref,
            fingerprint=fingerprint,
            recent_failures=recent_failures,
            outputs=outputs,
            conn=conn,
        )
    if bug is None:
        return None
    _link_bug_evidence(
        bug_id=bug.bug_id,
        evidence_kind="healing_run",
        evidence_ref=healing_run_id,
        evidence_role=EVIDENCE_ROLE_OBSERVED_IN,
        notes=f"Healer {healer.healer_ref} reported {status}.",
        conn=conn,
    )
    return bug.bug_id


def _maybe_resolve_verifier_bug(
    *,
    verifier_ref: str,
    target_kind: str,
    target_ref: str,
    healing_run_id: str | None,
    post_verification: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> str | None:
    if not healing_run_id or str(post_verification.get("status") or "") != "passed":
        return None
    fingerprint = _latest_failed_verification_fingerprint(
        verifier_ref=verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        conn=conn,
    )
    if not fingerprint:
        return None
    bug = _load_open_bug_by_fingerprint(fingerprint=fingerprint, conn=conn)
    if bug is None:
        return None
    _link_bug_evidence(
        bug_id=bug.bug_id,
        evidence_kind="healing_run",
        evidence_ref=healing_run_id,
        evidence_role=EVIDENCE_ROLE_ATTEMPTED_FIX,
        notes=f"Healer run repaired verifier target {target_kind}:{target_ref or 'global'}.",
        conn=conn,
    )
    verification_run_id = str(post_verification.get("verification_run_id") or "").strip()
    if verification_run_id:
        _link_bug_evidence(
            bug_id=bug.bug_id,
            evidence_kind="verification_run",
            evidence_ref=verification_run_id,
            evidence_role=EVIDENCE_ROLE_VALIDATES_FIX,
            notes=f"Verifier {verifier_ref} passed after healing.",
            conn=conn,
        )
    db = _optional_connection(conn)
    if db is None:
        return None
    from runtime.bug_tracker import BugStatus, BugTracker

    tracker = BugTracker(db)
    resolved = tracker.resolve(bug.bug_id, BugStatus.FIXED)
    return resolved.bug_id if resolved is not None else bug.bug_id


def _builtin_verify_schema_authority(*, inputs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_verify_schema_authority(inputs=inputs)


def _builtin_verify_receipt_provenance(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_verify_receipt_provenance(
        inputs=inputs,
        conn=conn,
        connection_fn=_connection,
    )


def _builtin_verify_memory_proof_links(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_verify_memory_proof_links(
        inputs=inputs,
        conn=conn,
        connection_fn=_connection,
    )


def _run_builtin_verifier(
    builtin_ref: str,
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    try:
        return _verifier_builtins.run_builtin_verifier(
            builtin_ref,
            inputs=inputs,
            conn=conn,
            connection_fn=_connection,
        )
    except _verifier_builtins.VerifierBuiltinsError as exc:
        raise VerifierAuthorityError(str(exc)) from exc


def run_registered_verifier(
    verifier_ref: str,
    *,
    inputs: dict[str, Any] | None = None,
    target_kind: str = "platform",
    target_ref: str = "",
    conn: "SyncPostgresConnection | None" = None,
    record_run: bool = True,
    promote_bug: bool = True,
) -> dict[str, Any]:
    verifier = _load_verifier(verifier_ref, conn=conn)
    merged_inputs = dict(verifier.default_inputs)
    merged_inputs.update(inputs or {})
    target_ref = _normalized_target_ref(
        target_kind=target_kind,
        target_ref=target_ref,
        fallback_ref=verifier.verifier_ref,
        inputs=merged_inputs,
    )
    started_ns = time.monotonic_ns()
    status = "error"
    outputs: dict[str, Any] = {}
    suggested_healer_ref = None
    try:
        if verifier.verifier_kind == "verification_ref":
            from runtime.verification import (
                VerificationBinding,
                resolve_verification_bindings,
                run_verify,
                summarize_verification,
            )

            db = _connection(conn)
            workdir = str(merged_inputs.get("workdir") or "").strip() or None
            commands = resolve_verification_bindings(
                db,
                [
                    VerificationBinding(
                        verification_ref=verifier.verification_ref or "",
                        inputs=merged_inputs,
                        label=verifier.display_name,
                    )
                ],
            )
            results = run_verify(commands, workdir=workdir)
            summary = summarize_verification(results)
            status = "passed" if summary.all_passed else "failed"
            outputs = {
                "verification_ref": verifier.verification_ref,
                "verification": summary.to_json(),
            }
        else:
            status, outputs = _run_builtin_verifier(
                verifier.builtin_ref or "",
                inputs=merged_inputs,
                conn=conn,
            )
    except Exception as exc:
        status = "error"
        outputs = _error_outputs(exc)

    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    if status != "passed":
        try:
            suggested_healer_ref = _first_bound_healer_ref(verifier_ref, conn=conn)
        except Exception:
            suggested_healer_ref = None
    outputs = _annotate_control_plane_outputs(
        kind="verification",
        primary_ref=verifier.verifier_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        outputs=outputs,
    )
    verification_run_id = (
        _record_verification_run(
            verifier=verifier,
            target_kind=target_kind,
            target_ref=target_ref,
            status=status,
            inputs=merged_inputs,
            outputs=outputs,
            suggested_healer_ref=suggested_healer_ref,
            healing_candidate=bool(suggested_healer_ref),
            duration_ms=duration_ms,
            conn=conn,
        )
        if record_run
        else None
    )
    bug_id = (
        _maybe_promote_verifier_bug(
            verifier=verifier,
            target_kind=target_kind,
            target_ref=target_ref,
            status=status,
            outputs=outputs,
            verification_run_id=verification_run_id,
            conn=conn,
        )
        if record_run and promote_bug and verification_run_id
        else None
    )
    return {
        "verification_run_id": verification_run_id,
        "verifier": verifier.to_json(),
        "status": status,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "inputs": merged_inputs,
        "outputs": outputs,
        "duration_ms": duration_ms,
        "suggested_healer_ref": suggested_healer_ref,
        "bug_id": bug_id,
    }


def _builtin_heal_schema_bootstrap(*, inputs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_heal_schema_bootstrap(inputs=inputs)


def _builtin_heal_receipt_provenance_backfill(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_heal_receipt_provenance_backfill(
        inputs=inputs,
        conn=conn,
        connection_fn=_connection,
    )


def _builtin_heal_proof_backfill(
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    return _verifier_builtins.builtin_heal_proof_backfill(
        inputs=inputs,
        conn=conn,
        connection_fn=_connection,
    )


def _run_builtin_healer(
    action_ref: str,
    *,
    inputs: dict[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[str, dict[str, Any]]:
    try:
        return _verifier_builtins.run_builtin_healer(
            action_ref,
            inputs=inputs,
            conn=conn,
            connection_fn=_connection,
        )
    except _verifier_builtins.VerifierBuiltinsError as exc:
        raise VerifierAuthorityError(str(exc)) from exc


def run_registered_healer(
    *,
    healer_ref: str | None = None,
    verifier_ref: str,
    inputs: dict[str, Any] | None = None,
    target_kind: str = "platform",
    target_ref: str = "",
    conn: "SyncPostgresConnection | None" = None,
    record_run: bool = True,
) -> dict[str, Any]:
    if healer_ref is None:
        bound_healers = _bound_healer_refs(verifier_ref, conn=conn)
        if not bound_healers:
            raise VerifierAuthorityError(f"no bound healer for verifier {verifier_ref}")
        if len(bound_healers) > 1:
            raise VerifierAuthorityError(
                f"multiple bound healers for verifier {verifier_ref}; specify healer_ref explicitly",
            )
        healer_ref = bound_healers[0]

    healer = _load_healer(healer_ref, conn=conn)
    merged_inputs = dict(inputs or {})
    target_ref = _normalized_target_ref(
        target_kind=target_kind,
        target_ref=target_ref,
        fallback_ref=verifier_ref,
        inputs=merged_inputs,
    )
    started_ns = time.monotonic_ns()
    action_status = "skipped"
    action_outputs: dict[str, Any] = {}
    post_verification: dict[str, Any] = {}
    status = "error"
    try:
        action_status, action_outputs = _run_builtin_healer(
            healer.action_ref,
            inputs=merged_inputs,
            conn=conn,
        )
        post_verification = run_registered_verifier(
            verifier_ref,
            inputs=merged_inputs,
            target_kind=target_kind,
            target_ref=target_ref,
            conn=conn,
            record_run=record_run,
        )
        status = (
            "succeeded"
            if action_status == "succeeded" and post_verification.get("status") == "passed"
            else "failed"
        )
    except Exception as exc:
        status = "error"
        action_outputs = {
            **action_outputs,
            **_error_outputs(exc),
        }
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    outputs = {
        "action_status": action_status,
        "action_outputs": action_outputs,
        "post_verification": post_verification,
    }
    outputs = _annotate_control_plane_outputs(
        kind="healing",
        primary_ref=healer.healer_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        status=status,
        outputs=outputs,
    )
    healing_run_id = (
        _record_healing_run(
            healer=healer,
            verifier_ref=verifier_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            status=status,
            inputs=merged_inputs,
            outputs=outputs,
            duration_ms=duration_ms,
            conn=conn,
        )
        if record_run
        else None
    )
    bug_id = (
        _maybe_promote_healer_bug(
            healer=healer,
            verifier_ref=verifier_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            status=status,
            outputs=outputs,
            healing_run_id=healing_run_id,
            conn=conn,
        )
        if record_run and healing_run_id
        else None
    )
    resolved_bug_id = (
        _maybe_resolve_verifier_bug(
            verifier_ref=verifier_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            healing_run_id=healing_run_id,
            post_verification=post_verification,
            conn=conn,
        )
        if record_run and healing_run_id and status == "succeeded"
        else None
    )
    return {
        "healing_run_id": healing_run_id,
        "healer": healer.to_json(),
        "verifier_ref": verifier_ref,
        "status": status,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "inputs": merged_inputs,
        "outputs": outputs,
        "duration_ms": duration_ms,
        "bug_id": bug_id,
        "resolved_bug_id": resolved_bug_id,
    }
