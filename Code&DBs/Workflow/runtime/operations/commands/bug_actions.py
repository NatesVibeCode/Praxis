"""Gateway-dispatched command wrappers for bug-tracker mutation actions.

Brings the four bug-tracker write paths in line with the dogfooding principle
(`project_dogfooding_principle.md`) — every internal command should route
through `operation_catalog_gateway.execute_operation_*` so it gets an
authority_operation_receipts row + emits an authority_event.

These wrappers preserve the existing business logic in
`surfaces/api/handlers/_bug_surface_contract.py`. The MCP tool
`praxis_bugs` should be refactored to dispatch through the gateway for
these four actions (file / resolve / attach_evidence / patch_resume) once
all four are registered.

Reads (list / search / stats / duplicate_check / packet / history) stay
direct-call — receipts/events are not value-add for queries.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from runtime.repo_policy_onboarding import consume_operator_disclosure
from runtime.workspace_paths import repo_root as workspace_repo_root


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic command models
# ─────────────────────────────────────────────────────────────────────────────


class BugFileCommand(BaseModel):
    """Input for `bug.file` — create a new bug row."""

    title: str = Field(..., description="Human-readable bug title (required).")
    description: str | None = Field(default=None, description="Optional longer-form description.")
    severity: str | None = Field(default=None, description="P0..P3 severity. Defaults to P2.")
    category: str | None = Field(default=None, description="BugCategory enum value. Defaults to OTHER.")
    filed_by: str | None = Field(default=None, description="Identifier of the filer.")
    source_kind: str | None = Field(default=None, description="Where the bug was filed from.")
    decision_ref: str | None = Field(default=None, description="Decision ref linking the filing to authority.")
    discovered_in_run_id: str | None = Field(default=None)
    discovered_in_receipt_id: str | None = Field(default=None)
    owner_ref: str | None = Field(default=None, description="Optional owner reference for the bug.")
    source_issue_id: str | None = Field(
        default=None,
        description=(
            "Optional linked issue id. This must reference an existing issues row; "
            "do not use it as a free-form dedupe key."
        ),
    )
    tags: tuple[str, ...] | None = Field(default=None, description="Optional bug tags.")
    resume_context: dict[str, Any] | None = Field(default=None, description="Resume context JSON object.")
    dry_run: bool = Field(default=False, description="Preview without persisting.")
    preview: bool = Field(default=False, description="Alias for dry_run.")
    include_similar_bugs: bool = Field(default=False, description="Surface similar-bug matches.")
    allow_duplicate: bool = Field(
        default=False,
        description=(
            "When false, filing is blocked if strong duplicate candidates are found. "
            "Set true only when the new bug is intentionally distinct."
        ),
    )


class BugResolveCommand(BaseModel):
    """Input for `bug.resolve` — flip status to FIXED / WONT_FIX / DEFERRED / FIX_PENDING_VERIFICATION."""

    bug_id: str = Field(..., description="Bug to resolve (required).")
    status: str = Field(..., description="Target status. FIXED requires verifier_ref or pending intermediate state.")
    verifier_ref: str | None = Field(default=None, description="Verifier registry ref required for FIXED status.")
    inputs: dict[str, Any] | None = Field(default=None, description="Verifier inputs payload (path, etc.).")
    target_kind: str | None = Field(default=None)
    target_ref: str | None = Field(default=None)
    resolution_summary: str | None = Field(default=None, description="Operator note on the resolution.")
    notes: str | None = Field(default=None, description="Alias for resolution_summary.")
    created_by: str | None = Field(default=None)
    promote_to_pattern: bool = Field(
        default=False,
        description=(
            "When true, explicitly materialize the resolved bug into platform pattern/"
            "anti-pattern authority after the bug resolution succeeds."
        ),
    )
    pattern_status: str = Field(
        default="confirmed",
        description="Pattern status to use when promote_to_pattern=true.",
    )


class BugAttachEvidenceCommand(BaseModel):
    """Input for `bug.attach_evidence` — link evidence (verification run, receipt, etc.) to a bug."""

    bug_id: str = Field(..., description="Bug to attach evidence to (required).")
    evidence_kind: str = Field(..., description="Evidence kind (e.g. 'verification_run', 'receipt') (required).")
    evidence_ref: str = Field(..., description="Pointer to the evidence row (required).")
    evidence_role: str = Field(default="observed_in", description="Role: observed_in / validates_fix / etc.")
    created_by: str | None = Field(default=None)
    notes: str | None = Field(default=None)


class BugPatchResumeCommand(BaseModel):
    """Input for `bug.patch_resume` — merge a JSON patch into resume_context."""

    bug_id: str = Field(..., description="Bug whose resume_context is being patched (required).")
    resume_patch: dict[str, Any] = Field(..., description="JSON object merged into resume_context (required).")


# ─────────────────────────────────────────────────────────────────────────────
# Handlers — gateway-friendly seam wrapping the existing _bug_surface_contract
# payload helpers. Business logic stays in surfaces/api/handlers; these only
# adapt subsystems → (bt, bt_mod) and convert ValueError into structured
# response shapes so the gateway records the failure cleanly.
# ─────────────────────────────────────────────────────────────────────────────


def _bug_subsystems(subsystems: Any) -> tuple[Any, Any]:
    return subsystems.get_bug_tracker(), subsystems.get_bug_tracker_mod()


def _serialize_bug(bug: Any) -> dict[str, Any]:
    """Serializer matching the public bug surface shape."""
    from surfaces.api.handlers._shared import _bug_to_dict

    return _bug_to_dict(bug) if bug is not None else {}


def _serialize_evidence_link(link: Any) -> dict[str, Any]:
    if link is None:
        return {}
    if isinstance(link, dict):
        return dict(link)
    return {
        "bug_evidence_link_id": getattr(link, "bug_evidence_link_id", None),
        "bug_id": getattr(link, "bug_id", None),
        "evidence_kind": getattr(link, "evidence_kind", None),
        "evidence_ref": getattr(link, "evidence_ref", None),
        "evidence_role": getattr(link, "evidence_role", None),
        "created_by": getattr(link, "created_by", None),
        "created_at": str(getattr(link, "created_at", "") or ""),
    }


def handle_bug_file(command: BugFileCommand, subsystems: Any) -> dict[str, Any]:
    """Dispatch bug-file through the gateway. Emits `bug.filed` on receipt."""
    from surfaces.api.handlers._bug_surface_contract import file_bug_payload

    bt, bt_mod = _bug_subsystems(subsystems)
    body = command.model_dump(exclude_none=False)
    try:
        payload = file_bug_payload(
            bt=bt,
            bt_mod=bt_mod,
            body=body,
            serialize_bug=_serialize_bug,
            filed_by_default="gateway.bug_file",
            source_kind_default="gateway.bug_file",
            include_similar_bugs=bool(command.include_similar_bugs),
        )
        if payload.get("ok") and payload.get("filed"):
            disclosure = consume_operator_disclosure(
                subsystems.get_pg_conn(),
                repo_root=str(workspace_repo_root()),
                disclosure_kind="bug",
            )
            if disclosure is not None:
                payload["operator_disclosure"] = disclosure
        return payload
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "reason_code": "bug.file.invalid"}


def handle_bug_resolve(command: BugResolveCommand, subsystems: Any) -> dict[str, Any]:
    """Dispatch bug-resolve through the gateway. Emits `bug.resolved` on receipt."""
    from surfaces.api.handlers._bug_surface_contract import resolve_bug_payload

    bt, bt_mod = _bug_subsystems(subsystems)
    body = command.model_dump(exclude_none=False)
    resolved_statuses = {
        bt_mod.BugStatus.FIXED,
        bt_mod.BugStatus.WONT_FIX,
        bt_mod.BugStatus.DEFERRED,
    }
    try:
        payload = resolve_bug_payload(
            bt=bt,
            bt_mod=bt_mod,
            body=body,
            serialize_bug=_serialize_bug,
            resolved_statuses=resolved_statuses,
        )
        if payload.get("ok") and payload.get("resolved"):
            if command.promote_to_pattern:
                try:
                    from runtime.platform_patterns import PlatformPatternAuthority

                    pattern_payload = PlatformPatternAuthority(
                        subsystems.get_pg_conn()
                    ).materialize_bug_resolution(
                        bug=dict(payload.get("bug") or {}),
                        status=command.pattern_status,
                        created_by=str(command.created_by or "gateway.bug_resolve"),
                    )
                    payload["pattern_promotion"] = pattern_payload
                    pattern_disclosure = consume_operator_disclosure(
                        subsystems.get_pg_conn(),
                        repo_root=str(workspace_repo_root()),
                        disclosure_kind="pattern",
                    )
                    if pattern_disclosure is not None:
                        payload["pattern_operator_disclosure"] = pattern_disclosure
                except Exception as exc:  # noqa: BLE001 - bug resolution already succeeded
                    payload["pattern_promotion"] = {
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                        "reason_code": "bug.resolve.pattern_promotion_failed",
                    }
            disclosure = consume_operator_disclosure(
                subsystems.get_pg_conn(),
                repo_root=str(workspace_repo_root()),
                disclosure_kind="bug",
            )
            if disclosure is not None:
                payload["operator_disclosure"] = disclosure
        return payload
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "reason_code": "bug.resolve.invalid"}


def handle_bug_attach_evidence(
    command: BugAttachEvidenceCommand, subsystems: Any
) -> dict[str, Any]:
    """Dispatch bug-attach-evidence through the gateway. Emits `bug.evidence_attached` on receipt."""
    from surfaces.api.handlers._bug_surface_contract import attach_evidence_payload

    bt, _bt_mod = _bug_subsystems(subsystems)
    body = command.model_dump(exclude_none=False)
    try:
        return attach_evidence_payload(
            bt=bt,
            body=body,
            serialize=_serialize_evidence_link,
            created_by_default="gateway.bug_attach_evidence",
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "reason_code": "bug.attach_evidence.invalid"}


def handle_bug_patch_resume(
    command: BugPatchResumeCommand, subsystems: Any
) -> dict[str, Any]:
    """Dispatch bug-patch-resume through the gateway. Emits `bug.resume_context_patched` on receipt."""
    from surfaces.api.handlers._bug_surface_contract import patch_resume_payload

    bt, _bt_mod = _bug_subsystems(subsystems)
    body = command.model_dump(exclude_none=False)
    try:
        return patch_resume_payload(
            bt=bt,
            body=body,
            serialize_bug=_serialize_bug,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "reason_code": "bug.patch_resume.invalid"}
