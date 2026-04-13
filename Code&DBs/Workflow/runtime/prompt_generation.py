"""Deterministic prompt assembly from structured authoring fields.

Generates the ``prompt`` field that the runtime spec requires, from the
simplified authoring surface: outcome_goal + task_type + contracts.

This is pure string assembly — no LLM calls.
"""

from __future__ import annotations

from typing import Any


def generate_job_prompt(
    *,
    outcome_goal: str,
    task_type: str,
    authoring_contract: dict[str, Any] | None = None,
    acceptance_contract: dict[str, Any] | None = None,
    anti_requirements: list[str] | None = None,
    scope_read: list[str] | None = None,
    scope_write: list[str] | None = None,
    verify_refs: list[str] | None = None,
    system_prompt_hint: str = "",
) -> str:
    """Build a prompt string from structured authoring fields.

    Sections are omitted when their input is empty, so the generated
    prompt stays clean regardless of how many optional fields are set.
    """
    sections: list[str] = []

    # -- Objective (always present) --
    sections.append(f"## Objective\n{outcome_goal.strip()}")

    # -- Scope --
    scope_lines: list[str] = []
    if scope_read:
        scope_lines.append(f"Read: {', '.join(scope_read)}")
    if scope_write:
        scope_lines.append(f"Write: {', '.join(scope_write)}")
    if scope_lines:
        sections.append("## Scope\n" + "\n".join(scope_lines))

    # -- Requirements (from authoring_contract) --
    if authoring_contract:
        req_lines = _render_authoring_requirements(authoring_contract)
        if req_lines:
            sections.append("## Requirements\n" + req_lines)

    # -- Constraints (anti_requirements) --
    if anti_requirements:
        bullets = "\n".join(f"- {r.strip()}" for r in anti_requirements if r.strip())
        if bullets:
            sections.append("## Constraints\n" + bullets)

    # -- Acceptance Criteria (from acceptance_contract) --
    if acceptance_contract:
        criteria = _render_acceptance_criteria(acceptance_contract)
        if criteria:
            sections.append("## Acceptance Criteria\n" + criteria)

    # -- Verification --
    if verify_refs:
        bullets = "\n".join(f"- `{v.strip()}`" for v in verify_refs if v.strip())
        if bullets:
            sections.append("## Verification\n" + bullets)

    return "\n\n".join(sections)


def _render_authoring_requirements(contract: dict[str, Any]) -> str:
    """Render authoring_contract fields into a requirements section."""
    parts: list[str] = []

    artifact_kind = str(contract.get("artifact_kind") or "").strip()
    if artifact_kind:
        parts.append(f"Produce: {artifact_kind}")

    required_sections = _str_list(contract.get("required_sections"))
    if required_sections:
        parts.append("Required sections: " + ", ".join(required_sections))

    required_fields = _str_list(contract.get("required_fields"))
    if required_fields:
        parts.append("Required fields: " + ", ".join(required_fields))

    submission_format = str(contract.get("submission_format") or "").strip()
    if submission_format:
        parts.append(f"Format: {submission_format}")

    notes = _str_list(contract.get("notes"))
    for note in notes:
        parts.append(f"- {note}")

    return "\n".join(parts)


def _render_acceptance_criteria(contract: dict[str, Any]) -> str:
    """Render acceptance_contract fields into a criteria section."""
    parts: list[str] = []

    structural = contract.get("structural")
    if isinstance(structural, dict):
        req_sections = _str_list(structural.get("required_sections"))
        if req_sections:
            parts.append("Must include sections: " + ", ".join(req_sections))
        req_fields = _str_list(structural.get("required_fields"))
        if req_fields:
            parts.append("Must include fields: " + ", ".join(req_fields))

    verify_refs = _str_list(contract.get("verify_refs"))
    if verify_refs:
        for ref in verify_refs:
            parts.append(f"- Pass: `{ref}`")

    review = contract.get("review")
    if isinstance(review, dict):
        criteria = _str_list(review.get("criteria"))
        for criterion in criteria:
            parts.append(f"- {criterion}")

    return "\n".join(parts)


def _str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if isinstance(item, str) and str(item).strip()]
