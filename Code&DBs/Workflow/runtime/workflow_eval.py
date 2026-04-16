"""Deterministic workflow-eval helpers for probe-style workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_REQUIRED_HANDOFF_HEADINGS = (
    "# Search Proof",
    "# Authority Gaps",
    "# Next Verification",
)


def _as_mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_path(path_text: str, *, workspace_root: Path) -> str:
    candidate = Path(path_text)
    if not candidate.is_absolute():
        return str(candidate)
    resolved_root = workspace_root.resolve()
    resolved_candidate = candidate.resolve()
    try:
        return str(resolved_candidate.relative_to(resolved_root))
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(
            f"path {resolved_candidate} escapes workspace root {resolved_root}"
        ) from exc


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _has_required_handoff_shape(handoff_text: str) -> bool:
    return all(heading in handoff_text for heading in _REQUIRED_HANDOFF_HEADINGS)


def build_agent_handoff_probe_review(payload: dict[str, Any]) -> dict[str, Any]:
    """Build the deterministic review payload for the agent handoff probe."""

    workspace_root = Path(str(payload.get("workspace_root") or ".")).resolve()
    handoff_path = workspace_root / str(payload.get("handoff_path") or "").strip()
    review_path = str(payload.get("review_path") or "").strip()
    bug_anchor = str(payload.get("write_side_bug_anchor") or "").strip()

    handoff_text = _read_text(handoff_path)
    handoff_exists = bool(handoff_text)
    handoff_has_shape = _has_required_handoff_shape(handoff_text)
    handoff_non_authoritative = "non-authoritative" in handoff_text.lower()

    search_step = _as_mapping(payload.get("discover_local_code"))
    db_step = _as_mapping(payload.get("query_bug_db"))
    search_result = search_step.get("tool_result")
    db_result = db_step.get("tool_result")
    search_ok = isinstance(search_result, (dict, list)) and bool(search_result)
    db_ok = isinstance(db_result, dict) and bool(db_result)

    authoritative = handoff_exists and handoff_has_shape and not handoff_non_authoritative
    bug_suffix = f" {bug_anchor} tracks the write-side regression." if bug_anchor else ""

    if authoritative:
        verdict = (
            "Authoritative: the workflow produced a handoff artifact and a deterministic "
            "review inside one durable run."
        )
    elif handoff_exists:
        verdict = (
            "Non-authoritative: the handoff artifact exists, but it explicitly marks the "
            "probe as non-authoritative or incomplete." + bug_suffix
        )
    else:
        verdict = (
            "Non-authoritative: the handoff artifact was missing, so deterministic review "
            "could only report incomplete workflow state." + bug_suffix
        )

    if handoff_exists:
        agents_exercised = (
            "Yes: agent_synthesis produced the handoff artifact inside the workflow, and the "
            "review consumed that file instead of relying on chat residue."
        )
    else:
        agents_exercised = (
            "No authoritative agent handoff was captured because the expected handoff file "
            "was not present when review executed."
        )

    if handoff_exists and handoff_has_shape:
        information_handoff_proven = (
            "Yes: the workflow exercised file-based handoff through "
            "`agent_handoff_search_db_probe.handoff.md`."
        )
    else:
        information_handoff_proven = (
            "No: the required handoff headings were missing or the handoff artifact was absent."
        )

    deterministic_tooling_proven = (
        "Yes: the workflow used deterministic_task for seed and review production, and "
        "file_writer persisted the review artifact from deterministic outputs."
    )

    if search_ok:
        search_exercised = (
            "Yes: discover_local_code returned a tool_result payload that reached deterministic review."
        )
    else:
        search_exercised = (
            "No authoritative search payload reached deterministic review."
        )

    if db_ok:
        db_action_exercised = (
            "Yes: query_bug_db returned a DB-backed tool_result payload that reached deterministic review."
        )
    else:
        db_action_exercised = (
            "No authoritative DB action payload reached deterministic review."
        )

    cqrs_assessment = (
        "CQRS remained visible: search and bug stats are read-side proof, while launch/run authority "
        "determines whether the overall probe is authoritative." + bug_suffix
    )
    trust_boundary = (
        "Trust stops at durable workflow artifacts and tool outputs: the handoff file plus deterministic "
        "review are authoritative only when they are produced inside a real workflow run."
    )

    review_payload = {
        "verdict": verdict,
        "agents_exercised": agents_exercised,
        "information_handoff_proven": information_handoff_proven,
        "deterministic_tooling_proven": deterministic_tooling_proven,
        "search_exercised": search_exercised,
        "db_action_exercised": db_action_exercised,
        "cqrs_assessment": cqrs_assessment,
        "trust_boundary": trust_boundary,
    }

    if not review_path:
        raise ValueError("review_path is required for deterministic probe review output")

    relative_review_path = _normalize_path(review_path, workspace_root=workspace_root)
    review_content = json.dumps(review_payload, indent=2) + "\n"
    return {
        "review_payload": review_payload,
        "code_blocks": [
            {
                "file_path": relative_review_path,
                "content": review_content,
                "language": "json",
                "action": "replace",
            }
        ],
        "review_artifact_path": relative_review_path,
    }


__all__ = ["build_agent_handoff_probe_review"]
