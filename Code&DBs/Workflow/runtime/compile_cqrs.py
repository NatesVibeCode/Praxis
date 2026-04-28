"""CQRS front door for compile.

Queries recognize and preview operator intent without mutating state.
Commands create or update workflow build state through the canonical workflow
runtime so MCP, CLI, and API do not grow separate compile semantics.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any

from runtime.intent_recognition import recognize_intent


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _title_from_intent(intent: str) -> str:
    clean = " ".join(intent.strip().split())
    if not clean:
        return "Compiled workflow"
    return clean[:64].rstrip(" .,;:") or "Compiled workflow"


def _workflow_id_from_intent() -> str:
    return f"wf_compile_{uuid.uuid4().hex[:12]}"


def _input_fingerprint(intent: str) -> str:
    return hashlib.sha256(intent.strip().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CompilePreview:
    intent: str
    recognition: dict[str, Any]
    input_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        spans = list(self.recognition.get("spans") or [])
        suggested_steps = list(self.recognition.get("suggested_steps") or [])
        gaps = list(self.recognition.get("gaps") or [])
        matches = list(self.recognition.get("matches") or [])
        enough_structure = bool(spans) and (bool(suggested_steps) or not gaps)
        next_actions: list[dict[str, str]] = []
        if gaps:
            next_actions.append(
                {
                    "action": "confirm_scope",
                    "reason": "recognized gaps should be confirmed before treating the plan as complete",
                }
            )
        next_actions.append(
            {
                "action": "materialize_workflow",
                "reason": "create or update a draft workflow through the command side",
            }
        )
        return {
            "kind": "compile_preview",
            "cqrs_role": "query",
            "ok": True,
            "intent": self.intent,
            "input_fingerprint": self.input_fingerprint,
            "recognition": self.recognition,
            "scope_packet": {
                "spans": spans,
                "matches": matches,
                "suggested_steps": suggested_steps,
                "gaps": gaps,
            },
            "enough_structure": enough_structure,
            "next_actions": next_actions,
        }


def preview_compile(
    intent: str,
    *,
    conn: Any,
    match_limit: int = 5,
) -> CompilePreview:
    clean_intent = _text(intent)
    if not clean_intent:
        raise ValueError("intent must be a non-empty string")
    recognition = recognize_intent(clean_intent, conn=conn, match_limit=match_limit).to_dict()
    return CompilePreview(
        intent=clean_intent,
        recognition=recognition,
        input_fingerprint=_input_fingerprint(clean_intent),
    )


def materialize_workflow(
    intent: str,
    *,
    conn: Any,
    workflow_id: str | None = None,
    title: str | None = None,
    enable_llm: bool | None = None,
    enable_full_compose: bool | None = None,
    match_limit: int = 5,
) -> dict[str, Any]:
    """Command side: create/update workflow build state from compile intent.

    `enable_full_compose` selects which compile pipeline runs:
      - True (or omitted): full fork-out compose (compose_plan_via_llm with
        plan_synthesis + plan_fork_author task types).
      - False: the compile_prose chain (compile_synthesize → compile_pill_match
        → compile_author → compile_finalize). Use when the operator wants the
        sub-task-routed compile path (e.g., to exercise compile_finalize voting
        for binding-gate auto-resolution).
    """

    clean_intent = _text(intent)
    if not clean_intent:
        raise ValueError("intent must be a non-empty string")
    preview = preview_compile(clean_intent, conn=conn, match_limit=match_limit).to_dict()
    normalized_title = _text(title) or _title_from_intent(clean_intent)

    from runtime.canonical_workflows import mutate_workflow_build, save_workflow
    from storage.postgres.workflow_runtime_repository import load_workflow_record

    normalized_workflow_id = _text(workflow_id)
    workflow_row = (
        load_workflow_record(conn, workflow_id=normalized_workflow_id)
        if normalized_workflow_id
        else None
    )
    if workflow_row is None:
        normalized_workflow_id = normalized_workflow_id or _workflow_id_from_intent()
        workflow_row = save_workflow(
            conn,
            workflow_id=None,
            body={
                "id": normalized_workflow_id,
                "name": normalized_title,
                "description": clean_intent[:200],
                "definition": {
                    "workflow_id": normalized_workflow_id,
                    "source_prose": clean_intent,
                    "compile_cqrs": {
                        "state": "started",
                        "input_fingerprint": preview["input_fingerprint"],
                    },
                },
            },
        )

    mutation_body: dict[str, Any] = {
        "prose": clean_intent,
        "title": normalized_title,
    }
    if enable_llm is not None:
        mutation_body["enable_llm"] = bool(enable_llm)
    if enable_full_compose is not None:
        mutation_body["enable_full_compose"] = bool(enable_full_compose)

    mutation = mutate_workflow_build(
        conn,
        workflow_id=normalized_workflow_id,
        subpath="bootstrap",
        body=mutation_body,
    )
    mutation["compile_preview"] = preview
    return {
        "kind": "compile_materialization",
        "cqrs_role": "command",
        "ok": True,
        "workflow_id": normalized_workflow_id,
        "workflow": {
            "id": normalized_workflow_id,
            "name": workflow_row.get("name") if isinstance(workflow_row, dict) else normalized_title,
        },
        "compile_preview": preview,
        "mutation": mutation,
    }


__all__ = [
    "CompilePreview",
    "materialize_workflow",
    "preview_compile",
]
