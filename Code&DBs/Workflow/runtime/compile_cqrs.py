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


class MaterializationError(RuntimeError):
    """Typed materialization failure that should persist a failed receipt."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _title_from_intent(intent: str) -> str:
    clean = " ".join(intent.strip().split())
    if not clean:
        return "Compiled workflow"
    return clean[:64].rstrip(" .,;:") or "Compiled workflow"


def _workflow_id_from_intent() -> str:
    return f"wf_compile_{uuid.uuid4().hex[:12]}"


def _graph_summary_from_mutation(mutation: dict[str, Any]) -> dict[str, Any]:
    bundle = mutation.get("build_bundle") if isinstance(mutation.get("build_bundle"), dict) else {}
    graph = bundle.get("build_graph") if isinstance(bundle.get("build_graph"), dict) else {}
    nodes = graph.get("nodes") if isinstance(graph.get("nodes"), list) else []
    edges = graph.get("edges") if isinstance(graph.get("edges"), list) else []
    projection_status = (
        bundle.get("projection_status")
        if isinstance(bundle.get("projection_status"), dict)
        else {}
    )
    candidate_manifest = (
        mutation.get("candidate_resolution_manifest")
        if isinstance(mutation.get("candidate_resolution_manifest"), dict)
        else {}
    )
    execution_manifest = (
        mutation.get("execution_manifest")
        if isinstance(mutation.get("execution_manifest"), dict)
        else None
    )
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "build_state": _text(projection_status.get("state")) or None,
        "execution_readiness": _text(candidate_manifest.get("execution_readiness")) or None,
        "has_candidate_resolution_manifest": bool(candidate_manifest),
        "has_execution_manifest": bool(execution_manifest),
    }


def _materialization_blockers(
    *,
    mutation: dict[str, Any],
    workflow_id: str,
    preview: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    definition = mutation.get("definition") if isinstance(mutation.get("definition"), dict) else {}
    provenance = (
        definition.get("compose_provenance")
        if isinstance(definition.get("compose_provenance"), dict)
        else None
    )
    graph_summary = _graph_summary_from_mutation(mutation)
    details: dict[str, Any] = {
        "workflow_id": workflow_id,
        "graph_summary": graph_summary,
        "compile_preview": preview,
        "planning_notes": mutation.get("planning_notes") if isinstance(mutation.get("planning_notes"), list) else [],
    }
    if provenance and provenance.get("ok") is False:
        details["compose_provenance"] = provenance
        return (
            _text(provenance.get("reason_code")) or "compile.materialize.compose_failed",
            details,
        )
    if graph_summary["node_count"] < 1:
        return "compile.materialize.empty_graph", details
    scope_packet = preview.get("scope_packet") if isinstance(preview.get("scope_packet"), dict) else {}
    recognized_span_count = len(scope_packet.get("spans") or []) if isinstance(scope_packet.get("spans"), list) else 0
    suggested_step_count = (
        len(scope_packet.get("suggested_steps") or [])
        if isinstance(scope_packet.get("suggested_steps"), list)
        else 0
    )
    if graph_summary["node_count"] < 2 and (recognized_span_count >= 3 or suggested_step_count >= 2):
        return "compile.materialize.under_decomposed", details
    if not graph_summary["has_candidate_resolution_manifest"]:
        return "compile.materialize.missing_resolution_manifest", details
    return None, details


def _input_fingerprint(intent: str) -> str:
    return hashlib.sha256(intent.strip().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MaterializePreview:
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
) -> MaterializePreview:
    clean_intent = _text(intent)
    if not clean_intent:
        raise ValueError("intent must be a non-empty string")
    recognition = recognize_intent(clean_intent, conn=conn, match_limit=match_limit).to_dict()
    return MaterializePreview(
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
    llm_timeout_seconds: int | None = None,
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
    mutation_body: dict[str, Any] = {
        "prose": clean_intent,
        "title": normalized_title,
    }
    if enable_llm is not None:
        mutation_body["enable_llm"] = bool(enable_llm)
    if enable_full_compose is not None:
        mutation_body["enable_full_compose"] = bool(enable_full_compose)
    if llm_timeout_seconds is not None:
        try:
            timeout_seconds = int(llm_timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise MaterializationError(
                "compile.materialize.invalid_timeout",
                "llm_timeout_seconds must be an integer",
                details={"llm_timeout_seconds": llm_timeout_seconds},
            ) from exc
        if timeout_seconds < 5 or timeout_seconds > 600:
            raise MaterializationError(
                "compile.materialize.invalid_timeout",
                "llm_timeout_seconds must be between 5 and 600",
                details={"llm_timeout_seconds": timeout_seconds},
            )
        mutation_body["llm_timeout_seconds"] = timeout_seconds

    use_full_compose = bool(enable_full_compose) if enable_full_compose is not None else True
    if use_full_compose:
        from runtime.compose_plan_via_llm import compose_plan_via_llm

        llm_overrides = (
            {"timeout_seconds": mutation_body["llm_timeout_seconds"]}
            if "llm_timeout_seconds" in mutation_body
            else None
        )
        compose_result = compose_plan_via_llm(
            clean_intent,
            conn=conn,
            plan_name=normalized_title,
            concurrency=5,
            llm_overrides=llm_overrides,
        )
        if compose_result.ok is not True:
            compose_payload = compose_result.to_dict()
            reason_code = _text(compose_payload.get("reason_code")) or "compile.materialize.compose_failed"
            raise MaterializationError(
                reason_code,
                f"Compile materialization blocked: {reason_code}",
                details={
                    "workflow_id": normalized_workflow_id or None,
                    "compile_preview": preview,
                    "compose_provenance": compose_payload,
                },
            )
        mutation_body["_compose_result"] = compose_result

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

    try:
        mutation = mutate_workflow_build(
            conn,
            workflow_id=normalized_workflow_id,
            subpath="bootstrap",
            body=mutation_body,
        )
    except Exception as exc:
        reason_code = _text(getattr(exc, "reason_code", None)) or type(exc).__name__
        details = getattr(exc, "details", None)
        merged_details = dict(details) if isinstance(details, dict) else {}
        merged_details.setdefault("workflow_id", normalized_workflow_id)
        merged_details.setdefault("compile_preview", preview)
        raise MaterializationError(
            reason_code,
            f"Compile materialization blocked: {reason_code}",
            details=merged_details,
        ) from exc
    mutation["compile_preview"] = preview
    reason_code, details = _materialization_blockers(
        mutation=mutation,
        workflow_id=normalized_workflow_id,
        preview=preview,
    )
    if reason_code:
        raise MaterializationError(
            reason_code,
            f"Compile materialization blocked: {reason_code}",
            details=details,
        )

    from runtime.workflow_build_moment import build_workflow_build_moment

    build_payload = build_workflow_build_moment(
        mutation["row"],
        conn=conn,
        definition=mutation["definition"],
        materialized_spec=mutation["materialized_spec"],
        build_bundle=mutation["build_bundle"],
        planning_notes=mutation["planning_notes"],
        intent_brief=mutation.get("intent_brief"),
        execution_manifest=mutation.get("execution_manifest"),
        progressive_build=mutation.get("progressive_build"),
        undo_receipt=mutation.get("undo_receipt"),
        mutation_event_id=mutation.get("mutation_event_id"),
        compile_preview=preview,
    )
    graph_summary = _graph_summary_from_mutation(mutation)
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
        "graph_summary": graph_summary,
        "build_payload": build_payload,
        "mutation": mutation,
    }


__all__ = [
    "MaterializationError",
    "MaterializePreview",
    "materialize_workflow",
    "preview_compile",
]
