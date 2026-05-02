"""CQRS command for committing one dispatch choice receipt."""

from __future__ import annotations

import json
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.execution_targets import (
    candidate_set_hash,
    selected_candidate_from_set,
)
from runtime.operations.queries.execution_targets import (
    QueryDispatchOptionsList,
    handle_query_dispatch_options_list,
)


SelectionKind = Literal[
    "default",
    "explicit_click",
    "programmatic_override",
    "ask_all",
]


class CommitDispatchChoiceCommand(BaseModel):
    task_slug: str = Field(default="auto/chat")
    workload_kind: str = Field(default="chat")
    candidate_set_hash: str
    selected_candidate_ref: str | None = None
    selected_provider_slug: str | None = None
    selected_model_slug: str | None = None
    selected_transport_type: str | None = None
    selection_kind: SelectionKind = "explicit_click"
    selected_by: str = "operator"
    surface: str = "app"
    dispatch_ref: str | None = None
    conversation_id: str | None = None
    include_cli: bool = True
    ask_all_candidate_refs: list[str] = Field(default_factory=list)

    @field_validator(
        "task_slug",
        "workload_kind",
        "candidate_set_hash",
        "selected_candidate_ref",
        "selected_provider_slug",
        "selected_model_slug",
        "selected_transport_type",
        "selected_by",
        "surface",
        "dispatch_ref",
        "conversation_id",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("dispatch choice text fields must be strings")
        cleaned = value.strip()
        return cleaned or None

    @model_validator(mode="after")
    def _validate_selection(self) -> "CommitDispatchChoiceCommand":
        if not self.candidate_set_hash:
            raise ValueError("candidate_set_hash is required")
        if self.selection_kind == "ask_all":
            if not self.ask_all_candidate_refs:
                raise ValueError("ask_all_candidate_refs is required for ask_all")
            return self
        if not (
            self.selected_candidate_ref
            or (self.selected_provider_slug and self.selected_model_slug)
            or self.selection_kind == "default"
        ):
            raise ValueError("selected_candidate_ref or selected provider/model is required")
        return self


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _insert_choice(conn: Any, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO execution_dispatch_choices (
            dispatch_choice_ref,
            dispatch_ref,
            workload_kind,
            task_slug,
            candidate_set_hash,
            selected_candidate_ref,
            selected_target_ref,
            selected_profile_ref,
            selected_provider_slug,
            selected_model_slug,
            selected_transport_type,
            selection_kind,
            selected_by,
            surface,
            conversation_id,
            candidate_set_json,
            selected_candidate_json,
            ask_all_candidates_json
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16::jsonb, $17::jsonb, $18::jsonb
        )
        ON CONFLICT (dispatch_choice_ref) DO NOTHING
        """,
        row["dispatch_choice_ref"],
        row.get("dispatch_ref"),
        row["workload_kind"],
        row["task_slug"],
        row["candidate_set_hash"],
        row.get("selected_candidate_ref"),
        row.get("selected_target_ref"),
        row.get("selected_profile_ref"),
        row.get("selected_provider_slug"),
        row.get("selected_model_slug"),
        row.get("selected_transport_type"),
        row["selection_kind"],
        row["selected_by"],
        row["surface"],
        row.get("conversation_id"),
        _json_dumps(row["candidate_set"]),
        _json_dumps(row["selected_candidate"]),
        _json_dumps(row["ask_all_candidates"]),
    )


def handle_commit_dispatch_choice(
    command: CommitDispatchChoiceCommand,
    subsystems: Any,
) -> dict[str, Any]:
    options = handle_query_dispatch_options_list(
        QueryDispatchOptionsList(
            task_slug=command.task_slug,
            workload_kind=command.workload_kind,
            include_disabled=True,
            include_cli=command.include_cli,
        ),
        subsystems,
    )
    if not options.get("ok"):
        return options

    candidates = list(options.get("candidates") or [])
    current_hash = candidate_set_hash(candidates)
    if current_hash != command.candidate_set_hash:
        return {
            "ok": False,
            "operation": "execution.dispatch_choice.commit",
            "error_code": "dispatch_choice.candidate_set_hash_mismatch",
            "candidate_set_hash": command.candidate_set_hash,
            "current_candidate_set_hash": current_hash,
        }

    by_ref = {candidate.get("candidate_ref"): candidate for candidate in candidates}

    if command.selection_kind == "default":
        permitted = [candidate for candidate in candidates if candidate.get("permitted")]
        if not permitted:
            return {
                "ok": False,
                "operation": "execution.dispatch_choice.commit",
                "error_code": "dispatch_choice.no_permitted_default",
            }
        selected = dict(permitted[0])
    elif command.selection_kind == "ask_all":
        selected_ref = command.ask_all_candidate_refs[0]
        candidate = by_ref.get(selected_ref)
        if candidate is None:
            return {
                "ok": False,
                "operation": "execution.dispatch_choice.commit",
                "error_code": "dispatch_choice.ask_all_candidate_not_in_set",
                "selected_candidate_ref": selected_ref,
            }
        selected = dict(candidate)
    else:
        try:
            selected = selected_candidate_from_set(
                candidates=candidates,
                selected_candidate_ref=command.selected_candidate_ref,
                selected_provider_slug=command.selected_provider_slug,
                selected_model_slug=command.selected_model_slug,
                selected_transport_type=command.selected_transport_type,
            )
        except ValueError as exc:
            return {
                "ok": False,
                "operation": "execution.dispatch_choice.commit",
                "error_code": "dispatch_choice.selected_candidate_not_in_set",
                "error": str(exc),
            }

    if not selected.get("permitted") or selected.get("disabled_reason"):
        return {
            "ok": False,
            "operation": "execution.dispatch_choice.commit",
            "error_code": "dispatch_choice.selected_candidate_disabled",
            "selected_candidate_ref": selected.get("candidate_ref"),
            "disabled_reason": selected.get("disabled_reason"),
        }

    ask_all_candidates: list[dict[str, Any]] = []
    if command.selection_kind == "ask_all":
        for candidate_ref in command.ask_all_candidate_refs:
            candidate = by_ref.get(candidate_ref)
            if candidate is None:
                return {
                    "ok": False,
                    "operation": "execution.dispatch_choice.commit",
                    "error_code": "dispatch_choice.ask_all_candidate_not_in_set",
                    "selected_candidate_ref": candidate_ref,
                }
            if not candidate.get("permitted") or candidate.get("disabled_reason"):
                return {
                    "ok": False,
                    "operation": "execution.dispatch_choice.commit",
                    "error_code": "dispatch_choice.ask_all_candidate_disabled",
                    "selected_candidate_ref": candidate_ref,
                    "disabled_reason": candidate.get("disabled_reason"),
                }
            ask_all_candidates.append(dict(candidate))

    choice_ref = f"dispatch_choice.{uuid4()}"
    row = {
        "dispatch_choice_ref": choice_ref,
        "dispatch_ref": command.dispatch_ref,
        "workload_kind": command.workload_kind,
        "task_slug": command.task_slug,
        "candidate_set_hash": current_hash,
        "selected_candidate_ref": selected.get("candidate_ref"),
        "selected_target_ref": selected.get("execution_target_ref"),
        "selected_profile_ref": selected.get("execution_profile_ref"),
        "selected_provider_slug": selected.get("provider_slug"),
        "selected_model_slug": selected.get("model_slug"),
        "selected_transport_type": selected.get("transport_type"),
        "selection_kind": command.selection_kind,
        "selected_by": command.selected_by or "operator",
        "surface": command.surface or "app",
        "conversation_id": command.conversation_id,
        "candidate_set": candidates,
        "selected_candidate": selected,
        "ask_all_candidates": ask_all_candidates,
    }
    _insert_choice(subsystems.get_pg_conn(), row)

    return {
        "ok": True,
        "operation": "execution.dispatch_choice.commit",
        "dispatch_choice_ref": choice_ref,
        "candidate_set_hash": current_hash,
        "selected_candidate_ref": selected.get("candidate_ref"),
        "selected_target_ref": selected.get("execution_target_ref"),
        "selected_profile_ref": selected.get("execution_profile_ref"),
        "selected_provider_slug": selected.get("provider_slug"),
        "selected_model_slug": selected.get("model_slug"),
        "selected_transport_type": selected.get("transport_type"),
        "selection_kind": command.selection_kind,
        "surface": command.surface,
        "selected_by": command.selected_by,
        "ask_all_count": len(ask_all_candidates),
    }


__all__ = [
    "CommitDispatchChoiceCommand",
    "handle_commit_dispatch_choice",
]
