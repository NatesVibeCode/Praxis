"""Thin workflow submission frontdoor.

This module is the stable import surface for submission writes and reads.
It owns request validation, workflow MCP context binding, and structured
error shaping only. Durable submission state lives in the shared runtime
service that this module loads lazily.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
import inspect
import asyncio
from runtime.async_bridge import run_sync_safe
from typing import Any, Callable, Protocol

from surfaces.mcp.catalog import canonical_tool_name
from surfaces.mcp.helpers import _serialize
from surfaces.mcp.runtime_context import WorkflowMcpRequestContext, get_current_workflow_mcp_context
from runtime.workflow.submission_contract import (
    SubmissionContractError,
    normalize_declared_operations as _normalize_declared_operations_impl,
    normalize_text as _normalize_text_impl,
    normalize_text_list as _normalize_text_list_impl,
    optional_text as _optional_text_impl,
)


_SUBMIT_TOOL_NAMES = {
    "praxis_submit_code_change_candidate": "code_change_candidate",
    "praxis_submit_research_result": "research_result",
    "praxis_submit_artifact_bundle": "artifact_bundle",
}
_READ_TOOL_NAME = "praxis_get_submission"
_REVIEW_TOOL_NAME = "praxis_review_submission"
_VALID_REVIEW_DECISIONS = frozenset({"approve", "request_changes", "reject"})


@dataclass(frozen=True, slots=True)
class SubmissionFrontdoorError(RuntimeError):
    """Raised when the submission frontdoor cannot safely complete."""

    reason_code: str
    message: str
    details: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.message)


class _SubmissionService(Protocol):
    def submit_code_change_candidate(self, **kwargs: Any) -> Any: ...

    def submit_research_result(self, **kwargs: Any) -> Any: ...

    def submit_artifact_bundle(self, **kwargs: Any) -> Any: ...

    def get_submission(self, **kwargs: Any) -> Any: ...

    def review_submission(self, **kwargs: Any) -> Any: ...


def _error(
    tool_name: str,
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": False,
        "tool": tool_name,
        "error": {
            "reason_code": reason_code,
            "message": message,
            "details": _serialize(dict(details or {})),
        },
    }


def _success(tool_name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    result = {"ok": True, "tool": tool_name}
    result.update(_serialize(dict(payload)))
    return result


def _frontdoor_response(tool_name: str, operation: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return operation()
    except SubmissionFrontdoorError as exc:
        return _error(tool_name, exc.reason_code, exc.message, details=exc.details)
    except Exception as exc:
        return _error(
            tool_name,
            "workflow_submission.service_error",
            "workflow submission frontdoor failed",
            details={"exception_type": type(exc).__name__, "message": str(exc)},
        )


def _require_text(value: object, *, field_name: str) -> str:
    try:
        return _normalize_text_impl(value, field_name=field_name)
    except SubmissionContractError as exc:
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            str(exc),
            details=exc.details,
        ) from exc


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        return _optional_text_impl(value, field_name=field_name)
    except SubmissionContractError as exc:
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            str(exc),
            details=exc.details,
        ) from exc


def _require_text_list(value: object, *, field_name: str) -> tuple[str, ...]:
    try:
        return tuple(_normalize_text_list_impl(value, field_name=field_name))
    except SubmissionContractError as exc:
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            str(exc),
            details=exc.details,
        ) from exc


def _require_declared_operations(value: object) -> tuple[dict[str, Any], ...]:
    try:
        return tuple(_normalize_declared_operations_impl(value))
    except SubmissionContractError as exc:
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            str(exc),
            details=exc.details,
        ) from exc


def _require_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            f"{field_name} must be an object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _normalize_context(
    context: WorkflowMcpRequestContext | None = None,
) -> WorkflowMcpRequestContext:
    active_context = context or get_current_workflow_mcp_context()
    if active_context is None:
        raise SubmissionFrontdoorError(
            "workflow_submission.context_missing",
            "workflow MCP session context is unavailable",
        )
    if not active_context.run_id:
        raise SubmissionFrontdoorError(
            "workflow_submission.context_missing",
            "workflow MCP session is missing run_id authority",
            details={"job_label": active_context.job_label},
        )
    if not active_context.workflow_id:
        raise SubmissionFrontdoorError(
            "workflow_submission.context_missing",
            "workflow MCP session is missing workflow_id authority",
            details={"run_id": active_context.run_id, "job_label": active_context.job_label},
        )
    if not active_context.job_label:
        raise SubmissionFrontdoorError(
            "workflow_submission.context_missing",
            "workflow MCP session is missing job_label authority",
            details={"run_id": active_context.run_id, "workflow_id": active_context.workflow_id},
        )
    return active_context


def _require_tool_admission(context: WorkflowMcpRequestContext, tool_name: str) -> None:
    canonical = canonical_tool_name(tool_name)
    allowed_tools = {canonical_tool_name(tool) for tool in context.allowed_tools}
    if canonical not in allowed_tools:
        raise SubmissionFrontdoorError(
            "workflow_submission.tool_not_allowed",
            "workflow MCP session does not admit this tool",
            details={
                "tool": canonical,
                "job_label": context.job_label,
                "allowed_tools": sorted(allowed_tools),
            },
        )


@lru_cache(maxsize=1)
def _load_submission_service() -> _SubmissionService:
    try:
        from runtime.workflow import submission_capture as submission_service
    except Exception as exc:  # pragma: no cover - import-time guard
        raise SubmissionFrontdoorError(
            "workflow_submission.service_missing",
            "runtime.workflow.submission_capture is not available",
            details={"module": "runtime.workflow.submission_capture", "error": f"{type(exc).__name__}: {exc}"},
        ) from exc

    return submission_service  # type: ignore[return-value]


def _call_service(
    tool_name: str,
    *,
    method_names: Sequence[str],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    service = _load_submission_service()
    for method_name in method_names:
        method = getattr(service, method_name, None)
        if method is None:
            continue
        try:
            result = method(**dict(payload))
            if inspect.isawaitable(result):
                try:
                    result = run_sync_safe(result)
                except RuntimeError as exc:
                    return _error(
                        tool_name,
                        "workflow_submission.service_async_unsupported",
                        "workflow submission service returned an awaitable",
                        details={"exception_type": type(exc).__name__, "message": str(exc)},
                    )
        except SubmissionFrontdoorError as exc:
            return _error(tool_name, exc.reason_code, exc.message, details=exc.details)
        except Exception as exc:
            reason_code = str(getattr(exc, "reason_code", "") or "").strip() or "workflow_submission.service_error"
            details = getattr(exc, "details", None)
            return _error(
                tool_name,
                reason_code,
                str(exc) or "workflow submission service failed",
                details=(
                    details
                    if isinstance(details, Mapping)
                    else {"exception_type": type(exc).__name__, "message": str(exc)}
                ),
            )
        return _success(tool_name, {"submission": _serialize(result)})

    return _error(
        tool_name,
        "workflow_submission.service_interface_missing",
        "workflow submission service does not expose the expected method",
        details={"module": type(service).__module__, "methods": list(method_names)},
    )


def _submit(
    *,
    tool_name: str,
    result_kind: str,
    summary: object,
    primary_paths: object,
    tests_ran: object | None = None,
    notes: object | None = None,
    declared_operations: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    active_context = _normalize_context(context)
    tool_name = canonical_tool_name(tool_name)
    _require_tool_admission(active_context, tool_name)
    normalized_result_kind = _require_text(result_kind, field_name="result_kind").lower()
    if normalized_result_kind != _SUBMIT_TOOL_NAMES[tool_name]:
        return _error(
            tool_name,
            "workflow_submission.invalid_input",
            "result_kind does not match the submit tool",
            details={
                "expected": _SUBMIT_TOOL_NAMES[tool_name],
                "actual": normalized_result_kind,
            },
        )

    payload = {
        "run_id": active_context.run_id,
        "workflow_id": active_context.workflow_id,
        "job_label": active_context.job_label,
        "result_kind": normalized_result_kind,
        "summary": _require_text(summary, field_name="summary"),
        "primary_paths": list(_require_text_list(primary_paths, field_name="primary_paths")),
        "tests_ran": list(_require_text_list(tests_ran, field_name="tests_ran")) if tests_ran is not None else None,
        "notes": _optional_text(notes, field_name="notes"),
        "declared_operations": list(_require_declared_operations(declared_operations)),
    }
    return _call_service(
        tool_name,
        method_names=(tool_name.removeprefix("praxis_"), tool_name),
        payload=payload,
    )


def submit_code_change_candidate(
    *,
    bug_id: object,
    proposal_payload: object,
    source_context_refs: object,
    base_head_ref: object | None = None,
    review_routing: object = "human_review",
    verifier_ref: object | None = None,
    verifier_inputs: object | None = None,
    summary: object | None = None,
    notes: object | None = None,
    routing_decision_record: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    def _operation() -> dict[str, Any]:
        active_context = _normalize_context(context)
        tool_name = "praxis_submit_code_change_candidate"
        _require_tool_admission(active_context, tool_name)
        payload = {
            "run_id": active_context.run_id,
            "workflow_id": active_context.workflow_id,
            "job_label": active_context.job_label,
            "bug_id": _require_text(bug_id, field_name="bug_id"),
            "proposal_payload": _require_mapping(proposal_payload, field_name="proposal_payload"),
            "source_context_refs": source_context_refs,
            "base_head_ref": _optional_text(base_head_ref, field_name="base_head_ref"),
            "review_routing": _require_text(review_routing, field_name="review_routing"),
            "verifier_ref": _optional_text(verifier_ref, field_name="verifier_ref"),
            "verifier_inputs": (
                _require_mapping(verifier_inputs, field_name="verifier_inputs")
                if verifier_inputs is not None
                else None
            ),
            "summary": _optional_text(summary, field_name="summary"),
            "notes": _optional_text(notes, field_name="notes"),
            "routing_decision_record": (
                _require_mapping(routing_decision_record, field_name="routing_decision_record")
                if routing_decision_record is not None
                else None
            ),
        }
        from runtime.operation_catalog_gateway import execute_operation_from_env
        from surfaces.mcp.subsystems import workflow_database_env

        result = execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="code_change_candidate.submit",
            payload=payload,
        )
        if isinstance(result, dict):
            result.setdefault("tool", tool_name)
        return result

    return _frontdoor_response("praxis_submit_code_change_candidate", _operation)


def submit_research_result(
    *,
    summary: object,
    primary_paths: object,
    result_kind: object,
    tests_ran: object | None = None,
    notes: object | None = None,
    declared_operations: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    return _frontdoor_response(
        "praxis_submit_research_result",
        lambda: _submit(
            tool_name="praxis_submit_research_result",
            result_kind=_require_text(result_kind, field_name="result_kind"),
            summary=summary,
            primary_paths=primary_paths,
            tests_ran=tests_ran,
            notes=notes,
            declared_operations=declared_operations,
            context=context,
        ),
    )


def submit_artifact_bundle(
    *,
    summary: object,
    primary_paths: object,
    result_kind: object,
    tests_ran: object | None = None,
    notes: object | None = None,
    declared_operations: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    return _frontdoor_response(
        "praxis_submit_artifact_bundle",
        lambda: _submit(
            tool_name="praxis_submit_artifact_bundle",
            result_kind=_require_text(result_kind, field_name="result_kind"),
            summary=summary,
            primary_paths=primary_paths,
            tests_ran=tests_ran,
            notes=notes,
            declared_operations=declared_operations,
            context=context,
        ),
    )


def _target_payload(
    *,
    tool_name: str,
    submission_id: object | None,
    job_label: object | None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    active_context = _normalize_context(context)
    _require_tool_admission(active_context, tool_name)
    target_submission_id = _optional_text(submission_id, field_name="submission_id")
    target_job_label = _optional_text(job_label, field_name="job_label")
    if bool(target_submission_id) == bool(target_job_label):
        raise SubmissionFrontdoorError(
            "workflow_submission.invalid_input",
            "provide exactly one of submission_id or job_label",
            details={
                "submission_id": target_submission_id,
                "job_label": target_job_label,
            },
        )
    payload: dict[str, Any] = {
        "run_id": active_context.run_id,
        "workflow_id": active_context.workflow_id,
    }
    if target_submission_id:
        payload["submission_id"] = target_submission_id
    if target_job_label:
        payload["job_label"] = target_job_label
    return payload


def get_submission(
    *,
    submission_id: object | None = None,
    job_label: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    return _frontdoor_response(
        _READ_TOOL_NAME,
        lambda: _call_service(
            _READ_TOOL_NAME,
            method_names=("get_submission", "praxis_get_submission", _READ_TOOL_NAME),
            payload=_target_payload(
                tool_name=_READ_TOOL_NAME,
                submission_id=submission_id,
                job_label=job_label,
                context=context,
            ),
        ),
    )


def review_submission(
    *,
    decision: object,
    summary: object,
    submission_id: object | None = None,
    job_label: object | None = None,
    notes: object | None = None,
    policy_snapshot_ref: object | None = None,
    target_ref: object | None = None,
    current_head_ref: object | None = None,
    promotion_intent_at: object | None = None,
    finalized_at: object | None = None,
    canonical_commit_ref: object | None = None,
    context: WorkflowMcpRequestContext | None = None,
) -> dict[str, Any]:
    return _frontdoor_response(
        _REVIEW_TOOL_NAME,
        lambda: _review_submission(
            decision=decision,
            summary=summary,
            submission_id=submission_id,
            job_label=job_label,
            notes=notes,
            policy_snapshot_ref=policy_snapshot_ref,
            target_ref=target_ref,
            current_head_ref=current_head_ref,
            promotion_intent_at=promotion_intent_at,
            finalized_at=finalized_at,
            canonical_commit_ref=canonical_commit_ref,
            context=context,
        ),
    )


def _review_submission(
    *,
    decision: object,
    summary: object,
    submission_id: object | None,
    job_label: object | None,
    notes: object | None,
    policy_snapshot_ref: object | None,
    target_ref: object | None,
    current_head_ref: object | None,
    promotion_intent_at: object | None,
    finalized_at: object | None,
    canonical_commit_ref: object | None,
    context: WorkflowMcpRequestContext | None,
) -> dict[str, Any]:
    active_context = _normalize_context(context)
    _require_tool_admission(active_context, _REVIEW_TOOL_NAME)
    normalized_decision = _require_text(decision, field_name="decision").lower()
    if normalized_decision not in _VALID_REVIEW_DECISIONS:
        return _error(
            _REVIEW_TOOL_NAME,
            "workflow_submission.invalid_input",
            "decision must be one of approve, request_changes, reject",
            details={"decision": normalized_decision},
        )

    payload = _target_payload(
        tool_name=_REVIEW_TOOL_NAME,
        submission_id=submission_id,
        job_label=job_label,
        context=active_context,
    )
    payload["reviewer_job_label"] = active_context.job_label
    payload.update(
        {
            "decision": normalized_decision,
            "summary": _require_text(summary, field_name="summary"),
            "notes": _optional_text(notes, field_name="notes"),
        }
    )
    if policy_snapshot_ref is not None:
        payload["policy_snapshot_ref"] = _require_text(
            policy_snapshot_ref,
            field_name="policy_snapshot_ref",
        )
    if target_ref is not None:
        payload["target_ref"] = _require_text(target_ref, field_name="target_ref")
    if current_head_ref is not None:
        payload["current_head_ref"] = _require_text(
            current_head_ref,
            field_name="current_head_ref",
        )
    if promotion_intent_at is not None:
        payload["promotion_intent_at"] = promotion_intent_at
    if finalized_at is not None:
        payload["finalized_at"] = finalized_at
    if canonical_commit_ref is not None:
        payload["canonical_commit_ref"] = _require_text(
            canonical_commit_ref,
            field_name="canonical_commit_ref",
        )
    return _call_service(
        _REVIEW_TOOL_NAME,
        method_names=("review_submission", "praxis_review_submission", _REVIEW_TOOL_NAME),
        payload=payload,
    )


__all__ = [
    "SubmissionFrontdoorError",
    "get_submission",
    "review_submission",
    "submit_artifact_bundle",
    "submit_code_change",
    "submit_code_change_candidate",
    "submit_research_result",
]
