"""Shared MCP tool invocation authority for CLI and JSON-RPC surfaces."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from runtime.workflow.mcp_session import (
    WorkflowMcpSessionError,
    verify_workflow_mcp_session_token,
)

from .catalog import canonical_tool_name, get_tool_catalog, resolve_tool_entry
from .runtime_context import (
    get_current_workflow_mcp_context,
    workflow_mcp_request_context,
)
from .subsystems import _subs

_EMBEDDING_PREWARM_TOOLS = frozenset(
    {
        "praxis_discover",
        "praxis_recall",
        "praxis_intent_match",
        "praxis_query",
        "praxis_research",
    }
)


@dataclass(frozen=True, slots=True)
class ToolInvocationError(RuntimeError):
    message: str
    reason_code: str = "mcp.tool_error"
    details: dict[str, Any] | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "error": self.message,
            "reason_code": self.reason_code,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def normalize_allowed_tool_names(value: object | None) -> set[str] | None:
    if value is None:
        return None
    raw_values: list[str]
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.replace("\n", ",").split(",")]
    elif isinstance(value, Sequence):
        raw_values = [str(part or "").strip() for part in value]
    else:
        return None
    allowed = {canonical_tool_name(item) for item in raw_values if item}
    return allowed or set()


def invoke_tool(
    tool_name: object,
    raw_arguments: object | None = None,
    *,
    allowed_tool_names: object | None = None,
    workflow_token: str = "",
    progress_emitter: Any = None,
) -> dict[str, Any]:
    canonical_name = canonical_tool_name(tool_name)
    catalog = get_tool_catalog()
    definition = catalog.get(canonical_name)
    if definition is None:
        raise ToolInvocationError(
            f"Tool not found: {tool_name}",
            reason_code="mcp.tool_not_found",
        )

    allowed = normalize_allowed_tool_names(allowed_tool_names)
    token_text = str(workflow_token or "").strip()
    claims: dict[str, Any] | None = None
    if token_text:
        try:
            claims = verify_workflow_mcp_session_token(token_text)
        except WorkflowMcpSessionError as exc:
            raise ToolInvocationError(str(exc), reason_code=exc.reason_code) from exc
        token_allowed = normalize_allowed_tool_names(claims.get("allowed_tools")) or set()
        if canonical_name not in token_allowed:
            raise ToolInvocationError(
                f"Tool not allowed by workflow token: {canonical_name}",
                reason_code="workflow_mcp.tool_not_allowed",
            )
        allowed = token_allowed if allowed is None else allowed & token_allowed

    if allowed is not None and canonical_name not in allowed:
        raise ToolInvocationError(
            f"Tool not allowed: {tool_name}",
            reason_code="mcp.tool_not_allowed",
        )

    if raw_arguments is None:
        tool_input: dict[str, Any] = {}
    elif isinstance(raw_arguments, dict):
        tool_input = dict(raw_arguments)
    else:
        raise ToolInvocationError(
            f"Tool arguments must be a JSON object: {tool_name}",
            reason_code="mcp.invalid_arguments",
        )

    if definition.requires_workflow_token and not token_text:
        active_context = get_current_workflow_mcp_context()
        if active_context is None:
            raise ToolInvocationError(
                f"workflow token is required for tool: {canonical_name}",
                reason_code="workflow_mcp.token_required",
            )

    if canonical_name in _EMBEDDING_PREWARM_TOOLS:
        _maybe_start_embedding_prewarm()

    handler, _ = resolve_tool_entry(canonical_name)
    call_context = _context_manager_for_claims(claims)
    try:
        with call_context:
            return _call_handler(
                handler,
                tool_input,
                workflow_token=token_text,
                progress_emitter=progress_emitter,
            )
    except ToolInvocationError:
        raise
    except Exception as exc:
        raise ToolInvocationError(
            f"{type(exc).__name__}: {exc}",
            reason_code="mcp.handler_error",
        ) from exc


def _context_manager_for_claims(claims: dict[str, Any] | None):
    if not claims:
        return nullcontext()
    return workflow_mcp_request_context(
        run_id=str(claims.get("run_id") or "").strip() or None,
        workflow_id=str(claims.get("workflow_id") or "").strip() or None,
        job_label=str(claims.get("job_label") or "").strip(),
        allowed_tools=claims.get("allowed_tools") or [],
        expires_at=int(claims.get("exp") or 0),
    )


def _maybe_start_embedding_prewarm() -> None:
    try:
        from runtime.embedding_service import (
            EmbeddingService,
            resolve_embedding_runtime_authority,
        )

        EmbeddingService.start_background_prewarm(
            resolve_embedding_runtime_authority().model_name
        )
    except Exception:
        # Tool execution still works if prewarm cannot be scheduled; this is
        # only a latency optimization for embedding-backed read paths.
        return


def _call_handler(
    handler: Any,
    tool_input: dict[str, Any],
    *,
    workflow_token: str,
    progress_emitter: Any,
) -> dict[str, Any]:
    signature = inspect.signature(handler)
    parameters = signature.parameters
    accepts_var_kw = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    positional_parameters = [
        parameter
        for parameter in parameters.values()
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    extra_kwargs: dict[str, Any] = {}
    if "_progress_emitter" in parameters:
        extra_kwargs["_progress_emitter"] = progress_emitter

    if positional_parameters:
        result = handler(tool_input, **extra_kwargs)
        if isinstance(result, dict):
            return result
        return {"result": result}

    call_kwargs: dict[str, Any] = {}
    if "_subsystems" in parameters or accepts_var_kw:
        call_kwargs["_subsystems"] = _subs
    if "_session_token" in parameters or accepts_var_kw:
        call_kwargs["_session_token"] = workflow_token
    if "_progress_emitter" in parameters or accepts_var_kw:
        call_kwargs["_progress_emitter"] = progress_emitter
    for key, value in tool_input.items():
        if key in parameters or accepts_var_kw:
            call_kwargs[key] = value
    result = handler(**call_kwargs)
    if isinstance(result, dict):
        return result
    return {"result": result}


__all__ = [
    "ToolInvocationError",
    "invoke_tool",
    "normalize_allowed_tool_names",
]
