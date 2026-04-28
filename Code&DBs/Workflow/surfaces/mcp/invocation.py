"""Shared MCP tool invocation authority for CLI and JSON-RPC surfaces."""

from __future__ import annotations

import inspect
import json
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from runtime.workflow.mcp_session import (
    WorkflowMcpSessionError,
    verify_workflow_mcp_session_token,
)

from .catalog import canonical_tool_name, get_tool_catalog, resolve_tool_entry

_EMBEDDING_PREWARM_TOOLS = frozenset(
    {
        "praxis_discover",
        "praxis_recall",
        "praxis_intent_match",
        "praxis_query",
        "praxis_research",
    }
)

_WORKFLOW_SCOPED_REQUIRES_NATIVE_CLAMP = frozenset(
    {
        "praxis_query",
        "praxis_discover",
        "praxis_recall",
        "praxis_graph",
        "praxis_research",
        "praxis_bugs",
        "praxis_receipts",
        "praxis_status_snapshot",
    }
)


def _subsystems():
    from .subsystems import _subs

    return _subs


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


def _evaluate_trigger_matches(
    tool_name: str,
    tool_input: dict[str, Any] | None,
) -> list:
    """Match a proposed MCP call against the operator-decision trigger
    registry. Universal across all agent harnesses (Claude Code / Codex /
    Gemini) because every harness routes through `invoke_tool`.

    Returns an empty list on any failure — never blocks the call.
    Surfacing is advisory; hard rejection lives at the data-authority
    layer (Packet 2). See `surfaces.policy.trigger_check` for the matcher.
    """
    try:
        from surfaces.policy import check as _check_triggers

        return _check_triggers(tool_name, tool_input or {})
    except Exception:  # noqa: BLE001 — fail open, never block tool dispatch
        return []


def _render_trigger_matches_payload(
    matches: list,
    tool_name: str,
) -> dict[str, Any]:
    """Render trigger matches as a structured payload for inclusion in tool
    results. Includes a rendered text block for direct surfacing AND a
    structured list so consumers can reason about the matches programmatically.
    """
    try:
        from surfaces.policy import render_additional_context

        rendered = render_additional_context(matches, tool_name)
    except Exception:  # noqa: BLE001 — degrade gracefully
        rendered = ""

    structured = []
    for match in matches:
        try:
            structured.append({
                "decision_key": match.decision_key,
                "title": match.title,
                "advisory_only": match.advisory_only,
                "trigger": match.trigger_repr(),
            })
        except Exception:  # noqa: BLE001
            continue

    return {
        "rendered": rendered,
        "matches": structured,
        "count": len(structured),
        "source": "surfaces.mcp.invocation:trigger_check",
    }


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
    token_text = str(workflow_token or "").strip()
    status_code = 200
    claims: dict[str, Any] | None = None
    tool_input: dict[str, Any] = {}
    result_payload: dict[str, Any] | None = None
    try:
        allowed = normalize_allowed_tool_names(allowed_tool_names)
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
            _enforce_workflow_shard_tool_contract(
                canonical_name=canonical_name,
                claims=claims,
            )

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
            from .runtime_context import get_current_workflow_mcp_context

            active_context = get_current_workflow_mcp_context()
            if active_context is None:
                raise ToolInvocationError(
                    f"workflow token is required for tool: {canonical_name}",
                    reason_code="workflow_mcp.token_required",
                )

        if canonical_name in _EMBEDDING_PREWARM_TOOLS:
            _maybe_start_embedding_prewarm()

        # JIT trigger-check against operator-decision registry. Surfaces
        # matching standing orders into the result payload as
        # `_standing_orders_surfaced` so any agent surface (Claude Code,
        # Codex, Gemini, raw HTTP) sees the same enforcement layer. Per
        # /praxis-debate fork: surfacing is advisory; hard rejection lives
        # at the data layer (Packet 2 — Policy Authority subsystem). This
        # is the universal floor — every MCP call goes through here so
        # enforcement does not depend on which harness the agent is in.
        trigger_matches = _evaluate_trigger_matches(canonical_name, tool_input)

        handler, _ = resolve_tool_entry(canonical_name)
        call_context = _context_manager_for_claims(claims)
        with call_context:
            result_payload = _call_handler(
                handler,
                tool_input,
                workflow_token=token_text,
                progress_emitter=progress_emitter,
            )
        if trigger_matches and isinstance(result_payload, dict):
            # Inject the surface into the result so the harness or
            # downstream consumer renders it back to the agent. Use a
            # neutral key ("_standing_orders_surfaced") that does not
            # collide with any tool's result schema.
            result_payload.setdefault(
                "_standing_orders_surfaced",
                _render_trigger_matches_payload(trigger_matches, canonical_name),
            )
        return result_payload
    except ToolInvocationError as exc:
        status_code = 400
        result_payload = exc.to_payload()
        raise
    except Exception as exc:
        status_code = 500
        result_payload = {
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "mcp.handler_error",
        }
        raise ToolInvocationError(
            f"{type(exc).__name__}: {exc}",
            reason_code="mcp.handler_error",
        ) from exc
    finally:
        _record_tool_usage(
            canonical_name=canonical_name,
            workflow_token=token_text,
            status_code=status_code,
            tool_input=tool_input,
            result_payload=result_payload or {},
            claims=claims,
        )


def _context_manager_for_claims(claims: dict[str, Any] | None):
    if not claims:
        return nullcontext()
    from .runtime_context import workflow_mcp_request_context

    return workflow_mcp_request_context(
        run_id=str(claims.get("run_id") or "").strip() or None,
        workflow_id=str(claims.get("workflow_id") or "").strip() or None,
        job_label=str(claims.get("job_label") or "").strip(),
        allowed_tools=claims.get("allowed_tools") or [],
        expires_at=int(claims.get("exp") or 0),
        source_refs=claims.get("source_refs") or [],
        access_policy=claims.get("access_policy") or {},
    )


def _claims_have_shard_scope(claims: dict[str, Any] | None) -> bool:
    if not isinstance(claims, dict):
        return False
    if claims.get("source_refs"):
        return True
    access_policy = claims.get("access_policy")
    if not isinstance(access_policy, dict):
        return False
    for key in (
        "resolved_read_scope",
        "declared_read_scope",
        "write_scope",
        "test_scope",
        "blast_radius",
        "allowed_record_refs",
        "allowed_entity_refs",
    ):
        value = access_policy.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            if any(str(item).strip() for item in value):
                return True
    return False


def _enforce_workflow_shard_tool_contract(
    *,
    canonical_name: str,
    claims: dict[str, Any] | None,
) -> None:
    if not _claims_have_shard_scope(claims):
        return
    if canonical_name not in _WORKFLOW_SCOPED_REQUIRES_NATIVE_CLAMP:
        return
    raise ToolInvocationError(
        f"Tool cannot prove workflow shard enforcement yet: {canonical_name}",
        reason_code="workflow_mcp.tool_scope_not_enforced",
        details={
            "tool": canonical_name,
            "allowed_alternatives": ["praxis_context_shard", "praxis_search"],
        },
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


def _record_tool_usage(
    *,
    canonical_name: str,
    workflow_token: str,
    status_code: int,
    tool_input: dict[str, Any],
    result_payload: dict[str, Any],
    claims: dict[str, Any] | None,
) -> None:
    query_chars = 0
    result_count = 0
    routed_to = ""
    reason_code = str(result_payload.get("reason_code") or "").strip()
    result_state = "error" if int(status_code) >= 400 else "ok"
    metadata: dict[str, Any] = {}
    if canonical_name == "praxis_query":
        query_chars = len(str(tool_input.get("question") or ""))
        routed_to = str(result_payload.get("routed_to") or "").strip()
        result_count = _tool_result_count(result_payload)
        result_state = _tool_result_state(
            status_code=int(status_code),
            result_payload=result_payload,
            result_count=result_count,
        )
        view = str(result_payload.get("view") or "").strip()
        if view:
            metadata["view"] = view
    usage_conn = None
    try:
        from storage.postgres import PostgresWorkflowSurfaceUsageRepository

        usage_conn = _subsystems().get_pg_conn()
        PostgresWorkflowSurfaceUsageRepository(usage_conn).record_event(
            surface_kind="mcp",
            transport_kind="mcp",
            entrypoint_kind="tool",
            entrypoint_name=canonical_name,
            caller_kind="workflow_session" if str(workflow_token or "").strip() else "direct",
            status_code=int(status_code),
            result_state=result_state,
            reason_code=reason_code,
            routed_to=routed_to,
            workflow_id=str((claims or {}).get("workflow_id") or "").strip(),
            run_id=str((claims or {}).get("run_id") or "").strip(),
            job_label=str((claims or {}).get("job_label") or "").strip(),
            payload_size_bytes=_json_size_bytes(tool_input),
            response_size_bytes=_json_size_bytes(result_payload),
            query_chars=query_chars,
            result_count=result_count,
            metadata=metadata,
        )
    except Exception as exc:
        from surfaces.api.handlers._surface_usage import record_surface_usage_failure

        record_surface_usage_failure(
            surface_kind="mcp",
            entrypoint_name=canonical_name,
            error=exc,
            conn=usage_conn,
        )
        return


def _json_size_bytes(value: Any) -> int:
    try:
        return len(json.dumps(value, sort_keys=True, default=str).encode("utf-8"))
    except TypeError:
        return 0


def _tool_result_count(result_payload: dict[str, Any]) -> int:
    count = result_payload.get("count")
    if isinstance(count, int) and count >= 0:
        return count
    for key in ("results", "bugs", "agents"):
        value = result_payload.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def _tool_result_state(
    *,
    status_code: int,
    result_payload: dict[str, Any],
    result_count: int,
) -> str:
    if status_code >= 400:
        return "error"
    status = str(result_payload.get("status") or "").strip().lower()
    if status in {"ok", "empty", "unavailable", "error"}:
        return status
    reason_code = str(result_payload.get("reason_code") or "").strip().lower()
    if reason_code.endswith(".unavailable"):
        return "unavailable"
    if result_count == 0 and (
        result_payload.get("results") == []
        or ("rollup" in result_payload and result_payload.get("rollup") is None)
    ):
        return "empty"
    return "ok"


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
        call_kwargs["_subsystems"] = _subsystems()
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
