"""Per-request workflow MCP session context."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True, slots=True)
class WorkflowMcpRequestContext:
    run_id: str | None
    workflow_id: str | None
    job_label: str
    allowed_tools: tuple[str, ...]
    expires_at: int


_CURRENT_CONTEXT: ContextVar[WorkflowMcpRequestContext | None] = ContextVar(
    "workflow_mcp_request_context",
    default=None,
)


def get_current_workflow_mcp_context() -> WorkflowMcpRequestContext | None:
    return _CURRENT_CONTEXT.get()


@contextmanager
def workflow_mcp_request_context(
    *,
    run_id: str | None,
    workflow_id: str | None,
    job_label: str,
    allowed_tools: Sequence[str],
    expires_at: int,
) -> Iterator[WorkflowMcpRequestContext]:
    context = WorkflowMcpRequestContext(
        run_id=str(run_id or "").strip() or None,
        workflow_id=str(workflow_id or "").strip() or None,
        job_label=str(job_label or "").strip(),
        allowed_tools=tuple(str(tool).strip() for tool in allowed_tools if str(tool).strip()),
        expires_at=int(expires_at),
    )
    token = _CURRENT_CONTEXT.set(context)
    try:
        yield context
    finally:
        _CURRENT_CONTEXT.reset(token)

