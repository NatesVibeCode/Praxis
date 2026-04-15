from __future__ import annotations

import pytest

from runtime.workflow import mcp_session


def test_mint_and_verify_workflow_mcp_session_token(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS", "600")
    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_000)

    token = mcp_session.mint_workflow_mcp_session_token(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job-alpha",
        allowed_tools=["praxis_context_shard", "praxis_query"],
    )

    claims = mcp_session.verify_workflow_mcp_session_token(token)

    assert claims["run_id"] == "run.alpha"
    assert claims["workflow_id"] == "workflow.alpha"
    assert claims["job_label"] == "job-alpha"
    assert claims["allowed_tools"] == ["praxis_context_shard", "praxis_query"]


def test_verify_workflow_mcp_session_token_rejects_expired_token(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS", "60")
    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_000)

    token = mcp_session.mint_workflow_mcp_session_token(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job-alpha",
        allowed_tools=["praxis_context_shard"],
    )

    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_061)

    try:
        mcp_session.verify_workflow_mcp_session_token(token)
    except mcp_session.WorkflowMcpSessionError as exc:
        assert exc.reason_code == "workflow_mcp.token_expired"
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected expired workflow MCP token to fail closed")


def test_mint_workflow_mcp_session_token_requires_explicit_signing_secret(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", raising=False)

    with pytest.raises(mcp_session.WorkflowMcpSessionError, match="PRAXIS_WORKFLOW_MCP_SIGNING_SECRET"):
        mcp_session.mint_workflow_mcp_session_token(
            run_id="run.alpha",
            workflow_id="workflow.alpha",
            job_label="job-alpha",
            allowed_tools=["praxis_context_shard"],
        )
