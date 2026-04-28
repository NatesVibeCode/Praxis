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
        source_refs=["BUG-123"],
        access_policy={
            "resolved_read_scope": ["Code&DBs/Workflow/runtime/example.py"],
            "write_scope": ["Code&DBs/Workflow/tests/unit/test_example.py"],
        },
    )

    claims = mcp_session.verify_workflow_mcp_session_token(token)

    assert claims["run_id"] == "run.alpha"
    assert claims["workflow_id"] == "workflow.alpha"
    assert claims["job_label"] == "job-alpha"
    assert claims["allowed_tools"] == ["praxis_context_shard", "praxis_query"]
    assert claims["source_refs"] == ["BUG-123"]
    assert claims["access_policy"]["resolved_read_scope"] == [
        "Code&DBs/Workflow/runtime/example.py"
    ]


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


def test_workflow_mcp_session_token_records_key_id_and_jti(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_KEY_ID", "kid-test")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS", "600")
    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_000)

    token = mcp_session.mint_workflow_mcp_session_token(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job-alpha",
        allowed_tools=["praxis_context_shard"],
    )

    claims = mcp_session.verify_workflow_mcp_session_token(token)

    assert claims["kid"] == "kid-test"
    assert isinstance(claims["jti"], str)
    assert claims["jti"]


def test_workflow_mcp_session_token_verifies_rotated_keyring(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS", "600")
    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_000)
    monkeypatch.setenv(
        "PRAXIS_WORKFLOW_MCP_SIGNING_KEYS_JSON",
        '{"active_kid":"kid-old","keys":{"kid-old":"old-secret","kid-new":"new-secret"}}',
    )
    token = mcp_session.mint_workflow_mcp_session_token(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job-alpha",
        allowed_tools=["praxis_context_shard"],
    )

    monkeypatch.setenv(
        "PRAXIS_WORKFLOW_MCP_SIGNING_KEYS_JSON",
        '{"active_kid":"kid-new","keys":{"kid-new":"new-secret","kid-old":"old-secret"}}',
    )

    claims = mcp_session.verify_workflow_mcp_session_token(token)

    assert claims["kid"] == "kid-old"
    assert claims["job_label"] == "job-alpha"


def test_workflow_mcp_session_token_rejects_revoked_jti(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS", "600")
    monkeypatch.setattr(mcp_session, "_current_time", lambda: 1_800_000_000)
    token = mcp_session.mint_workflow_mcp_session_token(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job-alpha",
        allowed_tools=["praxis_context_shard"],
    )
    claims = mcp_session.verify_workflow_mcp_session_token(token)

    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_REVOKED_JTIS", claims["jti"])

    with pytest.raises(mcp_session.WorkflowMcpSessionError) as exc_info:
        mcp_session.verify_workflow_mcp_session_token(token)

    assert exc_info.value.reason_code == "workflow_mcp.token_revoked"
