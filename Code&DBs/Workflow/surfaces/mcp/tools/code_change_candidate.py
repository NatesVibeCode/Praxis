"""MCP tools for code-change candidate review/materialization."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_code_change_candidate_review(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="code_change_candidate.review",
        payload=payload,
    )


def tool_praxis_code_change_candidate_materialize(params: dict) -> dict:
    payload = {key: value for key, value in params.items() if value is not None}
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="code_change_candidate.materialize",
        payload=payload,
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_code_change_candidate_review": (
        tool_praxis_code_change_candidate_review,
        {
            "kind": "write",
            "operation_names": ["code_change_candidate.review"],
            "description": (
                "Review a sealed code-change candidate. Writes the canonical decision "
                "to workflow_job_submission_reviews through the CQRS gateway; it never "
                "mutates source."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "reviewer_ref": {"type": "string"},
                    "decision": {
                        "type": "string",
                        "enum": ["approve", "reject", "request_changes"],
                    },
                    "reasons": {"type": "array", "items": {"type": "string"}},
                    "override_reasons": {"type": "array", "items": {"type": "string"}},
                    "summary": {"type": "string"},
                    "evidence_refs": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["candidate_id", "reviewer_ref", "decision"],
            },
        },
    ),
    "praxis_code_change_candidate_materialize": (
        tool_praxis_code_change_candidate_materialize,
        {
            "kind": "write",
            "operation_names": ["code_change_candidate.materialize"],
            "description": (
                "Materialize an approved or auto-apply code-change candidate. The CQRS "
                "handler rechecks verifier/gate evidence before applying source."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "materialized_by": {"type": "string"},
                    "repo_root": {"type": "string"},
                },
                "required": ["candidate_id"],
            },
        },
    ),
}
