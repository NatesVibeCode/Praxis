"""Workflow submission MCP tools."""

from __future__ import annotations

from typing import Any

import surfaces.api.workflow_submission as workflow_submission


def tool_praxis_submit_code_change_candidate(params: dict) -> dict:
    return workflow_submission.submit_code_change_candidate(
        bug_id=params.get("bug_id"),
        proposal_payload=params.get("proposal_payload"),
        source_context_refs=params.get("source_context_refs"),
        base_head_ref=params.get("base_head_ref"),
        review_routing=params.get("review_routing") or "human_review",
        verifier_ref=params.get("verifier_ref"),
        verifier_inputs=params.get("verifier_inputs"),
        summary=params.get("summary"),
        notes=params.get("notes"),
        routing_decision_record=params.get("routing_decision_record"),
    )


def tool_praxis_submit_research_result(params: dict) -> dict:
    return workflow_submission.submit_research_result(
        summary=params.get("summary"),
        primary_paths=params.get("primary_paths"),
        result_kind=params.get("result_kind"),
        tests_ran=params.get("tests_ran"),
        notes=params.get("notes"),
        declared_operations=params.get("declared_operations"),
    )


def tool_praxis_submit_artifact_bundle(params: dict) -> dict:
    return workflow_submission.submit_artifact_bundle(
        summary=params.get("summary"),
        primary_paths=params.get("primary_paths"),
        result_kind=params.get("result_kind"),
        tests_ran=params.get("tests_ran"),
        notes=params.get("notes"),
        declared_operations=params.get("declared_operations"),
    )


def tool_praxis_get_submission(params: dict) -> dict:
    return workflow_submission.get_submission(
        submission_id=params.get("submission_id"),
        job_label=params.get("job_label"),
    )


def tool_praxis_review_submission(params: dict) -> dict:
    return workflow_submission.review_submission(
        submission_id=params.get("submission_id"),
        job_label=params.get("job_label"),
        decision=params.get("decision"),
        summary=params.get("summary"),
        notes=params.get("notes"),
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_submit_code_change_candidate": (
        tool_praxis_submit_code_change_candidate,
        {
            "description": (
                "Submit a structured code-change candidate for the current workflow MCP session. "
                "The agent does not edit live source; it provides a small proposal payload plus "
                "source snapshots. Runtime validates the proposal and derives the patch artifact."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "bug_id": {"type": "string", "description": "Bug this candidate is intended to fix."},
                    "summary": {"type": "string", "description": "Optional reviewer-facing summary."},
                    "proposal_payload": {
                        "type": "object",
                        "properties": {
                            "intended_files": {"type": "array", "items": {"type": "string"}},
                            "rationale": {"type": "string"},
                            "edits": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "file": {"type": "string"},
                                        "action": {
                                            "type": "string",
                                            "enum": ["full_file_replace", "exact_block_replace"],
                                        },
                                        "new_content": {"type": "string"},
                                        "old_block": {"type": "string"},
                                        "new_block": {"type": "string"},
                                    },
                                    "required": ["file", "action"],
                                },
                            },
                            "verifier_ref": {"type": "string"},
                            "verifier_inputs": {"type": "object"},
                        },
                        "required": ["intended_files", "edits"],
                    },
                    "source_context_refs": {
                        "description": (
                            "Full source snapshots the candidate was authored against. "
                            "Accepted shapes: {path: content}, {files: [{path, content}]}, "
                            "or [{path, content}]."
                        )
                    },
                    "base_head_ref": {"type": "string"},
                    "review_routing": {
                        "type": "string",
                        "enum": ["auto_apply", "human_review"],
                        "default": "human_review",
                    },
                    "verifier_ref": {"type": "string"},
                    "verifier_inputs": {"type": "object"},
                    "notes": {"type": "string"},
                    "routing_decision_record": {"type": "object"},
                },
                "required": ["bug_id", "proposal_payload", "source_context_refs"],
            },
        },
    ),
    "praxis_submit_research_result": (
        tool_praxis_submit_research_result,
        {
            "description": (
                "Submit a sealed research result for the current workflow MCP session. "
                "The session token owns run_id, workflow_id, and job_label. This tool never accepts "
                "those ids as input and returns structured errors instead of stack traces."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "primary_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "result_kind": {
                        "type": "string",
                        "enum": ["research_result"],
                        "default": "research_result",
                    },
                    "tests_ran": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "notes": {"type": "string"},
                    "declared_operations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string"},
                                "action": {
                                    "type": "string",
                                    "enum": ["create", "update", "delete", "rename"],
                                },
                                "from_path": {"type": "string"},
                            },
                            "required": ["path", "action"],
                        },
                    },
                },
                "required": ["summary", "primary_paths", "result_kind"],
            },
        },
    ),
    "praxis_submit_artifact_bundle": (
        tool_praxis_submit_artifact_bundle,
        {
            "description": (
                "Submit a sealed artifact bundle result for the current workflow MCP session. "
                "The session token owns run_id, workflow_id, and job_label. This tool never accepts "
                "those ids as input and returns structured errors instead of stack traces."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "primary_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "result_kind": {
                        "type": "string",
                        "enum": ["artifact_bundle"],
                        "default": "artifact_bundle",
                    },
                    "tests_ran": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "notes": {"type": "string"},
                    "declared_operations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string"},
                                "action": {
                                    "type": "string",
                                    "enum": ["create", "update", "delete", "rename"],
                                },
                                "from_path": {"type": "string"},
                            },
                            "required": ["path", "action"],
                        },
                    },
                },
                "required": ["summary", "primary_paths", "result_kind"],
            },
        },
    ),
    "praxis_get_submission": (
        tool_praxis_get_submission,
        {
            "description": (
                "Read a sealed workflow submission within the current workflow MCP session. "
                "The session token owns run_id/workflow_id and the tool only accepts submission_id "
                "or job_label for the target submission."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "submission_id": {"type": "string"},
                    "job_label": {"type": "string"},
                },
                "required": [],
            },
        },
    ),
    "praxis_review_submission": (
        tool_praxis_review_submission,
        {
            "description": (
                "Review a sealed workflow submission within the current workflow MCP session. "
                "The session token owns run_id/workflow_id/job_label for the reviewer. The tool only "
                "accepts submission_id or job_label for the target submission."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "submission_id": {"type": "string"},
                    "job_label": {"type": "string"},
                    "decision": {
                        "type": "string",
                        "enum": ["approve", "request_changes", "reject"],
                    },
                    "summary": {"type": "string"},
                    "notes": {"type": "string"},
                },
                "required": ["decision", "summary"],
            },
        },
    ),
}
