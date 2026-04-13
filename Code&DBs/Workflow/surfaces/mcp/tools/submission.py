"""Tools: praxis_submit_code_change, praxis_submit_research_result, praxis_submit_artifact_bundle, praxis_get_submission, praxis_review_submission."""

from __future__ import annotations

from typing import Any

import surfaces.api.workflow_submission as workflow_submission


def tool_praxis_submit_code_change(params: dict) -> dict:
    return workflow_submission.submit_code_change(
        summary=params.get("summary"),
        primary_paths=params.get("primary_paths"),
        result_kind=params.get("result_kind"),
        tests_ran=params.get("tests_ran"),
        notes=params.get("notes"),
        declared_operations=params.get("declared_operations"),
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
    "praxis_submit_code_change": (
        tool_praxis_submit_code_change,
        {
            "description": (
                "Submit a sealed code-change result for the current workflow MCP session. "
                "The session token owns run_id, workflow_id, and job_label. This tool never accepts "
                "those ids as input and returns structured errors instead of stack traces."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string", "description": "One-line result summary."},
                    "primary_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Primary files or paths touched by the submission.",
                    },
                    "result_kind": {
                        "type": "string",
                        "enum": ["code_change"],
                        "default": "code_change",
                    },
                    "tests_ran": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional tests or checks that were run.",
                    },
                    "notes": {"type": "string", "description": "Optional reviewer-facing notes."},
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
