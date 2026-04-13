"""Execution control bundles for workflow jobs.

These bundles are the explicit authority handed to workflow model lanes.
They tell a model what control surfaces it is allowed to use, how to orient
itself, and what read/write boundary applies to the job.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from adapters.task_profiles import infer_task_type, merge_allowed_tools, resolve_profile
from surfaces.mcp.catalog import canonical_tool_name, get_tool_catalog
from .artifact_contracts import (
    normalize_acceptance_contract,
    normalize_authoring_contract,
    render_acceptance_contract,
    render_authoring_contract,
)


_BUCKET_MCP_TOOLS: dict[str, tuple[str, ...]] = {
    "general": ("praxis_query", "praxis_status", "praxis_integration"),
    "build": ("praxis_query", "praxis_discover", "praxis_recall", "praxis_health", "praxis_integration"),
    "debug": ("praxis_query", "praxis_status", "praxis_heal", "praxis_receipts", "praxis_health", "praxis_discover"),
    "review": ("praxis_query", "praxis_status", "praxis_receipts", "praxis_bugs", "praxis_health"),
    "research": ("praxis_query", "praxis_research", "praxis_recall", "praxis_discover", "praxis_graph", "praxis_integration"),
    "analysis": ("praxis_query", "praxis_status", "praxis_recall", "praxis_graph", "praxis_integration"),
    "architecture": ("praxis_query", "praxis_discover", "praxis_graph", "praxis_recall", "praxis_integration"),
}

_BUCKET_SKILLS: dict[str, tuple[str, ...]] = {
    "general": ("cli-summary",),
    "build": ("workflow", "cli-summary", "cli-discover"),
    "debug": ("cli-debug", "cli-quality", "cli-summary"),
    "review": ("review", "cli-quality", "cli-summary"),
    "research": ("cli-research", "cli-discover", "cli-summary"),
    "analysis": ("cli-summary", "cli-research"),
    "architecture": ("workflow", "review", "cli-summary"),
}

_TASK_PROFILE_ALIASES: dict[str, str] = {
    "build": "code_generation",
    "implement": "code_generation",
    "code_generation": "code_generation",
    "code_edit": "code_edit",
    "edit": "code_edit",
    "debug": "debug",
    "review": "code_review",
    "code_review": "code_review",
    "research": "research",
    "analysis": "analysis",
    "architecture": "architecture",
}

_INTERNAL_WORKFLOW_MCP_TOOLS: tuple[str, ...] = ("praxis_context_shard",)
_SUBMISSION_REVIEW_TASK_TYPES = frozenset(
    {
        "review",
        "code_review",
        "reviewer",
        "verifier",
        "orchestrator",
        "publish",
        "publish_policy",
        "ops_review",
    }
)
_SUBMISSION_READ_TOOL = "praxis_get_submission"
_SUBMISSION_REVIEW_TOOL = "praxis_review_submission"
_SUBMISSION_RESULT_KIND_TO_TOOL = {
    "code_change": "praxis_submit_code_change",
    "research_result": "praxis_submit_research_result",
    "artifact_bundle": "praxis_submit_artifact_bundle",
}


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    items: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _bucket_from_task(
    *,
    task_type: str,
    capabilities: Sequence[str],
    verify_refs: Sequence[str],
) -> str:
    lowered_caps = {str(item).strip().lower() for item in capabilities if str(item).strip()}
    lowered_task_type = task_type.strip().lower()
    if lowered_task_type in {"debug"}:
        return "debug"
    if lowered_task_type in {"review", "code_review"}:
        return "review"
    if lowered_task_type in {"research"} or any(cap.startswith("research/") for cap in lowered_caps):
        return "research"
    if lowered_task_type in {"analysis"}:
        return "analysis"
    if lowered_task_type in {"architecture"}:
        return "architecture"
    if lowered_task_type in {"build", "implement", "code_generation", "code_edit"}:
        return "build"
    if verify_refs:
        return "build"
    return "general"


def _profile_task_type(task_type: str) -> str:
    lowered = task_type.strip().lower()
    return _TASK_PROFILE_ALIASES.get(lowered, lowered or "general")


def _select_mcp_tool_names(
    *,
    bucket: str,
    verify_refs: Sequence[str],
    explicit_mcp_tools: Sequence[str],
) -> list[str]:
    names = [*_INTERNAL_WORKFLOW_MCP_TOOLS, *_BUCKET_MCP_TOOLS.get(bucket, _BUCKET_MCP_TOOLS["general"])]
    if verify_refs:
        names.append("praxis_workflow_validate")
    names.extend(canonical_tool_name(name) for name in explicit_mcp_tools if str(name).strip())
    catalog = get_tool_catalog()
    return [name for name in _dedupe_strings(names) if name in catalog]


def _mcp_tool_entries(tool_names: Sequence[str]) -> list[dict[str, Any]]:
    catalog = get_tool_catalog()
    entries: list[dict[str, Any]] = []
    for tool_name in tool_names:
        definition = catalog.get(tool_name)
        if definition is None:
            continue
        entries.append(
            {
                "name": tool_name,
                "default_action": definition.default_action,
                "description": definition.description,
                "inputs": list(definition.inputs),
                "required_args": list(definition.required_args),
            }
        )
    return entries


def _submission_result_kind(*, task_type: str, bucket: str) -> str:
    lowered = task_type.strip().lower()
    if lowered in {"research", "analysis", "architecture", "debate"}:
        return "research_result"
    if bucket == "build":
        return "code_change"
    return "artifact_bundle"


_SUBMISSION_DEFAULT_REQUIRED_TASK_TYPES = frozenset({
    "debate", "research", "analysis", "architecture",
})

_VERIFICATION_REQUIRED_TASK_TYPES = frozenset({
    "build", "implement", "code_generation", "code_edit",
    "refactor", "test", "wiring",
})


def _default_submission_required(task_type: str) -> bool:
    """Auto-require submission for output-oriented task types (debate, research,
    analysis) so CLI agents always seal their output.  Other types remain
    opt-in to avoid scope_violation failures when write_scope is omitted."""
    return task_type.strip().lower() in _SUBMISSION_DEFAULT_REQUIRED_TASK_TYPES


def _completion_contract(
    *,
    task_type: str,
    bucket: str,
    submission_required: bool | None,
    downstream_labels: Sequence[str] | None,
    verify_refs: Sequence[str] = (),
) -> dict[str, Any]:
    normalized_task_type = task_type.strip().lower()
    # For output-critical task types (debate, research, etc.), always require
    # submission regardless of spec override — an explicit False causes
    # permanent content loss (BUG-91630F20).
    if normalized_task_type in _SUBMISSION_DEFAULT_REQUIRED_TASK_TYPES:
        normalized_submission_required = True
    elif submission_required is not None:
        normalized_submission_required = bool(submission_required)
    else:
        normalized_submission_required = _default_submission_required(normalized_task_type)
    # verification_required: code task types must pass verify_refs to succeed.
    verification_required = normalized_task_type in _VERIFICATION_REQUIRED_TASK_TYPES
    result_kind = _submission_result_kind(task_type=normalized_task_type, bucket=bucket)
    submit_tool_names = (
        [_SUBMISSION_RESULT_KIND_TO_TOOL[result_kind], _SUBMISSION_READ_TOOL]
        if normalized_submission_required
        else []
    )
    review_tool_names = (
        [_SUBMISSION_READ_TOOL, _SUBMISSION_REVIEW_TOOL]
        if normalized_task_type in _SUBMISSION_REVIEW_TASK_TYPES
        else []
    )
    return {
        "submission_required": normalized_submission_required,
        "verification_required": verification_required,
        "result_kind": result_kind,
        "submit_tool_names": submit_tool_names,
        "review_tool_names": review_tool_names,
        "downstream_labels": _dedupe_strings(_string_list(downstream_labels)),
    }


def _orient_hint(
    *,
    bucket: str,
    label: str,
    write_scope: Sequence[str],
    read_scope: Sequence[str],
) -> dict[str, Any]:
    scope_hint = ", ".join(list(write_scope)[:3] or list(read_scope)[:3]) or "the declared scope"
    question = {
        "build": f"What exists already, what is the current status, and what repo surfaces matter for job {label} touching {scope_hint}?",
        "debug": f"What is failing, what changed recently, and what evidence matters for job {label} around {scope_hint}?",
        "review": f"What should be checked, what risks already exist, and what evidence matters for review job {label} in {scope_hint}?",
        "research": f"What prior findings, artifacts, and known surfaces matter for research job {label} about {scope_hint}?",
        "analysis": f"What status, prior findings, and graph context matter for analysis job {label} around {scope_hint}?",
        "architecture": f"What existing contracts, workflows, and graph surfaces matter for architecture job {label} touching {scope_hint}?",
    }.get(bucket, f"What status and existing system context matter for job {label} in {scope_hint}?")
    return {
        "entrypoint_tool": "praxis_query",
        "suggested_question": question,
        "notes": [
            "Start with praxis_query before broader tool use.",
            "Use only the admitted MCP tools and adapter tools listed in this bundle.",
            "Stay inside the declared read/write boundary and verification refs.",
        ],
    }


def build_execution_bundle(
    *,
    job_label: str,
    prompt: str,
    task_type: str | None,
    capabilities: Sequence[str] | None = None,
    allowed_tools: Sequence[str] | None = None,
    explicit_mcp_tools: Sequence[str] | None = None,
    explicit_skill_refs: Sequence[str] | None = None,
    write_scope: Sequence[str] | None = None,
    declared_read_scope: Sequence[str] | None = None,
    resolved_read_scope: Sequence[str] | None = None,
    blast_radius: Sequence[str] | None = None,
    test_scope: Sequence[str] | None = None,
    verify_refs: Sequence[str] | None = None,
    context_sections: Sequence[Mapping[str, Any]] | None = None,
    run_id: str | None = None,
    workflow_id: str | None = None,
    submission_required: bool | None = None,
    downstream_labels: Sequence[str] | None = None,
    output_schema: Mapping[str, Any] | None = None,
    authoring_contract: Mapping[str, Any] | None = None,
    acceptance_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_task_type = str(task_type or "").strip() or infer_task_type(prompt, label=job_label)
    normalized_capabilities = _dedupe_strings(_string_list(capabilities))
    normalized_verify_refs = _dedupe_strings(_string_list(verify_refs))
    bucket = _bucket_from_task(
        task_type=normalized_task_type,
        capabilities=normalized_capabilities,
        verify_refs=normalized_verify_refs,
    )
    profile = resolve_profile(_profile_task_type(normalized_task_type))
    normalized_allowed_tools = _dedupe_strings(
        [canonical_tool_name(tool) for tool in merge_allowed_tools(profile.allowed_tools, _string_list(allowed_tools))]
    )
    completion_contract = _completion_contract(
        task_type=normalized_task_type,
        bucket=bucket,
        submission_required=submission_required,
        downstream_labels=downstream_labels,
        verify_refs=normalized_verify_refs,
    )
    normalized_authoring_contract = normalize_authoring_contract(
        output_schema=output_schema,
        authoring_contract=authoring_contract,
        acceptance_contract=acceptance_contract,
    )
    normalized_acceptance_contract = normalize_acceptance_contract(
        output_schema=output_schema,
        authoring_contract=authoring_contract,
        acceptance_contract=acceptance_contract,
        verify_refs=normalized_verify_refs,
    )
    normalized_mcp_tools = _select_mcp_tool_names(
        bucket=bucket,
        verify_refs=normalized_verify_refs,
        explicit_mcp_tools=[
            *_string_list(explicit_mcp_tools),
            *_string_list(completion_contract.get("submit_tool_names")),
            *_string_list(completion_contract.get("review_tool_names")),
        ],
    )
    normalized_skill_refs = _dedupe_strings(
        [*_BUCKET_SKILLS.get(bucket, _BUCKET_SKILLS["general"]), *_string_list(explicit_skill_refs)],
    )
    normalized_write_scope = _dedupe_strings(_string_list(write_scope))
    normalized_declared_read_scope = _dedupe_strings(_string_list(declared_read_scope))
    normalized_resolved_read_scope = _dedupe_strings(_string_list(resolved_read_scope))
    normalized_blast_radius = _dedupe_strings(_string_list(blast_radius))
    normalized_test_scope = _dedupe_strings(_string_list(test_scope))
    section_names = [
        str(section.get("name") or "").strip()
        for section in _dict_list(context_sections)
        if str(section.get("name") or "").strip()
    ]
    return {
        "bundle_version": 1,
        "run_id": str(run_id or "").strip() or None,
        "workflow_id": str(workflow_id or "").strip() or None,
        "job_label": job_label,
        "task_type": normalized_task_type,
        "tool_bucket": bucket,
        "allowed_tools": normalized_allowed_tools,
        "capabilities": normalized_capabilities,
        "mcp_tools": _mcp_tool_entries(normalized_mcp_tools),
        "mcp_tool_names": normalized_mcp_tools,
        "skill_refs": normalized_skill_refs,
        "completion_contract": completion_contract,
        "authoring_contract": normalized_authoring_contract,
        "acceptance_contract": normalized_acceptance_contract,
        "orient": _orient_hint(
            bucket=bucket,
            label=job_label,
            write_scope=normalized_write_scope,
            read_scope=normalized_resolved_read_scope or normalized_declared_read_scope,
        ),
        "access_policy": {
            "workspace_mode": "docker_packet_only",
            "write_scope": normalized_write_scope,
            "declared_read_scope": normalized_declared_read_scope,
            "resolved_read_scope": normalized_resolved_read_scope,
            "blast_radius": normalized_blast_radius,
            "test_scope": normalized_test_scope,
            "verify_refs": normalized_verify_refs,
            "context_section_names": section_names,
        },
    }


def render_execution_bundle(bundle: Mapping[str, Any] | None) -> str:
    if not isinstance(bundle, Mapping) or not bundle:
        return ""

    def _render_list(name: str, values: object) -> list[str]:
        items = _string_list(values)
        if not items:
            return []
        return [f"{name}:\n" + "\n".join(f"- {item}" for item in items)]

    parts = ["--- EXECUTION CONTROL BUNDLE ---"]
    for field_name in ("job_label", "task_type", "tool_bucket"):
        value = str(bundle.get(field_name) or "").strip()
        if value:
            parts.append(f"{field_name}: {value}")

    allowed_tools = _string_list(bundle.get("allowed_tools"))
    if allowed_tools:
        parts.append(
            "tool_policy: "
            + json.dumps({"allowed_tools": allowed_tools, "mcp_tools": _string_list(bundle.get("mcp_tool_names"))})
        )

    orient = bundle.get("orient")
    if isinstance(orient, Mapping) and orient:
        rendered_orient = {
            "entrypoint_tool": str(orient.get("entrypoint_tool") or "").strip(),
            "suggested_question": str(orient.get("suggested_question") or "").strip(),
        }
        parts.append("orient: " + json.dumps(rendered_orient, default=str))

    completion_contract = bundle.get("completion_contract")
    if isinstance(completion_contract, Mapping) and completion_contract:
        submit_tools = _string_list(completion_contract.get("submit_tool_names"))
        if completion_contract.get("submission_required") and submit_tools:
            tool_name = submit_tools[0]
            parts.append(
                f"\n** SUBMISSION REQUIRED **\n"
                f"When you have completed your task, you MUST call the {tool_name} tool "
                f"with a summary of your work. Your output will not be recorded unless you submit it.\n"
            )
        else:
            parts.append("completion_contract: " + json.dumps(dict(completion_contract), default=str))
        if completion_contract.get("verification_required"):
            parts.append(
                "\n** VERIFICATION REQUIRED **\n"
                "This job requires all verify_refs to pass. The job will be marked FAILED "
                "if verification does not pass, regardless of other output.\n"
            )

    authoring_contract = bundle.get("authoring_contract")
    if isinstance(authoring_contract, Mapping) and authoring_contract:
        rendered_authoring = render_authoring_contract(authoring_contract)
        if rendered_authoring:
            parts.append("\n" + rendered_authoring)

    acceptance_contract = bundle.get("acceptance_contract")
    if isinstance(acceptance_contract, Mapping) and acceptance_contract:
        rendered_acceptance = render_acceptance_contract(acceptance_contract)
        if rendered_acceptance:
            parts.append("\n" + rendered_acceptance)

    parts.extend(_render_list("skill_refs", bundle.get("skill_refs")))

    # Scope lists (write_scope, read_scope, blast_radius, test_scope, verify_refs)
    # are already rendered in the execution context shard. Don't duplicate them here.

    parts.append("--- END EXECUTION CONTROL BUNDLE ---")
    return "\n".join(parts)


def inject_execution_bundle_into_messages(
    messages: Sequence[Mapping[str, Any]],
    *,
    execution_bundle: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    rendered_bundle = render_execution_bundle(execution_bundle)
    normalized_messages = [
        {
            "role": str(message.get("role") or "user"),
            "content": str(message.get("content") or ""),
        }
        for message in messages
        if isinstance(message, Mapping)
    ]
    if not rendered_bundle:
        return normalized_messages
    for message in normalized_messages:
        if message["role"].strip().lower() == "user":
            content = message["content"]
            message["content"] = f"{content}\n\n{rendered_bundle}" if content else rendered_bundle
            return normalized_messages
    normalized_messages.append({"role": "user", "content": rendered_bundle})
    return normalized_messages


__all__ = [
    "build_execution_bundle",
    "inject_execution_bundle_into_messages",
    "render_execution_bundle",
]
