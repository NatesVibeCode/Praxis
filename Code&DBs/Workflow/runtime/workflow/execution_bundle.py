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
from .decision_context import render_decision_pack
from .artifact_contracts import (
    normalize_acceptance_contract,
    normalize_authoring_contract,
    render_acceptance_contract,
    render_authoring_contract,
)


_BUCKET_MCP_TOOLS: dict[str, tuple[str, ...]] = {
    "general": ("praxis_query", "praxis_status_snapshot", "praxis_integration"),
    "build": ("praxis_query", "praxis_discover", "praxis_recall", "praxis_health", "praxis_integration"),
    "debug": ("praxis_query", "praxis_status_snapshot", "praxis_heal", "praxis_receipts", "praxis_health", "praxis_discover"),
    "review": ("praxis_query", "praxis_status_snapshot", "praxis_receipts", "praxis_bugs", "praxis_health"),
    "research": ("praxis_query", "praxis_research", "praxis_recall", "praxis_discover", "praxis_graph", "praxis_integration"),
    "analysis": ("praxis_query", "praxis_status_snapshot", "praxis_recall", "praxis_graph", "praxis_integration"),
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


def _execution_manifest_tool_authority(
    *,
    execution_manifest: Mapping[str, Any] | None,
    explicit_mcp_tools: Sequence[str],
    explicit_allowed_tools: Sequence[str],
) -> tuple[list[str], list[str], list[str]]:
    if not isinstance(execution_manifest, Mapping):
        return [], [], []
    allowlist = execution_manifest.get("tool_allowlist")
    if not isinstance(allowlist, Mapping):
        return [], [], []
    mcp_tools = [
        *_INTERNAL_WORKFLOW_MCP_TOOLS,
        *_string_list(allowlist.get("mcp_tools")),
        *_string_list(explicit_mcp_tools),
    ]
    adapter_tools = [
        *_string_list(allowlist.get("adapter_tools")),
        *_string_list(explicit_allowed_tools),
    ]
    verify_refs = _string_list(execution_manifest.get("verify_refs"))
    if verify_refs:
        mcp_tools.append("praxis_workflow_validate")
    catalog = get_tool_catalog()
    normalized_mcp_tools = [
        name
        for name in _dedupe_strings(canonical_tool_name(item) for item in mcp_tools if str(item).strip())
        if name in catalog
    ]
    return normalized_mcp_tools, _dedupe_strings(adapter_tools), _dedupe_strings(verify_refs)


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


_VERIFICATION_REQUIRED_TASK_TYPES = frozenset({
    "build", "implement", "code_generation", "code_edit",
    "refactor", "test", "wiring",
})

# Review/research/audit/debate produce written deliverables (research_result
# kind, sealed via praxis_submit_research_result). Their seal contract is
# "produce a tangible artifact in write_scope" — same auto-seal diff flow as
# build, but the proof shape is a markdown/JSON report rather than a code
# change. Without this, "the agent ran cleanly with no on-disk output" passes
# review jobs silently — operators get no link + content back, just an empty
# success receipt.
_DELIVERABLE_REQUIRED_TASK_TYPES = frozenset({
    "review", "research", "analysis", "audit", "debate",
})

_SUBMISSION_DEFAULT_REQUIRED_TASK_TYPES = (
    _VERIFICATION_REQUIRED_TASK_TYPES | _DELIVERABLE_REQUIRED_TASK_TYPES
)


def _default_submission_required(task_type: str) -> bool:
    """Mutating jobs must seal a submission; non-mutating packets remain opt-in."""
    return task_type.strip().lower() in _SUBMISSION_DEFAULT_REQUIRED_TASK_TYPES


def _completion_contract(
    *,
    task_type: str,
    bucket: str,
    submission_required: bool | None,
    downstream_labels: Sequence[str] | None,
    verify_refs: Sequence[str] = (),
    # Spec-level overrides from the job's `completion_contract` block.
    # When present, these win over the task_type/bucket-derived defaults —
    # spec authority comes first. Empty / None keeps the default.
    result_kind_override: str | None = None,
    submit_tool_names_override: Sequence[str] | None = None,
    verification_required_override: bool | None = None,
) -> dict[str, Any]:
    normalized_task_type = task_type.strip().lower()
    if submission_required is not None:
        normalized_submission_required = bool(submission_required)
    else:
        normalized_submission_required = _default_submission_required(normalized_task_type)
    if verification_required_override is not None:
        verification_required = bool(verification_required_override)
    else:
        verification_required = normalized_task_type in _VERIFICATION_REQUIRED_TASK_TYPES
    # Spec-level result_kind wins; fall back to task_type/bucket derivation.
    normalized_override = str(result_kind_override or "").strip().lower()
    if normalized_override and normalized_override in _SUBMISSION_RESULT_KIND_TO_TOOL:
        result_kind = normalized_override
    else:
        result_kind = _submission_result_kind(task_type=normalized_task_type, bucket=bucket)
    # Spec-level submit_tool_names wins; else derive from the (possibly-
    # overridden) result_kind. The read tool is always appended so agents
    # can inspect their own submissions during multi-attempt work.
    explicit_submit_tools = _dedupe_strings(
        [str(name).strip() for name in (submit_tool_names_override or []) if str(name).strip()]
    )
    if normalized_submission_required:
        if explicit_submit_tools:
            derived_submit_tools = list(explicit_submit_tools)
            if _SUBMISSION_READ_TOOL not in derived_submit_tools:
                derived_submit_tools.append(_SUBMISSION_READ_TOOL)
            submit_tool_names = derived_submit_tools
        else:
            submit_tool_names = [_SUBMISSION_RESULT_KIND_TO_TOOL[result_kind], _SUBMISSION_READ_TOOL]
    else:
        submit_tool_names = []
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
    entrypoint_tool: str | None = None,
) -> dict[str, Any]:
    normalized_entrypoint = str(entrypoint_tool or "praxis_query").strip() or "praxis_query"
    scope_hint = ", ".join(list(write_scope)[:3] or list(read_scope)[:3]) or "the declared scope"
    question = {
        "build": f"What exists already, what is the current status, and what repo surfaces matter for job {label} touching {scope_hint}?",
        "debug": f"What is failing, what changed recently, and what evidence matters for job {label} around {scope_hint}?",
        "review": f"What should be checked, what risks already exist, and what evidence matters for review job {label} in {scope_hint}?",
        "research": f"What prior findings, artifacts, and known surfaces matter for research job {label} about {scope_hint}?",
        "analysis": f"What status, prior findings, and graph context matter for analysis job {label} around {scope_hint}?",
        "architecture": f"What existing contracts, workflows, and graph surfaces matter for architecture job {label} touching {scope_hint}?",
    }.get(bucket, f"What status and existing system context matter for job {label} in {scope_hint}?")
    # The normalized_entrypoint is an MCP tool name like `praxis_query`. In
    # the sandbox, those tools are callable via the uniform `praxis` CLI
    # (drop the `praxis_` prefix): `praxis_query` → `praxis query "..."`.
    # This replaces per-provider MCP client config (.claude.json,
    # .codex/config.toml, etc.) with a single shell-callable binary the
    # agent can invoke from any CLI's native Bash/shell tool.
    cli_subcommand = normalized_entrypoint.removeprefix("praxis_") or "query"
    return {
        "entrypoint_tool": normalized_entrypoint,
        "suggested_question": question,
        "notes": [
            f"Call tools via the `praxis` shell command. Start with `praxis {cli_subcommand} \"...\"` before broader tool use.",
            "Every tool in mcp_tool_names is callable as `praxis <subcommand> ...`. Run `praxis --help` for the full list.",
            "At the end of mutating jobs, submit results via `praxis submit_code_change` or `praxis submit_artifact_bundle` — the sealed submission is the authoritative deliverable.",
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
    approval_required: bool | None = None,
    approval_question: str | None = None,
    context_sections: Sequence[Mapping[str, Any]] | None = None,
    run_id: str | None = None,
    workflow_id: str | None = None,
    sandbox_profile_ref: str | None = None,
    sandbox_profile: Mapping[str, Any] | None = None,
    submission_required: bool | None = None,
    # Optional spec-level overrides for the completion contract. These
    # propagate the job's `completion_contract.result_kind` and
    # `completion_contract.submit_tool_names` through the bundle so a
    # build-bucket job can intentionally seal as artifact_bundle
    # (bypassing the baseline requirement) without changing its
    # task_type. When None, task_type/bucket defaults apply.
    result_kind: str | None = None,
    submit_tool_names: Sequence[str] | None = None,
    verification_required: bool | None = None,
    downstream_labels: Sequence[str] | None = None,
    output_schema: Mapping[str, Any] | None = None,
    authoring_contract: Mapping[str, Any] | None = None,
    acceptance_contract: Mapping[str, Any] | None = None,
    decision_pack: Mapping[str, Any] | None = None,
    execution_manifest: Mapping[str, Any] | None = None,
    require_manifest_authority: bool = False,
) -> dict[str, Any]:
    manifest_mcp_tools, manifest_allowed_tools, manifest_verify_refs = _execution_manifest_tool_authority(
        execution_manifest=execution_manifest,
        explicit_mcp_tools=_string_list(explicit_mcp_tools),
        explicit_allowed_tools=_string_list(allowed_tools),
    )
    if require_manifest_authority and not (manifest_mcp_tools or manifest_allowed_tools or manifest_verify_refs):
        raise ValueError(
            "builder-originated workflow execution requires ExecutionManifest authority; "
            "prompt/task bucket fallback is not permitted",
        )
    normalized_task_type = (
        str(task_type or "").strip()
        or (
            "approved_execution"
            if require_manifest_authority or manifest_mcp_tools or manifest_allowed_tools
            else infer_task_type(prompt, label=job_label, require_authority=True)
        )
    )
    normalized_capabilities = _dedupe_strings(_string_list(capabilities))
    normalized_verify_refs = (
        manifest_verify_refs
        if manifest_verify_refs
        else _dedupe_strings(_string_list(verify_refs))
    )
    bucket = _bucket_from_task(
        task_type=normalized_task_type,
        capabilities=normalized_capabilities,
        verify_refs=normalized_verify_refs,
    )
    normalized_write_scope = _dedupe_strings(_string_list(write_scope))
    # A non-empty write_scope is the sandbox isolation boundary required by
    # SandboxRuntime — it is NOT itself evidence of mutation intent. Only
    # force the submission/verification contract when the task_type is one
    # that actually mutates code (build/implement/refactor/test/wiring/
    # code_generation/code_edit). Audit, review, research, debate, etc.
    # carry a write_scope for isolation but produce no on-disk diff, so
    # forcing them through the seal-gate makes every such job fail with
    # workflow_submission.required_missing despite running successfully.
    mutation_requires_submission = (
        bool(normalized_write_scope)
        and _default_submission_required(normalized_task_type)
    )
    effective_submission_required = True if mutation_requires_submission else submission_required
    effective_verification_required = (
        True if mutation_requires_submission else verification_required
    )
    effective_result_kind = (
        "code_change"
        if mutation_requires_submission and not str(result_kind or "").strip()
        else result_kind
    )
    profile = resolve_profile(_profile_task_type(normalized_task_type))
    normalized_allowed_tools = (
        manifest_allowed_tools
        if manifest_allowed_tools
        else _dedupe_strings(
            [
                canonical_tool_name(tool)
                for tool in merge_allowed_tools(
                    profile.allowed_tools,
                    _string_list(allowed_tools),
                )
            ]
        )
    )
    completion_contract = _completion_contract(
        task_type=normalized_task_type,
        bucket=bucket,
        submission_required=effective_submission_required,
        downstream_labels=downstream_labels,
        verify_refs=normalized_verify_refs,
        result_kind_override=effective_result_kind,
        submit_tool_names_override=submit_tool_names,
        verification_required_override=effective_verification_required,
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
    normalized_decision_pack = (
        json.loads(json.dumps(dict(decision_pack), sort_keys=True, default=str))
        if isinstance(decision_pack, Mapping) and decision_pack
        else None
    )
    normalized_mcp_tools = (
        manifest_mcp_tools
        if manifest_mcp_tools
        else _select_mcp_tool_names(
            bucket=bucket,
            verify_refs=normalized_verify_refs,
            explicit_mcp_tools=[
                *_string_list(explicit_mcp_tools),
                *_string_list(completion_contract.get("submit_tool_names")),
                *_string_list(completion_contract.get("review_tool_names")),
            ],
        )
    )
    orient_entrypoint = next(
        (
            tool_name
            for tool_name in (
                "praxis_query",
                "praxis_status_snapshot",
                "praxis_integration",
                "praxis_workflow_validate",
                "praxis_get_submission",
                "praxis_review_submission",
            )
            if tool_name in normalized_mcp_tools
        ),
        normalized_mcp_tools[0] if normalized_mcp_tools else "praxis_query",
    )
    normalized_skill_refs = _dedupe_strings(
        [*_BUCKET_SKILLS.get(bucket, _BUCKET_SKILLS["general"]), *_string_list(explicit_skill_refs)],
    )
    normalized_declared_read_scope = _dedupe_strings(_string_list(declared_read_scope))
    normalized_resolved_read_scope = _dedupe_strings(_string_list(resolved_read_scope))
    normalized_blast_radius = _dedupe_strings(_string_list(blast_radius))
    normalized_test_scope = _dedupe_strings(_string_list(test_scope))
    normalized_approval_question = str(approval_question or "").strip()
    section_names = [
        str(section.get("name") or "").strip()
        for section in _dict_list(context_sections)
        if str(section.get("name") or "").strip()
    ]
    return {
        "bundle_version": 1,
        "run_id": str(run_id or "").strip() or None,
        "workflow_id": str(workflow_id or "").strip() or None,
        "sandbox_profile_ref": str(sandbox_profile_ref or "").strip() or None,
        "sandbox_profile": (
            json.loads(json.dumps(dict(sandbox_profile), sort_keys=True, default=str))
            if isinstance(sandbox_profile, Mapping) and sandbox_profile
            else None
        ),
        "job_label": job_label,
        "task_type": normalized_task_type,
        "tool_bucket": bucket,
        "allowed_tools": normalized_allowed_tools,
        "capabilities": normalized_capabilities,
        "mcp_tools": _mcp_tool_entries(normalized_mcp_tools),
        "mcp_tool_names": normalized_mcp_tools,
        "skill_refs": normalized_skill_refs,
        "approval_required": bool(approval_required),
        "approval_question": normalized_approval_question or None,
        "decision_pack": normalized_decision_pack,
        "completion_contract": completion_contract,
        "authoring_contract": normalized_authoring_contract,
        "acceptance_contract": normalized_acceptance_contract,
        "orient": _orient_hint(
            bucket=bucket,
            label=job_label,
            write_scope=normalized_write_scope,
            read_scope=normalized_resolved_read_scope or normalized_declared_read_scope,
            entrypoint_tool=orient_entrypoint,
        ),
        "execution_manifest_ref": (
            str(execution_manifest.get("execution_manifest_ref") or "").strip()
            if isinstance(execution_manifest, Mapping)
            else None
        ),
        "approved_bundle_refs": (
            _dedupe_strings(_string_list(execution_manifest.get("approved_bundle_refs")))
            if isinstance(execution_manifest, Mapping)
            else []
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
    sandbox_profile_ref = str(bundle.get("sandbox_profile_ref") or "").strip()
    if sandbox_profile_ref:
        parts.append(f"sandbox_profile_ref: {sandbox_profile_ref}")

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

    if bundle.get("approval_required"):
        approval_question = str(bundle.get("approval_question") or "").strip()
        parts.append(
            "\n** APPROVAL REQUIRED **\n"
            "This job pauses for human approval before execution.\n"
            + (f"Question: {approval_question}\n" if approval_question else "")
        )

    rendered_decision_pack = render_decision_pack(bundle.get("decision_pack"))
    if rendered_decision_pack:
        parts.append("\n" + rendered_decision_pack)

    completion_contract = bundle.get("completion_contract")
    if isinstance(completion_contract, Mapping) and completion_contract:
        submit_tools = _string_list(completion_contract.get("submit_tool_names"))
        if completion_contract.get("submission_required") and submit_tools:
            tool_name = submit_tools[0]
            # Shell-shape instruction matches the uniform sandbox tool surface:
            # every sandbox image has `/usr/local/bin/praxis` that POSTs to the
            # MCP bridge. The agent invokes the tool via its native shell/Bash
            # tool — no per-provider MCP client config. Strip the `praxis_`
            # prefix for the `praxis <verb>` shell form.
            shell_verb = tool_name.removeprefix("praxis_") or tool_name
            result_kind = str(completion_contract.get("result_kind") or "").strip()
            result_kind_arg = f" --result-kind {result_kind}" if result_kind else ""
            parts.append(
                f"\n** SUBMISSION REQUIRED **\n"
                f"When you have completed your task, you MUST seal your work with the "
                f"{tool_name} tool. Your output is NOT recorded unless you submit it — "
                f"describing the work in stdout does not count.\n\n"
                f"Invoke from your shell/Bash tool:\n"
                f"  praxis {shell_verb} --summary \"<one-sentence description>\" "
                f"--primary-paths '[\"<path1>\", \"<path2>\"]'{result_kind_arg} "
                f"[--notes \"<evidence/rationale>\"]\n\n"
                f"The `praxis` binary is preinstalled at /usr/local/bin/praxis and reads "
                f"its credentials from PRAXIS_WORKFLOW_MCP_URL + PRAXIS_WORKFLOW_MCP_TOKEN "
                f"which are already set in your environment. Alternatively: "
                f"`praxis workflow tools call {tool_name} --input-json '{{\"summary\":\"...\", "
                f"\"primary_paths\":[\"...\"], \"result_kind\":\"{result_kind or '<kind>'}\"}}'`.\n"
            )
        else:
            parts.append(
                "completion_contract: "
                + json.dumps(dict(completion_contract), sort_keys=True, default=str)
            )
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
