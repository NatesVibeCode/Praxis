"""Spec compiler: takes minimal intent and produces a fully-contexted workflow spec.

The compiler bridges the gap between high-level intent (description + files)
and the detailed WorkflowSpec format used by the workflow system.

It automatically:
- Generates prompts from stage templates
- Infers capabilities from description and stage
- Maps stages to tiers
- Auto-generates verify commands from write scope
- Computes context sections (future integration with scope_resolver)
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping
import uuid

from registry.provider_execution_registry import (
    default_model_for_provider,
    default_provider_slug,
    registered_providers,
    resolve_default_adapter_type,
    resolve_lane_policy,
)
from runtime.capability_catalog import (
    CapabilityCatalogError,
    select_capability_catalog_entries,
    sync_capability_catalog,
)
from runtime.compile_reuse import stable_hash
from runtime.native_authority import default_native_authority_refs
from runtime.definition_compile_kernel import build_definition as build_definition_kernel
from runtime.verification import sync_verify_refs


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_STAGES = frozenset(("build", "fix", "review", "test", "research"))

_AGENT_STAGE_ROUTE_ALIASES: dict[str, str] = {
    "fix": "build",
}

_REPO_PATH_PATTERN = re.compile(
    r"(?:Code&DBs/Workflow|scripts|config|policy|Skills|README\.md|SETUP\.md)"
    r"[A-Za-z0-9_./&@+=-]*"
)

# Stage → tier mapping
_STAGE_TO_TIER: dict[str, str] = {
    "research": "economy",
    "review": "mid",
    "build": "mid",
    "fix": "mid",
    "test": "economy",
}

# Stage → prompt template
#
# 2026-04-27: review/research/audit prompts now demand a STRUCTURED JSON
# deliverable so the seal records (analyzed_paths, findings, conclusions)
# rather than free-form prose. This mirrors the shape of a research/audit
# report: "here's what I looked at, here's what I found, here's my
# conclusion." Auto-seal captures the JSON file as the artifact_ref so
# downstream consumers can parse the structured fields back.
_REVIEW_DELIVERABLE_INSTRUCTIONS = (
    "\n\n---\nDELIVERABLE CONTRACT (NON-NEGOTIABLE)\n"
    "Your job is incomplete until you write a single structured JSON file\n"
    "to the path below. The seal gate captures whatever lands in write_scope\n"
    "as your submission. No prose-only output, no chat-only output, no\n"
    "stdout-only verbal seal. A real on-disk JSON or no submission.\n"
    "\n"
    "Path: {write_scope}/{step_deliverable}.analysis.json\n"
    "\n"
    "Pick ONE of three completion_status values and follow that mode's\n"
    "required fields. Self-report honestly — if you cannot complete the spec\n"
    "as written, say so explicitly with diverged or declined; the operator\n"
    "would rather read a structured 'I could not do X because Y' than try\n"
    "to decipher silence.\n"
    "\n"
    "Mode 1 — completion_status=\"completed\" (you did the work as specified):\n"
    "{{\n"
    '  "completion_status": "completed",\n'
    '  "result_kind": "analysis_result",\n'
    '  "analyzed_paths": ["<repo-relative path or DB ref you inspected>", ...],\n'
    '  "findings": [\n'
    '    {{"observation": "<concrete, specific fact — name files, lines, '
    "fields, or behaviors>\", \"severity\": \"info|warning|critical\", "
    '"evidence_path": "<repo-relative path that proves this finding>"}}\n'
    "  ],\n"
    '  "conclusions": "<2-3 sentence paragraph>",\n'
    '  "recommendations": [\n'
    '    {{"action": "<concrete next step>", "rationale": "<why>"}}\n'
    "  ],\n"
    '  "links": [{{"label": "<label>", "ref": "<bug id, decision ref, URL>"}}]\n'
    "}}\n"
    "\n"
    "Mode 2 — completion_status=\"diverged\" (you completed, but in a\n"
    "different shape than the spec asked for — e.g. you found the spec\n"
    "was based on a wrong premise, or a different deliverable was more\n"
    "useful):\n"
    "{{\n"
    '  "completion_status": "diverged",\n'
    '  "result_kind": "analysis_result",\n'
    '  "divergence_reason": "<2-3 sentences: what the spec asked for, what '
    "you actually did, and why the divergence was the right call>\",\n"
    '  "analyzed_paths": [...],\n'
    '  "findings": [...],          // structure same as Mode 1\n'
    '  "conclusions": "...",\n'
    '  "recommendations": [...],\n'
    '  "links": [...]\n'
    "}}\n"
    "\n"
    "Mode 3 — completion_status=\"declined\" (you genuinely cannot complete\n"
    "this — missing context, blocked by an authority gap, the task as\n"
    "written is incoherent, etc.):\n"
    "{{\n"
    '  "completion_status": "declined",\n'
    '  "result_kind": "analysis_declined",\n'
    '  "decline_reason": "<2-3 sentences: what you tried, what blocked you, '
    "and what the operator would need to do to unblock>\",\n"
    '  "analyzed_paths": [...],     // optional but include any path you did read\n'
    '  "links": [{{"label": "<label>", "ref": "<bug id you filed for the blocker>"}}]\n'
    "}}\n"
    "\n"
    "Rules for Mode 1 / Mode 2 (the seal gate trusts your structure but the\n"
    "operator will read it; soft-pass costs you trust):\n"
    "- analyzed_paths MUST list at least 3 distinct repo paths or DB refs.\n"
    "- findings MUST contain at least 2 entries with concrete evidence_path.\n"
    "  Generic 'looks fine' / 'seems ok' are not findings.\n"
    "- conclusions MUST be a 2-3 sentence paragraph.\n"
    "- recommendations MUST contain at least 1 concrete next action.\n"
    "\n"
    "Rule for Mode 3:\n"
    "- decline_reason MUST name a specific blocker (missing tool, missing\n"
    "  authority row, conflicting policy, ambiguous spec) — not 'I could\n"
    "  not figure it out'. File a bug for the blocker if one fits.\n"
    "\n"
    "The file MUST be valid JSON parseable by json.loads.\n"
)

_STAGE_TEMPLATES: dict[str, str] = {
    "build": "Implement the following in {write_scope}:\n\n{description}",
    "fix": "Fix the following issue in {write_scope}:\n\n{description}",
    "review": (
        "Review {write_scope} for the following:\n\n{description}"
        + _REVIEW_DELIVERABLE_INSTRUCTIONS
    ),
    "test": "Write tests for {write_scope}:\n\n{description}",
    "research": (
        "Research and report on the following:\n\n{description}"
        + _REVIEW_DELIVERABLE_INSTRUCTIONS
    ),
}

def _default_workspace_ref(conn=None) -> str:
    return default_native_authority_refs(conn)[0]


def _default_runtime_profile_ref(conn=None) -> str:
    return default_native_authority_refs(conn)[1]


def _default_provider_slug() -> str:
    return default_provider_slug()


def _default_llm_adapter() -> str:
    return resolve_default_adapter_type()


# ---------------------------------------------------------------------------
# Intent dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Intent:
    """Minimal intent input to the compiler."""

    description: str
    write: list[str]
    stage: str
    read: list[str] | None = None
    label: str | None = None
    timeout: int = 300
    max_tokens: int = 4096
    temperature: float = 0.0


# ---------------------------------------------------------------------------
# Compiled spec output
# ---------------------------------------------------------------------------

@dataclass
class CompiledSpec:
    """Output of spec compilation."""

    prompt: str
    scope_write: list[str]
    scope_read: list[str] | None = None
    context_sections: list[dict[str, str]] | None = None
    capabilities: list[str] | None = None
    tier: str | None = None
    label: str | None = None
    task_type: str | None = None
    verify_refs: list[str] | None = None
    timeout: int = 300
    max_tokens: int = 4096
    temperature: float = 0.0
    provider_slug: str | None = None
    model_slug: str | None = None
    adapter_type: str | None = None
    workspace_ref: str | None = None
    runtime_profile_ref: str | None = None
    max_retries: int = 0
    definition_graph: dict[str, Any] | None = None
    definition_revision: str | None = None
    compiled_prose: str | None = None
    narrative_blocks: list[dict[str, Any]] | None = None
    draft_flow: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict, dropping None values."""
        result = asdict(self)
        return {k: v for k, v in result.items() if v is not None}

    def to_dispatch_spec_dict(self) -> dict[str, Any]:
        """Convert to a WorkflowSpec-compatible dict."""
        spec_dict = self.to_dict()
        # Remove fields that WorkflowSpec doesn't use
        for key in list(spec_dict.keys()):
            if key.startswith("_"):
                spec_dict.pop(key, None)
        for key in ("definition_graph", "compiled_prose", "narrative_blocks", "draft_flow"):
            spec_dict.pop(key, None)
        return spec_dict


@dataclass(frozen=True)
class PromptLaunchSpec:
    """Canonical inline workflow.submit payload for prompt-backed launches."""

    name: str
    workflow_id: str
    phase: str
    graph_runtime_submit: bool
    jobs: list[dict[str, Any]]
    workspace_ref: str | None = None
    runtime_profile_ref: str | None = None
    definition_revision: str | None = None
    plan_revision: str | None = None
    packet_provenance: dict[str, Any] | None = None

    def to_inline_spec_dict(self) -> dict[str, Any]:
        spec_dict = {
            "name": self.name,
            "workflow_id": self.workflow_id,
            "phase": self.phase,
            "graph_runtime_submit": self.graph_runtime_submit,
            "jobs": self.jobs,
        }
        if self.workspace_ref is not None:
            spec_dict["workspace_ref"] = self.workspace_ref
        if self.runtime_profile_ref is not None:
            spec_dict["runtime_profile_ref"] = self.runtime_profile_ref
        if self.definition_revision is not None:
            spec_dict["definition_revision"] = self.definition_revision
        if self.plan_revision is not None:
            spec_dict["plan_revision"] = self.plan_revision
        if self.packet_provenance is not None:
            spec_dict["packet_provenance"] = self.packet_provenance
        return spec_dict


# ---------------------------------------------------------------------------
# Compiler logic
# ---------------------------------------------------------------------------

def _validate_intent(intent: Intent) -> tuple[bool, list[str]]:
    """Validate an Intent. Returns (is_valid, list_of_errors)."""
    errors: list[str] = []

    if not intent.description or not intent.description.strip():
        errors.append("description must be non-empty")

    if not intent.write:
        errors.append("write must have at least one file")

    if intent.stage not in VALID_STAGES:
        errors.append(f"stage must be one of {sorted(VALID_STAGES)}, got {intent.stage!r}")

    if intent.timeout <= 0:
        errors.append("timeout must be positive")

    if intent.max_tokens <= 0:
        errors.append("max_tokens must be positive")

    if not (0.0 <= intent.temperature <= 2.0):
        errors.append("temperature must be between 0.0 and 2.0")

    return len(errors) == 0, errors


def _generate_label(stage: str, description: str) -> str:
    """Generate a label from stage and description."""
    # Extract first 3-5 words from description
    words = re.split(r'\s+', description.strip())[:4]
    slug = '-'.join(w.lower() for w in words if w.isalnum() or w == '-')
    slug = re.sub(r'-+', '-', slug).strip('-')
    return f"{stage}:{slug}"


def _agent_route_stage(stage: str) -> str:
    """Map prompt stages onto admitted route lanes."""
    return _AGENT_STAGE_ROUTE_ALIASES.get(stage.strip().lower(), stage)


def _clean_repo_path_candidate(raw: str) -> str | None:
    path = raw.strip().strip("`'\"[](){}<>,;.")
    if not path or path.startswith("/") or ".." in path.split("/"):
        return None
    path = path.split("::", 1)[0].split("#", 1)[0].strip().strip("`'\"[](){}<>,;.")
    basename = path.rsplit("/", 1)[-1]
    if path not in {"README.md", "SETUP.md"} and ("." not in basename or "/" not in path):
        return None
    return path


def _iter_text_fragments(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        fragments: list[str] = []
        for nested in value.values():
            fragments.extend(_iter_text_fragments(nested))
        return fragments
    if isinstance(value, (list, tuple, set)):
        fragments = []
        for nested in value:
            fragments.extend(_iter_text_fragments(nested))
        return fragments
    return [str(value)]


def _repo_paths_from_bug_row(bug: Mapping[str, Any]) -> list[str]:
    """Extract concrete repo paths from bug authority text.

    Bug descriptions often carry audit evidence like
    ``Code&DBs/Workflow/runtime/foo.py::symbol``. Treat those paths as the
    best available source shard before falling back to workspace root.
    """
    fragments: list[str] = []
    for key in ("description", "summary", "resume_context"):
        fragments.extend(_iter_text_fragments(bug.get(key)))

    paths: list[str] = []
    seen: set[str] = set()
    for fragment in fragments:
        for match in _REPO_PATH_PATTERN.finditer(fragment):
            path = _clean_repo_path_candidate(match.group(0))
            if not path or path in seen:
                continue
            seen.add(path)
            paths.append(path)
    return sorted(paths)


def _repo_paths_for_bug_ids(
    bugs_by_id: Mapping[str, Mapping[str, Any]],
    bug_ids: list[str],
) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for bug_id in bug_ids:
        bug = bugs_by_id.get(bug_id)
        if not bug:
            continue
        for path in _repo_paths_from_bug_row(bug):
            if path in seen:
                continue
            seen.add(path)
            paths.append(path)
    return sorted(paths)


def _generate_prompt(
    stage: str,
    write_scope: list[str],
    description: str,
    label: str = "step",
) -> str:
    """Generate a prompt from stage template and description.

    ``label`` is sanitized into a filename-safe slug used by review/research
    templates that demand a structured JSON deliverable at
    ``{write_scope}/{step_deliverable}.analysis.json``. Other stage templates
    ignore the variable.
    """
    template = _STAGE_TEMPLATES[stage]
    write_str = ", ".join(write_scope) if len(write_scope) > 1 else (write_scope[0] if write_scope else "")
    step_deliverable = re.sub(r"[^a-zA-Z0-9_-]+", "-", label).strip("-") or "step"
    return template.format(
        write_scope=write_str,
        description=description,
        step_deliverable=step_deliverable,
    )


def _verify_ref_name(verification_ref: str, file_path: str) -> str:
    digest = sha256(f"{verification_ref}|{file_path}".encode("utf-8")).hexdigest()[:12]
    path_slug = re.sub(r"[^a-z0-9]+", "-", file_path.lower()).strip("-")
    ref_slug = verification_ref.replace(".", "-")
    return f"verify_ref.{ref_slug}.{path_slug}.{digest}"


def _file_has_admitted_verifier(file_path: str) -> bool:
    """Return ``True`` if the file has an admitted verifier today.

    Currently only Python files (.py) have admitted verifiers via
    :func:`_generate_verify_refs`. Non-Python extensions silent-zero — the
    Python-only ladder Phase 1.5 is queued to replace with catalog-backed
    dispatch OR post-run write_set derivation. This predicate is the single
    source of truth for both the generator (``_generate_verify_refs``) and
    the gap surfacer (``_compute_verification_gaps``), so widening the
    admitted set is a one-edit change.
    """
    return bool(file_path) and file_path.endswith(".py")


def _compute_verification_gaps(write_scope: list[str]) -> list[dict[str, str]]:
    """Surface files in write_scope with no admitted verifier.

    Per architecture-policy::platform-architecture::fail-closed-at-compile-
    no-silent-defaults plus expected-envelope-vs-actual-truth-separation,
    the launch compiler must name verification gaps explicitly in the
    packet_map rather than letting the Python-only ladder silently produce
    zero verifiers for JS / TS / SQL / MD / other write-scope files.

    Phase 1.5 drives this list toward empty by extending
    :func:`_file_has_admitted_verifier`'s admitted set (catalog-backed) or
    moving verification to post-run write_set reconciliation.
    """
    gaps: list[dict[str, str]] = []
    for file_path in write_scope:
        if not file_path or not file_path.strip():
            # Blank / whitespace-only entries are invalid input, not gaps —
            # match the silent-drop behavior of ``_generate_verify_refs``.
            continue
        if _file_has_admitted_verifier(file_path):
            continue
        gaps.append(
            {
                "file": file_path,
                "missing_type": "verifier",
                "reason_code": "verifier.no_admitted_for_extension",
            }
        )
    return gaps


def _generate_verify_refs(write_scope: list[str]) -> list[dict[str, Any]]:
    """Generate DB-backed verification ref rows from write scope."""
    refs: list[dict[str, Any]] = []

    for file_path in write_scope:
        if not _file_has_admitted_verifier(file_path):
            continue

        verification_ref = "verification.python.py_compile"
        label = f"Compile {file_path}"
        if "test" in file_path.lower():
            verification_ref = "verification.python.pytest_file"
            label = f"Pytest {file_path}"
        verify_ref = _verify_ref_name(verification_ref, file_path)
        refs.append(
            {
                "verify_ref": verify_ref,
                "verification_ref": verification_ref,
                "label": label,
                "description": f"Verify Python file {file_path}",
                "inputs": {"path": file_path},
                "enabled": True,
                "binding_revision": f"binding.{verify_ref}",
                "decision_ref": "decision.verify_refs.bootstrap.20260408",
            }
        )

    return refs


def compile_spec(
    intent_dict: dict[str, Any],
    *,
    auto_read_scope: bool = False,
    conn: Any | None = None,
) -> tuple[CompiledSpec, list[str]]:
    """Compile a minimal intent dict into a full WorkflowSpec.

    Args:
        intent_dict: Dict with at least "description", "write", "stage".
        auto_read_scope: If True, compute read scope from write scope
                        (requires scope_resolver integration).

    Returns:
        (CompiledSpec, list_of_warnings)
    """
    warnings: list[str] = []

    # Parse intent from dict
    try:
        intent = Intent(
            description=intent_dict.get("description", ""),
            write=intent_dict.get("write", []),
            stage=intent_dict.get("stage", "build"),
            read=intent_dict.get("read"),
            label=intent_dict.get("label"),
            timeout=intent_dict.get("timeout", 300),
            max_tokens=intent_dict.get("max_tokens", 4096),
            temperature=intent_dict.get("temperature", 0.0),
        )
    except Exception as exc:
        raise ValueError(f"Failed to parse intent: {exc}") from exc

    # Validate intent
    is_valid, errors = _validate_intent(intent)
    if not is_valid:
        raise ValueError(f"Invalid intent: {'; '.join(errors)}")

    # Generate label if not provided
    label = intent.label or _generate_label(intent.stage, intent.description)

    # Generate prompt
    prompt = _generate_prompt(intent.stage, intent.write, intent.description)

    if conn is None:
        raise CapabilityCatalogError("compile_spec requires Postgres authority for capability selection")

    sync_capability_catalog(conn)
    capability_rows = select_capability_catalog_entries(
        conn,
        description=intent.description,
        stage=intent.stage,
        label=label,
        write_scope=intent.write,
    )
    all_caps = [
        str(row.get("capability_slug") or "").strip()
        for row in capability_rows
        if str(row.get("capability_slug") or "").strip()
    ]
    if not all_caps:
        raise CapabilityCatalogError("capability_catalog selection returned no capability slugs")

    # Get tier from stage
    tier = _STAGE_TO_TIER.get(intent.stage, "mid")

    # Generate verify commands
    verify_rows = _generate_verify_refs(intent.write)
    verify_refs = [row["verify_ref"] for row in verify_rows]
    if conn is not None and verify_rows:
        sync_verify_refs(conn, verify_refs=verify_rows)

    # Build context sections (empty for now; will be filled by scope_resolver)
    context_sections: list[dict[str, str]] | None = None

    # Determine read scope (empty for now; will be computed by scope_resolver if auto_read_scope=True)
    read_scope = intent.read

    workspace_ref: str | None = None
    runtime_profile_ref: str | None = None
    if conn is None:
        workspace_ref = _default_workspace_ref()
        runtime_profile_ref = _default_runtime_profile_ref()
    else:
        from registry.native_runtime_profile_sync import NativeRuntimeProfileSyncError

        try:
            workspace_ref = _default_workspace_ref(conn)
        except NativeRuntimeProfileSyncError:
            workspace_ref = None
        try:
            runtime_profile_ref = _default_runtime_profile_ref(conn)
        except NativeRuntimeProfileSyncError:
            runtime_profile_ref = None

    # Build compiled spec
    kernel_definition = build_definition_kernel(
        source_prose=intent.description,
        compiled_prose=prompt,
        references=[],
        capabilities=capability_rows,
        authority="",
        sla={},
    )
    spec = CompiledSpec(
        prompt=kernel_definition["compiled_prose"],
        scope_write=intent.write,
        scope_read=read_scope,
        context_sections=context_sections,
        capabilities=all_caps,
        tier=tier,
        label=label,
        task_type=intent.stage,
        verify_refs=verify_refs or None,
        timeout=intent.timeout,
        max_tokens=intent.max_tokens,
        temperature=intent.temperature,
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        definition_graph=kernel_definition["definition_graph"],
        definition_revision=kernel_definition["definition_revision"],
        compiled_prose=kernel_definition["compiled_prose"],
        narrative_blocks=kernel_definition["narrative_blocks"],
        draft_flow=kernel_definition["draft_flow"],
    )

    return spec, warnings


def compile_intent_from_file(file_path: str) -> tuple[Intent | None, list[str]]:
    """Load and compile an intent from a JSON file.

    Returns:
        (Intent or None, list_of_errors)
    """
    import json

    if not os.path.exists(file_path):
        return None, [f"File not found: {file_path}"]

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        return None, [f"Invalid JSON in {file_path}: {exc}"]

    try:
        intent = Intent(
            description=data.get("description", ""),
            write=data.get("write", []),
            stage=data.get("stage", "build"),
            read=data.get("read"),
            label=data.get("label"),
            timeout=data.get("timeout", 300),
            max_tokens=data.get("max_tokens", 4096),
            temperature=data.get("temperature", 0.0),
        )
    except Exception as exc:
        return None, [f"Failed to parse intent: {exc}"]

    is_valid, errors = _validate_intent(intent)
    return (intent if is_valid else None), errors


def compile_prompt_launch_spec(
    *,
    prompt: str,
    provider_slug: str | None = None,
    model_slug: str | None = None,
    tier: str | None = None,
    adapter_type: str | None = None,
    scope_write: list[str] | None = None,
    workdir: str | None = None,
    context_files: list[str] | None = None,
    timeout: int = 300,
    task_type: str | None = None,
    system_prompt: str | None = None,
    workspace_ref: str | None = None,
    runtime_profile_ref: str | None = None,
    workflow_id: str = "workflow_cli_prompt",
) -> PromptLaunchSpec:
    """Compile a prompt launch into the inline workflow.submit shape.

    This keeps prompt-backed launches on the same canonical inline spec shape
    regardless of which CLI or API surface collects the prompt arguments.
    """

    if provider_slug is None:
        provider_slug = _default_provider_slug()
    if adapter_type is None:
        adapter_type = resolve_default_adapter_type(provider_slug)

    if scope_write and not workdir:
        workdir = os.getcwd()

    normalized_provider_slug = str(provider_slug or "").strip()
    if (
        normalized_provider_slug
        and "/" not in normalized_provider_slug
        and not normalized_provider_slug.startswith("auto/")
        and adapter_type in {"cli_llm", "llm_task"}
    ):
        lane_policy = resolve_lane_policy(normalized_provider_slug, adapter_type)
        admitted_by_policy = bool(
            isinstance(lane_policy, dict) and lane_policy.get("admitted_by_policy")
        )
        if admitted_by_policy:
            lane_policy = None
        else:
            lane_policy = lane_policy if isinstance(lane_policy, dict) else {}
    else:
        lane_policy = None

    if lane_policy is not None:
        message_parts = [
            f"provider {normalized_provider_slug!r} is not admitted for {adapter_type}"
        ]
        policy_reason = str(lane_policy.get("policy_reason") or "").strip()
        decision_ref = str(lane_policy.get("decision_ref") or "").strip()
        if policy_reason:
            message_parts.append(f"reason: {policy_reason}")
        if decision_ref:
            message_parts.append(f"decision_ref: {decision_ref}")
        message_parts.append(f"known providers: {', '.join(registered_providers())}")
        raise ValueError("; ".join(message_parts))

    context_sections: list[dict[str, str]] = []
    if context_files or scope_write:
        all_paths = list(context_files or []) + list(scope_write or [])
        for fpath in all_paths:
            abs_path = os.path.join(workdir or ".", fpath)
            try:
                with open(abs_path, encoding="utf-8") as fh:
                    content = fh.read()
            except OSError:
                continue
            context_sections.append(
                {
                    "name": f"FILE: {fpath}",
                    "content": content,
                }
            )

    compiled_prompt = prompt
    if scope_write:
        compiled_prompt += (
            "\n\nReturn your response as JSON with this schema:\n"
            '{"code_blocks": [{"file_path": "<path>", '
            '"content": "<FULL FILE>", "language": "python", '
            '"action": "replace"}], '
            '"explanation": "<what you changed>"}'
        )
        if not system_prompt:
            system_prompt = "You are a code editor. Return ONLY valid JSON structured output."

    resolved_model_slug = model_slug
    if resolved_model_slug is None:
        if normalized_provider_slug and "/" not in normalized_provider_slug and not normalized_provider_slug.startswith("auto/"):
            resolved_model_slug = default_model_for_provider(normalized_provider_slug)

    resolved_workflow_id = workflow_id
    if workflow_id == "workflow_cli_prompt":
        resolved_workflow_id = f"workflow_cli_prompt.{uuid.uuid4().hex[:12]}"

    launch_job = {
        "label": "run",
        "agent": f"{provider_slug}/{resolved_model_slug}" if resolved_model_slug else provider_slug,
        "prompt": compiled_prompt,
        "adapter_type": adapter_type,
        "tier": tier,
        "timeout": timeout,
        "write_scope": list(scope_write or []),
        "workdir": workdir,
        "context_sections": context_sections,
        "system_prompt": system_prompt,
        "task_type": task_type,
    }
    definition_payload = {
        "graph_runtime_submit": True,
        "phase": "execute",
        "jobs": [launch_job],
    }
    definition_revision = f"def_{stable_hash(definition_payload)[:16]}"
    plan_payload = {
        "definition_revision": definition_revision,
        "graph_runtime_submit": True,
        "phase": "execute",
        "jobs": [launch_job],
    }
    plan_revision = f"plan_{stable_hash(plan_payload)[:16]}"
    packet_provenance = {
        "source_kind": "prompt_launch",
        "definition_row": {"definition_revision": definition_revision},
        "compiled_spec_row": {
            "definition_revision": definition_revision,
            "plan_revision": plan_revision,
        },
    }

    return PromptLaunchSpec(
        name=compiled_prompt[:80] or "workflow cli prompt",
        workflow_id=resolved_workflow_id,
        phase="execute",
        graph_runtime_submit=True,
        jobs=[launch_job],
        workspace_ref=str(workspace_ref or "").strip() or None,
        runtime_profile_ref=str(runtime_profile_ref or "").strip() or None,
        definition_revision=definition_revision,
        plan_revision=plan_revision,
        packet_provenance=packet_provenance,
    )


# ---------------------------------------------------------------------------
# Plan → spec → launch continuous flow
# ---------------------------------------------------------------------------
#
# The programmatic counterpart to the Moon UI's graph-to-run chain
# (roadmap_item.make.moon.ui.emit.runnable.graph.authority.for.gated.9.step.workflows).
# Callers describe packets as minimal intents; this module compiles, translates
# into the platform's workflow spec shape, and submits — never handing the
# spec-JSON step back to a caller or LLM.
#
# Collapses the plan→spec→validate→submit→launch friction tracked in
# BUG-8DB03A36 (submission lifecycle spread) and BUG-5D0140CD (validation
# contract spread).


@dataclass(frozen=True)
class PlanPacket:
    """One unit of work in a plan.

    ``description`` is the only required field per architecture-policy::
    platform-architecture::source-refs-plural-canonical-shape (caller tax
    3→1). ``write`` defaults to an empty list: source-authority resolvers
    (``_plan_packets_from_bugs`` etc.) populate it from authority metadata;
    explicit packets with empty write are surfaced as UnresolvedWriteScopeError
    at ``compile_plan`` time per fail-closed-at-compile-no-silent-defaults.
    ``stage`` defaults to "build" and is validated against _STAGE_TEMPLATES.

    ``bug_ref`` holds the primary bug this packet resolves (singular).
    ``bug_refs`` holds the full list when the packet resolves a cluster of
    related bugs (e.g. from ``derive_bug_packets``) — the primary is the
    first entry. Both may be set; bug_refs is authoritative when present.
    """

    description: str
    write: list[str] = field(default_factory=list)
    stage: str = "build"
    label: str | None = None
    read: list[str] | None = None
    depends_on: list[str] | None = None
    bug_ref: str | None = None
    bug_refs: list[str] | None = None
    agent: str | None = None
    complexity: str | None = None


@dataclass(frozen=True)
class Plan:
    """A named batch of packets, submitted as one workflow_run.

    Callers either hand-write ``packets`` or supply ``source_refs`` with ref
    IDs that dispatch by prefix to the right source authority:

      - ``BUG-*`` → ``derive_bug_packets`` via ``_plan_packets_from_bugs``
      - ``roadmap_item.*`` → ``_plan_packets_from_roadmap_items``
      - ``idea.*`` / ``operator_idea.*`` → ``_plan_packets_from_ideas``
      - ``friction.*`` / ``friction_event.*`` → ``_plan_packets_from_friction``

    Unresolvable prefixes raise :class:`UnresolvedSourceRefError` (becomes a
    typed gap in Phase 1.5 per architecture-policy::platform-architecture::
    fail-closed-at-compile-no-silent-defaults).

    Legacy ``from_bugs`` / ``from_roadmap_items`` / ``from_ideas`` /
    ``from_friction`` fields are accepted as deprecated aliases; they merge
    into ``source_refs`` at coerce time per architecture-policy::platform-
    architecture::source-refs-plural-canonical-shape. Canonical internal
    shape is ``source_refs: []``.

    Explicit ``packets`` always win — if both ``packets`` and any source-ref
    input are set, ``_coerce_plan`` raises an ambiguity error.
    """

    name: str
    packets: list[PlanPacket]
    workflow_id: str | None = None
    why: str | None = None
    phase: str = "build"
    workdir: str | None = None
    source_refs: list[str] | None = None
    from_bugs: list[str] | None = None
    from_roadmap_items: list[str] | None = None
    from_ideas: list[str] | None = None
    from_friction: list[str] | None = None


@dataclass(frozen=True)
class LaunchReceipt:
    """Result of launch_plan: one run_id covering every packet as a job."""

    run_id: str
    spec_name: str
    workflow_id: str
    total_jobs: int
    packet_map: list[dict[str, Any]]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "spec_name": self.spec_name,
            "workflow_id": self.workflow_id,
            "total_jobs": self.total_jobs,
            "packet_map": list(self.packet_map),
            "warnings": list(self.warnings),
        }


def _coerce_packet(value: Any) -> PlanPacket:
    if isinstance(value, PlanPacket):
        return value
    if not isinstance(value, dict):
        raise ValueError(
            f"plan packet must be a PlanPacket or dict, got {type(value).__name__}"
        )
    return PlanPacket(
        description=str(value.get("description") or "").strip(),
        write=list(value.get("write") or []),
        stage=str(value.get("stage") or "build"),
        label=value.get("label"),
        read=list(value["read"]) if value.get("read") else None,
        depends_on=list(value["depends_on"]) if value.get("depends_on") else None,
        bug_ref=value.get("bug_ref"),
        bug_refs=list(value["bug_refs"]) if value.get("bug_refs") else None,
        agent=value.get("agent"),
        complexity=value.get("complexity"),
    )


def _emit_plan_launched_event(
    conn: Any,
    *,
    run_id: str,
    workflow_id: str,
    spec_name: str,
    total_jobs: int,
    packet_labels: list[str],
    source_refs: list[str] | None = None,
) -> str | None:
    """Emit the ``plan.launched`` conceptual event after a successful launch.

    Writes to ``public.system_events`` via the canonical
    :func:`runtime.system_events.emit_system_event` helper. Per architecture-
    policy::platform-architecture::expected-envelope-vs-actual-truth-
    separation, this event marks the pre-run envelope crossing into
    runtime — downstream projections (future Phase 2/3) wire it into Moon
    observability and the agent operating manifest's "what changed recently"
    question.

    Best-effort: emission failures are returned as an error string rather
    than raised, so a successful submit_workflow_command is not rolled back
    by a degraded system_events write path. Caller should fold the returned
    error (if any) into the receipt's ``warnings`` list.

    Returns ``None`` on success; a warning-formatted error string on failure.

    Formal ``authority_event_contracts`` registration is queued for a
    follow-up CQRS-consistency packet. Until then, consumers subscribe via
    the freeform ``system_events`` stream.
    """
    try:
        from runtime.system_events import emit_system_event
    except Exception as exc:
        return f"plan.launched event emission skipped: system_events unavailable ({type(exc).__name__}: {exc})"
    try:
        emit_system_event(
            conn,
            event_type="plan.launched",
            source_id=run_id or workflow_id,
            source_type="launch_plan",
            payload={
                "run_id": run_id,
                "workflow_id": workflow_id,
                "spec_name": spec_name,
                "total_jobs": total_jobs,
                "source_refs": list(source_refs or []),
                "packet_labels": list(packet_labels),
            },
        )
    except Exception as exc:
        return f"plan.launched event emission failed: {type(exc).__name__}: {exc}"
    return None


def _bind_packet_data_pills(
    description: str,
    *,
    conn: Any,
) -> dict[str, Any]:
    """Extract and validate ``object.field`` data pills from a packet
    description via :func:`runtime.intent_binding.bind_data_pills`.

    Shared entry point for ``compile_plan`` (attaches pills to the job dict
    so ``_build_packet_map_entry`` can surface them on the receipt) and
    ``propose_plan`` (surfaces pills in ``packet_declarations``). Honors
    architecture-policy::platform-architecture::data-pill-primitive-family
    — data pills are atomic field references against
    ``data_dictionary_entries``; ambiguous and unbound pills appear in the
    returned structure so callers can promote them to typed gaps.

    Returns a normalized dict with keys ``bound``, ``ambiguous``,
    ``unbound``, ``warnings``. Empty descriptions and module-import
    failures (data-dictionary substrate degraded) return warnings rather
    than raising — the launch compiler must not block on degraded binding
    substrate. Per-call exceptions are caught and folded into the warnings
    list for the same reason.
    """
    result: dict[str, Any] = {
        "bound": [],
        "ambiguous": [],
        "unbound": [],
        "warnings": [],
    }
    text = (description or "").strip()
    if not text:
        return result
    try:
        from runtime.intent_binding import bind_data_pills as _bind
    except Exception as exc:
        result["warnings"].append(
            f"intent_binding unavailable: {type(exc).__name__}: {exc}"
        )
        return result
    try:
        bound_intent = _bind(text, conn=conn)
    except Exception as exc:
        result["warnings"].append(
            f"bind_data_pills failed: {type(exc).__name__}: {exc}"
        )
        return result
    pills = bound_intent.to_dict()
    pills.pop("intent", None)
    result["bound"] = list(pills.get("bound") or [])
    result["ambiguous"] = list(pills.get("ambiguous") or [])
    result["unbound"] = list(pills.get("unbound") or [])
    inner_warnings = list(pills.get("warnings") or [])
    if inner_warnings:
        result["warnings"].extend(inner_warnings)
    return result


def _build_packet_map_entry(
    *,
    job: dict[str, Any],
    packet: PlanPacket | None = None,
) -> dict[str, Any]:
    """Shape one :attr:`LaunchReceipt.packet_map` entry with legacy + derived
    fields.

    Legacy fields (``label``, ``bug_ref``, ``bug_refs``, ``agent``, ``stage``)
    preserved for consumer back-compat. Derived fields added per architecture-
    policy::platform-architecture::expected-envelope-vs-actual-truth-separation
    (all pre-run envelope; post-run truth lands in a separate receipt field
    when Phase 3 writes actual_write_set back):

    - ``inferred_stage`` — stage the compiler locked in (``job['task_type']``
      or ``packet.stage``). Diverges from ``stage`` when stage inference
      lands in a future phase.
    - ``resolved_agent`` — agent string after compile. May still contain the
      ``auto/{stage}`` placeholder until dispatch-time resolution; distinct
      field so post-run reconciliation can overwrite without clobbering the
      legacy ``agent``.
    - ``capabilities`` — capability_slug list the catalog bound to this
      packet (from ``CompiledSpec.capabilities``).
    - ``write_envelope`` — pre-run allowed scope (declared ``packet.write``).
    - ``expected_gates`` — verify_ref IDs the compiler expects to run.
    - ``verification_gaps`` — files in ``write_envelope`` with no admitted
      verifier. Names the Python-only ladder's silent-zero behavior
      explicitly; Phase 1.5 drives this list toward empty.

    ``packet`` is optional so ``launch_proposed`` (which only has
    ``spec_dict['jobs']``, not the original packets) can still build
    enriched entries using the job-level mirrors.
    """
    write_envelope = list(
        packet.write if packet is not None else (job.get("write_scope") or [])
    )
    declared_stage = (
        packet.stage if packet is not None else str(job.get("task_type") or "")
    )
    bug_ref = packet.bug_ref if packet is not None else job.get("bug_ref")
    bug_refs: list[str] | None
    if packet is not None:
        bug_refs = list(packet.bug_refs) if packet.bug_refs else None
    else:
        raw = job.get("bug_refs")
        bug_refs = list(raw) if isinstance(raw, list) else None
    agent = job["agent"]
    job_produces = list(job.get("produces") or [])
    job_consumes = list(job.get("consumes") or [])
    job_consumes_any = list(job.get("consumes_any") or [])
    # Each produced type becomes a typed gate that auto-satisfies when the
    # runtime emits a matching produced artifact. No human review required —
    # gate satisfaction is computable from typed state per
    # architecture-policy::platform-architecture::legal-equals-computable-to-
    # non-gap-output. Closes BUG-2729F8B7 (Moon generated workflow has no
    # release gates or typed gate contracts).
    expected_typed_gates = [
        {
            "type": type_token,
            "kind": "typed_produce",
            "auto_satisfies_when_produced": True,
        }
        for type_token in job_produces
    ]
    return {
        # Legacy fields (stable consumer contract):
        "label": job["label"],
        "bug_ref": bug_ref,
        "bug_refs": bug_refs,
        "agent": agent,
        "stage": declared_stage,
        # Derived fields (pre-run envelope):
        "inferred_stage": str(job.get("task_type") or declared_stage),
        "resolved_agent": agent,
        "capabilities": list(job.get("capabilities") or []),
        "write_envelope": write_envelope,
        "expected_gates": list(job.get("verify_refs") or []),
        "expected_typed_gates": expected_typed_gates,
        "consumes": job_consumes,
        "consumes_any": job_consumes_any,
        "produces": job_produces,
        "verification_gaps": _compute_verification_gaps(write_envelope),
        # Data pills (Phase 1.1.f) — atomic object.field refs extracted from
        # packet.description against data_dictionary_entries. Surfaced even
        # when zero bound so consumers can iterate unconditionally.
        "data_pills": dict(
            job.get("data_pills")
            or {"bound": [], "ambiguous": [], "unbound": [], "warnings": []}
        ),
    }


def _plan_packets_from_bugs(
    bug_ids: list[str],
    *,
    conn: Any,
    program_id: str,
) -> list[PlanPacket]:
    """Materialize PlanPackets from bug IDs via derive_bug_packets.

    Fetches the rows from the bugs table, runs the deterministic
    lane+wave classifier, then translates each derived cluster into one
    PlanPacket. Wave-level depends_on_wave edges are converted into
    per-PlanPacket label-based depends_on so the workflow engine's
    existing dependency wiring can run the waves correctly.

    Missing / unknown bug IDs are silently dropped by derive_bug_packets —
    the caller sees fewer packets than IDs supplied. No silent mutation.
    """
    if not bug_ids:
        return []

    deduped = list({bug_id.strip(): None for bug_id in bug_ids if bug_id and bug_id.strip()})
    if not deduped:
        return []

    placeholders = ", ".join(f"${i+1}" for i in range(len(deduped)))
    rows = conn.execute(
        f"SELECT bug_id, title, category, severity, status, summary, description, resume_context "
        f"FROM bugs WHERE bug_id IN ({placeholders})",
        *deduped,
    )
    bugs = _annotate_bug_rows_for_packet_derivation(
        conn=conn,
        bugs=[dict(row) for row in rows or []],
    )
    if not bugs:
        return []

    from runtime.bug_resolution_program import derive_bug_packets

    derived = derive_bug_packets(program_id=program_id, bugs=bugs)
    if not derived:
        return []

    bugs_by_id: dict[str, dict[str, Any]] = {
        str(bug.get("bug_id") or ""): bug
        for bug in bugs
        if str(bug.get("bug_id") or "").strip()
    }
    # Map wave_id → set of packet labels in that wave, for depends_on wiring.
    wave_to_labels: dict[str, list[str]] = {}
    for packet in derived:
        wave_to_labels.setdefault(str(packet["wave_id"]), []).append(str(packet["packet_slug"]))

    plan_packets: list[PlanPacket] = []
    for packet in derived:
        cluster_label = str(packet["cluster"]["label"])
        bug_ids_for_packet = list(packet["bug_ids"])
        derived_scope_paths = _repo_paths_for_bug_ids(bugs_by_id, bug_ids_for_packet)
        done_criteria = list(packet.get("done_criteria") or [])
        description_lines = [
            f"Resolve bug cluster: {cluster_label}",
            f"Bugs: {', '.join(bug_ids_for_packet)}",
            f"Highest severity: {packet.get('highest_severity')}",
            f"Lane: {packet.get('lane_label')} (wave {packet.get('wave_id')})",
            f"Verification surface: {packet.get('verification_surface')}",
        ]
        if derived_scope_paths:
            description_lines.append("Derived repo scope:")
            description_lines.extend(f"  - {path}" for path in derived_scope_paths)
        if done_criteria:
            description_lines.append("Done when all of:")
            description_lines.extend(f"  - {criterion}" for criterion in done_criteria)
        stop_boundary = packet.get("stop_boundary")
        if stop_boundary:
            description_lines.append(f"Stop boundary: {stop_boundary}")
        description = "\n".join(description_lines)

        # Wave deps → label deps. Exclude the packet's own label.
        depends_on_labels: list[str] = []
        own_label = str(packet["packet_slug"])
        for dep_wave in packet.get("depends_on_wave") or []:
            for label in wave_to_labels.get(str(dep_wave), []):
                if label != own_label and label not in depends_on_labels:
                    depends_on_labels.append(label)

        plan_packets.append(
            PlanPacket(
                description=description,
                # Prefer concrete repo paths named by bug evidence. Only
                # fall back to workspace root when the bug record carries no
                # path evidence; ProposedPlan surfaces that broad shard so
                # the caller can narrow before launch.
                write=derived_scope_paths or ["."],
                # Bug packets are "fix" semantically, but route authority
                # has no task_type_route_profiles row for a fix lane. Use
                # the canonical coding route instead of emitting auto/fix.
                stage="build",
                label=own_label,
                read=derived_scope_paths or None,
                depends_on=depends_on_labels or None,
                bug_ref=bug_ids_for_packet[0] if bug_ids_for_packet else None,
                bug_refs=bug_ids_for_packet or None,
            )
        )
    return plan_packets


def _annotate_bug_rows_for_packet_derivation(
    *,
    conn: Any,
    bugs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach derived replay state without treating it as bug-row authority."""

    needs_replay_state = [
        bug
        for bug in bugs
        if "replay_ready" not in bug or "replay_reason_code" not in bug
    ]
    if not needs_replay_state:
        return bugs

    try:
        from runtime.bug_tracker import BugTracker

        tracker = BugTracker(conn)
    except Exception:
        tracker = None

    for bug in needs_replay_state:
        hint: Mapping[str, Any] | None = None
        if tracker is not None:
            try:
                hint = tracker.replay_hint(
                    str(bug.get("bug_id") or ""),
                    receipt_limit=1,
                    allow_backfill=False,
                )
            except Exception:
                hint = None
        bug["replay_ready"] = bool(hint and hint.get("available"))
        bug["replay_reason_code"] = (
            str((hint or {}).get("reason_code") or "bug.replay_hint_unavailable")
        )
    return bugs


def _plan_packets_from_roadmap_items(
    item_ids: list[str],
    *,
    conn: Any,
) -> list[PlanPacket]:
    """Materialize PlanPackets from roadmap_item IDs.

    Each active roadmap item becomes one PlanPacket with description built
    from title + summary + acceptance_criteria.must_have. Stage defaults to
    'fix' when the item has a source_bug_id, else 'build'. Completed items
    are silently dropped — the caller sees fewer packets than IDs supplied.

    Write scope defaults to workspace root '.' — roadmap items don't carry
    enforced registry paths today; ProposedPlan surfaces this as a warning.
    No cross-item dependency wiring in MVP — caller edits depends_on
    explicitly when the items have a known order.
    """
    if not item_ids:
        return []

    deduped = list({item.strip(): None for item in item_ids if item and item.strip()})
    if not deduped:
        return []

    placeholders = ", ".join(f"${i+1}" for i in range(len(deduped)))
    rows = conn.execute(
        f"SELECT roadmap_item_id, title, summary, acceptance_criteria, "
        f"priority, lifecycle, source_bug_id "
        f"FROM roadmap_items WHERE roadmap_item_id IN ({placeholders})",
        *deduped,
    )
    items = [
        dict(row)
        for row in rows or []
        if str(dict(row).get("lifecycle") or "") != "completed"
    ]
    if not items:
        return []

    plan_packets: list[PlanPacket] = []
    for item in items:
        item_id = str(item["roadmap_item_id"])
        title = str(item.get("title") or item_id)
        summary = str(item.get("summary") or "").strip()
        acceptance_raw = item.get("acceptance_criteria") or {}
        if isinstance(acceptance_raw, str):
            import json as _json
            try:
                acceptance = _json.loads(acceptance_raw)
            except (ValueError, TypeError):
                acceptance = {}
        else:
            acceptance = dict(acceptance_raw) if isinstance(acceptance_raw, dict) else {}
        must_have = list(acceptance.get("must_have") or [])
        outcome_gate = str(acceptance.get("outcome_gate") or "").strip()

        priority = str(item.get("priority") or "").strip()
        source_bug_id = item.get("source_bug_id")
        stage = "fix" if source_bug_id else "build"

        description_lines = [f"Roadmap item: {title}"]
        if priority:
            description_lines.append(f"Priority: {priority}")
        if summary:
            description_lines.append(f"Summary: {summary}")
        if must_have:
            description_lines.append("Must have:")
            description_lines.extend(f"  - {criterion}" for criterion in must_have)
        if outcome_gate:
            description_lines.append(f"Outcome gate: {outcome_gate}")
        description = "\n".join(description_lines)

        # Derive a stable, reasonably short label from the roadmap_item_id.
        # Drop the leading 'roadmap_item.' prefix if present; cap at 64 chars.
        label_seed = item_id
        if label_seed.startswith("roadmap_item."):
            label_seed = label_seed[len("roadmap_item.") :]
        label = label_seed[:64]

        plan_packets.append(
            PlanPacket(
                description=description,
                write=["."],
                stage=stage,
                label=label,
                bug_ref=str(source_bug_id) if source_bug_id else None,
            )
        )
    return plan_packets


def _plan_packets_from_ideas(
    idea_ids: list[str],
    *,
    conn: Any,
) -> list[PlanPacket]:
    """Materialize PlanPackets from operator_ideas rows (open status only).

    Each open idea becomes one PlanPacket. Description is built from title
    + summary + owner_ref + decision_ref. Stage defaults to 'build' — ideas
    are forward work by nature. Non-open ideas (promoted / rejected /
    superseded / archived) are silently dropped; the roadmap or bug tracker
    is the right surface for those lifecycles.
    """
    if not idea_ids:
        return []
    deduped = list({item.strip(): None for item in idea_ids if item and item.strip()})
    if not deduped:
        return []
    placeholders = ", ".join(f"${i+1}" for i in range(len(deduped)))
    rows = conn.execute(
        f"SELECT idea_id, title, summary, status, owner_ref, decision_ref "
        f"FROM operator_ideas WHERE idea_id IN ({placeholders})",
        *deduped,
    )
    ideas = [
        dict(row) for row in rows or [] if str(dict(row).get("status") or "") == "open"
    ]
    if not ideas:
        return []
    plan_packets: list[PlanPacket] = []
    for idea in ideas:
        idea_id = str(idea["idea_id"])
        title = str(idea.get("title") or idea_id)
        summary = str(idea.get("summary") or "").strip()
        owner = str(idea.get("owner_ref") or "").strip()
        decision = str(idea.get("decision_ref") or "").strip()

        description_lines = [f"Operator idea: {title}"]
        if summary:
            description_lines.append(f"Summary: {summary}")
        if owner:
            description_lines.append(f"Owner: {owner}")
        if decision:
            description_lines.append(f"Decision ref: {decision}")
        description = "\n".join(description_lines)

        label_seed = idea_id
        for prefix in ("operator_idea.", "idea."):
            if label_seed.startswith(prefix):
                label_seed = label_seed[len(prefix) :]
                break
        label = label_seed[:64]

        plan_packets.append(
            PlanPacket(
                description=description,
                write=["."],
                stage="build",
                label=label,
            )
        )
    return plan_packets


def _plan_packets_from_friction(
    event_ids: list[str],
    *,
    conn: Any,
) -> list[PlanPacket]:
    """Materialize PlanPackets from friction_events rows.

    Each event becomes one PlanPacket describing the friction_type +
    message + job_label that produced it. Stage defaults to 'fix' because
    friction is, by definition, a failure signal. No lifecycle filter —
    friction_events are events, not tracked items.
    """
    if not event_ids:
        return []
    deduped = list({item.strip(): None for item in event_ids if item and item.strip()})
    if not deduped:
        return []
    placeholders = ", ".join(f"${i+1}" for i in range(len(deduped)))
    rows = conn.execute(
        f"SELECT event_id, friction_type, source, job_label, message, timestamp "
        f"FROM friction_events WHERE event_id IN ({placeholders})",
        *deduped,
    )
    events = [dict(row) for row in rows or []]
    if not events:
        return []
    plan_packets: list[PlanPacket] = []
    for event in events:
        event_id = str(event["event_id"])
        friction_type = str(event.get("friction_type") or "").strip() or "friction"
        source = str(event.get("source") or "").strip() or "unknown"
        job_label = str(event.get("job_label") or "").strip() or "unknown"
        message = str(event.get("message") or "").strip()

        description_lines = [
            f"Friction event: {friction_type}",
            f"Source: {source}",
            f"Job label: {job_label}",
        ]
        if message:
            description_lines.append(f"Message: {message}")
        description = "\n".join(description_lines)

        # event_id can be long; truncate for a reasonable label.
        label_seed = event_id.replace(".", "_")
        label = f"friction_{label_seed[:56]}"

        plan_packets.append(
            PlanPacket(
                description=description,
                write=["."],
                stage="fix",
                label=label,
            )
        )
    return plan_packets


# Prefix-to-kind dispatch for source_refs resolution. Checked in order; first
# matching prefix wins. Kinds map to the per-authority resolvers invoked by
# _plan_packets_from_source_refs. New resolvers (decision., review.,
# discovery.) land in Phase 1.2's built-not-wired compiler wiring sweep.
_SOURCE_REF_DISPATCH: tuple[tuple[str, str], ...] = (
    ("BUG-", "bug"),
    ("bug-", "bug"),
    ("roadmap_item.", "roadmap_item"),
    ("operator_idea.", "idea"),
    ("idea.", "idea"),
    ("friction_event.", "friction"),
    ("friction.", "friction"),
)


class UnresolvedSourceRefError(ValueError):
    """Raised when one or more source_refs have no admitted resolver.

    Per architecture-policy::platform-architecture::fail-closed-at-compile-
    no-silent-defaults, unresolvable refs must not silently pass through.
    Phase 1.5 converts this exception into a typed_gap row with
    ``missing_type='source_authority_resolver'`` and
    ``legal_repair_actions=['add_resolver_for_prefix']``; until then it
    surfaces as this typed exception with the full ref list preserved on
    ``unresolved_refs`` so callers can build the repair packet themselves.
    """

    def __init__(self, unresolved_refs: list[str]) -> None:
        self.unresolved_refs = list(unresolved_refs)
        known_prefixes = sorted({prefix for prefix, _ in _SOURCE_REF_DISPATCH})
        preview = ", ".join(repr(r) for r in unresolved_refs[:5])
        if len(unresolved_refs) > 5:
            preview += ", ..."
        super().__init__(
            f"{len(unresolved_refs)} source_ref(s) have no admitted resolver: "
            f"{preview}. Known prefixes: {', '.join(known_prefixes)}. "
            "(decision., review., discovery. resolvers are queued in Phase "
            "1.2 built-not-wired sweep.)"
        )


def _classify_source_ref(ref: str) -> str | None:
    """Return the source-authority kind for a ref, or ``None`` if its prefix
    has no admitted resolver.
    """
    for prefix, kind in _SOURCE_REF_DISPATCH:
        if ref.startswith(prefix):
            return kind
    return None


def _plan_packets_from_source_refs(
    refs: list[str],
    *,
    conn: Any,
    program_id: str,
) -> list[PlanPacket]:
    """Polymorphic resolver: group refs by prefix, call the per-kind source-
    authority resolver, concatenate materialized PlanPackets.

    Canonical entry point per architecture-policy::platform-architecture::
    source-refs-plural-canonical-shape. Legacy ``from_bugs`` /
    ``from_roadmap_items`` / ``from_ideas`` / ``from_friction`` on
    :class:`Plan` are deprecated aliases; ``_coerce_plan`` merges them into
    ``source_refs`` before calling this function.

    Unresolvable prefixes raise :class:`UnresolvedSourceRefError` with ALL
    offending refs collected (not just the first), matching the atomic-
    failure shape of :class:`CompilePlanError`. Per-kind resolvers that
    materialize 0 packets for N refs raise :class:`ValueError` with the
    kind-specific diagnostic the legacy path used.
    """
    if not refs:
        return []

    by_kind: dict[str, list[str]] = {}
    unresolved: list[str] = []
    for raw in refs:
        if not isinstance(raw, str):
            continue
        ref = raw.strip()
        if not ref:
            continue
        kind = _classify_source_ref(ref)
        if kind is None:
            unresolved.append(ref)
            continue
        by_kind.setdefault(kind, []).append(ref)

    if unresolved:
        raise UnresolvedSourceRefError(unresolved)

    packets: list[PlanPacket] = []

    bug_refs = by_kind.get("bug")
    if bug_refs:
        bug_packets = _plan_packets_from_bugs(
            bug_refs, conn=conn, program_id=program_id
        )
        if not bug_packets:
            raise ValueError(
                f"source_refs supplied {len(bug_refs)} bug ID(s) but no packets "
                "could be materialized — check bug IDs exist in Praxis.db"
            )
        packets.extend(bug_packets)

    roadmap_refs = by_kind.get("roadmap_item")
    if roadmap_refs:
        roadmap_packets = _plan_packets_from_roadmap_items(
            roadmap_refs, conn=conn
        )
        if not roadmap_packets:
            raise ValueError(
                f"source_refs supplied {len(roadmap_refs)} roadmap_item ID(s) "
                "but no packets could be materialized — check IDs exist in "
                "Praxis.db and are not already completed"
            )
        packets.extend(roadmap_packets)

    idea_refs = by_kind.get("idea")
    if idea_refs:
        idea_packets = _plan_packets_from_ideas(idea_refs, conn=conn)
        if not idea_packets:
            raise ValueError(
                f"source_refs supplied {len(idea_refs)} idea ID(s) but no packets "
                "could be materialized — check IDs exist in Praxis.db and are "
                "still open (promoted/rejected/superseded/archived skipped)"
            )
        packets.extend(idea_packets)

    friction_refs = by_kind.get("friction")
    if friction_refs:
        friction_packets = _plan_packets_from_friction(
            friction_refs, conn=conn
        )
        if not friction_packets:
            raise ValueError(
                f"source_refs supplied {len(friction_refs)} friction_event ID(s) "
                "but no packets could be materialized — check IDs exist in Praxis.db"
            )
        packets.extend(friction_packets)

    return packets


def _coerce_plan(plan: Any, *, conn: Any = None) -> Plan:
    if isinstance(plan, Plan):
        return plan
    if not isinstance(plan, dict):
        raise ValueError(
            f"plan must be a Plan or dict, got {type(plan).__name__}"
        )

    explicit_packets_raw = plan.get("packets") or []
    source_refs_raw = plan.get("source_refs") or None
    from_bugs_raw = plan.get("from_bugs") or None
    from_roadmap_items_raw = plan.get("from_roadmap_items") or None
    from_ideas_raw = plan.get("from_ideas") or None
    from_friction_raw = plan.get("from_friction") or None

    # Merge legacy from_* aliases into the canonical source_refs list.
    merged_source_refs: list[str] = []
    for raw, field_name in (
        (source_refs_raw, "source_refs"),
        (from_bugs_raw, "from_bugs"),
        (from_roadmap_items_raw, "from_roadmap_items"),
        (from_ideas_raw, "from_ideas"),
        (from_friction_raw, "from_friction"),
    ):
        if raw is None:
            continue
        if not isinstance(raw, list) or not all(
            isinstance(item, str) for item in raw
        ):
            raise ValueError(
                f"plan.{field_name} must be a list of ref ID strings"
            )
        merged_source_refs.extend(raw)

    has_source_refs = bool(merged_source_refs)

    if explicit_packets_raw and has_source_refs:
        raise ValueError(
            "plan accepts either explicit 'packets' OR source_refs (including "
            "legacy from_* aliases), not both — remove one to resolve the "
            "ambiguity"
        )

    plan_name = str(plan.get("name") or "launch_plan").strip() or "launch_plan"
    packets: list[PlanPacket] = []

    if has_source_refs:
        if conn is None:
            raise ValueError(
                "plan source_refs (or legacy from_* aliases) require a live "
                "Postgres conn to materialize packets; pass conn=... to "
                "compile_plan / propose_plan / launch_plan"
            )
        program_id = str(plan.get("program_id") or f"plan.{plan_name}").strip()
        packets = _plan_packets_from_source_refs(
            merged_source_refs, conn=conn, program_id=program_id
        )
    else:
        packets = [_coerce_packet(p) for p in explicit_packets_raw]

    return Plan(
        name=plan_name,
        packets=packets,
        workflow_id=(str(plan["workflow_id"]).strip() if plan.get("workflow_id") else None),
        why=plan.get("why"),
        phase=str(plan.get("phase") or "build"),
        workdir=plan.get("workdir"),
        source_refs=list(merged_source_refs) if has_source_refs else None,
        from_bugs=list(from_bugs_raw) if from_bugs_raw else None,
        from_roadmap_items=list(from_roadmap_items_raw) if from_roadmap_items_raw else None,
        from_ideas=list(from_ideas_raw) if from_ideas_raw else None,
        from_friction=list(from_friction_raw) if from_friction_raw else None,
    )


def _enrich_prompt_with_context(
    base_prompt: str,
    *,
    bug_refs: list[str] | None,
    verify_refs: list[str] | None,
    read_scope: list[str] | None,
) -> str:
    """Append a Context section to the compiled prompt when extra grounding exists.

    The stage-template prompt that ``compile_spec`` emits is intentionally
    thin — it covers the 'what' and 'where', but not the 'why' or the
    'proof'. When the packet carries bug references, verifier references,
    or an explicit read scope, this helper appends a single Context block
    so the agent sees the grounding without having to go fetch it.

    Pure helper — does not touch authority, does not mutate inputs. Returns
    the original prompt unchanged when no extra context is available.
    """
    context_lines: list[str] = []
    if bug_refs:
        context_lines.append(
            "- Addresses bug(s): " + ", ".join(bug_refs)
        )
    if read_scope:
        context_lines.append(
            "- Reference files (read before writing): " + ", ".join(read_scope)
        )
    if verify_refs:
        context_lines.append(
            "- Must pass verifier(s): " + ", ".join(verify_refs)
        )

    if not context_lines:
        return base_prompt

    return base_prompt.rstrip() + "\n\n---\nContext:\n" + "\n".join(context_lines)


def _packet_to_job(
    packet: PlanPacket,
    *,
    compiled: CompiledSpec,
    workdir: str,
    index: int,
) -> dict[str, Any]:
    """Translate a compiled packet into a workflow-spec job dict.

    This is the translation that used to require a human/LLM step:
    CompiledSpec fields get mapped onto the job shape submit expects.
    """
    label = packet.label or compiled.label or f"packet_{index}"
    agent = packet.agent or f"auto/{_agent_route_stage(packet.stage)}"
    effective_bug_refs = (
        list(packet.bug_refs)
        if packet.bug_refs
        else ([packet.bug_ref] if packet.bug_ref else None)
    )
    prompt = _enrich_prompt_with_context(
        compiled.prompt,
        bug_refs=effective_bug_refs,
        verify_refs=list(compiled.verify_refs) if compiled.verify_refs else None,
        read_scope=list(packet.read) if packet.read else None,
    )
    from runtime.workflow_type_contracts import route_type_contract

    type_contract = route_type_contract(
        agent,
        title=label,
        summary=packet.description,
    )
    job: dict[str, Any] = {
        "label": label,
        "agent": agent,
        "prompt": prompt,
        "task_type": packet.stage,
        "write_scope": list(packet.write),
        "workdir": workdir,
        # Typed contract derived from agent route + label + description so
        # workers and graph projections see machine-readable consumes/
        # produces alongside the prompt. Without this, runtime jobs were
        # typeless and downstream gates / data_dictionary_lineage / typed_gap
        # surfacing had nothing to bind to (BUG-C6EE740C chain).
        "consumes": list(type_contract["consumes"]),
        "consumes_any": list(type_contract["consumes_any"]),
        "produces": list(type_contract["produces"]),
    }
    if packet.read:
        job["read_scope"] = list(packet.read)
    if packet.depends_on:
        job["depends_on"] = list(packet.depends_on)
    if packet.complexity:
        job["complexity"] = packet.complexity
    if compiled.verify_refs:
        job["verify_refs"] = list(compiled.verify_refs)
    if compiled.capabilities:
        job["capabilities"] = list(compiled.capabilities)
    if compiled.tier and not agent.startswith("auto/"):
        job["tier"] = compiled.tier
    if packet.bug_refs:
        job["bug_refs"] = list(packet.bug_refs)
        if not packet.bug_ref:
            job["bug_ref"] = packet.bug_refs[0]
    if packet.bug_ref and "bug_ref" not in job:
        job["bug_ref"] = packet.bug_ref
    return job


class CompilePlanError(ValueError):
    """Raised when one or more packets fail to compile.

    Collects every per-packet failure before raising so the caller sees the
    full problem set in one pass instead of fix-one-retry-one-retry. The
    ``failures`` attribute holds a list of ``{index, label, error_type,
    message}`` dicts; ``str(exc)`` renders a readable multi-packet summary.
    """

    def __init__(self, failures: list[dict[str, Any]]) -> None:
        self.failures = failures
        lines = [f"{len(failures)} packet(s) failed to compile:"]
        for entry in failures:
            lines.append(
                f"  - packet[{entry['index']}] label={entry['label']!r}: "
                f"{entry['error_type']}: {entry['message']}"
            )
        super().__init__("\n".join(lines))


class UnresolvedWriteScopeError(ValueError):
    """Raised when one or more packets have empty write scope at compile.

    Per architecture-policy::platform-architecture::fail-closed-at-compile-
    no-silent-defaults, explicit packets without a ``write`` list cannot
    silently pass through to dispatch as workspace-wide ``["."]``. Source-
    authority-resolved packets (bugs, roadmap, ideas, friction) get their
    write scope populated by the resolver from authority metadata — if you
    supplied description-only prose without a source_ref and without
    write, the compiler cannot infer the file set and names the gap
    explicitly.

    Phase 1.5 converts this into a typed_gap row with
    ``missing_type='write_scope'`` and
    ``legal_repair_actions=['supply_write', 'add_source_ref', 'run_scope_resolver']``.
    The ``unresolved_writes`` attribute is a list of
    ``{index, label, description_preview}`` dicts so callers can construct
    the repair packet themselves.
    """

    def __init__(self, unresolved_writes: list[dict[str, Any]]) -> None:
        self.unresolved_writes = list(unresolved_writes)
        lines = [
            f"{len(unresolved_writes)} packet(s) have empty write scope "
            f"(no file list declared and no source authority to infer from):"
        ]
        for entry in unresolved_writes:
            lines.append(
                f"  - packet[{entry['index']}] label={entry['label']!r} "
                f"description={entry['description_preview']!r}"
            )
        lines.append(
            "Repair paths: (1) add explicit write=[...] to the packet, "
            "(2) supply source_refs=['BUG-X' | 'roadmap_item.Y' | ...] "
            "so the resolver derives write from authority, (3) call the "
            "scope_resolver before launching. (typed_gap conversion queued "
            "in Phase 1.5.)"
        )
        super().__init__("\n".join(lines))


class UnresolvedStageError(ValueError):
    """Raised when one or more packet stages have no admitted template.

    Per architecture-policy::platform-architecture::fail-closed-at-compile-
    no-silent-defaults, an unknown packet.stage cannot pass through to
    ``_packet_to_job`` with a fake ``auto/{stage}`` route string that later
    fails at ``_generate_prompt`` as a raw ``KeyError``. This error surfaces
    the problem atomically at compile — all unresolved stages collected
    before raise, so the caller fixes the full set in one pass.

    Phase 1.5 converts this into a typed_gap row with
    ``missing_type='stage_template'`` and
    ``legal_repair_actions=['add_stage_template', 'use_known_stage']``; for
    now the structured exception carries ``unresolved_stages`` as a list of
    ``{index, label, stage}`` dicts so callers can construct the repair
    packet themselves.
    """

    def __init__(self, unresolved_stages: list[dict[str, Any]]) -> None:
        self.unresolved_stages = list(unresolved_stages)
        known = sorted(_STAGE_TEMPLATES.keys())
        lines = [
            f"{len(unresolved_stages)} packet(s) have unresolved stages "
            f"(no template admitted in _STAGE_TEMPLATES):"
        ]
        for entry in unresolved_stages:
            lines.append(
                f"  - packet[{entry['index']}] label={entry['label']!r} "
                f"stage={entry['stage']!r}"
            )
        lines.append(
            f"Known stages: {', '.join(known)}. Add a template for a new "
            "stage, or use a known one. (typed_gap conversion queued in "
            "Phase 1.5 per fail-closed-at-compile-no-silent-defaults.)"
        )
        super().__init__("\n".join(lines))


def _deterministic_workflow_id(plan_obj: Plan) -> str:
    """Hash the plan's stable content so the same plan compiles to the same
    workflow_id. Explicit plan.workflow_id still wins — this only runs when
    the caller hasn't supplied one.
    """
    payload = {
        "name": plan_obj.name,
        "why": plan_obj.why,
        "phase": plan_obj.phase,
        "packets": [
            {
                "description": p.description,
                "write": list(p.write),
                "stage": p.stage,
                "label": p.label,
                "read": list(p.read) if p.read else None,
                "depends_on": list(p.depends_on) if p.depends_on else None,
                "bug_ref": p.bug_ref,
                "bug_refs": list(p.bug_refs) if p.bug_refs else None,
                "agent": p.agent,
                "complexity": p.complexity,
            }
            for p in plan_obj.packets
        ],
    }
    return f"plan.{stable_hash(payload)[:16]}"


def _plan_execution_manifest(
    *,
    plan_obj: Plan,
    workflow_id: str,
    jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest_jobs: list[dict[str, Any]] = []
    verify_refs: list[str] = []
    write_scope: list[str] = []
    read_scope: list[str] = []
    for job in jobs:
        job_verify_refs = list(job.get("verify_refs") or [])
        job_write_scope = list(job.get("write_scope") or [])
        job_read_scope = list(job.get("read_scope") or [])
        verify_refs.extend(str(ref) for ref in job_verify_refs if str(ref).strip())
        write_scope.extend(str(path) for path in job_write_scope if str(path).strip())
        read_scope.extend(str(path) for path in job_read_scope if str(path).strip())
        manifest_jobs.append(
            {
                "label": job.get("label"),
                "agent": job.get("agent"),
                "task_type": job.get("task_type"),
                "write_scope": job_write_scope,
                "read_scope": job_read_scope,
                "verify_refs": job_verify_refs,
                "bug_ref": job.get("bug_ref"),
                "bug_refs": list(job.get("bug_refs") or []),
                "depends_on": list(job.get("depends_on") or []),
                "consumes": list(job.get("consumes") or []),
                "consumes_any": list(job.get("consumes_any") or []),
                "produces": list(job.get("produces") or []),
            }
        )
    manifest_payload = {
        "manifest_kind": "launch_plan_inline_execution_manifest",
        "plan_name": plan_obj.name,
        "workflow_id": workflow_id,
        "phase": plan_obj.phase,
        "why": plan_obj.why,
        "source_refs": list(plan_obj.source_refs or []),
        "jobs": manifest_jobs,
        "write_scope": list(dict.fromkeys(write_scope)),
        "declared_read_scope": list(dict.fromkeys(read_scope)),
        "verify_refs": list(dict.fromkeys(verify_refs)),
    }
    definition_revision = f"definition.{stable_hash(manifest_payload)[:16]}"
    manifest_revision = f"manifest.{stable_hash({**manifest_payload, 'definition_revision': definition_revision})[:16]}"
    execution_manifest_ref = f"execution_manifest:{workflow_id}:{definition_revision}:{manifest_revision}"
    return {
        "execution_manifest_version": 1,
        "execution_manifest_ref": execution_manifest_ref,
        "definition_revision": definition_revision,
        "manifest_revision": manifest_revision,
        **manifest_payload,
        "hardening_report": {
            "status": "inline_compiled",
            "source": "runtime.spec_compiler.compile_plan",
            "job_count": len(jobs),
            "verify_ref_count": len(manifest_payload["verify_refs"]),
        },
    }


def compile_plan(
    plan: Plan | dict,
    *,
    conn: Any,
    workdir: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Compile a plan into a submittable workflow-spec dict.

    Returns (spec_dict, warnings). No submission; this is the pure translation
    step. Use :func:`launch_plan` for the full continuous flow.

    Atomic: if any packet fails to compile, collects every per-packet failure
    before raising :class:`CompilePlanError`. No partial spec_dict returned.
    The caller fixes all packets at once instead of fix-one-retry-one.

    Idempotent: when ``plan.workflow_id`` is not supplied, the workflow_id is
    derived from a stable hash of the plan content. Same plan in → same
    workflow_id out, so repeated compile_plan calls produce the same spec
    (submission idempotency then composes through submit_workflow_command's
    existing idempotency_key).
    """
    plan_obj = _coerce_plan(plan, conn=conn)
    if not plan_obj.packets:
        raise ValueError("plan.packets must have at least one packet")

    resolved_workdir = str(workdir or plan_obj.workdir or os.getcwd())
    warnings_all: list[str] = []
    jobs: list[dict[str, Any]] = []
    used_labels: set[str] = set()
    failures: list[dict[str, Any]] = []
    unresolved_stages: list[dict[str, Any]] = []
    unresolved_writes: list[dict[str, Any]] = []

    for index, packet in enumerate(plan_obj.packets):
        packet_label = (
            packet.label
            or packet.bug_ref
            or (packet.bug_refs[0] if packet.bug_refs else None)
            or f"packet_{index}"
        )
        # Pre-validate write scope per architecture-policy::platform-
        # architecture::fail-closed-at-compile-no-silent-defaults. An empty
        # write list means the packet has no declared output target AND no
        # source-authority resolver populated one; silently defaulting to
        # workspace-root would break the fail-closed contract.
        if not packet.write:
            unresolved_writes.append(
                {
                    "index": index,
                    "label": packet_label,
                    "description_preview": (packet.description or "")[:80],
                }
            )
            continue
        # Pre-validate stage admission. An unknown stage would otherwise
        # pass through to ``_packet_to_job`` as a fake ``auto/{stage}``
        # agent and raise a raw ``KeyError`` inside ``_generate_prompt``.
        if packet.stage not in _STAGE_TEMPLATES:
            unresolved_stages.append(
                {
                    "index": index,
                    "label": packet_label,
                    "stage": packet.stage,
                }
            )
            continue
        intent_dict: dict[str, Any] = {
            "description": packet.description,
            "write": list(packet.write),
            "stage": packet.stage,
        }
        if packet.read:
            intent_dict["read"] = list(packet.read)
        if packet.label:
            intent_dict["label"] = packet.label

        try:
            compiled, warnings = compile_spec(intent_dict, conn=conn)
        except Exception as exc:
            failures.append(
                {
                    "index": index,
                    "label": packet_label,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            )
            continue

        warnings_all.extend(f"{packet_label}: {w}" for w in warnings)
        job = _packet_to_job(packet, compiled=compiled, workdir=resolved_workdir, index=index)
        # Bind data pills per architecture-policy::platform-architecture::
        # data-pill-primitive-family — compile-time extraction of
        # object.field refs from prose so the packet_map surfaces
        # bound / ambiguous / unbound pills alongside capabilities + gates.
        job["data_pills"] = _bind_packet_data_pills(packet.description, conn=conn)
        base_label = job["label"]
        label = base_label
        suffix = 1
        while label in used_labels:
            suffix += 1
            label = f"{base_label}__{suffix}"
        if label != base_label:
            job["label"] = label
        used_labels.add(label)
        jobs.append(job)

    # Priority order: write scope is most fundamental (packet has no output
    # target at all); stage is the next gate (vocabulary match); other
    # compile failures follow. Fixing them in this order minimizes retry
    # cycles — each raise surfaces the earliest blocker atomically across
    # all packets.
    #
    # Before raising, emit typed_gap.created events for structured gap
    # errors so observers (Moon, operator console, projections) see the
    # failure at event-stream level — not just as an exception string.
    # Opt-in via conn: if no conn, skip emission (unit-test paths stay
    # cheap). Per architecture-policy::platform-architecture::conceptual-
    # events-register-through-operation-catalog-registry + fail-closed-
    # at-compile-no-silent-defaults.
    if unresolved_writes:
        err_write = UnresolvedWriteScopeError(unresolved_writes)
        if conn is not None:
            try:
                from runtime.typed_gap_events import (
                    emit_typed_gaps_for_compile_errors,
                )

                emit_typed_gaps_for_compile_errors(
                    conn, err_write, source_ref=f"compile_plan:{plan_obj.name}"
                )
            except Exception:
                # Best-effort — never let a degraded event bus block the
                # raise of the underlying error.
                pass
        raise err_write
    if unresolved_stages:
        err_stage = UnresolvedStageError(unresolved_stages)
        if conn is not None:
            try:
                from runtime.typed_gap_events import (
                    emit_typed_gaps_for_compile_errors,
                )

                emit_typed_gaps_for_compile_errors(
                    conn, err_stage, source_ref=f"compile_plan:{plan_obj.name}"
                )
            except Exception:
                pass
        raise err_stage
    if failures:
        raise CompilePlanError(failures)

    resolved_workflow_id = plan_obj.workflow_id or _deterministic_workflow_id(plan_obj)
    execution_manifest = _plan_execution_manifest(
        plan_obj=plan_obj,
        workflow_id=resolved_workflow_id,
        jobs=jobs,
    )
    spec_dict: dict[str, Any] = {
        "name": plan_obj.name,
        "workflow_id": resolved_workflow_id,
        "definition_revision": execution_manifest["definition_revision"],
        "execution_manifest_ref": execution_manifest["execution_manifest_ref"],
        "execution_manifest": execution_manifest,
        "phase": plan_obj.phase,
        "workdir": resolved_workdir,
        "jobs": jobs,
    }
    if plan_obj.why:
        spec_dict["why"] = plan_obj.why
    return spec_dict, warnings_all


@dataclass(frozen=True)
class ProposedPlan:
    """Translated plan with preview, before any submission.

    Output of ``propose_plan``: the workflow spec the platform will submit
    plus the preview payload (resolved agents, rendered prompts, execution
    bundles, etc.) the caller needs to approve before spending resources.
    ``packet_declarations`` also carry a ``data_pills`` entry — the output
    of ``bind_data_pills`` against each packet's description, so the caller
    sees any typo'd / hallucinated field refs before spending compile time.
    ``binding_summary`` rolls up the bound/ambiguous/unbound counts across
    all packets.
    """

    spec_dict: dict[str, Any]
    preview: dict[str, Any]
    warnings: list[str]
    workflow_id: str
    spec_name: str
    total_jobs: int
    packet_declarations: list[dict[str, Any]]
    binding_summary: dict[str, Any]
    unresolved_routes: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_dict": dict(self.spec_dict),
            "preview": dict(self.preview),
            "warnings": list(self.warnings),
            "workflow_id": self.workflow_id,
            "spec_name": self.spec_name,
            "total_jobs": self.total_jobs,
            "packet_declarations": list(self.packet_declarations),
            "binding_summary": dict(self.binding_summary),
            "unresolved_routes": list(self.unresolved_routes),
        }


def propose_plan(
    plan: Plan | dict,
    *,
    conn: Any,
    workdir: str | None = None,
) -> ProposedPlan:
    """Translate + preview a plan without submitting.

    HONEST SCOPE: this covers the translation + per-step authoring that
    the platform can do deterministically (prompt from stage template,
    model routing via ``TaskTypeRouter``, failover via ``RoutePlan``,
    verify_refs for declared write scope). It does NOT cover the upstream
    planning layers the caller still owns:

    - Layer 1 (Bind): extracting data pills (fields/objects) from prose
      intent and validating them against schema/capability authority.
    - Layer 2 (Decompose): splitting prose intent into distinct executable
      steps.
    - Layer 3 (Re-order): reordering steps by data-flow topology (which
      step's output feeds which step's input).
    - Most of layer 4 (Author): writing the actual step prompt. The stage
      template is a shim, not a real authoring pass.

    Use this instead of ``launch_plan`` when the caller (user or LLM) must
    inspect the translated spec and approve before resources spend. The
    returned ``ProposedPlan`` contains everything needed to either run
    ``launch_proposed`` or discard.
    """
    spec_dict, warnings_all = compile_plan(plan, conn=conn, workdir=workdir)
    plan_obj = _coerce_plan(plan, conn=conn)

    from runtime.workflow._admission import preview_workflow_execution

    preview = preview_workflow_execution(conn, inline_spec=spec_dict)

    binding_totals = {"bound": 0, "ambiguous": 0, "unbound": 0}
    binding_unbound: list[dict[str, Any]] = []
    binding_ambiguous: list[dict[str, Any]] = []
    binding_error: str | None = None

    packet_declarations: list[dict[str, Any]] = []
    for packet, job in zip(plan_obj.packets, spec_dict["jobs"]):
        # compile_plan already bound data pills via _bind_packet_data_pills
        # and attached them to the job dict; reuse rather than re-binding
        # (DRY per data-pill-primitive-family policy — one helper, one
        # source of truth for binding results).
        data_pills = dict(
            job.get("data_pills")
            or {"bound": [], "ambiguous": [], "unbound": [], "warnings": []}
        )
        binding_totals["bound"] += len(data_pills.get("bound") or [])
        binding_totals["ambiguous"] += len(data_pills.get("ambiguous") or [])
        binding_totals["unbound"] += len(data_pills.get("unbound") or [])
        for entry in data_pills.get("unbound") or []:
            binding_unbound.append({"label": job["label"], **entry})
        for entry in data_pills.get("ambiguous") or []:
            binding_ambiguous.append({"label": job["label"], **entry})
        for warn in data_pills.get("warnings") or []:
            if binding_error is None and str(warn).startswith("intent_binding unavailable"):
                binding_error = str(warn)
        # Add resolved bound pills to the job's prompt so the agent sees the
        # typed fields it's expected to work with, not just the free-prose
        # description. Non-destructive — appends after the Context block
        # already set by _enrich_prompt_with_context in _packet_to_job.
        bound_entries = data_pills.get("bound") or []
        if bound_entries:
            field_refs = []
            for entry in bound_entries:
                object_kind = entry.get("object_kind")
                field_path = entry.get("field_path")
                field_kind = entry.get("field_kind") or "unknown"
                if object_kind and field_path:
                    field_refs.append(f"{object_kind}.{field_path} ({field_kind})")
            if field_refs:
                job["prompt"] = (
                    job["prompt"].rstrip()
                    + "\n- Bound data fields: "
                    + ", ".join(field_refs)
                )
        packet_declarations.append(
            {
                "label": job["label"],
                "declared_description": packet.description,
                "declared_write": list(packet.write),
                "declared_stage": packet.stage,
                "declared_label": packet.label,
                "declared_depends_on": list(packet.depends_on) if packet.depends_on else None,
                "declared_bug_ref": packet.bug_ref,
                "declared_bug_refs": list(packet.bug_refs) if packet.bug_refs else None,
                "declared_agent": packet.agent,
                "declared_complexity": packet.complexity,
                "data_pills": data_pills,
            }
        )

    binding_summary: dict[str, Any] = {
        "totals": binding_totals,
        "unbound_refs": binding_unbound,
        "ambiguous_refs": binding_ambiguous,
    }
    if binding_error:
        binding_summary["error"] = binding_error
        warnings_all = [*warnings_all, binding_error]

    # Source-authority-derived packets default write=["."] (workspace root).
    # Warn so the caller can narrow before launch if they have a tighter
    # scope in mind. ``source_refs`` is canonical; legacy ``from_*`` fields
    # are checked for belt-and-suspenders backwards compat when a Plan is
    # constructed directly (not via _coerce_plan).
    if (
        plan_obj.source_refs
        or plan_obj.from_bugs
        or plan_obj.from_roadmap_items
        or plan_obj.from_ideas
        or plan_obj.from_friction
    ):
        broad_scope_labels = sorted(
            packet["label"]
            for packet in packet_declarations
            if packet["declared_write"] == ["."]
        )
        if broad_scope_labels:
            warnings_all = [
                *warnings_all,
                (
                    f"{len(broad_scope_labels)} source-derived packet(s) "
                    f"{broad_scope_labels} default write scope to workspace root; "
                    "narrow before launch if you know the target files"
                ),
            ]
    if binding_unbound:
        labels = sorted({entry["label"] for entry in binding_unbound})
        warnings_all = [
            *warnings_all,
            (
                f"{len(binding_unbound)} unbound data-pill reference(s) across "
                f"packet(s) {labels}; fix typos or drop hallucinated fields "
                f"before launching"
            ),
        ]
    if binding_ambiguous:
        labels = sorted({entry["label"] for entry in binding_ambiguous})
        warnings_all = [
            *warnings_all,
            (
                f"{len(binding_ambiguous)} ambiguous data-pill reference(s) "
                f"across packet(s) {labels}; disambiguate before launching"
            ),
        ]

    # Confidence floor: surface unresolved auto routes so the caller can
    # decide before approval whether the plan is worth launching. The
    # resolver has already run inside preview_workflow_execution; we just
    # roll up jobs whose route_status != "resolved" and isn't explicit.
    unresolved_routes: list[dict[str, Any]] = []
    for preview_job in preview.get("jobs") or []:
        status = str(preview_job.get("route_status") or "").strip()
        if status not in {"resolved", "explicit", "not_applicable"}:
            unresolved_routes.append(
                {
                    "label": preview_job.get("label"),
                    "requested_agent": preview_job.get("requested_agent"),
                    "resolved_agent": preview_job.get("resolved_agent"),
                    "route_status": status or "unknown",
                    "route_reason": preview_job.get("route_reason"),
                }
            )
    if unresolved_routes:
        labels = sorted(
            {str(entry["label"]) for entry in unresolved_routes if entry.get("label")}
        )
        warnings_all = [
            *warnings_all,
            (
                f"{len(unresolved_routes)} job(s) have unresolved agent routes "
                f"across packet(s) {labels}; launch_approved will still submit, "
                "but the workflow will fail at dispatch unless routes resolve "
                "before the run starts"
            ),
        ]

    return ProposedPlan(
        spec_dict=spec_dict,
        preview=preview,
        warnings=warnings_all,
        workflow_id=str(spec_dict["workflow_id"]),
        spec_name=str(spec_dict["name"]),
        total_jobs=len(spec_dict["jobs"]),
        packet_declarations=packet_declarations,
        binding_summary=binding_summary,
        unresolved_routes=unresolved_routes,
    )


@dataclass(frozen=True)
class ApprovedPlan:
    """A ProposedPlan bound to an explicit approval record.

    Produced by :func:`approve_proposed_plan`. The hash is computed over the
    ProposedPlan's spec_dict at approval time; :func:`launch_approved`
    re-hashes the plan's spec_dict at submit time and refuses to launch if
    the hashes disagree — so a spec that was tampered with between approve
    and launch (whether by accident or intent) fails closed.

    ``approved_by`` is whatever identifier the caller uses to name the
    approver: an operator email, an agent slug, a CI system name. The field
    is free-form by design; callers upstream decide what identity kinds
    they accept.
    """

    proposed: ProposedPlan
    approved_by: str
    approved_at: str  # ISO-8601 timestamp, assigned at approve time
    proposal_hash: str
    approval_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "proposed": self.proposed.to_dict(),
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
            "proposal_hash": self.proposal_hash,
            "approval_note": self.approval_note,
        }


def _hash_proposed_plan(proposed: ProposedPlan) -> str:
    """Hash the spec_dict — the only part that actually gets submitted.

    Preview payload, binding_summary, and packet_declarations are derived
    views that can shift between propose calls without invalidating the
    approval. Only the spec_dict the platform will actually run gets hashed.
    """
    return stable_hash(proposed.spec_dict)


def approve_proposed_plan(
    proposed: ProposedPlan,
    *,
    approved_by: str,
    approval_note: str | None = None,
) -> ApprovedPlan:
    """Wrap a :class:`ProposedPlan` with an explicit approval record.

    No submission here — just records who approved what, when, and bakes
    the spec_dict hash so downstream launch can detect tampering.
    """
    approver = str(approved_by or "").strip()
    if not approver:
        raise ValueError("approved_by is required — pick an identifier for the approver")
    proposal_hash = _hash_proposed_plan(proposed)
    approved_at = datetime.now(timezone.utc).isoformat()
    return ApprovedPlan(
        proposed=proposed,
        approved_by=approver,
        approved_at=approved_at,
        proposal_hash=proposal_hash,
        approval_note=str(approval_note).strip() if approval_note else None,
    )


class ApprovalHashMismatchError(ValueError):
    """Raised when an ApprovedPlan's recorded hash no longer matches its spec.

    Means the ProposedPlan was modified between approval and launch.
    Launch fails closed; caller must re-propose + re-approve.
    """


class LaunchSubmitFailedError(RuntimeError):
    """Raised when submit_workflow_command did not produce a runnable workflow run.

    The CQRS bus accepted the command row but the dispatch path returned a
    failed/approval_required status (or empty run_id). Returning a
    LaunchReceipt with ``run_id=""`` would silently lie to the caller —
    every wrapper above would then have no signal that nothing executed.
    Fail-closed instead.

    ``submit_result`` carries the raw payload from
    :func:`render_workflow_submit_response` so callers can surface
    error_code / error_detail / command_id for the operator.
    """

    def __init__(self, submit_result: dict[str, Any], *, spec_name: str) -> None:
        self.submit_result = dict(submit_result)
        self.spec_name = spec_name
        self.status = str(submit_result.get("status") or "unknown")
        self.error_code = str(
            submit_result.get("error_code")
            or "control.command.workflow_submit_missing_run_id"
        )
        self.error_detail = str(
            submit_result.get("error_detail")
            or submit_result.get("error")
            or "workflow submit command did not produce a workflow run"
        )
        super().__init__(
            f"launch of {spec_name!r} failed at submit: status={self.status} "
            f"error_code={self.error_code} detail={self.error_detail}"
        )


def _ensure_run_id_or_raise(
    submit_result: dict[str, Any], *, spec_name: str
) -> str:
    """Return a non-empty run_id from a submit_result, or raise LaunchSubmitFailedError.

    The CQRS submit path returns ``status='failed'`` / ``'approval_required'``
    with no run_id when dispatch can't materialize a workflow run. Coercing
    that to an empty string and constructing a LaunchReceipt anyway means
    the caller has no signal that nothing executed. This helper centralizes
    the fail-closed behavior for every launch_* function.
    """
    status = str(submit_result.get("status") or "")
    run_id_value = submit_result.get("run_id")
    if status in {"failed", "approval_required"} or not (
        isinstance(run_id_value, str) and run_id_value.strip()
    ):
        raise LaunchSubmitFailedError(submit_result, spec_name=spec_name)
    return run_id_value


def launch_approved(
    approved: ApprovedPlan,
    *,
    conn: Any,
    requested_by_kind: str = "workflow",
    requested_by_ref: str | None = None,
) -> LaunchReceipt:
    """Submit an :class:`ApprovedPlan` — strictly typed launch path.

    Recomputes the spec_dict hash and refuses to launch if it differs from
    the approval's recorded hash. A mismatch means the proposal was
    tampered with after approval; launch fails closed. Caller must
    re-propose + re-approve to get a fresh approval on the new spec.

    ``approved_by`` is threaded into the control-command bus as the
    ``requested_by_ref`` so the audit trail shows who approved the launch.
    """
    current_hash = _hash_proposed_plan(approved.proposed)
    if current_hash != approved.proposal_hash:
        raise ApprovalHashMismatchError(
            f"ApprovedPlan hash mismatch — the spec changed after approval by "
            f"{approved.approved_by!r} at {approved.approved_at}. "
            f"Expected {approved.proposal_hash!r}, got {current_hash!r}. "
            "Re-propose and re-approve before launching."
        )

    receipt = launch_proposed(
        approved.proposed,
        conn=conn,
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref or approved.approved_by,
    )
    return receipt


def launch_proposed(
    proposed: ProposedPlan,
    *,
    conn: Any,
    requested_by_kind: str = "workflow",
    requested_by_ref: str | None = None,
) -> LaunchReceipt:
    """Submit a ``ProposedPlan`` previously built by ``propose_plan``.

    Two-phase alternative to ``launch_plan``: propose → inspect → launch.
    Use when the caller must approve the translated spec before it runs.
    """
    from runtime.control_commands import submit_workflow_command

    submit_result = submit_workflow_command(
        conn,
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref or proposed.spec_name,
        inline_spec=proposed.spec_dict,
        spec_name=proposed.spec_name,
        total_jobs=proposed.total_jobs,
        dispatch_reason=f"launch_proposed:{proposed.spec_name}",
    )
    run_id = _ensure_run_id_or_raise(submit_result, spec_name=proposed.spec_name)

    packet_map: list[dict[str, Any]] = [
        _build_packet_map_entry(job=job) for job in proposed.spec_dict["jobs"]
    ]
    warnings = list(proposed.warnings)
    event_error = _emit_plan_launched_event(
        conn,
        run_id=run_id,
        workflow_id=proposed.workflow_id,
        spec_name=proposed.spec_name,
        total_jobs=int(submit_result.get("total_jobs") or proposed.total_jobs),
        packet_labels=[
            str(job.get("label") or "") for job in proposed.spec_dict["jobs"]
        ],
        source_refs=None,  # launch_proposed doesn't retain the original plan_obj
    )
    if event_error:
        warnings.append(event_error)
    return LaunchReceipt(
        run_id=run_id,
        spec_name=proposed.spec_name,
        workflow_id=proposed.workflow_id,
        total_jobs=int(submit_result.get("total_jobs") or proposed.total_jobs),
        packet_map=packet_map,
        warnings=warnings,
    )


def launch_plan(
    plan: Plan | dict,
    *,
    conn: Any,
    workdir: str | None = None,
    requested_by_kind: str = "workflow",
    requested_by_ref: str | None = None,
) -> LaunchReceipt:
    """Translate a packet list into a workflow spec and submit it in one call.

    HONEST SCOPE — this is the layer-5 translation primitive, not a planner.
    It owns:

    - Per-packet prompt rendering from the stage template (partial layer 4)
    - Model routing via ``TaskTypeRouter`` (layer 5)
    - Failover chain via ``RoutePlan`` (layer 6)
    - Submission through the ``submit_workflow_command`` CQRS bus (layer 7)

    It does NOT own:

    - Layer 1 (Bind): extracting + validating data pills
    - Layer 2 (Decompose): prose → discrete steps
    - Layer 3 (Re-order): data-flow-aware step ordering
    - Most of layer 4 (Author): real per-step prompt authoring

    Those planning layers are the caller's responsibility (user or LLM).
    If you need to see what will actually run before spend, use
    ``propose_plan`` for the translate-and-preview pair, then
    ``launch_proposed`` to submit only if the proposal is approved.
    """
    spec_dict, warnings_all = compile_plan(plan, conn=conn, workdir=workdir)
    plan_obj = _coerce_plan(plan, conn=conn)

    from runtime.control_commands import submit_workflow_command

    submit_result = submit_workflow_command(
        conn,
        requested_by_kind=requested_by_kind,
        requested_by_ref=requested_by_ref or plan_obj.name,
        inline_spec=spec_dict,
        spec_name=plan_obj.name,
        total_jobs=len(spec_dict["jobs"]),
        dispatch_reason=f"launch_plan:{plan_obj.name}",
    )
    run_id = _ensure_run_id_or_raise(submit_result, spec_name=plan_obj.name)

    packet_map: list[dict[str, Any]] = [
        _build_packet_map_entry(packet=packet, job=job)
        for packet, job in zip(plan_obj.packets, spec_dict["jobs"])
    ]
    event_error = _emit_plan_launched_event(
        conn,
        run_id=run_id,
        workflow_id=str(spec_dict["workflow_id"]),
        spec_name=plan_obj.name,
        total_jobs=int(submit_result.get("total_jobs") or len(spec_dict["jobs"])),
        packet_labels=[str(job.get("label") or "") for job in spec_dict["jobs"]],
        source_refs=list(plan_obj.source_refs or []),
    )
    if event_error:
        warnings_all = [*warnings_all, event_error]
    return LaunchReceipt(
        run_id=run_id,
        spec_name=plan_obj.name,
        workflow_id=str(spec_dict["workflow_id"]),
        total_jobs=int(submit_result.get("total_jobs") or len(spec_dict["jobs"])),
        packet_map=packet_map,
        warnings=warnings_all,
    )
