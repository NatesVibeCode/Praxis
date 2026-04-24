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
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any
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

# Stage → tier mapping
_STAGE_TO_TIER: dict[str, str] = {
    "research": "economy",
    "review": "mid",
    "build": "mid",
    "fix": "mid",
    "test": "economy",
}

# Stage → prompt template
_STAGE_TEMPLATES: dict[str, str] = {
    "build": "Implement the following in {write_scope}:\n\n{description}",
    "fix": "Fix the following issue in {write_scope}:\n\n{description}",
    "review": "Review {write_scope} for the following:\n\n{description}",
    "test": "Write tests for {write_scope}:\n\n{description}",
    "research": "Research and report on the following:\n\n{description}",
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


def _generate_prompt(stage: str, write_scope: list[str], description: str) -> str:
    """Generate a prompt from stage template and description."""
    template = _STAGE_TEMPLATES[stage]
    write_str = ", ".join(write_scope) if len(write_scope) > 1 else (write_scope[0] if write_scope else "")
    return template.format(write_scope=write_str, description=description)


def _verify_ref_name(verification_ref: str, file_path: str) -> str:
    digest = sha256(f"{verification_ref}|{file_path}".encode("utf-8")).hexdigest()[:12]
    path_slug = re.sub(r"[^a-z0-9]+", "-", file_path.lower()).strip("-")
    ref_slug = verification_ref.replace(".", "-")
    return f"verify_ref.{ref_slug}.{path_slug}.{digest}"


def _generate_verify_refs(write_scope: list[str]) -> list[dict[str, Any]]:
    """Generate DB-backed verification ref rows from write scope."""
    refs: list[dict[str, Any]] = []

    for file_path in write_scope:
        if not file_path:
            continue

        if file_path.endswith(".py"):
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

    description/write/stage match Intent; everything else is derivable or
    optional wiring (label auto-generated, agent defaults to auto/<stage>,
    verify_refs computed from write scope).
    """

    description: str
    write: list[str]
    stage: str = "build"
    label: str | None = None
    read: list[str] | None = None
    depends_on: list[str] | None = None
    bug_ref: str | None = None
    agent: str | None = None
    complexity: str | None = None


@dataclass(frozen=True)
class Plan:
    """A named batch of packets, submitted as one workflow_run."""

    name: str
    packets: list[PlanPacket]
    workflow_id: str | None = None
    why: str | None = None
    phase: str = "build"
    workdir: str | None = None


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
        agent=value.get("agent"),
        complexity=value.get("complexity"),
    )


def _coerce_plan(plan: Any) -> Plan:
    if isinstance(plan, Plan):
        return plan
    if not isinstance(plan, dict):
        raise ValueError(
            f"plan must be a Plan or dict, got {type(plan).__name__}"
        )
    packets = [_coerce_packet(p) for p in (plan.get("packets") or [])]
    return Plan(
        name=str(plan.get("name") or "launch_plan").strip() or "launch_plan",
        packets=packets,
        workflow_id=(str(plan["workflow_id"]).strip() if plan.get("workflow_id") else None),
        why=plan.get("why"),
        phase=str(plan.get("phase") or "build"),
        workdir=plan.get("workdir"),
    )


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
    agent = packet.agent or f"auto/{packet.stage}"
    job: dict[str, Any] = {
        "label": label,
        "agent": agent,
        "prompt": compiled.prompt,
        "task_type": packet.stage,
        "write_scope": list(packet.write),
        "workdir": workdir,
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
    if packet.bug_ref:
        job["bug_ref"] = packet.bug_ref
    return job


def compile_plan(
    plan: Plan | dict,
    *,
    conn: Any,
    workdir: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Compile a plan into a submittable workflow-spec dict.

    Returns (spec_dict, warnings). No submission; this is the pure translation
    step. Use :func:`launch_plan` for the full continuous flow.
    """
    plan_obj = _coerce_plan(plan)
    if not plan_obj.packets:
        raise ValueError("plan.packets must have at least one packet")

    resolved_workdir = str(workdir or plan_obj.workdir or os.getcwd())
    warnings_all: list[str] = []
    jobs: list[dict[str, Any]] = []
    used_labels: set[str] = set()

    for index, packet in enumerate(plan_obj.packets):
        intent_dict: dict[str, Any] = {
            "description": packet.description,
            "write": list(packet.write),
            "stage": packet.stage,
        }
        if packet.read:
            intent_dict["read"] = list(packet.read)
        if packet.label:
            intent_dict["label"] = packet.label
        compiled, warnings = compile_spec(intent_dict, conn=conn)
        prefix = packet.label or packet.bug_ref or f"packet_{index}"
        warnings_all.extend(f"{prefix}: {w}" for w in warnings)
        job = _packet_to_job(packet, compiled=compiled, workdir=resolved_workdir, index=index)
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

    resolved_workflow_id = plan_obj.workflow_id or f"plan.{uuid.uuid4().hex[:12]}"
    spec_dict: dict[str, Any] = {
        "name": plan_obj.name,
        "workflow_id": resolved_workflow_id,
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
    Pass to ``launch_proposed`` to submit, or ignore to cancel.
    """

    spec_dict: dict[str, Any]
    preview: dict[str, Any]
    warnings: list[str]
    workflow_id: str
    spec_name: str
    total_jobs: int
    packet_declarations: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec_dict": dict(self.spec_dict),
            "preview": dict(self.preview),
            "warnings": list(self.warnings),
            "workflow_id": self.workflow_id,
            "spec_name": self.spec_name,
            "total_jobs": self.total_jobs,
            "packet_declarations": list(self.packet_declarations),
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
    plan_obj = _coerce_plan(plan)

    from runtime.workflow._admission import preview_workflow_execution

    preview = preview_workflow_execution(conn, inline_spec=spec_dict)

    packet_declarations: list[dict[str, Any]] = [
        {
            "label": job["label"],
            "declared_description": packet.description,
            "declared_write": list(packet.write),
            "declared_stage": packet.stage,
            "declared_label": packet.label,
            "declared_depends_on": list(packet.depends_on) if packet.depends_on else None,
            "declared_bug_ref": packet.bug_ref,
            "declared_agent": packet.agent,
            "declared_complexity": packet.complexity,
        }
        for packet, job in zip(plan_obj.packets, spec_dict["jobs"])
    ]

    return ProposedPlan(
        spec_dict=spec_dict,
        preview=preview,
        warnings=warnings_all,
        workflow_id=str(spec_dict["workflow_id"]),
        spec_name=str(spec_dict["name"]),
        total_jobs=len(spec_dict["jobs"]),
        packet_declarations=packet_declarations,
    )


def launch_proposed(
    proposed: ProposedPlan,
    *,
    conn: Any,
    requested_by_kind: str = "launch_plan",
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

    packet_map: list[dict[str, Any]] = [
        {
            "label": job.get("label"),
            "bug_ref": job.get("bug_ref"),
            "agent": job.get("agent"),
            "stage": job.get("task_type"),
        }
        for job in proposed.spec_dict["jobs"]
    ]
    return LaunchReceipt(
        run_id=str(submit_result.get("run_id") or ""),
        spec_name=proposed.spec_name,
        workflow_id=proposed.workflow_id,
        total_jobs=int(submit_result.get("total_jobs") or proposed.total_jobs),
        packet_map=packet_map,
        warnings=proposed.warnings,
    )


def launch_plan(
    plan: Plan | dict,
    *,
    conn: Any,
    workdir: str | None = None,
    requested_by_kind: str = "launch_plan",
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
    plan_obj = _coerce_plan(plan)

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

    packet_map: list[dict[str, Any]] = [
        {
            "label": job["label"],
            "bug_ref": packet.bug_ref,
            "agent": job["agent"],
            "stage": packet.stage,
        }
        for packet, job in zip(plan_obj.packets, spec_dict["jobs"])
    ]
    return LaunchReceipt(
        run_id=str(submit_result.get("run_id") or ""),
        spec_name=plan_obj.name,
        workflow_id=str(spec_dict["workflow_id"]),
        total_jobs=int(submit_result.get("total_jobs") or len(spec_dict["jobs"])),
        packet_map=packet_map,
        warnings=warnings_all,
    )
