"""Execution setup and surface/receipt builders for the operating model compiler."""

from __future__ import annotations

import json
import re
from typing import Any

from runtime.intent_lexicon import text_has_any


def build_execution_setup(
    *,
    title: str,
    definition: dict[str, Any],
    jobs: list[dict[str, Any]],
    unresolved: list[str],
    conn: Any | None,
    route_hints: tuple[tuple[str, str], ...] = (),
) -> dict[str, Any]:
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    capabilities = definition.get("capabilities") if isinstance(definition.get("capabilities"), list) else []
    trigger_intent = definition.get("trigger_intent") if isinstance(definition.get("trigger_intent"), list) else []
    draft_flow = definition.get("draft_flow") if isinstance(definition.get("draft_flow"), list) else []
    compiled_prose = _as_text(definition.get("compiled_prose"))

    capability_slugs = [_as_text(capability.get("slug")) for capability in capabilities if isinstance(capability, dict)]
    has_fanout = "research/fan-out" in capability_slugs
    has_local_knowledge = "research/local-knowledge" in capability_slugs
    has_external_scan = "research/gemini-cli" in capability_slugs or text_has_any(
        compiled_prose,
        "external",
        "web",
        "online",
        "internet",
        "browse",
        "broad sweep",
        "multiple sources",
        "cross-check",
    )
    has_research = has_local_knowledge or has_fanout or has_external_scan or text_has_any(
        compiled_prose,
        "research",
        "investigate",
        "analyze",
        "compare",
        "brief",
        "sources",
        "search",
        "api docs",
    )
    review_slugs = {
        _as_text(reference.get("slug"))
        for reference in references
        if isinstance(reference, dict)
        and _as_text(reference.get("type")) == "agent"
        and _infer_agent_route(
            _as_text(reference.get("slug")),
            reference,
            route_hints=route_hints,
        ) == "auto/review"
    }
    has_review = bool(review_slugs) or text_has_any(
        compiled_prose,
        "review",
        "validate",
        "verify",
        "judge",
        "audit",
        "check",
    )
    has_build_cues = text_has_any(
        compiled_prose,
        "build",
        "create",
        "created",
        "implement",
        "develop",
        "fix",
        "edit",
        "update",
        "change",
        "refactor",
    )
    has_workflow_cues = bool(trigger_intent) or text_has_any(
        compiled_prose,
        "workflow",
        "pipeline",
        "route",
        "routing",
        "trigger",
        "stage",
        "staged",
        "step",
        "handoff",
        "ingest",
        "closure",
        "approval",
    )
    has_connector_flow = text_has_any(
        compiled_prose,
        "connector",
        "common objects",
        "api docs",
        "application ui",
        "documentation",
    )
    has_multiple_jobs = len(jobs) > 1 or len(draft_flow) > 1
    has_workflow = has_multiple_jobs or has_workflow_cues or (
        has_connector_flow and has_build_cues and (has_research or has_review or text_has_any(compiled_prose, "test", "qa"))
    )
    # BUG-3330D2CD + architecture-policy::compile::retrieval-is-the-filter-no-
    # template-fallbacks (2026-04-25): the keyword-gated constant functions
    # infer_briefing_fields / infer_blocking_inputs / connector_flow_self_
    # scaffolds_inputs emitted a hardcoded 5-item list whenever prose
    # contained {connector, api docs, common objects, application, docs}.
    # That template preempted the 23 prose-grounded capability nodes in
    # definition_graph and turned the Moon build_graph render into 5 fake
    # "Resolve typed input gap" stubs. Retrieval is the filter — if it finds
    # nothing, a retrieval.no_match typed_gap fires downstream (future
    # commit). These two lists stay here as anchors so _build_execution_setup
    # keeps its shape, but they carry no content until retrieval-grounded
    # projection lands.
    briefing_fields: list[str] = []
    blocking_inputs: list[str] = []
    needs_long_running = has_fanout or text_has_any(
        compiled_prose,
        "long-running",
        "long running",
        "broad sweep",
        "multiple sources",
        "cross-check",
        "monitor",
        "ongoing",
    )
    requires_citations = has_research or text_has_any(
        compiled_prose,
        "citation",
        "source",
        "grounded",
        "evidence",
    )

    if has_connector_flow and has_workflow and has_build_cues:
        task_class = "workflow"
        method_key = "staged_execution"
        method_label = "Staged Execution"
        method_summary = (
            "Preserve the workflow as explicit intake, research, planning, and build stages with step-by-step handoffs."
        )
        runtime_profile_ref = "compile/workflow.staged"
        cost_tier = "medium"
        timeout_seconds = 1800 if has_research else 1200
        fanout_workers = 1
    elif has_research and (has_fanout or has_external_scan) and not has_connector_flow:
        task_class = "deep_research"
        method_key = "seed_fanout_synthesize"
        method_label = "Seed, Fan Out, Synthesize"
        method_summary = (
            "Use an expensive seed phase to decompose the research question, "
            "parallel workers for breadth, and a strong synthesis phase to reconcile findings."
        )
        runtime_profile_ref = "compile/research.deep.seed_fanout"
        cost_tier = "high"
        timeout_seconds = 3600 if needs_long_running else 2400
        fanout_workers = 4
    elif has_workflow and has_build_cues and text_has_any(compiled_prose, "workflow", "stage", "staged", "trigger", "route"):
        task_class = "workflow"
        method_key = "staged_execution"
        method_label = "Staged Execution"
        method_summary = (
            "Preserve the workflow as explicit intake, research, planning, and build stages with step-by-step handoffs."
        )
        runtime_profile_ref = "compile/workflow.staged"
        cost_tier = "medium"
        timeout_seconds = 1800 if has_research else 1200
        fanout_workers = 1
    elif has_research:
        task_class = "research"
        method_key = "grounded_research"
        method_label = "Grounded Research"
        method_summary = (
            "Use a research-biased setup with explicit grounding, source capture, and a final verification pass."
        )
        runtime_profile_ref = "compile/research.grounded"
        cost_tier = "medium"
        timeout_seconds = 1800
        fanout_workers = 1
    elif has_workflow:
        task_class = "workflow"
        method_key = "staged_execution"
        method_label = "Staged Execution"
        method_summary = (
            "Build a staged setup that preserves ordering, trigger semantics, and explicit step boundaries."
        )
        runtime_profile_ref = "compile/workflow.staged"
        cost_tier = "medium"
        timeout_seconds = 1200
        fanout_workers = 1
    elif has_review and (has_build_cues or has_multiple_jobs):
        task_class = "build_review"
        method_key = "implement_then_review"
        method_label = "Implement Then Review"
        method_summary = (
            "Split execution into build and review phases so the output is checked by a separate agent before acceptance."
        )
        runtime_profile_ref = "compile/build.review_gated"
        cost_tier = "medium"
        timeout_seconds = 1500
        fanout_workers = 1
    else:
        task_class = "task"
        method_key = "single_agent"
        method_label = "Single Agent"
        method_summary = (
            "Keep the setup direct: one bounded agent phase with explicit prompt and controls."
        )
        runtime_profile_ref = "compile/task.single"
        cost_tier = "low"
        timeout_seconds = 900
        fanout_workers = 1

    route_catalog = load_agent_route_catalog(conn)
    phases = build_execution_phases(
        title=title,
        method_key=method_key,
        has_review=has_review,
        requires_citations=requires_citations,
        needs_long_running=needs_long_running,
        fanout_workers=fanout_workers,
        route_catalog=route_catalog,
        draft_flow=draft_flow,
        blocking_inputs=blocking_inputs,
    )

    return {
        "setup_version": 1,
        "setup_state": "compiled_preview",
        "planner_required": True,
        "title": title,
        "task_class": task_class,
        "runtime_profile_ref": runtime_profile_ref,
        "method": {
            "key": method_key,
            "label": method_label,
            "summary": method_summary,
        },
        "constraints": {
            "requires_citations": requires_citations,
            "long_running": needs_long_running,
            "review_required": has_review,
            "unresolved_references": list(unresolved),
            "briefing_fields": briefing_fields,
            "blocking_inputs": blocking_inputs,
        },
        "budget_policy": {
            "cost_tier": cost_tier,
            "timeout_seconds": timeout_seconds,
            "fanout_workers": fanout_workers,
            "completion_tier": "extended" if needs_long_running or cost_tier == "high" else "standard",
        },
        "phase_count": len(phases),
        "trigger_count": len(trigger_intent),
        "phases": phases,
        "reference_slugs": [
            _as_text(reference.get("slug"))
            for reference in references
            if isinstance(reference, dict) and _as_text(reference.get("slug"))
        ],
        "capability_slugs": [slug for slug in capability_slugs if slug],
    }


def build_execution_phases(
    *,
    title: str,
    method_key: str,
    has_review: bool,
    requires_citations: bool,
    needs_long_running: bool,
    fanout_workers: int,
    route_catalog: dict[str, dict[str, Any]],
    draft_flow: list[dict[str, Any]],
    blocking_inputs: list[str],
) -> list[dict[str, Any]]:
    first_step_id = _first_draft_flow_step_id(draft_flow)
    if method_key == "seed_fanout_synthesize":
        phases = [
            make_execution_phase(
                phase_id="phase-001",
                step_id=first_step_id,
                kind="seed",
                title="Seed research plan",
                purpose=(
                    "Turn the workflow objective into a search plan, sub-questions, and bounded worker briefs "
                    f"for {title or 'the current operating model'}."
                ),
                agent_route="auto/high",
                system_prompt=(
                    "You are the lead research strategist. Break the task into a plan, design the worker briefs, "
                    "and keep the synthesis contract explicit before fan-out starts."
                ),
                temperature=0.1,
                max_tokens=6000,
                timeout_seconds=900,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                outputs=["search plan", "worker briefs", "synthesis contract"],
            ),
            make_execution_phase(
                phase_id="phase-002",
                kind="fanout",
                title="Run parallel research workers",
                purpose="Investigate parallel slices of the question so breadth stays cheap while coverage expands.",
                agent_route="auto/research",
                system_prompt=(
                    "You are a bounded research worker. Investigate only your assigned slice, capture evidence, "
                    "note contradictions, and avoid writing the final synthesis."
                ),
                temperature=0.1,
                max_tokens=3200,
                timeout_seconds=1200 if needs_long_running else 900,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                outputs=["source-backed findings", "open questions", "conflicts"],
                fanout_count=fanout_workers,
            ),
            make_execution_phase(
                phase_id="phase-003",
                kind="synthesis",
                title="Synthesize and reconcile",
                purpose="Merge the worker results, resolve conflicts, and produce the decision-ready output.",
                agent_route="auto/reasoning",
                system_prompt=(
                    "You are the synthesis lead. Reconcile cross-source findings, rank confidence, preserve citations, "
                    "and keep unsupported claims out of the final output."
                ),
                temperature=0.1,
                max_tokens=6000,
                timeout_seconds=1200,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                outputs=["final synthesis", "confidence notes", "follow-up questions"],
            ),
        ]
        if has_review or requires_citations:
            phases.append(
                make_execution_phase(
                    phase_id="phase-004",
                    kind="review",
                    title="Verify evidence quality",
                    purpose="Check the synthesis for unsupported claims, weak evidence, and missing citations.",
                    agent_route="auto/review",
                    system_prompt=(
                        "You are the verification reviewer. Audit claims against evidence, flag unsupported sections, "
                        "and require repair when the source chain is weak."
                    ),
                    temperature=0.0,
                    max_tokens=2400,
                    timeout_seconds=600,
                    route_catalog=route_catalog,
                    requires_citations=requires_citations,
                    outputs=["verification report", "repair instructions"],
                )
            )
        return phases

    if method_key == "grounded_research":
        phases = [
            make_execution_phase(
                phase_id="phase-001",
                step_id=first_step_id,
                kind="research",
                title="Research with grounding",
                purpose="Run a grounded research pass that records evidence instead of producing a loose summary.",
                agent_route="auto/research",
                system_prompt=(
                    "You are a research operator. Gather findings, keep the source chain explicit, "
                    "and return only evidence-backed claims."
                ),
                temperature=0.1,
                max_tokens=4200,
                timeout_seconds=1200,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                outputs=["research notes", "source log"],
            )
        ]
        if has_review or requires_citations:
            phases.append(
                make_execution_phase(
                    phase_id="phase-002",
                    kind="review",
                    title="Validate grounding",
                    purpose="Check the research output before it becomes the accepted result.",
                    agent_route="auto/review",
                    system_prompt=(
                        "You are the review agent. Reject unsupported claims, surface ambiguity, "
                        "and keep the result grounded in evidence."
                    ),
                    temperature=0.0,
                    max_tokens=2200,
                    timeout_seconds=600,
                    route_catalog=route_catalog,
                    requires_citations=requires_citations,
                    outputs=["grounding review", "repair notes"],
                )
            )
        return phases

    if method_key == "implement_then_review":
        return [
            make_execution_phase(
                phase_id="phase-001",
                step_id=first_step_id,
                kind="build",
                title="Implement the workflow",
                purpose="Execute the primary build work with explicit stage boundaries and concrete outputs.",
                agent_route="auto/build",
                system_prompt=(
                    "You are the implementation lead. Build the requested deliverable directly, "
                    "keep assumptions explicit, and leave the result ready for independent review."
                ),
                temperature=0.1,
                max_tokens=4000,
                timeout_seconds=900,
                route_catalog=route_catalog,
                requires_citations=False,
                outputs=["primary deliverable"],
            ),
            make_execution_phase(
                phase_id="phase-002",
                kind="review",
                title="Review before acceptance",
                purpose="Use a separate review pass so the acceptance criteria are enforced by another agent.",
                agent_route="auto/review",
                system_prompt=(
                    "You are the review agent. Check the deliverable against the success contract, "
                    "identify concrete defects, and avoid rubber-stamping."
                ),
                temperature=0.0,
                max_tokens=2200,
                timeout_seconds=600,
                route_catalog=route_catalog,
                requires_citations=False,
                outputs=["review findings", "acceptance decision"],
            ),
        ]

    if method_key == "staged_execution":
        if len(draft_flow) > 1:
            phases = build_staged_workflow_phases(
                draft_flow=draft_flow,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                blocking_inputs=blocking_inputs,
            )
            if phases:
                return phases
        phases = [
            make_execution_phase(
                phase_id="phase-001",
                step_id=first_step_id,
                kind="stage",
                title="Run staged workflow steps",
                purpose="Execute the workflow as explicit ordered stages rather than collapsing it into one prompt.",
                agent_route="auto/build",
                system_prompt=(
                    "You are the stage operator. Execute only the current stage, preserve handoff state, "
                    "and keep the next stage unambiguous."
                ),
                temperature=0.1,
                max_tokens=3600,
                timeout_seconds=900,
                route_catalog=route_catalog,
                requires_citations=requires_citations,
                outputs=["stage output", "handoff state"],
            )
        ]
        if has_review:
            phases.append(
                make_execution_phase(
                    phase_id="phase-002",
                    kind="review",
                    title="Validate stage outputs",
                    purpose="Add a review phase when the definition suggests explicit validation or judgment.",
                    agent_route="auto/review",
                    system_prompt=(
                        "You are the stage reviewer. Validate the staged output, enforce acceptance criteria, "
                        "and keep the workflow from drifting."
                    ),
                    temperature=0.0,
                    max_tokens=2200,
                    timeout_seconds=600,
                    route_catalog=route_catalog,
                    requires_citations=False,
                    outputs=["validation report"],
                )
            )
        return phases

    return [
        make_execution_phase(
            phase_id="phase-001",
            step_id=first_step_id,
            kind="execute",
            title="Execute directly",
            purpose="Keep the task bounded and direct with one explicit agent phase.",
            agent_route="auto/build",
            system_prompt=(
                "You are the lead operator. Execute the task directly, keep state explicit, "
                "and return a result that is ready for downstream action."
            ),
            temperature=0.1,
            max_tokens=3200,
            timeout_seconds=900,
            route_catalog=route_catalog,
            requires_citations=False,
            outputs=["completed result"],
        )
    ]


def make_execution_phase(
    *,
    phase_id: str,
    step_id: str | None = None,
    kind: str,
    title: str,
    purpose: str,
    agent_route: str,
    system_prompt: str,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
    route_catalog: dict[str, dict[str, Any]],
    requires_citations: bool,
    outputs: list[str],
    fanout_count: int | None = None,
) -> dict[str, Any]:
    from runtime.workflow_type_contracts import route_type_contract

    route_target = route_catalog.get(agent_route, {})
    type_contract = route_type_contract(agent_route, title=title, summary=purpose)
    phase = {
        "id": phase_id,
        "kind": kind,
        "title": title,
        "purpose": purpose,
        "agent_route": agent_route,
        "resolved_agent_slug": _as_text(route_target.get("resolved_agent_slug")) or agent_route,
        "provider_slug": _as_text(route_target.get("provider_slug")),
        "model_slug": _as_text(route_target.get("model_slug")),
        "concrete_model_slug": _as_text(route_target.get("concrete_model_slug")),
        "system_prompt": system_prompt,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "timeout_seconds": timeout_seconds,
        "requires_citations": requires_citations,
        "outputs": outputs,
        # Typed contract derived from agent_route + title + purpose so the
        # phase carries machine-readable consumes/produces alongside the
        # human-readable outputs list. Closes the typed-node gap that left
        # Moon Composer nodes with prose-only contracts (BUG-C6EE740C and
        # the data-dictionary / gates / Provide-X chain it gates).
        "consumes": type_contract["consumes"],
        "consumes_any": type_contract["consumes_any"],
        "produces": type_contract["produces"],
    }
    if step_id:
        phase["step_id"] = step_id
    if fanout_count is not None:
        phase["fanout_count"] = fanout_count
    return phase


def _first_draft_flow_step_id(draft_flow: list[dict[str, Any]]) -> str | None:
    ordered_steps = sorted(
        [step for step in draft_flow if isinstance(step, dict)],
        key=lambda step: int(step.get("order") or 0),
    )
    if not ordered_steps:
        return None
    step_id = _as_text(ordered_steps[0].get("id"))
    return step_id or None


def build_staged_workflow_phases(
    *,
    draft_flow: list[dict[str, Any]],
    route_catalog: dict[str, dict[str, Any]],
    requires_citations: bool,
    blocking_inputs: list[str],
) -> list[dict[str, Any]]:
    phases: list[dict[str, Any]] = []
    ordered_steps = sorted(
        [step for step in draft_flow if isinstance(step, dict)],
        key=lambda step: int(step.get("order") or 0),
    )
    total_steps = len(ordered_steps)
    for index, step in enumerate(ordered_steps, start=1):
        title = _as_text(step.get("title")) or f"Step {index}"
        summary = _as_text(step.get("summary")) or title
        role = infer_stage_role(title=title, summary=summary)
        phase = make_execution_phase(
            phase_id=f"phase-{index:03d}",
            kind=role["kind"],
            title=title,
            purpose=summary,
            agent_route=role["agent_route"],
            system_prompt=role["system_prompt"],
            temperature=0.1 if role["kind"] != "review" else 0.0,
            max_tokens=3600 if role["kind"] in {"research", "build"} else 2800,
            timeout_seconds=1200 if role["kind"] == "research" else 900,
            route_catalog=route_catalog,
            requires_citations=requires_citations or role["kind"] == "research",
            outputs=infer_stage_outputs(title=title, summary=summary, role_kind=role["kind"]),
        )
        phase["step_id"] = _as_text(step.get("id")) or f"step-{index:03d}"
        phase["role_label"] = role["label"]
        phase["required_inputs"] = infer_stage_inputs(
            role_kind=role["kind"],
            title=title,
            summary=summary,
            blocking_inputs=blocking_inputs,
        )
        phase["completion_criteria"] = infer_completion_criteria(
            role_kind=role["kind"],
            outputs=phase["outputs"],
        )
        if index < total_steps:
            phase["handoff_target"] = _as_text(ordered_steps[index].get("title")) or f"Step {index + 1}"
        persistence_targets = infer_persistence_targets(title=title, summary=summary)
        if persistence_targets:
            phase["persistence_targets"] = persistence_targets
        phases.append(phase)
    return phases


def infer_stage_role(*, title: str, summary: str) -> dict[str, str]:
    haystack = f"{title} {summary}".lower()
    if text_has_any(haystack, "review", "validate", "verify", "audit", "check"):
        return {
            "kind": "review",
            "label": "Review",
            "agent_route": "auto/review",
            "system_prompt": (
                "You are the review operator. Validate the stage output, enforce the contract, "
                "and surface concrete defects before the workflow advances."
            ),
        }
    if text_has_any(haystack, "build", "implement", "write", "create", "fix", "update"):
        return {
            "kind": "build",
            "label": "Build",
            "agent_route": "auto/build",
            "system_prompt": (
                "You are the implementation operator. Build only this stage, persist the required outputs, "
                "and leave the next handoff explicit."
            ),
        }
    if text_has_any(haystack, "plan", "design", "map", "schema", "persistence", "contract", "scope"):
        return {
            "kind": "plan",
            "label": "Plan",
            "agent_route": "auto/reasoning",
            "system_prompt": (
                "You are the planning operator. Turn upstream findings into an explicit execution contract "
                "with inputs, outputs, persistence targets, and unambiguous handoffs."
            ),
        }
    if text_has_any(haystack, "research", "docs", "documentation", "api", "brave", "internet", "browse", "source"):
        return {
            "kind": "research",
            "label": "Research",
            "agent_route": "auto/research",
            "system_prompt": (
                "You are the research operator. Use outbound docs and web research when required, "
                "capture sources, and keep unsupported claims out of the handoff."
            ),
        }
    if text_has_any(haystack, "capture", "collect", "gather", "intake", "identify"):
        return {
            "kind": "intake",
            "label": "Intake",
            "agent_route": "auto/build",
            "system_prompt": (
                "You are the intake operator. Capture the missing context cleanly and produce a structured handoff "
                "for the next stage without guessing."
            ),
        }
    return {
        "kind": "stage",
        "label": "Stage",
        "agent_route": "auto/build",
        "system_prompt": (
            "You are the stage operator. Execute only the current stage, preserve the handoff state, "
            "and keep the next step explicit."
        ),
    }


def infer_stage_inputs(
    *,
    role_kind: str,
    title: str,
    summary: str,
    blocking_inputs: list[str],
) -> list[str]:
    haystack = f"{title} {summary}".lower()
    suggested: list[str] = []
    if role_kind == "intake":
        suggested.extend(
            match_blocking_inputs(
                blocking_inputs,
                "application",
                "scope",
            ) or ["Target application or interface in scope"]
        )
    elif role_kind == "research":
        relevant = [
            item
            for item in blocking_inputs
            if (
                "application" in item.lower()
                or "internet" in item.lower()
                or "auth" in item.lower()
            )
        ]
        suggested.extend(
            dedupe_texts(relevant) or [
                "Target application or API in scope",
                "Official API docs entrypoint or outbound internet research route",
            ]
        )
    elif role_kind == "plan":
        suggested.extend(
            match_blocking_inputs(
                blocking_inputs,
                "persistence",
                "object",
                "scope",
            ) or [
                "Captured UI notes and research findings",
                "Persistence contract and object model targets",
            ]
        )
    elif role_kind == "build":
        suggested.extend(
            match_blocking_inputs(
                blocking_inputs,
                "object",
                "persistence",
                "auth",
            ) or [
                "Approved connector or implementation plan",
                "Persistence targets and object mappings",
            ]
        )
    elif role_kind == "review":
        suggested.append("Prior stage output and acceptance criteria")
    if "common objects" in haystack and "Common object scope and target field mappings" not in suggested:
        suggested.append("Common object scope and target field mappings")
    return dedupe_texts(suggested)


def infer_stage_outputs(*, title: str, summary: str, role_kind: str) -> list[str]:
    haystack = f"{title} {summary}".lower()
    if role_kind == "intake":
        return ["captured context", "surface inventory", "scope handoff"]
    if role_kind == "research":
        return ["source-backed findings", "auth and endpoint notes", "source log"]
    if role_kind == "plan":
        outputs = ["execution contract", "handoff plan"]
        if text_has_any(haystack, "connector", "common objects", "persistence", "record"):
            outputs.insert(0, "persistence contract")
            outputs.append("field mapping plan")
        return dedupe_texts(outputs)
    if role_kind == "build":
        outputs = ["implemented deliverable", "handoff state"]
        if "connector" in haystack:
            outputs.insert(0, "basic connector")
        return dedupe_texts(outputs)
    if role_kind == "review":
        return ["review findings", "acceptance decision"]
    return ["stage output", "handoff state"]


def infer_completion_criteria(*, role_kind: str, outputs: list[str]) -> str:
    if role_kind == "research":
        return "Complete when findings are source-backed and the next stage has the source log it needs."
    if role_kind == "plan":
        return "Complete when the next stage can execute without inventing inputs, outputs, or persistence targets."
    if role_kind == "build":
        return "Complete when the deliverable exists and the follow-on review or runtime handoff is explicit."
    if role_kind == "review":
        return "Complete when defects are surfaced or the output is explicitly accepted."
    if outputs:
        return f"Complete when {outputs[0]} is ready for the next stage."
    return "Complete when the stage output is ready for handoff."


def infer_persistence_targets(*, title: str, summary: str) -> list[str]:
    haystack = f"{title} {summary}".lower()
    targets: list[str] = []
    if text_has_any(haystack, "record", "persist", "store", "docs", "documentation"):
        targets.append("source-backed docs notes")
    if text_has_any(haystack, "connector", "common objects", "mapping", "schema"):
        targets.append("connector configuration and object mappings")
    return dedupe_texts(targets)


# Deleted 2026-04-25 under architecture-policy::compile::retrieval-is-the-
# filter-no-template-fallbacks and BUG-3330D2CD:
#   - infer_blocking_inputs
#   - infer_briefing_fields
#   - connector_flow_self_scaffolds_inputs
# These were keyword-gated constant functions that emitted a hardcoded
# 5-item list (Target application / Official API docs / Authentication /
# Persistence / Common object) whenever prose contained any of
# {connector, api docs, common objects, application, docs}. The list was
# written to execution_setup.constraints.blocking_inputs and turned into
# the 5 "Resolve typed input gap" stubs that Moon rendered as build_graph,
# preempting the 23 prose-grounded capability nodes definition_graph
# already contained. The policy is clear: retrieval is the filter. No
# silent template substitution. Projection from definition_graph with
# pill-typed edges is the follow-up commit; this cut removes the lie at
# its source so the empty-but-honest state is visible.


def match_blocking_inputs(blocking_inputs: list[str], *keywords: str) -> list[str]:
    matches = [
        item
        for item in blocking_inputs
        if any(keyword in item.lower() for keyword in keywords if keyword)
    ]
    return dedupe_texts(matches)


def dedupe_texts(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        text = _as_text(value)
        if text and text not in deduped:
            deduped.append(text)
    return deduped


def build_surface_manifest(
    *,
    execution_setup: dict[str, Any],
    definition: dict[str, Any],
    unresolved: list[str],
) -> dict[str, Any]:
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    trigger_intent = definition.get("trigger_intent") if isinstance(definition.get("trigger_intent"), list) else []
    constraints = execution_setup.get("constraints") if isinstance(execution_setup.get("constraints"), dict) else {}
    budget_policy = execution_setup.get("budget_policy") if isinstance(execution_setup.get("budget_policy"), dict) else {}
    phases = execution_setup.get("phases") if isinstance(execution_setup.get("phases"), list) else []
    method = execution_setup.get("method") if isinstance(execution_setup.get("method"), dict) else {}

    metrics = [
        {"id": "method", "label": "Method", "value": _as_text(method.get("label")) or _as_text(method.get("key")), "emphasis": "high"},
        {"id": "runtime_profile_ref", "label": "Runtime Profile", "value": _as_text(execution_setup.get("runtime_profile_ref")), "emphasis": "high"},
        {"id": "cost_tier", "label": "Cost Tier", "value": _as_text(budget_policy.get("cost_tier")) or "unknown", "emphasis": "medium"},
        {"id": "timeout_seconds", "label": "Timeout", "value": format_seconds_label(int(budget_policy.get("timeout_seconds") or 0)), "emphasis": "medium"},
        {"id": "phase_count", "label": "Phases", "value": str(int(execution_setup.get("phase_count") or len(phases) or 0)), "emphasis": "medium"},
    ]
    fanout_workers = int(budget_policy.get("fanout_workers") or 0)
    if fanout_workers > 1:
        metrics.append({"id": "fanout_workers", "label": "Fan-Out Workers", "value": str(fanout_workers), "emphasis": "medium"})
    if constraints.get("requires_citations"):
        metrics.append({"id": "grounding", "label": "Grounding", "value": "Citations required", "emphasis": "high"})
    blocking_inputs = [
        _as_text(item)
        for item in (constraints.get("blocking_inputs") if isinstance(constraints.get("blocking_inputs"), list) else [])
        if _as_text(item)
    ]
    briefing_fields = [
        _as_text(item)
        for item in (constraints.get("briefing_fields") if isinstance(constraints.get("briefing_fields"), list) else [])
        if _as_text(item)
    ]
    if briefing_fields and not blocking_inputs:
        metrics.append({"id": "briefing_fields", "label": "Suggested Inputs", "value": str(len(briefing_fields)), "emphasis": "medium"})
    if blocking_inputs:
        metrics.append({"id": "blocking_inputs", "label": "Blocking Inputs", "value": str(len(blocking_inputs)), "emphasis": "high"})

    approaches = []
    for phase in phases:
        if not isinstance(phase, dict):
            continue
        approaches.append(
            {
                "id": _as_text(phase.get("id")),
                "label": _as_text(phase.get("title")),
                "summary": _as_text(phase.get("purpose")),
                "agent_route": _as_text(phase.get("agent_route")),
                "model": _as_text(phase.get("concrete_model_slug")) or _as_text(phase.get("resolved_agent_slug")),
            }
        )

    reference_count = len([reference for reference in references if isinstance(reference, dict) and _as_text(reference.get("slug"))])
    resolved_reference_count = len(
        [
            reference
            for reference in references
            if isinstance(reference, dict)
            and _as_text(reference.get("slug"))
            and reference.get("resolved") is not False
            and _as_text(reference.get("resolved_to"))
        ]
    )
    metrics.append(
        {
            "id": "references",
            "label": "References",
            "value": f"{resolved_reference_count}/{reference_count} resolved" if reference_count else "0 captured",
            "emphasis": "medium",
        }
    )

    objects = []
    for reference in references[:8]:
        if not isinstance(reference, dict):
            continue
        slug = _as_text(reference.get("slug"))
        if not slug:
            continue
        objects.append(
            {
                "kind": _as_text(reference.get("type")) or "reference",
                "label": slug,
                "status": "resolved" if reference.get("resolved") is not False and _as_text(reference.get("resolved_to")) else "unresolved",
                "resolved_to": _as_text(reference.get("resolved_to")),
                "display_name": _as_text(reference.get("display_name")),
            }
        )

    workflows = []
    for trigger in trigger_intent[:4]:
        if not isinstance(trigger, dict):
            continue
        workflows.append(
            {
                "kind": "trigger",
                "label": _as_text(trigger.get("title")) or _as_text(trigger.get("event_type")) or "Trigger",
                "summary": _as_text(trigger.get("summary")) or "Trigger intent detected in the definition.",
            }
        )

    commands = []
    if unresolved:
        commands.append({"id": "resolve_references", "label": "Resolve references", "reason": "Planning is blocked until unresolved references are fixed."})
    if blocking_inputs:
        commands.append({"id": "fill_blocking_inputs", "label": "Fill briefing blockers", "reason": "Planner authority is incomplete until the missing connector and persistence inputs are explicit."})
    elif briefing_fields:
        commands.append({"id": "fill_briefing_fields", "label": "Shape briefing inputs", "reason": "Research and analysis stay clearer when topic, scope, freshness, source rules, and deliverable shape are explicit before planning."})
    commands.append({"id": "generate_plan", "label": "Generate runnable plan", "reason": "Turn the compile-time setup into queue authority when you are ready to run it."})
    commands.append({"id": "edit_in_builder", "label": "Edit in builder", "reason": "Open the compiled setup in the visual builder if you want to change stages or overrides manually."})
    if _as_text(method.get("key")) == "staged_execution" and not workflows:
        commands.append({"id": "attach_trigger", "label": "Attach trigger", "reason": "This setup looks workflow-shaped, but it still runs manually until you attach an explicit trigger."})
    if any(isinstance(phase, dict) and not _as_text(phase.get("concrete_model_slug")) for phase in phases):
        commands.append({"id": "resolve_model_routes", "label": "Resolve model routes", "reason": "Compile kept route aliases for one or more phases because concrete provider/model bindings are not available yet."})

    badges = [
        _as_text(method.get("label")) or "Compiled setup",
        "Long-running" if constraints.get("long_running") else "Bounded",
        "Citations required" if constraints.get("requires_citations") else "Direct execution",
    ]
    if unresolved:
        badges.append("Resolve refs first")

    headline = f"Built a {_as_text(method.get('label')) or 'compiled'} setup for {_as_text(execution_setup.get('title')) or 'this operating model'}."

    return {
        "version": 1,
        "headline": headline,
        "badges": [badge for badge in badges if badge],
        "surface_now": {
            "metrics": metrics,
            "approaches": approaches,
            "objects": objects,
            "workflows": workflows,
            "commands": commands,
        },
    }


def build_build_receipt(
    *,
    execution_setup: dict[str, Any],
    surface_manifest: dict[str, Any],
    definition: dict[str, Any],
    unresolved: list[str],
) -> dict[str, Any]:
    method = execution_setup.get("method") if isinstance(execution_setup.get("method"), dict) else {}
    constraints = execution_setup.get("constraints") if isinstance(execution_setup.get("constraints"), dict) else {}
    budget_policy = execution_setup.get("budget_policy") if isinstance(execution_setup.get("budget_policy"), dict) else {}
    phases = execution_setup.get("phases") if isinstance(execution_setup.get("phases"), list) else []
    capability_slugs = execution_setup.get("capability_slugs") if isinstance(execution_setup.get("capability_slugs"), list) else []
    blocking_inputs = constraints.get("blocking_inputs") if isinstance(constraints.get("blocking_inputs"), list) else []
    briefing_fields = constraints.get("briefing_fields") if isinstance(constraints.get("briefing_fields"), list) else []
    compiled_prose = _as_text(definition.get("compiled_prose"))

    decisions = [
        {"aspect": "method", "choice": _as_text(method.get("key")) or "compiled_setup", "reason": method_reason(execution_setup=execution_setup, compiled_prose=compiled_prose)},
        {"aspect": "runtime_profile", "choice": _as_text(execution_setup.get("runtime_profile_ref")) or "compile/task.single", "reason": "Runtime profile follows the compiled method so model choice, budget, and control surfaces stay coherent."},
        {"aspect": "budget_policy", "choice": _as_text(budget_policy.get("cost_tier")) or "unknown", "reason": budget_reason(execution_setup=execution_setup)},
    ]
    if constraints.get("requires_citations"):
        decisions.append({"aspect": "grounding", "choice": "citations_required", "reason": "The definition reads like research or evidence work, so the setup keeps the source chain explicit."})
    if phases:
        first_phase = phases[0] if isinstance(phases[0], dict) else {}
        decisions.append({"aspect": "seed_phase", "choice": _as_text(first_phase.get("concrete_model_slug")) or _as_text(first_phase.get("agent_route")) or "auto/build", "reason": "The first phase owns decomposition, so the setup front-loads the strongest reasoning lane before execution fans out or commits."})
    if unresolved:
        decisions.append({"aspect": "planner_state", "choice": "planning_blocked", "reason": "Compile still built the setup, but unresolved references keep it in preview until the definition is safe to plan."})
    if blocking_inputs:
        decisions.append({"aspect": "briefing_blockers", "choice": "blocking_inputs_required", "reason": "The workflow shape is visible, but connector-specific briefing fields are still missing, so readiness would be overstated without calling them out."})
    elif briefing_fields:
        decisions.append({"aspect": "briefing_schema", "choice": "suggested_inputs_emitted", "reason": "Compile inferred the research brief fields so new data gets an explicit contract instead of living as loose prose."})

    tradeoffs = tradeoffs_for_method(
        method_key=_as_text(method.get("key")),
        cost_tier=_as_text(budget_policy.get("cost_tier")),
        long_running=bool(constraints.get("long_running")),
        unresolved=bool(unresolved),
    )
    authority_refs = [reference for reference in (_as_text(execution_setup.get("runtime_profile_ref")), *[slug for slug in capability_slugs if _as_text(slug)]) if reference]
    summary = f"Built a {_as_text(method.get('label')) or 'compiled'} setup with {len(phases)} phase{'' if len(phases) == 1 else 's'}."
    explanation = f"{summary} {surface_manifest.get('headline') or ''} The setup stays inspectable now and planner authority remains explicit later.".strip()

    return {
        "version": 1,
        "summary": summary,
        "explanation": explanation,
        "decisions": decisions,
        "tradeoffs": tradeoffs,
        "authority_refs": authority_refs,
    }


def build_data_audit(
    *,
    definition: dict[str, Any],
    execution_setup: dict[str, Any],
    surface_manifest: dict[str, Any],
    unresolved: list[str],
) -> dict[str, Any]:
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    phases = execution_setup.get("phases") if isinstance(execution_setup.get("phases"), list) else []
    trigger_intent = definition.get("trigger_intent") if isinstance(definition.get("trigger_intent"), list) else []
    resolved_reference_count = len(
        [
            reference
            for reference in references
            if isinstance(reference, dict)
            and _as_text(reference.get("slug"))
            and reference.get("resolved") is not False
            and _as_text(reference.get("resolved_to"))
        ]
    )
    prompt_bytes = sum(len(_as_text(phase.get("system_prompt"))) for phase in phases if isinstance(phase, dict))
    resolved_model_count = len([phase for phase in phases if isinstance(phase, dict) and _as_text(phase.get("concrete_model_slug"))])
    return {
        "definition_bytes": len(json.dumps(definition, sort_keys=True)),
        "execution_setup_bytes": len(json.dumps(execution_setup, sort_keys=True)),
        "surface_manifest_bytes": len(json.dumps(surface_manifest, sort_keys=True)),
        "phase_prompt_bytes": prompt_bytes,
        "phase_count": len(phases),
        "reference_count": len([reference for reference in references if isinstance(reference, dict) and _as_text(reference.get("slug"))]),
        "resolved_reference_count": resolved_reference_count,
        "unresolved_reference_count": max(len(unresolved), 0),
        "trigger_count": len(trigger_intent),
        "resolved_model_count": resolved_model_count,
        "transport_mode": "definition_embedded",
    }


def build_data_gaps(
    *,
    execution_setup: dict[str, Any],
    definition: dict[str, Any],
    unresolved: list[str],
) -> list[str]:
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    trigger_intent = definition.get("trigger_intent") if isinstance(definition.get("trigger_intent"), list) else []
    phases = execution_setup.get("phases") if isinstance(execution_setup.get("phases"), list) else []
    method = execution_setup.get("method") if isinstance(execution_setup.get("method"), dict) else {}
    constraints = execution_setup.get("constraints") if isinstance(execution_setup.get("constraints"), dict) else {}
    blocking_inputs = constraints.get("blocking_inputs") if isinstance(constraints.get("blocking_inputs"), list) else []

    gaps: list[str] = []
    if not any(isinstance(reference, dict) and _as_text(reference.get("slug")) for reference in references):
        gaps.append("No explicit references were captured, so the surfaced objects and integrations are still inferred from prose.")
    if unresolved:
        gaps.append("Some references are still unresolved, so planner authority remains blocked until those slugs are fixed.")
    if blocking_inputs:
        gaps.append(
            "Blocking briefing inputs are still missing: "
            + ", ".join(_as_text(item) for item in blocking_inputs if _as_text(item))
            + "."
        )
    if _as_text(method.get("key")) == "staged_execution" and not trigger_intent:
        gaps.append("No trigger intent was captured, so this workflow-shaped setup is still manual by default.")
    if any(isinstance(phase, dict) and not _as_text(phase.get("concrete_model_slug")) for phase in phases):
        gaps.append("Concrete provider/model bindings are missing for one or more phases, so route aliases are still standing in.")
    if any(isinstance(phase, dict) and _as_text(phase.get("system_prompt")) for phase in phases):
        gaps.append("Prompt and persona authority refs are not attached yet; phases currently carry compiled inline prompts.")
    return gaps


def load_agent_route_catalog(conn: Any | None) -> dict[str, dict[str, Any]]:
    if conn is None:
        return {}
    from registry.agent_config import AgentRegistry

    registry = AgentRegistry.load_from_postgres(conn)
    catalog: dict[str, dict[str, Any]] = {}
    for route_slug in (
        "auto/high",
        "auto/reasoning",
        "auto/research",
        "auto/analysis",
        "auto/build",
        "auto/review",
        "auto/instant",
        "auto/chat",
        "auto/support",
        "auto/draft",
        "auto/classify",
    ):
        agent = registry.get(route_slug)
        if agent is None:
            continue
        catalog[route_slug] = {
            "resolved_agent_slug": route_slug,
            "provider_slug": agent.provider,
            "model_slug": agent.model,
            "concrete_model_slug": f"{agent.provider}/{agent.model}",
            "context_window": agent.context_window,
            "max_output_tokens": agent.max_output_tokens,
        }
    return catalog


def method_reason(*, execution_setup: dict[str, Any], compiled_prose: str) -> str:
    capability_slugs = execution_setup.get("capability_slugs") if isinstance(execution_setup.get("capability_slugs"), list) else []
    method = execution_setup.get("method") if isinstance(execution_setup.get("method"), dict) else {}
    method_key = _as_text(method.get("key"))
    if method_key == "seed_fanout_synthesize":
        return "Research and breadth signals were detected, so the setup uses a strong seed phase, cheaper parallel workers, and a synthesis pass instead of one giant prompt."
    if method_key == "grounded_research":
        return "The definition reads like evidence work, so the setup keeps research grounded and adds verification before acceptance."
    if method_key == "implement_then_review":
        return "The work includes distinct execution and validation concerns, so the setup separates build and review instead of trusting one pass."
    if method_key == "staged_execution":
        return "Workflow cues were detected, so the setup preserves ordered stages and trigger-friendly boundaries instead of flattening everything into one agent pass."
    if "research/fan-out" in capability_slugs:
        return "Parallel research capability was selected, so the setup preserves a staged method instead of collapsing into one step."
    return "The definition is bounded enough to execute directly, so the setup stays simple while keeping prompt and controls explicit."


def budget_reason(*, execution_setup: dict[str, Any]) -> str:
    constraints = execution_setup.get("constraints") if isinstance(execution_setup.get("constraints"), dict) else {}
    budget_policy = execution_setup.get("budget_policy") if isinstance(execution_setup.get("budget_policy"), dict) else {}
    cost_tier = _as_text(budget_policy.get("cost_tier"))
    fanout_workers = int(budget_policy.get("fanout_workers") or 0)
    if cost_tier == "high":
        return "High cost is intentional here: the setup spends more on decomposition and synthesis so the parallel work starts cleanly and ends grounded."
    if fanout_workers > 1:
        return "Budget stays moderate by using breadth where it matters and avoiding an expensive single-model monolith."
    if constraints.get("long_running"):
        return "The setup extends timeout and completion budget because the task looks broader than a short interactive pass."
    return "The setup keeps the budget bounded because the task does not justify a deep multi-phase run."


def tradeoffs_for_method(
    *,
    method_key: str,
    cost_tier: str,
    long_running: bool,
    unresolved: bool,
) -> list[str]:
    tradeoffs: list[str] = []
    if method_key == "seed_fanout_synthesize":
        tradeoffs.extend(
            [
                "Higher seed and synthesis cost buys better decomposition and cross-source reconciliation.",
                "Cheaper fan-out workers keep broad research affordable once the plan is stable.",
            ]
        )
    elif method_key == "grounded_research":
        tradeoffs.append("Grounding adds verification overhead, but it prevents the final output from being a soft uncited summary.")
    elif method_key == "implement_then_review":
        tradeoffs.append("Separate review increases latency slightly, but it makes acceptance criteria explicit and observable.")
    else:
        tradeoffs.append("A simpler method is faster to run, but it has less structural protection against drift than a staged setup.")
    if cost_tier == "high":
        tradeoffs.append("This setup is cost-aware, not cost-minimal.")
    if long_running:
        tradeoffs.append("This setup reserves more wall-clock time because the task looks too broad for a short bounded pass.")
    if unresolved:
        tradeoffs.append("The setup is built now, but planning still blocks until unresolved references are fixed.")
    return tradeoffs


def format_seconds_label(seconds: int) -> str:
    if seconds <= 0:
        return "unspecified"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"

def _infer_agent_route(
    slug: str,
    reference: dict[str, Any] | None = None,
    *,
    route_hints: tuple[tuple[str, str], ...] = (),
) -> str:
    haystack = slug.lower()
    if reference:
        haystack = " ".join(
            [
                haystack,
                _as_text(reference.get("description")),
                _as_text((reference.get("config") or {}).get("description")),
            ]
        ).lower()
    for hint, route in route_hints:
        if hint in haystack:
            return route
    if any(token in haystack for token in ("review", "validate", "verify", "audit", "judge", "critic")):
        return "auto/review"
    if any(token in haystack for token in ("research", "investigate", "analyze", "search", "brief")):
        return "auto/research"
    if any(token in haystack for token in ("reason", "synth", "reconcile", "deduce")):
        return "auto/reasoning"
    return "auto/build"


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()
