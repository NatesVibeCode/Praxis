"""Operating-model planner.

Turns a saved operating-model definition into a runnable queue spec.
Compile owns meaning; planning owns executable authority.
"""

from __future__ import annotations

import logging
import json
import hashlib
from typing import Any

from runtime.compiler import _as_text, _derive_title, _infer_agent_route, _workflow_id_for_title, _slugify
from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.compile_reuse import module_surface_revision, stable_hash
from runtime.build_authority import build_authority_bundle
from runtime.definition_compile_kernel import materialize_definition
from runtime.edge_release import normalize_edge_release

logger = logging.getLogger(__name__)


class PlanningBlockedError(ValueError):
    """Raised when a definition cannot be planned into executable authority."""

    def __init__(self, unresolved: list[str]) -> None:
        self.unresolved = unresolved
        joined = ", ".join(unresolved) if unresolved else "unknown references"
        super().__init__(f"Planning blocked until unresolved references are fixed: {joined}")


def missing_execution_plan_message(workflow_name: str | None = None) -> str:
    if workflow_name:
        return f"Workflow '{workflow_name}' has no current execution plan. Generate plan first."
    return "Workflow has no current execution plan. Generate plan first."


def _edge_gate_runtime_release(edge_gate: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    release = normalize_edge_release(edge_gate)
    edge_type = _as_text(release.get("edge_type")) or "after_success"
    if edge_type != "conditional":
        return edge_type, None
    release_condition = release.get("release_condition")
    if not isinstance(release_condition, dict) or not release_condition:
        return "after_success", None
    return "conditional", json.loads(json.dumps(release_condition))


def _edge_gate_validation_config(edge_gate: dict[str, Any]) -> dict[str, Any] | None:
    release = edge_gate.get("release") if isinstance(edge_gate.get("release"), dict) else {}
    if _as_text(release.get("edge_type")) != "validation" and _as_text(release.get("family")) != "validation":
        return None
    config = release.get("config") if isinstance(release.get("config"), dict) else {}
    return config


def _edge_gate_validation_refs(edge_gate: dict[str, Any]) -> list[str]:
    config = _edge_gate_validation_config(edge_gate)
    if config is None:
        return []
    return list(
        dict.fromkeys(
            [
                *_string_list(config.get("verify_refs")),
                _as_text(config.get("verify_ref")),
            ]
        )
    )


def _edge_gate_legacy_verify_command(edge_gate: dict[str, Any]) -> str:
    config = _edge_gate_validation_config(edge_gate)
    if config is None:
        return ""
    return _as_text(config.get("verify_command"))


def _edge_gate_retry_max_attempts(edge_gate: dict[str, Any]) -> int | None:
    release = edge_gate.get("release") if isinstance(edge_gate.get("release"), dict) else {}
    if _as_text(release.get("edge_type")) != "retry" and _as_text(release.get("family")) != "retry":
        return None
    config = release.get("config") if isinstance(release.get("config"), dict) else {}
    try:
        attempts = int(config.get("max_attempts") or 0)
    except (TypeError, ValueError):
        attempts = 0
    return max(attempts, 1) if attempts > 0 else 3


def current_compiled_spec(definition: Any, compiled_spec: Any) -> dict[str, Any] | None:
    definition_dict = materialize_definition(_json_dict(definition))
    compiled_spec_dict = _json_dict(compiled_spec)
    definition_revision = _as_text(definition_dict.get("definition_revision"))
    compiled_revision = _as_text(compiled_spec_dict.get("definition_revision"))
    if not definition_revision or not compiled_revision or definition_revision != compiled_revision:
        return None
    if not _compiled_spec_surface_is_current(compiled_spec_dict):
        return None
    if not _compiled_spec_matches_definition(definition_dict, compiled_spec_dict):
        return None
    return json.loads(json.dumps(compiled_spec_dict))


def unresolved_reference_slugs(definition: Any) -> list[str]:
    definition_dict = materialize_definition(_json_dict(definition))
    unresolved: list[str] = []
    for reference in definition_dict.get("references", []) if isinstance(definition_dict.get("references"), list) else []:
        if not isinstance(reference, dict):
            continue
        slug = _as_text(reference.get("slug"))
        if not slug:
            continue
        if reference.get("resolved") is False or not _as_text(reference.get("resolved_to")):
            unresolved.append(slug)
    return sorted(set(unresolved))


def _approved_reference_slugs(review: dict[str, Any] | None) -> set[str]:
    if not isinstance(review, dict):
        return set()
    approved: set[str] = set()
    for raw in review.get("approved_binding_refs", []) if isinstance(review.get("approved_binding_refs"), list) else []:
        if isinstance(raw, dict):
            for key in ("candidate_ref", "target_ref"):
                value = _as_text(raw.get(key))
                if value:
                    approved.add(value)
            slot_ref = _as_text(raw.get("slot_ref"))
        else:
            slot_ref = _as_text(raw)
        if slot_ref:
            approved.add(slot_ref)
            if slot_ref.startswith("binding:ref-"):
                approved.add(slot_ref[len("binding:ref-"):].replace("-", "_"))
    return approved


def _has_explicit_build_authority_state(definition: dict[str, Any]) -> bool:
    return any(
        key in definition
        for key in (
            "binding_ledger",
            "import_snapshots",
            "authority_attachments",
            "build_graph",
            "build_issues",
            "projection_status",
        )
    )


def missing_execution_manifest_message(workflow_name: str | None = None) -> str:
    if workflow_name:
        return f"Workflow '{workflow_name}' has no approved execution manifest. Review and harden the workflow first."
    return "Workflow has no approved execution manifest. Review and harden the workflow first."


def _compile_planned_definition(
    definition: dict[str, Any],
    *,
    title: str | None,
    conn: Any | None,
    planning_source: str,
) -> dict[str, Any]:
    compiled_prose = _as_text(definition.get("compiled_prose"))
    source_prose = _as_text(definition.get("source_prose"))
    definition_revision = _as_text(definition.get("definition_revision"))
    if not definition_revision:
        raise ValueError("definition.definition_revision is required")
    effective_title = _as_text(title) or _derive_title(source_prose, compiled_prose)
    compile_provenance = _plan_compile_provenance(definition=definition, title=effective_title)

    if conn is not None:
        artifact_store = CompileArtifactStore(conn)
        try:
            reusable_plan = artifact_store.load_reusable_artifact(
                artifact_kind="plan",
                input_fingerprint=compile_provenance["input_fingerprint"],
            )
        except CompileArtifactError as exc:
            logger.warning("Skipping reusable plan artifact: %s", exc)
            reusable_plan = None
        if reusable_plan is not None:
            compiled_spec = json.loads(json.dumps(reusable_plan.payload, default=str))
            return {
                "compiled_spec": compiled_spec,
                "planning_notes": ["Reused plan from exact authority and context match."],
                "reuse_provenance": {
                    "artifact_kind": "plan",
                    "decision": "reused",
                    "reason_code": "plan.compile.exact_input_match",
                    "input_fingerprint": compile_provenance["input_fingerprint"],
                    "artifact_ref": reusable_plan.artifact_ref,
                    "revision_ref": reusable_plan.revision_ref,
                    "content_hash": reusable_plan.content_hash,
                    "decision_ref": reusable_plan.decision_ref,
                },
            }

    jobs = _plan_jobs(definition)
    compiled_spec = {
        "name": effective_title,
        "workflow_id": _workflow_id_for_title(effective_title),
        "phase": "build",
        "outcome_goal": compiled_prose or source_prose,
        "jobs": jobs,
        "triggers": _plan_triggers(definition),
        "definition_revision": definition_revision,
    }
    compiled_spec["compile_provenance"] = compile_provenance
    compiled_spec["plan_revision"] = _plan_revision(compiled_spec)
    if conn is not None:
        try:
            artifact_store = CompileArtifactStore(conn)
            artifact_store.record_plan(
                plan=compiled_spec,
                authority_refs=[definition_revision],
                decision_ref=f"decision.compile.plan.{compiled_spec['plan_revision']}",
                parent_artifact_ref=definition_revision,
                input_fingerprint=compile_provenance["input_fingerprint"],
            )
        except Exception:
            pass
    return {
        "compiled_spec": compiled_spec,
        "planning_notes": [
            f"Planned {len(compiled_spec['jobs'])} jobs from {planning_source}.",
            f"Planned {len(compiled_spec['triggers'])} triggers from trigger_intent.",
        ],
        "reuse_provenance": {
            "artifact_kind": "plan",
            "decision": "compiled",
            "reason_code": "plan.compile.miss",
            "input_fingerprint": compile_provenance["input_fingerprint"],
        },
    }


def harden_reviewed_definition(
    definition: dict[str, Any],
    *,
    title: str | None = None,
    conn: Any | None = None,
    candidate_manifest: dict[str, Any] | None = None,
    reviewable_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(definition, dict):
        raise ValueError("definition is required and must be an object")
    definition = materialize_definition(definition)

    authority_bundle = build_authority_bundle(definition)
    projection_status = authority_bundle.get("projection_status") if isinstance(authority_bundle, dict) else {}
    if _as_text((projection_status or {}).get("state")) == "blocked":
        blocking_labels = [
            _as_text(issue.get("label")) or _as_text(issue.get("issue_id"))
            for issue in authority_bundle.get("build_issues", [])
            if isinstance(issue, dict) and _as_text(issue.get("severity")) == "blocking"
        ]
        raise PlanningBlockedError([label for label in blocking_labels if label])

    manifest, review = _review_contract_for_definition(
        definition=definition,
        conn=conn,
        candidate_manifest=(candidate_manifest if isinstance(candidate_manifest, dict) else None),
        reviewable_plan=(reviewable_plan if isinstance(reviewable_plan, dict) else None),
    )
    hardening_blockers = _review_hardening_blockers(manifest=manifest, review=review)
    if hardening_blockers:
        raise PlanningBlockedError(hardening_blockers)

    approved_references = _approved_reference_slugs(review)
    unresolved = [
        slug
        for slug in unresolved_reference_slugs(definition)
        if slug not in approved_references
    ]
    if unresolved:
        raise PlanningBlockedError(unresolved)

    return _compile_planned_definition(
        definition,
        title=title,
        conn=conn,
        planning_source="reviewed_authority",
    )


def plan_definition(
    definition: dict[str, Any],
    *,
    title: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    if not isinstance(definition, dict):
        raise ValueError("definition is required and must be an object")
    definition = materialize_definition(definition)

    unresolved = unresolved_reference_slugs(definition)
    if unresolved:
        raise PlanningBlockedError(unresolved)
    if _has_explicit_build_authority_state(definition):
        raise PlanningBlockedError(
            [
                "Reviewed planning artifacts are required before hardening can proceed. "
                "Use harden_reviewed_definition for builder-owned workflow state."
            ]
        )
    return _compile_planned_definition(
        definition,
        title=title,
        conn=conn,
        planning_source="draft_flow",
    )


def _review_contract_for_definition(
    *,
    definition: dict[str, Any],
    conn: Any | None,
    candidate_manifest: dict[str, Any] | None,
    reviewable_plan: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if isinstance(candidate_manifest, dict) and isinstance(reviewable_plan, dict):
        return candidate_manifest, reviewable_plan
    from runtime.build_planning_contract import (
        build_candidate_resolution_manifest,
        build_reviewable_plan,
    )

    workflow_id = _as_text(definition.get("workflow_id")) or None
    manifest = (
        candidate_manifest
        if isinstance(candidate_manifest, dict)
        else build_candidate_resolution_manifest(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=None,
        )
    )
    review = (
        reviewable_plan
        if isinstance(reviewable_plan, dict)
        else build_reviewable_plan(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=None,
            candidate_manifest=manifest,
        )
    )
    return manifest, review


def _review_hardening_blockers(
    *,
    manifest: dict[str, Any] | None,
    review: dict[str, Any] | None,
) -> list[str]:
    if not isinstance(manifest, dict) or not isinstance(review, dict):
        return ["Reviewed planning artifacts are required before hardening can proceed."]
    blockers: list[str] = []
    if _as_text(manifest.get("execution_readiness")) != "ready":
        blockers.extend(
            [
                _as_text(item.get("reason"))
                for item in manifest.get("required_confirmations", [])
                if isinstance(item, dict) and _as_text(item.get("reason"))
            ]
        )
        if not blockers:
            blockers.append("Reviewed approvals are incomplete.")
    if review.get("proposal_requests"):
        blockers.append("Proposal requests must be resolved before hardening can proceed.")
    if review.get("widening_ops"):
        blockers.append("Pending widening requests must be resolved before hardening can proceed.")
    if review.get("required_unapproved_slots"):
        blockers.append("Required binding approvals are incomplete.")
    if review.get("required_unapproved_bundle_slots"):
        blockers.append("Required capability bundle approvals are incomplete.")
    if not review.get("approved_workflow_shape_ref"):
        blockers.append("An approved workflow shape is required before hardening can proceed.")
    if not review.get("approved_binding_refs"):
        blockers.append("At least one approved binding is required before hardening can proceed.")
    if not review.get("approved_bundle_refs"):
        blockers.append("At least one approved capability bundle is required before hardening can proceed.")
    return list(dict.fromkeys([item for item in blockers if item]))

def _plan_jobs(definition: dict[str, Any]) -> list[dict[str, Any]]:
    definition = materialize_definition(definition)
    compiled_prose = _as_text(definition.get("compiled_prose"))
    references = definition.get("references") if isinstance(definition.get("references"), list) else []
    narrative_blocks = definition.get("narrative_blocks") if isinstance(definition.get("narrative_blocks"), list) else []
    draft_flow = definition.get("draft_flow") if isinstance(definition.get("draft_flow"), list) else []
    execution_setup = definition.get("execution_setup") if isinstance(definition.get("execution_setup"), dict) else {}

    references_by_slug = {
        _as_text(reference.get("slug")): reference
        for reference in references
        if isinstance(reference, dict) and _as_text(reference.get("slug"))
    }
    blocks_by_id = {
        _as_text(block.get("id")): block
        for block in narrative_blocks
        if isinstance(block, dict) and _as_text(block.get("id"))
    }
    edge_gate_by_pair = {
        (_as_text(edge_gate.get("from_node_id")), _as_text(edge_gate.get("to_node_id"))): edge_gate
        for edge_gate in execution_setup.get("edge_gates", [])
        if isinstance(edge_gate, dict)
        and _as_text(edge_gate.get("from_node_id"))
        and _as_text(edge_gate.get("to_node_id"))
    }
    validation_refs_by_step_id: dict[str, list[str]] = {}
    retry_max_attempts_by_step_id: dict[str, int] = {}
    approval_edge_pairs: list[tuple[str, str]] = []
    for (from_id, to_id), edge_gate in edge_gate_by_pair.items():
        legacy_verify_command = _edge_gate_legacy_verify_command(edge_gate)
        if legacy_verify_command:
            edge_id = _as_text(edge_gate.get("edge_id")) or f"{from_id}->{to_id}"
            raise PlanningBlockedError([
                (
                    f"validation edge {edge_id} uses legacy verify_command; "
                    "register verifier authority and use verify_refs instead"
                )
            ])
        verify_refs = _edge_gate_validation_refs(edge_gate)
        if verify_refs:
            validation_refs_by_step_id.setdefault(from_id, []).extend(verify_refs)
        retry_max_attempts = _edge_gate_retry_max_attempts(edge_gate)
        if retry_max_attempts is not None:
            retry_max_attempts_by_step_id[to_id] = max(retry_max_attempts_by_step_id.get(to_id, 0), retry_max_attempts)
        release = normalize_edge_release(edge_gate)
        if _as_text(release.get("family")) == "approval":
            approval_edge_pairs.append((from_id, to_id))
    phase_by_step_id = {
        _as_text(phase.get("step_id")): phase
        for phase in execution_setup.get("phases", [])
        if isinstance(phase, dict) and _as_text(phase.get("step_id"))
    }

    label_by_step_id: dict[str, str] = {}
    title_by_step_id: dict[str, str] = {}
    used_labels: set[str] = set()
    jobs: list[dict[str, Any]] = []

    ordered_steps = sorted(
        (step for step in draft_flow if isinstance(step, dict)),
        key=lambda step: int(step.get("order") or 0),
    )
    if not ordered_steps:
        ordered_steps = [
            {
                "id": "step-001",
                "title": "Execute operating model",
                "summary": compiled_prose,
                "source_block_ids": [],
                "reference_slugs": [],
                "capability_slugs": [],
                "depends_on": [],
                "order": 1,
            }
        ]

    for index, step in enumerate(ordered_steps, start=1):
        step_id = _as_text(step.get("id"))
        if not step_id:
            continue
        label = _unique_label(_slugify(_as_text(step.get("title")) or f"step-{index}"), used_labels)
        label_by_step_id[step_id] = label
        title_by_step_id[step_id] = _as_text(step.get("title")) or f"Step {index}"

    approval_questions_by_step_id: dict[str, list[str]] = {}
    for from_id, to_id in approval_edge_pairs:
        from_title = title_by_step_id.get(from_id) or label_by_step_id.get(from_id) or from_id
        to_title = title_by_step_id.get(to_id) or label_by_step_id.get(to_id) or to_id
        approval_questions_by_step_id.setdefault(to_id, []).append(
            f"Approve transition from {from_title} to {to_title}?"
        )

    for index, step in enumerate(ordered_steps, start=1):
        step_id = _as_text(step.get("id"))
        if not step_id:
            continue
        label = label_by_step_id[step_id]
        reference_slugs = _string_list(step.get("reference_slugs"))
        capability_slugs = _string_list(step.get("capability_slugs"))
        source_block_ids = _string_list(step.get("source_block_ids"))
        agent_slug, agent_route = _agent_for_step(
            step,
            explicit_phase=phase_by_step_id.get(step_id),
            reference_slugs=reference_slugs,
            source_block_ids=source_block_ids,
            references_by_slug=references_by_slug,
            blocks_by_id=blocks_by_id,
        )
        integration_job = _integration_job_from_route(
            agent_route,
            step=step,
            explicit_phase=phase_by_step_id.get(step_id),
            compiled_prose=compiled_prose,
            label=label,
        )
        if integration_job is not None:
            integration_id, integration_action, integration_args = integration_job
            agent_slug = f"integration/{integration_id}/{integration_action}"
        else:
            integration_id = ""
            integration_action = ""
            integration_args = None

        source_text = "\n".join(
            _as_text(blocks_by_id[block_id].get("text"))
            for block_id in source_block_ids
            if block_id in blocks_by_id and _as_text(blocks_by_id[block_id].get("text"))
        )
        prompt_sections = [
            "## Operating Model",
            compiled_prose,
            "## Planned Step",
            f"{int(step.get('order') or index)}. {_as_text(step.get('title')) or f'Step {index}'}",
            "## Step Summary",
            _as_text(step.get("summary")) or source_text or compiled_prose,
        ]
        if source_text:
            prompt_sections.extend(["## Source Narrative", source_text])
        if reference_slugs:
            prompt_sections.extend(["## References", "\n".join(reference_slugs)])
        if capability_slugs:
            prompt_sections.extend(["## Capabilities", "\n".join(capability_slugs)])
        if agent_slug and not integration_job:
            prompt_sections.extend(["## Responsible Agent", agent_slug])
        elif integration_job is not None:
            prompt_sections.extend([
                "## Integration",
                f"@{integration_id}/{integration_action}",
            ])

        job = {
            "label": label,
            "agent": agent_route,
            "prompt": "\n".join(prompt_sections),
            "source_step_id": _as_text(step.get("id")) or None,
            "source_node_id": _as_text(step.get("id")) or None,
        }
        if integration_job is not None:
            job["agent"] = agent_slug
            job["integration_id"] = integration_id
            job["integration_action"] = integration_action
            job["integration_args"] = integration_args
        elif agent_slug:
            job["agent_name"] = agent_slug
            job["system_prompt"] = (
                f"You are {agent_slug}. Execute only the responsibilities assigned in this planned operating-model step."
            )
        verify_refs = validation_refs_by_step_id.get(step_id)
        if verify_refs:
            deduped_verify_refs = [ref for ref in dict.fromkeys(ref.strip() for ref in verify_refs) if ref]
            if deduped_verify_refs:
                job["verify_refs"] = deduped_verify_refs
        approval_questions = approval_questions_by_step_id.get(step_id)
        if approval_questions:
            deduped_questions = [question for question in dict.fromkeys(question.strip() for question in approval_questions) if question]
            if deduped_questions:
                job["approval_required"] = True
                job["approval_question"] = deduped_questions[0]
        retry_max_attempts = retry_max_attempts_by_step_id.get(step_id)
        if retry_max_attempts is not None:
            job["max_attempts"] = retry_max_attempts
        dependency_edges: list[dict[str, Any]] = []
        depends_on: list[str] = []
        for dependency_step_id in _string_list(step.get("depends_on")):
            dependency_label = label_by_step_id.get(dependency_step_id)
            if not dependency_label:
                continue
            depends_on.append(dependency_label)
            edge_type = "after_success"
            release_condition: dict[str, Any] | None = None
            edge_gate = edge_gate_by_pair.get((dependency_step_id, step_id))
            if isinstance(edge_gate, dict):
                edge_type, release_condition = _edge_gate_runtime_release(edge_gate)
            edge_spec = {
                "label": dependency_label,
                "edge_type": edge_type,
            }
            if release_condition is not None:
                edge_spec["release_condition"] = release_condition
            dependency_edges.append(edge_spec)
        if depends_on:
            job["depends_on"] = depends_on
        if any(edge.get("edge_type") != "after_success" for edge in dependency_edges):
            job["dependency_edges"] = dependency_edges
        jobs.append(job)

    return jobs


def _plan_triggers(definition: dict[str, Any]) -> list[dict[str, Any]]:
    definition = materialize_definition(definition)
    trigger_intent = definition.get("trigger_intent") if isinstance(definition.get("trigger_intent"), list) else []
    triggers: list[dict[str, Any]] = []
    for item in trigger_intent:
        if not isinstance(item, dict):
            continue
        event_type = _as_text(item.get("event_type")) or "manual"
        trigger = {
            "event_type": event_type,
            "filter": item.get("filter") if isinstance(item.get("filter"), dict) else {},
            "source_trigger_id": _as_text(item.get("id")) or None,
        }
        source_ref = _as_text(item.get("source_ref"))
        if source_ref:
            trigger["source_ref"] = source_ref
        cron_expression = _as_text(item.get("cron_expression"))
        if cron_expression:
            trigger["cron_expression"] = cron_expression
        triggers.append(trigger)
    return triggers


def _plan_revision(compiled_spec: dict[str, Any]) -> str:
    payload = json.dumps(compiled_spec, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"plan_{digest}"


def _plan_surface_revision() -> str:
    return module_surface_revision(__file__)


def _plan_compile_provenance(
    *,
    definition: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    definition_compile_provenance = (
        dict(definition.get("compile_provenance"))
        if isinstance(definition.get("compile_provenance"), dict)
        else {}
    )
    normalized_definition = json.loads(json.dumps(definition, default=str))
    input_payload = {
        "artifact_kind": "plan",
        "surface_revision": _plan_surface_revision(),
        "definition_revision": _as_text(definition.get("definition_revision")),
        "definition_content_fingerprint": stable_hash(normalized_definition),
        "definition_input_fingerprint": _as_text(definition_compile_provenance.get("input_fingerprint")),
        "title": title,
    }
    return {
        "artifact_kind": "plan",
        "input_fingerprint": stable_hash(input_payload),
        "surface_revision": input_payload["surface_revision"],
        "definition_revision": input_payload["definition_revision"],
        "definition_content_fingerprint": input_payload["definition_content_fingerprint"],
        "definition_input_fingerprint": input_payload["definition_input_fingerprint"],
        "title": title,
    }


def _compiled_spec_surface_is_current(compiled_spec: dict[str, Any]) -> bool:
    compile_provenance = compiled_spec.get("compile_provenance")
    if not isinstance(compile_provenance, dict):
        return True
    surface_revision = _as_text(compile_provenance.get("surface_revision"))
    if not surface_revision:
        return True
    return surface_revision == _plan_surface_revision()


def _compiled_spec_matches_definition(
    definition: dict[str, Any],
    compiled_spec: dict[str, Any],
) -> bool:
    compile_provenance = compiled_spec.get("compile_provenance")
    if not isinstance(compile_provenance, dict):
        return True
    input_fingerprint = _as_text(compile_provenance.get("input_fingerprint"))
    if not input_fingerprint:
        return False
    expected_provenance = _plan_compile_provenance(
        definition=definition,
        title=_as_text(compiled_spec.get("name")) or _derive_title(
            _as_text(definition.get("source_prose")),
            _as_text(definition.get("compiled_prose")),
        ),
    )
    if input_fingerprint != expected_provenance["input_fingerprint"]:
        return False
    plan_revision = _as_text(compiled_spec.get("plan_revision"))
    if not plan_revision:
        return False
    return plan_revision == _computed_plan_revision(compiled_spec)


def _computed_plan_revision(compiled_spec: dict[str, Any]) -> str:
    payload = json.loads(json.dumps(compiled_spec, default=str))
    payload.pop("plan_revision", None)
    return _plan_revision(payload)


def _agent_for_step(
    step: dict[str, Any],
    *,
    explicit_phase: dict[str, Any] | None,
    reference_slugs: list[str],
    source_block_ids: list[str],
    references_by_slug: dict[str, dict[str, Any]],
    blocks_by_id: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    explicit_route = _as_text((explicit_phase or {}).get("agent_route"))
    referenced_agent_slug = ""
    for slug in reference_slugs:
        reference = references_by_slug.get(slug)
        if isinstance(reference, dict) and reference.get("type") == "agent":
            referenced_agent_slug = slug
            if not explicit_route:
                route = _as_text((reference.get("config") or {}).get("route")) or _infer_agent_route(slug, reference)
                return slug, route
            break

    if explicit_route:
        return referenced_agent_slug, explicit_route

    block_text = " ".join(
        _as_text(blocks_by_id[block_id].get("text"))
        for block_id in source_block_ids
        if block_id in blocks_by_id
    )
    title = _as_text(step.get("title"))
    summary = _as_text(step.get("summary"))
    route = _infer_agent_route(_slugify(title) or "execute-agent", {"description": f"{title} {summary} {block_text}"})
    return "", route


def _integration_job_from_route(
    route: str,
    *,
    step: dict[str, Any],
    explicit_phase: dict[str, Any] | None,
    compiled_prose: str,
    label: str,
) -> tuple[str, str, dict[str, Any]] | None:
    normalized_route = _as_text(route)
    if not normalized_route.startswith("@") or "/" not in normalized_route:
        return None

    integration_id, _, integration_action = normalized_route[1:].partition("/")
    if not integration_id or not integration_action:
        return None

    phase = explicit_phase if isinstance(explicit_phase, dict) else {}
    integration_args = _json_dict(phase.get("integration_args"))

    if normalized_route == "@notifications/send":
        title = _as_text(integration_args.get("title")) or _as_text(step.get("title")) or "Notification"
        message = (
            _as_text(integration_args.get("message"))
            or _as_text(step.get("summary"))
            or _as_text(phase.get("system_prompt"))
            or compiled_prose
            or title
        )
        metadata = {
            **_json_dict(integration_args.get("metadata")),
            "source_step_id": _as_text(step.get("id")) or "",
            "source_node_id": _as_text(step.get("id")) or "",
            "job_label": label,
        }
        return (
            "notifications",
            "send",
            {
                **integration_args,
                "title": title,
                "message": message,
                "status": _as_text(integration_args.get("status")) or "info",
                "metadata": metadata,
            },
        )

    return (
        integration_id,
        integration_action,
        integration_args,
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_as_text(item) for item in value if _as_text(item)]


def _unique_label(base: str, used_labels: set[str]) -> str:
    candidate = base or "step"
    if candidate not in used_labels:
        used_labels.add(candidate)
        return candidate
    index = 2
    while f"{candidate}-{index}" in used_labels:
        index += 1
    resolved = f"{candidate}-{index}"
    used_labels.add(resolved)
    return resolved


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
    if isinstance(value, dict):
        return value
    return {}


__all__ = [
    "PlanningBlockedError",
    "current_compiled_spec",
    "harden_reviewed_definition",
    "missing_execution_manifest_message",
    "missing_execution_plan_message",
    "plan_definition",
    "unresolved_reference_slugs",
]
