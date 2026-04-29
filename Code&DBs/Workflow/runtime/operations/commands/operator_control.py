from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class RoadmapWriteCommand(BaseModel):
    action: str = "preview"
    dry_run: bool | None = None
    title: str | None = None
    intent_brief: str | None = None
    template: str = "single_capability"
    priority: str = "p2"
    parent_roadmap_item_id: str | None = None
    slug: str | None = None
    depends_on: tuple[str, ...] = ()
    source_bug_id: str | None = None
    source_idea_id: str | None = None
    registry_paths: tuple[str, ...] = ()
    decision_ref: str | None = None
    item_kind: str | None = None
    status: str | None = None
    lifecycle: str | None = None
    tier: str | None = None
    phase_ready: bool | None = None
    approval_tag: str | None = None
    reference_doc: str | None = None
    outcome_gate: str | None = None
    proof_kind: str | None = None
    roadmap_item_id: str | None = None
    phase_order: str | None = None


_ROADMAP_PREVIEW_FIRST_ACTIONS = frozenset({"update", "retire", "re-parent", "reparent"})


def _normalized_roadmap_write_action(command: RoadmapWriteCommand) -> tuple[str, dict[str, Any]]:
    action = (command.action or "preview").strip().lower()
    if action in _ROADMAP_PREVIEW_FIRST_ACTIONS:
        if not (command.roadmap_item_id or "").strip():
            raise ValueError(f"roadmap_item_id is required for action='{action}'")
        if action in {"re-parent", "reparent"} and not (
            command.parent_roadmap_item_id or ""
        ).strip():
            raise ValueError("parent_roadmap_item_id is required for action='re-parent'")
        effective_dry_run = command.dry_run is not False
        payload: dict[str, Any] = {
            "action": "preview" if effective_dry_run else "commit",
            "lifecycle": (
                "retired"
                if action == "retire" and command.lifecycle is None
                else command.lifecycle
            ),
            "dry_run": effective_dry_run,
        }
        return action, payload
    return action, {
        "action": command.action,
        "lifecycle": command.lifecycle,
        "dry_run": command.dry_run,
    }


class WorkItemCloseoutCommand(BaseModel):
    action: str = "preview"
    bug_ids: tuple[str, ...] = ()
    roadmap_item_ids: tuple[str, ...] = ()


class OperatorIdeasCommand(BaseModel):
    action: str = "list"
    idea_id: str | None = None
    idea_key: str | None = None
    title: str | None = None
    summary: str | None = None
    source_kind: str = "operator"
    source_ref: str | None = None
    owner_ref: str | None = None
    decision_ref: str | None = None
    status: str | None = None
    resolution_summary: str | None = None
    roadmap_item_id: str | None = None
    promoted_by: str | None = None
    opened_at: datetime | None = None
    resolved_at: datetime | None = None
    promoted_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    idea_ids: tuple[str, ...] = ()
    open_only: bool = True
    limit: int = 50


class TaskRouteEligibilityCommand(BaseModel):
    provider_slug: str
    eligibility_status: str
    effective_to: datetime | None = None
    task_type: str | None = None
    model_slug: str | None = None
    reason_code: str = "operator_control"
    rationale: str | None = None
    effective_from: datetime | None = None
    decision_ref: str | None = None


class TaskRouteRequestCommand(BaseModel):
    task_type: str
    provider_slug: str
    model_slug: str
    sub_task_type: str = "*"
    transport_type: str | None = None
    temperature: float | None = None
    clear_temperature: bool = False
    max_tokens: int | None = None
    clear_max_tokens: bool = False
    reasoning_control: dict[str, Any] | None = None
    clear_reasoning_control: bool = False
    request_contract_ref: str | None = None
    clear_request_contract_ref: bool = False
    cache_policy: dict[str, Any] | None = None
    clear_cache_policy: bool = False
    structured_output_policy: dict[str, Any] | None = None
    clear_structured_output_policy: bool = False
    streaming_policy: dict[str, Any] | None = None
    clear_streaming_policy: bool = False
    reason_code: str = "operator_control"
    rationale: str | None = None
    decision_ref: str | None = None


class NativePrimaryCutoverGateCommand(BaseModel):
    decided_by: str
    decision_source: str
    rationale: str
    roadmap_item_id: str | None = None
    workflow_class_id: str | None = None
    schedule_definition_id: str | None = None
    title: str | None = None
    gate_name: str | None = None
    gate_policy: dict[str, Any] | None = None
    required_evidence: dict[str, Any] | None = None
    decided_at: datetime | None = None
    opened_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FunctionalAreaRecordCommand(BaseModel):
    area_slug: str
    title: str
    summary: str | None = None
    area_status: str = "active"
    created_at: datetime | None = None
    updated_at: datetime | None = None


class OperatorObjectRelationRecordCommand(BaseModel):
    relation_kind: str
    source_kind: str
    source_ref: str
    target_kind: str
    target_ref: str
    relation_status: str = "active"
    relation_metadata: dict[str, Any] | None = None
    bound_by_decision_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ArchitecturePolicyRecordCommand(BaseModel):
    authority_domain: str
    policy_slug: str
    title: str
    rationale: str
    decided_by: str
    decision_source: str
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    decided_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # Migration 302 — drillability + provenance.
    # decision_provenance: 'explicit' (operator unequivocally said so) or
    # 'inferred' (model guessed). Default 'inferred' — model-authored
    # writes should err on the side of advisory until promoted.
    decision_provenance: str | None = None
    # Optional deeper motivation, separate from rationale.
    decision_why: str | None = None


class CircuitOverrideCommand(BaseModel):
    provider_slug: str
    override_state: str
    effective_to: datetime | None = None
    reason_code: str = "operator_control"
    rationale: str | None = None
    effective_from: datetime | None = None
    decided_by: str | None = None
    decision_source: str | None = None


class OperatorDecisionRecordCommand(BaseModel):
    decision_key: str
    decision_kind: str
    title: str
    rationale: str
    decided_by: str
    decision_source: str
    decision_status: str = "decided"
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    decision_scope_kind: str | None = None
    decision_scope_ref: str | None = None
    scope_clamp: dict[str, Any] | None = None


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


def handle_operator_roadmap_write(
    command: RoadmapWriteCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    requested_action, normalized = _normalized_roadmap_write_action(command)
    return OperatorControlFrontdoor().roadmap_write(
        action=normalized["action"],
        title=command.title,
        intent_brief=command.intent_brief,
        template=command.template,
        priority=command.priority,
        parent_roadmap_item_id=command.parent_roadmap_item_id,
        slug=command.slug,
        depends_on=command.depends_on,
        source_bug_id=command.source_bug_id,
        source_idea_id=command.source_idea_id,
        registry_paths=command.registry_paths,
        decision_ref=command.decision_ref,
        item_kind=command.item_kind,
        status=command.status,
        lifecycle=normalized["lifecycle"],
        tier=command.tier,
        phase_ready=command.phase_ready,
        approval_tag=command.approval_tag,
        reference_doc=command.reference_doc,
        outcome_gate=command.outcome_gate,
        proof_kind=command.proof_kind,
        roadmap_item_id=command.roadmap_item_id,
        phase_order=command.phase_order,
        env=_resolved_env(subsystems),
    ) | {"requested_action": requested_action, "dry_run": normalized["dry_run"]}


def handle_operator_ideas(
    command: OperatorIdeasCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().operator_ideas(
        action=command.action,
        idea_id=command.idea_id,
        idea_key=command.idea_key,
        title=command.title,
        summary=command.summary,
        source_kind=command.source_kind,
        source_ref=command.source_ref,
        owner_ref=command.owner_ref,
        decision_ref=command.decision_ref,
        status=command.status,
        resolution_summary=command.resolution_summary,
        roadmap_item_id=command.roadmap_item_id,
        promoted_by=command.promoted_by,
        opened_at=command.opened_at,
        resolved_at=command.resolved_at,
        promoted_at=command.promoted_at,
        created_at=command.created_at,
        updated_at=command.updated_at,
        idea_ids=command.idea_ids,
        open_only=command.open_only,
        limit=command.limit,
        env=_resolved_env(subsystems),
    )


def handle_work_item_closeout(
    command: WorkItemCloseoutCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().reconcile_work_item_closeout(
        action=command.action,
        bug_ids=command.bug_ids,
        roadmap_item_ids=command.roadmap_item_ids,
        env=_resolved_env(subsystems),
    )


def handle_task_route_eligibility(
    command: TaskRouteEligibilityCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().set_task_route_eligibility_window(
        provider_slug=command.provider_slug,
        eligibility_status=command.eligibility_status,
        effective_to=command.effective_to,
        task_type=command.task_type,
        model_slug=command.model_slug,
        reason_code=command.reason_code,
        rationale=command.rationale,
        effective_from=command.effective_from,
        decision_ref=command.decision_ref,
        env=_resolved_env(subsystems),
    )


def handle_task_route_request(
    command: TaskRouteRequestCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    supplied = set(command.model_fields_set)
    fields_to_update = supplied & {
        "temperature",
        "max_tokens",
        "reasoning_control",
        "request_contract_ref",
        "cache_policy",
        "structured_output_policy",
        "streaming_policy",
    }
    if command.clear_temperature:
        fields_to_update.add("temperature")
    if command.clear_max_tokens:
        fields_to_update.add("max_tokens")
    if command.clear_reasoning_control:
        fields_to_update.add("reasoning_control")
    if command.clear_request_contract_ref:
        fields_to_update.add("request_contract_ref")
    if command.clear_cache_policy:
        fields_to_update.add("cache_policy")
    if command.clear_structured_output_policy:
        fields_to_update.add("structured_output_policy")
    if command.clear_streaming_policy:
        fields_to_update.add("streaming_policy")

    return OperatorControlFrontdoor().set_task_route_request(
        task_type=command.task_type,
        provider_slug=command.provider_slug,
        model_slug=command.model_slug,
        sub_task_type=command.sub_task_type,
        transport_type=command.transport_type,
        fields_to_update=frozenset(fields_to_update),
        temperature=command.temperature,
        clear_temperature=command.clear_temperature,
        max_tokens=command.max_tokens,
        clear_max_tokens=command.clear_max_tokens,
        reasoning_control=command.reasoning_control,
        clear_reasoning_control=command.clear_reasoning_control,
        request_contract_ref=command.request_contract_ref,
        clear_request_contract_ref=command.clear_request_contract_ref,
        cache_policy=command.cache_policy,
        clear_cache_policy=command.clear_cache_policy,
        structured_output_policy=command.structured_output_policy,
        clear_structured_output_policy=command.clear_structured_output_policy,
        streaming_policy=command.streaming_policy,
        clear_streaming_policy=command.clear_streaming_policy,
        reason_code=command.reason_code,
        rationale=command.rationale,
        decision_ref=command.decision_ref,
        env=_resolved_env(subsystems),
    )


def handle_native_primary_cutover_gate(
    command: NativePrimaryCutoverGateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().admit_native_primary_cutover_gate(
        decided_by=command.decided_by,
        decision_source=command.decision_source,
        rationale=command.rationale,
        roadmap_item_id=command.roadmap_item_id,
        workflow_class_id=command.workflow_class_id,
        schedule_definition_id=command.schedule_definition_id,
        title=command.title,
        gate_name=command.gate_name,
        gate_policy=command.gate_policy,
        required_evidence=command.required_evidence,
        decided_at=command.decided_at,
        opened_at=command.opened_at,
        created_at=command.created_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


def handle_functional_area_record(
    command: FunctionalAreaRecordCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().record_functional_area(
        area_slug=command.area_slug,
        title=command.title,
        summary=command.summary,
        area_status=command.area_status,
        created_at=command.created_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


def handle_operator_object_relation_record(
    command: OperatorObjectRelationRecordCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().record_operator_object_relation(
        relation_kind=command.relation_kind,
        source_kind=command.source_kind,
        source_ref=command.source_ref,
        target_kind=command.target_kind,
        target_ref=command.target_ref,
        relation_status=command.relation_status,
        relation_metadata=command.relation_metadata,
        bound_by_decision_id=command.bound_by_decision_id,
        created_at=command.created_at,
        updated_at=command.updated_at,
        env=_resolved_env(subsystems),
    )


def handle_architecture_policy_record(
    command: ArchitecturePolicyRecordCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().record_architecture_policy_decision(
        authority_domain=command.authority_domain,
        policy_slug=command.policy_slug,
        title=command.title,
        rationale=command.rationale,
        decided_by=command.decided_by,
        decision_source=command.decision_source,
        effective_from=command.effective_from,
        effective_to=command.effective_to,
        decided_at=command.decided_at,
        created_at=command.created_at,
        updated_at=command.updated_at,
        decision_provenance=command.decision_provenance,
        decision_why=command.decision_why,
        env=_resolved_env(subsystems),
    )


def handle_circuit_override(
    command: CircuitOverrideCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().set_circuit_breaker_override(
        provider_slug=command.provider_slug,
        override_state=command.override_state,
        effective_to=command.effective_to,
        reason_code=command.reason_code,
        rationale=command.rationale,
        effective_from=command.effective_from,
        decided_by=command.decided_by,
        decision_source=command.decision_source,
        env=_resolved_env(subsystems),
    )


def handle_operator_decision_record(
    command: OperatorDecisionRecordCommand,
    subsystems: Any,
) -> dict[str, Any]:
    from surfaces.api.operator_write import OperatorControlFrontdoor

    return OperatorControlFrontdoor().record_operator_decision(
        decision_key=command.decision_key,
        decision_kind=command.decision_kind,
        title=command.title,
        rationale=command.rationale,
        decided_by=command.decided_by,
        decision_source=command.decision_source,
        decision_status=command.decision_status,
        effective_from=command.effective_from,
        effective_to=command.effective_to,
        decision_scope_kind=command.decision_scope_kind,
        decision_scope_ref=command.decision_scope_ref,
        scope_clamp=command.scope_clamp,
        env=_resolved_env(subsystems),
    )


__all__ = [
    "ArchitecturePolicyRecordCommand",
    "CircuitOverrideCommand",
    "FunctionalAreaRecordCommand",
    "NativePrimaryCutoverGateCommand",
    "OperatorIdeasCommand",
    "OperatorDecisionRecordCommand",
    "OperatorObjectRelationRecordCommand",
    "RoadmapWriteCommand",
    "TaskRouteEligibilityCommand",
    "TaskRouteRequestCommand",
    "WorkItemCloseoutCommand",
    "handle_architecture_policy_record",
    "handle_circuit_override",
    "handle_functional_area_record",
    "handle_native_primary_cutover_gate",
    "handle_operator_ideas",
    "handle_operator_decision_record",
    "handle_operator_object_relation_record",
    "handle_operator_roadmap_write",
    "handle_task_route_eligibility",
    "handle_task_route_request",
    "handle_work_item_closeout",
]
