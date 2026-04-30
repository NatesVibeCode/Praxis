"""Deterministic Virtual Lab simulation runtime primitives.

This module is pure domain code. It runs modeled actions against Phase 6
Virtual Lab state, evaluates automation rules, collects traces, and returns
typed assertion/verifier output. It does not call live integrations, read
ambient time, use ambient randomness, persist state, or silently accept
unsupported behavior.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from core.object_truth_ops import canonical_digest, canonical_value

from .state import (
    ActorIdentity,
    EnvironmentRevision,
    EventEnvelope,
    ObjectStateRecord,
    StateCommandResult,
    VirtualLabStateError,
    apply_overlay_patch_command,
    environment_revision_from_dict,
    replace_overlay_command,
    restore_object_command,
    object_state_record_from_dict,
    tombstone_object_command,
    virtual_lab_digest,
)


SIMULATION_SCHEMA_VERSION = 1
SIMULATION_RUNTIME_VERSION = "virtual_lab.simulation.v1"

SimulationStatus = Literal["passed", "failed", "blocked"]
SimulationStopReason = Literal[
    "success",
    "assertion_failed",
    "verifier_failed",
    "guardrail_exceeded",
    "unsupported_capability",
    "runtime_fault",
]
SimulationSeverity = Literal["info", "warning", "error", "blocker"]
SimulationSourceArea = Literal["action", "automation", "assertion", "verifier", "transition", "runtime"]
ActionResultStatus = Literal["succeeded", "no_op", "retryable_error", "terminal_error", "unsupported"]
AutomationStatus = Literal["active", "disabled"]

SUPPORTED_ACTION_KINDS = {
    "patch_object",
    "replace_object_overlay",
    "tombstone_object",
    "restore_object",
}
SUPPORTED_PREDICATE_KINDS = {
    "always",
    "event_type",
    "object_field_equals",
    "payload_field_equals",
}
SUPPORTED_ASSERTION_KINDS = {
    "final_object_field_equals",
    "event_count_at_least",
    "no_blockers",
}
SUPPORTED_VERIFIER_KINDS = {
    "trace_contains_event_type",
    "no_blockers",
    "all_assertions_passed",
}


class SimulationRuntimeError(RuntimeError):
    """Raised when a simulation scenario cannot be represented safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    seed: str
    clock_start: str
    clock_step_seconds: int = 1
    max_actions: int = 100
    max_automation_firings: int = 50
    max_recursion_depth: int = 8

    def __post_init__(self) -> None:
        object.__setattr__(self, "seed", _required_text(self.seed, "seed"))
        object.__setattr__(self, "clock_start", _normalize_datetime(self.clock_start, "clock_start"))
        if int(self.clock_step_seconds) < 0:
            raise SimulationRuntimeError(
                "simulation.invalid_clock_step",
                "clock_step_seconds must be zero or positive",
                details={"clock_step_seconds": self.clock_step_seconds},
            )
        object.__setattr__(self, "clock_step_seconds", int(self.clock_step_seconds))
        for field_name in ("max_actions", "max_automation_firings", "max_recursion_depth"):
            value = int(getattr(self, field_name))
            if value < 1:
                raise SimulationRuntimeError(
                    f"simulation.invalid_{field_name}",
                    f"{field_name} must be positive",
                    details={field_name: value},
                )
            object.__setattr__(self, field_name, value)

    @property
    def config_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.simulation_config.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.simulation_config.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "seed": self.seed,
            "clock_start": self.clock_start,
            "clock_step_seconds": self.clock_step_seconds,
            "max_actions": self.max_actions,
            "max_automation_firings": self.max_automation_firings,
            "max_recursion_depth": self.max_recursion_depth,
        }
        if include_digest:
            payload["config_digest"] = self.config_digest
        return payload


@dataclass(frozen=True, slots=True)
class SimulationAction:
    action_id: str
    action_kind: str
    object_id: str | None = None
    instance_id: str = "primary"
    payload: dict[str, Any] = field(default_factory=dict)
    actor: ActorIdentity | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "action_id", _required_text(self.action_id, "action_id"))
        object.__setattr__(self, "action_kind", _required_text(self.action_kind, "action_kind"))
        object.__setattr__(self, "object_id", _optional_text(self.object_id))
        object.__setattr__(self, "instance_id", _required_text(self.instance_id, "instance_id"))
        object.__setattr__(self, "payload", _mapping(self.payload, "payload"))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_action.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "action_id": self.action_id,
            "action_kind": self.action_kind,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "payload": canonical_value(self.payload),
            "actor": None if self.actor is None else self.actor.to_json(),
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class AutomationPredicate:
    predicate_kind: str
    event_type: str | None = None
    object_id: str | None = None
    instance_id: str = "primary"
    field_path: tuple[str, ...] = ()
    expected: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "predicate_kind", _required_text(self.predicate_kind, "predicate_kind"))
        object.__setattr__(self, "event_type", _optional_text(self.event_type))
        object.__setattr__(self, "object_id", _optional_text(self.object_id))
        object.__setattr__(self, "instance_id", _required_text(self.instance_id, "instance_id"))
        object.__setattr__(self, "field_path", _clean_path(self.field_path))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.automation_predicate.v1",
            "predicate_kind": self.predicate_kind,
            "event_type": self.event_type,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "field_path": list(self.field_path),
            "expected": canonical_value(self.expected),
        }


@dataclass(frozen=True, slots=True)
class AutomationRule:
    rule_id: str
    name: str
    predicate: AutomationPredicate
    effects: tuple[SimulationAction, ...]
    priority: int = 100
    status: AutomationStatus = "active"
    max_firings: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _required_text(self.rule_id, "rule_id"))
        object.__setattr__(self, "name", _required_text(self.name, "name"))
        object.__setattr__(self, "effects", tuple(self.effects or ()))
        object.__setattr__(self, "priority", int(self.priority))
        if self.status not in {"active", "disabled"}:
            raise SimulationRuntimeError(
                "simulation.invalid_automation_status",
                "automation rule status is not supported",
                details={"rule_id": self.rule_id, "status": self.status},
            )
        if self.max_firings is not None and int(self.max_firings) < 1:
            raise SimulationRuntimeError(
                "simulation.invalid_rule_max_firings",
                "max_firings must be positive when provided",
                details={"rule_id": self.rule_id, "max_firings": self.max_firings},
            )
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.automation_rule.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "rule_id": self.rule_id,
            "name": self.name,
            "predicate": self.predicate.to_json(),
            "effects": [effect.to_json() for effect in self.effects],
            "priority": self.priority,
            "status": self.status,
            "max_firings": self.max_firings,
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class SimulationAssertion:
    assertion_id: str
    assertion_kind: str
    object_id: str | None = None
    instance_id: str = "primary"
    field_path: tuple[str, ...] = ()
    expected: Any = None
    event_type: str | None = None
    min_count: int = 1
    severity: SimulationSeverity = "error"

    def __post_init__(self) -> None:
        object.__setattr__(self, "assertion_id", _required_text(self.assertion_id, "assertion_id"))
        object.__setattr__(self, "assertion_kind", _required_text(self.assertion_kind, "assertion_kind"))
        object.__setattr__(self, "object_id", _optional_text(self.object_id))
        object.__setattr__(self, "instance_id", _required_text(self.instance_id, "instance_id"))
        object.__setattr__(self, "field_path", _clean_path(self.field_path))
        object.__setattr__(self, "event_type", _optional_text(self.event_type))
        object.__setattr__(self, "min_count", int(self.min_count))
        object.__setattr__(self, "severity", _severity(self.severity))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_assertion.v1",
            "assertion_id": self.assertion_id,
            "assertion_kind": self.assertion_kind,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "field_path": list(self.field_path),
            "expected": canonical_value(self.expected),
            "event_type": self.event_type,
            "min_count": self.min_count,
            "severity": self.severity,
        }


@dataclass(frozen=True, slots=True)
class SimulationVerifier:
    verifier_id: str
    verifier_kind: str
    event_type: str | None = None
    min_count: int = 1
    severity: SimulationSeverity = "error"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "verifier_id", _required_text(self.verifier_id, "verifier_id"))
        object.__setattr__(self, "verifier_kind", _required_text(self.verifier_kind, "verifier_kind"))
        object.__setattr__(self, "event_type", _optional_text(self.event_type))
        object.__setattr__(self, "min_count", int(self.min_count))
        object.__setattr__(self, "severity", _severity(self.severity))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_verifier.v1",
            "verifier_id": self.verifier_id,
            "verifier_kind": self.verifier_kind,
            "event_type": self.event_type,
            "min_count": self.min_count,
            "severity": self.severity,
            "metadata": canonical_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class SimulationInitialState:
    revision: EnvironmentRevision
    object_states: tuple[ObjectStateRecord, ...]

    def __post_init__(self) -> None:
        states = tuple(self.object_states or ())
        if not states:
            raise SimulationRuntimeError(
                "simulation.initial_state_empty",
                "simulation initial state requires at least one object state",
            )
        seen: set[str] = set()
        for state in states:
            if state.environment_id != self.revision.environment_id or state.revision_id != self.revision.revision_id:
                raise SimulationRuntimeError(
                    "simulation.initial_state_revision_mismatch",
                    "object state must belong to the initial revision",
                    details={"object_id": state.object_id, "instance_id": state.instance_id},
                )
            key = _state_key(state.object_id, state.instance_id)
            if key in seen:
                raise SimulationRuntimeError(
                    "simulation.duplicate_initial_state",
                    "initial state contains duplicate object instances",
                    details={"object_id": state.object_id, "instance_id": state.instance_id},
                )
            seen.add(key)
        object.__setattr__(self, "object_states", tuple(sorted(states, key=lambda item: (item.object_id, item.instance_id))))

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_initial_state.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "revision": self.revision.to_json(),
            "object_states": [state.to_json() for state in self.object_states],
        }


@dataclass(frozen=True, slots=True)
class SimulationScenario:
    scenario_id: str
    initial_state: SimulationInitialState
    actions: tuple[SimulationAction, ...]
    config: SimulationConfig
    automation_rules: tuple[AutomationRule, ...] = ()
    assertions: tuple[SimulationAssertion, ...] = ()
    verifiers: tuple[SimulationVerifier, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenario_id", _required_text(self.scenario_id, "scenario_id"))
        object.__setattr__(self, "actions", tuple(self.actions or ()))
        object.__setattr__(
            self,
            "automation_rules",
            tuple(sorted(tuple(self.automation_rules or ()), key=lambda item: (item.priority, item.rule_id))),
        )
        object.__setattr__(self, "assertions", tuple(self.assertions or ()))
        object.__setattr__(self, "verifiers", tuple(self.verifiers or ()))
        object.__setattr__(self, "metadata", _mapping(self.metadata, "metadata"))

    @property
    def scenario_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.simulation_scenario.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.simulation_scenario.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "scenario_id": self.scenario_id,
            "initial_state": self.initial_state.to_json(),
            "actions": [action.to_json() for action in self.actions],
            "config": self.config.to_json(),
            "automation_rules": [rule.to_json() for rule in self.automation_rules],
            "assertions": [assertion.to_json() for assertion in self.assertions],
            "verifiers": [verifier.to_json() for verifier in self.verifiers],
            "metadata": canonical_value(self.metadata),
        }
        if include_digest:
            payload["scenario_digest"] = self.scenario_digest
        return payload


def simulation_config_from_dict(payload: dict[str, Any]) -> SimulationConfig:
    return SimulationConfig(
        seed=payload.get("seed"),
        clock_start=payload.get("clock_start"),
        clock_step_seconds=int(payload.get("clock_step_seconds", 1)),
        max_actions=int(payload.get("max_actions", 100)),
        max_automation_firings=int(payload.get("max_automation_firings", 50)),
        max_recursion_depth=int(payload.get("max_recursion_depth", 8)),
    )


def simulation_action_from_dict(payload: dict[str, Any]) -> SimulationAction:
    actor_payload = payload.get("actor")
    actor = None
    if isinstance(actor_payload, dict):
        actor = ActorIdentity(
            actor_id=actor_payload.get("actor_id"),
            actor_type=actor_payload.get("actor_type"),
        )
    return SimulationAction(
        action_id=payload.get("action_id"),
        action_kind=payload.get("action_kind"),
        object_id=payload.get("object_id"),
        instance_id=payload.get("instance_id") or "primary",
        payload=_payload_dict(payload.get("payload"), "action.payload"),
        actor=actor,
        metadata=_payload_dict(payload.get("metadata"), "action.metadata"),
    )


def automation_predicate_from_dict(payload: dict[str, Any]) -> AutomationPredicate:
    return AutomationPredicate(
        predicate_kind=payload.get("predicate_kind"),
        event_type=payload.get("event_type"),
        object_id=payload.get("object_id"),
        instance_id=payload.get("instance_id") or "primary",
        field_path=_clean_path(payload.get("field_path")),
        expected=payload.get("expected"),
    )


def automation_rule_from_dict(payload: dict[str, Any]) -> AutomationRule:
    predicate_payload = payload.get("predicate")
    if not isinstance(predicate_payload, dict):
        raise SimulationRuntimeError(
            "simulation.automation_predicate_required",
            "automation rule requires predicate as a JSON object",
            details={"rule_id": payload.get("rule_id")},
        )
    effects_payload = payload.get("effects") or []
    if not isinstance(effects_payload, list) or not all(isinstance(item, dict) for item in effects_payload):
        raise SimulationRuntimeError(
            "simulation.automation_effects_not_list",
            "automation rule effects must be a list of JSON objects",
            details={"rule_id": payload.get("rule_id")},
        )
    return AutomationRule(
        rule_id=payload.get("rule_id"),
        name=payload.get("name"),
        predicate=automation_predicate_from_dict(predicate_payload),
        effects=tuple(simulation_action_from_dict(dict(item)) for item in effects_payload),
        priority=int(payload.get("priority", 100)),
        status=payload.get("status") or "active",
        max_firings=None if payload.get("max_firings") is None else int(payload["max_firings"]),
        metadata=_payload_dict(payload.get("metadata"), "automation_rule.metadata"),
    )


def simulation_assertion_from_dict(payload: dict[str, Any]) -> SimulationAssertion:
    return SimulationAssertion(
        assertion_id=payload.get("assertion_id"),
        assertion_kind=payload.get("assertion_kind"),
        object_id=payload.get("object_id"),
        instance_id=payload.get("instance_id") or "primary",
        field_path=_clean_path(payload.get("field_path")),
        expected=payload.get("expected"),
        event_type=payload.get("event_type"),
        min_count=int(payload.get("min_count", 1)),
        severity=payload.get("severity") or "error",
    )


def simulation_verifier_from_dict(payload: dict[str, Any]) -> SimulationVerifier:
    return SimulationVerifier(
        verifier_id=payload.get("verifier_id"),
        verifier_kind=payload.get("verifier_kind"),
        event_type=payload.get("event_type"),
        min_count=int(payload.get("min_count", 1)),
        severity=payload.get("severity") or "error",
        metadata=_payload_dict(payload.get("metadata"), "verifier.metadata"),
    )


def simulation_initial_state_from_dict(payload: dict[str, Any]) -> SimulationInitialState:
    revision_payload = payload.get("revision")
    state_payloads = payload.get("object_states") or []
    if not isinstance(revision_payload, dict):
        raise SimulationRuntimeError(
            "simulation.initial_revision_required",
            "simulation initial state requires revision as a JSON object",
        )
    if not isinstance(state_payloads, list) or not all(isinstance(item, dict) for item in state_payloads):
        raise SimulationRuntimeError(
            "simulation.initial_object_states_not_list",
            "simulation initial state object_states must be a list of JSON objects",
        )
    return SimulationInitialState(
        revision=environment_revision_from_dict(revision_payload),
        object_states=tuple(object_state_record_from_dict(dict(item)) for item in state_payloads),
    )


def simulation_scenario_from_dict(payload: dict[str, Any]) -> SimulationScenario:
    initial_state_payload = payload.get("initial_state")
    config_payload = payload.get("config")
    if not isinstance(initial_state_payload, dict):
        raise SimulationRuntimeError(
            "simulation.initial_state_required",
            "simulation scenario requires initial_state as a JSON object",
        )
    if not isinstance(config_payload, dict):
        raise SimulationRuntimeError(
            "simulation.config_required",
            "simulation scenario requires config as a JSON object",
        )
    actions = _list_of_dicts(payload.get("actions"), "actions")
    automation_rules = _list_of_dicts(payload.get("automation_rules"), "automation_rules")
    assertions = _list_of_dicts(payload.get("assertions"), "assertions")
    verifiers = _list_of_dicts(payload.get("verifiers"), "verifiers")
    return SimulationScenario(
        scenario_id=payload.get("scenario_id"),
        initial_state=simulation_initial_state_from_dict(initial_state_payload),
        actions=tuple(simulation_action_from_dict(item) for item in actions),
        config=simulation_config_from_dict(config_payload),
        automation_rules=tuple(automation_rule_from_dict(item) for item in automation_rules),
        assertions=tuple(simulation_assertion_from_dict(item) for item in assertions),
        verifiers=tuple(simulation_verifier_from_dict(item) for item in verifiers),
        metadata=_payload_dict(payload.get("metadata"), "scenario.metadata"),
    )


@dataclass(frozen=True, slots=True)
class SimulationTypedGap:
    gap_id: str
    code: str
    message: str
    severity: SimulationSeverity
    source_area: SimulationSourceArea
    trace_event_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_typed_gap.v1",
            "gap_id": self.gap_id,
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "source_area": self.source_area,
            "trace_event_id": self.trace_event_id,
            "details": canonical_value(self.details),
        }


@dataclass(frozen=True, slots=True)
class PromotionBlocker:
    blocker_id: str
    code: str
    message: str
    source_area: SimulationSourceArea
    gap_id: str | None = None
    trace_event_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.promotion_blocker.v1",
            "blocker_id": self.blocker_id,
            "code": self.code,
            "message": self.message,
            "source_area": self.source_area,
            "gap_id": self.gap_id,
            "trace_event_id": self.trace_event_id,
            "details": canonical_value(self.details),
        }


@dataclass(frozen=True, slots=True)
class StateTransition:
    transition_id: str
    object_id: str
    instance_id: str
    event_id: str
    event_type: str
    sequence_number: int
    pre_state_digest: str
    post_state_digest: str
    causation_id: str | None
    action_id: str
    payload: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.state_transition.v1",
            "transition_id": self.transition_id,
            "object_id": self.object_id,
            "instance_id": self.instance_id,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "sequence_number": self.sequence_number,
            "pre_state_digest": self.pre_state_digest,
            "post_state_digest": self.post_state_digest,
            "causation_id": self.causation_id,
            "action_id": self.action_id,
            "payload": canonical_value(self.payload),
        }


@dataclass(frozen=True, slots=True)
class SimulationEvent:
    event_id: str
    sequence_number: int
    event_type: str
    occurred_at: str
    source_area: SimulationSourceArea
    causation_id: str | None
    correlation_id: str
    payload: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.simulation_event.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "event_id": self.event_id,
            "sequence_number": self.sequence_number,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at,
            "source_area": self.source_area,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "payload": canonical_value(self.payload),
        }


@dataclass(frozen=True, slots=True)
class ActionExecutionResult:
    action_id: str
    action_kind: str
    status: ActionResultStatus
    command_id: str
    receipt_status: str | None
    state_event_ids: tuple[str, ...] = ()
    transition_ids: tuple[str, ...] = ()
    gaps: tuple[SimulationTypedGap, ...] = ()
    blockers: tuple[PromotionBlocker, ...] = ()
    message: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.action_execution_result.v1",
            "action_id": self.action_id,
            "action_kind": self.action_kind,
            "status": self.status,
            "command_id": self.command_id,
            "receipt_status": self.receipt_status,
            "state_event_ids": list(self.state_event_ids),
            "transition_ids": list(self.transition_ids),
            "gaps": [gap.to_json() for gap in self.gaps],
            "blockers": [blocker.to_json() for blocker in self.blockers],
            "message": self.message,
            "data": canonical_value(self.data),
        }


@dataclass(frozen=True, slots=True)
class AutomationEvaluationResult:
    rule_id: str
    triggering_event_id: str
    eligible: bool
    reason_code: str
    message: str

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.automation_evaluation_result.v1",
            "rule_id": self.rule_id,
            "triggering_event_id": self.triggering_event_id,
            "eligible": self.eligible,
            "reason_code": self.reason_code,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class AutomationFiringResult:
    firing_id: str
    rule_id: str
    triggering_event_id: str
    effect_action_ids: tuple[str, ...]
    recursion_depth: int

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.automation_firing_result.v1",
            "firing_id": self.firing_id,
            "rule_id": self.rule_id,
            "triggering_event_id": self.triggering_event_id,
            "effect_action_ids": list(self.effect_action_ids),
            "recursion_depth": self.recursion_depth,
        }


@dataclass(frozen=True, slots=True)
class AssertionResult:
    assertion_id: str
    assertion_kind: str
    passed: bool
    severity: SimulationSeverity
    message: str
    location: dict[str, Any] = field(default_factory=dict)
    expected: Any = None
    actual: Any = None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.assertion_result.v1",
            "assertion_id": self.assertion_id,
            "assertion_kind": self.assertion_kind,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "location": canonical_value(self.location),
            "expected": canonical_value(self.expected),
            "actual": canonical_value(self.actual),
        }


@dataclass(frozen=True, slots=True)
class VerifierResult:
    verifier_id: str
    verifier_kind: str
    status: Literal["passed", "failed", "error"]
    severity: SimulationSeverity
    findings: tuple[dict[str, Any], ...] = ()
    summary: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "virtual_lab.verifier_result.v1",
            "verifier_id": self.verifier_id,
            "verifier_kind": self.verifier_kind,
            "status": self.status,
            "severity": self.severity,
            "findings": [canonical_value(finding) for finding in self.findings],
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True)
class SimulationTrace:
    run_id: str
    events: tuple[SimulationEvent, ...] = ()
    state_events: tuple[EventEnvelope, ...] = ()
    transitions: tuple[StateTransition, ...] = ()
    automation_evaluations: tuple[AutomationEvaluationResult, ...] = ()
    automation_firings: tuple[AutomationFiringResult, ...] = ()

    @property
    def trace_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.simulation_trace.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.simulation_trace.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "run_id": self.run_id,
            "events": [event.to_json() for event in self.events],
            "state_events": [event.to_json() for event in self.state_events],
            "transitions": [transition.to_json() for transition in self.transitions],
            "automation_evaluations": [item.to_json() for item in self.automation_evaluations],
            "automation_firings": [item.to_json() for item in self.automation_firings],
        }
        if include_digest:
            payload["trace_digest"] = self.trace_digest
        return payload


@dataclass(frozen=True, slots=True)
class SimulationRunResult:
    run_id: str
    scenario_id: str
    status: SimulationStatus
    stop_reason: SimulationStopReason
    trace: SimulationTrace
    final_state: tuple[ObjectStateRecord, ...]
    action_results: tuple[ActionExecutionResult, ...]
    assertion_results: tuple[AssertionResult, ...]
    verifier_results: tuple[VerifierResult, ...]
    gaps: tuple[SimulationTypedGap, ...]
    blockers: tuple[PromotionBlocker, ...]
    warnings: tuple[dict[str, Any], ...] = ()

    @property
    def result_digest(self) -> str:
        return virtual_lab_digest(self.to_json(include_digest=False), purpose="virtual_lab.simulation_result.v1")

    def to_json(self, *, include_digest: bool = True) -> dict[str, Any]:
        payload = {
            "kind": "virtual_lab.simulation_run_result.v1",
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "runtime_version": SIMULATION_RUNTIME_VERSION,
            "run_id": self.run_id,
            "scenario_id": self.scenario_id,
            "status": self.status,
            "stop_reason": self.stop_reason,
            "trace": self.trace.to_json(),
            "final_state": [state.to_json() for state in self.final_state],
            "action_results": [item.to_json() for item in self.action_results],
            "assertion_results": [item.to_json() for item in self.assertion_results],
            "verifier_results": [item.to_json() for item in self.verifier_results],
            "gaps": [gap.to_json() for gap in self.gaps],
            "blockers": [blocker.to_json() for blocker in self.blockers],
            "warnings": [canonical_value(warning) for warning in self.warnings],
        }
        if include_digest:
            payload["result_digest"] = self.result_digest
        return payload


@dataclass(slots=True)
class _PendingAction:
    action: SimulationAction
    source: Literal["scenario", "automation"]
    causation_id: str | None
    recursion_depth: int
    rule_id: str | None = None
    firing_ordinal: int | None = None


@dataclass(slots=True)
class _SimulationContext:
    scenario: SimulationScenario
    run_id: str
    correlation_id: str
    clock: "_ControlledClock"
    states: dict[str, ObjectStateRecord]
    state_events: list[EventEnvelope] = field(default_factory=list)
    transitions: list[StateTransition] = field(default_factory=list)
    events: list[SimulationEvent] = field(default_factory=list)
    action_results: list[ActionExecutionResult] = field(default_factory=list)
    automation_evaluations: list[AutomationEvaluationResult] = field(default_factory=list)
    automation_firings: list[AutomationFiringResult] = field(default_factory=list)
    assertion_results: list[AssertionResult] = field(default_factory=list)
    verifier_results: list[VerifierResult] = field(default_factory=list)
    gaps: list[SimulationTypedGap] = field(default_factory=list)
    blockers: list[PromotionBlocker] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    action_count: int = 0
    automation_firing_count: int = 0
    rule_firing_counts: dict[str, int] = field(default_factory=dict)


class _ControlledClock:
    def __init__(self, config: SimulationConfig) -> None:
        self._start = _parse_datetime(config.clock_start)
        self._step = timedelta(seconds=config.clock_step_seconds)
        self._ticks = 0

    def tick(self) -> str:
        value = self._start + (self._step * self._ticks)
        self._ticks += 1
        return _datetime_to_utc(value)


def run_simulation_scenario(scenario: SimulationScenario, *, run_id: str | None = None) -> SimulationRunResult:
    """Run a deterministic scenario against modeled Virtual Lab state."""

    resolved_run_id = _optional_text(run_id) or _run_id_for_scenario(scenario)
    context = _SimulationContext(
        scenario=scenario,
        run_id=resolved_run_id,
        correlation_id=resolved_run_id,
        clock=_ControlledClock(scenario.config),
        states={
            _state_key(state.object_id, state.instance_id): state
            for state in scenario.initial_state.object_states
        },
    )
    queue: deque[_PendingAction] = deque(
        _PendingAction(action=action, source="scenario", causation_id=None, recursion_depth=0)
        for action in scenario.actions
    )

    stop_reason: SimulationStopReason = "success"
    while queue:
        if context.action_count >= scenario.config.max_actions:
            _append_gap_and_blocker(
                context,
                code="simulation.max_actions_exceeded",
                message="simulation exceeded the configured max action count",
                source_area="runtime",
                details={"max_actions": scenario.config.max_actions},
            )
            _append_event(
                context,
                "runtime.guardrail_exceeded",
                "runtime",
                None,
                {"reason_code": "simulation.max_actions_exceeded"},
            )
            stop_reason = "guardrail_exceeded"
            break

        pending = queue.popleft()
        result, state_events = _dispatch_action(context, pending)
        context.action_results.append(result)
        context.gaps.extend(result.gaps)
        context.blockers.extend(result.blockers)
        context.action_count += 1

        if result.status == "unsupported":
            stop_reason = "unsupported_capability"
            break
        if result.status == "terminal_error":
            stop_reason = "runtime_fault"
            break

        pending_automation = _evaluate_automations(context, state_events, pending.recursion_depth)
        if context.blockers and any(blocker.code.startswith("simulation.automation_loop_guard") for blocker in context.blockers):
            stop_reason = "guardrail_exceeded"
            break
        if pending_automation:
            queue = deque(pending_automation + list(queue))

    context.assertion_results.extend(_run_assertions(context))
    context.verifier_results.extend(_run_verifiers(context))
    severe_assertion_failed = any(
        not item.passed and item.severity in {"error", "blocker"}
        for item in context.assertion_results
    )
    if not context.scenario.verifiers and not context.blockers and not severe_assertion_failed:
        verifier_event = _append_event(
            context,
            "verifier.required",
            "verifier",
            None,
            {"reason_code": "simulation.verifier_required"},
        )
        gap, blocker = _gap_and_blocker(
            code="simulation.verifier_required",
            message="simulation cannot report green status without at least one verifier",
            source_area="verifier",
            trace_event_id=verifier_event.event_id,
        )
        context.gaps.append(gap)
        context.blockers.append(blocker)

    if stop_reason == "success":
        if context.blockers:
            stop_reason = _blocker_stop_reason(context.blockers)
        elif any(not item.passed and item.severity in {"error", "blocker"} for item in context.assertion_results):
            stop_reason = "assertion_failed"
        elif any(item.status != "passed" and item.severity in {"error", "blocker"} for item in context.verifier_results):
            stop_reason = "verifier_failed"

    status: SimulationStatus
    if context.blockers or stop_reason in {"guardrail_exceeded", "unsupported_capability", "runtime_fault"}:
        status = "blocked"
    elif stop_reason in {"assertion_failed", "verifier_failed"}:
        status = "failed"
    else:
        status = "passed"

    trace = SimulationTrace(
        run_id=context.run_id,
        events=tuple(context.events),
        state_events=tuple(context.state_events),
        transitions=tuple(context.transitions),
        automation_evaluations=tuple(context.automation_evaluations),
        automation_firings=tuple(context.automation_firings),
    )
    return SimulationRunResult(
        run_id=context.run_id,
        scenario_id=scenario.scenario_id,
        status=status,
        stop_reason=stop_reason,
        trace=trace,
        final_state=tuple(context.states[key] for key in sorted(context.states)),
        action_results=tuple(context.action_results),
        assertion_results=tuple(context.assertion_results),
        verifier_results=tuple(context.verifier_results),
        gaps=_dedupe_gaps(context.gaps),
        blockers=_dedupe_blockers(context.blockers),
        warnings=tuple(context.warnings),
    )


def _dispatch_action(
    context: _SimulationContext,
    pending: _PendingAction,
) -> tuple[ActionExecutionResult, tuple[EventEnvelope, ...]]:
    action = pending.action
    actor = action.actor or ActorIdentity(actor_id="virtual_lab.simulation", actor_type="system")
    command_id = _command_id(action, pending)
    _append_event(
        context,
        "action.started",
        "action",
        pending.causation_id,
        {
            "action": action.to_json(),
            "command_id": command_id,
            "source": pending.source,
            "rule_id": pending.rule_id,
            "recursion_depth": pending.recursion_depth,
        },
    )

    if action.action_kind not in SUPPORTED_ACTION_KINDS:
        gap, blocker = _gap_and_blocker(
            code="simulation.unsupported_action",
            message="simulated action kind is not supported",
            source_area="action",
            details={"action_id": action.action_id, "action_kind": action.action_kind},
        )
        result = ActionExecutionResult(
            action_id=action.action_id,
            action_kind=action.action_kind,
            status="unsupported",
            command_id=command_id,
            receipt_status=None,
            gaps=(gap,),
            blockers=(blocker,),
            message="unsupported simulated action",
        )
        result_event = _append_event(
            context,
            "action.result",
            "action",
            pending.causation_id,
            {"result": result.to_json()},
        )
        gap = _with_trace_event(gap, result_event.event_id)
        blocker = _blocker_with_trace_event(blocker, result_event.event_id, gap.gap_id)
        return (
            ActionExecutionResult(
                action_id=result.action_id,
                action_kind=result.action_kind,
                status=result.status,
                command_id=result.command_id,
                receipt_status=result.receipt_status,
                gaps=(gap,),
                blockers=(blocker,),
                message=result.message,
            ),
            (),
        )

    state = _state_for_action(context, action)
    if state is None:
        gap, blocker = _gap_and_blocker(
            code="simulation.object_state_not_found",
            message="simulated action targets an object instance missing from initial state",
            source_area="action",
            details={"action_id": action.action_id, "object_id": action.object_id, "instance_id": action.instance_id},
        )
        result = ActionExecutionResult(
            action_id=action.action_id,
            action_kind=action.action_kind,
            status="terminal_error",
            command_id=command_id,
            receipt_status=None,
            gaps=(gap,),
            blockers=(blocker,),
            message="target object state not found",
        )
        result_event = _append_event(context, "action.result", "action", pending.causation_id, {"result": result.to_json()})
        gap = _with_trace_event(gap, result_event.event_id)
        blocker = _blocker_with_trace_event(blocker, result_event.event_id, gap.gap_id)
        return (
            ActionExecutionResult(
                action_id=result.action_id,
                action_kind=result.action_kind,
                status=result.status,
                command_id=result.command_id,
                receipt_status=result.receipt_status,
                gaps=(gap,),
                blockers=(blocker,),
                message=result.message,
            ),
            (),
        )

    occurred_at = context.clock.tick()
    recorded_at = context.clock.tick()
    try:
        state_result = _execute_state_command(
            context=context,
            action=action,
            state=state,
            actor=actor,
            command_id=command_id,
            occurred_at=occurred_at,
            recorded_at=recorded_at,
            causation_id=pending.causation_id,
        )
    except (SimulationRuntimeError, VirtualLabStateError) as exc:
        reason_code = getattr(exc, "reason_code", "simulation.action_runtime_error")
        details = getattr(exc, "details", {})
        gap, blocker = _gap_and_blocker(
            code=reason_code,
            message=str(exc),
            source_area="action",
            details={"action_id": action.action_id, **details},
        )
        result = ActionExecutionResult(
            action_id=action.action_id,
            action_kind=action.action_kind,
            status="terminal_error",
            command_id=command_id,
            receipt_status=None,
            gaps=(gap,),
            blockers=(blocker,),
            message=str(exc),
        )
        result_event = _append_event(context, "action.result", "action", pending.causation_id, {"result": result.to_json()})
        gap = _with_trace_event(gap, result_event.event_id)
        blocker = _blocker_with_trace_event(blocker, result_event.event_id, gap.gap_id)
        return (
            ActionExecutionResult(
                action_id=result.action_id,
                action_kind=result.action_kind,
                status=result.status,
                command_id=result.command_id,
                receipt_status=result.receipt_status,
                gaps=(gap,),
                blockers=(blocker,),
                message=result.message,
            ),
            (),
        )

    emitted_events = state_result.events
    transitions = tuple(_transition_from_event(event, action) for event in emitted_events)
    if state_result.receipt.status in {"accepted", "no_op"}:
        context.states[_state_key(state_result.state.object_id, state_result.state.instance_id)] = state_result.state
    context.state_events.extend(emitted_events)
    context.transitions.extend(transitions)

    result_status = _action_status_from_receipt(state_result.receipt.status)
    gaps: tuple[SimulationTypedGap, ...] = ()
    blockers: tuple[PromotionBlocker, ...] = ()
    if result_status == "terminal_error":
        gap, blocker = _gap_and_blocker(
            code="simulation.action_receipt_rejected",
            message="state command returned a terminal receipt",
            source_area="action",
            details={"receipt": state_result.receipt.to_json()},
        )
        gaps = (gap,)
        blockers = (blocker,)
    result = ActionExecutionResult(
        action_id=action.action_id,
        action_kind=action.action_kind,
        status=result_status,
        command_id=command_id,
        receipt_status=state_result.receipt.status,
        state_event_ids=tuple(event.event_id for event in emitted_events),
        transition_ids=tuple(transition.transition_id for transition in transitions),
        gaps=gaps,
        blockers=blockers,
        message="action dispatched through virtual lab state command",
        data={"receipt": state_result.receipt.to_json()},
    )
    result_event = _append_event(
        context,
        "action.result",
        "action",
        pending.causation_id,
        {"result": result.to_json()},
    )
    if gaps or blockers:
        gaps = tuple(_with_trace_event(gap, result_event.event_id) for gap in gaps)
        blockers = tuple(_blocker_with_trace_event(blocker, result_event.event_id, gaps[0].gap_id) for blocker in blockers)
        result = ActionExecutionResult(
            action_id=result.action_id,
            action_kind=result.action_kind,
            status=result.status,
            command_id=result.command_id,
            receipt_status=result.receipt_status,
            state_event_ids=result.state_event_ids,
            transition_ids=result.transition_ids,
            gaps=gaps,
            blockers=blockers,
            message=result.message,
            data=result.data,
        )
    return result, emitted_events


def _execute_state_command(
    *,
    context: _SimulationContext,
    action: SimulationAction,
    state: ObjectStateRecord,
    actor: ActorIdentity,
    command_id: str,
    occurred_at: str,
    recorded_at: str,
    causation_id: str | None,
) -> StateCommandResult:
    revision = context.scenario.initial_state.revision
    common = {
        "revision": revision,
        "state": state,
        "actor": actor,
        "command_id": command_id,
        "occurred_at": occurred_at,
        "recorded_at": recorded_at,
        "stream_events": tuple(context.state_events),
        "causation_id": causation_id,
        "correlation_id": context.correlation_id,
    }
    if action.action_kind == "patch_object":
        return apply_overlay_patch_command(
            **common,
            patch=_required_payload_mapping(action, "patch"),
        )
    if action.action_kind == "replace_object_overlay":
        return replace_overlay_command(
            revision=revision,
            state=state,
            actor=actor,
            command_id=command_id,
            occurred_at=occurred_at,
            recorded_at=recorded_at,
            stream_events=tuple(context.state_events),
            overlay_state=_required_payload_mapping(action, "overlay_state"),
        )
    if action.action_kind == "tombstone_object":
        return tombstone_object_command(
            revision=revision,
            state=state,
            actor=actor,
            command_id=command_id,
            occurred_at=occurred_at,
            recorded_at=recorded_at,
            stream_events=tuple(context.state_events),
        )
    if action.action_kind == "restore_object":
        return restore_object_command(
            revision=revision,
            state=state,
            actor=actor,
            command_id=command_id,
            occurred_at=occurred_at,
            recorded_at=recorded_at,
            stream_events=tuple(context.state_events),
        )
    raise SimulationRuntimeError(
        "simulation.unsupported_action",
        "simulated action kind is not supported",
        details={"action_kind": action.action_kind},
    )


def _evaluate_automations(
    context: _SimulationContext,
    state_events: tuple[EventEnvelope, ...],
    recursion_depth: int,
) -> list[_PendingAction]:
    pending: list[_PendingAction] = []
    for state_event in state_events:
        for rule in context.scenario.automation_rules:
            evaluation = _evaluate_rule(context, rule, state_event)
            context.automation_evaluations.append(evaluation)
            _append_event(
                context,
                "automation.evaluated",
                "automation",
                state_event.event_id,
                {"evaluation": evaluation.to_json()},
            )
            if not evaluation.eligible:
                continue

            if recursion_depth + 1 > context.scenario.config.max_recursion_depth:
                _append_automation_loop_guard(
                    context,
                    "simulation.automation_loop_guard.recursion_depth",
                    "automation recursion depth exceeded the configured guardrail",
                    state_event,
                    rule,
                    {"max_recursion_depth": context.scenario.config.max_recursion_depth},
                )
                return pending
            if context.automation_firing_count >= context.scenario.config.max_automation_firings:
                _append_automation_loop_guard(
                    context,
                    "simulation.automation_loop_guard.max_firings",
                    "automation firing count exceeded the configured guardrail",
                    state_event,
                    rule,
                    {"max_automation_firings": context.scenario.config.max_automation_firings},
                )
                return pending

            rule_count = context.rule_firing_counts.get(rule.rule_id, 0)
            if rule.max_firings is not None and rule_count >= rule.max_firings:
                _append_automation_loop_guard(
                    context,
                    "simulation.automation_loop_guard.rule_max_firings",
                    "automation rule exceeded its configured firing guardrail",
                    state_event,
                    rule,
                    {"max_firings": rule.max_firings},
                )
                return pending

            context.automation_firing_count += 1
            firing_ordinal = context.automation_firing_count
            context.rule_firing_counts[rule.rule_id] = rule_count + 1
            firing = AutomationFiringResult(
                firing_id=_stable_ref(
                    "virtual_lab_automation_firing",
                    {
                        "run_id": context.run_id,
                        "rule_id": rule.rule_id,
                        "triggering_event_id": state_event.event_id,
                        "ordinal": firing_ordinal,
                    },
                ),
                rule_id=rule.rule_id,
                triggering_event_id=state_event.event_id,
                effect_action_ids=tuple(effect.action_id for effect in rule.effects),
                recursion_depth=recursion_depth + 1,
            )
            context.automation_firings.append(firing)
            firing_event = _append_event(
                context,
                "automation.fired",
                "automation",
                state_event.event_id,
                {"firing": firing.to_json()},
            )
            for effect in rule.effects:
                pending.append(
                    _PendingAction(
                        action=effect,
                        source="automation",
                        causation_id=firing_event.event_id,
                        recursion_depth=recursion_depth + 1,
                        rule_id=rule.rule_id,
                        firing_ordinal=firing_ordinal,
                    )
                )
    return pending


def _evaluate_rule(
    context: _SimulationContext,
    rule: AutomationRule,
    state_event: EventEnvelope,
) -> AutomationEvaluationResult:
    if rule.status != "active":
        return AutomationEvaluationResult(rule.rule_id, state_event.event_id, False, "rule_disabled", "rule is disabled")
    predicate = rule.predicate
    if predicate.predicate_kind not in SUPPORTED_PREDICATE_KINDS:
        gap, blocker = _gap_and_blocker(
            code="simulation.unsupported_rule_predicate",
            message="automation rule predicate kind is not supported",
            source_area="automation",
            details={"rule_id": rule.rule_id, "predicate_kind": predicate.predicate_kind},
        )
        context.gaps.append(gap)
        context.blockers.append(blocker)
        return AutomationEvaluationResult(
            rule.rule_id,
            state_event.event_id,
            False,
            "unsupported_rule_predicate",
            "rule predicate kind is unsupported",
        )
    if predicate.predicate_kind == "always":
        return AutomationEvaluationResult(rule.rule_id, state_event.event_id, True, "eligible", "predicate always matches")
    if predicate.predicate_kind == "event_type":
        eligible = state_event.event_type == predicate.event_type
        return AutomationEvaluationResult(
            rule.rule_id,
            state_event.event_id,
            eligible,
            "eligible" if eligible else "event_type_mismatch",
            "event type matched" if eligible else "event type did not match",
        )
    if predicate.predicate_kind == "object_field_equals":
        state = context.states.get(_state_key(predicate.object_id, predicate.instance_id))
        actual = None if state is None else _read_path(state.effective_state, predicate.field_path)
        eligible = actual == predicate.expected
        return AutomationEvaluationResult(
            rule.rule_id,
            state_event.event_id,
            eligible,
            "eligible" if eligible else "object_field_mismatch",
            "object field matched" if eligible else "object field did not match",
        )
    if predicate.predicate_kind == "payload_field_equals":
        actual = _read_path(state_event.payload, predicate.field_path)
        eligible = actual == predicate.expected
        return AutomationEvaluationResult(
            rule.rule_id,
            state_event.event_id,
            eligible,
            "eligible" if eligible else "payload_field_mismatch",
            "payload field matched" if eligible else "payload field did not match",
        )
    raise AssertionError("predicate support drift")


def _run_assertions(context: _SimulationContext) -> tuple[AssertionResult, ...]:
    results: list[AssertionResult] = []
    for assertion in context.scenario.assertions:
        if assertion.assertion_kind not in SUPPORTED_ASSERTION_KINDS:
            gap, blocker = _gap_and_blocker(
                code="simulation.unverifiable_assertion",
                message="simulation assertion kind is not supported",
                source_area="assertion",
                details={"assertion_id": assertion.assertion_id, "assertion_kind": assertion.assertion_kind},
            )
            context.gaps.append(gap)
            context.blockers.append(blocker)
            results.append(
                AssertionResult(
                    assertion_id=assertion.assertion_id,
                    assertion_kind=assertion.assertion_kind,
                    passed=False,
                    severity="blocker",
                    message="assertion kind is unsupported",
                )
            )
            continue
        if assertion.assertion_kind == "final_object_field_equals":
            state = context.states.get(_state_key(assertion.object_id, assertion.instance_id))
            actual = None if state is None else _read_path(state.effective_state, assertion.field_path)
            passed = actual == assertion.expected
            results.append(
                AssertionResult(
                    assertion_id=assertion.assertion_id,
                    assertion_kind=assertion.assertion_kind,
                    passed=passed,
                    severity=assertion.severity,
                    message="final object field matched" if passed else "final object field did not match",
                    location={
                        "object_id": assertion.object_id,
                        "instance_id": assertion.instance_id,
                        "field_path": list(assertion.field_path),
                    },
                    expected=assertion.expected,
                    actual=actual,
                )
            )
        elif assertion.assertion_kind == "event_count_at_least":
            actual_count = sum(1 for event in context.state_events if event.event_type == assertion.event_type)
            passed = actual_count >= assertion.min_count
            results.append(
                AssertionResult(
                    assertion_id=assertion.assertion_id,
                    assertion_kind=assertion.assertion_kind,
                    passed=passed,
                    severity=assertion.severity,
                    message="event count met threshold" if passed else "event count did not meet threshold",
                    location={"event_type": assertion.event_type},
                    expected={"min_count": assertion.min_count},
                    actual={"count": actual_count},
                )
            )
        elif assertion.assertion_kind == "no_blockers":
            passed = not context.blockers
            results.append(
                AssertionResult(
                    assertion_id=assertion.assertion_id,
                    assertion_kind=assertion.assertion_kind,
                    passed=passed,
                    severity=assertion.severity,
                    message="no blockers present" if passed else "blockers are present",
                    expected={"blocker_count": 0},
                    actual={"blocker_count": len(context.blockers)},
                )
            )
    for result in results:
        _append_event(context, "assertion.evaluated", "assertion", None, {"assertion_result": result.to_json()})
    return tuple(results)


def _run_verifiers(context: _SimulationContext) -> tuple[VerifierResult, ...]:
    results: list[VerifierResult] = []
    for verifier in context.scenario.verifiers:
        if verifier.verifier_kind not in SUPPORTED_VERIFIER_KINDS:
            gap, blocker = _gap_and_blocker(
                code="simulation.unsupported_verifier",
                message="simulation verifier kind is not supported",
                source_area="verifier",
                details={"verifier_id": verifier.verifier_id, "verifier_kind": verifier.verifier_kind},
            )
            context.gaps.append(gap)
            context.blockers.append(blocker)
            result = VerifierResult(
                verifier_id=verifier.verifier_id,
                verifier_kind=verifier.verifier_kind,
                status="error",
                severity="blocker",
                findings=(gap.to_json(),),
                summary="verifier kind is unsupported",
            )
        elif verifier.verifier_kind == "trace_contains_event_type":
            actual_count = sum(1 for event in context.state_events if event.event_type == verifier.event_type)
            passed = actual_count >= verifier.min_count
            result = VerifierResult(
                verifier_id=verifier.verifier_id,
                verifier_kind=verifier.verifier_kind,
                status="passed" if passed else "failed",
                severity=verifier.severity,
                findings=(
                    {
                        "code": "simulation.verifier.event_count",
                        "event_type": verifier.event_type,
                        "expected_min_count": verifier.min_count,
                        "actual_count": actual_count,
                    },
                ),
                summary="required event type observed" if passed else "required event type missing",
            )
        elif verifier.verifier_kind == "no_blockers":
            passed = not context.blockers
            result = VerifierResult(
                verifier_id=verifier.verifier_id,
                verifier_kind=verifier.verifier_kind,
                status="passed" if passed else "failed",
                severity=verifier.severity,
                findings=tuple(blocker.to_json() for blocker in context.blockers),
                summary="no blockers present" if passed else "blockers present",
            )
        elif verifier.verifier_kind == "all_assertions_passed":
            failed = [item.to_json() for item in context.assertion_results if not item.passed]
            result = VerifierResult(
                verifier_id=verifier.verifier_id,
                verifier_kind=verifier.verifier_kind,
                status="passed" if not failed else "failed",
                severity=verifier.severity,
                findings=tuple(failed),
                summary="all assertions passed" if not failed else "one or more assertions failed",
            )
        else:
            raise AssertionError("verifier support drift")
        results.append(result)
        _append_event(context, "verifier.evaluated", "verifier", None, {"verifier_result": result.to_json()})
    return tuple(results)


def _append_event(
    context: _SimulationContext,
    event_type: str,
    source_area: SimulationSourceArea,
    causation_id: str | None,
    payload: dict[str, Any],
) -> SimulationEvent:
    sequence_number = len(context.events) + 1
    occurred_at = context.clock.tick()
    basis = {
        "run_id": context.run_id,
        "sequence_number": sequence_number,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "source_area": source_area,
        "causation_id": causation_id,
        "correlation_id": context.correlation_id,
        "payload": canonical_value(payload),
    }
    event = SimulationEvent(
        event_id=_stable_ref("virtual_lab_simulation_event", basis),
        sequence_number=sequence_number,
        event_type=event_type,
        occurred_at=occurred_at,
        source_area=source_area,
        causation_id=causation_id,
        correlation_id=context.correlation_id,
        payload=payload,
    )
    context.events.append(event)
    return event


def _append_automation_loop_guard(
    context: _SimulationContext,
    code: str,
    message: str,
    state_event: EventEnvelope,
    rule: AutomationRule,
    details: dict[str, Any],
) -> None:
    event = _append_event(
        context,
        "runtime.guardrail_exceeded",
        "automation",
        state_event.event_id,
        {
            "reason_code": code,
            "rule_id": rule.rule_id,
            "triggering_event_id": state_event.event_id,
            "details": canonical_value(details),
        },
    )
    gap, blocker = _gap_and_blocker(
        code=code,
        message=message,
        source_area="automation",
        trace_event_id=event.event_id,
        details={"rule_id": rule.rule_id, "triggering_event_id": state_event.event_id, **details},
    )
    context.gaps.append(gap)
    context.blockers.append(blocker)


def _append_gap_and_blocker(
    context: _SimulationContext,
    *,
    code: str,
    message: str,
    source_area: SimulationSourceArea,
    details: dict[str, Any] | None = None,
) -> None:
    gap, blocker = _gap_and_blocker(code=code, message=message, source_area=source_area, details=details)
    context.gaps.append(gap)
    context.blockers.append(blocker)


def _gap_and_blocker(
    *,
    code: str,
    message: str,
    source_area: SimulationSourceArea,
    trace_event_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> tuple[SimulationTypedGap, PromotionBlocker]:
    basis = {
        "code": code,
        "message": message,
        "source_area": source_area,
        "trace_event_id": trace_event_id,
        "details": canonical_value(details or {}),
    }
    gap_id = _stable_ref("virtual_lab_simulation_gap", basis)
    gap = SimulationTypedGap(
        gap_id=gap_id,
        code=code,
        message=message,
        severity="blocker",
        source_area=source_area,
        trace_event_id=trace_event_id,
        details=details or {},
    )
    blocker = PromotionBlocker(
        blocker_id=_stable_ref("virtual_lab_promotion_blocker", basis),
        code=code,
        message=message,
        source_area=source_area,
        gap_id=gap_id,
        trace_event_id=trace_event_id,
        details=details or {},
    )
    return gap, blocker


def _with_trace_event(gap: SimulationTypedGap, trace_event_id: str) -> SimulationTypedGap:
    return SimulationTypedGap(
        gap_id=gap.gap_id,
        code=gap.code,
        message=gap.message,
        severity=gap.severity,
        source_area=gap.source_area,
        trace_event_id=trace_event_id,
        details=gap.details,
    )


def _blocker_with_trace_event(blocker: PromotionBlocker, trace_event_id: str, gap_id: str | None) -> PromotionBlocker:
    return PromotionBlocker(
        blocker_id=blocker.blocker_id,
        code=blocker.code,
        message=blocker.message,
        source_area=blocker.source_area,
        gap_id=gap_id,
        trace_event_id=trace_event_id,
        details=blocker.details,
    )


def _transition_from_event(event: EventEnvelope, action: SimulationAction) -> StateTransition:
    transition = StateTransition(
        transition_id=_stable_ref(
            "virtual_lab_state_transition",
            {"event_id": event.event_id, "action_id": action.action_id},
        ),
        object_id=action.object_id or "",
        instance_id=action.instance_id,
        event_id=event.event_id,
        event_type=event.event_type,
        sequence_number=event.sequence_number,
        pre_state_digest=event.pre_state_digest,
        post_state_digest=event.post_state_digest,
        causation_id=event.causation_id,
        action_id=action.action_id,
        payload=event.payload,
    )
    return transition


def _state_for_action(context: _SimulationContext, action: SimulationAction) -> ObjectStateRecord | None:
    if not action.object_id:
        return None
    return context.states.get(_state_key(action.object_id, action.instance_id))


def _required_payload_mapping(action: SimulationAction, field_name: str) -> dict[str, Any]:
    value = action.payload.get(field_name)
    if not isinstance(value, dict):
        raise SimulationRuntimeError(
            f"simulation.{field_name}_payload_required",
            f"{action.action_kind} requires payload.{field_name} as an object",
            details={"action_id": action.action_id, "payload": action.payload},
        )
    return dict(canonical_value(value))


def _action_status_from_receipt(receipt_status: str) -> ActionResultStatus:
    if receipt_status == "accepted":
        return "succeeded"
    if receipt_status == "no_op":
        return "no_op"
    return "terminal_error"


def _command_id(action: SimulationAction, pending: _PendingAction) -> str:
    if pending.source == "scenario":
        return action.action_id
    return f"{action.action_id}.automation_firing_{pending.firing_ordinal or 0}"


def _blocker_stop_reason(blockers: list[PromotionBlocker]) -> SimulationStopReason:
    codes = {blocker.code for blocker in blockers}
    if any(code.startswith("simulation.automation_loop_guard") or code == "simulation.max_actions_exceeded" for code in codes):
        return "guardrail_exceeded"
    if "simulation.unsupported_action" in codes:
        return "unsupported_capability"
    if "simulation.verifier_required" in codes or "simulation.unsupported_verifier" in codes:
        return "verifier_failed"
    return "runtime_fault"


def _dedupe_gaps(gaps: list[SimulationTypedGap]) -> tuple[SimulationTypedGap, ...]:
    seen: set[str] = set()
    deduped: list[SimulationTypedGap] = []
    for gap in gaps:
        if gap.gap_id in seen:
            continue
        seen.add(gap.gap_id)
        deduped.append(gap)
    return tuple(deduped)


def _dedupe_blockers(blockers: list[PromotionBlocker]) -> tuple[PromotionBlocker, ...]:
    seen: set[str] = set()
    deduped: list[PromotionBlocker] = []
    for blocker in blockers:
        if blocker.blocker_id in seen:
            continue
        seen.add(blocker.blocker_id)
        deduped.append(blocker)
    return tuple(deduped)


def _run_id_for_scenario(scenario: SimulationScenario) -> str:
    return _stable_ref(
        "virtual_lab_simulation_run",
        {
            "scenario_id": scenario.scenario_id,
            "scenario_digest": scenario.scenario_digest,
            "seed": scenario.config.seed,
        },
    )


def _stable_ref(prefix: str, value: dict[str, Any]) -> str:
    digest = canonical_digest(value, purpose=f"{prefix}.v1")
    return f"{prefix}.{digest[:20]}"


def _state_key(object_id: str | None, instance_id: str) -> str:
    return f"{_required_text(object_id, 'object_id')}#{_required_text(instance_id, 'instance_id')}"


def _read_path(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SimulationRuntimeError(
            f"simulation.{field_name}_required",
            f"{field_name} is required",
        )
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SimulationRuntimeError(
            f"simulation.{field_name}_not_object",
            f"{field_name} must be a JSON object",
            details={"value_type": type(value).__name__},
        )
    return dict(canonical_value(value))


def _payload_dict(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return _mapping(value, field_name)


def _list_of_dicts(value: Any, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise SimulationRuntimeError(
            f"simulation.{field_name}_not_list",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def _clean_path(value: tuple[str, ...] | list[str] | str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = value.split(".")
    else:
        parts = list(value)
    return tuple(str(part).strip() for part in parts if str(part).strip())


def _severity(value: str) -> SimulationSeverity:
    text = _required_text(value, "severity")
    if text not in {"info", "warning", "error", "blocker"}:
        raise SimulationRuntimeError(
            "simulation.invalid_severity",
            "severity is not supported",
            details={"severity": text},
        )
    return text  # type: ignore[return-value]


def _normalize_datetime(value: Any, field_name: str) -> str:
    return _datetime_to_utc(_parse_datetime(_required_text(value, field_name)))


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SimulationRuntimeError(
            "simulation.invalid_datetime",
            "datetime value must be ISO-8601",
            details={"value": value},
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _datetime_to_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "ActionExecutionResult",
    "AutomationEvaluationResult",
    "AutomationFiringResult",
    "AutomationPredicate",
    "AutomationRule",
    "PromotionBlocker",
    "SIMULATION_RUNTIME_VERSION",
    "SIMULATION_SCHEMA_VERSION",
    "SimulationAction",
    "SimulationAssertion",
    "SimulationConfig",
    "SimulationEvent",
    "SimulationInitialState",
    "SimulationRunResult",
    "SimulationRuntimeError",
    "SimulationScenario",
    "SimulationTrace",
    "SimulationTypedGap",
    "SimulationVerifier",
    "StateTransition",
    "automation_predicate_from_dict",
    "automation_rule_from_dict",
    "run_simulation_scenario",
    "simulation_action_from_dict",
    "simulation_assertion_from_dict",
    "simulation_config_from_dict",
    "simulation_initial_state_from_dict",
    "simulation_scenario_from_dict",
    "simulation_verifier_from_dict",
]
