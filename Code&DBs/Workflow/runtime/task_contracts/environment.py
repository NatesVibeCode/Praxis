"""Task environment contract primitives.

This module is deliberately pure domain code. It does not persist contracts,
launch work, or call the operation catalog. It answers one question before a
task can execute: does this frozen task environment contract still bind to one
active hierarchy path, one accountability chain, explicit policy boundaries,
and a non-stale revision?
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


NODE_STATUS_ACTIVE = "active"
NODE_STATUS_DEPRECATED = "deprecated"
NODE_STATUS_DRAFT = "draft"
NODE_STATUS_RETIRED = "retired"

CONTRACT_STATUS_ACTIVE = "active"
CONTRACT_STATUS_DRAFT = "draft"
CONTRACT_STATUS_EXPIRED = "expired"
CONTRACT_STATUS_REVOKED = "revoked"
CONTRACT_STATUS_SUPERSEDED = "superseded"

SOP_STATUS_ACTIVE = "active"
SOP_STATUS_DEPRECATED = "deprecated"
SOP_STATUS_RETIRED = "retired"

STALENESS_FRESH = "fresh"
STALENESS_STALE = "stale"

_WRITE_ACCESS_MODES = {"append", "update", "write"}
_HIGH_RISK_TOOL_CLASSES = (
    "admin_override",
    "code_execution",
    "deployment",
    "structured_data_write",
)
_INDEPENDENT_REVIEW_VALUES = {
    "business_signoff",
    "external",
    "independent",
    "independent_reviewer",
}
_TRIVIAL_VERIFIER_TYPES = {"existence_check", "presence_check", "none"}
_CLASSIFICATION_RANK = {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "restricted": 3,
    "regulated": 4,
}


def _clean_ref(value: object) -> str:
    return str(value or "").strip()


def _clean_tuple(values: tuple[object, ...] | list[object] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    return tuple(str(value).strip() for value in values if str(value).strip())


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _effective_at(
    effective_from: datetime,
    effective_to: datetime | None,
    as_of: datetime,
) -> bool:
    start = _as_utc(effective_from)
    end = _as_utc(effective_to) if effective_to is not None else None
    current = _as_utc(as_of)
    return start <= current and (end is None or current < end)


def _dimension_contains(container: str, child: str) -> bool:
    container = container.strip()
    child = child.strip()
    return container in {"*", "any"} or container == child


def _classification_contains(container: str, child: str) -> bool:
    container_key = container.strip().lower()
    child_key = child.strip().lower()
    if container_key in {"*", "any"}:
        return True
    if container_key in _CLASSIFICATION_RANK and child_key in _CLASSIFICATION_RANK:
        return _CLASSIFICATION_RANK[container_key] >= _CLASSIFICATION_RANK[child_key]
    return container_key == child_key


def _locator_contains(container: str, child: str) -> bool:
    container = container.strip()
    child = child.strip()
    if container in {"*", "any"}:
        return True
    if container.endswith("/*"):
        container = container[:-2]
    container = container.rstrip("/")
    child = child.rstrip("/")
    return child == container or child.startswith(f"{container}/")


def _is_cross_tenant(value: str) -> bool:
    value = value.strip()
    return value in {"*", "any"} or "," in value


@dataclass(frozen=True, slots=True)
class ContractInvalidState:
    """A typed invalid state suitable for receipts, gaps, and UI display."""

    reason_code: str
    message: str
    severity: str = "error"
    field_ref: str | None = None
    evidence_refs: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "reason_code", _clean_ref(self.reason_code))
        object.__setattr__(self, "message", _clean_ref(self.message))
        object.__setattr__(self, "severity", _clean_ref(self.severity) or "error")
        object.__setattr__(self, "evidence_refs", _clean_tuple(list(self.evidence_refs)))
        object.__setattr__(self, "details", dict(self.details or {}))

    def to_json(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code,
            "message": self.message,
            "severity": self.severity,
            "field_ref": self.field_ref,
            "evidence_refs": list(self.evidence_refs),
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason_code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class HierarchyNode:
    node_id: str
    node_type: str
    node_name: str
    parent_node_id: str | None
    status: str
    effective_from: datetime
    owner_ref: str | None
    steward_ref: str | None
    default_sop_refs: tuple[str, ...]
    default_tool_policy_ref: str | None
    default_scope_policy_ref: str | None
    default_model_policy_ref: str | None
    default_verifier_refs: tuple[str, ...]
    revision_id: str
    effective_to: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _clean_ref(self.node_id))
        object.__setattr__(self, "node_type", _clean_ref(self.node_type))
        object.__setattr__(self, "node_name", _clean_ref(self.node_name))
        object.__setattr__(self, "parent_node_id", _clean_ref(self.parent_node_id) or None)
        object.__setattr__(self, "status", _clean_ref(self.status))
        object.__setattr__(self, "owner_ref", _clean_ref(self.owner_ref) or None)
        object.__setattr__(self, "steward_ref", _clean_ref(self.steward_ref) or None)
        object.__setattr__(self, "default_sop_refs", _clean_tuple(list(self.default_sop_refs)))
        object.__setattr__(self, "default_verifier_refs", _clean_tuple(list(self.default_verifier_refs)))
        object.__setattr__(self, "revision_id", _clean_ref(self.revision_id))

    def is_active_at(self, as_of: datetime) -> bool:
        return self.status == NODE_STATUS_ACTIVE and _effective_at(
            self.effective_from,
            self.effective_to,
            as_of,
        )

    def is_effective_at(self, as_of: datetime) -> bool:
        return _effective_at(self.effective_from, self.effective_to, as_of)

    def to_json(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "node_name": self.node_name,
            "parent_node_id": self.parent_node_id,
            "status": self.status,
            "effective_from": _as_utc(self.effective_from).isoformat(),
            "effective_to": None if self.effective_to is None else _as_utc(self.effective_to).isoformat(),
            "owner_ref": self.owner_ref,
            "steward_ref": self.steward_ref,
            "default_sop_refs": list(self.default_sop_refs),
            "default_tool_policy_ref": self.default_tool_policy_ref,
            "default_scope_policy_ref": self.default_scope_policy_ref,
            "default_model_policy_ref": self.default_model_policy_ref,
            "default_verifier_refs": list(self.default_verifier_refs),
            "revision_id": self.revision_id,
        }


@dataclass(frozen=True, slots=True)
class HierarchyPath:
    leaf_node_id: str
    node_ids: tuple[str, ...]
    nodes: tuple[HierarchyNode, ...]

    @property
    def canonical_path(self) -> str:
        return "/".join(self.node_ids)

    def to_json(self) -> dict[str, Any]:
        return {
            "leaf_node_id": self.leaf_node_id,
            "node_ids": list(self.node_ids),
            "canonical_path": self.canonical_path,
            "nodes": [node.to_json() for node in self.nodes],
        }


@dataclass(frozen=True, slots=True)
class ResponsibilityPolicy:
    allow_missing_steward_with_exception: bool = False
    exception_ref: str | None = None
    exception_expires_at: datetime | None = None

    def exception_active_at(self, as_of: datetime) -> bool:
        if not self.allow_missing_steward_with_exception or not self.exception_ref:
            return False
        if self.exception_expires_at is None:
            return True
        return _as_utc(as_of) < _as_utc(self.exception_expires_at)


@dataclass(frozen=True, slots=True)
class ResolvedResponsibility:
    owner_ref: str | None
    steward_ref: str | None
    owner_source_node_id: str | None
    steward_source_node_id: str | None
    exception_ref: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "owner_ref": self.owner_ref,
            "steward_ref": self.steward_ref,
            "owner_source_node_id": self.owner_source_node_id,
            "steward_source_node_id": self.steward_source_node_id,
            "exception_ref": self.exception_ref,
        }


@dataclass(frozen=True, slots=True)
class SopReference:
    sop_ref: str
    sop_title: str
    sop_version: str
    sop_status: str
    sop_owner_ref: str
    effective_from: datetime
    source_uri: str | None = None
    effective_to: datetime | None = None
    exception_policy_ref: str | None = None
    primary: bool = False

    def active_at(self, as_of: datetime) -> bool:
        return self.sop_status == SOP_STATUS_ACTIVE and _effective_at(
            self.effective_from,
            self.effective_to,
            as_of,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "sop_ref": self.sop_ref,
            "sop_title": self.sop_title,
            "sop_version": self.sop_version,
            "sop_status": self.sop_status,
            "sop_owner_ref": self.sop_owner_ref,
            "effective_from": _as_utc(self.effective_from).isoformat(),
            "effective_to": None if self.effective_to is None else _as_utc(self.effective_to).isoformat(),
            "source_uri": self.source_uri,
            "exception_policy_ref": self.exception_policy_ref,
            "primary": self.primary,
        }


@dataclass(frozen=True, slots=True)
class SopGap:
    gap_ref: str
    owner_approval_ref: str
    review_expires_at: datetime
    reason_code: str = "sop.gap"

    def active_at(self, as_of: datetime) -> bool:
        return bool(self.gap_ref and self.owner_approval_ref) and _as_utc(as_of) < _as_utc(
            self.review_expires_at
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "gap_ref": self.gap_ref,
            "owner_approval_ref": self.owner_approval_ref,
            "review_expires_at": _as_utc(self.review_expires_at).isoformat(),
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True, slots=True)
class AllowedTool:
    tool_ref: str
    tool_name: str
    tool_class: str
    capabilities: tuple[str, ...]
    data_domains: tuple[str, ...]
    approval_level: str
    logging_requirements: tuple[str, ...]
    allowed_operations: tuple[str, ...]
    prohibited_operations: tuple[str, ...] = ()
    approval_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_ref", _clean_ref(self.tool_ref))
        object.__setattr__(self, "tool_class", _clean_ref(self.tool_class))
        object.__setattr__(self, "capabilities", _clean_tuple(list(self.capabilities)))
        object.__setattr__(self, "data_domains", _clean_tuple(list(self.data_domains)))
        object.__setattr__(self, "logging_requirements", _clean_tuple(list(self.logging_requirements)))
        object.__setattr__(self, "allowed_operations", _clean_tuple(list(self.allowed_operations)))
        object.__setattr__(self, "prohibited_operations", _clean_tuple(list(self.prohibited_operations)))
        object.__setattr__(self, "approval_ref", _clean_ref(self.approval_ref) or None)

    def allows_operation(self, operation_ref: str) -> PolicyDecision:
        operation_ref = _clean_ref(operation_ref)
        if operation_ref in self.prohibited_operations:
            return PolicyDecision(
                allowed=False,
                reason_code="task_contract.tool_operation_prohibited",
                message="operation is explicitly prohibited for tool",
                details={"tool_ref": self.tool_ref, "operation_ref": operation_ref},
            )
        if operation_ref not in self.allowed_operations:
            return PolicyDecision(
                allowed=False,
                reason_code="task_contract.tool_operation_unlisted",
                message="operation is not in the tool allow-list",
                details={"tool_ref": self.tool_ref, "operation_ref": operation_ref},
            )
        return PolicyDecision(
            allowed=True,
            reason_code="task_contract.tool_operation_allowed",
            message="operation is allowed by contract",
            details={"tool_ref": self.tool_ref, "operation_ref": operation_ref},
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "tool_ref": self.tool_ref,
            "tool_name": self.tool_name,
            "tool_class": self.tool_class,
            "capabilities": list(self.capabilities),
            "data_domains": list(self.data_domains),
            "approval_level": self.approval_level,
            "logging_requirements": list(self.logging_requirements),
            "allowed_operations": list(self.allowed_operations),
            "prohibited_operations": list(self.prohibited_operations),
            "approval_ref": self.approval_ref,
        }


@dataclass(frozen=True, slots=True)
class ScopeGrant:
    scope_ref: str
    resource_type: str
    resource_locator: str
    access_mode: str
    environment: str
    classification: str
    tenant_boundary: str
    change_constraints: tuple[str, ...] = ()
    rollback_requirement: str | None = None
    append_only: bool = False
    exception_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope_ref", _clean_ref(self.scope_ref))
        object.__setattr__(self, "resource_type", _clean_ref(self.resource_type))
        object.__setattr__(self, "resource_locator", _clean_ref(self.resource_locator))
        object.__setattr__(self, "access_mode", _clean_ref(self.access_mode))
        object.__setattr__(self, "environment", _clean_ref(self.environment))
        object.__setattr__(self, "classification", _clean_ref(self.classification))
        object.__setattr__(self, "tenant_boundary", _clean_ref(self.tenant_boundary))
        object.__setattr__(self, "change_constraints", _clean_tuple(list(self.change_constraints)))
        object.__setattr__(self, "exception_ref", _clean_ref(self.exception_ref) or None)

    def contains(self, other: "ScopeGrant") -> bool:
        return (
            _dimension_contains(self.resource_type, other.resource_type)
            and _dimension_contains(self.environment, other.environment)
            and _dimension_contains(self.tenant_boundary, other.tenant_boundary)
            and _classification_contains(self.classification, other.classification)
            and _locator_contains(self.resource_locator, other.resource_locator)
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "scope_ref": self.scope_ref,
            "resource_type": self.resource_type,
            "resource_locator": self.resource_locator,
            "access_mode": self.access_mode,
            "environment": self.environment,
            "classification": self.classification,
            "tenant_boundary": self.tenant_boundary,
            "change_constraints": list(self.change_constraints),
            "rollback_requirement": self.rollback_requirement,
            "append_only": self.append_only,
            "exception_ref": self.exception_ref,
        }


@dataclass(frozen=True, slots=True)
class ModelPolicy:
    model_policy_ref: str
    approved_model_classes: tuple[str, ...]
    approved_model_ids: tuple[str, ...]
    reasoning_limit: str
    tool_use_limit: str
    data_handling_constraints: tuple[str, ...]
    retention_constraints: tuple[str, ...]
    human_review_requirement: bool
    disallowed_use_cases: tuple[str, ...]
    status: str = "active"
    approved_aliases: tuple[str, ...] = ()
    permitted_input_classifications: tuple[str, ...] = ()
    permitted_output_classifications: tuple[str, ...] = ()
    approved_for_high_impact: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_policy_ref", _clean_ref(self.model_policy_ref))
        object.__setattr__(self, "approved_model_classes", _clean_tuple(list(self.approved_model_classes)))
        object.__setattr__(self, "approved_model_ids", _clean_tuple(list(self.approved_model_ids)))
        object.__setattr__(self, "approved_aliases", _clean_tuple(list(self.approved_aliases)))
        object.__setattr__(
            self,
            "permitted_input_classifications",
            _clean_tuple(list(self.permitted_input_classifications)),
        )
        object.__setattr__(
            self,
            "permitted_output_classifications",
            _clean_tuple(list(self.permitted_output_classifications)),
        )

    def allows_model(self, model_ref: str | None) -> bool:
        if not model_ref:
            return True
        return model_ref in set(self.approved_model_ids) | set(self.approved_aliases)

    def permits_classification(self, classification: str, *, output: bool = False) -> bool:
        permitted = (
            self.permitted_output_classifications
            if output
            else self.permitted_input_classifications
        )
        if not permitted:
            return True
        return any(_classification_contains(item, classification) for item in permitted)

    def to_json(self) -> dict[str, Any]:
        return {
            "model_policy_ref": self.model_policy_ref,
            "approved_model_classes": list(self.approved_model_classes),
            "approved_model_ids": list(self.approved_model_ids),
            "reasoning_limit": self.reasoning_limit,
            "tool_use_limit": self.tool_use_limit,
            "data_handling_constraints": list(self.data_handling_constraints),
            "retention_constraints": list(self.retention_constraints),
            "human_review_requirement": self.human_review_requirement,
            "disallowed_use_cases": list(self.disallowed_use_cases),
            "status": self.status,
            "approved_aliases": list(self.approved_aliases),
            "permitted_input_classifications": list(self.permitted_input_classifications),
            "permitted_output_classifications": list(self.permitted_output_classifications),
            "approved_for_high_impact": self.approved_for_high_impact,
        }


@dataclass(frozen=True, slots=True)
class VerifierReference:
    verifier_ref: str
    verifier_type: str
    applicability_rule: str
    pass_criteria: str
    failure_severity: str
    independence_requirement: str
    evidence_output_ref: str

    @property
    def is_independent(self) -> bool:
        return self.independence_requirement in _INDEPENDENT_REVIEW_VALUES

    @property
    def is_nontrivial(self) -> bool:
        return self.verifier_type not in _TRIVIAL_VERIFIER_TYPES

    def to_json(self) -> dict[str, Any]:
        return {
            "verifier_ref": self.verifier_ref,
            "verifier_type": self.verifier_type,
            "applicability_rule": self.applicability_rule,
            "pass_criteria": self.pass_criteria,
            "failure_severity": self.failure_severity,
            "independence_requirement": self.independence_requirement,
            "evidence_output_ref": self.evidence_output_ref,
        }


@dataclass(frozen=True, slots=True)
class StalenessPolicy:
    staleness_policy_ref: str
    review_interval_days: int
    trigger_types: tuple[str, ...]
    block_on_stale: bool
    grace_period_days: int
    revalidation_requirements: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "staleness_policy_ref", _clean_ref(self.staleness_policy_ref))
        object.__setattr__(self, "trigger_types", _clean_tuple(list(self.trigger_types)))
        object.__setattr__(
            self,
            "revalidation_requirements",
            _clean_tuple(list(self.revalidation_requirements)),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "staleness_policy_ref": self.staleness_policy_ref,
            "review_interval_days": self.review_interval_days,
            "trigger_types": list(self.trigger_types),
            "block_on_stale": self.block_on_stale,
            "grace_period_days": self.grace_period_days,
            "revalidation_requirements": list(self.revalidation_requirements),
        }


@dataclass(frozen=True, slots=True)
class StalenessSignal:
    trigger_type: str
    source_ref: str
    observed_at: datetime
    detail: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "trigger_type": self.trigger_type,
            "source_ref": self.source_ref,
            "observed_at": _as_utc(self.observed_at).isoformat(),
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class StalenessDecision:
    status: str
    blocks_execution: bool
    reason_codes: tuple[str, ...]
    stale_since: datetime | None
    triggering_signals: tuple[StalenessSignal, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "blocks_execution": self.blocks_execution,
            "reason_codes": list(self.reason_codes),
            "stale_since": None if self.stale_since is None else _as_utc(self.stale_since).isoformat(),
            "triggering_signals": [signal.to_json() for signal in self.triggering_signals],
        }


@dataclass(frozen=True, slots=True)
class RevisionRecord:
    revision_id: str
    entity_type: str
    entity_id: str
    prior_revision_id: str | None
    change_summary: str
    changed_by: str
    changed_at: datetime
    change_reason: str
    supersedes_effective_from: datetime
    approval_ref: str | None = None
    payload_hash: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "revision_id": self.revision_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "prior_revision_id": self.prior_revision_id,
            "change_summary": self.change_summary,
            "changed_by": self.changed_by,
            "changed_at": _as_utc(self.changed_at).isoformat(),
            "change_reason": self.change_reason,
            "approval_ref": self.approval_ref,
            "supersedes_effective_from": _as_utc(self.supersedes_effective_from).isoformat(),
            "payload_hash": self.payload_hash,
        }


@dataclass(frozen=True, slots=True)
class RevisionCheck:
    ok: bool
    head_revision_id: str | None
    invalid_states: tuple[ContractInvalidState, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "head_revision_id": self.head_revision_id,
            "invalid_states": [state.to_json() for state in self.invalid_states],
        }


@dataclass(frozen=True, slots=True)
class ContractPolicyBounds:
    """Inherited parent policy limits that a child contract may only narrow."""

    allowed_tool_refs: tuple[str, ...] = ()
    read_scope: tuple[ScopeGrant, ...] = ()
    write_scope: tuple[ScopeGrant, ...] = ()
    model_policy_refs: tuple[str, ...] = ()
    verifier_refs: tuple[str, ...] = ()
    exception_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_tool_refs", _clean_tuple(list(self.allowed_tool_refs)))
        object.__setattr__(self, "model_policy_refs", _clean_tuple(list(self.model_policy_refs)))
        object.__setattr__(self, "verifier_refs", _clean_tuple(list(self.verifier_refs)))
        object.__setattr__(self, "exception_refs", _clean_tuple(list(self.exception_refs)))


@dataclass(frozen=True, slots=True)
class TaskEnvironmentContract:
    contract_id: str
    task_ref: str
    hierarchy_node_id: str
    owner_ref: str | None
    steward_ref: str | None
    sop_refs: tuple[SopReference, ...]
    allowed_tools: tuple[AllowedTool, ...]
    read_scope: tuple[ScopeGrant, ...]
    write_scope: tuple[ScopeGrant, ...]
    model_policy: ModelPolicy | None
    verifier_refs: tuple[VerifierReference, ...]
    input_classification: str
    output_classification: str
    data_retention_ref: str
    staleness_policy: StalenessPolicy | None
    revision_id: str
    status: str
    effective_from: datetime
    effective_to: datetime | None = None
    revision_no: int = 1
    parent_revision_id: str | None = None
    sop_gap: SopGap | None = None
    requested_model_ref: str | None = None
    risk_level: str = "normal"
    contract_hash: str | None = None
    dependency_hash: str | None = None
    allowed_task_types: tuple[str, ...] = ()
    object_truth_contract_refs: tuple[str, ...] = ()
    created_from: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_id", _clean_ref(self.contract_id))
        object.__setattr__(self, "task_ref", _clean_ref(self.task_ref))
        object.__setattr__(self, "hierarchy_node_id", _clean_ref(self.hierarchy_node_id))
        object.__setattr__(self, "owner_ref", _clean_ref(self.owner_ref) or None)
        object.__setattr__(self, "steward_ref", _clean_ref(self.steward_ref) or None)
        object.__setattr__(self, "input_classification", _clean_ref(self.input_classification))
        object.__setattr__(self, "output_classification", _clean_ref(self.output_classification))
        object.__setattr__(self, "data_retention_ref", _clean_ref(self.data_retention_ref))
        object.__setattr__(self, "revision_id", _clean_ref(self.revision_id))
        object.__setattr__(self, "status", _clean_ref(self.status))
        object.__setattr__(self, "parent_revision_id", _clean_ref(self.parent_revision_id) or None)
        object.__setattr__(self, "requested_model_ref", _clean_ref(self.requested_model_ref) or None)
        object.__setattr__(self, "risk_level", _clean_ref(self.risk_level) or "normal")
        object.__setattr__(self, "allowed_task_types", _clean_tuple(list(self.allowed_task_types)))
        object.__setattr__(
            self,
            "object_truth_contract_refs",
            _clean_tuple(list(self.object_truth_contract_refs)),
        )
        object.__setattr__(self, "created_from", dict(self.created_from or {}))

    def effective_at(self, as_of: datetime) -> bool:
        return _effective_at(self.effective_from, self.effective_to, as_of)

    def find_tool(self, tool_ref: str) -> AllowedTool | None:
        tool_ref = _clean_ref(tool_ref)
        for tool in self.allowed_tools:
            if tool.tool_ref == tool_ref:
                return tool
        return None

    def is_tool_operation_allowed(self, tool_ref: str, operation_ref: str) -> PolicyDecision:
        tool = self.find_tool(tool_ref)
        if tool is None:
            return PolicyDecision(
                allowed=False,
                reason_code="task_contract.tool_unlisted",
                message="tool is denied by default because it is not listed",
                details={"tool_ref": tool_ref, "operation_ref": operation_ref},
            )
        return tool.allows_operation(operation_ref)

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "task_environment_contract",
            "schema_version": 1,
            "contract_id": self.contract_id,
            "task_ref": self.task_ref,
            "hierarchy_node_id": self.hierarchy_node_id,
            "owner_ref": self.owner_ref,
            "steward_ref": self.steward_ref,
            "sop_refs": [sop.to_json() for sop in self.sop_refs],
            "sop_gap": None if self.sop_gap is None else self.sop_gap.to_json(),
            "allowed_tools": [tool.to_json() for tool in self.allowed_tools],
            "read_scope": [scope.to_json() for scope in self.read_scope],
            "write_scope": [scope.to_json() for scope in self.write_scope],
            "model_policy": None if self.model_policy is None else self.model_policy.to_json(),
            "verifier_refs": [verifier.to_json() for verifier in self.verifier_refs],
            "input_classification": self.input_classification,
            "output_classification": self.output_classification,
            "data_retention_ref": self.data_retention_ref,
            "staleness_policy": (
                None if self.staleness_policy is None else self.staleness_policy.to_json()
            ),
            "revision_id": self.revision_id,
            "revision_no": self.revision_no,
            "parent_revision_id": self.parent_revision_id,
            "status": self.status,
            "effective_from": _as_utc(self.effective_from).isoformat(),
            "effective_to": None if self.effective_to is None else _as_utc(self.effective_to).isoformat(),
            "requested_model_ref": self.requested_model_ref,
            "risk_level": self.risk_level,
            "contract_hash": self.contract_hash,
            "dependency_hash": self.dependency_hash,
            "allowed_task_types": list(self.allowed_task_types),
            "object_truth_contract_refs": list(self.object_truth_contract_refs),
            "created_from": dict(self.created_from),
        }


@dataclass(frozen=True, slots=True)
class ContractEvaluationContext:
    hierarchy_nodes: tuple[HierarchyNode, ...]
    as_of: datetime
    staleness_signals: tuple[StalenessSignal, ...] = ()
    policy_bounds: ContractPolicyBounds | None = None
    responsibility_policy: ResponsibilityPolicy | None = None
    required_tooling: bool = True
    high_risk_tool_classes: tuple[str, ...] = _HIGH_RISK_TOOL_CLASSES
    revision_chain: tuple[RevisionRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class ContractEvaluationResult:
    ok: bool
    status: str
    invalid_states: tuple[ContractInvalidState, ...]
    warnings: tuple[ContractInvalidState, ...]
    hierarchy_path: HierarchyPath | None
    responsibility: ResolvedResponsibility | None
    staleness_decision: StalenessDecision
    revision_check: RevisionCheck | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "invalid_states": [state.to_json() for state in self.invalid_states],
            "warnings": [state.to_json() for state in self.warnings],
            "hierarchy_path": None if self.hierarchy_path is None else self.hierarchy_path.to_json(),
            "responsibility": None if self.responsibility is None else self.responsibility.to_json(),
            "staleness_decision": self.staleness_decision.to_json(),
            "revision_check": None if self.revision_check is None else self.revision_check.to_json(),
        }


def resolve_active_hierarchy_path(
    nodes: tuple[HierarchyNode, ...] | list[HierarchyNode],
    *,
    leaf_node_id: str,
    as_of: datetime,
) -> tuple[HierarchyPath | None, tuple[ContractInvalidState, ...]]:
    """Resolve exactly one active root-to-leaf hierarchy path."""

    leaf_node_id = _clean_ref(leaf_node_id)
    as_of = _as_utc(as_of)
    invalid: list[ContractInvalidState] = []
    effective_by_id: dict[str, list[HierarchyNode]] = {}
    active_by_id: dict[str, list[HierarchyNode]] = {}

    for node in nodes:
        if not node.is_effective_at(as_of):
            continue
        effective_by_id.setdefault(node.node_id, []).append(node)
        if node.status == NODE_STATUS_ACTIVE:
            active_by_id.setdefault(node.node_id, []).append(node)

    for node_id, active_nodes in active_by_id.items():
        if len(active_nodes) > 1:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.hierarchy_ambiguous_active_revision",
                    message="hierarchy node has multiple active revisions",
                    field_ref=f"hierarchy.{node_id}",
                    details={"revision_ids": [node.revision_id for node in active_nodes]},
                )
            )

    leaf_candidates = active_by_id.get(leaf_node_id, [])
    if len(leaf_candidates) != 1:
        effective_leaf = effective_by_id.get(leaf_node_id, [])
        invalid.append(
            ContractInvalidState(
                reason_code=(
                    "task_contract.hierarchy_node_not_active"
                    if effective_leaf
                    else "task_contract.hierarchy_node_missing"
                ),
                message="task must resolve to exactly one active hierarchy node",
                field_ref="hierarchy_node_id",
                details={
                    "hierarchy_node_id": leaf_node_id,
                    "effective_statuses": [node.status for node in effective_leaf],
                },
            )
        )
        return None, tuple(invalid)

    node = leaf_candidates[0]
    reverse_path = [node]
    seen = {node.node_id}
    while node.parent_node_id:
        parent_candidates = active_by_id.get(node.parent_node_id, [])
        if len(parent_candidates) != 1:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.hierarchy_parent_not_active",
                    message="hierarchy path parent is missing or not uniquely active",
                    field_ref=f"hierarchy.{node.node_id}.parent_node_id",
                    details={
                        "node_id": node.node_id,
                        "parent_node_id": node.parent_node_id,
                        "active_parent_count": len(parent_candidates),
                    },
                )
            )
            return None, tuple(invalid)
        parent = parent_candidates[0]
        if parent.node_id in seen:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.hierarchy_cycle",
                    message="hierarchy path contains a cycle",
                    field_ref=f"hierarchy.{parent.node_id}",
                )
            )
            return None, tuple(invalid)
        seen.add(parent.node_id)
        reverse_path.append(parent)
        node = parent

    path_nodes = tuple(reversed(reverse_path))
    return (
        HierarchyPath(
            leaf_node_id=leaf_node_id,
            node_ids=tuple(node.node_id for node in path_nodes),
            nodes=path_nodes,
        ),
        tuple(invalid),
    )


def resolve_responsibility(
    path: HierarchyPath,
    contract: TaskEnvironmentContract,
    *,
    policy: ResponsibilityPolicy | None = None,
    as_of: datetime | None = None,
) -> tuple[ResolvedResponsibility, tuple[ContractInvalidState, ...], tuple[ContractInvalidState, ...]]:
    """Resolve owner/steward from the hierarchy and compare contract mirrors."""

    invalid: list[ContractInvalidState] = []
    warnings: list[ContractInvalidState] = []
    owner_source: HierarchyNode | None = None
    steward_source: HierarchyNode | None = None

    for node in path.nodes:
        if not node.owner_ref:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.hierarchy_owner_missing",
                    message="active hierarchy node is missing owner_ref",
                    field_ref=f"hierarchy.{node.node_id}.owner_ref",
                )
            )
        if not node.steward_ref:
            if policy and as_of and policy.exception_active_at(as_of):
                warnings.append(
                    ContractInvalidState(
                        reason_code="task_contract.hierarchy_steward_missing_exception",
                        message="active hierarchy node is missing steward_ref under temporary exception",
                        severity="warning",
                        field_ref=f"hierarchy.{node.node_id}.steward_ref",
                        evidence_refs=(policy.exception_ref or "",),
                    )
                )
            else:
                invalid.append(
                    ContractInvalidState(
                        reason_code="task_contract.hierarchy_steward_missing",
                        message="active hierarchy node is missing steward_ref",
                        field_ref=f"hierarchy.{node.node_id}.steward_ref",
                    )
                )
        if node.owner_ref:
            owner_source = node
        if node.steward_ref:
            steward_source = node

    resolved = ResolvedResponsibility(
        owner_ref=None if owner_source is None else owner_source.owner_ref,
        steward_ref=None if steward_source is None else steward_source.steward_ref,
        owner_source_node_id=None if owner_source is None else owner_source.node_id,
        steward_source_node_id=None if steward_source is None else steward_source.node_id,
        exception_ref=None if policy is None else policy.exception_ref,
    )

    if not contract.owner_ref:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_owner_missing",
                message="contract is missing resolved owner_ref",
                field_ref="owner_ref",
            )
        )
    elif resolved.owner_ref and contract.owner_ref != resolved.owner_ref:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_owner_mismatch",
                message="contract owner_ref does not match hierarchy resolution",
                field_ref="owner_ref",
                details={"contract_owner_ref": contract.owner_ref, "resolved_owner_ref": resolved.owner_ref},
            )
        )

    if not contract.steward_ref:
        if policy and as_of and policy.exception_active_at(as_of):
            warnings.append(
                ContractInvalidState(
                    reason_code="task_contract.contract_steward_missing_exception",
                    message="contract is missing steward_ref under temporary exception",
                    severity="warning",
                    field_ref="steward_ref",
                    evidence_refs=(policy.exception_ref or "",),
                )
            )
        else:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.contract_steward_missing",
                    message="contract is missing resolved steward_ref",
                    field_ref="steward_ref",
                )
            )
    elif resolved.steward_ref and contract.steward_ref != resolved.steward_ref:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_steward_mismatch",
                message="contract steward_ref does not match hierarchy resolution",
                field_ref="steward_ref",
                details={
                    "contract_steward_ref": contract.steward_ref,
                    "resolved_steward_ref": resolved.steward_ref,
                },
            )
        )

    return resolved, tuple(invalid), tuple(warnings)


def decide_contract_staleness(
    contract: TaskEnvironmentContract,
    *,
    as_of: datetime,
    signals: tuple[StalenessSignal, ...] | list[StalenessSignal] = (),
) -> StalenessDecision:
    policy = contract.staleness_policy
    if policy is None:
        return StalenessDecision(
            status=STALENESS_STALE,
            blocks_execution=True,
            reason_codes=("task_contract.staleness_policy_missing",),
            stale_since=_as_utc(contract.effective_from),
        )

    as_of = _as_utc(as_of)
    triggering: list[StalenessSignal] = []
    reason_codes: list[str] = []
    stale_since: datetime | None = None

    for signal in signals:
        if signal.trigger_type not in policy.trigger_types:
            continue
        if _as_utc(signal.observed_at) < _as_utc(contract.effective_from):
            continue
        triggering.append(signal)
        reason_codes.append(f"task_contract.stale.{signal.trigger_type}")
        stale_since = min(_as_utc(signal.observed_at), stale_since) if stale_since else _as_utc(signal.observed_at)

    review_due_at = _as_utc(contract.effective_from) + timedelta(days=policy.review_interval_days)
    if as_of > review_due_at:
        stale_since = min(review_due_at, stale_since) if stale_since else review_due_at
        reason_codes.append("task_contract.stale.review_interval_exceeded")

    if not reason_codes:
        return StalenessDecision(
            status=STALENESS_FRESH,
            blocks_execution=False,
            reason_codes=(),
            stale_since=None,
        )

    return StalenessDecision(
        status=STALENESS_STALE,
        blocks_execution=policy.block_on_stale,
        reason_codes=tuple(sorted(set(reason_codes))),
        stale_since=stale_since,
        triggering_signals=tuple(triggering),
    )


def validate_append_only_revision_chain(
    revisions: tuple[RevisionRecord, ...] | list[RevisionRecord],
) -> RevisionCheck:
    invalid: list[ContractInvalidState] = []
    if not revisions:
        return RevisionCheck(
            ok=False,
            head_revision_id=None,
            invalid_states=(
                ContractInvalidState(
                    reason_code="task_contract.revision_chain_empty",
                    message="revision chain must contain at least one record",
                    field_ref="revision_id",
                ),
            ),
        )

    by_id: dict[str, RevisionRecord] = {}
    referenced: set[str] = set()
    entity_keys = {(revision.entity_type, revision.entity_id) for revision in revisions}

    if len(entity_keys) != 1:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_chain_entity_mismatch",
                message="revision chain contains more than one entity",
                details={"entity_keys": sorted([f"{kind}:{ref}" for kind, ref in entity_keys])},
            )
        )

    for revision in revisions:
        if revision.revision_id in by_id:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.revision_duplicate_id",
                    message="revision_id appears more than once",
                    field_ref="revision_id",
                    details={"revision_id": revision.revision_id},
                )
            )
        by_id[revision.revision_id] = revision
        if revision.prior_revision_id:
            referenced.add(revision.prior_revision_id)

    roots = [revision for revision in revisions if not revision.prior_revision_id]
    if len(roots) != 1:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_chain_root_count_invalid",
                message="revision chain must have exactly one root",
                details={"root_count": len(roots)},
            )
        )

    for revision in revisions:
        if revision.prior_revision_id and revision.prior_revision_id not in by_id:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.revision_prior_missing",
                    message="revision must identify an existing predecessor",
                    field_ref="prior_revision_id",
                    details={
                        "revision_id": revision.revision_id,
                        "prior_revision_id": revision.prior_revision_id,
                    },
                )
            )

    heads = [revision for revision in revisions if revision.revision_id not in referenced]
    if len(heads) != 1:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_chain_branching",
                message="revision chain must have exactly one head",
                details={"head_count": len(heads)},
            )
        )

    head_revision_id = heads[0].revision_id if len(heads) == 1 else None
    if head_revision_id:
        seen: set[str] = set()
        cursor: RevisionRecord | None = by_id[head_revision_id]
        while cursor is not None:
            if cursor.revision_id in seen:
                invalid.append(
                    ContractInvalidState(
                        reason_code="task_contract.revision_chain_cycle",
                        message="revision chain contains a cycle",
                        details={"revision_id": cursor.revision_id},
                    )
                )
                break
            seen.add(cursor.revision_id)
            if cursor.prior_revision_id is None:
                cursor = None
            else:
                cursor = by_id.get(cursor.prior_revision_id)
        if len(seen) != len(revisions):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.revision_chain_disconnected",
                    message="revision chain contains disconnected records",
                    details={"visited_count": len(seen), "revision_count": len(revisions)},
                )
            )

    return RevisionCheck(
        ok=not invalid,
        head_revision_id=head_revision_id,
        invalid_states=tuple(invalid),
    )


def validate_next_revision(prior: RevisionRecord, candidate: RevisionRecord) -> RevisionCheck:
    invalid: list[ContractInvalidState] = []
    if prior.entity_type != candidate.entity_type or prior.entity_id != candidate.entity_id:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_entity_changed",
                message="new revision must belong to the same entity",
                details={
                    "prior_entity": f"{prior.entity_type}:{prior.entity_id}",
                    "candidate_entity": f"{candidate.entity_type}:{candidate.entity_id}",
                },
            )
        )
    if candidate.prior_revision_id != prior.revision_id:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_prior_mismatch",
                message="new revision must identify the exact predecessor",
                field_ref="prior_revision_id",
                details={
                    "expected_prior_revision_id": prior.revision_id,
                    "candidate_prior_revision_id": candidate.prior_revision_id,
                },
            )
        )
    if candidate.revision_id == prior.revision_id:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_id_reused",
                message="new revision cannot reuse prior revision_id",
                field_ref="revision_id",
            )
        )
    if _as_utc(candidate.changed_at) < _as_utc(prior.changed_at):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_time_regressed",
                message="new revision changed_at cannot be earlier than predecessor",
                field_ref="changed_at",
            )
        )
    return RevisionCheck(
        ok=not invalid,
        head_revision_id=candidate.revision_id,
        invalid_states=tuple(invalid),
    )


def validate_task_environment_contract(
    contract: TaskEnvironmentContract,
    context: ContractEvaluationContext,
) -> ContractEvaluationResult:
    as_of = _as_utc(context.as_of)
    invalid: list[ContractInvalidState] = []
    warnings: list[ContractInvalidState] = []

    if contract.status != CONTRACT_STATUS_ACTIVE:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_not_active",
                message="task may execute only against an active contract revision",
                field_ref="status",
                details={"status": contract.status},
            )
        )
    if not contract.effective_at(as_of):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_not_effective",
                message="contract revision is not effective at execution time",
                field_ref="effective_from",
            )
        )
    if contract.revision_no > 1 and not contract.parent_revision_id:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.revision_parent_missing",
                message="non-root contract revision must identify parent_revision_id",
                field_ref="parent_revision_id",
            )
        )

    hierarchy_path, path_invalid = resolve_active_hierarchy_path(
        context.hierarchy_nodes,
        leaf_node_id=contract.hierarchy_node_id,
        as_of=as_of,
    )
    invalid.extend(path_invalid)

    responsibility = None
    if hierarchy_path is not None:
        responsibility, responsibility_invalid, responsibility_warnings = resolve_responsibility(
            hierarchy_path,
            contract,
            policy=context.responsibility_policy,
            as_of=as_of,
        )
        invalid.extend(responsibility_invalid)
        warnings.extend(responsibility_warnings)

    invalid.extend(_validate_sop_policy(contract, as_of))
    invalid.extend(_validate_tool_policy(contract, context))
    invalid.extend(_validate_scope_policy(contract, context))
    invalid.extend(_validate_model_policy(contract, context))
    invalid.extend(_validate_verifier_policy(contract, context))

    staleness_decision = decide_contract_staleness(
        contract,
        as_of=as_of,
        signals=context.staleness_signals,
    )
    if staleness_decision.blocks_execution:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.contract_stale",
                message="contract is stale and policy blocks execution",
                field_ref="staleness_policy_ref",
                details={"staleness_reason_codes": list(staleness_decision.reason_codes)},
            )
        )
    elif staleness_decision.status == STALENESS_STALE:
        warnings.append(
            ContractInvalidState(
                reason_code="task_contract.contract_stale_nonblocking",
                message="contract is stale but policy does not block execution",
                severity="warning",
                field_ref="staleness_policy_ref",
                details={"staleness_reason_codes": list(staleness_decision.reason_codes)},
            )
        )

    revision_check = None
    if context.revision_chain:
        revision_check = validate_append_only_revision_chain(context.revision_chain)
        invalid.extend(revision_check.invalid_states)

    ok = not invalid
    status = "valid" if ok else "blocked"
    if ok and warnings:
        status = "valid_with_warnings"

    return ContractEvaluationResult(
        ok=ok,
        status=status,
        invalid_states=tuple(invalid),
        warnings=tuple(warnings),
        hierarchy_path=hierarchy_path,
        responsibility=responsibility,
        staleness_decision=staleness_decision,
        revision_check=revision_check,
    )


def _validate_sop_policy(
    contract: TaskEnvironmentContract,
    as_of: datetime,
) -> tuple[ContractInvalidState, ...]:
    invalid: list[ContractInvalidState] = []
    active_sops = [sop for sop in contract.sop_refs if sop.active_at(as_of)]
    primary_sops = [sop for sop in active_sops if sop.primary]

    if not contract.sop_refs and contract.sop_gap is None:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.sop_or_gap_missing",
                message="contract must reference an active SOP or explicit SOP gap",
                field_ref="sop_refs",
            )
        )

    for sop in contract.sop_refs:
        if sop.sop_status in {SOP_STATUS_DEPRECATED, SOP_STATUS_RETIRED}:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.sop_not_assignable",
                    message="deprecated or retired SOP cannot be assigned to new task contract",
                    field_ref=f"sop_refs.{sop.sop_ref}",
                    details={"sop_status": sop.sop_status},
                )
            )
        elif not sop.active_at(as_of):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.sop_not_effective",
                    message="SOP reference is not active at execution time",
                    field_ref=f"sop_refs.{sop.sop_ref}",
                )
            )

    if active_sops and len(primary_sops) != 1:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.sop_primary_count_invalid",
                message="active SOP references must designate exactly one primary SOP",
                field_ref="sop_refs",
                details={"primary_count": len(primary_sops)},
            )
        )

    if not active_sops:
        if contract.sop_gap is None:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.sop_active_missing",
                    message="contract has no active SOP and no explicit SOP gap",
                    field_ref="sop_refs",
                )
            )
        elif not contract.sop_gap.active_at(as_of):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.sop_gap_invalid",
                    message="SOP gap requires owner approval and unexpired review window",
                    field_ref="sop_gap",
                    details=contract.sop_gap.to_json(),
                )
            )

    return tuple(invalid)


def _validate_tool_policy(
    contract: TaskEnvironmentContract,
    context: ContractEvaluationContext,
) -> tuple[ContractInvalidState, ...]:
    invalid: list[ContractInvalidState] = []
    if context.required_tooling and not contract.allowed_tools:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.allowed_tools_missing",
                message="contract must enumerate allowed tools",
                field_ref="allowed_tools",
            )
        )

    seen_tools: set[str] = set()
    bounds = context.policy_bounds
    high_risk_classes = set(context.high_risk_tool_classes)
    for tool in contract.allowed_tools:
        if tool.tool_ref in seen_tools:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.allowed_tool_duplicate",
                    message="allowed tool appears more than once",
                    field_ref=f"allowed_tools.{tool.tool_ref}",
                )
            )
        seen_tools.add(tool.tool_ref)

        if bounds and bounds.allowed_tool_refs and tool.tool_ref not in bounds.allowed_tool_refs:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.tool_policy_broadened",
                    message="child contract may not add tools outside inherited policy bounds",
                    field_ref=f"allowed_tools.{tool.tool_ref}",
                    details={"allowed_tool_refs": list(bounds.allowed_tool_refs)},
                )
            )
        if tool.tool_class != "no_tool" and not tool.allowed_operations:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.tool_operations_missing",
                    message="allowed tool must enumerate allowed operations",
                    field_ref=f"allowed_tools.{tool.tool_ref}.allowed_operations",
                )
            )
        if set(tool.allowed_operations) & set(tool.prohibited_operations):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.tool_policy_conflict",
                    message="tool operation cannot be both allowed and prohibited",
                    field_ref=f"allowed_tools.{tool.tool_ref}",
                )
            )
        if tool.tool_class in high_risk_classes and not tool.approval_ref:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.high_risk_tool_approval_missing",
                    message="high-risk tool class requires explicit approval reference",
                    field_ref=f"allowed_tools.{tool.tool_ref}.approval_ref",
                    details={"tool_class": tool.tool_class},
                )
            )
    return tuple(invalid)


def _validate_scope_policy(
    contract: TaskEnvironmentContract,
    context: ContractEvaluationContext,
) -> tuple[ContractInvalidState, ...]:
    invalid: list[ContractInvalidState] = []
    if not contract.read_scope:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.read_scope_missing",
                message="contract must declare read scope explicitly",
                field_ref="read_scope",
            )
        )

    bounds = context.policy_bounds
    for scope in contract.read_scope:
        if scope.access_mode != "read":
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.read_scope_access_mode_invalid",
                    message="read scope access_mode must be read",
                    field_ref=f"read_scope.{scope.scope_ref}.access_mode",
                )
            )
        if _is_cross_tenant(scope.tenant_boundary) and not scope.exception_ref:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.cross_tenant_scope_denied",
                    message="cross-tenant scope requires explicit exception",
                    field_ref=f"read_scope.{scope.scope_ref}.tenant_boundary",
                )
            )
        if bounds and bounds.read_scope and not any(bound.contains(scope) for bound in bounds.read_scope):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.read_scope_policy_broadened",
                    message="read scope is outside inherited parent policy bounds",
                    field_ref=f"read_scope.{scope.scope_ref}",
                    details={"resource_locator": scope.resource_locator},
                )
            )

    for scope in contract.write_scope:
        if scope.access_mode not in _WRITE_ACCESS_MODES:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.write_scope_access_mode_invalid",
                    message="write scope access_mode must be append, update, or write",
                    field_ref=f"write_scope.{scope.scope_ref}.access_mode",
                )
            )
        if scope.append_only and scope.access_mode != "append":
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.append_only_write_mode_invalid",
                    message="append-only scope cannot use overwrite/update semantics",
                    field_ref=f"write_scope.{scope.scope_ref}.access_mode",
                )
            )
        if _is_cross_tenant(scope.tenant_boundary) and not scope.exception_ref:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.cross_tenant_scope_denied",
                    message="cross-tenant scope requires explicit exception",
                    field_ref=f"write_scope.{scope.scope_ref}.tenant_boundary",
                )
            )
        if not any(read_scope.contains(scope) for read_scope in contract.read_scope):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.write_scope_not_within_read_scope",
                    message="write scope must be narrower than or equal to read scope",
                    field_ref=f"write_scope.{scope.scope_ref}",
                    details={"resource_locator": scope.resource_locator},
                )
            )
        if bounds and bounds.write_scope and not any(bound.contains(scope) for bound in bounds.write_scope):
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.write_scope_policy_broadened",
                    message="write scope is outside inherited parent policy bounds",
                    field_ref=f"write_scope.{scope.scope_ref}",
                    details={"resource_locator": scope.resource_locator},
                )
            )
    return tuple(invalid)


def _validate_model_policy(
    contract: TaskEnvironmentContract,
    context: ContractEvaluationContext,
) -> tuple[ContractInvalidState, ...]:
    invalid: list[ContractInvalidState] = []
    model_policy = contract.model_policy
    if model_policy is None:
        return (
            ContractInvalidState(
                reason_code="task_contract.model_policy_missing",
                message="contract must bind to an approved model policy",
                field_ref="model_policy",
            ),
        )

    if model_policy.status != "active":
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.model_policy_not_active",
                message="model policy must be active",
                field_ref="model_policy.status",
                details={"status": model_policy.status},
            )
        )
    if context.policy_bounds and context.policy_bounds.model_policy_refs:
        if model_policy.model_policy_ref not in context.policy_bounds.model_policy_refs:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.model_policy_broadened",
                    message="contract model policy is outside inherited policy bounds",
                    field_ref="model_policy.model_policy_ref",
                    details={"model_policy_ref": model_policy.model_policy_ref},
                )
            )
    if not model_policy.allows_model(contract.requested_model_ref):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.model_not_allowed",
                message="requested model is outside model policy",
                field_ref="requested_model_ref",
                details={"requested_model_ref": contract.requested_model_ref},
            )
        )
    if not model_policy.permits_classification(contract.input_classification):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.model_input_classification_denied",
                message="model policy does not permit input classification",
                field_ref="input_classification",
                details={"input_classification": contract.input_classification},
            )
        )
    if not model_policy.permits_classification(contract.output_classification, output=True):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.model_output_classification_denied",
                message="model policy does not permit output classification",
                field_ref="output_classification",
                details={"output_classification": contract.output_classification},
            )
        )
    if contract.risk_level == "high" and not model_policy.human_review_requirement:
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.high_impact_human_review_missing",
                message="high-impact task requires model policy human review",
                field_ref="model_policy.human_review_requirement",
            )
        )
    return tuple(invalid)


def _validate_verifier_policy(
    contract: TaskEnvironmentContract,
    context: ContractEvaluationContext,
) -> tuple[ContractInvalidState, ...]:
    invalid: list[ContractInvalidState] = []
    if not contract.verifier_refs:
        return (
            ContractInvalidState(
                reason_code="task_contract.verifier_refs_missing",
                message="contract must include at least one verifier reference",
                field_ref="verifier_refs",
            ),
        )

    for verifier in contract.verifier_refs:
        if context.policy_bounds and context.policy_bounds.verifier_refs:
            if verifier.verifier_ref not in context.policy_bounds.verifier_refs:
                invalid.append(
                    ContractInvalidState(
                        reason_code="task_contract.verifier_policy_broadened",
                        message="contract verifier is outside inherited policy bounds",
                        field_ref=f"verifier_refs.{verifier.verifier_ref}",
                    )
                )
        if not verifier.evidence_output_ref:
            invalid.append(
                ContractInvalidState(
                    reason_code="task_contract.verifier_evidence_output_missing",
                    message="verifier must declare evidence output reference",
                    field_ref=f"verifier_refs.{verifier.verifier_ref}.evidence_output_ref",
                )
            )

    if contract.write_scope and not any(verifier.is_nontrivial for verifier in contract.verifier_refs):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.write_verifier_nontrivial_missing",
                message="write-enabled task needs a nontrivial verifier",
                field_ref="verifier_refs",
            )
        )
    if contract.risk_level == "high" and not any(verifier.is_independent for verifier in contract.verifier_refs):
        invalid.append(
            ContractInvalidState(
                reason_code="task_contract.independent_verifier_missing",
                message="high-risk task requires independent verifier coverage",
                field_ref="verifier_refs",
            )
        )
    return tuple(invalid)


def canonical_json(value: Any) -> str:
    """Serialize a contract payload with deterministic key ordering."""

    def _normalize(item: Any) -> Any:
        if isinstance(item, datetime):
            return _as_utc(item).isoformat()
        if hasattr(item, "to_json"):
            return _normalize(item.to_json())
        if is_dataclass(item):
            return _normalize(item.__dict__)
        if isinstance(item, dict):
            return {str(key): _normalize(item[key]) for key in sorted(item)}
        if isinstance(item, (list, tuple)):
            return [_normalize(child) for child in item]
        return item

    return json.dumps(_normalize(value), sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()

