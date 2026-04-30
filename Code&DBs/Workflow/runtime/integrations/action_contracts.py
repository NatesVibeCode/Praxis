"""Typed integration action and automation contract capture.

This module is deliberately capture-only. It does not execute integrations,
write registry rows, rotate credentials, or register CQRS operations. The
contract objects make action behavior explicit enough for Object Truth and
Virtual Lab phases to reason about side effects, replay, permissions, and
rollback before live automation is promoted.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, fields, is_dataclass, replace
from enum import Enum
import hashlib
import json
import re
from typing import Any


class ContractEnum(str, Enum):
    """String enum with stable JSON values."""

    def __str__(self) -> str:
        return self.value


class TriggerType(ContractEnum):
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT_DRIVEN = "event_driven"
    WEBHOOK_DRIVEN = "webhook_driven"
    WORKFLOW_STEP = "workflow_step"


class ContractStatus(ContractEnum):
    DRAFT = "draft"
    CAPTURED = "captured"
    OWNER_REVIEWED = "owner_reviewed"
    ACCEPTED = "accepted"
    SUPERSEDED = "superseded"


class ExecutionMode(ContractEnum):
    SYNC = "sync"
    ASYNC = "async"
    CALLBACK = "callback"
    UNKNOWN = "unknown"


class IdempotencyState(ContractEnum):
    FULLY_IDEMPOTENT = "fully_idempotent"
    CONDITIONALLY_IDEMPOTENT = "conditionally_idempotent"
    NON_IDEMPOTENT = "non_idempotent"
    UNKNOWN = "unknown"


class IdempotencyKeyOrigin(ContractEnum):
    CLIENT_GENERATED = "client_generated"
    WORKFLOW_RUN_GENERATED = "workflow_run_generated"
    PROVIDER_GENERATED = "provider_generated"
    RESOURCE_DERIVED = "resource_derived"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class DedupeScope(ContractEnum):
    PER_RESOURCE = "per_resource"
    PER_WORKFLOW_RUN = "per_workflow_run"
    PER_TENANT = "per_tenant"
    GLOBAL = "global"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class RetryPolicyKind(ContractEnum):
    NONE = "none"
    FIXED_BACKOFF = "fixed_backoff"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    PROVIDER_DEFINED = "provider_defined"
    MANUAL_ONLY = "manual_only"
    UNKNOWN = "unknown"


class SideEffectKind(ContractEnum):
    NONE = "none"
    RECORD_CREATE = "record_create"
    RECORD_UPDATE = "record_update"
    RECORD_DELETE = "record_delete"
    EXTERNAL_MUTATION = "external_mutation"
    EXTERNAL_HTTP_REQUEST = "external_http_request"
    WORKFLOW_DISPATCH = "workflow_dispatch"
    WORKFLOW_STATE_TRANSITION = "workflow_state_transition"
    NOTIFICATION_SEND = "notification_send"
    FILE_GENERATION = "file_generation"
    FILE_TRANSFER = "file_transfer"
    EVENT_EMIT = "event_emit"
    QUOTA_OR_BILLING = "quota_or_billing"
    DOWNSTREAM_AUTOMATION = "downstream_automation"
    UNKNOWN = "unknown"


class IdentityType(ContractEnum):
    USER = "user"
    SERVICE_ACCOUNT = "service_account"
    BOT = "bot"
    API_KEY = "api_key"
    OAUTH_CLIENT = "oauth_client"
    WEBHOOK_SECRET = "webhook_secret"
    SHARED_SYSTEM_IDENTITY = "shared_system_identity"
    UNKNOWN = "unknown"


class ExecutionIdentityMode(ContractEnum):
    CALLER_IDENTITY = "caller_identity"
    DELEGATED_IDENTITY = "delegated_identity"
    SHARED_SYSTEM_IDENTITY = "shared_system_identity"
    UNKNOWN = "unknown"


class EventDirection(ContractEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    INTERNAL = "internal"


class EventDeliverySemantics(ContractEnum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EFFECTIVELY_ONCE = "effectively_once"
    BEST_EFFORT = "best_effort"
    UNKNOWN = "unknown"


class RollbackClass(ContractEnum):
    REVERSIBLE = "reversible"
    COMPENSATABLE = "compensatable"
    FORWARD_FIX_ONLY = "forward_fix_only"
    MANUAL_ONLY = "manual_only"


class GapKind(ContractEnum):
    MISSING_INPUT_SCHEMA_TYPING = "missing_input_schema_typing"
    MISSING_OUTPUT_SCHEMA_TYPING = "missing_output_schema_typing"
    UNKNOWN_SIDE_EFFECTS = "unknown_side_effects"
    UNKNOWN_IDEMPOTENCY_BEHAVIOR = "unknown_idempotency_behavior"
    UNCLEAR_PERMISSIONS = "unclear_permissions"
    UNDOCUMENTED_WEBHOOK_EVENT_VERSIONING = "undocumented_webhook_event_versioning"
    MISSING_ROLLBACK_PATH = "missing_rollback_path"
    MISSING_OBSERVABILITY_OR_AUDIT_COVERAGE = "missing_observability_or_audit_coverage"
    UNVERIFIED_AUTOMATION_SNAPSHOT = "unverified_automation_snapshot"


class GapSeverity(ContractEnum):
    BLOCKER = "blocker"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AutomationRuleStatus(ContractEnum):
    ACTIVE = "active"
    DISABLED = "disabled"
    INTENDED = "intended"
    UNKNOWN = "unknown"


class SnapshotConfidence(ContractEnum):
    STRUCTURED_EXPORT = "structured_export"
    ADMIN_CAPTURE = "admin_capture"
    RUNBOOK = "runbook"
    INFERRED = "inferred"
    UNKNOWN = "unknown"


_MUTATING_HTTP_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_READ_ACTION_PREFIXES = (
    "check",
    "describe",
    "get",
    "health",
    "list",
    "read",
    "search",
)
_MUTATING_ACTION_PREFIXES = (
    "cancel",
    "create",
    "delete",
    "dispatch",
    "emit",
    "generate",
    "invoke",
    "patch",
    "post",
    "publish",
    "put",
    "send",
    "sync",
    "trigger",
    "update",
    "upload",
    "write",
)
_PLACEHOLDER_RE = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_.-]*)\s*}}")
_REQUIRED_OBSERVABILITY_DIMENSIONS = (
    "action_id",
    "automation_rule_id",
    "source_system",
    "target_system",
    "tenant",
    "workflow_run_id",
    "event_type",
    "result_state",
)


def stable_json_dumps(value: Any) -> str:
    """Serialize a contract payload deterministically for hashing."""

    return json.dumps(to_plain(value), sort_keys=True, separators=(",", ":"), default=str)


def stable_digest(value: Any) -> str:
    """Return a stable SHA-256 digest for a contract payload."""

    return hashlib.sha256(stable_json_dumps(value).encode("utf-8")).hexdigest()


def to_plain(value: Any) -> Any:
    """Convert dataclasses and enums to JSON-safe primitive containers."""

    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: to_plain(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, dict):
        return {str(key): to_plain(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_plain(item) for item in value]
    return value


def _tuple_of(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    return (value,)


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _sequence_of_mappings(value: object) -> tuple[dict[str, Any], ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return ()
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, dict))


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _enum_values(values: tuple[ContractEnum, ...]) -> tuple[str, ...]:
    return tuple(item.value for item in values)


@dataclass(frozen=True, slots=True)
class ExternalSystemRef:
    system_ref: str
    display_name: str = ""
    provider: str = ""
    environment_ref: str | None = None
    tenant_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "system_ref", _clean_text(self.system_ref))
        object.__setattr__(self, "display_name", _clean_text(self.display_name))
        object.__setattr__(self, "provider", _clean_text(self.provider))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class ActionSystems:
    source: ExternalSystemRef
    target: ExternalSystemRef

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class ContractField:
    name: str
    field_type: str
    required: bool = True
    description: str = ""
    default: Any = None
    constraints: dict[str, Any] = field(default_factory=dict)
    sensitive: bool = False
    source_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _clean_text(self.name))
        object.__setattr__(self, "field_type", _clean_text(self.field_type) or "unknown")
        object.__setattr__(self, "description", _clean_text(self.description))
        object.__setattr__(self, "constraints", dict(self.constraints or {}))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class PayloadEnvelope:
    schema_ref: str
    fields: tuple[ContractField, ...] = ()
    schema_version: str = "1"
    allow_additional_fields: bool = False
    validation_rules: tuple[str, ...] = ()
    examples: tuple[dict[str, Any], ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_ref", _clean_text(self.schema_ref))
        object.__setattr__(self, "schema_version", _clean_text(self.schema_version) or "1")
        object.__setattr__(self, "fields", tuple(self.fields or ()))
        object.__setattr__(self, "validation_rules", tuple(str(item) for item in _tuple_of(self.validation_rules)))
        object.__setattr__(self, "examples", tuple(dict(item) for item in self.examples or ()))
        object.__setattr__(self, "description", _clean_text(self.description))

    def field_names(self) -> tuple[str, ...]:
        return tuple(item.name for item in self.fields)

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class OutputEnvelope:
    success: PayloadEnvelope
    partial_success: PayloadEnvelope | None = None
    result_states: tuple[str, ...] = ("succeeded", "failed", "skipped")
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "result_states", tuple(str(item) for item in _tuple_of(self.result_states)))
        object.__setattr__(self, "description", _clean_text(self.description))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class ErrorEnvelope:
    schema_ref: str
    error_code_field: str = "error"
    summary_field: str = "summary"
    data_field: str = "data"
    retryable_error_codes: tuple[str, ...] = ()
    terminal_error_codes: tuple[str, ...] = ()
    unknown_error_behavior: str = "treat_as_failed_and_require_operator_review"

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_ref", _clean_text(self.schema_ref))
        object.__setattr__(self, "error_code_field", _clean_text(self.error_code_field) or "error")
        object.__setattr__(self, "summary_field", _clean_text(self.summary_field) or "summary")
        object.__setattr__(self, "data_field", _clean_text(self.data_field) or "data")
        object.__setattr__(self, "retryable_error_codes", tuple(str(item) for item in _tuple_of(self.retryable_error_codes)))
        object.__setattr__(self, "terminal_error_codes", tuple(str(item) for item in _tuple_of(self.terminal_error_codes)))
        object.__setattr__(self, "unknown_error_behavior", _clean_text(self.unknown_error_behavior))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class IdempotencyContract:
    state: IdempotencyState
    key_origin: IdempotencyKeyOrigin = IdempotencyKeyOrigin.UNKNOWN
    key_fields: tuple[str, ...] = ()
    dedupe_scope: DedupeScope = DedupeScope.UNKNOWN
    retention_window: str | None = None
    replay_after_success: str = "unknown"
    replay_after_timeout: str = "unknown"
    replay_after_partial_failure: str = "unknown"
    downstream_deduplication: str = "unknown"
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", IdempotencyState(self.state))
        object.__setattr__(self, "key_origin", IdempotencyKeyOrigin(self.key_origin))
        object.__setattr__(self, "dedupe_scope", DedupeScope(self.dedupe_scope))
        object.__setattr__(self, "key_fields", tuple(str(item) for item in _tuple_of(self.key_fields)))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class SideEffectSpec:
    kind: SideEffectKind
    target_ref: str
    description: str
    persistence: str = "unknown"
    downstream_automation: str = "unknown"
    human_visible: bool = False
    quota_or_billing_impact: str = "unknown"
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", SideEffectKind(self.kind))
        object.__setattr__(self, "target_ref", _clean_text(self.target_ref))
        object.__setattr__(self, "description", _clean_text(self.description))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class RetryReplayContract:
    retry_policy: RetryPolicyKind
    max_attempts: int | None = None
    backoff: str | None = None
    timeout_behavior: str = "unknown"
    retryable_error_codes: tuple[str, ...] = ()
    duplicate_handling: str = "unknown"
    replay_requires_receipt: bool = True
    dead_letter_path: str | None = None
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "retry_policy", RetryPolicyKind(self.retry_policy))
        object.__setattr__(self, "retryable_error_codes", tuple(str(item) for item in _tuple_of(self.retryable_error_codes)))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class PermissionBinding:
    identity_type: IdentityType
    identity_ref: str = ""
    owner_ref: str = ""
    auth_kind: str = ""
    credential_ref: str | None = None
    env_var_ref: str | None = None
    roles: tuple[str, ...] = ()
    scopes: tuple[str, ...] = ()
    resource_permissions: tuple[str, ...] = ()
    execution_identity_mode: ExecutionIdentityMode = ExecutionIdentityMode.UNKNOWN
    tenant_isolation: str = "unknown"
    least_privilege_rationale: str = ""
    rotation_approval_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "identity_type", IdentityType(self.identity_type))
        object.__setattr__(self, "execution_identity_mode", ExecutionIdentityMode(self.execution_identity_mode))
        object.__setattr__(self, "roles", tuple(str(item) for item in _tuple_of(self.roles)))
        object.__setattr__(self, "scopes", tuple(str(item) for item in _tuple_of(self.scopes)))
        object.__setattr__(self, "resource_permissions", tuple(str(item) for item in _tuple_of(self.resource_permissions)))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class EventDeliveryContract:
    event_name: str
    direction: EventDirection
    producer: str
    consumer: str
    contract_version: str = ""
    endpoint_or_topic: str = ""
    payload_schema_ref: str = ""
    authentication: str = "unknown"
    signature_verification: str = "unknown"
    delivery_semantics: EventDeliverySemantics = EventDeliverySemantics.UNKNOWN
    ordering_guarantee: str = "unknown"
    retry_policy: RetryPolicyKind = RetryPolicyKind.UNKNOWN
    replay_handling: str = "unknown"
    duplicate_suppression: str = "unknown"
    dead_letter_path: str | None = None
    failure_blocks_parent: bool | None = None
    monitoring_owner: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_name", _clean_text(self.event_name))
        object.__setattr__(self, "direction", EventDirection(self.direction))
        object.__setattr__(self, "delivery_semantics", EventDeliverySemantics(self.delivery_semantics))
        object.__setattr__(self, "retry_policy", RetryPolicyKind(self.retry_policy))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class RollbackContract:
    rollback_class: RollbackClass
    trigger_criteria: tuple[str, ...] = ()
    max_safe_window: str | None = None
    irreversible_data: tuple[str, ...] = ()
    rollback_idempotency: IdempotencyState = IdempotencyState.UNKNOWN
    approval_required: bool = True
    operator_playbook_ref: str | None = None
    compensating_action_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "rollback_class", RollbackClass(self.rollback_class))
        object.__setattr__(self, "rollback_idempotency", IdempotencyState(self.rollback_idempotency))
        object.__setattr__(self, "trigger_criteria", tuple(str(item) for item in _tuple_of(self.trigger_criteria)))
        object.__setattr__(self, "irreversible_data", tuple(str(item) for item in _tuple_of(self.irreversible_data)))

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class ObservabilityAuditContract:
    structured_logs: tuple[str, ...] = ()
    metrics: tuple[str, ...] = ()
    traces: tuple[str, ...] = ()
    audit_entries: tuple[str, ...] = ()
    required_dimensions: tuple[str, ...] = _REQUIRED_OBSERVABILITY_DIMENSIONS
    retained_dimensions: tuple[str, ...] = ()
    receipt_required: bool = True
    event_receipt_counters: tuple[str, ...] = ()
    alert_thresholds: tuple[str, ...] = ()
    retention_ref: str | None = None
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "structured_logs",
            "metrics",
            "traces",
            "audit_entries",
            "required_dimensions",
            "retained_dimensions",
            "event_receipt_counters",
            "alert_thresholds",
        ):
            object.__setattr__(self, name, tuple(str(item) for item in _tuple_of(getattr(self, name))))

    def missing_dimensions(self) -> tuple[str, ...]:
        retained = set(self.retained_dimensions)
        return tuple(item for item in self.required_dimensions if item not in retained)

    def has_minimum_signal(self) -> bool:
        return bool(self.structured_logs and self.metrics and self.audit_entries)

    def as_dict(self) -> dict[str, Any]:
        return to_plain(self)


@dataclass(frozen=True, slots=True)
class TypedContractGap:
    gap_kind: GapKind
    related_ref: str
    description: str
    evidence_source: str
    severity: GapSeverity
    proposed_owner: str
    required_follow_up: str
    disposition: str = "open"
    gap_id: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "gap_kind", GapKind(self.gap_kind))
        object.__setattr__(self, "severity", GapSeverity(self.severity))
        object.__setattr__(self, "context", dict(self.context or {}))

    def resolved_gap_id(self) -> str:
        if self.gap_id:
            return self.gap_id
        payload = {
            "gap_kind": self.gap_kind.value,
            "related_ref": self.related_ref,
            "description": self.description,
            "evidence_source": self.evidence_source,
            "severity": self.severity.value,
            "proposed_owner": self.proposed_owner,
            "required_follow_up": self.required_follow_up,
            "context": self.context,
        }
        return f"typed_gap.integration_action_contract.{stable_digest(payload)[:16]}"

    def as_dict(self) -> dict[str, Any]:
        data = to_plain(self)
        data["gap_id"] = self.resolved_gap_id()
        return data


@dataclass(frozen=True, slots=True)
class IntegrationActionContract:
    action_id: str
    name: str
    owner: str
    systems: ActionSystems
    trigger_types: tuple[TriggerType, ...]
    inputs: PayloadEnvelope
    outputs: OutputEnvelope
    errors: ErrorEnvelope
    side_effects: tuple[SideEffectSpec, ...]
    idempotency: IdempotencyContract
    retry_replay: RetryReplayContract
    permissions: tuple[PermissionBinding, ...]
    rollback: RollbackContract
    observability: ObservabilityAuditContract
    webhook_events: tuple[EventDeliveryContract, ...] = ()
    preconditions: tuple[str, ...] = ()
    invariants: tuple[str, ...] = ()
    timeout_s: int | None = None
    execution_mode: ExecutionMode = ExecutionMode.UNKNOWN
    automation_rule_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    open_gaps: tuple[TypedContractGap, ...] = ()
    status: ContractStatus = ContractStatus.DRAFT
    captured_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "action_id", _clean_text(self.action_id))
        object.__setattr__(self, "name", _clean_text(self.name))
        object.__setattr__(self, "owner", _clean_text(self.owner))
        object.__setattr__(self, "trigger_types", tuple(TriggerType(item) for item in _tuple_of(self.trigger_types)))
        object.__setattr__(self, "side_effects", tuple(self.side_effects or ()))
        object.__setattr__(self, "permissions", tuple(self.permissions or ()))
        object.__setattr__(self, "webhook_events", tuple(self.webhook_events or ()))
        object.__setattr__(self, "preconditions", tuple(str(item) for item in _tuple_of(self.preconditions)))
        object.__setattr__(self, "invariants", tuple(str(item) for item in _tuple_of(self.invariants)))
        object.__setattr__(self, "execution_mode", ExecutionMode(self.execution_mode))
        object.__setattr__(self, "automation_rule_refs", tuple(str(item) for item in _tuple_of(self.automation_rule_refs)))
        object.__setattr__(self, "evidence_refs", tuple(str(item) for item in _tuple_of(self.evidence_refs)))
        object.__setattr__(self, "open_gaps", tuple(self.open_gaps or ()))
        object.__setattr__(self, "status", ContractStatus(self.status))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    def is_mutating(self) -> bool:
        return any(
            item.kind
            not in {
                SideEffectKind.NONE,
            }
            for item in self.side_effects
        )

    def contract_hash(self) -> str:
        return stable_digest(self.as_dict(include_hash=False))

    def as_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        data = to_plain(self)
        data["trigger_types"] = _enum_values(self.trigger_types)
        data["side_effects"] = [item.as_dict() for item in self.side_effects]
        data["permissions"] = [item.as_dict() for item in self.permissions]
        data["webhook_events"] = [item.as_dict() for item in self.webhook_events]
        data["open_gaps"] = [item.as_dict() for item in self.open_gaps]
        if include_hash:
            data["contract_hash"] = self.contract_hash()
        return data

    def validation_gaps(self) -> tuple[TypedContractGap, ...]:
        return validate_action_contract(self)


@dataclass(frozen=True, slots=True)
class AutomationRuleSnapshot:
    rule_id: str
    name: str
    source_of_truth_ref: str
    snapshot_timestamp: str
    trigger_condition: str
    owner: str
    status: AutomationRuleStatus = AutomationRuleStatus.UNKNOWN
    filter_conditions: tuple[str, ...] = ()
    execution_steps: tuple[str, ...] = ()
    suppression_rules: tuple[str, ...] = ()
    rate_limits: tuple[str, ...] = ()
    environment_dependencies: tuple[str, ...] = ()
    linked_action_ids: tuple[str, ...] = ()
    pause_disable_method: str = ""
    capture_method: SnapshotConfidence = SnapshotConfidence.UNKNOWN
    confidence_notes: str = ""
    evidence_refs: tuple[str, ...] = ()
    open_gaps: tuple[TypedContractGap, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", AutomationRuleStatus(self.status))
        object.__setattr__(self, "capture_method", SnapshotConfidence(self.capture_method))
        for name in (
            "filter_conditions",
            "execution_steps",
            "suppression_rules",
            "rate_limits",
            "environment_dependencies",
            "linked_action_ids",
            "evidence_refs",
        ):
            object.__setattr__(self, name, tuple(str(item) for item in _tuple_of(getattr(self, name))))
        object.__setattr__(self, "open_gaps", tuple(self.open_gaps or ()))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    def snapshot_hash(self) -> str:
        return stable_digest(self.as_dict(include_hash=False))

    def as_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        data = to_plain(self)
        data["open_gaps"] = [item.as_dict() for item in self.open_gaps]
        if include_hash:
            data["snapshot_hash"] = self.snapshot_hash()
        return data

    def validation_gaps(self) -> tuple[TypedContractGap, ...]:
        return validate_automation_snapshot(self)


@dataclass(frozen=True, slots=True)
class ContractInventory:
    contracts: tuple[IntegrationActionContract, ...] = ()
    automation_snapshots: tuple[AutomationRuleSnapshot, ...] = ()
    captured_at: str | None = None
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "contracts", tuple(self.contracts or ()))
        object.__setattr__(self, "automation_snapshots", tuple(self.automation_snapshots or ()))
        object.__setattr__(self, "evidence_refs", tuple(str(item) for item in _tuple_of(self.evidence_refs)))

    def all_gaps(self) -> tuple[TypedContractGap, ...]:
        gaps: list[TypedContractGap] = []
        for contract in self.contracts:
            gaps.extend(contract.validation_gaps())
        for snapshot in self.automation_snapshots:
            gaps.extend(snapshot.validation_gaps())
        return tuple(_dedupe_gaps(gaps))

    def as_dict(self) -> dict[str, Any]:
        gaps = self.all_gaps()
        return {
            "kind": "integration_action_contract_inventory",
            "captured_at": self.captured_at,
            "contract_count": len(self.contracts),
            "automation_snapshot_count": len(self.automation_snapshots),
            "gap_count": len(gaps),
            "evidence_refs": list(self.evidence_refs),
            "contracts": [item.as_dict() for item in self.contracts],
            "automation_snapshots": [item.as_dict() for item in self.automation_snapshots],
            "typed_gaps": [item.as_dict() for item in gaps],
        }


def _enum_or_default(enum_cls: Callable[[Any], ContractEnum], value: object, default: ContractEnum) -> ContractEnum:
    if value is None or value == "":
        return default
    return enum_cls(value)


def _contract_field_from_mapping(payload: object) -> ContractField:
    data = _mapping(payload)
    return ContractField(
        name=str(data.get("name") or ""),
        field_type=str(data.get("field_type") or data.get("type") or "unknown"),
        required=bool(data.get("required", True)),
        description=str(data.get("description") or ""),
        default=data.get("default"),
        constraints=_mapping(data.get("constraints")),
        sensitive=bool(data.get("sensitive", False)),
        source_ref=data.get("source_ref"),
    )


def _payload_envelope_from_mapping(payload: object) -> PayloadEnvelope:
    data = _mapping(payload)
    return PayloadEnvelope(
        schema_ref=str(data.get("schema_ref") or ""),
        fields=tuple(_contract_field_from_mapping(item) for item in _sequence_of_mappings(data.get("fields"))),
        schema_version=str(data.get("schema_version") or "1"),
        allow_additional_fields=bool(data.get("allow_additional_fields", False)),
        validation_rules=tuple(str(item) for item in _tuple_of(data.get("validation_rules")) if str(item)),
        examples=tuple(dict(item) for item in _sequence_of_mappings(data.get("examples"))),
        description=str(data.get("description") or ""),
    )


def _output_envelope_from_mapping(payload: object) -> OutputEnvelope:
    data = _mapping(payload)
    partial_success = data.get("partial_success")
    return OutputEnvelope(
        success=_payload_envelope_from_mapping(data.get("success")),
        partial_success=_payload_envelope_from_mapping(partial_success) if partial_success else None,
        result_states=tuple(str(item) for item in _tuple_of(data.get("result_states") or ("succeeded", "failed", "skipped"))),
        description=str(data.get("description") or ""),
    )


def _error_envelope_from_mapping(payload: object) -> ErrorEnvelope:
    data = _mapping(payload)
    return ErrorEnvelope(
        schema_ref=str(data.get("schema_ref") or ""),
        error_code_field=str(data.get("error_code_field") or "error"),
        summary_field=str(data.get("summary_field") or "summary"),
        data_field=str(data.get("data_field") or "data"),
        retryable_error_codes=tuple(str(item) for item in _tuple_of(data.get("retryable_error_codes"))),
        terminal_error_codes=tuple(str(item) for item in _tuple_of(data.get("terminal_error_codes"))),
        unknown_error_behavior=str(
            data.get("unknown_error_behavior") or "treat_as_failed_and_require_operator_review"
        ),
    )


def _external_system_from_mapping(payload: object, *, default_ref: str) -> ExternalSystemRef:
    data = _mapping(payload)
    return ExternalSystemRef(
        system_ref=str(data.get("system_ref") or default_ref),
        display_name=str(data.get("display_name") or ""),
        provider=str(data.get("provider") or ""),
        environment_ref=data.get("environment_ref"),
        tenant_ref=data.get("tenant_ref"),
    )


def _action_systems_from_mapping(payload: object) -> ActionSystems:
    data = _mapping(payload)
    return ActionSystems(
        source=_external_system_from_mapping(data.get("source"), default_ref="unknown.source"),
        target=_external_system_from_mapping(data.get("target"), default_ref="unknown.target"),
    )


def _idempotency_from_mapping(payload: object) -> IdempotencyContract:
    data = _mapping(payload)
    return IdempotencyContract(
        state=_enum_or_default(IdempotencyState, data.get("state"), IdempotencyState.UNKNOWN),
        key_origin=_enum_or_default(IdempotencyKeyOrigin, data.get("key_origin"), IdempotencyKeyOrigin.UNKNOWN),
        key_fields=tuple(str(item) for item in _tuple_of(data.get("key_fields"))),
        dedupe_scope=_enum_or_default(DedupeScope, data.get("dedupe_scope"), DedupeScope.UNKNOWN),
        retention_window=data.get("retention_window"),
        replay_after_success=str(data.get("replay_after_success") or "unknown"),
        replay_after_timeout=str(data.get("replay_after_timeout") or "unknown"),
        replay_after_partial_failure=str(data.get("replay_after_partial_failure") or "unknown"),
        downstream_deduplication=str(data.get("downstream_deduplication") or "unknown"),
        evidence_ref=data.get("evidence_ref"),
    )


def _side_effect_from_mapping(payload: object) -> SideEffectSpec:
    data = _mapping(payload)
    return SideEffectSpec(
        kind=_enum_or_default(SideEffectKind, data.get("kind"), SideEffectKind.UNKNOWN),
        target_ref=str(data.get("target_ref") or "unknown.target"),
        description=str(data.get("description") or ""),
        persistence=str(data.get("persistence") or "unknown"),
        downstream_automation=str(data.get("downstream_automation") or "unknown"),
        human_visible=bool(data.get("human_visible", False)),
        quota_or_billing_impact=str(data.get("quota_or_billing_impact") or "unknown"),
        evidence_ref=data.get("evidence_ref"),
    )


def _retry_replay_from_mapping(payload: object) -> RetryReplayContract:
    data = _mapping(payload)
    return RetryReplayContract(
        retry_policy=_enum_or_default(RetryPolicyKind, data.get("retry_policy"), RetryPolicyKind.UNKNOWN),
        max_attempts=data.get("max_attempts"),
        backoff=data.get("backoff"),
        timeout_behavior=str(data.get("timeout_behavior") or "unknown"),
        retryable_error_codes=tuple(str(item) for item in _tuple_of(data.get("retryable_error_codes"))),
        duplicate_handling=str(data.get("duplicate_handling") or "unknown"),
        replay_requires_receipt=bool(data.get("replay_requires_receipt", True)),
        dead_letter_path=data.get("dead_letter_path"),
        evidence_ref=data.get("evidence_ref"),
    )


def _permission_from_mapping(payload: object) -> PermissionBinding:
    data = _mapping(payload)
    return PermissionBinding(
        identity_type=_enum_or_default(IdentityType, data.get("identity_type"), IdentityType.UNKNOWN),
        identity_ref=str(data.get("identity_ref") or ""),
        owner_ref=str(data.get("owner_ref") or ""),
        auth_kind=str(data.get("auth_kind") or ""),
        credential_ref=data.get("credential_ref"),
        env_var_ref=data.get("env_var_ref"),
        roles=tuple(str(item) for item in _tuple_of(data.get("roles"))),
        scopes=tuple(str(item) for item in _tuple_of(data.get("scopes"))),
        resource_permissions=tuple(str(item) for item in _tuple_of(data.get("resource_permissions"))),
        execution_identity_mode=_enum_or_default(
            ExecutionIdentityMode,
            data.get("execution_identity_mode"),
            ExecutionIdentityMode.UNKNOWN,
        ),
        tenant_isolation=str(data.get("tenant_isolation") or "unknown"),
        least_privilege_rationale=str(data.get("least_privilege_rationale") or ""),
        rotation_approval_ref=data.get("rotation_approval_ref"),
    )


def _event_delivery_from_mapping(payload: object) -> EventDeliveryContract:
    data = _mapping(payload)
    return EventDeliveryContract(
        event_name=str(data.get("event_name") or ""),
        direction=_enum_or_default(EventDirection, data.get("direction"), EventDirection.INTERNAL),
        producer=str(data.get("producer") or ""),
        consumer=str(data.get("consumer") or ""),
        contract_version=str(data.get("contract_version") or ""),
        endpoint_or_topic=str(data.get("endpoint_or_topic") or ""),
        payload_schema_ref=str(data.get("payload_schema_ref") or ""),
        authentication=str(data.get("authentication") or "unknown"),
        signature_verification=str(data.get("signature_verification") or "unknown"),
        delivery_semantics=_enum_or_default(
            EventDeliverySemantics,
            data.get("delivery_semantics"),
            EventDeliverySemantics.UNKNOWN,
        ),
        ordering_guarantee=str(data.get("ordering_guarantee") or "unknown"),
        retry_policy=_enum_or_default(RetryPolicyKind, data.get("retry_policy"), RetryPolicyKind.UNKNOWN),
        replay_handling=str(data.get("replay_handling") or "unknown"),
        duplicate_suppression=str(data.get("duplicate_suppression") or "unknown"),
        dead_letter_path=data.get("dead_letter_path"),
        failure_blocks_parent=data.get("failure_blocks_parent"),
        monitoring_owner=str(data.get("monitoring_owner") or ""),
    )


def _rollback_from_mapping(payload: object) -> RollbackContract:
    data = _mapping(payload)
    return RollbackContract(
        rollback_class=_enum_or_default(RollbackClass, data.get("rollback_class"), RollbackClass.MANUAL_ONLY),
        trigger_criteria=tuple(str(item) for item in _tuple_of(data.get("trigger_criteria"))),
        max_safe_window=data.get("max_safe_window"),
        irreversible_data=tuple(str(item) for item in _tuple_of(data.get("irreversible_data"))),
        rollback_idempotency=_enum_or_default(
            IdempotencyState,
            data.get("rollback_idempotency"),
            IdempotencyState.UNKNOWN,
        ),
        approval_required=bool(data.get("approval_required", True)),
        operator_playbook_ref=data.get("operator_playbook_ref"),
        compensating_action_ref=data.get("compensating_action_ref"),
    )


def _observability_from_mapping(payload: object) -> ObservabilityAuditContract:
    data = _mapping(payload)
    return ObservabilityAuditContract(
        structured_logs=tuple(str(item) for item in _tuple_of(data.get("structured_logs"))),
        metrics=tuple(str(item) for item in _tuple_of(data.get("metrics"))),
        traces=tuple(str(item) for item in _tuple_of(data.get("traces"))),
        audit_entries=tuple(str(item) for item in _tuple_of(data.get("audit_entries"))),
        required_dimensions=tuple(str(item) for item in _tuple_of(data.get("required_dimensions") or _REQUIRED_OBSERVABILITY_DIMENSIONS)),
        retained_dimensions=tuple(str(item) for item in _tuple_of(data.get("retained_dimensions"))),
        receipt_required=bool(data.get("receipt_required", True)),
        event_receipt_counters=tuple(str(item) for item in _tuple_of(data.get("event_receipt_counters"))),
        alert_thresholds=tuple(str(item) for item in _tuple_of(data.get("alert_thresholds"))),
        retention_ref=data.get("retention_ref"),
        evidence_ref=data.get("evidence_ref"),
    )


def _gap_from_mapping(payload: object) -> TypedContractGap | None:
    data = _mapping(payload)
    if not data.get("gap_kind"):
        return None
    return TypedContractGap(
        gap_kind=GapKind(data.get("gap_kind")),
        related_ref=str(data.get("related_ref") or ""),
        description=str(data.get("description") or ""),
        evidence_source=str(data.get("evidence_source") or "contract_payload"),
        severity=_enum_or_default(GapSeverity, data.get("severity"), GapSeverity.MEDIUM),
        proposed_owner=str(data.get("proposed_owner") or ""),
        required_follow_up=str(data.get("required_follow_up") or ""),
        disposition=str(data.get("disposition") or "open"),
        gap_id=data.get("gap_id"),
        context=_mapping(data.get("context")),
    )


def _typed_gaps_from_payload(payload: object) -> tuple[TypedContractGap, ...]:
    gaps: list[TypedContractGap] = []
    for item in _sequence_of_mappings(payload):
        gap = _gap_from_mapping(item)
        if gap is not None:
            gaps.append(gap)
    return tuple(gaps)


def integration_action_contract_from_dict(payload: object) -> IntegrationActionContract:
    """Rehydrate a JSON contract packet for validation and hashing."""

    data = _mapping(payload)
    action_id = str(data.get("action_id") or data.get("action_contract_id") or "")
    if not action_id:
        raise ValueError("integration action contract requires action_id")
    return IntegrationActionContract(
        action_id=action_id,
        name=str(data.get("name") or action_id),
        owner=str(data.get("owner") or data.get("owner_ref") or ""),
        systems=_action_systems_from_mapping(data.get("systems")),
        trigger_types=tuple(
            _enum_or_default(TriggerType, item, TriggerType.WORKFLOW_STEP)
            for item in _tuple_of(data.get("trigger_types") or (TriggerType.WORKFLOW_STEP.value,))
        ),
        inputs=_payload_envelope_from_mapping(data.get("inputs")),
        outputs=_output_envelope_from_mapping(data.get("outputs")),
        errors=_error_envelope_from_mapping(data.get("errors")),
        side_effects=tuple(_side_effect_from_mapping(item) for item in _sequence_of_mappings(data.get("side_effects"))),
        idempotency=_idempotency_from_mapping(data.get("idempotency")),
        retry_replay=_retry_replay_from_mapping(data.get("retry_replay")),
        permissions=tuple(_permission_from_mapping(item) for item in _sequence_of_mappings(data.get("permissions"))),
        rollback=_rollback_from_mapping(data.get("rollback")),
        observability=_observability_from_mapping(data.get("observability")),
        webhook_events=tuple(_event_delivery_from_mapping(item) for item in _sequence_of_mappings(data.get("webhook_events"))),
        preconditions=tuple(str(item) for item in _tuple_of(data.get("preconditions"))),
        invariants=tuple(str(item) for item in _tuple_of(data.get("invariants"))),
        timeout_s=data.get("timeout_s"),
        execution_mode=_enum_or_default(ExecutionMode, data.get("execution_mode"), ExecutionMode.UNKNOWN),
        automation_rule_refs=tuple(str(item) for item in _tuple_of(data.get("automation_rule_refs"))),
        evidence_refs=tuple(str(item) for item in _tuple_of(data.get("evidence_refs"))),
        open_gaps=_typed_gaps_from_payload(data.get("open_gaps") or data.get("typed_gaps")),
        status=_enum_or_default(ContractStatus, data.get("status"), ContractStatus.DRAFT),
        captured_at=data.get("captured_at"),
        metadata=_mapping(data.get("metadata")),
    )


def automation_rule_snapshot_from_dict(payload: object) -> AutomationRuleSnapshot:
    """Rehydrate a JSON automation snapshot for validation and hashing."""

    data = _mapping(payload)
    rule_id = str(data.get("rule_id") or data.get("automation_rule_id") or "")
    if not rule_id:
        raise ValueError("automation rule snapshot requires rule_id")
    return AutomationRuleSnapshot(
        rule_id=rule_id,
        name=str(data.get("name") or rule_id),
        source_of_truth_ref=str(data.get("source_of_truth_ref") or ""),
        snapshot_timestamp=str(data.get("snapshot_timestamp") or ""),
        trigger_condition=str(data.get("trigger_condition") or ""),
        owner=str(data.get("owner") or data.get("owner_ref") or ""),
        status=_enum_or_default(AutomationRuleStatus, data.get("status"), AutomationRuleStatus.UNKNOWN),
        filter_conditions=tuple(str(item) for item in _tuple_of(data.get("filter_conditions"))),
        execution_steps=tuple(str(item) for item in _tuple_of(data.get("execution_steps"))),
        suppression_rules=tuple(str(item) for item in _tuple_of(data.get("suppression_rules"))),
        rate_limits=tuple(str(item) for item in _tuple_of(data.get("rate_limits"))),
        environment_dependencies=tuple(str(item) for item in _tuple_of(data.get("environment_dependencies"))),
        linked_action_ids=tuple(str(item) for item in _tuple_of(data.get("linked_action_ids"))),
        pause_disable_method=str(data.get("pause_disable_method") or ""),
        capture_method=_enum_or_default(SnapshotConfidence, data.get("capture_method"), SnapshotConfidence.UNKNOWN),
        confidence_notes=str(data.get("confidence_notes") or ""),
        evidence_refs=tuple(str(item) for item in _tuple_of(data.get("evidence_refs"))),
        open_gaps=_typed_gaps_from_payload(data.get("open_gaps") or data.get("typed_gaps")),
        metadata=_mapping(data.get("metadata")),
    )


def _gap(
    *,
    kind: GapKind,
    related_ref: str,
    description: str,
    evidence_source: str,
    severity: GapSeverity,
    proposed_owner: str,
    required_follow_up: str,
    context: dict[str, Any] | None = None,
) -> TypedContractGap:
    return TypedContractGap(
        gap_kind=kind,
        related_ref=related_ref,
        description=description,
        evidence_source=evidence_source,
        severity=severity,
        proposed_owner=proposed_owner,
        required_follow_up=required_follow_up,
        context=context or {},
    )


def _dedupe_gaps(gaps: list[TypedContractGap]) -> tuple[TypedContractGap, ...]:
    seen: set[str] = set()
    deduped: list[TypedContractGap] = []
    for gap in gaps:
        gap_id = gap.resolved_gap_id()
        if gap_id in seen:
            continue
        seen.add(gap_id)
        deduped.append(gap)
    return tuple(deduped)


def validate_action_contract(contract: IntegrationActionContract) -> tuple[TypedContractGap, ...]:
    """Return typed gaps that prevent blind automation trust."""

    gaps = list(contract.open_gaps)
    related_ref = contract.action_id

    if not contract.inputs.schema_ref:
        gaps.append(_gap(
            kind=GapKind.MISSING_INPUT_SCHEMA_TYPING,
            related_ref=related_ref,
            description="Action input envelope has no stable schema reference.",
            evidence_source="contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Capture the input schema ref, required fields, defaults, and validation constraints.",
        ))

    if not contract.outputs.success.schema_ref or not contract.outputs.success.fields:
        gaps.append(_gap(
            kind=GapKind.MISSING_OUTPUT_SCHEMA_TYPING,
            related_ref=related_ref,
            description="Action output envelope is missing a success schema or success fields.",
            evidence_source="contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Capture success, partial-success, and error payload fields.",
        ))

    if contract.is_mutating() and contract.idempotency.state == IdempotencyState.UNKNOWN:
        gaps.append(_gap(
            kind=GapKind.UNKNOWN_IDEMPOTENCY_BEHAVIOR,
            related_ref=related_ref,
            description="Mutating action does not have evidenced idempotency and replay behavior.",
            evidence_source="contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Prove replay-after-success, timeout, and partial-failure behavior before automation promotion.",
        ))

    for side_effect in contract.side_effects:
        if side_effect.kind == SideEffectKind.UNKNOWN or "unknown" in {
            side_effect.persistence,
            side_effect.downstream_automation,
            side_effect.quota_or_billing_impact,
        }:
            gaps.append(_gap(
                kind=GapKind.UNKNOWN_SIDE_EFFECTS,
                related_ref=related_ref,
                description=f"Side effect {side_effect.kind.value} lacks persistence, downstream automation, or quota evidence.",
                evidence_source=side_effect.evidence_ref or "contract_validation",
                severity=GapSeverity.HIGH,
                proposed_owner=contract.owner or "integration_owner",
                required_follow_up="Capture downstream writes, notifications, quota impact, and cascading automation evidence.",
                context={"target_ref": side_effect.target_ref, "side_effect_kind": side_effect.kind.value},
            ))

    if not contract.permissions:
        gaps.append(_gap(
            kind=GapKind.UNCLEAR_PERMISSIONS,
            related_ref=related_ref,
            description="Action has no executing identity or permission binding.",
            evidence_source="contract_validation",
            severity=GapSeverity.BLOCKER,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Record executing identity, auth kind, roles/scopes, and credential reference without secret material.",
        ))
    for permission in contract.permissions:
        if permission.identity_type == IdentityType.UNKNOWN:
            gaps.append(_gap(
                kind=GapKind.UNCLEAR_PERMISSIONS,
                related_ref=related_ref,
                description="Action executing identity is unknown.",
                evidence_source="contract_validation",
                severity=GapSeverity.BLOCKER,
                proposed_owner=contract.owner or "integration_owner",
                required_follow_up="Identify whether execution uses caller, delegated, service, API key, OAuth client, or webhook identity.",
            ))
        if permission.identity_type in {IdentityType.API_KEY, IdentityType.SHARED_SYSTEM_IDENTITY} and not permission.owner_ref:
            gaps.append(_gap(
                kind=GapKind.UNCLEAR_PERMISSIONS,
                related_ref=related_ref,
                description="Shared/API-key execution identity lacks an owner reference.",
                evidence_source="contract_validation",
                severity=GapSeverity.HIGH,
                proposed_owner=contract.owner or "integration_owner",
                required_follow_up="Assign credential ownership and rotation approval boundary.",
                context={"identity_type": permission.identity_type.value},
            ))

    if contract.rollback.rollback_class == RollbackClass.MANUAL_ONLY and not contract.rollback.operator_playbook_ref:
        gaps.append(_gap(
            kind=GapKind.MISSING_ROLLBACK_PATH,
            related_ref=related_ref,
            description="Manual-only rollback is declared without an operator playbook reference.",
            evidence_source="contract_validation",
            severity=GapSeverity.MEDIUM,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Attach the manual remediation playbook or mark the action forward-fix-only with approval.",
        ))

    for event in contract.webhook_events:
        if (
            not event.contract_version
            or not event.payload_schema_ref
            or event.delivery_semantics == EventDeliverySemantics.UNKNOWN
        ):
            gaps.append(_gap(
                kind=GapKind.UNDOCUMENTED_WEBHOOK_EVENT_VERSIONING,
                related_ref=related_ref,
                description=f"Event {event.event_name or '<unnamed>'} lacks version, payload schema, or delivery semantics.",
                evidence_source="contract_validation",
                severity=GapSeverity.HIGH,
                proposed_owner=event.monitoring_owner or contract.owner or "integration_owner",
                required_follow_up="Capture event version, schema, delivery semantics, replay, duplicate suppression, and dead-letter path.",
                context={"event_name": event.event_name, "direction": event.direction.value},
            ))

    if not contract.observability.has_minimum_signal() or contract.observability.missing_dimensions():
        gaps.append(_gap(
            kind=GapKind.MISSING_OBSERVABILITY_OR_AUDIT_COVERAGE,
            related_ref=related_ref,
            description="Action observability is missing required logs, metrics, audit entries, or retained dimensions.",
            evidence_source=contract.observability.evidence_ref or "contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=contract.owner or "integration_owner",
            required_follow_up="Prove logs, metrics, audit entries, receipt/counter coverage, and queryable dimensions.",
            context={"missing_dimensions": list(contract.observability.missing_dimensions())},
        ))

    return tuple(_dedupe_gaps(gaps))


def validate_automation_snapshot(snapshot: AutomationRuleSnapshot) -> tuple[TypedContractGap, ...]:
    """Return typed gaps for an automation rule snapshot."""

    gaps = list(snapshot.open_gaps)
    related_ref = snapshot.rule_id

    if (
        not snapshot.source_of_truth_ref
        or not snapshot.snapshot_timestamp
        or snapshot.capture_method in {SnapshotConfidence.INFERRED, SnapshotConfidence.UNKNOWN}
    ):
        gaps.append(_gap(
            kind=GapKind.UNVERIFIED_AUTOMATION_SNAPSHOT,
            related_ref=related_ref,
            description="Automation snapshot is not backed by an authoritative export or timestamped capture.",
            evidence_source="contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=snapshot.owner or "automation_owner",
            required_follow_up="Capture a structured export, admin capture, or runbook-backed snapshot with timestamp.",
        ))

    if not snapshot.linked_action_ids:
        gaps.append(_gap(
            kind=GapKind.UNVERIFIED_AUTOMATION_SNAPSHOT,
            related_ref=related_ref,
            description="Automation rule is not linked to any action contracts.",
            evidence_source="contract_validation",
            severity=GapSeverity.HIGH,
            proposed_owner=snapshot.owner or "automation_owner",
            required_follow_up="Link each automation step to a typed integration action contract.",
        ))

    if not snapshot.pause_disable_method:
        gaps.append(_gap(
            kind=GapKind.UNVERIFIED_AUTOMATION_SNAPSHOT,
            related_ref=related_ref,
            description="Automation rule has no documented pause or disable method.",
            evidence_source="contract_validation",
            severity=GapSeverity.MEDIUM,
            proposed_owner=snapshot.owner or "automation_owner",
            required_follow_up="Record the operator-visible pause/disable path.",
        ))

    if snapshot.status == AutomationRuleStatus.UNKNOWN:
        gaps.append(_gap(
            kind=GapKind.UNVERIFIED_AUTOMATION_SNAPSHOT,
            related_ref=related_ref,
            description="Automation rule live/disabled/intended status is unknown.",
            evidence_source="contract_validation",
            severity=GapSeverity.MEDIUM,
            proposed_owner=snapshot.owner or "automation_owner",
            required_follow_up="Confirm whether the rule is active, disabled, or intended-only.",
        ))

    return tuple(_dedupe_gaps(gaps))


def generic_integration_result_output(schema_ref: str) -> OutputEnvelope:
    """Return the current IntegrationResult success envelope."""

    return OutputEnvelope(
        success=PayloadEnvelope(
            schema_ref=schema_ref,
            fields=(
                ContractField("status", "string", description="Terminal result state."),
                ContractField("data", "object|null", required=False, description="Action-specific success payload."),
                ContractField("summary", "string", description="Operator-readable result summary."),
                ContractField("error", "string|null", required=False, description="Error code when failed."),
            ),
            validation_rules=("status in {succeeded, failed, skipped}",),
        ),
        result_states=("succeeded", "failed", "skipped"),
        description="Runtime integration result envelope.",
    )


def generic_integration_error_envelope(schema_ref: str) -> ErrorEnvelope:
    """Return the current IntegrationResult error envelope."""

    return ErrorEnvelope(
        schema_ref=schema_ref,
        retryable_error_codes=(
            "connection_error",
            "connector_network_error",
            "connector_rate_limited",
            "connector_server_error",
            "connector_timeout",
            "webhook_exception",
        ),
        terminal_error_codes=(
            "action_not_found",
            "auth_resolution_failed",
            "connector_auth_error",
            "connector_credential_missing",
            "integration_executor_not_bound",
            "integration_not_connected",
            "invalid_url",
            "missing_url",
            "ssrf_blocked",
        ),
    )


def _input_fields_from_capability(capability: dict[str, Any]) -> tuple[ContractField, ...]:
    fields_by_name: dict[str, ContractField] = {}
    body_template = capability.get("body_template") or capability.get("requestBodyTemplate")
    for name in _placeholders_from_value(body_template):
        fields_by_name[name] = ContractField(
            name=name,
            field_type="string",
            required=True,
            description=f"Interpolated into {capability.get('action', 'action')} request body.",
            source_ref="capability.body_template",
        )

    return tuple(fields_by_name[name] for name in sorted(fields_by_name))


def _placeholders_from_value(value: object) -> tuple[str, ...]:
    names: set[str] = set()
    if isinstance(value, str):
        names.update(match.group(1) for match in _PLACEHOLDER_RE.finditer(value))
    elif isinstance(value, dict):
        for item in value.values():
            names.update(_placeholders_from_value(item))
    elif isinstance(value, list):
        for item in value:
            names.update(_placeholders_from_value(item))
    return tuple(sorted(names))


def _field_type_for_value(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if value is None:
        return "unknown"
    return "string"


def _capability_method(capability: dict[str, Any]) -> str:
    return _clean_text(capability.get("method")).upper()


def _action_name(capability: dict[str, Any]) -> str:
    return _clean_text(capability.get("action"))


def _action_is_mutating(action: str, capability: dict[str, Any]) -> bool:
    method = _capability_method(capability)
    normalized = action.lower()
    if method in _MUTATING_HTTP_METHODS:
        return True
    if method in {"GET", "HEAD", "OPTIONS"}:
        return False
    if normalized.startswith(_READ_ACTION_PREFIXES):
        return False
    return normalized.startswith(_MUTATING_ACTION_PREFIXES)


def _default_idempotency(action: str, capability: dict[str, Any]) -> IdempotencyContract:
    if not _action_is_mutating(action, capability):
        return IdempotencyContract(
            state=IdempotencyState.FULLY_IDEMPOTENT,
            key_origin=IdempotencyKeyOrigin.UNAVAILABLE,
            dedupe_scope=DedupeScope.UNAVAILABLE,
            replay_after_success="safe_read_replay",
            replay_after_timeout="safe_read_replay",
            replay_after_partial_failure="safe_read_replay",
            downstream_deduplication="not_required_for_read_only_action",
            evidence_ref="runtime.integrations.action_contracts.read_only_inference",
        )
    return IdempotencyContract(
        state=IdempotencyState.UNKNOWN,
        key_origin=IdempotencyKeyOrigin.UNKNOWN,
        dedupe_scope=DedupeScope.UNKNOWN,
    )


def _default_retry(action: str, capability: dict[str, Any]) -> RetryReplayContract:
    if not _action_is_mutating(action, capability):
        return RetryReplayContract(
            retry_policy=RetryPolicyKind.NONE,
            timeout_behavior="caller_may_repeat_read",
            duplicate_handling="read_only_duplicate_has_no_side_effect",
            replay_requires_receipt=False,
        )
    return RetryReplayContract(
        retry_policy=RetryPolicyKind.UNKNOWN,
        timeout_behavior="unknown",
        duplicate_handling="unknown",
        replay_requires_receipt=True,
    )


def _default_side_effects(
    *,
    integration_id: str,
    action: str,
    capability: dict[str, Any],
) -> tuple[SideEffectSpec, ...]:
    if not _action_is_mutating(action, capability):
        return (
            SideEffectSpec(
                kind=SideEffectKind.NONE,
                target_ref=integration_id,
                description="Read-only action inference; no persistent mutation expected.",
                persistence="none",
                downstream_automation="none",
                quota_or_billing_impact="read_quota_possible",
            ),
        )

    method = _capability_method(capability)
    if integration_id == "notifications" or action.startswith("send"):
        return (
            SideEffectSpec(
                kind=SideEffectKind.NOTIFICATION_SEND,
                target_ref=integration_id,
                description="Sends a human-facing or operator-facing notification.",
                persistence="external_delivery_or_file_append",
                downstream_automation="unknown",
                human_visible=True,
                quota_or_billing_impact="unknown",
            ),
        )
    if integration_id in {"workflow", "praxis-dispatch"}:
        kind = SideEffectKind.WORKFLOW_STATE_TRANSITION if action == "cancel" else SideEffectKind.WORKFLOW_DISPATCH
        return (
            SideEffectSpec(
                kind=kind,
                target_ref="praxis.workflow_runtime",
                description=f"Mutates Praxis workflow runtime through {integration_id}/{action}.",
                persistence="workflow_runtime_tables_and_authority_events",
                downstream_automation="may_trigger_workflow_jobs",
                human_visible=True,
                quota_or_billing_impact="provider_and_runtime_capacity",
                evidence_ref="runtime.integrations.platform",
            ),
        )
    return (
        SideEffectSpec(
            kind=SideEffectKind.EXTERNAL_HTTP_REQUEST if method else SideEffectKind.EXTERNAL_MUTATION,
            target_ref=integration_id,
            description=f"Mutating integration action {integration_id}/{action}; downstream effect must be evidenced.",
            persistence="unknown",
            downstream_automation="unknown",
            human_visible=False,
            quota_or_billing_impact="unknown",
            evidence_ref="integration_registry.capabilities",
        ),
    )


def _permission_from_auth_shape(
    definition: dict[str, Any],
    *,
    owner: str,
) -> PermissionBinding:
    auth_shape = _mapping(definition.get("auth_shape"))
    auth_kind = _clean_text(auth_shape.get("kind")).lower()
    credential_ref = _clean_text(auth_shape.get("credential_ref")) or None
    env_var_ref = _clean_text(auth_shape.get("env_var")) or None
    scopes = tuple(str(item) for item in _tuple_of(auth_shape.get("scopes")) if str(item))

    identity_type = IdentityType.UNKNOWN
    execution_mode = ExecutionIdentityMode.UNKNOWN
    if auth_kind == "oauth2":
        identity_type = IdentityType.OAUTH_CLIENT
        execution_mode = ExecutionIdentityMode.DELEGATED_IDENTITY
    elif auth_kind in {"api_key", "env_var"} or credential_ref or env_var_ref:
        identity_type = IdentityType.API_KEY
        execution_mode = ExecutionIdentityMode.SHARED_SYSTEM_IDENTITY
    elif auth_kind in {"none", "anonymous"}:
        identity_type = IdentityType.SERVICE_ACCOUNT
        execution_mode = ExecutionIdentityMode.SHARED_SYSTEM_IDENTITY

    return PermissionBinding(
        identity_type=identity_type,
        identity_ref=credential_ref or env_var_ref or _clean_text(definition.get("id")),
        owner_ref=owner if identity_type != IdentityType.UNKNOWN else "",
        auth_kind=auth_kind or "unknown",
        credential_ref=credential_ref,
        env_var_ref=env_var_ref,
        scopes=scopes,
        execution_identity_mode=execution_mode,
        tenant_isolation="unknown",
        least_privilege_rationale="captured_from_integration_auth_shape" if identity_type != IdentityType.UNKNOWN else "",
    )


def _default_rollback(action: str, capability: dict[str, Any]) -> RollbackContract:
    if not _action_is_mutating(action, capability):
        return RollbackContract(
            rollback_class=RollbackClass.FORWARD_FIX_ONLY,
            trigger_criteria=("read_only_action_no_rollback_needed",),
            rollback_idempotency=IdempotencyState.FULLY_IDEMPOTENT,
            approval_required=False,
        )
    return RollbackContract(
        rollback_class=RollbackClass.MANUAL_ONLY,
        trigger_criteria=("operator_detected_bad_side_effect", "failed_verifier_or_drift_readback"),
        rollback_idempotency=IdempotencyState.UNKNOWN,
    )


def _default_observability(
    *,
    integration_id: str,
    action: str,
    automation_rule_refs: tuple[str, ...] = (),
) -> ObservabilityAuditContract:
    retained_dimensions = (
        "action_id",
        "source_system",
        "target_system",
        "workflow_run_id",
        "result_state",
    )
    if automation_rule_refs:
        retained_dimensions = retained_dimensions + ("automation_rule_id",)
    return ObservabilityAuditContract(
        structured_logs=("runtime.integration_execution_log",),
        metrics=("integration_action_execution_status", "integration_action_latency_ms", "integration_action_retry_count"),
        audit_entries=("authority_operation_receipts_or_runtime_event",),
        retained_dimensions=retained_dimensions,
        receipt_required=True,
        event_receipt_counters=("integration_action_result",),
        evidence_ref=f"runtime.integrations.{integration_id}.{action}",
    )


def draft_contract_from_registry_definition(
    definition: dict[str, Any],
    capability: dict[str, Any],
    *,
    captured_at: str | None = None,
    owner: str = "integration_owner",
) -> IntegrationActionContract:
    """Create a conservative draft contract from a registry definition.

    The result intentionally leaves idempotency, side effects, permissions, and
    observability as gaps when the registry row does not prove them.
    """

    integration_id = _clean_text(definition.get("id"))
    action = _action_name(capability)
    if not integration_id:
        raise ValueError("integration definition requires id")
    if not action:
        raise ValueError("capability requires action")

    provider = _clean_text(definition.get("provider")) or "unknown"
    target_name = _clean_text(definition.get("display_name")) or _clean_text(definition.get("name")) or integration_id
    action_id = f"integration_action.{integration_id}.{action}"
    schema_prefix = f"{action_id}.schema"

    contract = IntegrationActionContract(
        action_id=action_id,
        name=_clean_text(capability.get("description")) or f"{target_name} / {action}",
        owner=owner,
        systems=ActionSystems(
            source=ExternalSystemRef("praxis.workflow", "Praxis Workflow", "praxis"),
            target=ExternalSystemRef(f"integration.{integration_id}", target_name, provider),
        ),
        trigger_types=(TriggerType.WORKFLOW_STEP,),
        inputs=PayloadEnvelope(
            schema_ref=f"{schema_prefix}.input",
            fields=_input_fields_from_capability(capability),
            allow_additional_fields=True,
            validation_rules=("registry capability arguments are accepted as action args",),
            description="Draft input envelope captured from integration_registry capability data.",
        ),
        outputs=generic_integration_result_output(f"{schema_prefix}.output"),
        errors=generic_integration_error_envelope(f"{schema_prefix}.error"),
        side_effects=_default_side_effects(
            integration_id=integration_id,
            action=action,
            capability=capability,
        ),
        idempotency=_default_idempotency(action, capability),
        retry_replay=_default_retry(action, capability),
        permissions=(_permission_from_auth_shape(definition, owner=owner),),
        rollback=_default_rollback(action, capability),
        observability=_default_observability(integration_id=integration_id, action=action),
        preconditions=("integration_registry.auth_status must be connected",),
        invariants=("do_not_store_secret_material_in_contract",),
        timeout_s=_timeout_from_capability(capability),
        execution_mode=ExecutionMode.SYNC,
        evidence_refs=("integration_registry",),
        captured_at=captured_at,
        metadata={
            "manifest_source": definition.get("manifest_source"),
            "connector_slug": definition.get("connector_slug"),
            "http_method": _capability_method(capability) or None,
            "path_present": bool(capability.get("path")),
        },
    )
    return _apply_known_action_overrides(contract, definition, capability)


def draft_contracts_from_registry_definition(
    definition: dict[str, Any],
    *,
    captured_at: str | None = None,
    owner: str = "integration_owner",
) -> tuple[IntegrationActionContract, ...]:
    """Return draft contracts for every capability on one registry row."""

    contracts: list[IntegrationActionContract] = []
    for capability in _sequence_of_mappings(definition.get("capabilities")):
        if _action_name(capability):
            contracts.append(
                draft_contract_from_registry_definition(
                    definition,
                    capability,
                    captured_at=captured_at,
                    owner=owner,
                )
            )
    return tuple(contracts)


def _timeout_from_capability(capability: dict[str, Any]) -> int | None:
    raw = capability.get("timeout") or capability.get("timeout_s")
    if raw is None:
        return None
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _apply_known_action_overrides(
    contract: IntegrationActionContract,
    definition: dict[str, Any],
    capability: dict[str, Any],
) -> IntegrationActionContract:
    integration_id = _clean_text(definition.get("id"))
    action = _action_name(capability)

    if (integration_id, action) in {
        ("praxis-dispatch", "check_status"),
        ("praxis-dispatch", "search_receipts"),
    }:
        return replace(
            contract,
            rollback=RollbackContract(
                rollback_class=RollbackClass.FORWARD_FIX_ONLY,
                trigger_criteria=("read_only_action_no_rollback_needed",),
                rollback_idempotency=IdempotencyState.FULLY_IDEMPOTENT,
                approval_required=False,
            ),
            observability=replace(
                contract.observability,
                retained_dimensions=_REQUIRED_OBSERVABILITY_DIMENSIONS,
                traces=("authority_operation_receipts",),
            ),
        )

    if (integration_id, action) == ("workflow", "cancel"):
        return replace(
            contract,
            idempotency=IdempotencyContract(
                state=IdempotencyState.CONDITIONALLY_IDEMPOTENT,
                key_origin=IdempotencyKeyOrigin.RESOURCE_DERIVED,
                key_fields=("run_id",),
                dedupe_scope=DedupeScope.PER_RESOURCE,
                replay_after_success="repeat cancel should converge on cancelled run state when the same run_id is used",
                replay_after_timeout="read run status before retrying cancel",
                replay_after_partial_failure="read control command response and run status before retrying",
                downstream_deduplication="workflow control command idempotency key derives from run_id",
                evidence_ref="runtime.integrations.platform.execute_workflow_cancel",
            ),
            retry_replay=RetryReplayContract(
                retry_policy=RetryPolicyKind.MANUAL_ONLY,
                timeout_behavior="read run status before retry",
                duplicate_handling="resource-derived run_id cancel convergence",
                replay_requires_receipt=True,
                evidence_ref="runtime.integrations.platform.execute_workflow_cancel",
            ),
            rollback=RollbackContract(
                rollback_class=RollbackClass.FORWARD_FIX_ONLY,
                trigger_criteria=("cancelled_wrong_run",),
                rollback_idempotency=IdempotencyState.NON_IDEMPOTENT,
                approval_required=True,
                operator_playbook_ref="workflow_control_runbook.restore_or_resubmit",
            ),
            observability=replace(
                contract.observability,
                retained_dimensions=_REQUIRED_OBSERVABILITY_DIMENSIONS,
                traces=("authority_operation_receipts", "authority_events"),
            ),
        )

    if (integration_id, action) in {
        ("workflow", "invoke"),
        ("praxis-dispatch", "dispatch_job"),
    }:
        return replace(
            contract,
            idempotency=IdempotencyContract(
                state=IdempotencyState.NON_IDEMPOTENT,
                key_origin=IdempotencyKeyOrigin.WORKFLOW_RUN_GENERATED,
                dedupe_scope=DedupeScope.PER_WORKFLOW_RUN,
                replay_after_success="duplicate invocation can create a separate workflow run",
                replay_after_timeout="inspect submission receipt or run status before retry",
                replay_after_partial_failure="inspect workflow submission receipt before retry",
                downstream_deduplication="not guaranteed by integration action",
                evidence_ref="runtime.integrations.platform",
            ),
            retry_replay=RetryReplayContract(
                retry_policy=RetryPolicyKind.MANUAL_ONLY,
                timeout_behavior="inspect workflow receipt before retry",
                duplicate_handling="duplicate dispatch may create another run",
                replay_requires_receipt=True,
                evidence_ref="runtime.integrations.platform",
            ),
            rollback=RollbackContract(
                rollback_class=RollbackClass.COMPENSATABLE,
                trigger_criteria=("duplicate_or_bad_workflow_run_created",),
                rollback_idempotency=IdempotencyState.CONDITIONALLY_IDEMPOTENT,
                approval_required=True,
                compensating_action_ref="integration_action.workflow.cancel",
                operator_playbook_ref="workflow_control_runbook.cancel_or_forward_fix",
            ),
            observability=replace(
                contract.observability,
                retained_dimensions=_REQUIRED_OBSERVABILITY_DIMENSIONS,
                traces=("authority_operation_receipts", "authority_events", "workflow_run_status"),
            ),
        )

    if (integration_id, action) == ("notifications", "send"):
        return replace(
            contract,
            idempotency=IdempotencyContract(
                state=IdempotencyState.NON_IDEMPOTENT,
                key_origin=IdempotencyKeyOrigin.UNAVAILABLE,
                dedupe_scope=DedupeScope.UNAVAILABLE,
                replay_after_success="duplicate replay can send duplicate notifications",
                replay_after_timeout="check notification channel logs before retry",
                replay_after_partial_failure="inspect per-channel delivery evidence before retry",
                downstream_deduplication="not guaranteed",
                evidence_ref="runtime.integrations.platform.execute_notification",
            ),
            retry_replay=RetryReplayContract(
                retry_policy=RetryPolicyKind.MANUAL_ONLY,
                timeout_behavior="inspect configured notification channel evidence before retry",
                duplicate_handling="duplicate notification possible",
                replay_requires_receipt=True,
            ),
            rollback=RollbackContract(
                rollback_class=RollbackClass.FORWARD_FIX_ONLY,
                trigger_criteria=("bad_or_duplicate_notification_sent",),
                rollback_idempotency=IdempotencyState.NON_IDEMPOTENT,
                approval_required=True,
                operator_playbook_ref="notification_runbook.corrective_followup",
            ),
        )

    return contract


__all__ = [
    "ActionSystems",
    "AutomationRuleSnapshot",
    "AutomationRuleStatus",
    "ContractField",
    "ContractInventory",
    "ContractStatus",
    "DedupeScope",
    "ErrorEnvelope",
    "EventDeliveryContract",
    "EventDeliverySemantics",
    "EventDirection",
    "ExecutionIdentityMode",
    "ExecutionMode",
    "ExternalSystemRef",
    "GapKind",
    "GapSeverity",
    "IdempotencyContract",
    "IdempotencyKeyOrigin",
    "IdempotencyState",
    "IdentityType",
    "IntegrationActionContract",
    "ObservabilityAuditContract",
    "OutputEnvelope",
    "PayloadEnvelope",
    "PermissionBinding",
    "RetryPolicyKind",
    "RetryReplayContract",
    "RollbackClass",
    "RollbackContract",
    "SideEffectKind",
    "SideEffectSpec",
    "SnapshotConfidence",
    "TriggerType",
    "TypedContractGap",
    "automation_rule_snapshot_from_dict",
    "draft_contract_from_registry_definition",
    "draft_contracts_from_registry_definition",
    "generic_integration_error_envelope",
    "generic_integration_result_output",
    "integration_action_contract_from_dict",
    "stable_digest",
    "stable_json_dumps",
    "to_plain",
    "validate_action_contract",
    "validate_automation_snapshot",
]
