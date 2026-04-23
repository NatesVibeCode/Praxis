"""Merged operator write surface over control writes and workflow-flow entrypoints."""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from collections.abc import AsyncIterator
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from authority.operator_control import (
    OperatorDecisionAuthorityRecord,
    operator_decision_scope_policy,
)
from policy.workflow_classes import (
    WorkflowClassAuthorityRecord,
    WorkflowClassCatalog,
    load_workflow_class_catalog,
)
from policy.native_primary_cutover import (
    NativePrimaryCutoverGateRecord,
    NativePrimaryCutoverRepository,
    NativePrimaryCutoverRuntime,
    PostgresNativePrimaryCutoverRepository,
)
from runtime.bug_evidence import EVIDENCE_ROLE_VALIDATES_FIX
from runtime.cache_invalidation import (
    CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE,
    CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT,
    aemit_cache_invalidation,
)
from runtime.bug_tracker import BugCategory, BugStatus, afile_bug
from runtime.circuit_breaker import invalidate_circuit_breaker_override_cache
from runtime.event_log import CHANNEL_DATASET, CHANNEL_SEMANTIC_ASSERTION, aemit
from runtime.instance import NativeWorkflowInstance, resolve_native_instance
from runtime.operator_object_relations import (
    FunctionalAreaRecord,
    OperatorObjectRelationRecord,
    OperatorObjectRelationRepository,
    PostgresOperatorObjectRelationRepository,
    SUPPORTED_FUNCTIONAL_AREA_STATUSES,
    SUPPORTED_OPERATOR_OBJECT_KINDS,
    SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES,
    functional_area_id_from_slug,
    operator_object_relation_id,
)
from runtime.route_authority_snapshot import invalidate_route_authority_cache_key
from runtime.recurring_review_repair_flow import (
    RecurringReviewRepairFlowRequest,
    RecurringReviewRepairFlowResolution,
    resolve_recurring_review_repair_flow,
)
from runtime.semantic_assertions import (
    SemanticAssertionRecord,
    SemanticAssertionRepository,
    SemanticPredicateRecord,
    normalize_semantic_assertion_record,
)
from runtime.work_item_workflow_bindings import (
    PostgresWorkItemWorkflowBindingRepository,
    WorkItemWorkflowBindingRecord,
    WorkItemWorkflowBindingRepository,
    WorkItemWorkflowBindingRuntime,
)
from storage.postgres import (
    PostgresOperatorIdeaRepository,
    PostgresOperatorControlRepository,
    PostgresWorkItemCloseoutRepository,
    PostgresRoadmapAuthoringRepository,
    PostgresSemanticAssertionRepository,
    PostgresTaskRouteEligibilityRepository,
    connect_workflow_database,
    resolve_workflow_authority_cache_key,
)
from ._operator_helpers import _json_compatible, _normalize_as_of, _now, _run_async
from ._payload_contract import (
    coerce_choice,
    coerce_slug,
    coerce_text_sequence,
    optional_text,
    require_text,
)

logger = logging.getLogger(__name__)


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Fetch rows."""

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Fetch one row."""

    def transaction(self) -> AsyncIterator[object]:
        """Open a transaction context."""

    async def close(self) -> None:
        """Close the connection."""


_TASK_ROUTE_ELIGIBILITY_STATUSES = frozenset({"eligible", "rejected"})
_ISSUE_STATUSES = frozenset({"open", "resolved"})
_ROADMAP_WRITE_ACTIONS = frozenset({"preview", "validate", "commit"})
_WORK_ITEM_CLOSEOUT_ACTIONS = frozenset({"preview", "commit"})
_OPERATOR_IDEA_ACTIONS = frozenset({"list", "file", "resolve", "promote"})
_OPERATOR_IDEA_STATUSES = frozenset(
    {"open", "promoted", "rejected", "superseded", "archived"}
)
_OPERATOR_IDEA_RESOLUTION_STATUSES = frozenset({"rejected", "superseded", "archived"})
_ROADMAP_ITEM_KINDS = frozenset({"capability", "initiative"})
_ROADMAP_STATUSES = frozenset({"active", "completed", "done"})
_ROADMAP_LIFECYCLES = frozenset({"idea", "planned", "claimed", "completed"})
_ROADMAP_PRIORITIES = frozenset({"p1", "p2"})
_CIRCUIT_BREAKER_OVERRIDE_STATES = frozenset({"open", "closed", "reset"})
_FUNCTIONAL_AREA_STATUSES = frozenset(SUPPORTED_FUNCTIONAL_AREA_STATUSES)
_OBJECT_RELATION_STATUSES = frozenset(SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES)
_OBJECT_RELATION_KINDS = frozenset(SUPPORTED_OPERATOR_OBJECT_KINDS)
_OBJECT_RELATION_SEMANTIC_PREDICATE_ALLOWLIST = tuple(SUPPORTED_OPERATOR_OBJECT_KINDS)
_OBJECT_RELATION_SEMANTIC_SOURCE_KIND = "operator_object_relation"
_OBJECT_RELATION_SEMANTIC_CARDINALITY_MODE = "single_active_per_edge"
_DECISION_SEMANTIC_SOURCE_KIND = "operator_decision"
_DECISION_SEMANTIC_OBJECT_ALLOWLIST = ("operator_decision",)
_DECISION_SEMANTIC_CARDINALITY_MODE = "many"
_ROADMAP_SEMANTIC_SOURCE_KIND = "roadmap_item"
_ROADMAP_SEMANTIC_SUBJECT_ALLOWLIST = ("roadmap_item",)
_ROADMAP_SEMANTIC_PREDICATE_SPECS: dict[str, dict[str, object]] = {
    "sourced_from_bug": {
        "object_kind_allowlist": ("bug",),
        "cardinality_mode": "single_active_per_subject",
        "description": (
            "Auto-registered bridge predicate mirroring roadmap_items.source_bug_id "
            "onto scoped semantic edges."
        ),
    },
    "sourced_from_idea": {
        "object_kind_allowlist": ("operator_idea",),
        "cardinality_mode": "single_active_per_subject",
        "description": (
            "Auto-registered bridge predicate mirroring roadmap_items.source_idea_id "
            "onto scoped semantic edges."
        ),
    },
    "governed_by_decision_ref": {
        "object_kind_allowlist": ("decision_ref",),
        "cardinality_mode": "single_active_per_subject",
        "description": (
            "Auto-registered bridge predicate mirroring roadmap_items.decision_ref "
            "onto scoped semantic edges."
        ),
    },
    "touches_repo_path": {
        "object_kind_allowlist": ("repo_path",),
        "cardinality_mode": "many",
        "description": (
            "Auto-registered bridge predicate mirroring roadmap_items.registry_paths "
            "onto scoped semantic edges."
        ),
    },
}
_BUG_CLOSEOUT_EVIDENCE_ROLE = EVIDENCE_ROLE_VALIDATES_FIX
_ROADMAP_COMPLETED_STATUS = "completed"
_ROADMAP_DEFAULT_LIFECYCLE = "planned"
_ROADMAP_CLAIMED_LIFECYCLE = "claimed"
_ROADMAP_COMPLETED_LIFECYCLE = "completed"
_BUG_CLOSEOUT_VERIFICATION_SUCCESS_STATUSES = frozenset({"passed", "succeeded", "success", "ok"})
_ROADMAP_BUG_CLOSEOUT_RELATION_KINDS = frozenset(
    {
        "fixed_by",
        "implemented_by_fix",
        "resolves_bug",
    }
)
_CAPABILITY_DELIVERED_BY_DECISION_FILING = "capability_delivered_by_decision_filing"


def _roadmap_acceptance_proof_kind(value: object) -> str | None:
    """Return acceptance_criteria.proof_kind when the roadmap row opts into
    capability-delivered-by-decision-filing closeout. Returns None otherwise.

    Narrow on purpose: closeout-by-decision-ref is opt-in per roadmap row so the
    default gate still requires source_bug_id + validates_fix proof. Operators
    set acceptance_criteria.proof_kind = 'capability_delivered_by_decision_filing'
    on rows whose deliverable IS the decision itself (e.g. standing-order policy
    filings where the decision row is the artifact).

    acceptance_criteria is canonically a JSON object but some historical rows
    stored a JSON array. This helper accepts either shape: a top-level object,
    or an array whose first mapping-valued element carries the marker.
    """

    candidate: Any = value
    if isinstance(candidate, (bytes, bytearray)):
        try:
            candidate = candidate.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(candidate, str):
        text = candidate.strip()
        if not text:
            return None
        try:
            import json as _json

            candidate = _json.loads(text)
        except ValueError:
            return None
    if isinstance(candidate, Mapping):
        proof_kind = candidate.get("proof_kind")
    elif isinstance(candidate, (list, tuple)):
        proof_kind = None
        for element in candidate:
            if isinstance(element, Mapping) and "proof_kind" in element:
                proof_kind = element.get("proof_kind")
                break
    else:
        return None
    if not isinstance(proof_kind, str):
        return None
    proof_kind = proof_kind.strip()
    return proof_kind or None


_require_text = require_text
_optional_text = optional_text
_coerce_text_sequence = coerce_text_sequence


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _normalize_task_route_eligibility_status(value: object) -> str:
    return coerce_choice(
        value,
        field_name="eligibility_status",
        choices=_TASK_ROUTE_ELIGIBILITY_STATUSES,
    )


def _normalize_issue_status(value: object | None) -> str:
    if value is None:
        return "open"
    return coerce_choice(
        value,
        field_name="status",
        choices=_ISSUE_STATUSES,
    )


def _normalize_functional_area_status(value: object | None) -> str:
    if value is None:
        return "active"
    return coerce_choice(
        value,
        field_name="area_status",
        choices=_FUNCTIONAL_AREA_STATUSES,
    )


def _normalize_object_relation_status(value: object | None) -> str:
    if value is None:
        return "active"
    return coerce_choice(
        value,
        field_name="relation_status",
        choices=_OBJECT_RELATION_STATUSES,
    )


def _normalize_object_kind(value: object, *, field_name: str) -> str:
    return coerce_choice(
        value,
        field_name=field_name,
        choices=_OBJECT_RELATION_KINDS,
    )


def _scope_fragment(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    normalized = value.strip().lower()
    fragments = [
        char if char.isalnum() else "-"
        for char in normalized
    ]
    collapsed = "".join(fragments).strip("-")
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    return collapsed or fallback


def _normalize_relation_kind(value: object) -> str:
    normalized = _require_text(value, field_name="relation_kind")
    return _scope_fragment(normalized, fallback="relation").replace("-", "_")


def _normalize_functional_area_ref(value: object, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized.startswith("functional_area."):
        return normalized
    return functional_area_id_from_slug(normalized)


def _operator_decision_id_from_key(
    *,
    decision_kind: str,
    decision_key: str,
) -> str:
    normalized_kind = _scope_fragment(decision_kind, fallback="decision").replace("-", "_")
    normalized_key = _require_text(decision_key, field_name="decision_key")
    raw_parts = [part.strip() for part in normalized_key.split("::") if part.strip()]
    normalized_parts = [
        _scope_fragment(part, fallback="segment").replace("-", "_")
        for part in raw_parts
    ]
    kind_prefix = normalized_kind.replace("_", "-")
    if normalized_parts and normalized_parts[0] == kind_prefix.replace("-", "_"):
        normalized_parts = normalized_parts[1:]
    if not normalized_parts:
        normalized_parts = ("default",)
    return ".".join(("operator_decision", normalized_kind, *normalized_parts))


def _operator_decision_to_json(
    decision: OperatorDecisionAuthorityRecord,
) -> dict[str, Any]:
    return {
        "operator_decision_id": decision.operator_decision_id,
        "decision_key": decision.decision_key,
        "decision_kind": decision.decision_kind,
        "decision_status": decision.decision_status,
        "title": decision.title,
        "rationale": decision.rationale,
        "decided_by": decision.decided_by,
        "decision_source": decision.decision_source,
        "effective_from": decision.effective_from.isoformat(),
        "effective_to": (
            None if decision.effective_to is None else decision.effective_to.isoformat()
        ),
        "decided_at": decision.decided_at.isoformat(),
        "created_at": decision.created_at.isoformat(),
        "updated_at": decision.updated_at.isoformat(),
        "decision_scope_kind": decision.decision_scope_kind,
        "decision_scope_ref": decision.decision_scope_ref,
    }


def _normalize_registry_paths(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = value
        value = parsed
    return coerce_text_sequence(value, field_name="registry_paths")


def _normalize_datetime_like(
    value: object,
    *,
    field_name: str,
    reason_code: str,
) -> datetime:
    candidate = value
    if isinstance(candidate, str):
        try:
            candidate = datetime.fromisoformat(candidate)
        except ValueError:
            pass
    return _normalize_as_of(
        candidate,
        error_type=ValueError,
        reason_code=reason_code,
    )


def _normalize_roadmap_action(value: object) -> str:
    return coerce_choice(
        value,
        field_name="action",
        choices=_ROADMAP_WRITE_ACTIONS,
    )


def _normalize_work_item_closeout_action(value: object) -> str:
    return coerce_choice(
        value,
        field_name="action",
        choices=_WORK_ITEM_CLOSEOUT_ACTIONS,
    )


def _normalize_operator_idea_action(value: object | None) -> str:
    if value is None:
        return "list"
    return coerce_choice(
        value,
        field_name="action",
        choices=_OPERATOR_IDEA_ACTIONS,
    )


def _normalize_operator_idea_status(
    value: object | None,
    *,
    terminal_only: bool = False,
) -> str | None:
    if value is None:
        return None
    return coerce_choice(
        value,
        field_name="status",
        choices=(
            _OPERATOR_IDEA_RESOLUTION_STATUSES
            if terminal_only
            else _OPERATOR_IDEA_STATUSES
        ),
    )


def _normalize_roadmap_item_kind(value: object | None, *, template: str) -> str:
    if value is None:
        return "capability" if template == "hard_cutover_program" else "capability"
    return coerce_choice(
        value,
        field_name="item_kind",
        choices=_ROADMAP_ITEM_KINDS,
    )


def _normalize_roadmap_status(value: object | None) -> str:
    if value is None:
        return "active"
    return coerce_choice(
        value,
        field_name="status",
        choices=_ROADMAP_STATUSES,
    )


def _normalize_roadmap_lifecycle(value: object | None) -> str:
    if value is None:
        return _ROADMAP_DEFAULT_LIFECYCLE
    return coerce_choice(
        value,
        field_name="lifecycle",
        choices=_ROADMAP_LIFECYCLES,
    )


def _normalize_roadmap_priority(value: object | None) -> str:
    if value is None:
        return "p2"
    return coerce_choice(
        value,
        field_name="priority",
        choices=_ROADMAP_PRIORITIES,
    )


def _slugify_roadmap_text(value: str) -> str:
    lowered = value.strip().lower()
    tokens = re.findall(r"[a-z0-9]+", lowered)
    return ".".join(tokens) or "item"


def _looks_like_full_roadmap_ref(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized.startswith(("roadmap_item.", "roadmap.item.", "roadmap."))


def _roadmap_key_from_item_id(roadmap_item_id: str) -> str:
    prefix = "roadmap_item."
    if roadmap_item_id.startswith(prefix):
        return f"roadmap.{roadmap_item_id[len(prefix):]}"
    return roadmap_item_id.replace("_", ".")


def _roadmap_dependency_id(
    *,
    roadmap_item_id: str,
    depends_on_roadmap_item_id: str,
    dependency_kind: str,
) -> str:
    return (
        f"roadmap_item_dependency."
        f"{roadmap_item_id.replace('_', '.').replace(':', '.').replace('/', '.')}"
        f".{dependency_kind}."
        f"{depends_on_roadmap_item_id.replace('_', '.').replace(':', '.').replace('/', '.')}"
    )


def _parse_phase_order(value: object) -> tuple[int, ...] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    tokens = value.strip().split(".")
    parsed: list[int] = []
    for token in tokens:
        if not token.isdigit():
            return None
        parsed.append(int(token))
    return tuple(parsed) if parsed else None


def _format_phase_order(parts: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in parts)


def _next_phase_order(existing_values: tuple[str, ...]) -> str:
    parsed_values = [parsed for value in existing_values if (parsed := _parse_phase_order(value)) is not None]
    if not parsed_values:
        return "1"
    best = max(parsed_values)
    if len(best) == 1:
        return _format_phase_order((best[0] + 1,))
    return _format_phase_order((*best[:-1], best[-1] + 1))


def _default_approval_tag(now: datetime) -> str:
    return f"operator-write-{now.astimezone(timezone.utc).date().isoformat()}"


def _default_decision_ref(slug: str, now: datetime) -> str:
    return f"decision.{now.astimezone(timezone.utc).date().isoformat()}.{slug}"


def _acceptance_payload(
    *,
    tier: str,
    phase_ready: bool,
    approval_tag: str,
    outcome_gate: str,
    phase_order: str,
    reference_doc: str | None,
    must_have: tuple[str, ...],
    proof_kind: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "tier": tier,
        "must_have": list(must_have),
        "phase_ready": phase_ready,
        "approval_tag": approval_tag,
        "outcome_gate": outcome_gate,
        "phase_order": phase_order,
    }
    if reference_doc:
        payload["reference_doc"] = reference_doc
    if proof_kind:
        payload["proof_kind"] = proof_kind
    return payload


@dataclass(frozen=True, slots=True)
class _RoadmapTemplateChild:
    suffix: str
    title: str
    priority: str
    summary: str
    must_have: tuple[str, ...]


_ROADMAP_TEMPLATE_CHILDREN: dict[str, tuple[_RoadmapTemplateChild, ...]] = {
    "single_capability": (),
    "hard_cutover_program": (
        _RoadmapTemplateChild(
            suffix="contracts",
            title="Canonical authoring contract and template pack",
            priority="p1",
            summary="Define the typed authoring contract, template library, and normalization rules so roadmap writes stop requiring hand-built rows.",
            must_have=(
                "Define the typed roadmap authoring contract.",
                "Ship reusable template definitions for common roadmap package shapes.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="validation_gate",
            title="Shared validation and normalization gate",
            priority="p1",
            summary="Move roadmap authoring through one preview-first validation gate that auto-fixes deterministic issues and blocks only on unsafe ambiguity.",
            must_have=(
                "Preview, validate, and commit all run through one shared gate.",
                "Ids, keys, and dependency ids are generated automatically when safe.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="frontdoors",
            title="CLI and MCP operator-write front doors",
            priority="p1",
            summary="Expose the shared gate through native-operator CLI and MCP so roadmap authoring stops depending on raw SQL or one-off scripts.",
            must_have=(
                "CLI and MCP call the same write service.",
                "Preview output is identical across both front doors.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="derived_views",
            title="Derived views and roadmap export cleanup",
            priority="p2",
            summary="Make roadmap markdown and operator views derived from DB-backed authority so authoring stays single-source.",
            must_have=(
                "Roadmap exports derive from DB-backed rows.",
                "No parallel markdown-only roadmap authority remains.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="proof",
            title="Validation proof and operator adoption",
            priority="p2",
            summary="Prove the gate is safe through preview parity, transaction safety, and representative roadmap authoring scenarios.",
            must_have=(
                "Representative roadmap authoring scenarios are covered by tests.",
                "Commit only occurs after the shared validation gate passes cleanly.",
            ),
        ),
    ),
    "data_dictionary_impact_program": (
        _RoadmapTemplateChild(
            suffix="contract",
            title="Shared data-dictionary contract and CQRS query seam",
            priority="p1",
            summary=(
                "Persist a canonical dictionary payload contract in the DB authority so "
                "all downstream tools consume one normalized schema and dependency graph "
                "shape, including lifecycle metadata and freshness."
            ),
            must_have=(
                "Define a shared dictionary payload contract (schema, summary, columns, dependencies).",
                "Guarantee schema and relationship payloads are returned through a single CQRS query path.",
                "Publish a backward-compatible contract version so existing and new tools can parse safely.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="scenario_composer",
            title="1+1=3 scenario composer across tools",
            priority="p1",
            summary=(
                "Build a deterministic scenario composer that fuses dictionary lineage with "
                "bug history, workflow-class pressure, and roadmap intent to produce emergent "
                "execution ideas."
            ),
            must_have=(
                "Generate at least three novel scenario candidates per trigger set from dictionary signals.",
                "Each scenario includes a root cause hypothesis, impacted query path, and proposed action.",
                "Persist scenario outputs as durable records linked to dictionary refresh receipts.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="impact_scoring",
            title="Impact scoring and prioritization for dictionary-driven scenarios",
            priority="p1",
            summary=(
                "Create a deterministic scoring model so generated scenarios are prioritized by "
                "blast radius and confidence instead of manual guesswork."
            ),
            must_have=(
                "Score scenarios across dependency breadth, incident history, and query complexity.",
                "Persist scoring components in audit fields for explainability and rerun determinism.",
                "Require top-scoring scenarios to be surfaced to operator-write and review workflows.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="tool_loop",
            title="Wire dictionary outputs into existing tools (queries, runbooks, roadmap)",
            priority="p1",
            summary=(
                "Add tool-level loop so dictionary output is consumed by MCP and CLI surfaces "
                "to propose roadmap items, runbooks, and scenario previews automatically."
            ),
            must_have=(
                "Update existing query surfaces to attach related scenario suggestions when dictionary depth threshold is met.",
                "Add a low-friction path from scenario output to roadmap creation payloads.",
                "Preserve a preview-first UX so all auto-generated changes are inspectable before commit.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="validation_gate",
            title="Validation, evidence, and auto-generation guardrails",
            priority="p2",
            summary=(
                "Add validation guards that only generate auto-scenarios when dictionary freshness, "
                "ownership, and acceptance criteria thresholds are met."
            ),
            must_have=(
                "Block auto-generation if dictionary freshness is stale or required metadata is missing.",
                "Attach generated scenarios to test-generation and schema-refresh tasks as acceptance criteria.",
                "Emit clear warnings for ambiguous relationships and unresolved table matches.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="observability",
            title="Observability and auditability for dictionary-to-scenario lineage",
            priority="p2",
            summary=(
                "Instrument receipts and audit rows that prove each scenario was generated from a "
                "specific dictionary snapshot and was then transformed into one or more roadmap rows."
            ),
            must_have=(
                "Record generation receipts for each scenario with source table set and snapshot timestamp.",
                "Create a roadmap query view that shows scenario lineage by dictionary version and run.",
                "Add a troubleshooting endpoint that proves why a scenario was skipped, gated, or committed.",
            ),
        ),
    ),
}


def _require_roadmap_template(value: object | None) -> str:
    if value is None:
        return "single_capability"
    normalized = _require_text(value, field_name="template").lower()
    if normalized not in _ROADMAP_TEMPLATE_CHILDREN:
        allowed = ", ".join(sorted(_ROADMAP_TEMPLATE_CHILDREN))
        raise ValueError(f"template must be one of {allowed}")
    return normalized


def _roadmap_item_payload(
    *,
    roadmap_item_id: str,
    roadmap_key: str,
    title: str,
    item_kind: str,
    status: str,
    lifecycle: str,
    priority: str,
    parent_roadmap_item_id: str | None,
    source_bug_id: str | None,
    source_idea_id: str | None,
    registry_paths: tuple[str, ...],
    summary: str,
    acceptance_criteria: Mapping[str, Any],
    decision_ref: str,
    created_at: datetime,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "roadmap_item_id": roadmap_item_id,
        "roadmap_key": roadmap_key,
        "title": title,
        "item_kind": item_kind,
        "status": status,
        "lifecycle": lifecycle,
        "priority": priority,
        "parent_roadmap_item_id": parent_roadmap_item_id,
        "source_bug_id": source_bug_id,
        "source_idea_id": source_idea_id,
        "registry_paths": list(registry_paths),
        "summary": summary,
        "acceptance_criteria": _json_compatible(acceptance_criteria),
        "decision_ref": decision_ref,
        "target_start_at": None,
        "target_end_at": None,
        "completed_at": None,
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
    }


def _roadmap_dependency_payload(
    *,
    roadmap_item_dependency_id: str,
    roadmap_item_id: str,
    depends_on_roadmap_item_id: str,
    dependency_kind: str,
    decision_ref: str,
    created_at: datetime,
) -> dict[str, Any]:
    return {
        "roadmap_item_dependency_id": roadmap_item_dependency_id,
        "roadmap_item_id": roadmap_item_id,
        "depends_on_roadmap_item_id": depends_on_roadmap_item_id,
        "dependency_kind": dependency_kind,
        "decision_ref": decision_ref,
        "created_at": created_at.isoformat(),
    }


def _task_route_scope_label(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
) -> str:
    if task_type is None and model_slug is None:
        return f"provider={provider_slug}"
    if task_type is None:
        return f"provider={provider_slug} model={model_slug}"
    if model_slug is None:
        return f"provider={provider_slug} task_type={task_type}"
    return f"provider={provider_slug} task_type={task_type} model={model_slug}"


def _task_route_eligibility_id(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_from: datetime,
) -> str:
    timestamp = effective_from.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        "task-route-eligibility."
        f"{_scope_fragment(provider_slug, fallback='provider')}"
        f".{_scope_fragment(task_type, fallback='any-task')}"
        f".{_scope_fragment(model_slug, fallback='any-model')}"
        f".{eligibility_status}.{timestamp}"
    )


def _task_route_decision_ref(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_from: datetime,
) -> str:
    timestamp = effective_from.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        "decision:task-route-eligibility:"
        f"{_scope_fragment(provider_slug, fallback='provider')}:"
        f"{_scope_fragment(task_type, fallback='any-task')}:"
        f"{_scope_fragment(model_slug, fallback='any-model')}:"
        f"{eligibility_status}:{timestamp}"
    )


def _default_task_route_rationale(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_to: datetime | None,
) -> str:
    action = "enabled" if eligibility_status == "eligible" else "disabled"
    until = (
        ""
        if effective_to is None
        else f" until {effective_to.astimezone(timezone.utc).isoformat()}"
    )
    return f"Operator {action} route scope {_task_route_scope_label(provider_slug=provider_slug, task_type=task_type, model_slug=model_slug)}{until}"


def _normalize_circuit_breaker_override_state(value: object) -> str:
    return coerce_choice(
        value,
        field_name="override_state",
        choices=_CIRCUIT_BREAKER_OVERRIDE_STATES,
    )


def _circuit_breaker_scope_label(provider_slug: str) -> str:
    return _scope_fragment(provider_slug, fallback="provider")


def _circuit_breaker_decision_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _circuit_breaker_operator_decision_id(
    provider_slug: str,
    *,
    override_state: str,
    effective_from: datetime,
) -> str:
    return (
        "operator-decision.circuit-breaker."
        f"{_circuit_breaker_scope_label(provider_slug)}."
        f"{override_state}."
        f"{_circuit_breaker_decision_timestamp(effective_from)}"
    )


def _circuit_breaker_decision_key(
    provider_slug: str,
    *,
    effective_from: datetime,
) -> str:
    return (
        "circuit-breaker::"
        f"{_circuit_breaker_scope_label(provider_slug)}::"
        f"{_circuit_breaker_decision_timestamp(effective_from)}"
    )


def _circuit_breaker_decision_kind(override_state: str) -> str:
    return {
        "open": "circuit_breaker_force_open",
        "closed": "circuit_breaker_force_closed",
        "reset": "circuit_breaker_reset",
    }[override_state]


def _circuit_breaker_override_title(provider_slug: str, override_state: str) -> str:
    action = {
        "open": "Force open",
        "closed": "Force closed",
        "reset": "Reset",
    }[override_state]
    return f"{action} circuit breaker for {provider_slug}"


def _default_circuit_breaker_rationale(
    *,
    provider_slug: str,
    override_state: str,
    effective_to: datetime | None,
) -> str:
    until = (
        ""
        if effective_to is None
        else f" until {effective_to.astimezone(timezone.utc).isoformat()}"
    )
    if override_state == "open":
        return f"Operator forced circuit breaker OPEN for {provider_slug}{until}"
    if override_state == "closed":
        return f"Operator forced circuit breaker CLOSED for {provider_slug}{until}"
    return f"Operator cleared manual circuit breaker override for {provider_slug}"


def _normalize_authority_domain_scope_ref(value: object) -> str:
    return coerce_slug(
        value,
        field_name="authority_domain",
        separator="_",
    )


def _normalize_architecture_policy_slug(value: object) -> str:
    return coerce_slug(
        value,
        field_name="policy_slug",
        separator="-",
    )


def _architecture_policy_operator_decision_id(
    authority_domain: str,
    policy_slug: str,
) -> str:
    return (
        "operator_decision.architecture_policy."
        f"{authority_domain}."
        f"{policy_slug.replace('-', '_')}"
    )


def _architecture_policy_decision_key(
    authority_domain: str,
    policy_slug: str,
) -> str:
    return (
        "architecture-policy::"
        f"{_scope_fragment(authority_domain, fallback='authority-domain')}::"
        f"{policy_slug}"
    )


@dataclass(frozen=True, slots=True)
class ArchitecturePolicyDecisionRecord:
    operator_decision_id: str
    decision_key: str
    authority_domain: str
    policy_slug: str
    decision_kind: str
    decision_status: str
    title: str
    rationale: str
    decided_by: str
    decision_source: str
    effective_from: datetime
    effective_to: datetime | None
    decided_at: datetime
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "operator_decision_id": self.operator_decision_id,
            "decision_key": self.decision_key,
            "authority_domain": self.authority_domain,
            "policy_slug": self.policy_slug,
            "decision_kind": self.decision_kind,
            "decision_status": self.decision_status,
            "title": self.title,
            "rationale": self.rationale,
            "decided_by": self.decided_by,
            "decision_source": self.decision_source,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "decided_at": self.decided_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def _architecture_policy_record_from_decision(
    decision: OperatorDecisionAuthorityRecord,
) -> ArchitecturePolicyDecisionRecord:
    parts = decision.decision_key.split("::")
    policy_slug = parts[2] if len(parts) >= 3 and parts[2] else decision.decision_key
    authority_domain = decision.decision_scope_ref or (
        parts[1].replace("-", "_") if len(parts) >= 2 else decision.decision_key
    )
    return ArchitecturePolicyDecisionRecord(
        operator_decision_id=decision.operator_decision_id,
        decision_key=decision.decision_key,
        authority_domain=authority_domain,
        policy_slug=policy_slug,
        decision_kind=decision.decision_kind,
        decision_status=decision.decision_status,
        title=decision.title,
        rationale=decision.rationale,
        decided_by=decision.decided_by,
        decision_source=decision.decision_source,
        effective_from=decision.effective_from,
        effective_to=decision.effective_to,
        decided_at=decision.decided_at,
        created_at=decision.created_at,
        updated_at=decision.updated_at,
    )


@dataclass(frozen=True, slots=True)
class CircuitBreakerOverrideRecord:
    operator_decision_id: str
    decision_key: str
    provider_slug: str
    override_state: str
    decision_kind: str
    decision_status: str
    title: str
    rationale: str
    decided_by: str
    decision_source: str
    effective_from: datetime
    effective_to: datetime | None
    decided_at: datetime
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "operator_decision_id": self.operator_decision_id,
            "decision_key": self.decision_key,
            "provider_slug": self.provider_slug,
            "override_state": self.override_state,
            "decision_kind": self.decision_kind,
            "decision_status": self.decision_status,
            "title": self.title,
            "rationale": self.rationale,
            "decided_by": self.decided_by,
            "decision_source": self.decision_source,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "decided_at": self.decided_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def _circuit_breaker_override_record_from_decision(
    decision: OperatorDecisionAuthorityRecord,
) -> CircuitBreakerOverrideRecord:
    prefix = "circuit-breaker::"
    provider_slug = decision.decision_scope_ref or decision.decision_key
    if decision.decision_scope_ref is None and decision.decision_key.startswith(prefix):
        suffix = decision.decision_key[len(prefix):]
        provider_slug = suffix.split("::", 1)[0]
    override_state = {
        "circuit_breaker_force_open": "open",
        "circuit_breaker_force_closed": "closed",
        "circuit_breaker_reset": "reset",
    }.get(decision.decision_kind, decision.decision_kind)
    return CircuitBreakerOverrideRecord(
        operator_decision_id=decision.operator_decision_id,
        decision_key=decision.decision_key,
        provider_slug=provider_slug,
        override_state=override_state,
        decision_kind=decision.decision_kind,
        decision_status=decision.decision_status,
        title=decision.title,
        rationale=decision.rationale,
        decided_by=decision.decided_by,
        decision_source=decision.decision_source,
        effective_from=decision.effective_from,
        effective_to=decision.effective_to,
        decided_at=decision.decided_at,
        created_at=decision.created_at,
        updated_at=decision.updated_at,
    )


@dataclass(frozen=True, slots=True)
class TaskRouteEligibilityRecord:
    task_route_eligibility_id: str
    task_type: str | None
    provider_slug: str
    model_slug: str | None
    eligibility_status: str
    reason_code: str
    rationale: str
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "task_route_eligibility_id": self.task_route_eligibility_id,
            "task_type": self.task_type,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "eligibility_status": self.eligibility_status,
            "reason_code": self.reason_code,
            "rationale": self.rationale,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "decision_ref": self.decision_ref,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class TaskRouteEligibilityWriteResult:
    task_route_eligibility: TaskRouteEligibilityRecord
    superseded_task_route_eligibility_ids: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "task_route_eligibility": self.task_route_eligibility.to_json(),
            "superseded_task_route_eligibility_ids": list(
                self.superseded_task_route_eligibility_ids
            ),
        }


def _closeout_resolution_summary(*, bug_id: str, evidence_count: int) -> str:
    noun = "evidence row" if evidence_count == 1 else "evidence rows"
    return (
        "Auto-closed by work-item closeout reconciler from explicit "
        f"{_BUG_CLOSEOUT_EVIDENCE_ROLE} proof ({evidence_count} {noun}) for {bug_id}."
    )


def _closeout_verification_passed(status: object) -> bool:
    return str(status or "").strip().lower() in _BUG_CLOSEOUT_VERIFICATION_SUCCESS_STATUSES


def _binding_status_supports_pipeline(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized not in {"inactive", "closed", "completed", "superseded", "cancelled"}


def _auto_promoted_bug_roadmap_item_id(bug_id: str) -> str:
    suffix = _scope_fragment(bug_id, fallback="bug").replace("-", ".")
    return f"roadmap_item.auto_bug.{suffix}"


def _auto_promoted_bug_priority(severity: str | None) -> str:
    normalized = (severity or "").strip().upper()
    if normalized in {"P0", "P1", "HIGH", "CRITICAL"}:
        return "p1"
    return "p2"


def _issue_id_from_slug(slug: str) -> str:
    return f"issue.{slug}"


def _issue_key_from_issue_id(issue_id: str) -> str:
    prefix = "issue."
    if issue_id.startswith(prefix):
        return f"issue.{issue_id[len(prefix):]}"
    return issue_id.replace("_", ".")


def _idea_id_from_slug(slug: str) -> str:
    return f"operator_idea.{slug}"


def _idea_key_from_idea_id(idea_id: str) -> str:
    prefix = "operator_idea."
    if idea_id.startswith(prefix):
        return f"idea.{idea_id[len(prefix):]}"
    return idea_id.replace("_", ".")


def _idea_promotion_id(*, idea_id: str, roadmap_item_id: str) -> str:
    return (
        "operator_idea_promotion."
        f"{_scope_fragment(idea_id, fallback='idea')}."
        f"{_scope_fragment(roadmap_item_id, fallback='roadmap')}"
    )


def _task_route_eligibility_record_from_row(row: Mapping[str, Any]) -> TaskRouteEligibilityRecord:
    return TaskRouteEligibilityRecord(
        task_route_eligibility_id=str(row["task_route_eligibility_id"]),
        task_type=str(row["task_type"]) if row["task_type"] is not None else None,
        provider_slug=str(row["provider_slug"]),
        model_slug=str(row["model_slug"]) if row["model_slug"] is not None else None,
        eligibility_status=str(row["eligibility_status"]),
        reason_code=str(row["reason_code"]),
        rationale=str(row["rationale"]),
        effective_from=row["effective_from"],
        effective_to=row["effective_to"],
        decision_ref=str(row["decision_ref"]),
        created_at=row["created_at"],
    )


@dataclass(slots=True)
class OperatorControlFrontdoor:
    """Repo-local operator surface for bounded operator-control writes."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    operator_control_repository_factory: Callable[[
        _Connection,
    ], PostgresOperatorControlRepository] | None = None
    task_route_eligibility_repository_factory: Callable[[
        _Connection,
    ], Any] | None = None
    roadmap_repository_factory: Callable[[
        _Connection,
    ], Any] | None = None
    operator_idea_repository_factory: Callable[[
        _Connection,
    ], PostgresOperatorIdeaRepository] | None = None
    object_relation_repository_factory: Callable[[
        _Connection,
    ], OperatorObjectRelationRepository] | None = None
    semantic_assertion_repository_factory: Callable[[
        _Connection,
    ], SemanticAssertionRepository] | None = None
    binding_repository_factory: Callable[[
        _Connection,
    ], WorkItemWorkflowBindingRepository] | None = None
    native_primary_cutover_repository_factory: Callable[[
        _Connection,
    ], NativePrimaryCutoverRepository] | None = None
    work_item_closeout_repository_factory: Callable[[
        _Connection,
    ], PostgresWorkItemCloseoutRepository] | None = None

    def __post_init__(self) -> None:
        if self.operator_control_repository_factory is None:
            self.operator_control_repository_factory = (
                self._default_operator_control_repository_factory
            )
        if self.task_route_eligibility_repository_factory is None:
            self.task_route_eligibility_repository_factory = (
                self._default_task_route_eligibility_repository_factory
            )
        if self.roadmap_repository_factory is None:
            self.roadmap_repository_factory = self._default_roadmap_repository_factory
        if self.operator_idea_repository_factory is None:
            self.operator_idea_repository_factory = (
                self._default_operator_idea_repository_factory
            )
        if self.object_relation_repository_factory is None:
            self.object_relation_repository_factory = (
                self._default_object_relation_repository_factory
            )
        if self.semantic_assertion_repository_factory is None:
            self.semantic_assertion_repository_factory = (
                self._default_semantic_assertion_repository_factory
            )
        if self.binding_repository_factory is None:
            self.binding_repository_factory = self._default_binding_repository_factory
        if self.native_primary_cutover_repository_factory is None:
            self.native_primary_cutover_repository_factory = (
                self._default_native_primary_cutover_repository_factory
            )
        if self.work_item_closeout_repository_factory is None:
            self.work_item_closeout_repository_factory = (
                self._default_work_item_closeout_repository_factory
            )

    @staticmethod
    def _default_operator_control_repository_factory(
        conn: _Connection,
    ) -> PostgresOperatorControlRepository:
        return PostgresOperatorControlRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_task_route_eligibility_repository_factory(
        conn: _Connection,
    ) -> Any:
        return PostgresTaskRouteEligibilityRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_roadmap_repository_factory(
        conn: _Connection,
    ) -> Any:
        return PostgresRoadmapAuthoringRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_operator_idea_repository_factory(
        conn: _Connection,
    ) -> PostgresOperatorIdeaRepository:
        return PostgresOperatorIdeaRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_object_relation_repository_factory(
        conn: _Connection,
    ) -> OperatorObjectRelationRepository:
        return PostgresOperatorObjectRelationRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_semantic_assertion_repository_factory(
        conn: _Connection,
    ) -> SemanticAssertionRepository:
        return PostgresSemanticAssertionRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_binding_repository_factory(
        conn: _Connection,
    ) -> WorkItemWorkflowBindingRepository:
        return PostgresWorkItemWorkflowBindingRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_native_primary_cutover_repository_factory(
        conn: _Connection,
    ) -> NativePrimaryCutoverRepository:
        return PostgresNativePrimaryCutoverRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_work_item_closeout_repository_factory(
        conn: _Connection,
    ) -> PostgresWorkItemCloseoutRepository:
        return PostgresWorkItemCloseoutRepository(conn)  # type: ignore[arg-type]

    async def _object_ref_exists(
        self,
        conn: _Connection,
        *,
        object_kind: str,
        object_ref: str,
    ) -> bool:
        normalized_kind = _normalize_object_kind(object_kind, field_name="object_kind")
        normalized_ref = (
            _normalize_functional_area_ref(object_ref, field_name="object_ref")
            if normalized_kind == "functional_area"
            else _require_text(object_ref, field_name="object_ref")
        )
        if normalized_kind == "repo_path":
            return True
        query_by_kind = {
            "issue": "SELECT 1 FROM issues WHERE issue_id = $1",
            "bug": "SELECT 1 FROM bugs WHERE bug_id = $1",
            "roadmap_item": "SELECT 1 FROM roadmap_items WHERE roadmap_item_id = $1",
            "operator_decision": "SELECT 1 FROM operator_decisions WHERE operator_decision_id = $1",
            "cutover_gate": "SELECT 1 FROM cutover_gates WHERE cutover_gate_id = $1",
            "workflow_class": "SELECT 1 FROM workflow_classes WHERE workflow_class_id = $1",
            "schedule_definition": "SELECT 1 FROM schedule_definitions WHERE schedule_definition_id = $1",
            "workflow_run": "SELECT 1 FROM workflow_runs WHERE run_id = $1",
            "document": (
                "SELECT 1 FROM memory_entities "
                "WHERE id = $1 AND entity_type = 'document' AND archived = false"
            ),
            "functional_area": "SELECT 1 FROM functional_areas WHERE functional_area_id = $1",
        }
        query = query_by_kind.get(normalized_kind)
        if query is None:
            raise ValueError(f"unsupported object_kind for relation lookup: {normalized_kind}")
        row = await conn.fetchrow(query, normalized_ref)
        return row is not None

    async def _require_object_ref_exists(
        self,
        conn: _Connection,
        *,
        object_kind: str,
        object_ref: str,
        field_name: str,
    ) -> str:
        normalized_kind = _normalize_object_kind(object_kind, field_name=f"{field_name}.kind")
        normalized_ref = (
            _normalize_functional_area_ref(object_ref, field_name=f"{field_name}.ref")
            if normalized_kind == "functional_area"
            else _require_text(object_ref, field_name=f"{field_name}.ref")
        )
        if not await self._object_ref_exists(
            conn,
            object_kind=normalized_kind,
            object_ref=normalized_ref,
        ):
            raise ValueError(
                f"{field_name}.ref does not resolve to a canonical {normalized_kind} row: {normalized_ref}"
            )
        return normalized_ref

    async def _record_functional_area(
        self,
        *,
        env: Mapping[str, str] | None,
        area_slug: str,
        title: str,
        summary: str,
        area_status: str = "active",
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> FunctionalAreaRecord:
        normalized_area_slug = coerce_slug(
            area_slug,
            field_name="area_slug",
            separator="-",
        )
        normalized_title = _require_text(title, field_name="title")
        normalized_summary = _require_text(summary, field_name="summary")
        normalized_status = _normalize_functional_area_status(area_status)
        now = _now()
        normalized_created_at = (
            now
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_created_at",
            )
        )
        normalized_updated_at = (
            normalized_created_at
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_updated_at",
            )
        )
        functional_area = FunctionalAreaRecord(
            functional_area_id=functional_area_id_from_slug(normalized_area_slug),
            area_slug=normalized_area_slug,
            title=normalized_title,
            area_status=normalized_status,
            summary=normalized_summary,
            created_at=normalized_created_at,
            updated_at=normalized_updated_at,
        )
        conn = await self.connect_database(env)
        try:
            assert self.object_relation_repository_factory is not None
            repository = self.object_relation_repository_factory(conn)
            return await repository.record_functional_area(functional_area=functional_area)
        finally:
            await conn.close()

    async def _record_operator_object_relation(
        self,
        *,
        env: Mapping[str, str] | None,
        relation_kind: str,
        source_kind: str,
        source_ref: str,
        target_kind: str,
        target_ref: str,
        relation_status: str = "active",
        relation_metadata: Mapping[str, Any] | None = None,
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> OperatorObjectRelationRecord:
        normalized_relation_kind = _normalize_relation_kind(relation_kind)
        normalized_source_kind = _normalize_object_kind(source_kind, field_name="source_kind")
        normalized_target_kind = _normalize_object_kind(target_kind, field_name="target_kind")
        normalized_status = _normalize_object_relation_status(relation_status)
        normalized_relation_metadata = (
            {}
            if relation_metadata is None
            else _json_compatible(
                _require_mapping(
                    relation_metadata,
                    field_name="relation_metadata",
                )
            )
        )
        now = _now()
        normalized_created_at = (
            now
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_created_at",
            )
        )
        normalized_updated_at = (
            normalized_created_at
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_updated_at",
            )
        )
        conn = await self.connect_database(env)
        try:
            normalized_source_ref = (
                await self._require_object_ref_exists(
                    conn,
                    object_kind=normalized_source_kind,
                    object_ref=source_ref,
                    field_name="source",
                )
                if normalized_source_kind != "repo_path"
                else _require_text(source_ref, field_name="source.ref")
            )
            normalized_target_ref = (
                await self._require_object_ref_exists(
                    conn,
                    object_kind=normalized_target_kind,
                    object_ref=target_ref,
                    field_name="target",
                )
                if normalized_target_kind != "repo_path"
                else _require_text(target_ref, field_name="target.ref")
            )
            normalized_bound_by_decision_id = None
            if bound_by_decision_id is not None:
                normalized_bound_by_decision_id = await self._require_object_ref_exists(
                    conn,
                    object_kind="operator_decision",
                    object_ref=bound_by_decision_id,
                    field_name="bound_by_decision",
                )
            assert self.object_relation_repository_factory is not None
            repository = self.object_relation_repository_factory(conn)
            relation = OperatorObjectRelationRecord(
                operator_object_relation_id=operator_object_relation_id(
                    relation_kind=normalized_relation_kind,
                    source_kind=normalized_source_kind,
                    source_ref=normalized_source_ref,
                    target_kind=normalized_target_kind,
                    target_ref=normalized_target_ref,
                ),
                relation_kind=normalized_relation_kind,
                relation_status=normalized_status,
                source_kind=normalized_source_kind,
                source_ref=normalized_source_ref,
                target_kind=normalized_target_kind,
                target_ref=normalized_target_ref,
                relation_metadata=cast(Mapping[str, Any], normalized_relation_metadata),
                bound_by_decision_id=normalized_bound_by_decision_id,
                created_at=normalized_created_at,
                updated_at=normalized_updated_at,
            )
            async with conn.transaction():
                persisted_relation = await repository.record_relation(relation=relation)
                await self._sync_semantic_bridge_for_relation(
                    conn,
                    relation=persisted_relation,
                )
            return persisted_relation
        finally:
            await conn.close()

    async def _ensure_semantic_bridge_predicate_for_relation(
        self,
        conn: _Connection,
        *,
        repository: SemanticAssertionRepository,
        relation: OperatorObjectRelationRecord,
    ) -> SemanticPredicateRecord:
        expected_predicate = SemanticPredicateRecord(
            predicate_slug=relation.relation_kind,
            predicate_status="active",
            subject_kind_allowlist=_OBJECT_RELATION_SEMANTIC_PREDICATE_ALLOWLIST,
            object_kind_allowlist=_OBJECT_RELATION_SEMANTIC_PREDICATE_ALLOWLIST,
            cardinality_mode=_OBJECT_RELATION_SEMANTIC_CARDINALITY_MODE,
            description=(
                "Auto-registered bridge predicate mirroring "
                f"operator_object_relations relation_kind '{relation.relation_kind}'."
            ),
            created_at=relation.created_at,
            updated_at=relation.updated_at,
        )
        existing_predicate = await repository.load_predicate(
            predicate_slug=expected_predicate.predicate_slug,
        )
        needs_sync = (
            existing_predicate is None
            or existing_predicate.predicate_status != expected_predicate.predicate_status
            or existing_predicate.subject_kind_allowlist
            != expected_predicate.subject_kind_allowlist
            or existing_predicate.object_kind_allowlist
            != expected_predicate.object_kind_allowlist
            or existing_predicate.cardinality_mode != expected_predicate.cardinality_mode
            or existing_predicate.description != expected_predicate.description
        )
        if not needs_sync:
            return existing_predicate
        persisted_predicate = await repository.upsert_predicate(
            predicate=expected_predicate,
        )
        await aemit(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            event_type="semantic_predicate_registered",
            entity_id=persisted_predicate.predicate_slug,
            entity_kind="semantic_predicate",
            payload={
                "semantic_predicate": persisted_predicate.to_json(),
                "bridge_source": "operator_object_relations",
            },
            emitted_by="operator_write.record_operator_object_relation",
        )
        return persisted_predicate

    @staticmethod
    def _semantic_bridge_assertion_for_relation(
        relation: OperatorObjectRelationRecord,
    ) -> SemanticAssertionRecord:
        qualifiers_json = (
            {"relation_metadata": dict(relation.relation_metadata)}
            if relation.relation_metadata
            else {}
        )
        return normalize_semantic_assertion_record(
            SemanticAssertionRecord(
                semantic_assertion_id="",
                predicate_slug=relation.relation_kind,
                assertion_status="active",
                subject_kind=relation.source_kind,
                subject_ref=relation.source_ref,
                object_kind=relation.target_kind,
                object_ref=relation.target_ref,
                qualifiers_json=qualifiers_json,
                source_kind=_OBJECT_RELATION_SEMANTIC_SOURCE_KIND,
                source_ref=relation.operator_object_relation_id,
                evidence_ref=None,
                bound_decision_id=relation.bound_by_decision_id,
                valid_from=relation.created_at,
                valid_to=None,
                created_at=relation.created_at,
                updated_at=relation.updated_at,
            )
        )

    async def _sync_semantic_bridge_for_relation(
        self,
        conn: _Connection,
        *,
        relation: OperatorObjectRelationRecord,
    ) -> str:
        assert self.semantic_assertion_repository_factory is not None
        repository = self.semantic_assertion_repository_factory(conn)
        predicate = await self._ensure_semantic_bridge_predicate_for_relation(
            conn,
            repository=repository,
            relation=relation,
        )
        bridge_assertion = self._semantic_bridge_assertion_for_relation(relation)
        if relation.relation_status == "inactive":
            existing_assertion = await repository.load_assertion(
                semantic_assertion_id=bridge_assertion.semantic_assertion_id,
            )
            if existing_assertion is None:
                tombstone_assertion = replace(
                    bridge_assertion,
                    assertion_status="retracted",
                    valid_to=relation.updated_at,
                )
                persisted_assertion, superseded_assertions = await repository.record_assertion(
                    assertion=tombstone_assertion,
                    cardinality_mode=predicate.cardinality_mode,
                    as_of=relation.updated_at,
                )
                await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_assertion_recorded",
                    entity_id=persisted_assertion.semantic_assertion_id,
                    entity_kind="semantic_assertion",
                    payload={
                        "semantic_assertion": persisted_assertion.to_json(),
                        "superseded_assertion_ids": [
                            item.semantic_assertion_id for item in superseded_assertions
                        ],
                        "bridge_source": "operator_object_relations",
                        "bridge_state": "inactive_tombstone",
                    },
                    emitted_by="operator_write.record_operator_object_relation",
                )
                return "tombstoned"
            retracted_assertion = await repository.retract_assertion(
                semantic_assertion_id=bridge_assertion.semantic_assertion_id,
                retracted_at=relation.updated_at,
                updated_at=relation.updated_at,
            )
            await aemit(
                conn,
                channel=CHANNEL_SEMANTIC_ASSERTION,
                event_type="semantic_assertion_retracted",
                entity_id=retracted_assertion.semantic_assertion_id,
                entity_kind="semantic_assertion",
                payload={
                    "semantic_assertion": retracted_assertion.to_json(),
                    "bridge_source": "operator_object_relations",
                },
                emitted_by="operator_write.record_operator_object_relation",
            )
            return "retracted"
        persisted_assertion, superseded_assertions = await repository.record_assertion(
            assertion=bridge_assertion,
            cardinality_mode=predicate.cardinality_mode,
            as_of=relation.updated_at,
        )
        await aemit(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            event_type="semantic_assertion_recorded",
            entity_id=persisted_assertion.semantic_assertion_id,
            entity_kind="semantic_assertion",
            payload={
                "semantic_assertion": persisted_assertion.to_json(),
                "superseded_assertion_ids": [
                    item.semantic_assertion_id for item in superseded_assertions
                ],
                "bridge_source": "operator_object_relations",
            },
            emitted_by="operator_write.record_operator_object_relation",
        )
        return "recorded"

    async def _ensure_semantic_bridge_predicate_for_operator_decision(
        self,
        conn: _Connection,
        *,
        repository: SemanticAssertionRepository,
        decision: OperatorDecisionAuthorityRecord,
    ) -> SemanticPredicateRecord | None:
        if decision.decision_scope_kind is None or decision.decision_scope_ref is None:
            return None
        policy = operator_decision_scope_policy(decision_kind=decision.decision_kind)
        if policy.scope_mode == "none":
            return None
        subject_kind_allowlist = (
            policy.allowed_scope_kinds
            if policy.allowed_scope_kinds
            else (decision.decision_scope_kind,)
        )
        expected_predicate = SemanticPredicateRecord(
            predicate_slug=decision.decision_kind,
            predicate_status="active",
            subject_kind_allowlist=subject_kind_allowlist,
            object_kind_allowlist=_DECISION_SEMANTIC_OBJECT_ALLOWLIST,
            cardinality_mode=_DECISION_SEMANTIC_CARDINALITY_MODE,
            description=(
                "Auto-registered bridge predicate mirroring "
                f"operator_decisions decision_kind '{decision.decision_kind}' "
                "onto scoped semantic edges."
            ),
            created_at=decision.created_at,
            updated_at=decision.updated_at,
        )
        existing_predicate = await repository.load_predicate(
            predicate_slug=expected_predicate.predicate_slug,
        )
        needs_sync = (
            existing_predicate is None
            or existing_predicate.predicate_status != expected_predicate.predicate_status
            or existing_predicate.subject_kind_allowlist
            != expected_predicate.subject_kind_allowlist
            or existing_predicate.object_kind_allowlist
            != expected_predicate.object_kind_allowlist
            or existing_predicate.cardinality_mode != expected_predicate.cardinality_mode
            or existing_predicate.description != expected_predicate.description
        )
        if not needs_sync:
            return existing_predicate
        persisted_predicate = await repository.upsert_predicate(
            predicate=expected_predicate,
        )
        await aemit(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            event_type="semantic_predicate_registered",
            entity_id=persisted_predicate.predicate_slug,
            entity_kind="semantic_predicate",
            payload={
                "semantic_predicate": persisted_predicate.to_json(),
                "bridge_source": "operator_decisions",
            },
            emitted_by="operator_write.record_operator_decision",
        )
        return persisted_predicate

    @staticmethod
    def _semantic_bridge_assertion_for_operator_decision(
        decision: OperatorDecisionAuthorityRecord,
    ) -> SemanticAssertionRecord | None:
        if decision.decision_scope_kind is None or decision.decision_scope_ref is None:
            return None
        return normalize_semantic_assertion_record(
            SemanticAssertionRecord(
                semantic_assertion_id="",
                predicate_slug=decision.decision_kind,
                assertion_status="active",
                subject_kind=decision.decision_scope_kind,
                subject_ref=decision.decision_scope_ref,
                object_kind="operator_decision",
                object_ref=decision.operator_decision_id,
                qualifiers_json={
                    "decision_key": decision.decision_key,
                    "decision_source": decision.decision_source,
                    "decision_status": decision.decision_status,
                },
                source_kind=_DECISION_SEMANTIC_SOURCE_KIND,
                source_ref=decision.operator_decision_id,
                evidence_ref=None,
                bound_decision_id=None,
                valid_from=decision.effective_from,
                valid_to=decision.effective_to,
                created_at=decision.created_at,
                updated_at=decision.updated_at,
            )
        )

    async def _sync_semantic_bridge_for_operator_decision(
        self,
        conn: _Connection,
        *,
        decision: OperatorDecisionAuthorityRecord,
        emitted_by: str,
    ) -> str:
        assert self.semantic_assertion_repository_factory is not None
        repository = self.semantic_assertion_repository_factory(conn)
        predicate = await self._ensure_semantic_bridge_predicate_for_operator_decision(
            conn,
            repository=repository,
            decision=decision,
        )
        bridge_assertion = self._semantic_bridge_assertion_for_operator_decision(decision)
        if predicate is None or bridge_assertion is None:
            return "skipped_unscoped"
        persisted_assertion, superseded_assertions = await repository.record_assertion(
            assertion=bridge_assertion,
            cardinality_mode=predicate.cardinality_mode,
            as_of=decision.updated_at,
        )
        await aemit(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            event_type="semantic_assertion_recorded",
            entity_id=persisted_assertion.semantic_assertion_id,
            entity_kind="semantic_assertion",
            payload={
                "semantic_assertion": persisted_assertion.to_json(),
                "superseded_assertion_ids": [
                    item.semantic_assertion_id for item in superseded_assertions
                ],
                "bridge_source": "operator_decisions",
            },
            emitted_by=emitted_by,
        )
        return "recorded"

    async def _ensure_semantic_bridge_predicate_for_roadmap_item(
        self,
        conn: _Connection,
        *,
        repository: SemanticAssertionRepository,
        predicate_slug: str,
        created_at: datetime,
        updated_at: datetime,
    ) -> SemanticPredicateRecord:
        spec = _ROADMAP_SEMANTIC_PREDICATE_SPECS[predicate_slug]
        expected_predicate = SemanticPredicateRecord(
            predicate_slug=predicate_slug,
            predicate_status="active",
            subject_kind_allowlist=_ROADMAP_SEMANTIC_SUBJECT_ALLOWLIST,
            object_kind_allowlist=cast(
                tuple[str, ...],
                spec["object_kind_allowlist"],
            ),
            cardinality_mode=cast(str, spec["cardinality_mode"]),
            description=cast(str, spec["description"]),
            created_at=created_at,
            updated_at=updated_at,
        )
        existing_predicate = await repository.load_predicate(
            predicate_slug=expected_predicate.predicate_slug,
        )
        needs_sync = (
            existing_predicate is None
            or existing_predicate.predicate_status != expected_predicate.predicate_status
            or existing_predicate.subject_kind_allowlist
            != expected_predicate.subject_kind_allowlist
            or existing_predicate.object_kind_allowlist
            != expected_predicate.object_kind_allowlist
            or existing_predicate.cardinality_mode != expected_predicate.cardinality_mode
            or existing_predicate.description != expected_predicate.description
        )
        if not needs_sync:
            return existing_predicate
        persisted_predicate = await repository.upsert_predicate(
            predicate=expected_predicate,
        )
        await aemit(
            conn,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            event_type="semantic_predicate_registered",
            entity_id=persisted_predicate.predicate_slug,
            entity_kind="semantic_predicate",
            payload={
                "semantic_predicate": persisted_predicate.to_json(),
                "bridge_source": "roadmap_items",
            },
            emitted_by="operator_write.roadmap_write",
        )
        return persisted_predicate

    def _roadmap_semantic_bridge_assertions_for_item(
        self,
        *,
        roadmap_item: Mapping[str, Any],
    ) -> tuple[SemanticAssertionRecord, ...]:
        roadmap_item_id = _require_text(
            roadmap_item.get("roadmap_item_id"),
            field_name="roadmap_item_id",
        )
        created_at = _normalize_datetime_like(
            roadmap_item.get("created_at"),
            field_name="created_at",
            reason_code="operator_control.invalid_created_at",
        )
        updated_at = _normalize_datetime_like(
            roadmap_item.get("updated_at") or created_at,
            field_name="updated_at",
            reason_code="operator_control.invalid_updated_at",
        )
        source_bug_id = _optional_text(
            roadmap_item.get("source_bug_id"),
            field_name="source_bug_id",
        )
        source_idea_id = _optional_text(
            roadmap_item.get("source_idea_id"),
            field_name="source_idea_id",
        )
        decision_ref = _optional_text(
            roadmap_item.get("decision_ref"),
            field_name="decision_ref",
        )
        registry_paths = _normalize_registry_paths(
            roadmap_item.get("registry_paths") or (),
        )
        assertions: list[SemanticAssertionRecord] = []
        if source_bug_id is not None:
            assertions.append(
                normalize_semantic_assertion_record(
                    SemanticAssertionRecord(
                        semantic_assertion_id="",
                        predicate_slug="sourced_from_bug",
                        assertion_status="active",
                        subject_kind="roadmap_item",
                        subject_ref=roadmap_item_id,
                        object_kind="bug",
                        object_ref=source_bug_id,
                        qualifiers_json={
                            "bridge_source": "roadmap_items",
                            "source_field": "source_bug_id",
                        },
                        source_kind=_ROADMAP_SEMANTIC_SOURCE_KIND,
                        source_ref=roadmap_item_id,
                        evidence_ref=None,
                        bound_decision_id=None,
                        valid_from=created_at,
                        valid_to=None,
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
            )
        if source_idea_id is not None:
            assertions.append(
                normalize_semantic_assertion_record(
                    SemanticAssertionRecord(
                        semantic_assertion_id="",
                        predicate_slug="sourced_from_idea",
                        assertion_status="active",
                        subject_kind="roadmap_item",
                        subject_ref=roadmap_item_id,
                        object_kind="operator_idea",
                        object_ref=source_idea_id,
                        qualifiers_json={
                            "bridge_source": "roadmap_items",
                            "source_field": "source_idea_id",
                        },
                        source_kind=_ROADMAP_SEMANTIC_SOURCE_KIND,
                        source_ref=roadmap_item_id,
                        evidence_ref=None,
                        bound_decision_id=None,
                        valid_from=created_at,
                        valid_to=None,
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
            )
        if decision_ref is not None:
            assertions.append(
                normalize_semantic_assertion_record(
                    SemanticAssertionRecord(
                        semantic_assertion_id="",
                        predicate_slug="governed_by_decision_ref",
                        assertion_status="active",
                        subject_kind="roadmap_item",
                        subject_ref=roadmap_item_id,
                        object_kind="decision_ref",
                        object_ref=decision_ref,
                        qualifiers_json={
                            "bridge_source": "roadmap_items",
                            "source_field": "decision_ref",
                        },
                        source_kind=_ROADMAP_SEMANTIC_SOURCE_KIND,
                        source_ref=roadmap_item_id,
                        evidence_ref=None,
                        bound_decision_id=None,
                        valid_from=created_at,
                        valid_to=None,
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
            )
        for repo_path in registry_paths:
            assertions.append(
                normalize_semantic_assertion_record(
                    SemanticAssertionRecord(
                        semantic_assertion_id="",
                        predicate_slug="touches_repo_path",
                        assertion_status="active",
                        subject_kind="roadmap_item",
                        subject_ref=roadmap_item_id,
                        object_kind="repo_path",
                        object_ref=repo_path,
                        qualifiers_json={
                            "bridge_source": "roadmap_items",
                            "source_field": "registry_paths",
                        },
                        source_kind=_ROADMAP_SEMANTIC_SOURCE_KIND,
                        source_ref=roadmap_item_id,
                        evidence_ref=None,
                        bound_decision_id=None,
                        valid_from=created_at,
                        valid_to=None,
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
            )
        return tuple(assertions)

    async def _sync_semantic_bridges_for_roadmap_item(
        self,
        conn: _Connection,
        *,
        roadmap_item: Mapping[str, Any],
        emitted_by: str,
    ) -> dict[str, int]:
        assert self.semantic_assertion_repository_factory is not None
        repository = self.semantic_assertion_repository_factory(conn)
        roadmap_item_id = _require_text(
            roadmap_item.get("roadmap_item_id"),
            field_name="roadmap_item_id",
        )
        updated_at = _normalize_datetime_like(
            roadmap_item.get("updated_at"),
            field_name="updated_at",
            reason_code="operator_control.invalid_updated_at",
        )
        desired_assertions = self._roadmap_semantic_bridge_assertions_for_item(
            roadmap_item=roadmap_item,
        )
        summary = {
            "processed": 1,
            "recorded": 0,
            "retracted": 0,
        }
        desired_ids = {
            assertion.semantic_assertion_id for assertion in desired_assertions
        }
        for desired_assertion in desired_assertions:
            predicate = await self._ensure_semantic_bridge_predicate_for_roadmap_item(
                conn,
                repository=repository,
                predicate_slug=desired_assertion.predicate_slug,
                created_at=desired_assertion.created_at,
                updated_at=desired_assertion.updated_at,
            )
            persisted_assertion, superseded_assertions = await repository.record_assertion(
                assertion=desired_assertion,
                cardinality_mode=predicate.cardinality_mode,
                as_of=desired_assertion.updated_at,
            )
            await aemit(
                conn,
                channel=CHANNEL_SEMANTIC_ASSERTION,
                event_type="semantic_assertion_recorded",
                entity_id=persisted_assertion.semantic_assertion_id,
                entity_kind="semantic_assertion",
                payload={
                    "semantic_assertion": persisted_assertion.to_json(),
                    "superseded_assertion_ids": [
                        item.semantic_assertion_id for item in superseded_assertions
                    ],
                    "bridge_source": "roadmap_items",
                    "roadmap_item_id": roadmap_item_id,
                },
                emitted_by=emitted_by,
            )
            summary["recorded"] += 1

        active_assertions = await repository.list_assertions(
            subject_kind="roadmap_item",
            subject_ref=roadmap_item_id,
            source_kind=_ROADMAP_SEMANTIC_SOURCE_KIND,
            source_ref=roadmap_item_id,
            active_at=updated_at,
            active_only=True,
            limit=1000,
        )
        for existing_assertion in active_assertions:
            if (
                existing_assertion.predicate_slug
                not in _ROADMAP_SEMANTIC_PREDICATE_SPECS
                or existing_assertion.semantic_assertion_id in desired_ids
            ):
                continue
            retracted_assertion = await repository.retract_assertion(
                semantic_assertion_id=existing_assertion.semantic_assertion_id,
                retracted_at=updated_at,
                updated_at=updated_at,
            )
            await aemit(
                conn,
                channel=CHANNEL_SEMANTIC_ASSERTION,
                event_type="semantic_assertion_retracted",
                entity_id=retracted_assertion.semantic_assertion_id,
                entity_kind="semantic_assertion",
                payload={
                    "semantic_assertion": retracted_assertion.to_json(),
                    "bridge_source": "roadmap_items",
                    "bridge_state": "stale_removed",
                    "roadmap_item_id": roadmap_item_id,
                },
                emitted_by=emitted_by,
            )
            summary["retracted"] += 1
        return summary

    async def _fetch_roadmap_items_for_semantic_bridge(
        self,
        conn: _Connection,
        *,
        as_of: datetime | None,
    ) -> tuple[Mapping[str, Any], ...]:
        rows = await conn.fetch(
            """
            SELECT
                roadmap_item_id,
                source_bug_id,
                source_idea_id,
                lifecycle,
                registry_paths,
                decision_ref,
                created_at,
                updated_at
            FROM roadmap_items
            WHERE ($1::timestamptz IS NULL OR created_at <= $1)
              AND ($1::timestamptz IS NULL OR updated_at <= $1)
            ORDER BY created_at, updated_at, roadmap_item_id
            """,
            as_of,
        )
        return tuple(cast(Mapping[str, Any], row) for row in rows)

    async def _record_work_item_workflow_binding(
        self,
        *,
        env: Mapping[str, str] | None,
        binding_kind: str,
        issue_id: str | None,
        bug_id: str | None,
        roadmap_item_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        workflow_run_id: str | None,
        binding_status: str,
        bound_by_decision_id: str | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> tuple[
        WorkItemWorkflowBindingRecord,
        dict[str, Any] | None,
        dict[str, Any] | None,
    ]:
        conn = await self.connect_database(env)
        try:
            assert self.binding_repository_factory is not None
            runtime = WorkItemWorkflowBindingRuntime(
                repository=self.binding_repository_factory(conn),
            )
            record = await runtime.record_binding(
                binding_kind=binding_kind,
                issue_id=issue_id,
                bug_id=bug_id,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                workflow_run_id=workflow_run_id,
                binding_status=binding_status,
                bound_by_decision_id=bound_by_decision_id,
                created_at=created_at,
                updated_at=updated_at,
            )
            if (
                roadmap_item_id is not None
                and _binding_status_supports_pipeline(binding_status)
                and (
                    workflow_class_id is not None
                    or schedule_definition_id is not None
                    or workflow_run_id is not None
                )
            ):
                claim_updated_at = (
                    _normalize_as_of(
                        updated_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_updated_at",
                    )
                    if updated_at is not None
                    else _now()
                )
                await self._claim_roadmap_item(
                    conn,
                    roadmap_item_id=roadmap_item_id,
                    updated_at=claim_updated_at,
                )
            auto_promoted_bug: dict[str, Any] | None = None
            auto_promoted_roadmap: dict[str, Any] | None = None
            if issue_id is not None:
                auto_promoted_bug, auto_promoted_roadmap = await self._ensure_issue_promoted_to_bug(
                    conn=conn,
                    runtime=runtime,
                    binding_kind=binding_kind,
                    issue_id=issue_id,
                    workflow_class_id=workflow_class_id,
                    schedule_definition_id=schedule_definition_id,
                    workflow_run_id=workflow_run_id,
                    binding_status=binding_status,
                    bound_by_decision_id=bound_by_decision_id,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            else:
                auto_promoted_roadmap = await self._ensure_bug_promoted_to_roadmap(
                    conn=conn,
                    runtime=runtime,
                    binding_kind=binding_kind,
                    bug_id=bug_id,
                    workflow_class_id=workflow_class_id,
                    schedule_definition_id=schedule_definition_id,
                    workflow_run_id=workflow_run_id,
                    binding_status=binding_status,
                    bound_by_decision_id=bound_by_decision_id,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            return record, auto_promoted_bug, auto_promoted_roadmap
        finally:
            await conn.close()

    async def _admit_native_primary_cutover_gate(
        self,
        *,
        env: Mapping[str, str] | None,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        title: str | None,
        gate_name: str | None,
        gate_policy: Mapping[str, Any] | None,
        required_evidence: Mapping[str, Any] | None,
        decided_at: datetime | None,
        opened_at: datetime | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> NativePrimaryCutoverGateRecord:
        conn = await self.connect_database(env)
        try:
            assert self.native_primary_cutover_repository_factory is not None
            runtime = NativePrimaryCutoverRuntime(
                repository=self.native_primary_cutover_repository_factory(conn),
            )
            async with conn.transaction():
                record = await runtime.admit_gate(
                    decided_by=decided_by,
                    decision_source=decision_source,
                    rationale=rationale,
                    roadmap_item_id=roadmap_item_id,
                    workflow_class_id=workflow_class_id,
                    schedule_definition_id=schedule_definition_id,
                    title=title,
                    gate_name=gate_name,
                    gate_policy=gate_policy,
                    required_evidence=required_evidence,
                    decided_at=decided_at,
                    opened_at=opened_at,
                    created_at=created_at,
                    updated_at=updated_at,
                )
                await self._sync_semantic_bridge_for_operator_decision(
                    conn,
                    decision=OperatorDecisionAuthorityRecord(
                        operator_decision_id=record.decision_id,
                        decision_key=record.decision_key,
                        decision_kind="native_primary_cutover",
                        decision_status=record.decision_status,
                        title=record.title,
                        rationale=record.rationale,
                        decided_by=record.decided_by,
                        decision_source=record.decision_source,
                        effective_from=record.opened_at,
                        effective_to=None,
                        decided_at=record.opened_at,
                        created_at=record.created_at,
                        updated_at=record.updated_at,
                        decision_scope_kind=record.target_kind,
                        decision_scope_ref=record.target_ref,
                    ),
                    emitted_by="operator_write.admit_native_primary_cutover_gate",
                )
            return record
        finally:
            await conn.close()

    async def _set_task_route_eligibility_window(
        self,
        *,
        env: Mapping[str, str] | None,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None,
        task_type: str | None,
        model_slug: str | None,
        reason_code: str,
        rationale: str | None,
        effective_from: datetime | None,
        decision_ref: str | None,
    ) -> TaskRouteEligibilityWriteResult:
        normalized_provider_slug = _require_text(
            provider_slug,
            field_name="provider_slug",
        ).lower()
        normalized_eligibility_status = _normalize_task_route_eligibility_status(
            eligibility_status,
        )
        normalized_task_type = _optional_text(task_type, field_name="task_type")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        normalized_reason_code = _require_text(reason_code, field_name="reason_code")
        normalized_effective_from = (
            _now()
            if effective_from is None
            else _normalize_as_of(
                effective_from,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_from",
            )
        )
        normalized_effective_to = (
            None
            if effective_to is None
            else _normalize_as_of(
                effective_to,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_to",
            )
        )
        if (
            normalized_effective_to is not None
            and normalized_effective_to <= normalized_effective_from
        ):
            raise ValueError("effective_to must be later than effective_from")
        normalized_rationale = (
            _optional_text(rationale, field_name="rationale")
            or _default_task_route_rationale(
                provider_slug=normalized_provider_slug,
                task_type=normalized_task_type,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                effective_to=normalized_effective_to,
            )
        )
        normalized_decision_ref = (
            _optional_text(decision_ref, field_name="decision_ref")
            or _task_route_decision_ref(
                provider_slug=normalized_provider_slug,
                task_type=normalized_task_type,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                effective_from=normalized_effective_from,
            )
        )
        task_route_eligibility_id = _task_route_eligibility_id(
            provider_slug=normalized_provider_slug,
            task_type=normalized_task_type,
            model_slug=normalized_model_slug,
            eligibility_status=normalized_eligibility_status,
            effective_from=normalized_effective_from,
        )

        conn = await self.connect_database(env)
        try:
            assert self.task_route_eligibility_repository_factory is not None
            repository = self.task_route_eligibility_repository_factory(conn)
            inserted_row, superseded_rows = await repository.record_task_route_eligibility_window(
                task_route_eligibility_id=task_route_eligibility_id,
                task_type=normalized_task_type,
                provider_slug=normalized_provider_slug,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                reason_code=normalized_reason_code,
                rationale=normalized_rationale,
                effective_from=normalized_effective_from,
                effective_to=normalized_effective_to,
                decision_ref=normalized_decision_ref,
                )
            if inserted_row is None:
                raise RuntimeError("failed to read inserted task route eligibility row")
            route_cache_key = resolve_workflow_authority_cache_key(env=env)
            await aemit_cache_invalidation(
                conn,
                cache_kind=CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT,
                cache_key=route_cache_key,
                reason="task_route_eligibility_window_write",
                invalidated_by="operator_write.set_task_route_eligibility",
                decision_ref=normalized_decision_ref,
            )
            invalidate_route_authority_cache_key(route_cache_key)
            return TaskRouteEligibilityWriteResult(
                task_route_eligibility=_task_route_eligibility_record_from_row(
                    inserted_row,
                ),
                superseded_task_route_eligibility_ids=tuple(
                    str(row["task_route_eligibility_id"])
                    for row in superseded_rows
                ),
            )
        finally:
            await conn.close()

    async def _fetch_roadmap_item(
        self,
        conn: _Connection,
        *,
        roadmap_item_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                lifecycle,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                source_idea_id,
                registry_paths,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_items
            WHERE roadmap_item_id = $1
            """,
            roadmap_item_id,
        )

    async def _roadmap_item_exists(
        self,
        conn: _Connection,
        *,
        roadmap_item_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT roadmap_item_id FROM roadmap_items WHERE roadmap_item_id = $1",
            roadmap_item_id,
        )
        return row is not None

    async def _claim_roadmap_item(
        self,
        conn: _Connection,
        *,
        roadmap_item_id: str,
        updated_at: datetime,
    ) -> Mapping[str, Any] | None:
        roadmap_item = await self._fetch_roadmap_item(
            conn,
            roadmap_item_id=roadmap_item_id,
        )
        if roadmap_item is None:
            return None
        current_lifecycle = (
            _optional_text(roadmap_item.get("lifecycle"), field_name="lifecycle")
            or _ROADMAP_DEFAULT_LIFECYCLE
        )
        if current_lifecycle in {_ROADMAP_CLAIMED_LIFECYCLE, _ROADMAP_COMPLETED_LIFECYCLE}:
            return roadmap_item
        return await conn.fetchrow(
            """
            UPDATE roadmap_items
            SET lifecycle = $2,
                updated_at = $3
            WHERE roadmap_item_id = $1
            RETURNING
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                lifecycle,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                source_idea_id,
                registry_paths,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            """,
            roadmap_item_id,
            _ROADMAP_CLAIMED_LIFECYCLE,
            updated_at,
        )

    async def _bug_exists(
        self,
        conn: _Connection,
        *,
        bug_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT bug_id FROM bugs WHERE bug_id = $1",
            bug_id,
        )
        return row is not None

    async def _idea_exists(
        self,
        conn: _Connection,
        *,
        idea_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT idea_id FROM operator_ideas WHERE idea_id = $1",
            idea_id,
        )
        return row is not None

    async def _issue_exists(
        self,
        conn: _Connection,
        *,
        issue_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT issue_id FROM issues WHERE issue_id = $1",
            issue_id,
        )
        return row is not None

    async def _fetch_issue_row(
        self,
        conn: _Connection,
        *,
        issue_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                issue_id,
                issue_key,
                title,
                status,
                severity,
                priority,
                summary,
                source_kind,
                discovered_in_run_id,
                discovered_in_receipt_id,
                owner_ref,
                decision_ref,
                resolution_summary,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM issues
            WHERE issue_id = $1
            """,
            issue_id,
        )

    async def _fetch_bug_row(
        self,
        conn: _Connection,
        *,
        bug_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                bug_id,
                title,
                severity,
                summary,
                source_issue_id,
                decision_ref,
                updated_at
            FROM bugs
            WHERE bug_id = $1
            """,
            bug_id,
        )

    async def _fetch_bug_row_by_source_issue_id(
        self,
        conn: _Connection,
        *,
        source_issue_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                bug_id,
                bug_key,
                title,
                status,
                severity,
                priority,
                summary,
                source_kind,
                discovered_in_run_id,
                discovered_in_receipt_id,
                owner_ref,
                source_issue_id,
                decision_ref,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM bugs
            WHERE source_issue_id = $1
            ORDER BY created_at ASC, bug_id ASC
            LIMIT 1
            """,
            source_issue_id,
        )

    async def _fetch_roadmap_item_by_source_bug_id(
        self,
        conn: _Connection,
        *,
        source_bug_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                roadmap_item_id,
                roadmap_key,
                title,
                status,
                lifecycle,
                priority,
                source_bug_id,
                source_idea_id,
                created_at,
                updated_at
            FROM roadmap_items
            WHERE source_bug_id = $1
            ORDER BY created_at ASC, roadmap_item_id ASC
            LIMIT 1
            """,
            source_bug_id,
        )

    async def _record_issue(
        self,
        *,
        env: Mapping[str, str] | None,
        title: str,
        summary: str,
        severity: str,
        priority: str,
        source_kind: str = "manual",
        issue_id: str | None = None,
        issue_key: str | None = None,
        status: str | None = None,
        owner_ref: str | None = None,
        decision_ref: str | None = None,
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        opened_at: datetime | None = None,
        resolved_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> dict[str, Any]:
        normalized_title = _require_text(title, field_name="title")
        normalized_summary = _require_text(summary, field_name="summary")
        normalized_severity = _require_text(severity, field_name="severity")
        normalized_priority = _require_text(priority, field_name="priority")
        normalized_source_kind = _require_text(source_kind, field_name="source_kind")
        normalized_status = (
            "resolved"
            if status is None and resolved_at is not None
            else _normalize_issue_status(status)
        )
        normalized_owner_ref = _optional_text(owner_ref, field_name="owner_ref")
        now = _now()
        normalized_opened_at = (
            now
            if opened_at is None
            else _normalize_as_of(
                opened_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_opened_at",
            )
        )
        normalized_created_at = (
            normalized_opened_at
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_created_at",
            )
        )
        normalized_updated_at = (
            normalized_created_at
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_updated_at",
            )
        )
        normalized_resolved_at = (
            None
            if resolved_at is None
            else _normalize_as_of(
                resolved_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_resolved_at",
            )
        )
        slug = (
            _optional_text(issue_id, field_name="issue_id")
            or _optional_text(issue_key, field_name="issue_key")
            or coerce_slug(normalized_title, field_name="title", separator="-")
        )
        normalized_issue_id = (
            slug
            if slug.startswith("issue.")
            else _issue_id_from_slug(coerce_slug(slug, field_name="issue_slug", separator="-"))
        )
        normalized_issue_key = (
            _optional_text(issue_key, field_name="issue_key")
            or _issue_key_from_issue_id(normalized_issue_id)
        )
        normalized_decision_ref = (
            _optional_text(decision_ref, field_name="decision_ref")
            or _default_decision_ref(
                _scope_fragment(normalized_issue_id, fallback="issue"),
                normalized_created_at,
            )
        )
        conn = await self.connect_database(env)
        try:
            await conn.execute(
                """
                INSERT INTO issues (
                    issue_id,
                    issue_key,
                    title,
                    status,
                    severity,
                    priority,
                    summary,
                    source_kind,
                    discovered_in_run_id,
                    discovered_in_receipt_id,
                    owner_ref,
                    decision_ref,
                    resolution_summary,
                    opened_at,
                    resolved_at,
                    created_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, $13, $14, $15, $16
                )
                ON CONFLICT (issue_id) DO UPDATE SET
                    issue_key = EXCLUDED.issue_key,
                    title = EXCLUDED.title,
                    status = EXCLUDED.status,
                    severity = EXCLUDED.severity,
                    priority = EXCLUDED.priority,
                    summary = EXCLUDED.summary,
                    source_kind = EXCLUDED.source_kind,
                    discovered_in_run_id = EXCLUDED.discovered_in_run_id,
                    discovered_in_receipt_id = EXCLUDED.discovered_in_receipt_id,
                    owner_ref = EXCLUDED.owner_ref,
                    decision_ref = EXCLUDED.decision_ref,
                    opened_at = EXCLUDED.opened_at,
                    resolved_at = EXCLUDED.resolved_at,
                    updated_at = EXCLUDED.updated_at
                """,
                normalized_issue_id,
                normalized_issue_key,
                normalized_title,
                normalized_status,
                normalized_severity,
                normalized_priority,
                normalized_summary,
                normalized_source_kind,
                _optional_text(
                    discovered_in_run_id,
                    field_name="discovered_in_run_id",
                ),
                _optional_text(
                    discovered_in_receipt_id,
                    field_name="discovered_in_receipt_id",
                ),
                normalized_owner_ref,
                normalized_decision_ref,
                normalized_opened_at,
                normalized_resolved_at,
                normalized_created_at,
                normalized_updated_at,
            )
            row = await self._fetch_issue_row(conn, issue_id=normalized_issue_id)
            if row is None:
                raise RuntimeError("failed to read issue row after write")
            return {
                key: (
                    value.isoformat()
                    if isinstance(value, datetime)
                    else value
                )
                for key, value in dict(row).items()
            }
        finally:
            await conn.close()

    async def _operator_ideas(
        self,
        *,
        env: Mapping[str, str] | None,
        action: str | None,
        idea_id: str | None,
        idea_key: str | None,
        title: str | None,
        summary: str | None,
        source_kind: str,
        source_ref: str | None,
        owner_ref: str | None,
        decision_ref: str | None,
        status: str | None,
        resolution_summary: str | None,
        roadmap_item_id: str | None,
        promoted_by: str | None,
        opened_at: datetime | None,
        resolved_at: datetime | None,
        promoted_at: datetime | None,
        created_at: datetime | None,
        updated_at: datetime | None,
        idea_ids: tuple[str, ...],
        open_only: bool,
        limit: int,
    ) -> dict[str, Any]:
        normalized_action = _normalize_operator_idea_action(action)
        now = _now()
        conn = await self.connect_database(env)
        try:
            assert self.operator_idea_repository_factory is not None
            repository = self.operator_idea_repository_factory(conn)
            if normalized_action == "list":
                normalized_status = _normalize_operator_idea_status(status)
                ideas = await repository.list_ideas(
                    idea_ids=idea_ids or None,
                    status=normalized_status,
                    open_only=bool(open_only) and normalized_status is None,
                    limit=max(1, int(limit)),
                )
                return {
                    "kind": "operator_ideas",
                    "action": "list",
                    "query": {
                        "idea_ids": list(idea_ids),
                        "status": normalized_status,
                        "open_only": bool(open_only) and normalized_status is None,
                        "limit": max(1, int(limit)),
                    },
                    "count": len(ideas),
                    "ideas": list(ideas),
                }

            if normalized_action == "file":
                normalized_title = _require_text(title, field_name="title")
                normalized_summary = _require_text(summary, field_name="summary")
                slug_source = (
                    _optional_text(idea_id, field_name="idea_id")
                    or _optional_text(idea_key, field_name="idea_key")
                    or coerce_slug(normalized_title, field_name="title", separator="-")
                )
                if slug_source.startswith("idea."):
                    slug_source = slug_source[len("idea.") :]
                normalized_idea_id = (
                    slug_source
                    if slug_source.startswith("operator_idea.")
                    else _idea_id_from_slug(
                        coerce_slug(slug_source, field_name="idea_slug", separator="-")
                    )
                )
                normalized_idea_key = (
                    _optional_text(idea_key, field_name="idea_key")
                    or _idea_key_from_idea_id(normalized_idea_id)
                )
                normalized_opened_at = (
                    now
                    if opened_at is None
                    else _normalize_as_of(
                        opened_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_opened_at",
                    )
                )
                normalized_created_at = (
                    normalized_opened_at
                    if created_at is None
                    else _normalize_as_of(
                        created_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_created_at",
                    )
                )
                normalized_updated_at = (
                    normalized_created_at
                    if updated_at is None
                    else _normalize_as_of(
                        updated_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_updated_at",
                    )
                )
                normalized_decision_ref = (
                    _optional_text(decision_ref, field_name="decision_ref")
                    or _default_decision_ref(
                        _scope_fragment(normalized_idea_id, fallback="idea"),
                        normalized_created_at,
                    )
                )
                idea = await repository.record_idea(
                    idea_id=normalized_idea_id,
                    idea_key=normalized_idea_key,
                    title=normalized_title,
                    summary=normalized_summary,
                    source_kind=_require_text(source_kind, field_name="source_kind"),
                    source_ref=_optional_text(source_ref, field_name="source_ref"),
                    owner_ref=_optional_text(owner_ref, field_name="owner_ref"),
                    decision_ref=normalized_decision_ref,
                    opened_at=normalized_opened_at,
                    created_at=normalized_created_at,
                    updated_at=normalized_updated_at,
                )
                return {"kind": "operator_ideas", "action": "file", "idea": idea}

            normalized_idea_id = _require_text(idea_id, field_name="idea_id")
            if normalized_action == "resolve":
                normalized_status = _normalize_operator_idea_status(
                    status or "rejected",
                    terminal_only=True,
                )
                normalized_resolved_at = (
                    now
                    if resolved_at is None
                    else _normalize_as_of(
                        resolved_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_resolved_at",
                    )
                )
                normalized_decision_ref = (
                    _optional_text(decision_ref, field_name="decision_ref")
                    or _default_decision_ref(
                        _scope_fragment(normalized_idea_id, fallback="idea"),
                        normalized_resolved_at,
                    )
                )
                idea = await repository.resolve_idea(
                    idea_id=normalized_idea_id,
                    status=_require_text(normalized_status, field_name="status"),
                    resolution_summary=_require_text(
                        resolution_summary,
                        field_name="resolution_summary",
                    ),
                    decision_ref=normalized_decision_ref,
                    resolved_at=normalized_resolved_at,
                )
                return {"kind": "operator_ideas", "action": "resolve", "idea": idea}

            if normalized_action == "promote":
                normalized_roadmap_item_id = _require_text(
                    roadmap_item_id,
                    field_name="roadmap_item_id",
                )
                normalized_promoted_at = (
                    now
                    if promoted_at is None
                    else _normalize_as_of(
                        promoted_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_promoted_at",
                    )
                )
                normalized_decision_ref = (
                    _optional_text(decision_ref, field_name="decision_ref")
                    or _default_decision_ref(
                        _scope_fragment(
                            f"{normalized_idea_id}.{normalized_roadmap_item_id}",
                            fallback="idea-promotion",
                        ),
                        normalized_promoted_at,
                    )
                )
                promotion = await repository.promote_idea(
                    idea_promotion_id=_idea_promotion_id(
                        idea_id=normalized_idea_id,
                        roadmap_item_id=normalized_roadmap_item_id,
                    ),
                    idea_id=normalized_idea_id,
                    roadmap_item_id=normalized_roadmap_item_id,
                    decision_ref=normalized_decision_ref,
                    promoted_by=(
                        _optional_text(promoted_by, field_name="promoted_by")
                        or "operator_ideas"
                    ),
                    promoted_at=normalized_promoted_at,
                )
                return {"kind": "operator_ideas", "action": "promote", **promotion}
        finally:
            await conn.close()

        raise ValueError(f"unsupported operator ideas action: {normalized_action}")

    async def _ensure_issue_promoted_to_bug(
        self,
        *,
        conn: _Connection,
        runtime: WorkItemWorkflowBindingRuntime,
        binding_kind: str,
        issue_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        workflow_run_id: str | None,
        binding_status: str,
        bound_by_decision_id: str | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if (
            issue_id is None
            or not _binding_status_supports_pipeline(binding_status)
            or (
                workflow_class_id is None
                and schedule_definition_id is None
                and workflow_run_id is None
            )
        ):
            return None, None

        existing_bug = await self._fetch_bug_row_by_source_issue_id(
            conn,
            source_issue_id=issue_id,
        )
        bug_id: str
        created = False
        if existing_bug is None:
            issue_row = await self._fetch_issue_row(conn, issue_id=issue_id)
            if issue_row is None:
                return None, None
            summary = (
                f"Auto-promoted from {issue_id}: "
                f"{_require_text(issue_row.get('summary'), field_name='issue.summary')}"
            )
            bug_row, _similar_bugs = await afile_bug(
                conn,
                title=_require_text(issue_row.get("title"), field_name="issue.title"),
                severity=_require_text(issue_row.get("severity"), field_name="issue.severity"),
                category=BugCategory.OTHER,
                description=summary,
                filed_by="operator_write",
                source_kind="issue_promotion",
                decision_ref=_require_text(
                    issue_row.get("decision_ref"),
                    field_name="issue.decision_ref",
                ),
                discovered_in_run_id=_optional_text(
                    issue_row.get("discovered_in_run_id"),
                    field_name="issue.discovered_in_run_id",
                ),
                discovered_in_receipt_id=_optional_text(
                    issue_row.get("discovered_in_receipt_id"),
                    field_name="issue.discovered_in_receipt_id",
                ),
                owner_ref=_optional_text(issue_row.get("owner_ref"), field_name="issue.owner_ref"),
                source_issue_id=issue_id,
                tags=(
                    "auto-promoted",
                    f"source_issue:{_scope_fragment(issue_id, fallback='issue')}",
                ),
                resume_context={
                    "promoted_from_issue_id": issue_id,
                    "issue_source_kind": _require_text(
                        issue_row.get("source_kind"),
                        field_name="issue.source_kind",
                    ),
                },
            )
            bug_id = _require_text(bug_row.get("bug_id"), field_name="bug_id")
            created = True
        else:
            bug_id = _require_text(existing_bug.get("bug_id"), field_name="bug_id")

        bug_binding = await runtime.record_binding(
            binding_kind=binding_kind,
            bug_id=bug_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
            binding_status=binding_status,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        auto_promoted_roadmap = await self._ensure_bug_promoted_to_roadmap(
            conn=conn,
            runtime=runtime,
            binding_kind=binding_kind,
            bug_id=bug_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
            binding_status=binding_status,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {
            "bug_id": bug_id,
            "created": created,
            "binding": bug_binding.to_json(),
        }, auto_promoted_roadmap

    async def _ensure_bug_promoted_to_roadmap(
        self,
        *,
        conn: _Connection,
        runtime: WorkItemWorkflowBindingRuntime,
        binding_kind: str,
        bug_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        workflow_run_id: str | None,
        binding_status: str,
        bound_by_decision_id: str | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> dict[str, Any] | None:
        if (
            bug_id is None
            or not _binding_status_supports_pipeline(binding_status)
            or (
                workflow_class_id is None
                and schedule_definition_id is None
                and workflow_run_id is None
            )
        ):
            return None

        existing_item = await self._fetch_roadmap_item_by_source_bug_id(
            conn,
            source_bug_id=bug_id,
        )
        roadmap_item_id: str
        roadmap_item_payload: Mapping[str, Any] | None = None
        created = False
        now = _now()
        if existing_item is None:
            bug_row = await self._fetch_bug_row(conn, bug_id=bug_id)
            if bug_row is None:
                return None
            roadmap_item_id = _auto_promoted_bug_roadmap_item_id(bug_id)
            created_at_value = (
                now
                if created_at is None
                else _normalize_as_of(
                    created_at,
                    error_type=ValueError,
                    reason_code="operator_control.invalid_created_at",
                )
            )
            updated_at_value = (
                created_at_value
                if updated_at is None
                else _normalize_as_of(
                    updated_at,
                    error_type=ValueError,
                    reason_code="operator_control.invalid_updated_at",
                )
            )
            phase_order = _next_phase_order(
                await self._roadmap_sibling_phase_orders(
                    conn,
                    parent_roadmap_item_id=None,
                )
            )
            title = _require_text(bug_row.get("title"), field_name="bug.title")
            summary = _require_text(bug_row.get("summary"), field_name="bug.summary")
            decision_ref = (
                _optional_text(bug_row.get("decision_ref"), field_name="bug.decision_ref")
                or _default_decision_ref(
                    _scope_fragment(bug_id, fallback="bug"),
                    created_at_value,
                )
            )
            acceptance = _acceptance_payload(
                tier="tier_1",
                phase_ready=False,
                approval_tag=_default_approval_tag(created_at_value),
                outcome_gate=f"Resolve {bug_id} through the bound workflow path.",
                phase_order=phase_order,
                reference_doc=None,
                must_have=(
                    f"Track active work for {bug_id} through workflow bindings.",
                    f"Close {bug_id} only after explicit validates_fix evidence exists.",
                ),
            )
            assert self.roadmap_repository_factory is not None
            repository = self.roadmap_repository_factory(conn)
            roadmap_item_payload = _roadmap_item_payload(
                roadmap_item_id=roadmap_item_id,
                roadmap_key=_roadmap_key_from_item_id(roadmap_item_id),
                title=title,
                item_kind="capability",
                status="active",
                lifecycle=_ROADMAP_CLAIMED_LIFECYCLE,
                priority=_auto_promoted_bug_priority(
                    _optional_text(
                        bug_row.get("severity"),
                        field_name="bug.severity",
                    )
                ),
                parent_roadmap_item_id=None,
                source_bug_id=bug_id,
                source_idea_id=None,
                registry_paths=(),
                summary=f"Auto-promoted from {bug_id}: {summary}",
                acceptance_criteria=acceptance,
                decision_ref=decision_ref,
                created_at=created_at_value,
                updated_at=updated_at_value,
            )
            await repository.record_roadmap_package(
                roadmap_items=[
                    roadmap_item_payload
                ],
                roadmap_item_dependencies=[],
            )
            created = True
        else:
            roadmap_item_id = _require_text(
                existing_item.get("roadmap_item_id"),
                field_name="roadmap_item_id",
            )
            roadmap_item_payload = await self._claim_roadmap_item(
                conn,
                roadmap_item_id=roadmap_item_id,
                updated_at=(
                    now
                    if updated_at is None
                    else _normalize_as_of(
                        updated_at,
                        error_type=ValueError,
                        reason_code="operator_control.invalid_updated_at",
                    )
                ),
            )

        if roadmap_item_payload is not None:
            semantic_bridge_summary = await self._sync_semantic_bridges_for_roadmap_item(
                conn,
                roadmap_item=roadmap_item_payload,
                emitted_by="operator_write.record_work_item_workflow_binding",
            )
        else:
            semantic_bridge_summary = {
                "processed": 0,
                "recorded": 0,
                "retracted": 0,
            }

        roadmap_binding = await runtime.record_binding(
            binding_kind=binding_kind,
            roadmap_item_id=roadmap_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
            binding_status=binding_status,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {
            "roadmap_item_id": roadmap_item_id,
            "created": created,
            "binding": roadmap_binding.to_json(),
            "semantic_bridge_summary": semantic_bridge_summary,
        }

    async def _roadmap_sibling_phase_orders(
        self,
        conn: _Connection,
        *,
        parent_roadmap_item_id: str | None,
    ) -> tuple[str, ...]:
        rows = await conn.fetch(
            """
            SELECT acceptance_criteria->>'phase_order' AS phase_order
            FROM roadmap_items
            WHERE parent_roadmap_item_id IS NOT DISTINCT FROM $1
            ORDER BY roadmap_item_id
            """,
            parent_roadmap_item_id,
        )
        return tuple(
            str(row["phase_order"])
            for row in rows
            if row.get("phase_order")
        )

    async def _prepare_roadmap_write(
        self,
        conn: _Connection,
        *,
        action: str,
        title: str,
        intent_brief: str,
        template: str,
        priority: str,
        parent_roadmap_item_id: str | None,
        slug: str | None,
        depends_on: tuple[str, ...],
        source_bug_id: str | None,
        source_idea_id: str | None,
        registry_paths: tuple[str, ...],
        decision_ref: str | None,
        item_kind: str | None,
        status: str | None,
        lifecycle: str | None,
        tier: str | None,
        phase_ready: bool | None,
        approval_tag: str | None,
        reference_doc: str | None,
        outcome_gate: str | None,
        proof_kind: str | None,
    ) -> dict[str, Any]:
        now = _now()
        normalized_action = _normalize_roadmap_action(action)
        normalized_title = _require_text(title, field_name="title")
        normalized_intent_brief = _require_text(
            intent_brief,
            field_name="intent_brief",
        )
        normalized_template = _require_roadmap_template(template)
        normalized_priority = _normalize_roadmap_priority(priority)
        normalized_parent = _optional_text(
            parent_roadmap_item_id,
            field_name="parent_roadmap_item_id",
        )
        normalized_source_bug_id = _optional_text(
            source_bug_id,
            field_name="source_bug_id",
        )
        normalized_source_idea_id = _optional_text(
            source_idea_id,
            field_name="source_idea_id",
        )
        normalized_registry_paths = _normalize_registry_paths(registry_paths)
        normalized_status = _normalize_roadmap_status(status)
        normalized_lifecycle = _normalize_roadmap_lifecycle(lifecycle)
        normalized_depends_on = tuple(
            dependency
            for dependency in depends_on
            if dependency != normalized_parent
        )
        auto_fixes: list[str] = []
        warnings: list[str] = []
        blocking_errors: list[str] = []

        parent_row: Mapping[str, Any] | None = None
        if normalized_parent is not None:
            parent_row = await self._fetch_roadmap_item(
                conn,
                roadmap_item_id=normalized_parent,
            )
            if parent_row is None:
                blocking_errors.append(
                    f"parent roadmap item not found: {normalized_parent}"
                )

        if normalized_source_bug_id is not None and not await self._bug_exists(
            conn,
            bug_id=normalized_source_bug_id,
        ):
            blocking_errors.append(
                f"source bug not found: {normalized_source_bug_id}"
            )
        if normalized_source_idea_id is not None and not await self._idea_exists(
            conn,
            idea_id=normalized_source_idea_id,
        ):
            blocking_errors.append(
                f"source idea not found: {normalized_source_idea_id}"
            )

        for dependency in normalized_depends_on:
            if not await self._roadmap_item_exists(conn, roadmap_item_id=dependency):
                blocking_errors.append(f"dependency roadmap item not found: {dependency}")

        normalized_slug = _optional_text(slug, field_name="slug")
        if normalized_slug is None:
            normalized_slug = _slugify_roadmap_text(normalized_title)
            auto_fixes.append(f"slug generated from title: {normalized_slug}")
        else:
            if _looks_like_full_roadmap_ref(normalized_slug):
                blocking_errors.append(
                    "slug must be a roadmap slug fragment, not a full roadmap item id or key: "
                    f"{normalized_slug}"
                )
            normalized_slug = _slugify_roadmap_text(normalized_slug)

        normalized_item_kind = _normalize_roadmap_item_kind(
            item_kind,
            template=normalized_template,
        )
        if normalized_status in {_ROADMAP_COMPLETED_STATUS, "done"} and lifecycle is None:
            normalized_lifecycle = _ROADMAP_COMPLETED_LIFECYCLE
            auto_fixes.append("lifecycle derived from completed status: completed")
        elif (
            normalized_lifecycle == _ROADMAP_COMPLETED_LIFECYCLE
            and normalized_status not in {_ROADMAP_COMPLETED_STATUS, "done"}
        ):
            normalized_status = _ROADMAP_COMPLETED_STATUS
            auto_fixes.append("status aligned to completed lifecycle: completed")
        if normalized_lifecycle == "idea":
            blocking_errors.append(
                "roadmap lifecycle 'idea' is retired for new roadmap writes; "
                "record pre-commitment work through praxis_operator_ideas and "
                "promote it into roadmap when committed"
            )

        parent_acceptance = (
            parent_row.get("acceptance_criteria")
            if parent_row is not None
            and isinstance(parent_row.get("acceptance_criteria"), Mapping)
            else {}
        )
        normalized_tier = (
            _optional_text(tier, field_name="tier")
            or (
                str(parent_acceptance.get("tier")).strip()
                if isinstance(parent_acceptance.get("tier"), str)
                and str(parent_acceptance.get("tier")).strip()
                else "tier_1"
            )
        )
        normalized_phase_ready = (
            bool(phase_ready)
            if phase_ready is not None
            else bool(parent_acceptance.get("phase_ready", False))
        )
        normalized_approval_tag = (
            _optional_text(approval_tag, field_name="approval_tag")
            or (
                str(parent_acceptance.get("approval_tag")).strip()
                if isinstance(parent_acceptance.get("approval_tag"), str)
                and str(parent_acceptance.get("approval_tag")).strip()
                else _default_approval_tag(now)
            )
        )
        if approval_tag is None:
            auto_fixes.append(f"approval_tag generated: {normalized_approval_tag}")
        normalized_reference_doc = _optional_text(
            reference_doc,
            field_name="reference_doc",
        )
        normalized_outcome_gate = (
            _optional_text(outcome_gate, field_name="outcome_gate")
            or normalized_intent_brief
        )
        normalized_decision_ref = (
            _optional_text(decision_ref, field_name="decision_ref")
            or _default_decision_ref(normalized_slug.replace(".", "-"), now)
        )
        if decision_ref is None:
            auto_fixes.append(f"decision_ref generated: {normalized_decision_ref}")

        normalized_proof_kind = _optional_text(proof_kind, field_name="proof_kind")
        if normalized_proof_kind is not None:
            if normalized_proof_kind != _CAPABILITY_DELIVERED_BY_DECISION_FILING:
                blocking_errors.append(
                    f"unsupported proof_kind '{normalized_proof_kind}': only "
                    f"'{_CAPABILITY_DELIVERED_BY_DECISION_FILING}' is currently recognized"
                )
            elif normalized_item_kind != "capability":
                blocking_errors.append(
                    f"proof_kind '{_CAPABILITY_DELIVERED_BY_DECISION_FILING}' "
                    "requires item_kind='capability' (decision-filing closeout is "
                    "only valid for capability rows)"
                )

        root_roadmap_item_id = (
            f"{normalized_parent}.{normalized_slug}"
            if normalized_parent is not None
            else f"roadmap_item.{normalized_slug}"
        )
        root_roadmap_key = _roadmap_key_from_item_id(root_roadmap_item_id)
        if root_roadmap_item_id in normalized_depends_on:
            blocking_errors.append(
                f"roadmap item cannot depend on itself: {root_roadmap_item_id}"
            )

        sibling_phase_orders = await self._roadmap_sibling_phase_orders(
            conn,
            parent_roadmap_item_id=normalized_parent,
        )
        root_phase_order = _next_phase_order(sibling_phase_orders)
        auto_fixes.append(f"phase_order assigned: {root_phase_order}")

        root_acceptance = _acceptance_payload(
            tier=normalized_tier,
            phase_ready=normalized_phase_ready,
            approval_tag=normalized_approval_tag,
            outcome_gate=normalized_outcome_gate,
            phase_order=root_phase_order,
            reference_doc=normalized_reference_doc,
            must_have=(normalized_intent_brief,),
            proof_kind=normalized_proof_kind,
        )

        root_item = _roadmap_item_payload(
            roadmap_item_id=root_roadmap_item_id,
            roadmap_key=root_roadmap_key,
            title=normalized_title,
            item_kind=normalized_item_kind,
            status=normalized_status,
            lifecycle=normalized_lifecycle,
            priority=normalized_priority,
            parent_roadmap_item_id=normalized_parent,
            source_bug_id=normalized_source_bug_id,
            source_idea_id=normalized_source_idea_id,
            registry_paths=normalized_registry_paths,
            summary=normalized_intent_brief,
            acceptance_criteria=root_acceptance,
            decision_ref=normalized_decision_ref,
            created_at=now,
            updated_at=now,
        )

        preview_items: list[dict[str, Any]] = [root_item]
        preview_dependencies: list[dict[str, Any]] = []

        for dependency in normalized_depends_on:
            preview_dependencies.append(
                _roadmap_dependency_payload(
                    roadmap_item_dependency_id=_roadmap_dependency_id(
                        roadmap_item_id=root_roadmap_item_id,
                        depends_on_roadmap_item_id=dependency,
                        dependency_kind="blocks",
                    ),
                    roadmap_item_id=root_roadmap_item_id,
                    depends_on_roadmap_item_id=dependency,
                    dependency_kind="blocks",
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                )
            )

        previous_item_id = root_roadmap_item_id
        template_children = _ROADMAP_TEMPLATE_CHILDREN[normalized_template]
        for index, child in enumerate(template_children, start=1):
            child_item_id = f"{root_roadmap_item_id}.{child.suffix}"
            child_phase_order = f"{root_phase_order}.{index}"
            child_acceptance = _acceptance_payload(
                tier=normalized_tier,
                phase_ready=normalized_phase_ready,
                approval_tag=normalized_approval_tag,
                outcome_gate=child.summary,
                phase_order=child_phase_order,
                reference_doc=normalized_reference_doc,
                must_have=child.must_have,
            )
            preview_items.append(
                _roadmap_item_payload(
                    roadmap_item_id=child_item_id,
                    roadmap_key=_roadmap_key_from_item_id(child_item_id),
                    title=child.title,
                    item_kind="capability",
                    status=normalized_status,
                    lifecycle=normalized_lifecycle,
                    priority=child.priority,
                    parent_roadmap_item_id=root_roadmap_item_id,
                    source_bug_id=normalized_source_bug_id,
                    source_idea_id=normalized_source_idea_id,
                    registry_paths=normalized_registry_paths,
                    summary=child.summary,
                    acceptance_criteria=child_acceptance,
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                    updated_at=now,
                )
            )
            preview_dependencies.append(
                _roadmap_dependency_payload(
                    roadmap_item_dependency_id=_roadmap_dependency_id(
                        roadmap_item_id=child_item_id,
                        depends_on_roadmap_item_id=previous_item_id,
                        dependency_kind="blocks",
                    ),
                    roadmap_item_id=child_item_id,
                    depends_on_roadmap_item_id=previous_item_id,
                    dependency_kind="blocks",
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                )
            )
            previous_item_id = child_item_id

        normalized_payload = {
            "action": normalized_action,
            "template": normalized_template,
            "title": normalized_title,
            "intent_brief": normalized_intent_brief,
            "slug": normalized_slug,
            "item_kind": normalized_item_kind,
            "status": normalized_status,
            "lifecycle": normalized_lifecycle,
            "priority": normalized_priority,
            "parent_roadmap_item_id": normalized_parent,
            "depends_on": list(normalized_depends_on),
            "source_bug_id": normalized_source_bug_id,
            "source_idea_id": normalized_source_idea_id,
            "registry_paths": list(normalized_registry_paths),
            "decision_ref": normalized_decision_ref,
            "tier": normalized_tier,
            "phase_ready": normalized_phase_ready,
            "approval_tag": normalized_approval_tag,
            "reference_doc": normalized_reference_doc,
            "outcome_gate": normalized_outcome_gate,
            "proof_kind": normalized_proof_kind,
            "root_phase_order": root_phase_order,
        }

        return {
            "action": normalized_action,
            "normalized_payload": normalized_payload,
            "auto_fixes": auto_fixes,
            "warnings": warnings,
            "blocking_errors": blocking_errors,
            "preview": {
                "roadmap_items": preview_items,
                "roadmap_item_dependencies": preview_dependencies,
            },
        }

    async def _roadmap_write(
        self,
        *,
        env: Mapping[str, str] | None,
        action: str,
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] = (),
        source_bug_id: str | None = None,
        source_idea_id: str | None = None,
        registry_paths: tuple[str, ...] = (),
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        lifecycle: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
        proof_kind: str | None = None,
    ) -> dict[str, Any]:
        conn = await self.connect_database(env)
        try:
            preview = await self._prepare_roadmap_write(
                conn,
                action=action,
                title=title,
                intent_brief=intent_brief,
                template=template,
                priority=priority,
                parent_roadmap_item_id=parent_roadmap_item_id,
                slug=slug,
                depends_on=depends_on,
                source_bug_id=source_bug_id,
                source_idea_id=source_idea_id,
                registry_paths=registry_paths,
                decision_ref=decision_ref,
                item_kind=item_kind,
                status=status,
                lifecycle=lifecycle,
                tier=tier,
                phase_ready=phase_ready,
                approval_tag=approval_tag,
                reference_doc=reference_doc,
                outcome_gate=outcome_gate,
                proof_kind=proof_kind,
            )
            if preview["blocking_errors"] or preview["action"] != "commit":
                preview["committed"] = False
                return preview

            assert self.roadmap_repository_factory is not None
            repository = self.roadmap_repository_factory(conn)
            async with conn.transaction():
                commit_summary = await repository.record_roadmap_package(
                    roadmap_items=preview["preview"]["roadmap_items"],
                    roadmap_item_dependencies=preview["preview"]["roadmap_item_dependencies"],
                )
                semantic_bridge_summary = {
                    "processed": 0,
                    "recorded": 0,
                    "retracted": 0,
                }
                for roadmap_item in preview["preview"]["roadmap_items"]:
                    item_summary = await self._sync_semantic_bridges_for_roadmap_item(
                        conn,
                        roadmap_item=_require_mapping(
                            roadmap_item,
                            field_name="preview.roadmap_item",
                        ),
                        emitted_by="operator_write.roadmap_write",
                    )
                    for key in semantic_bridge_summary:
                        semantic_bridge_summary[key] += item_summary[key]

            preview["committed"] = True
            preview["commit_summary"] = commit_summary
            preview["semantic_bridge_summary"] = semantic_bridge_summary
            return preview
        finally:
            await conn.close()

    async def _fetch_bug_rows_for_closeout(
        self,
        conn: _Connection,
        *,
        bug_ids: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        if bug_ids:
            rows = await conn.fetch(
                """
                SELECT
                    bug_id,
                    title,
                    status,
                    resolution_summary,
                    resolved_at,
                    updated_at
                FROM bugs
                WHERE bug_id = ANY($1::text[])
                ORDER BY bug_id
                """,
                list(bug_ids),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    bug_id,
                    title,
                    status,
                    resolution_summary,
                    resolved_at,
                    updated_at
                FROM bugs
                WHERE resolved_at IS NULL
                ORDER BY opened_at DESC, created_at DESC, bug_id
                """
            )
        return tuple(rows)

    async def _fetch_bug_evidence_for_closeout(
        self,
        conn: _Connection,
        *,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[Mapping[str, Any], ...]]:
        if not bug_ids:
            return {}
        rows = await conn.fetch(
            """
            SELECT
                bel.bug_id,
                bel.evidence_kind,
                bel.evidence_ref,
                bel.evidence_role,
                vr.status AS verification_status
            FROM bug_evidence_links AS bel
            LEFT JOIN verification_runs AS vr
              ON bel.evidence_kind = 'verification_run'
             AND vr.verification_run_id = bel.evidence_ref
            WHERE bel.bug_id = ANY($1::text[])
            ORDER BY bug_id, created_at, bug_evidence_link_id
            """,
            list(bug_ids),
        )
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["bug_id"]), []).append(dict(row))
        return {bug_id: tuple(items) for bug_id, items in grouped.items()}

    async def _fetch_roadmap_rows_for_closeout(
        self,
        conn: _Connection,
        *,
        roadmap_item_ids: tuple[str, ...],
        source_bug_ids: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        if roadmap_item_ids:
            return tuple(
                await conn.fetch(
                    """
                    SELECT
                        roadmap_item_id,
                        title,
                        status,
                        lifecycle,
                        item_kind,
                        source_bug_id,
                        decision_ref,
                        acceptance_criteria,
                        completed_at,
                        updated_at
                    FROM roadmap_items
                    WHERE roadmap_item_id = ANY($1::text[])
                    ORDER BY roadmap_item_id
                    """,
                    list(roadmap_item_ids),
                )
            )
        if not source_bug_ids:
            return ()
        return tuple(
            await conn.fetch(
                """
                SELECT
                    roadmap_item_id,
                    title,
                    status,
                    lifecycle,
                    item_kind,
                    source_bug_id,
                    decision_ref,
                    acceptance_criteria,
                    completed_at,
                    updated_at
                FROM roadmap_items
                WHERE source_bug_id = ANY($1::text[])
                  AND completed_at IS NULL
                ORDER BY roadmap_item_id
                """,
                list(source_bug_ids),
            )
        )

    async def _fetch_decision_proof_for_closeout(
        self,
        conn: _Connection,
        *,
        decision_refs: tuple[str, ...],
    ) -> dict[str, Mapping[str, Any]]:
        """Return {decision_ref: decision_row} for refs that resolve to a decided operator_decision.

        A roadmap_items.decision_ref is accepted as capability-delivered proof if it
        resolves (by operator_decision_id or by decision_key) to an operator_decisions
        row with status='decided' and is still effective (effective_to is NULL or future).
        """

        if not decision_refs:
            return {}
        rows = await conn.fetch(
            """
            SELECT
                operator_decision_id,
                decision_key,
                decision_kind,
                decision_status,
                title,
                effective_from,
                effective_to,
                decided_at
            FROM operator_decisions
            WHERE (operator_decision_id = ANY($1::text[])
                   OR decision_key = ANY($1::text[]))
              AND decision_status = 'decided'
              AND (effective_to IS NULL OR effective_to > now())
            """,
            list(decision_refs),
        )
        ref_set = set(decision_refs)
        resolved: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            payload = dict(row)
            for key in (str(payload["operator_decision_id"]), str(payload["decision_key"])):
                if key in ref_set and key not in resolved:
                    resolved[key] = payload
        return resolved

    async def _fetch_roadmap_bug_relation_rows_for_closeout(
        self,
        conn: _Connection,
        *,
        roadmap_item_ids: tuple[str, ...],
        bug_ids: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        if not roadmap_item_ids and not bug_ids:
            return ()
        return tuple(
            await conn.fetch(
                """
                SELECT
                    operator_object_relation_id,
                    relation_kind,
                    relation_status,
                    source_ref AS roadmap_item_id,
                    target_ref AS bug_id
                FROM operator_object_relations
                WHERE relation_status = 'active'
                  AND source_kind = 'roadmap_item'
                  AND target_kind = 'bug'
                  AND relation_kind = ANY($1::text[])
                  AND (
                        (
                            cardinality($2::text[]) > 0
                            AND source_ref = ANY($2::text[])
                        )
                     OR (
                            cardinality($3::text[]) > 0
                            AND target_ref = ANY($3::text[])
                        )
                  )
                ORDER BY source_ref, target_ref, relation_kind, operator_object_relation_id
                """,
                list(sorted(_ROADMAP_BUG_CLOSEOUT_RELATION_KINDS)),
                list(roadmap_item_ids),
                list(bug_ids),
            )
        )

    async def _reconcile_work_item_closeout(
        self,
        *,
        env: Mapping[str, str] | None,
        action: str,
        bug_ids: tuple[str, ...],
        roadmap_item_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        normalized_action = _normalize_work_item_closeout_action(action)
        conn = await self.connect_database(env)
        try:
            scoped_roadmap_rows = await self._fetch_roadmap_rows_for_closeout(
                conn,
                roadmap_item_ids=roadmap_item_ids,
                source_bug_ids=(),
            )
            supplemental_bug_ids = tuple(
                dict.fromkeys(
                    str(row["source_bug_id"])
                    for row in scoped_roadmap_rows
                    if row.get("source_bug_id") is not None
                )
            )
            scoped_relation_rows = await self._fetch_roadmap_bug_relation_rows_for_closeout(
                conn,
                roadmap_item_ids=tuple(
                    str(row["roadmap_item_id"]) for row in scoped_roadmap_rows
                ),
                bug_ids=bug_ids,
            )
            supplemental_relation_bug_ids = tuple(
                dict.fromkeys(str(row["bug_id"]) for row in scoped_relation_rows)
            )
            scoped_bug_ids = tuple(
                dict.fromkeys((*bug_ids, *supplemental_bug_ids, *supplemental_relation_bug_ids))
            )
            bug_rows = await self._fetch_bug_rows_for_closeout(
                conn,
                bug_ids=scoped_bug_ids,
            )
            if not roadmap_item_ids:
                proof_bug_ids = tuple(str(row["bug_id"]) for row in bug_rows)
                scoped_roadmap_rows = await self._fetch_roadmap_rows_for_closeout(
                    conn,
                    roadmap_item_ids=(),
                    source_bug_ids=proof_bug_ids,
                )
                scoped_relation_rows = await self._fetch_roadmap_bug_relation_rows_for_closeout(
                    conn,
                    roadmap_item_ids=(),
                    bug_ids=proof_bug_ids,
                )
                existing_roadmap_item_ids = {
                    str(row["roadmap_item_id"]) for row in scoped_roadmap_rows
                }
                relation_roadmap_item_ids = tuple(
                    roadmap_item_id
                    for roadmap_item_id in dict.fromkeys(
                        str(row["roadmap_item_id"]) for row in scoped_relation_rows
                    )
                    if roadmap_item_id not in existing_roadmap_item_ids
                )
                if relation_roadmap_item_ids:
                    relation_roadmap_rows = await self._fetch_roadmap_rows_for_closeout(
                        conn,
                        roadmap_item_ids=relation_roadmap_item_ids,
                        source_bug_ids=(),
                    )
                    scoped_roadmap_rows = tuple((*scoped_roadmap_rows, *relation_roadmap_rows))

            relation_rows_by_roadmap_item_id: dict[str, tuple[Mapping[str, Any], ...]] = {}
            relation_rows_by_roadmap: dict[str, list[Mapping[str, Any]]] = {}
            for relation_row in scoped_relation_rows:
                relation_rows_by_roadmap.setdefault(
                    str(relation_row["roadmap_item_id"]),
                    [],
                ).append(dict(relation_row))
            relation_rows_by_roadmap_item_id = {
                roadmap_item_id: tuple(rows)
                for roadmap_item_id, rows in relation_rows_by_roadmap.items()
            }

            evidence_by_bug_id = await self._fetch_bug_evidence_for_closeout(
                conn,
                bug_ids=tuple(str(row["bug_id"]) for row in bug_rows),
            )
            proof_bug_ids = {
                bug_id
                for bug_id, rows in evidence_by_bug_id.items()
                if any(
                    str(row["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE
                    and str(row["evidence_kind"]) == "verification_run"
                    and _closeout_verification_passed(row.get("verification_status"))
                    for row in rows
                )
            }
            capability_decision_refs = tuple(
                ref
                for ref in dict.fromkeys(
                    str(row["decision_ref"])
                    for row in scoped_roadmap_rows
                    if row.get("item_kind") == "capability"
                    and row.get("decision_ref") is not None
                    and str(row.get("decision_ref") or "").strip()
                )
                if ref
            )
            decision_proof_by_ref = await self._fetch_decision_proof_for_closeout(
                conn,
                decision_refs=capability_decision_refs,
            )
            now = _now()

            bug_candidates: list[dict[str, Any]] = []
            bug_skipped: list[dict[str, Any]] = []
            for row in bug_rows:
                bug_id = str(row["bug_id"])
                evidence_refs = [
                    {
                        "kind": str(evidence["evidence_kind"]),
                        "ref": str(evidence["evidence_ref"]),
                        "role": str(evidence["evidence_role"]),
                        "verification_status": (
                            str(evidence["verification_status"])
                            if evidence.get("verification_status") is not None
                            else None
                        ),
                    }
                    for evidence in evidence_by_bug_id.get(bug_id, ())
                    if str(evidence["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE
                    and str(evidence["evidence_kind"]) == "verification_run"
                    and _closeout_verification_passed(evidence.get("verification_status"))
                ]
                if row.get("resolved_at") is None and evidence_refs:
                    bug_candidates.append(
                        {
                            "bug_id": bug_id,
                            "current_status": str(row["status"]),
                            "next_status": BugStatus.FIXED.value,
                            "reason_codes": ["explicit_passed_fix_proof_present"],
                            "evidence_refs": evidence_refs,
                            "resolution_summary": _closeout_resolution_summary(
                                bug_id=bug_id,
                                evidence_count=len(evidence_refs),
                            ),
                        }
                    )
                    continue
                reason_codes = []
                if row.get("resolved_at") is not None:
                    reason_codes.append("already_resolved")
                if bug_id not in proof_bug_ids:
                    reason_codes.append("missing_passed_validates_fix_verification")
                if reason_codes:
                    bug_skipped.append(
                        {
                            "bug_id": bug_id,
                            "current_status": str(row["status"]),
                            "reason_codes": reason_codes,
                        }
                    )

            roadmap_candidates: list[dict[str, Any]] = []
            roadmap_skipped: list[dict[str, Any]] = []
            for row in scoped_roadmap_rows:
                roadmap_item_id = str(row["roadmap_item_id"])
                source_bug_id = (
                    str(row["source_bug_id"])
                    if row.get("source_bug_id") is not None
                    else None
                )
                source_bug_link_source = (
                    "roadmap_items.source_bug_id" if source_bug_id is not None else None
                )
                source_bug_relation_id: str | None = None
                relation_rows = relation_rows_by_roadmap_item_id.get(roadmap_item_id, ())
                if source_bug_id is None and relation_rows:
                    relation_row = next(
                        (
                            candidate
                            for candidate in relation_rows
                            if str(candidate["bug_id"]) in proof_bug_ids
                        ),
                        relation_rows[0],
                    )
                    source_bug_id = str(relation_row["bug_id"])
                    source_bug_link_source = "operator_object_relations"
                    source_bug_relation_id = str(relation_row["operator_object_relation_id"])
                if row.get("completed_at") is None and source_bug_id in proof_bug_ids:
                    roadmap_candidates.append(
                        {
                            "roadmap_item_id": roadmap_item_id,
                            "source_bug_id": source_bug_id,
                            "source_bug_link_source": source_bug_link_source,
                            "source_bug_relation_id": source_bug_relation_id,
                            "current_status": str(row["status"]),
                            "current_lifecycle": str(row["lifecycle"]),
                            "next_status": _ROADMAP_COMPLETED_STATUS,
                            "next_lifecycle": _ROADMAP_COMPLETED_LIFECYCLE,
                            "reason_codes": [
                                (
                                    "relation_bug_has_explicit_passed_fix_proof"
                                    if source_bug_link_source == "operator_object_relations"
                                    else "source_bug_has_explicit_passed_fix_proof"
                                ),
                            ],
                            "evidence_refs": [
                                {
                                    "kind": str(evidence["evidence_kind"]),
                                    "ref": str(evidence["evidence_ref"]),
                                    "role": str(evidence["evidence_role"]),
                                    "verification_status": (
                                        str(evidence["verification_status"])
                                        if evidence.get("verification_status") is not None
                                        else None
                                    ),
                                }
                                for evidence in evidence_by_bug_id.get(source_bug_id or "", ())
                                if str(evidence["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE
                                and str(evidence["evidence_kind"]) == "verification_run"
                                and _closeout_verification_passed(
                                    evidence.get("verification_status")
                                )
                            ],
                        }
                    )
                    continue
                decision_ref_value = (
                    str(row.get("decision_ref") or "").strip() or None
                )
                item_kind = str(row.get("item_kind") or "").strip()
                acceptance_criteria_value = row.get("acceptance_criteria")
                proof_kind_value = _roadmap_acceptance_proof_kind(
                    acceptance_criteria_value
                )
                declares_decision_filing_proof = (
                    proof_kind_value == _CAPABILITY_DELIVERED_BY_DECISION_FILING
                )
                decision_proof = (
                    decision_proof_by_ref.get(decision_ref_value)
                    if (
                        decision_ref_value is not None
                        and item_kind == "capability"
                    )
                    else None
                )
                if (
                    row.get("completed_at") is None
                    and source_bug_id is None
                    and decision_proof is not None
                ):
                    roadmap_candidates.append(
                        {
                            "roadmap_item_id": roadmap_item_id,
                            "source_bug_id": None,
                            "source_bug_link_source": None,
                            "source_bug_relation_id": None,
                            "current_status": str(row["status"]),
                            "current_lifecycle": str(row["lifecycle"]),
                            "next_status": _ROADMAP_COMPLETED_STATUS,
                            "next_lifecycle": _ROADMAP_COMPLETED_LIFECYCLE,
                            "reason_codes": [
                                (
                                    "capability_delivered_by_decision_filing_proof_present"
                                    if declares_decision_filing_proof
                                    else "capability_delivered_by_decision_ref_proof_present"
                                ),
                            ],
                            "evidence_refs": [
                                {
                                    "kind": "operator_decision",
                                    "ref": str(decision_proof["operator_decision_id"]),
                                    "role": "capability_delivered_decision",
                                    "decision_key": str(decision_proof["decision_key"]),
                                    "decision_kind": str(decision_proof["decision_kind"]),
                                    "decision_status": str(decision_proof["decision_status"]),
                                }
                            ],
                        }
                    )
                    continue
                reason_codes = []
                if row.get("completed_at") is not None:
                    reason_codes.append("already_completed")
                if source_bug_id is None:
                    if (
                        item_kind == "capability"
                        and decision_ref_value is not None
                    ):
                        if decision_proof is None:
                            reason_codes.append(
                                (
                                    "capability_decision_filing_proof_not_decided"
                                    if declares_decision_filing_proof
                                    else "capability_decision_ref_not_decided"
                                )
                            )
                    else:
                        reason_codes.append("missing_source_bug")
                elif source_bug_id not in proof_bug_ids:
                    reason_codes.append(
                        "relation_bug_missing_passed_fix_verification"
                        if source_bug_link_source == "operator_object_relations"
                        else "source_bug_missing_passed_fix_verification"
                    )
                if reason_codes:
                    roadmap_skipped.append(
                        {
                            "roadmap_item_id": roadmap_item_id,
                            "source_bug_id": source_bug_id,
                            "source_bug_link_source": source_bug_link_source,
                            "source_bug_relation_id": source_bug_relation_id,
                            "current_status": str(row["status"]),
                            "current_lifecycle": str(row["lifecycle"]),
                            "item_kind": item_kind or None,
                            "decision_ref": decision_ref_value,
                            "proof_kind": proof_kind_value,
                            "reason_codes": reason_codes,
                        }
                    )

            payload: dict[str, Any] = {
                "action": normalized_action,
                "proof_threshold": {
                    "bug_requires_evidence_role": _BUG_CLOSEOUT_EVIDENCE_ROLE,
                    "bug_requires_passed_verification": True,
                    "roadmap_requires_source_bug_fix_proof": True,
                    "roadmap_requires_bug_fix_proof": True,
                    "roadmap_capability_without_bug_accepts_decided_decision_ref": True,
                    "roadmap_bug_link_authorities": [
                        "roadmap_items.source_bug_id",
                        "operator_object_relations.active_roadmap_item_to_bug",
                    ],
                },
                "evaluated": {
                    "bug_ids": [str(row["bug_id"]) for row in bug_rows],
                    "roadmap_item_ids": [str(row["roadmap_item_id"]) for row in scoped_roadmap_rows],
                },
                "candidates": {
                    "bugs": bug_candidates,
                    "roadmap_items": roadmap_candidates,
                },
                "skipped": {
                    "bugs": bug_skipped,
                    "roadmap_items": roadmap_skipped,
                },
                "committed": False,
                "applied": {
                    "bugs": [],
                    "issues": [],
                    "roadmap_items": [],
                },
            }
            if normalized_action != "commit":
                return payload

            async with conn.transaction():
                # DECISION: closeout write-side effects are delegated to the repository seam.
                # SEE: storage.postgres.work_item_closeout_repository for canonical bug/roadmap mutation contract.
                assert self.work_item_closeout_repository_factory is not None
                closeout_repository = self.work_item_closeout_repository_factory(conn)
                applied_bug_rows = []
                if bug_candidates:
                    applied_bug_rows = await closeout_repository.mark_bugs_fixed(
                        bug_ids=tuple(candidate["bug_id"] for candidate in bug_candidates),
                        resolution_summaries_by_bug_id={
                            candidate["bug_id"]: candidate["resolution_summary"]
                            for candidate in bug_candidates
                        },
                        resolved_at=now,
                    )
                applied_issue_rows = []
                if applied_bug_rows:
                    applied_issue_rows = await closeout_repository.mark_issues_resolved_by_bug_ids(
                        bug_ids=tuple(str(row["bug_id"]) for row in applied_bug_rows),
                        resolved_at=now,
                    )
                applied_roadmap_rows = []
                if roadmap_candidates:
                    applied_roadmap_rows = await closeout_repository.mark_roadmap_items_completed(
                        roadmap_item_ids=tuple(
                            candidate["roadmap_item_id"] for candidate in roadmap_candidates
                        ),
                        completed_status=_ROADMAP_COMPLETED_STATUS,
                        completed_at=now,
                    )
            roadmap_candidates_by_id = {
                str(candidate["roadmap_item_id"]): candidate for candidate in roadmap_candidates
            }
            payload["committed"] = True
            payload["applied"] = {
                "bugs": [
                    {
                        "bug_id": str(row["bug_id"]),
                        "status": str(row["status"]),
                        "resolved_at": row["resolved_at"].isoformat() if row["resolved_at"] is not None else None,
                        "resolution_summary": str(row["resolution_summary"]) if row["resolution_summary"] is not None else None,
                    }
                    for row in applied_bug_rows
                ],
                "issues": [
                    {
                        "issue_id": str(row["issue_id"]),
                        "status": str(row["status"]),
                        "resolved_at": row["resolved_at"].isoformat() if row["resolved_at"] is not None else None,
                        "resolution_summary": str(row["resolution_summary"]) if row["resolution_summary"] is not None else None,
                    }
                    for row in applied_issue_rows
                ],
                "roadmap_items": [
                    {
                        "roadmap_item_id": str(row["roadmap_item_id"]),
                        "status": str(row["status"]),
                        "lifecycle": str(row["lifecycle"]),
                        "completed_at": row["completed_at"].isoformat() if row["completed_at"] is not None else None,
                        "source_bug_id": (
                            str(row["source_bug_id"])
                            if row["source_bug_id"] is not None
                            else roadmap_candidates_by_id.get(
                                str(row["roadmap_item_id"]), {}
                            ).get("source_bug_id")
                        ),
                        "source_bug_link_source": roadmap_candidates_by_id.get(
                            str(row["roadmap_item_id"]), {}
                        ).get("source_bug_link_source"),
                        "source_bug_relation_id": roadmap_candidates_by_id.get(
                            str(row["roadmap_item_id"]), {}
                        ).get("source_bug_relation_id"),
                    }
                    for row in applied_roadmap_rows
                ],
            }
            return payload
        finally:
            await conn.close()

    async def _set_circuit_breaker_override(
        self,
        *,
        env: Mapping[str, str] | None,
        provider_slug: str,
        override_state: str,
        effective_to: datetime | None,
        reason_code: str,
        rationale: str | None,
        effective_from: datetime | None,
        decided_by: str | None,
        decision_source: str | None,
    ) -> CircuitBreakerOverrideRecord:
        normalized_provider_slug = _require_text(
            provider_slug,
            field_name="provider_slug",
        ).lower()
        normalized_override_state = _normalize_circuit_breaker_override_state(
            override_state,
        )
        normalized_reason_code = _require_text(reason_code, field_name="reason_code")
        normalized_effective_from = (
            _now()
            if effective_from is None
            else _normalize_as_of(
                effective_from,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_from",
            )
        )
        normalized_effective_to = (
            normalized_effective_from
            if normalized_override_state == "reset" and effective_to is None
            else (
                None
                if effective_to is None
                else _normalize_as_of(
                    effective_to,
                    error_type=ValueError,
                    reason_code="operator_control.invalid_effective_to",
                )
            )
        )
        if (
            normalized_effective_to is not None
            and normalized_override_state != "reset"
            and normalized_effective_to <= normalized_effective_from
        ):
            raise ValueError("effective_to must be later than effective_from")
        normalized_rationale = (
            _optional_text(rationale, field_name="rationale")
            or _default_circuit_breaker_rationale(
                provider_slug=normalized_provider_slug,
                override_state=normalized_override_state,
                effective_to=normalized_effective_to,
            )
        )
        normalized_decided_by = (
            _optional_text(decided_by, field_name="decided_by")
            or "workflow circuits"
        )
        source_suffix = _scope_fragment(normalized_reason_code, fallback="operator-control")
        normalized_decision_source = (
            _optional_text(decision_source, field_name="decision_source")
            or f"workflow.circuits.{source_suffix}"
        )
        decision = OperatorDecisionAuthorityRecord(
            operator_decision_id=_circuit_breaker_operator_decision_id(
                normalized_provider_slug,
                override_state=normalized_override_state,
                effective_from=normalized_effective_from,
            ),
            decision_key=_circuit_breaker_decision_key(
                normalized_provider_slug,
                effective_from=normalized_effective_from,
            ),
            decision_kind=_circuit_breaker_decision_kind(normalized_override_state),
            decision_status=(
                "inactive" if normalized_override_state == "reset" else "active"
            ),
            title=_circuit_breaker_override_title(
                normalized_provider_slug,
                normalized_override_state,
            ),
            rationale=normalized_rationale,
            decided_by=normalized_decided_by,
            decision_source=normalized_decision_source,
            effective_from=normalized_effective_from,
            effective_to=normalized_effective_to,
            decided_at=normalized_effective_from,
            created_at=normalized_effective_from,
            updated_at=_now(),
            decision_scope_kind="provider",
            decision_scope_ref=normalized_provider_slug,
        )

        conn = await self.connect_database(env)
        try:
            assert self.operator_control_repository_factory is not None
            repository = self.operator_control_repository_factory(conn)
            async with conn.transaction():
                persisted = await repository.record_operator_decision(
                    operator_decision=decision,
                )
                await self._sync_semantic_bridge_for_operator_decision(
                    conn,
                    decision=persisted,
                    emitted_by="operator_write.set_circuit_breaker_override",
                )
                await aemit_cache_invalidation(
                    conn,
                    cache_kind=CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE,
                    cache_key=persisted.decision_scope_ref or normalized_provider_slug,
                    reason=f"circuit_breaker_override_{normalized_override_state}",
                    invalidated_by="operator_write.set_circuit_breaker_override",
                    decision_ref=persisted.operator_decision_id,
                )
        finally:
            await conn.close()

        invalidate_circuit_breaker_override_cache()
        return _circuit_breaker_override_record_from_decision(persisted)

    async def _record_architecture_policy_decision(
        self,
        *,
        env: Mapping[str, str] | None,
        authority_domain: str,
        policy_slug: str,
        title: str,
        rationale: str,
        decided_by: str,
        decision_source: str,
        effective_from: datetime | None,
        effective_to: datetime | None,
        decided_at: datetime | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> ArchitecturePolicyDecisionRecord:
        normalized_authority_domain = _normalize_authority_domain_scope_ref(authority_domain)
        normalized_policy_slug = _normalize_architecture_policy_slug(policy_slug)
        normalized_title = _require_text(title, field_name="title")
        normalized_rationale = _require_text(rationale, field_name="rationale")
        normalized_decided_by = _require_text(decided_by, field_name="decided_by")
        normalized_decision_source = _require_text(
            decision_source,
            field_name="decision_source",
        )
        normalized_effective_from = (
            _now()
            if effective_from is None
            else _normalize_as_of(
                effective_from,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_from",
            )
        )
        normalized_effective_to = (
            None
            if effective_to is None
            else _normalize_as_of(
                effective_to,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_to",
            )
        )
        if (
            normalized_effective_to is not None
            and normalized_effective_to <= normalized_effective_from
        ):
            raise ValueError("effective_to must be later than effective_from")
        normalized_decided_at = (
            normalized_effective_from
            if decided_at is None
            else _normalize_as_of(
                decided_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_decided_at",
            )
        )
        normalized_created_at = (
            normalized_effective_from
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_created_at",
            )
        )
        normalized_updated_at = (
            _now()
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="operator_control.invalid_updated_at",
            )
        )
        decision = OperatorDecisionAuthorityRecord(
            operator_decision_id=_architecture_policy_operator_decision_id(
                normalized_authority_domain,
                normalized_policy_slug,
            ),
            decision_key=_architecture_policy_decision_key(
                normalized_authority_domain,
                normalized_policy_slug,
            ),
            decision_kind="architecture_policy",
            decision_status="decided",
            title=normalized_title,
            rationale=normalized_rationale,
            decided_by=normalized_decided_by,
            decision_source=normalized_decision_source,
            effective_from=normalized_effective_from,
            effective_to=normalized_effective_to,
            decided_at=normalized_decided_at,
            created_at=normalized_created_at,
            updated_at=normalized_updated_at,
            decision_scope_kind="authority_domain",
            decision_scope_ref=normalized_authority_domain,
        )

        conn = await self.connect_database(env)
        try:
            assert self.operator_control_repository_factory is not None
            repository = self.operator_control_repository_factory(conn)
            async with conn.transaction():
                persisted = await repository.record_operator_decision(
                    operator_decision=decision,
                )
                await self._sync_semantic_bridge_for_operator_decision(
                    conn,
                    decision=persisted,
                    emitted_by="operator_write.record_architecture_policy_decision",
                )
        finally:
            await conn.close()

        return _architecture_policy_record_from_decision(persisted)

    async def record_architecture_policy_decision_async(
        self,
        *,
        authority_domain: str,
        policy_slug: str,
        title: str,
        rationale: str,
        decided_by: str,
        decision_source: str,
        effective_from: datetime | None = None,
        effective_to: datetime | None = None,
        decided_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        record = await self._record_architecture_policy_decision(
            env=env,
            authority_domain=authority_domain,
            policy_slug=policy_slug,
            title=title,
            rationale=rationale,
            decided_by=decided_by,
            decision_source=decision_source,
            effective_from=effective_from,
            effective_to=effective_to,
            decided_at=decided_at,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"architecture_policy_decision": record.to_json()}

    async def set_circuit_breaker_override_async(
        self,
        *,
        provider_slug: str,
        override_state: str,
        effective_to: datetime | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decided_by: str | None = None,
        decision_source: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        record = await self._set_circuit_breaker_override(
            env=env,
            provider_slug=provider_slug,
            override_state=override_state,
            effective_to=effective_to,
            reason_code=reason_code,
            rationale=rationale,
            effective_from=effective_from,
            decided_by=decided_by,
            decision_source=decision_source,
        )
        return {"circuit_breaker_override": record.to_json()}

    async def set_task_route_eligibility_window_async(
        self,
        *,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None = None,
        task_type: str | None = None,
        model_slug: str | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decision_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        result = await self._set_task_route_eligibility_window(
            env=env,
            provider_slug=provider_slug,
            eligibility_status=eligibility_status,
            effective_to=effective_to,
            task_type=task_type,
            model_slug=model_slug,
            reason_code=reason_code,
            rationale=rationale,
            effective_from=effective_from,
            decision_ref=decision_ref,
        )
        return result.to_json()

    async def record_issue_async(
        self,
        *,
        title: str,
        summary: str,
        severity: str = "medium",
        priority: str = "p2",
        source_kind: str = "manual",
        issue_id: str | None = None,
        issue_key: str | None = None,
        status: str | None = None,
        owner_ref: str | None = None,
        decision_ref: str | None = None,
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        opened_at: datetime | None = None,
        resolved_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return {
            "issue": await self._record_issue(
                env=env,
                title=title,
                summary=summary,
                severity=severity,
                priority=priority,
                source_kind=source_kind,
                issue_id=issue_id,
                issue_key=issue_key,
                status=status,
                owner_ref=owner_ref,
                decision_ref=decision_ref,
                discovered_in_run_id=discovered_in_run_id,
                discovered_in_receipt_id=discovered_in_receipt_id,
                opened_at=opened_at,
                resolved_at=resolved_at,
                created_at=created_at,
                updated_at=updated_at,
            )
        }

    async def operator_ideas_async(
        self,
        *,
        action: str = "list",
        idea_id: str | None = None,
        idea_key: str | None = None,
        title: str | None = None,
        summary: str | None = None,
        source_kind: str = "operator",
        source_ref: str | None = None,
        owner_ref: str | None = None,
        decision_ref: str | None = None,
        status: str | None = None,
        resolution_summary: str | None = None,
        roadmap_item_id: str | None = None,
        promoted_by: str | None = None,
        opened_at: datetime | None = None,
        resolved_at: datetime | None = None,
        promoted_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        idea_ids: tuple[str, ...] | list[str] | None = None,
        open_only: bool = True,
        limit: int = 50,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._operator_ideas(
            env=env,
            action=action,
            idea_id=idea_id,
            idea_key=idea_key,
            title=title,
            summary=summary,
            source_kind=source_kind,
            source_ref=source_ref,
            owner_ref=owner_ref,
            decision_ref=decision_ref,
            status=status,
            resolution_summary=resolution_summary,
            roadmap_item_id=roadmap_item_id,
            promoted_by=promoted_by,
            opened_at=opened_at,
            resolved_at=resolved_at,
            promoted_at=promoted_at,
            created_at=created_at,
            updated_at=updated_at,
            idea_ids=_coerce_text_sequence(idea_ids, field_name="idea_ids"),
            open_only=open_only,
            limit=limit,
        )

    async def roadmap_write_async(
        self,
        *,
        action: str = "preview",
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] | list[str] | None = None,
        source_bug_id: str | None = None,
        source_idea_id: str | None = None,
        registry_paths: tuple[str, ...] | list[str] | None = None,
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        lifecycle: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
        proof_kind: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._roadmap_write(
            env=env,
            action=action,
            title=title,
            intent_brief=intent_brief,
            template=template,
            priority=priority,
            parent_roadmap_item_id=parent_roadmap_item_id,
            slug=slug,
            depends_on=_coerce_text_sequence(depends_on, field_name="depends_on"),
            source_bug_id=source_bug_id,
            source_idea_id=source_idea_id,
            registry_paths=_coerce_text_sequence(registry_paths, field_name="registry_paths"),
            decision_ref=decision_ref,
            item_kind=item_kind,
            status=status,
            lifecycle=lifecycle,
            tier=tier,
            phase_ready=phase_ready,
            approval_tag=approval_tag,
            reference_doc=reference_doc,
            outcome_gate=outcome_gate,
            proof_kind=proof_kind,
        )

    async def reconcile_work_item_closeout_async(
        self,
        *,
        action: str = "preview",
        bug_ids: tuple[str, ...] | list[str] | None = None,
        roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
        env: Mapping[str, str] | None = None,
        ) -> dict[str, Any]:
        return await self._reconcile_work_item_closeout(
            env=env,
            action=action,
            bug_ids=_coerce_text_sequence(bug_ids, field_name="bug_ids"),
            roadmap_item_ids=_coerce_text_sequence(
                roadmap_item_ids,
                field_name="roadmap_item_ids",
            ),
        )

    async def record_operator_decision_async(
        self,
        *,
        decision_key: str,
        decision_kind: str,
        title: str,
        rationale: str,
        decided_by: str,
        decision_source: str,
        decision_status: str = "decided",
        effective_from: datetime | None = None,
        effective_to: datetime | None = None,
        decision_scope_kind: str | None = None,
        decision_scope_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_decision_key = _require_text(
            decision_key,
            field_name="decision_key",
        )
        normalized_decision_kind = _require_text(
            decision_kind,
            field_name="decision_kind",
        )
        normalized_effective_from = (
            _now()
            if effective_from is None
            else _normalize_as_of(
                effective_from,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_from",
            )
        )
        normalized_effective_to = (
            None
            if effective_to is None
            else _normalize_as_of(
                effective_to,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_to",
            )
        )
        if (
            normalized_effective_to is not None
            and normalized_effective_to < normalized_effective_from
        ):
            raise ValueError("effective_to must be later than or equal to effective_from")
        now = _now()
        decision = OperatorDecisionAuthorityRecord(
            operator_decision_id=_operator_decision_id_from_key(
                decision_kind=normalized_decision_kind,
                decision_key=normalized_decision_key,
            ),
            decision_key=normalized_decision_key,
            decision_kind=normalized_decision_kind,
            decision_status=_require_text(
                decision_status,
                field_name="decision_status",
            ),
            title=_require_text(title, field_name="title"),
            rationale=_require_text(rationale, field_name="rationale"),
            decided_by=_require_text(decided_by, field_name="decided_by"),
            decision_source=_require_text(
                decision_source,
                field_name="decision_source",
            ),
            effective_from=normalized_effective_from,
            effective_to=normalized_effective_to,
            decided_at=normalized_effective_from,
            created_at=normalized_effective_from,
            updated_at=now,
            decision_scope_kind=_optional_text(
                decision_scope_kind,
                field_name="decision_scope_kind",
            ),
            decision_scope_ref=_optional_text(
                decision_scope_ref,
                field_name="decision_scope_ref",
            ),
        )

        conn = await self.connect_database(env)
        try:
            assert self.operator_control_repository_factory is not None
            repository = self.operator_control_repository_factory(conn)
            async with conn.transaction():
                persisted = await repository.record_operator_decision(
                    operator_decision=decision,
                )
                await self._sync_semantic_bridge_for_operator_decision(
                    conn,
                    decision=persisted,
                    emitted_by="operator_write.record_operator_decision",
                )
        finally:
            await conn.close()
        return {"operator_decision": _operator_decision_to_json(persisted)}

    async def backfill_semantic_bridges_async(
        self,
        *,
        include_object_relations: bool = True,
        include_operator_decisions: bool = True,
        include_roadmap_items: bool = True,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        if (
            not include_object_relations
            and not include_operator_decisions
            and not include_roadmap_items
        ):
            raise ValueError(
                "backfill_semantic_bridges requires include_object_relations or "
                "include_operator_decisions or include_roadmap_items"
            )
        normalized_as_of = (
            None
            if as_of is None
            else _normalize_as_of(
                as_of,
                error_type=ValueError,
                reason_code="operator_control.invalid_as_of",
            )
        )
        conn = await self.connect_database(env)
        try:
            relation_summary = {
                "processed": 0,
                "recorded": 0,
                "retracted": 0,
                "tombstoned": 0,
            }
            decision_summary = {
                "processed": 0,
                "recorded": 0,
                "skipped_unscoped": 0,
            }
            roadmap_summary = {
                "processed": 0,
                "recorded": 0,
                "retracted": 0,
            }
            projection_as_of = normalized_as_of or _now()
            async with conn.transaction():
                if include_object_relations:
                    assert self.object_relation_repository_factory is not None
                    relation_repository = self.object_relation_repository_factory(conn)
                    relation_rows = await relation_repository.list_relations(
                        as_of=normalized_as_of,
                    )
                    for relation in sorted(
                        relation_rows,
                        key=lambda record: (
                            record.created_at,
                            record.updated_at,
                            record.operator_object_relation_id,
                        ),
                    ):
                        relation_summary["processed"] += 1
                        outcome = await self._sync_semantic_bridge_for_relation(
                            conn,
                            relation=relation,
                        )
                        relation_summary[outcome] += 1
                if include_operator_decisions:
                    assert self.operator_control_repository_factory is not None
                    operator_repository = self.operator_control_repository_factory(conn)
                    if not hasattr(
                        operator_repository,
                        "fetch_operator_decisions_for_semantic_bridge",
                    ):
                        raise ValueError(
                            "operator_control_repository_factory must provide "
                            "fetch_operator_decisions_for_semantic_bridge for semantic replay"
                        )
                    decision_rows = await operator_repository.fetch_operator_decisions_for_semantic_bridge(  # type: ignore[attr-defined]
                        as_of=normalized_as_of,
                    )
                    for decision in decision_rows:
                        decision_summary["processed"] += 1
                        outcome = await self._sync_semantic_bridge_for_operator_decision(
                            conn,
                            decision=decision,
                            emitted_by="operator_write.backfill_semantic_bridges",
                        )
                        decision_summary[outcome] += 1
                if include_roadmap_items:
                    roadmap_rows = await self._fetch_roadmap_items_for_semantic_bridge(
                        conn,
                        as_of=normalized_as_of,
                    )
                    for roadmap_item in roadmap_rows:
                        item_summary = await self._sync_semantic_bridges_for_roadmap_item(
                            conn,
                            roadmap_item=roadmap_item,
                            emitted_by="operator_write.backfill_semantic_bridges",
                        )
                        for key in roadmap_summary:
                            roadmap_summary[key] += item_summary[key]
                assert self.semantic_assertion_repository_factory is not None
                semantic_repository = self.semantic_assertion_repository_factory(conn)
                await semantic_repository.rebuild_current_assertions(
                    as_of=projection_as_of,
                )
                await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_bridge_backfilled",
                    entity_id="operator_control",
                    entity_kind="operator_control",
                    payload={
                        "bridge_source": "operator_control",
                        "as_of": (
                            None
                            if normalized_as_of is None
                            else normalized_as_of.isoformat()
                        ),
                        "object_relations": dict(relation_summary),
                        "operator_decisions": dict(decision_summary),
                        "roadmap_items": dict(roadmap_summary),
                    },
                    emitted_by="operator_write.backfill_semantic_bridges",
                )
        finally:
            await conn.close()
        authority_memory_refresh: dict[str, Any]
        try:
            from runtime.authority_memory_projection import (
                refresh_authority_memory_projection,
            )

            refresh_result = await refresh_authority_memory_projection(
                env=env,
                as_of=projection_as_of,
            )
            authority_memory_refresh = refresh_result.to_json()
        except Exception as exc:  # noqa: BLE001 - projection refresh is best-effort
            logger.warning(
                "authority memory projection refresh failed after semantic bridge backfill: %s",
                exc,
            )
            authority_memory_refresh = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        return {
            "semantic_bridge_backfill": {
                "as_of": None if normalized_as_of is None else normalized_as_of.isoformat(),
                "object_relations": relation_summary,
                "operator_decisions": decision_summary,
                "roadmap_items": roadmap_summary,
                "authority_memory_refresh": authority_memory_refresh,
            }
        }

    async def list_operator_decisions_async(
        self,
        *,
        as_of: datetime | None = None,
        decision_kind: str | None = None,
        decision_source: str | None = None,
        decision_scope_kind: str | None = None,
        decision_scope_ref: str | None = None,
        active_only: bool = True,
        limit: int = 100,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_as_of = (
            _now()
            if as_of is None
            else _normalize_as_of(
                as_of,
                error_type=ValueError,
                reason_code="operator_control.invalid_as_of",
            )
        )
        normalized_decision_kind = (
            None if decision_kind is None else _require_text(decision_kind, field_name="decision_kind")
        )
        normalized_decision_source = (
            None
            if decision_source is None
            else _require_text(decision_source, field_name="decision_source")
        )
        normalized_scope_kind = (
            None
            if decision_scope_kind is None
            else _require_text(decision_scope_kind, field_name="decision_scope_kind")
        )
        normalized_scope_ref = (
            None
            if decision_scope_ref is None
            else _require_text(decision_scope_ref, field_name="decision_scope_ref")
        )
        normalized_limit = max(1, int(limit or 100))

        conn = await self.connect_database(env)
        try:
            assert self.operator_control_repository_factory is not None
            repository = self.operator_control_repository_factory(conn)
            rows = await repository.list_operator_decisions(
                decision_kind=normalized_decision_kind,
                decision_source=normalized_decision_source,
                decision_scope_kind=normalized_scope_kind,
                decision_scope_ref=normalized_scope_ref,
                active_only=active_only,
                as_of=normalized_as_of,
                limit=normalized_limit,
            )
        finally:
            await conn.close()
        return {
            "operator_decisions": [
                _operator_decision_to_json(row)
                for row in rows
            ],
            "as_of": normalized_as_of.isoformat(),
            "filters": {
                "decision_kind": normalized_decision_kind,
                "decision_source": normalized_decision_source,
                "decision_scope_kind": normalized_scope_kind,
                "decision_scope_ref": normalized_scope_ref,
                "active_only": active_only,
                "limit": normalized_limit,
            },
        }

    def set_circuit_breaker_override(
        self,
        *,
        provider_slug: str,
        override_state: str,
        effective_to: datetime | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decided_by: str | None = None,
        decision_source: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.set_circuit_breaker_override_async(
                provider_slug=provider_slug,
                override_state=override_state,
                effective_to=effective_to,
                reason_code=reason_code,
                rationale=rationale,
                effective_from=effective_from,
                decided_by=decided_by,
                decision_source=decision_source,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def record_operator_decision(
        self,
        *,
        decision_key: str,
        decision_kind: str,
        title: str,
        rationale: str,
        decided_by: str,
        decision_source: str,
        decision_status: str = "decided",
        effective_from: datetime | None = None,
        effective_to: datetime | None = None,
        decision_scope_kind: str | None = None,
        decision_scope_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.record_operator_decision_async(
                decision_key=decision_key,
                decision_kind=decision_kind,
                title=title,
                rationale=rationale,
                decided_by=decided_by,
                decision_source=decision_source,
                decision_status=decision_status,
                effective_from=effective_from,
                effective_to=effective_to,
                decision_scope_kind=decision_scope_kind,
                decision_scope_ref=decision_scope_ref,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def backfill_semantic_bridges(
        self,
        *,
        include_object_relations: bool = True,
        include_operator_decisions: bool = True,
        include_roadmap_items: bool = True,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.backfill_semantic_bridges_async(
                include_object_relations=include_object_relations,
                include_operator_decisions=include_operator_decisions,
                include_roadmap_items=include_roadmap_items,
                as_of=as_of,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def list_operator_decisions(
        self,
        *,
        as_of: datetime | None = None,
        decision_kind: str | None = None,
        decision_source: str | None = None,
        decision_scope_kind: str | None = None,
        decision_scope_ref: str | None = None,
        active_only: bool = True,
        limit: int = 100,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.list_operator_decisions_async(
                as_of=as_of,
                decision_kind=decision_kind,
                decision_source=decision_source,
                decision_scope_kind=decision_scope_kind,
                decision_scope_ref=decision_scope_ref,
                active_only=active_only,
                limit=limit,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def record_architecture_policy_decision(
        self,
        *,
        authority_domain: str,
        policy_slug: str,
        title: str,
        rationale: str,
        decided_by: str,
        decision_source: str,
        effective_from: datetime | None = None,
        effective_to: datetime | None = None,
        decided_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.record_architecture_policy_decision_async(
                authority_domain=authority_domain,
                policy_slug=policy_slug,
                title=title,
                rationale=rationale,
                decided_by=decided_by,
                decision_source=decision_source,
                effective_from=effective_from,
                effective_to=effective_to,
                decided_at=decided_at,
                created_at=created_at,
                updated_at=updated_at,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def set_task_route_eligibility_window(
        self,
        *,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None = None,
        task_type: str | None = None,
        model_slug: str | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decision_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        result = _run_async(
            self._set_task_route_eligibility_window(
                env=env,
                provider_slug=provider_slug,
                eligibility_status=eligibility_status,
                effective_to=effective_to,
                task_type=task_type,
                model_slug=model_slug,
                reason_code=reason_code,
                rationale=rationale,
                effective_from=effective_from,
                decision_ref=decision_ref,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return result.to_json()

    def reconcile_work_item_closeout(
        self,
        *,
        action: str = "preview",
        bug_ids: tuple[str, ...] | list[str] | None = None,
        roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.reconcile_work_item_closeout_async(
                action=action,
                bug_ids=bug_ids,
                roadmap_item_ids=roadmap_item_ids,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def record_issue(
        self,
        *,
        title: str,
        summary: str,
        severity: str = "medium",
        priority: str = "p2",
        source_kind: str = "manual",
        issue_id: str | None = None,
        issue_key: str | None = None,
        status: str | None = None,
        owner_ref: str | None = None,
        decision_ref: str | None = None,
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        opened_at: datetime | None = None,
        resolved_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.record_issue_async(
                title=title,
                summary=summary,
                severity=severity,
                priority=priority,
                source_kind=source_kind,
                issue_id=issue_id,
                issue_key=issue_key,
                status=status,
                owner_ref=owner_ref,
                decision_ref=decision_ref,
                discovered_in_run_id=discovered_in_run_id,
                discovered_in_receipt_id=discovered_in_receipt_id,
                opened_at=opened_at,
                resolved_at=resolved_at,
                created_at=created_at,
                updated_at=updated_at,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def operator_ideas(
        self,
        *,
        action: str = "list",
        idea_id: str | None = None,
        idea_key: str | None = None,
        title: str | None = None,
        summary: str | None = None,
        source_kind: str = "operator",
        source_ref: str | None = None,
        owner_ref: str | None = None,
        decision_ref: str | None = None,
        status: str | None = None,
        resolution_summary: str | None = None,
        roadmap_item_id: str | None = None,
        promoted_by: str | None = None,
        opened_at: datetime | None = None,
        resolved_at: datetime | None = None,
        promoted_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        idea_ids: tuple[str, ...] | list[str] | None = None,
        open_only: bool = True,
        limit: int = 50,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.operator_ideas_async(
                action=action,
                idea_id=idea_id,
                idea_key=idea_key,
                title=title,
                summary=summary,
                source_kind=source_kind,
                source_ref=source_ref,
                owner_ref=owner_ref,
                decision_ref=decision_ref,
                status=status,
                resolution_summary=resolution_summary,
                roadmap_item_id=roadmap_item_id,
                promoted_by=promoted_by,
                opened_at=opened_at,
                resolved_at=resolved_at,
                promoted_at=promoted_at,
                created_at=created_at,
                updated_at=updated_at,
                idea_ids=idea_ids,
                open_only=open_only,
                limit=limit,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def roadmap_write(
        self,
        *,
        action: str = "preview",
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] | list[str] | None = None,
        source_bug_id: str | None = None,
        source_idea_id: str | None = None,
        registry_paths: tuple[str, ...] | list[str] | None = None,
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        lifecycle: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
        proof_kind: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.roadmap_write_async(
                action=action,
                title=title,
                intent_brief=intent_brief,
                template=template,
                priority=priority,
                parent_roadmap_item_id=parent_roadmap_item_id,
                slug=slug,
                depends_on=depends_on,
                source_bug_id=source_bug_id,
                source_idea_id=source_idea_id,
                registry_paths=registry_paths,
                decision_ref=decision_ref,
                item_kind=item_kind,
                status=status,
                lifecycle=lifecycle,
                tier=tier,
                phase_ready=phase_ready,
                approval_tag=approval_tag,
                reference_doc=reference_doc,
                outcome_gate=outcome_gate,
                proof_kind=proof_kind,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    async def record_functional_area_async(
        self,
        *,
        area_slug: str,
        title: str,
        summary: str,
        area_status: str = "active",
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical functional area in async contexts."""

        record = await self._record_functional_area(
            env=env,
            area_slug=area_slug,
            title=title,
            summary=summary,
            area_status=area_status,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"functional_area": record.to_json()}

    async def record_operator_object_relation_async(
        self,
        *,
        relation_kind: str,
        source_kind: str,
        source_ref: str,
        target_kind: str,
        target_ref: str,
        relation_status: str = "active",
        relation_metadata: Mapping[str, Any] | None = None,
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical cross-object semantic relation in async contexts."""

        record = await self._record_operator_object_relation(
            env=env,
            relation_kind=relation_kind,
            source_kind=source_kind,
            source_ref=source_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            relation_status=relation_status,
            relation_metadata=relation_metadata,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"operator_object_relation": record.to_json()}

    async def record_work_item_workflow_binding_async(
        self,
        *,
        binding_kind: str,
        issue_id: str | None = None,
        bug_id: str | None = None,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        workflow_run_id: str | None = None,
        binding_status: str = "active",
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical work-item workflow binding in async contexts."""

        record, auto_promoted_bug, auto_promoted_roadmap = await self._record_work_item_workflow_binding(
            env=env,
            binding_kind=binding_kind,
            issue_id=issue_id,
            bug_id=bug_id,
            roadmap_item_id=roadmap_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
            binding_status=binding_status,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        payload: dict[str, Any] = {"binding": record.to_json()}
        if auto_promoted_bug is not None:
            payload["auto_promoted_bug"] = auto_promoted_bug
        if auto_promoted_roadmap is not None:
            payload["auto_promoted_roadmap"] = auto_promoted_roadmap
        return payload

    async def admit_native_primary_cutover_gate_async(
        self,
        *,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        title: str | None = None,
        gate_name: str | None = None,
        gate_policy: Mapping[str, Any] | None = None,
        required_evidence: Mapping[str, Any] | None = None,
        decided_at: datetime | None = None,
        opened_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Admit one bounded native-primary cutover gate in async contexts."""

        record = await self._admit_native_primary_cutover_gate(
            env=env,
            decided_by=decided_by,
            decision_source=decision_source,
            rationale=rationale,
            roadmap_item_id=roadmap_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            title=title,
            gate_name=gate_name,
            gate_policy=gate_policy,
            required_evidence=required_evidence,
            decided_at=decided_at,
            opened_at=opened_at,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"native_primary_cutover": record.to_json()}

    def record_work_item_workflow_binding(
        self,
        *,
        binding_kind: str,
        issue_id: str | None = None,
        bug_id: str | None = None,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        workflow_run_id: str | None = None,
        binding_status: str = "active",
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical work-item workflow binding through Postgres."""

        record, auto_promoted_bug, auto_promoted_roadmap = _run_async(
            self._record_work_item_workflow_binding(
                env=env,
                binding_kind=binding_kind,
                issue_id=issue_id,
                bug_id=bug_id,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                workflow_run_id=workflow_run_id,
                binding_status=binding_status,
                bound_by_decision_id=bound_by_decision_id,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        payload: dict[str, Any] = {"binding": record.to_json()}
        if auto_promoted_bug is not None:
            payload["auto_promoted_bug"] = auto_promoted_bug
        if auto_promoted_roadmap is not None:
            payload["auto_promoted_roadmap"] = auto_promoted_roadmap
        return payload

    def record_functional_area(
        self,
        *,
        area_slug: str,
        title: str,
        summary: str,
        area_status: str = "active",
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical functional area through Postgres."""

        record = _run_async(
            self._record_functional_area(
                env=env,
                area_slug=area_slug,
                title=title,
                summary=summary,
                area_status=area_status,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return {"functional_area": record.to_json()}

    def record_operator_object_relation(
        self,
        *,
        relation_kind: str,
        source_kind: str,
        source_ref: str,
        target_kind: str,
        target_ref: str,
        relation_status: str = "active",
        relation_metadata: Mapping[str, Any] | None = None,
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical cross-object semantic relation through Postgres."""

        record = _run_async(
            self._record_operator_object_relation(
                env=env,
                relation_kind=relation_kind,
                source_kind=source_kind,
                source_ref=source_ref,
                target_kind=target_kind,
                target_ref=target_ref,
                relation_status=relation_status,
                relation_metadata=relation_metadata,
                bound_by_decision_id=bound_by_decision_id,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return {"operator_object_relation": record.to_json()}

    def admit_native_primary_cutover_gate(
        self,
        *,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        title: str | None = None,
        gate_name: str | None = None,
        gate_policy: Mapping[str, Any] | None = None,
        required_evidence: Mapping[str, Any] | None = None,
        decided_at: datetime | None = None,
        opened_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Admit one bounded native-primary cutover gate through Postgres."""

        record = _run_async(
            self._admit_native_primary_cutover_gate(
                env=env,
                decided_by=decided_by,
                decision_source=decision_source,
                rationale=rationale,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                title=title,
                gate_name=gate_name,
                gate_policy=gate_policy,
                required_evidence=required_evidence,
                decided_at=decided_at,
                opened_at=opened_at,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return {"native_primary_cutover": record.to_json()}


def record_work_item_workflow_binding(
    *,
    binding_kind: str,
    issue_id: str | None = None,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
    binding_status: str = "active",
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical work-item workflow binding through the default frontdoor."""

    return OperatorControlFrontdoor().record_work_item_workflow_binding(
        binding_kind=binding_kind,
        issue_id=issue_id,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
        binding_status=binding_status,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def record_functional_area(
    *,
    area_slug: str,
    title: str,
    summary: str,
    area_status: str = "active",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical functional area through the default frontdoor."""

    return OperatorControlFrontdoor().record_functional_area(
        area_slug=area_slug,
        title=title,
        summary=summary,
        area_status=area_status,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_functional_area(
    *,
    area_slug: str,
    title: str,
    summary: str,
    area_status: str = "active",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical functional area through the default async frontdoor."""

    return await OperatorControlFrontdoor().record_functional_area_async(
        area_slug=area_slug,
        title=title,
        summary=summary,
        area_status=area_status,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def record_operator_object_relation(
    *,
    relation_kind: str,
    source_kind: str,
    source_ref: str,
    target_kind: str,
    target_ref: str,
    relation_status: str = "active",
    relation_metadata: Mapping[str, Any] | None = None,
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical cross-object semantic relation through the default frontdoor."""

    return OperatorControlFrontdoor().record_operator_object_relation(
        relation_kind=relation_kind,
        source_kind=source_kind,
        source_ref=source_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        relation_status=relation_status,
        relation_metadata=relation_metadata,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_operator_object_relation(
    *,
    relation_kind: str,
    source_kind: str,
    source_ref: str,
    target_kind: str,
    target_ref: str,
    relation_status: str = "active",
    relation_metadata: Mapping[str, Any] | None = None,
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical cross-object semantic relation through the default async frontdoor."""

    return await OperatorControlFrontdoor().record_operator_object_relation_async(
        relation_kind=relation_kind,
        source_kind=source_kind,
        source_ref=source_ref,
        target_kind=target_kind,
        target_ref=target_ref,
        relation_status=relation_status,
        relation_metadata=relation_metadata,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_work_item_workflow_binding(
    *,
    binding_kind: str,
    issue_id: str | None = None,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
    binding_status: str = "active",
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical work-item workflow binding through the default async frontdoor."""

    return await OperatorControlFrontdoor().record_work_item_workflow_binding_async(
        binding_kind=binding_kind,
        issue_id=issue_id,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
        binding_status=binding_status,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def set_circuit_breaker_override(
    *,
    provider_slug: str,
    override_state: str,
    effective_to: datetime | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decided_by: str | None = None,
    decision_source: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Persist one durable circuit-breaker override through the default frontdoor."""

    return OperatorControlFrontdoor().set_circuit_breaker_override(
        provider_slug=provider_slug,
        override_state=override_state,
        effective_to=effective_to,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decided_by=decided_by,
        decision_source=decision_source,
        env=env,
    )


def record_operator_decision(
    *,
    decision_key: str,
    decision_kind: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
    decision_status: str = "decided",
    effective_from: datetime | None = None,
    effective_to: datetime | None = None,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical operator decision row through the default frontdoor."""

    return OperatorControlFrontdoor().record_operator_decision(
        decision_key=decision_key,
        decision_kind=decision_kind,
        title=title,
        rationale=rationale,
        decided_by=decided_by,
        decision_source=decision_source,
        decision_status=decision_status,
        effective_from=effective_from,
        effective_to=effective_to,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
        env=env,
    )


async def arecord_operator_decision(
    *,
    decision_key: str,
    decision_kind: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
    decision_status: str = "decided",
    effective_from: datetime | None = None,
    effective_to: datetime | None = None,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical operator decision row in async contexts."""

    return await OperatorControlFrontdoor().record_operator_decision_async(
        decision_key=decision_key,
        decision_kind=decision_kind,
        title=title,
        rationale=rationale,
        decided_by=decided_by,
        decision_source=decision_source,
        decision_status=decision_status,
        effective_from=effective_from,
        effective_to=effective_to,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
        env=env,
    )


def backfill_semantic_bridges(
    *,
    include_object_relations: bool = True,
    include_operator_decisions: bool = True,
    include_roadmap_items: bool = True,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Replay legacy operator semantic rows into semantic_assertions."""

    return OperatorControlFrontdoor().backfill_semantic_bridges(
        include_object_relations=include_object_relations,
        include_operator_decisions=include_operator_decisions,
        include_roadmap_items=include_roadmap_items,
        as_of=as_of,
        env=env,
    )


async def abackfill_semantic_bridges(
    *,
    include_object_relations: bool = True,
    include_operator_decisions: bool = True,
    include_roadmap_items: bool = True,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Replay legacy operator semantic rows into semantic_assertions in async contexts."""

    return await OperatorControlFrontdoor().backfill_semantic_bridges_async(
        include_object_relations=include_object_relations,
        include_operator_decisions=include_operator_decisions,
        include_roadmap_items=include_roadmap_items,
        as_of=as_of,
        env=env,
    )


def list_operator_decisions(
    *,
    as_of: datetime | None = None,
    decision_kind: str | None = None,
    decision_source: str | None = None,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
    active_only: bool = True,
    limit: int = 100,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """List effective operator decisions through the default frontdoor."""

    return OperatorControlFrontdoor().list_operator_decisions(
        as_of=as_of,
        decision_kind=decision_kind,
        decision_source=decision_source,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
        active_only=active_only,
        limit=limit,
        env=env,
    )


async def alist_operator_decisions(
    *,
    as_of: datetime | None = None,
    decision_kind: str | None = None,
    decision_source: str | None = None,
    decision_scope_kind: str | None = None,
    decision_scope_ref: str | None = None,
    active_only: bool = True,
    limit: int = 100,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """List effective operator decisions in async contexts."""

    return await OperatorControlFrontdoor().list_operator_decisions_async(
        as_of=as_of,
        decision_kind=decision_kind,
        decision_source=decision_source,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
        active_only=active_only,
        limit=limit,
        env=env,
    )


async def aset_circuit_breaker_override(
    *,
    provider_slug: str,
    override_state: str,
    effective_to: datetime | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decided_by: str | None = None,
    decision_source: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Persist one durable circuit-breaker override in async contexts."""

    return await OperatorControlFrontdoor().set_circuit_breaker_override_async(
        provider_slug=provider_slug,
        override_state=override_state,
        effective_to=effective_to,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decided_by=decided_by,
        decision_source=decision_source,
        env=env,
    )


def record_architecture_policy_decision(
    *,
    authority_domain: str,
    policy_slug: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
    effective_from: datetime | None = None,
    effective_to: datetime | None = None,
    decided_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Persist one durable architecture-policy decision through the default frontdoor."""

    return OperatorControlFrontdoor().record_architecture_policy_decision(
        authority_domain=authority_domain,
        policy_slug=policy_slug,
        title=title,
        rationale=rationale,
        decided_by=decided_by,
        decision_source=decision_source,
        effective_from=effective_from,
        effective_to=effective_to,
        decided_at=decided_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_architecture_policy_decision(
    *,
    authority_domain: str,
    policy_slug: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
    effective_from: datetime | None = None,
    effective_to: datetime | None = None,
    decided_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Persist one durable architecture-policy decision in async contexts."""

    return await OperatorControlFrontdoor().record_architecture_policy_decision_async(
        authority_domain=authority_domain,
        policy_slug=policy_slug,
        title=title,
        rationale=rationale,
        decided_by=decided_by,
        decision_source=decision_source,
        effective_from=effective_from,
        effective_to=effective_to,
        decided_at=decided_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def admit_native_primary_cutover_gate(
    *,
    decided_by: str,
    decision_source: str,
    rationale: str,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    title: str | None = None,
    gate_name: str | None = None,
    gate_policy: Mapping[str, Any] | None = None,
    required_evidence: Mapping[str, Any] | None = None,
    decided_at: datetime | None = None,
    opened_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Admit one bounded native-primary cutover gate through the default frontdoor."""

    return OperatorControlFrontdoor().admit_native_primary_cutover_gate(
        decided_by=decided_by,
        decision_source=decision_source,
        rationale=rationale,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=decided_at,
        opened_at=opened_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def aadmit_native_primary_cutover_gate(
    *,
    decided_by: str,
    decision_source: str,
    rationale: str,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    title: str | None = None,
    gate_name: str | None = None,
    gate_policy: Mapping[str, Any] | None = None,
    required_evidence: Mapping[str, Any] | None = None,
    decided_at: datetime | None = None,
    opened_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Admit one bounded native-primary cutover gate through the default async frontdoor."""

    return await OperatorControlFrontdoor().admit_native_primary_cutover_gate_async(
        decided_by=decided_by,
        decision_source=decision_source,
        rationale=rationale,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=decided_at,
        opened_at=opened_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def reconcile_work_item_closeout(
    *,
    action: str = "preview",
    bug_ids: tuple[str, ...] | list[str] | None = None,
    roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Preview or commit proof-backed bug and roadmap closeout through the default frontdoor."""

    return OperatorControlFrontdoor().reconcile_work_item_closeout(
        action=action,
        bug_ids=bug_ids,
        roadmap_item_ids=roadmap_item_ids,
        env=env,
    )


async def areconcile_work_item_closeout(
    *,
    action: str = "preview",
    bug_ids: tuple[str, ...] | list[str] | None = None,
    roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Preview or commit proof-backed bug and roadmap closeout through the default async frontdoor."""

    return await OperatorControlFrontdoor().reconcile_work_item_closeout_async(
        action=action,
        bug_ids=bug_ids,
        roadmap_item_ids=roadmap_item_ids,
        env=env,
    )


def record_issue(
    *,
    title: str,
    summary: str,
    severity: str = "medium",
    priority: str = "p2",
    source_kind: str = "manual",
    issue_id: str | None = None,
    issue_key: str | None = None,
    status: str | None = None,
    owner_ref: str | None = None,
    decision_ref: str | None = None,
    discovered_in_run_id: str | None = None,
    discovered_in_receipt_id: str | None = None,
    opened_at: datetime | None = None,
    resolved_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical upstream issue through the default frontdoor."""

    return OperatorControlFrontdoor().record_issue(
        title=title,
        summary=summary,
        severity=severity,
        priority=priority,
        source_kind=source_kind,
        issue_id=issue_id,
        issue_key=issue_key,
        status=status,
        owner_ref=owner_ref,
        decision_ref=decision_ref,
        discovered_in_run_id=discovered_in_run_id,
        discovered_in_receipt_id=discovered_in_receipt_id,
        opened_at=opened_at,
        resolved_at=resolved_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_issue(
    *,
    title: str,
    summary: str,
    severity: str = "medium",
    priority: str = "p2",
    source_kind: str = "manual",
    issue_id: str | None = None,
    issue_key: str | None = None,
    status: str | None = None,
    owner_ref: str | None = None,
    decision_ref: str | None = None,
    discovered_in_run_id: str | None = None,
    discovered_in_receipt_id: str | None = None,
    opened_at: datetime | None = None,
    resolved_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical upstream issue through the default async frontdoor."""

    return await OperatorControlFrontdoor().record_issue_async(
        title=title,
        summary=summary,
        severity=severity,
        priority=priority,
        source_kind=source_kind,
        issue_id=issue_id,
        issue_key=issue_key,
        status=status,
        owner_ref=owner_ref,
        decision_ref=decision_ref,
        discovered_in_run_id=discovered_in_run_id,
        discovered_in_receipt_id=discovered_in_receipt_id,
        opened_at=opened_at,
        resolved_at=resolved_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def operator_ideas(
    *,
    action: str = "list",
    idea_id: str | None = None,
    idea_key: str | None = None,
    title: str | None = None,
    summary: str | None = None,
    source_kind: str = "operator",
    source_ref: str | None = None,
    owner_ref: str | None = None,
    decision_ref: str | None = None,
    status: str | None = None,
    resolution_summary: str | None = None,
    roadmap_item_id: str | None = None,
    promoted_by: str | None = None,
    opened_at: datetime | None = None,
    resolved_at: datetime | None = None,
    promoted_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    idea_ids: tuple[str, ...] | list[str] | None = None,
    open_only: bool = True,
    limit: int = 50,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record, resolve, promote, or list pre-commitment operator ideas."""

    return OperatorControlFrontdoor().operator_ideas(
        action=action,
        idea_id=idea_id,
        idea_key=idea_key,
        title=title,
        summary=summary,
        source_kind=source_kind,
        source_ref=source_ref,
        owner_ref=owner_ref,
        decision_ref=decision_ref,
        status=status,
        resolution_summary=resolution_summary,
        roadmap_item_id=roadmap_item_id,
        promoted_by=promoted_by,
        opened_at=opened_at,
        resolved_at=resolved_at,
        promoted_at=promoted_at,
        created_at=created_at,
        updated_at=updated_at,
        idea_ids=idea_ids,
        open_only=open_only,
        limit=limit,
        env=env,
    )


async def aoperator_ideas(
    *,
    action: str = "list",
    idea_id: str | None = None,
    idea_key: str | None = None,
    title: str | None = None,
    summary: str | None = None,
    source_kind: str = "operator",
    source_ref: str | None = None,
    owner_ref: str | None = None,
    decision_ref: str | None = None,
    status: str | None = None,
    resolution_summary: str | None = None,
    roadmap_item_id: str | None = None,
    promoted_by: str | None = None,
    opened_at: datetime | None = None,
    resolved_at: datetime | None = None,
    promoted_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    idea_ids: tuple[str, ...] | list[str] | None = None,
    open_only: bool = True,
    limit: int = 50,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record, resolve, promote, or list pre-commitment operator ideas."""

    return await OperatorControlFrontdoor().operator_ideas_async(
        action=action,
        idea_id=idea_id,
        idea_key=idea_key,
        title=title,
        summary=summary,
        source_kind=source_kind,
        source_ref=source_ref,
        owner_ref=owner_ref,
        decision_ref=decision_ref,
        status=status,
        resolution_summary=resolution_summary,
        roadmap_item_id=roadmap_item_id,
        promoted_by=promoted_by,
        opened_at=opened_at,
        resolved_at=resolved_at,
        promoted_at=promoted_at,
        created_at=created_at,
        updated_at=updated_at,
        idea_ids=idea_ids,
        open_only=open_only,
        limit=limit,
        env=env,
    )


def roadmap_write(
    *,
    action: str = "preview",
    title: str,
    intent_brief: str,
    template: str = "single_capability",
    priority: str = "p2",
    parent_roadmap_item_id: str | None = None,
    slug: str | None = None,
    depends_on: tuple[str, ...] | list[str] | None = None,
    source_bug_id: str | None = None,
    source_idea_id: str | None = None,
    registry_paths: tuple[str, ...] | list[str] | None = None,
    decision_ref: str | None = None,
    item_kind: str | None = None,
    status: str | None = None,
    lifecycle: str | None = None,
    tier: str | None = None,
    phase_ready: bool | None = None,
    approval_tag: str | None = None,
    reference_doc: str | None = None,
    outcome_gate: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write one roadmap item or packaged roadmap program through the default frontdoor."""

    return OperatorControlFrontdoor().roadmap_write(
        action=action,
        title=title,
        intent_brief=intent_brief,
        template=template,
        priority=priority,
        parent_roadmap_item_id=parent_roadmap_item_id,
        slug=slug,
        depends_on=depends_on,
        source_bug_id=source_bug_id,
        source_idea_id=source_idea_id,
        registry_paths=registry_paths,
        decision_ref=decision_ref,
        item_kind=item_kind,
        status=status,
        lifecycle=lifecycle,
        tier=tier,
        phase_ready=phase_ready,
        approval_tag=approval_tag,
        reference_doc=reference_doc,
        outcome_gate=outcome_gate,
        env=env,
    )


async def aroadmap_write(
    *,
    action: str = "preview",
    title: str,
    intent_brief: str,
    template: str = "single_capability",
    priority: str = "p2",
    parent_roadmap_item_id: str | None = None,
    slug: str | None = None,
    depends_on: tuple[str, ...] | list[str] | None = None,
    source_bug_id: str | None = None,
    source_idea_id: str | None = None,
    registry_paths: tuple[str, ...] | list[str] | None = None,
    decision_ref: str | None = None,
    item_kind: str | None = None,
    status: str | None = None,
    lifecycle: str | None = None,
    tier: str | None = None,
    phase_ready: bool | None = None,
    approval_tag: str | None = None,
    reference_doc: str | None = None,
    outcome_gate: str | None = None,
    proof_kind: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write one roadmap item or packaged roadmap program through the default async frontdoor."""

    return await OperatorControlFrontdoor().roadmap_write_async(
        action=action,
        title=title,
        intent_brief=intent_brief,
        template=template,
        priority=priority,
        parent_roadmap_item_id=parent_roadmap_item_id,
        slug=slug,
        depends_on=depends_on,
        source_bug_id=source_bug_id,
        source_idea_id=source_idea_id,
        registry_paths=registry_paths,
        decision_ref=decision_ref,
        item_kind=item_kind,
        status=status,
        lifecycle=lifecycle,
        tier=tier,
        phase_ready=phase_ready,
        approval_tag=approval_tag,
        reference_doc=reference_doc,
        outcome_gate=outcome_gate,
        proof_kind=proof_kind,
        env=env,
    )


def set_task_route_eligibility_window(
    *,
    provider_slug: str,
    eligibility_status: str,
    effective_to: datetime | None = None,
    task_type: str | None = None,
    model_slug: str | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decision_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one bounded task-route eligibility window through the default frontdoor."""

    return OperatorControlFrontdoor().set_task_route_eligibility_window(
        provider_slug=provider_slug,
        eligibility_status=eligibility_status,
        effective_to=effective_to,
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decision_ref=decision_ref,
        env=env,
    )


async def aset_task_route_eligibility_window(
    *,
    provider_slug: str,
    eligibility_status: str,
    effective_to: datetime | None = None,
    task_type: str | None = None,
    model_slug: str | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decision_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one bounded task-route eligibility window through the default async frontdoor."""

    return await OperatorControlFrontdoor().set_task_route_eligibility_window_async(
        provider_slug=provider_slug,
        eligibility_status=eligibility_status,
        effective_to=effective_to,
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decision_ref=decision_ref,
        env=env,
    )


class NativeWorkflowFlowError(RuntimeError):
    """Raised when the native workflow-flow surface cannot complete safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class _WorkflowFlowSpec:
    flow_name: str
    class_name: str


_FLOW_SPECS: tuple[_WorkflowFlowSpec, ...] = (
    _WorkflowFlowSpec(flow_name="review", class_name="review"),
    _WorkflowFlowSpec(flow_name="repair", class_name="repair"),
    _WorkflowFlowSpec(flow_name="loop", class_name="loop"),
)


@dataclass(frozen=True, slots=True)
class NativeWorkflowFlowRecord:
    """One operator-visible workflow flow resolved from class authority."""

    flow_name: str
    workflow_class: WorkflowClassAuthorityRecord
    as_of: datetime

    @property
    def class_name(self) -> str:
        return self.workflow_class.class_name

    @property
    def class_kind(self) -> str:
        return self.workflow_class.class_kind

    @property
    def workflow_lane_id(self) -> str:
        return self.workflow_class.workflow_lane_id

    @property
    def review_required(self) -> bool:
        return self.workflow_class.review_required

    @property
    def decision_ref(self) -> str:
        return self.workflow_class.decision_ref

    def to_json(self) -> dict[str, Any]:
        return {
            "flow_name": self.flow_name,
            "workflow_class": {
                "workflow_class_id": self.workflow_class.workflow_class_id,
                "class_name": self.workflow_class.class_name,
                "class_kind": self.workflow_class.class_kind,
                "workflow_lane_id": self.workflow_class.workflow_lane_id,
                "status": self.workflow_class.status,
                "queue_shape": _json_compatible(self.workflow_class.queue_shape),
                "throttle_policy": _json_compatible(self.workflow_class.throttle_policy),
                "review_required": self.workflow_class.review_required,
                "effective_from": self.workflow_class.effective_from.isoformat(),
                "effective_to": (
                    None
                    if self.workflow_class.effective_to is None
                    else self.workflow_class.effective_to.isoformat()
                ),
                "decision_ref": self.workflow_class.decision_ref,
                "created_at": self.workflow_class.created_at.isoformat(),
            },
        }


@dataclass(frozen=True, slots=True)
class NativeWorkflowFlowCatalog:
    """Inspectable snapshot of native review, repair, and loop flow authority."""

    flow_records: tuple[NativeWorkflowFlowRecord, ...]
    as_of: datetime
    workflow_class_authority: str = "policy.workflow_classes"

    @property
    def flow_names(self) -> tuple[str, ...]:
        return tuple(record.flow_name for record in self.flow_records)

    @classmethod
    def from_workflow_class_catalog(
        cls,
        *,
        class_catalog: WorkflowClassCatalog,
    ) -> "NativeWorkflowFlowCatalog":
        flow_records = tuple(
            NativeWorkflowFlowRecord(
                flow_name=spec.flow_name,
                workflow_class=class_catalog.resolve(class_name=spec.class_name).workflow_class,
                as_of=class_catalog.as_of,
            )
            for spec in _FLOW_SPECS
        )
        return cls(
            flow_records=flow_records,
            as_of=class_catalog.as_of,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "workflow_class_authority": self.workflow_class_authority,
            "as_of": self.as_of.isoformat(),
            "flow_names": list(self.flow_names),
            "flows": [record.to_json() for record in self.flow_records],
        }


@dataclass(frozen=True, slots=True)
class NativeRecurringReviewRepairFlowReadModel:
    """Operator-visible recurring review/repair read over the bounded flow seam."""

    recurring_review_repair_flow: RecurringReviewRepairFlowResolution
    as_of: datetime
    recurring_flow_authority: str = "runtime.recurring_review_repair_flow"

    def to_json(self) -> dict[str, Any]:
        return {
            "recurring_flow_authority": self.recurring_flow_authority,
            "as_of": self.as_of.isoformat(),
            "recurring_review_repair_flow": self.recurring_review_repair_flow.to_json(),
        }


@dataclass(slots=True)
class NativeWorkflowFlowFrontdoor:
    """Repo-local frontdoor for workflow-class review, repair, and loop flows."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )

    def _resolve_instance(
        self,
        *,
        env: Mapping[str, str] | None,
    ) -> tuple[Mapping[str, str], NativeWorkflowInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    async def _inspect_workflow_flows(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
    ) -> NativeWorkflowFlowCatalog:
        conn = await self.connect_database(env)
        try:
            class_catalog = await load_workflow_class_catalog(
                conn,
                as_of=as_of,
            )
            return NativeWorkflowFlowCatalog.from_workflow_class_catalog(
                class_catalog=class_catalog,
            )
        finally:
            await conn.close()

    async def _inspect_recurring_review_repair_flow(
        self,
        *,
        env: Mapping[str, str] | None,
        request: RecurringReviewRepairFlowRequest,
        as_of: datetime,
    ) -> NativeRecurringReviewRepairFlowReadModel:
        conn = await self.connect_database(env)
        try:
            resolution = await resolve_recurring_review_repair_flow(
                conn,  # type: ignore[arg-type]
                request=request,
                as_of=as_of,
            )
            return NativeRecurringReviewRepairFlowReadModel(
                recurring_review_repair_flow=resolution,
                as_of=resolution.as_of,
            )
        finally:
            await conn.close()

    def inspect_workflow_flows(
        self,
        *,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Inspect the native review, repair, and loop flows through class authority."""

        source, instance = self._resolve_instance(env=env)
        flow_catalog = _run_async(
            self._inspect_workflow_flows(
                env=source,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeWorkflowFlowError,
                        reason_code="operator_workflow_flows.invalid_as_of",
                    )
                ),
            ),
            error_type=NativeWorkflowFlowError,
            reason_code="operator_workflow_flows.async_boundary_required",
            message="native workflow-flow sync entrypoints require a non-async call boundary",
        )
        return {
            "native_instance": instance.to_contract(),
            "workflow_class_authority": flow_catalog.workflow_class_authority,
            "as_of": flow_catalog.as_of.isoformat(),
            "flow_names": list(flow_catalog.flow_names),
            "flows": [record.to_json() for record in flow_catalog.flow_records],
        }

    def inspect_recurring_review_repair_flow(
        self,
        *,
        request: RecurringReviewRepairFlowRequest,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Inspect one bounded recurring review/repair operator path."""

        source, instance = self._resolve_instance(env=env)
        read_model = _run_async(
            self._inspect_recurring_review_repair_flow(
                env=source,
                request=request,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeWorkflowFlowError,
                        reason_code="operator_workflow_flows.invalid_as_of",
                    )
                ),
            ),
            error_type=NativeWorkflowFlowError,
            reason_code="operator_workflow_flows.async_boundary_required",
            message="native workflow-flow sync entrypoints require a non-async call boundary",
        )
        payload = read_model.to_json()
        return {
            "native_instance": instance.to_contract(),
            "recurring_flow_authority": payload["recurring_flow_authority"],
            "as_of": payload["as_of"],
            "recurring_review_repair_flow": payload["recurring_review_repair_flow"],
        }


def inspect_workflow_flows(
    *,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Inspect the native review, repair, and loop flows through repo-local authority."""

    return NativeWorkflowFlowFrontdoor().inspect_workflow_flows(
        env=env,
        as_of=as_of,
    )


def inspect_recurring_review_repair_flow(
    *,
    request: RecurringReviewRepairFlowRequest,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Inspect one bounded recurring review/repair path through repo-local authority."""

    return NativeWorkflowFlowFrontdoor().inspect_recurring_review_repair_flow(
        request=request,
        env=env,
        as_of=as_of,
    )


# --------------------------------------------------------------------------
# Dataset refinery write helpers.
#
# These helpers are the only sanctioned write path for the dataset refinery
# authority tables (dataset_scoring_policies, dataset_promotions). Reads go
# through surfaces/api/dataset_read.py; subscribers may write to
# dataset_raw_candidates and dataset_candidate_scores (those rows are
# derived, not human-decided). Each helper:
#   * validates inputs against contracts/dataset.py
#   * inserts in a single transaction
#   * emits on CHANNEL_DATASET so the curation projection subscriber and
#     downstream observers wake up
#   * calls aemit_cache_invalidation so any cached read paths drop their
#     stale entries
# --------------------------------------------------------------------------

CACHE_KIND_DATASET_CURATED_PROJECTION = "dataset_curated_projection"
CACHE_KIND_DATASET_SCORING_POLICY = "dataset_scoring_policy"
EVENT_DATASET_POLICY_RECORDED = "dataset_policy_recorded"
EVENT_DATASET_PROMOTION_RECORDED = "dataset_promotion_recorded"
EVENT_DATASET_PROMOTION_SUPERSEDED = "dataset_promotion_superseded"
EVENT_DATASET_CANDIDATE_REJECTED = "dataset_candidate_rejected"


async def _arecord_dataset_decision(
    conn: _Connection,
    *,
    decision_kind: str,
    decision_key: str,
    decision_scope_kind: str,
    decision_scope_ref: str,
    title: str,
    rationale: str,
    decided_by: str,
    decision_source: str,
) -> str:
    """Insert one dataset-scoped operator_decisions row on the given connection
    and return its operator_decision_id.

    This bypasses the OperatorControlFrontdoor factory because dataset writes
    need to share a transaction with the downstream dataset_* row, so the
    decision + promotion/rejection/supersede land atomically.
    """

    now = _now()
    record = OperatorDecisionAuthorityRecord(
        operator_decision_id=_operator_decision_id_from_key(
            decision_kind=decision_kind,
            decision_key=decision_key,
        ),
        decision_key=decision_key,
        decision_kind=decision_kind,
        decision_status="decided",
        title=title,
        rationale=rationale,
        decided_by=decided_by,
        decision_source=decision_source,
        effective_from=now,
        effective_to=None,
        decided_at=now,
        created_at=now,
        updated_at=now,
        decision_scope_kind=decision_scope_kind,
        decision_scope_ref=decision_scope_ref,
    )
    repository = PostgresOperatorControlRepository(conn)
    persisted = await repository.record_operator_decision(operator_decision=record)
    return persisted.operator_decision_id


async def arecord_dataset_policy(
    *,
    policy_slug: str,
    specialist_target: str,
    rubric: Mapping[str, Any],
    decided_by: str,
    rationale: str,
    auto_promote: bool = False,
    policy_id: str | None = None,
    supersedes_policy_id: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record a new dataset scoring policy and (optionally) supersede a prior one."""

    from contracts.dataset import DatasetScoringPolicy

    pid = policy_id or f"pol_{uuid.uuid4().hex[:20]}"
    # Round-trip through the contract so rubric shape is validated.
    DatasetScoringPolicy(
        policy_id=pid,
        policy_slug=policy_slug,
        specialist_target=specialist_target,
        rubric=dict(rubric),
        decided_by=decided_by,
        rationale=rationale,
        auto_promote=bool(auto_promote),
    )

    conn = await connect_workflow_database(env)
    try:
        async with conn.transaction():
            await conn.execute(
                """INSERT INTO dataset_scoring_policies (
                        policy_id, policy_slug, specialist_target, rubric,
                        auto_promote, decided_by, rationale
                    ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)""",
                pid,
                policy_slug,
                specialist_target,
                json.dumps(dict(rubric)),
                bool(auto_promote),
                decided_by,
                rationale,
            )
            if supersedes_policy_id:
                await conn.execute(
                    """UPDATE dataset_scoring_policies
                          SET superseded_by = $1
                        WHERE policy_id = $2 AND superseded_by IS NULL""",
                    pid,
                    supersedes_policy_id,
                )
            event_id = await aemit(
                conn,
                channel=CHANNEL_DATASET,
                event_type=EVENT_DATASET_POLICY_RECORDED,
                entity_id=pid,
                entity_kind="dataset_scoring_policy",
                payload={
                    "policy_id": pid,
                    "policy_slug": policy_slug,
                    "specialist_target": specialist_target,
                    "auto_promote": bool(auto_promote),
                    "supersedes_policy_id": supersedes_policy_id,
                    "decided_by": decided_by,
                },
                emitted_by="operator_write.arecord_dataset_policy",
            )
            await aemit_cache_invalidation(
                conn,
                cache_kind=CACHE_KIND_DATASET_SCORING_POLICY,
                cache_key=specialist_target,
                reason=f"new policy {pid}",
                invalidated_by="operator_write.arecord_dataset_policy",
            )
    finally:
        await conn.close()
    return {
        "policy_id": pid,
        "policy_slug": policy_slug,
        "specialist_target": specialist_target,
        "auto_promote": bool(auto_promote),
        "supersedes_policy_id": supersedes_policy_id,
        "event_id": event_id,
    }


async def arecord_dataset_promotion(
    *,
    candidate_ids: Sequence[str],
    dataset_family: str,
    specialist_target: str,
    policy_id: str,
    payload: Mapping[str, Any],
    promoted_by: str,
    rationale: str,
    promotion_kind: str = "manual",
    split_tag: str | None = None,
    decision_ref: str | None = None,
    promotion_id: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one append-only promotion + emit the projection trigger event."""

    from contracts.dataset import DatasetPromotion

    pid = promotion_id or f"prom_{uuid.uuid4().hex[:20]}"
    DatasetPromotion(
        promotion_id=pid,
        candidate_ids=tuple(candidate_ids),
        dataset_family=dataset_family,
        specialist_target=specialist_target,
        policy_id=policy_id,
        payload=dict(payload),
        promoted_by=promoted_by,
        promotion_kind=promotion_kind,
        rationale=rationale,
        split_tag=split_tag,
        decision_ref=decision_ref,
    )

    conn = await connect_workflow_database(env)
    try:
        async with conn.transaction():
            resolved_decision_ref = decision_ref
            if not (resolved_decision_ref or "").strip():
                resolved_decision_ref = await _arecord_dataset_decision(
                    conn,
                    decision_kind="dataset_promotion",
                    decision_key=f"dataset-promotion::{pid}",
                    decision_scope_kind="dataset_specialist",
                    decision_scope_ref=specialist_target,
                    title=f"Dataset promotion {pid}",
                    rationale=rationale,
                    decided_by=promoted_by,
                    decision_source=f"operator_write.arecord_dataset_promotion:{promotion_kind}",
                )
            await conn.execute(
                """INSERT INTO dataset_promotions (
                        promotion_id, candidate_ids, dataset_family, specialist_target,
                        policy_id, payload, split_tag, promoted_by, promotion_kind,
                        rationale, decision_ref
                    ) VALUES ($1, $2::text[], $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11)""",
                pid,
                list(candidate_ids),
                dataset_family,
                specialist_target,
                policy_id,
                json.dumps(dict(payload)),
                split_tag,
                promoted_by,
                promotion_kind,
                rationale,
                resolved_decision_ref,
            )
            event_id = await aemit(
                conn,
                channel=CHANNEL_DATASET,
                event_type=EVENT_DATASET_PROMOTION_RECORDED,
                entity_id=pid,
                entity_kind="dataset_promotion",
                payload={
                    "promotion_id": pid,
                    "dataset_family": dataset_family,
                    "specialist_target": specialist_target,
                    "policy_id": policy_id,
                    "promoted_by": promoted_by,
                    "promotion_kind": promotion_kind,
                    "split_tag": split_tag,
                    "candidate_ids": list(candidate_ids),
                    "decision_ref": resolved_decision_ref,
                },
                emitted_by="operator_write.arecord_dataset_promotion",
            )
            await aemit_cache_invalidation(
                conn,
                cache_kind=CACHE_KIND_DATASET_CURATED_PROJECTION,
                cache_key=f"{specialist_target}:{dataset_family}:{split_tag or 'none'}",
                reason=f"new promotion {pid}",
                invalidated_by="operator_write.arecord_dataset_promotion",
            )
    finally:
        await conn.close()
    return {
        "promotion_id": pid,
        "dataset_family": dataset_family,
        "specialist_target": specialist_target,
        "policy_id": policy_id,
        "split_tag": split_tag,
        "candidate_ids": list(candidate_ids),
        "decision_ref": resolved_decision_ref,
        "event_id": event_id,
    }


async def arecord_dataset_rejection(
    *,
    candidate_id: str,
    rejected_by: str,
    reason: str,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record an operator's explicit rejection of a candidate.

    Writes a typed operator_decisions row (decision_kind=dataset_rejection,
    scope=dataset_candidate/candidate_id) alongside the event so the
    rejection is queryable as decision-table authority, not only as an
    event. The candidate row itself is left untouched (its score may
    already be 'rejected').
    """

    if not (reason or "").strip():
        raise ValueError("rejection reason must be non-blank")
    if not (rejected_by or "").strip():
        raise ValueError("rejected_by must be non-blank")
    if not (candidate_id or "").strip():
        raise ValueError("candidate_id must be non-blank")

    conn = await connect_workflow_database(env)
    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT candidate_id FROM dataset_raw_candidates WHERE candidate_id = $1",
                candidate_id,
            )
            if row is None:
                raise ValueError(f"unknown candidate_id {candidate_id!r}")
            decision_id = await _arecord_dataset_decision(
                conn,
                decision_kind="dataset_rejection",
                decision_key=f"dataset-rejection::{candidate_id}",
                decision_scope_kind="dataset_candidate",
                decision_scope_ref=candidate_id,
                title=f"Dataset candidate {candidate_id} rejected",
                rationale=reason,
                decided_by=rejected_by,
                decision_source="operator_write.arecord_dataset_rejection",
            )
            event_id = await aemit(
                conn,
                channel=CHANNEL_DATASET,
                event_type=EVENT_DATASET_CANDIDATE_REJECTED,
                entity_id=candidate_id,
                entity_kind="dataset_raw_candidate",
                payload={
                    "candidate_id": candidate_id,
                    "rejected_by": rejected_by,
                    "reason": reason,
                    "decision_ref": decision_id,
                },
                emitted_by="operator_write.arecord_dataset_rejection",
            )
    finally:
        await conn.close()
    return {
        "candidate_id": candidate_id,
        "rejected_by": rejected_by,
        "reason": reason,
        "decision_ref": decision_id,
        "event_id": event_id,
    }


async def asupersede_dataset_promotion(
    *,
    promotion_id: str,
    superseded_reason: str,
    superseded_by: str | None = None,
    superseded_by_operator: str,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Mark a promotion superseded.

    If ``superseded_by`` is omitted, a tombstone promotion is created so the
    constraint that ``superseded_by`` and ``superseded_reason`` are paired
    is satisfied while still recording who/why.
    """

    if not (superseded_reason or "").strip():
        raise ValueError("superseded_reason must be non-blank")
    if not (superseded_by_operator or "").strip():
        raise ValueError("superseded_by_operator must be non-blank")

    conn = await connect_workflow_database(env)
    try:
        async with conn.transaction():
            original = await conn.fetchrow(
                """SELECT promotion_id, candidate_ids, dataset_family,
                          specialist_target, policy_id, split_tag, superseded_by
                     FROM dataset_promotions WHERE promotion_id = $1""",
                promotion_id,
            )
            if original is None:
                raise ValueError(f"unknown promotion_id {promotion_id!r}")
            if original["superseded_by"] is not None:
                raise ValueError(
                    f"promotion {promotion_id!r} already superseded by "
                    f"{original['superseded_by']!r}"
                )
            tombstone_id = superseded_by
            if tombstone_id is None:
                tombstone_id = f"prom_tomb_{uuid.uuid4().hex[:16]}"
                await conn.execute(
                    """INSERT INTO dataset_promotions (
                            promotion_id, candidate_ids, dataset_family,
                            specialist_target, policy_id, payload, split_tag,
                            promoted_by, promotion_kind, rationale
                        ) VALUES (
                            $1, $2::text[], $3, $4, $5, $6::jsonb, $7, $8,
                            'auto', $9
                        )""",
                    tombstone_id,
                    list(original["candidate_ids"]),
                    original["dataset_family"],
                    original["specialist_target"],
                    original["policy_id"],
                    json.dumps(
                        {
                            "tombstone": True,
                            "supersedes_promotion_id": promotion_id,
                            "reason": superseded_reason,
                        }
                    ),
                    original["split_tag"],
                    f"system:operator_supersede:{superseded_by_operator}",
                    f"superseded by {superseded_by_operator}: {superseded_reason}",
                )
            await conn.execute(
                """UPDATE dataset_promotions
                      SET superseded_by = $1, superseded_reason = $2
                    WHERE promotion_id = $3 AND superseded_by IS NULL""",
                tombstone_id,
                superseded_reason,
                promotion_id,
            )
            decision_id = await _arecord_dataset_decision(
                conn,
                decision_kind="dataset_promotion_supersede",
                decision_key=f"dataset-promotion-supersede::{promotion_id}",
                decision_scope_kind="dataset_promotion",
                decision_scope_ref=promotion_id,
                title=f"Dataset promotion {promotion_id} superseded",
                rationale=superseded_reason,
                decided_by=superseded_by_operator,
                decision_source="operator_write.asupersede_dataset_promotion",
            )
            event_id = await aemit(
                conn,
                channel=CHANNEL_DATASET,
                event_type=EVENT_DATASET_PROMOTION_SUPERSEDED,
                entity_id=promotion_id,
                entity_kind="dataset_promotion",
                payload={
                    "promotion_id": promotion_id,
                    "tombstone_promotion_id": tombstone_id,
                    "reason": superseded_reason,
                    "superseded_by_operator": superseded_by_operator,
                    "decision_ref": decision_id,
                },
                emitted_by="operator_write.asupersede_dataset_promotion",
            )
            await aemit_cache_invalidation(
                conn,
                cache_kind=CACHE_KIND_DATASET_CURATED_PROJECTION,
                cache_key=f"{original['specialist_target']}:{original['dataset_family']}:{original['split_tag'] or 'none'}",
                reason=f"superseded {promotion_id}",
                invalidated_by="operator_write.asupersede_dataset_promotion",
            )
    finally:
        await conn.close()
    return {
        "promotion_id": promotion_id,
        "tombstone_promotion_id": tombstone_id,
        "reason": superseded_reason,
        "superseded_by_operator": superseded_by_operator,
        "decision_ref": decision_id,
        "event_id": event_id,
    }


__all__ = [
    "ArchitecturePolicyDecisionRecord",
    "NativeWorkflowFlowCatalog",
    "NativeWorkflowFlowError",
    "NativeWorkflowFlowFrontdoor",
    "NativeWorkflowFlowRecord",
    "NativeRecurringReviewRepairFlowReadModel",
    "CircuitBreakerOverrideRecord",
    "OperatorControlFrontdoor",
    "TaskRouteEligibilityRecord",
    "TaskRouteEligibilityWriteResult",
    "CACHE_KIND_DATASET_CURATED_PROJECTION",
    "CACHE_KIND_DATASET_SCORING_POLICY",
    "EVENT_DATASET_CANDIDATE_REJECTED",
    "EVENT_DATASET_POLICY_RECORDED",
    "EVENT_DATASET_PROMOTION_RECORDED",
    "EVENT_DATASET_PROMOTION_SUPERSEDED",
    "abackfill_semantic_bridges",
    "arecord_architecture_policy_decision",
    "arecord_dataset_policy",
    "arecord_dataset_promotion",
    "arecord_dataset_rejection",
    "asupersede_dataset_promotion",
    "arecord_functional_area",
    "aoperator_ideas",
    "arecord_issue",
    "arecord_operator_decision",
    "arecord_operator_object_relation",
    "aset_circuit_breaker_override",
    "aadmit_native_primary_cutover_gate",
    "aroadmap_write",
    "areconcile_work_item_closeout",
    "aset_task_route_eligibility_window",
    "arecord_work_item_workflow_binding",
    "admit_native_primary_cutover_gate",
    "inspect_workflow_flows",
    "inspect_recurring_review_repair_flow",
    "backfill_semantic_bridges",
    "record_architecture_policy_decision",
    "record_functional_area",
    "operator_ideas",
    "record_issue",
    "record_operator_decision",
    "record_operator_object_relation",
    "list_operator_decisions",
    "alist_operator_decisions",
    "roadmap_write",
    "reconcile_work_item_closeout",
    "record_work_item_workflow_binding",
    "set_circuit_breaker_override",
    "set_task_route_eligibility_window",
]
