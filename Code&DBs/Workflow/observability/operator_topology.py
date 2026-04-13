"""Combined operator topology surfaces over graph, cutover, and scoreboard state.

This module consolidates the graph projection, cutover graph status, and
cutover scoreboard read models behind one topology boundary. The surfaces
remain derived and read-only.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import asyncpg

from receipts import EvidenceRow, ReceiptV1
from authority.operator_control import (
    CutoverGateAuthorityRecord,
    OperatorControlAuthority,
    OperatorDecisionAuthorityRecord,
    load_operator_control_authority,
)
from runtime._helpers import (
    _append_indexed_lines,
    _dedupe,
    _fail as _shared_fail,
    _format_bool,
)
from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord

from .graph_lineage import graph_lineage_run
from .graph_topology import graph_topology_run
from .read_models import (
    GraphLineageReadModel,
    GraphTopologyReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
)
from .readers import inspect_run, replay_run

if TYPE_CHECKING:
    from .operator_dashboard import NativeOperatorStatusReadModel, NativeOperatorSupportSnapshot

__all__ = [
    "NativeCutoverGraphBindingStatus",
    "NativeCutoverGraphGateStatus",
    "NativeCutoverGraphStatusReadModel",
    "NativeCutoverReceiptSnapshot",
    "NativeCutoverScoreboardReadModel",
    "NativeCutoverStatusSnapshot",
    "OperatorGraphBugRecord",
    "OperatorGraphEdge",
    "OperatorGraphFreshness",
    "OperatorGraphNode",
    "OperatorGraphProjectionError",
    "OperatorGraphReadModel",
    "OperatorGraphRoadmapRecord",
    "cutover_graph_status_run",
    "cutover_scoreboard_run",
    "load_operator_graph_projection",
    "render_cutover_graph_status",
    "render_cutover_scoreboard",
]


class OperatorGraphProjectionError(RuntimeError):
    """Raised when the operator graph projection cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=OperatorGraphProjectionError)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "operator_graph.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "operator_graph.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "operator_graph.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "operator_graph.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "operator_graph.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    normalized_value = _json_value(value)
    if not isinstance(normalized_value, Mapping):
        raise _fail(
            "operator_graph.invalid_row",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return normalized_value


def _json_value(value: object) -> object:
    if isinstance(value, str):
        import json

        return json.loads(value)
    return value


def _format_optional_text(value: str | None) -> str:
    return value if value is not None and value != "" else "-"


def _format_optional_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def _node_id(kind: str, canonical_ref: str) -> str:
    return f"{kind}:{canonical_ref}"


def _binding_target_kind(field_name: str) -> str:
    if field_name.endswith("_id"):
        return field_name[:-3]
    return field_name


def _max_datetime(current: datetime | None, candidate: datetime | None) -> datetime | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


@dataclass(frozen=True, slots=True)
class OperatorGraphBugRecord:
    """Canonical bug row projected into the operator graph."""

    bug_id: str
    bug_key: str
    title: str
    status: str
    severity: str
    priority: str
    summary: str
    source_kind: str
    decision_ref: str
    opened_at: datetime
    resolved_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OperatorGraphRoadmapRecord:
    """Canonical roadmap row projected into the operator graph."""

    roadmap_item_id: str
    roadmap_key: str
    title: str
    item_kind: str
    status: str
    priority: str
    parent_roadmap_item_id: str | None
    source_bug_id: str | None
    summary: str
    acceptance_criteria: Mapping[str, Any]
    decision_ref: str
    target_start_at: datetime | None
    target_end_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OperatorGraphNode:
    """Graph-ready node projected from one canonical authority row."""

    node_id: str
    node_kind: str
    canonical_ref: str
    lookup_ref: str
    title: str
    status: str
    summary: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OperatorGraphEdge:
    """Graph-ready edge projected from one canonical authority row."""

    edge_id: str
    edge_kind: str
    source_kind: str
    source_node_id: str
    target_kind: str
    target_ref: str
    target_node_id: str | None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperatorGraphFreshness:
    """Freshness metadata for one projection snapshot."""

    as_of: datetime
    latest_source_created_at: datetime | None
    latest_source_updated_at: datetime | None
    source_row_count: int


@dataclass(frozen=True, slots=True)
class OperatorGraphReadModel:
    """One explicit operator graph projection."""

    as_of: datetime
    completeness: ProjectionCompleteness
    freshness: OperatorGraphFreshness
    bugs: tuple[OperatorGraphBugRecord, ...]
    roadmap_items: tuple[OperatorGraphRoadmapRecord, ...]
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...]
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...]
    work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...]
    nodes: tuple[OperatorGraphNode, ...]
    edges: tuple[OperatorGraphEdge, ...]


def _bug_record_from_row(row: Mapping[str, object]) -> OperatorGraphBugRecord:
    return OperatorGraphBugRecord(
        bug_id=_require_text(row["bug_id"], field_name="bug_id"),
        bug_key=_require_text(row["bug_key"], field_name="bug_key"),
        title=_require_text(row["title"], field_name="title"),
        status=_require_text(row["status"], field_name="status"),
        severity=_require_text(row["severity"], field_name="severity"),
        priority=_require_text(row["priority"], field_name="priority"),
        summary=_require_text(row["summary"], field_name="summary"),
        source_kind=_require_text(row["source_kind"], field_name="source_kind"),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        opened_at=_require_datetime(row["opened_at"], field_name="opened_at"),
        resolved_at=(
            _require_datetime(row["resolved_at"], field_name="resolved_at")
            if row["resolved_at"] is not None
            else None
        ),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
        updated_at=_require_datetime(row["updated_at"], field_name="updated_at"),
    )


def _roadmap_record_from_row(row: Mapping[str, object]) -> OperatorGraphRoadmapRecord:
    acceptance_criteria = _require_mapping(
        row["acceptance_criteria"],
        field_name="acceptance_criteria",
    )
    return OperatorGraphRoadmapRecord(
        roadmap_item_id=_require_text(row["roadmap_item_id"], field_name="roadmap_item_id"),
        roadmap_key=_require_text(row["roadmap_key"], field_name="roadmap_key"),
        title=_require_text(row["title"], field_name="title"),
        item_kind=_require_text(row["item_kind"], field_name="item_kind"),
        status=_require_text(row["status"], field_name="status"),
        priority=_require_text(row["priority"], field_name="priority"),
        parent_roadmap_item_id=(
            _require_text(row["parent_roadmap_item_id"], field_name="parent_roadmap_item_id")
            if row["parent_roadmap_item_id"] is not None
            else None
        ),
        source_bug_id=(
            _require_text(row["source_bug_id"], field_name="source_bug_id")
            if row["source_bug_id"] is not None
            else None
        ),
        summary=_require_text(row["summary"], field_name="summary"),
        acceptance_criteria=acceptance_criteria,
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        target_start_at=(
            _require_datetime(row["target_start_at"], field_name="target_start_at")
            if row["target_start_at"] is not None
            else None
        ),
        target_end_at=(
            _require_datetime(row["target_end_at"], field_name="target_end_at")
            if row["target_end_at"] is not None
            else None
        ),
        completed_at=(
            _require_datetime(row["completed_at"], field_name="completed_at")
            if row["completed_at"] is not None
            else None
        ),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
        updated_at=_require_datetime(row["updated_at"], field_name="updated_at"),
    )


async def _fetch_bug_records(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> tuple[OperatorGraphBugRecord, ...]:
    try:
        rows = await conn.fetch(
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
                decision_ref,
                opened_at,
                resolved_at,
                created_at,
                updated_at
            FROM bugs
            WHERE opened_at <= $1
              AND created_at <= $1
            ORDER BY bug_key, opened_at DESC, created_at DESC, bug_id
            """,
            as_of,
        )
    except asyncpg.PostgresError as exc:
        raise _fail(
            "operator_graph.read_failed",
            "failed to read bug rows",
            details={"sqlstate": getattr(exc, "sqlstate", None)},
        ) from exc
    return tuple(_bug_record_from_row(cast(Mapping[str, object], row)) for row in rows)


async def _fetch_roadmap_records(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> tuple[OperatorGraphRoadmapRecord, ...]:
    try:
        rows = await conn.fetch(
            """
            SELECT
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_items
            WHERE created_at <= $1
            ORDER BY roadmap_key, created_at DESC, roadmap_item_id
            """,
            as_of,
        )
    except asyncpg.PostgresError as exc:
        raise _fail(
            "operator_graph.read_failed",
            "failed to read roadmap rows",
            details={"sqlstate": getattr(exc, "sqlstate", None)},
        ) from exc
    return tuple(_roadmap_record_from_row(cast(Mapping[str, object], row)) for row in rows)


async def _fetch_binding_records(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> tuple[WorkItemWorkflowBindingRecord, ...]:
    try:
        rows = await conn.fetch(
            """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
            WHERE created_at <= $1
            ORDER BY binding_kind, created_at DESC, work_item_workflow_binding_id
            """,
            as_of,
        )
    except asyncpg.PostgresError as exc:
        raise _fail(
            "operator_graph.read_failed",
            "failed to read work-item binding rows",
            details={"sqlstate": getattr(exc, "sqlstate", None)},
        ) from exc

    bindings: list[WorkItemWorkflowBindingRecord] = []
    for row in rows:
        mapping = cast(Mapping[str, object], row)
        bindings.append(
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id=_require_text(
                    mapping["work_item_workflow_binding_id"],
                    field_name="work_item_workflow_binding_id",
                ),
                binding_kind=_require_text(mapping["binding_kind"], field_name="binding_kind"),
                binding_status=_require_text(
                    mapping["binding_status"],
                    field_name="binding_status",
                ),
                roadmap_item_id=(
                    _require_text(mapping["roadmap_item_id"], field_name="roadmap_item_id")
                    if mapping["roadmap_item_id"] is not None
                    else None
                ),
                bug_id=(
                    _require_text(mapping["bug_id"], field_name="bug_id")
                    if mapping["bug_id"] is not None
                    else None
                ),
                cutover_gate_id=(
                    _require_text(mapping["cutover_gate_id"], field_name="cutover_gate_id")
                    if mapping["cutover_gate_id"] is not None
                    else None
                ),
                workflow_class_id=(
                    _require_text(mapping["workflow_class_id"], field_name="workflow_class_id")
                    if mapping["workflow_class_id"] is not None
                    else None
                ),
                schedule_definition_id=(
                    _require_text(
                        mapping["schedule_definition_id"],
                        field_name="schedule_definition_id",
                    )
                    if mapping["schedule_definition_id"] is not None
                    else None
                ),
                workflow_run_id=(
                    _require_text(mapping["workflow_run_id"], field_name="workflow_run_id")
                    if mapping["workflow_run_id"] is not None
                    else None
                ),
                bound_by_decision_id=(
                    _require_text(
                        mapping["bound_by_decision_id"],
                        field_name="bound_by_decision_id",
                    )
                    if mapping["bound_by_decision_id"] is not None
                    else None
                ),
                created_at=_require_datetime(mapping["created_at"], field_name="created_at"),
                updated_at=_require_datetime(mapping["updated_at"], field_name="updated_at"),
            )
        )
    return tuple(bindings)


def _build_nodes(
    *,
    bugs: tuple[OperatorGraphBugRecord, ...],
    roadmap_items: tuple[OperatorGraphRoadmapRecord, ...],
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...],
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...],
) -> tuple[tuple[OperatorGraphNode, ...], dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    nodes: list[OperatorGraphNode] = []
    node_ids: set[str] = set()
    bug_id_lookup: dict[str, str] = {}
    roadmap_id_lookup: dict[str, str] = {}
    decision_id_lookup: dict[str, str] = {}
    gate_id_lookup: dict[str, str] = {}

    def add_node(node: OperatorGraphNode) -> None:
        if node.node_id in node_ids:
            raise _fail(
                "operator_graph.duplicate_node_id",
                f"duplicate graph node id {node.node_id!r}",
                details={"node_id": node.node_id},
            )
        node_ids.add(node.node_id)
        nodes.append(node)

    for record in bugs:
        node_id = _node_id("bug", record.bug_id)
        bug_id_lookup[record.bug_id] = node_id
        add_node(
            OperatorGraphNode(
                node_id=node_id,
                node_kind="bug",
                canonical_ref=record.bug_id,
                lookup_ref=record.bug_key,
                title=record.title,
                status=record.status,
                summary=record.summary,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        )

    for record in roadmap_items:
        node_id = _node_id("roadmap_item", record.roadmap_item_id)
        roadmap_id_lookup[record.roadmap_item_id] = node_id
        add_node(
            OperatorGraphNode(
                node_id=node_id,
                node_kind="roadmap_item",
                canonical_ref=record.roadmap_item_id,
                lookup_ref=record.roadmap_key,
                title=record.title,
                status=record.status,
                summary=record.summary,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        )

    for record in operator_decisions:
        node_id = _node_id("operator_decision", record.operator_decision_id)
        decision_id_lookup[record.operator_decision_id] = node_id
        add_node(
            OperatorGraphNode(
                node_id=node_id,
                node_kind="operator_decision",
                canonical_ref=record.operator_decision_id,
                lookup_ref=record.decision_key,
                title=record.title,
                status=record.decision_status,
                summary=record.rationale,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        )

    for record in cutover_gates:
        node_id = _node_id("cutover_gate", record.cutover_gate_id)
        gate_id_lookup[record.cutover_gate_id] = node_id
        add_node(
            OperatorGraphNode(
                node_id=node_id,
                node_kind="cutover_gate",
                canonical_ref=record.cutover_gate_id,
                lookup_ref=record.gate_key,
                title=record.gate_name,
                status=record.gate_status,
                summary=f"targets {record.target_kind}:{record.target_ref}",
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        )

    return tuple(nodes), bug_id_lookup, roadmap_id_lookup, decision_id_lookup, gate_id_lookup


def _decision_lookup(
    *,
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...],
    decision_id_lookup: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    decision_key_lookup: dict[str, str] = {}
    for record in operator_decisions:
        decision_key_lookup[record.decision_key] = decision_id_lookup[record.operator_decision_id]
    return decision_key_lookup, decision_id_lookup


def _resolve_decision_ref(
    *,
    decision_ref: str,
    decision_key_lookup: Mapping[str, str],
    decision_id_lookup: Mapping[str, str],
) -> str | None:
    by_key = decision_key_lookup.get(decision_ref)
    by_id = decision_id_lookup.get(decision_ref)
    if by_key is not None and by_id is not None and by_key != by_id:
        raise _fail(
            "operator_graph.decision_ref_ambiguous",
            f"decision_ref {decision_ref!r} matches multiple operator decisions",
            details={"decision_ref": decision_ref},
        )
    return by_key if by_key is not None else by_id


def _build_edges(
    *,
    bugs: tuple[OperatorGraphBugRecord, ...],
    roadmap_items: tuple[OperatorGraphRoadmapRecord, ...],
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...],
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...],
    work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    bug_id_lookup: Mapping[str, str],
    roadmap_id_lookup: Mapping[str, str],
    decision_id_lookup: Mapping[str, str],
    gate_id_lookup: Mapping[str, str],
) -> tuple[tuple[OperatorGraphEdge, ...], tuple[str, ...]]:
    edges: list[OperatorGraphEdge] = []
    edge_ids: set[str] = set()
    missing_refs: list[str] = []
    decision_key_lookup, decision_id_lookup = _decision_lookup(
        operator_decisions=operator_decisions,
        decision_id_lookup=dict(decision_id_lookup),
    )

    def add_edge(
        *,
        source_kind: str,
        source_node_id: str,
        edge_kind: str,
        target_kind: str,
        target_ref: str,
        target_node_id: str | None,
        created_at: datetime | None = None,
        missing_ref: str | None = None,
    ) -> None:
        if target_node_id is None and missing_ref is not None:
            missing_refs.append(missing_ref)
        edge_id = f"{source_node_id}:{edge_kind}:{target_kind}:{target_ref}"
        if edge_id in edge_ids:
            missing_refs.append(f"operator_graph.duplicate_edge_id:{edge_id}")
            return
        edge_ids.add(edge_id)
        edges.append(
            OperatorGraphEdge(
                edge_id=edge_id,
                edge_kind=edge_kind,
                source_kind=source_kind,
                source_node_id=source_node_id,
                target_kind=target_kind,
                target_ref=target_ref,
                target_node_id=target_node_id,
                created_at=created_at,
            )
        )

    for record in bugs:
        source_node_id = bug_id_lookup.get(record.bug_id)
        if source_node_id is None:
            missing_refs.append(f"operator_graph.bug_missing:{record.bug_id}")
            continue
        target_node_id = _resolve_decision_ref(
            decision_ref=record.decision_ref,
            decision_key_lookup=decision_key_lookup,
            decision_id_lookup=decision_id_lookup,
        )
        add_edge(
            source_kind="bug",
            source_node_id=source_node_id,
            edge_kind="decision_ref",
            target_kind="operator_decision",
            target_ref=record.decision_ref,
            target_node_id=target_node_id,
            created_at=record.created_at,
            missing_ref=f"operator_graph.decision_ref_missing:bug:{record.bug_id}:{record.decision_ref}",
        )

    for record in roadmap_items:
        source_node_id = roadmap_id_lookup.get(record.roadmap_item_id)
        if source_node_id is None:
            missing_refs.append(f"operator_graph.roadmap_missing:{record.roadmap_item_id}")
            continue

        if record.parent_roadmap_item_id is not None:
            target_node_id = roadmap_id_lookup.get(record.parent_roadmap_item_id)
            add_edge(
                source_kind="roadmap_item",
                source_node_id=source_node_id,
                edge_kind="parent_roadmap_item",
                target_kind="roadmap_item",
                target_ref=record.parent_roadmap_item_id,
                target_node_id=target_node_id,
                created_at=record.created_at,
                missing_ref=(
                    f"operator_graph.parent_roadmap_missing:"
                    f"{record.roadmap_item_id}:{record.parent_roadmap_item_id}"
                ),
            )

        if record.source_bug_id is not None:
            target_node_id = bug_id_lookup.get(record.source_bug_id)
            add_edge(
                source_kind="roadmap_item",
                source_node_id=source_node_id,
                edge_kind="source_bug",
                target_kind="bug",
                target_ref=record.source_bug_id,
                target_node_id=target_node_id,
                created_at=record.created_at,
                missing_ref=(
                    f"operator_graph.source_bug_missing:"
                    f"{record.roadmap_item_id}:{record.source_bug_id}"
                ),
            )

        target_node_id = _resolve_decision_ref(
            decision_ref=record.decision_ref,
            decision_key_lookup=decision_key_lookup,
            decision_id_lookup=decision_id_lookup,
        )
        add_edge(
            source_kind="roadmap_item",
            source_node_id=source_node_id,
            edge_kind="decision_ref",
            target_kind="operator_decision",
            target_ref=record.decision_ref,
            target_node_id=target_node_id,
            created_at=record.created_at,
            missing_ref=(
                f"operator_graph.decision_ref_missing:roadmap_item:"
                f"{record.roadmap_item_id}:{record.decision_ref}"
            ),
        )

    for record in cutover_gates:
        source_node_id = gate_id_lookup.get(record.cutover_gate_id)
        if source_node_id is None:
            missing_refs.append(f"operator_graph.cutover_gate_missing:{record.cutover_gate_id}")
            continue

        if record.target_kind == "roadmap_item":
            target_node_id = roadmap_id_lookup.get(record.target_ref)
            add_edge(
                source_kind="cutover_gate",
                source_node_id=source_node_id,
                edge_kind="target_roadmap_item",
                target_kind="roadmap_item",
                target_ref=record.target_ref,
                target_node_id=target_node_id,
                created_at=record.created_at,
                missing_ref=(
                    f"operator_graph.target_roadmap_missing:"
                    f"{record.cutover_gate_id}:{record.target_ref}"
                ),
            )
        elif record.target_kind == "workflow_class":
            add_edge(
                source_kind="cutover_gate",
                source_node_id=source_node_id,
                edge_kind="target_workflow_class",
                target_kind="workflow_class",
                target_ref=record.target_ref,
                target_node_id=None,
                created_at=record.created_at,
            )
        elif record.target_kind == "schedule_definition":
            add_edge(
                source_kind="cutover_gate",
                source_node_id=source_node_id,
                edge_kind="target_schedule_definition",
                target_kind="schedule_definition",
                target_ref=record.target_ref,
                target_node_id=None,
                created_at=record.created_at,
            )
        else:
            add_edge(
                source_kind="cutover_gate",
                source_node_id=source_node_id,
                edge_kind=f"target_{record.target_kind}",
                target_kind=record.target_kind,
                target_ref=record.target_ref,
                target_node_id=None,
                created_at=record.created_at,
            )

        opened_by_node_id = decision_id_lookup.get(record.opened_by_decision_id)
        add_edge(
            source_kind="cutover_gate",
            source_node_id=source_node_id,
            edge_kind="opened_by_decision",
            target_kind="operator_decision",
            target_ref=record.opened_by_decision_id,
            target_node_id=opened_by_node_id,
            created_at=record.created_at,
            missing_ref=(
                f"operator_graph.opened_by_decision_missing:"
                f"{record.cutover_gate_id}:{record.opened_by_decision_id}"
            ),
        )

        if record.closed_by_decision_id is not None:
            closed_by_node_id = decision_id_lookup.get(record.closed_by_decision_id)
            add_edge(
                source_kind="cutover_gate",
                source_node_id=source_node_id,
                edge_kind="closed_by_decision",
                target_kind="operator_decision",
                target_ref=record.closed_by_decision_id,
                target_node_id=closed_by_node_id,
                created_at=record.closed_at or record.updated_at,
                missing_ref=(
                    f"operator_graph.closed_by_decision_missing:"
                    f"{record.cutover_gate_id}:{record.closed_by_decision_id}"
                ),
            )

    for record in work_item_workflow_bindings:
        source_kind = record.source_kind
        if source_kind == "bug":
            source_node_id = bug_id_lookup.get(record.source_id)
        elif source_kind == "roadmap_item":
            source_node_id = roadmap_id_lookup.get(record.source_id)
        else:
            source_node_id = gate_id_lookup.get(record.source_id)

        if source_node_id is None:
            missing_refs.append(
                f"operator_graph.binding_source_missing:"
                f"{record.work_item_workflow_binding_id}:{source_kind}:{record.source_id}"
            )
            continue

        if record.bound_by_decision_id is not None:
            target_node_id = decision_id_lookup.get(record.bound_by_decision_id)
            add_edge(
                source_kind=source_kind,
                source_node_id=source_node_id,
                edge_kind="bound_by_decision",
                target_kind="operator_decision",
                target_ref=record.bound_by_decision_id,
                target_node_id=target_node_id,
                created_at=record.created_at,
                missing_ref=(
                    f"operator_graph.binding_decision_missing:"
                    f"{record.work_item_workflow_binding_id}:{record.bound_by_decision_id}"
                ),
            )

        for target_kind, target_ref in record.target_refs.items():
            add_edge(
                source_kind=source_kind,
                source_node_id=source_node_id,
                edge_kind=f"targets_{_binding_target_kind(target_kind)}",
                target_kind=_binding_target_kind(target_kind),
                target_ref=target_ref,
                target_node_id=None,
                created_at=record.created_at,
            )

    return tuple(edges), _dedupe(missing_refs)


def _freshness_from_rows(
    *,
    bugs: tuple[OperatorGraphBugRecord, ...],
    roadmap_items: tuple[OperatorGraphRoadmapRecord, ...],
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...],
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...],
    work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
    as_of: datetime,
) -> OperatorGraphFreshness:
    latest_created_at: datetime | None = None
    latest_updated_at: datetime | None = None
    for record in bugs:
        latest_created_at = _max_datetime(latest_created_at, record.created_at)
        latest_updated_at = _max_datetime(latest_updated_at, record.updated_at)
    for record in roadmap_items:
        latest_created_at = _max_datetime(latest_created_at, record.created_at)
        latest_updated_at = _max_datetime(latest_updated_at, record.updated_at)
    for record in operator_decisions:
        latest_created_at = _max_datetime(latest_created_at, record.created_at)
        latest_updated_at = _max_datetime(latest_updated_at, record.updated_at)
    for record in cutover_gates:
        latest_created_at = _max_datetime(latest_created_at, record.created_at)
        latest_updated_at = _max_datetime(latest_updated_at, record.updated_at)
    for record in work_item_workflow_bindings:
        latest_created_at = _max_datetime(latest_created_at, record.created_at)
        latest_updated_at = _max_datetime(latest_updated_at, record.updated_at)
    return OperatorGraphFreshness(
        as_of=as_of,
        latest_source_created_at=latest_created_at,
        latest_source_updated_at=latest_updated_at,
        source_row_count=(
            len(bugs)
            + len(roadmap_items)
            + len(operator_decisions)
            + len(cutover_gates)
            + len(work_item_workflow_bindings)
        ),
    )


def _build_operator_graph_projection(
    *,
    as_of: datetime,
    bugs: tuple[OperatorGraphBugRecord, ...],
    roadmap_items: tuple[OperatorGraphRoadmapRecord, ...],
    operator_decisions: tuple[OperatorDecisionAuthorityRecord, ...],
    cutover_gates: tuple[CutoverGateAuthorityRecord, ...],
    work_item_workflow_bindings: tuple[WorkItemWorkflowBindingRecord, ...],
) -> OperatorGraphReadModel:
    nodes, bug_id_lookup, roadmap_id_lookup, decision_id_lookup, gate_id_lookup = _build_nodes(
        bugs=bugs,
        roadmap_items=roadmap_items,
        operator_decisions=operator_decisions,
        cutover_gates=cutover_gates,
    )
    edges, missing_refs = _build_edges(
        bugs=bugs,
        roadmap_items=roadmap_items,
        operator_decisions=operator_decisions,
        cutover_gates=cutover_gates,
        work_item_workflow_bindings=work_item_workflow_bindings,
        bug_id_lookup=bug_id_lookup,
        roadmap_id_lookup=roadmap_id_lookup,
        decision_id_lookup=decision_id_lookup,
        gate_id_lookup=gate_id_lookup,
    )
    freshness = _freshness_from_rows(
        bugs=bugs,
        roadmap_items=roadmap_items,
        operator_decisions=operator_decisions,
        cutover_gates=cutover_gates,
        work_item_workflow_bindings=work_item_workflow_bindings,
        as_of=as_of,
    )
    return OperatorGraphReadModel(
        as_of=as_of,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs,
            missing_evidence_refs=missing_refs,
        ),
        freshness=freshness,
        bugs=bugs,
        roadmap_items=roadmap_items,
        operator_decisions=operator_decisions,
        cutover_gates=cutover_gates,
        work_item_workflow_bindings=work_item_workflow_bindings,
        nodes=nodes,
        edges=edges,
    )


async def load_operator_graph_projection(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> OperatorGraphReadModel:
    """Load one explicit operator graph projection from Postgres-backed authority rows."""

    normalized_as_of = _normalize_as_of(as_of)
    async with conn.transaction():
        authority = await load_operator_control_authority(conn, as_of=normalized_as_of)
        bugs = await _fetch_bug_records(conn, as_of=normalized_as_of)
        roadmap_items = await _fetch_roadmap_records(conn, as_of=normalized_as_of)
        work_item_workflow_bindings = await _fetch_binding_records(
            conn,
            as_of=normalized_as_of,
        )
    return _build_operator_graph_projection(
        as_of=normalized_as_of,
        bugs=bugs,
        roadmap_items=roadmap_items,
        operator_decisions=authority.operator_decisions,
        cutover_gates=authority.cutover_gates,
        work_item_workflow_bindings=work_item_workflow_bindings,
    )


def _flatten_text_values(value: object) -> tuple[str, ...]:
    refs: list[str] = []

    def visit(item: object) -> None:
        if isinstance(item, str):
            if item:
                refs.append(item)
            return
        if isinstance(item, Mapping):
            for nested_item in item.values():
                visit(nested_item)
            return
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            for nested_item in item:
                visit(nested_item)

    visit(value)
    return _dedupe(refs)


def _sorted_evidence_rows(
    canonical_evidence: Sequence[EvidenceRow],
) -> tuple[EvidenceRow, ...]:
    return tuple(
        sorted(
            (
                row
                for row in canonical_evidence
                if isinstance(row, EvidenceRow)
            ),
            key=lambda item: (item.evidence_seq, item.row_id),
        )
    )


def _binding_is_relevant(
    *,
    binding: WorkItemWorkflowBindingRecord,
    gates: Sequence[CutoverGateAuthorityRecord],
    run_id: str,
) -> bool:
    if binding.workflow_run_id == run_id:
        return True
    if binding.cutover_gate_id is not None:
        return True
    for gate in gates:
        if gate.target_kind == "roadmap_item" and binding.roadmap_item_id == gate.target_ref:
            return True
        if gate.target_kind == "workflow_class" and binding.workflow_class_id == gate.target_ref:
            return True
        if gate.target_kind == "schedule_definition" and binding.schedule_definition_id == gate.target_ref:
            return True
    return False


def _binding_matches_gate(
    *,
    binding: WorkItemWorkflowBindingRecord,
    gate: CutoverGateAuthorityRecord,
) -> bool:
    if binding.cutover_gate_id == gate.cutover_gate_id:
        return True
    if gate.target_kind == "roadmap_item":
        return binding.roadmap_item_id == gate.target_ref
    if gate.target_kind == "workflow_class":
        return binding.workflow_class_id == gate.target_ref
    if gate.target_kind == "schedule_definition":
        return binding.schedule_definition_id == gate.target_ref
    return False


@dataclass(frozen=True, slots=True)
class NativeCutoverGraphGateStatus:
    """One cutover gate with explicit binding coverage metadata."""

    cutover_gate_id: str
    gate_key: str
    gate_name: str
    gate_kind: str
    gate_status: str
    target_kind: str
    target_ref: str
    required_evidence_refs: tuple[str, ...]
    binding_ids: tuple[str, ...]
    coverage_state: str
    coverage_reason: str | None


@dataclass(frozen=True, slots=True)
class NativeCutoverGraphBindingStatus:
    """One work-item binding with explicit graph linkage metadata."""

    work_item_workflow_binding_id: str
    binding_kind: str
    binding_status: str
    source_kind: str
    source_id: str
    cutover_gate_id: str | None
    workflow_class_id: str | None
    schedule_definition_id: str | None
    workflow_run_id: str | None
    bound_by_decision_id: str | None
    linked_gate_keys: tuple[str, ...]
    linkage_state: str
    linkage_reason: str | None


@dataclass(frozen=True, slots=True)
class NativeCutoverGraphStatusReadModel:
    """One derived cutover/planning graph status surface."""

    run_id: str
    request_id: str | None
    watermark: ProjectionWatermark
    evidence_refs: tuple[str, ...]
    graph_topology: GraphTopologyReadModel
    graph_lineage: GraphLineageReadModel
    cutover_gates: tuple[NativeCutoverGraphGateStatus, ...]
    work_bindings: tuple[NativeCutoverGraphBindingStatus, ...]
    completeness: ProjectionCompleteness
    status_state: str
    status_reason: str | None


def _gate_statuses(
    *,
    gates: Sequence[CutoverGateAuthorityRecord],
    work_bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> tuple[NativeCutoverGraphGateStatus, ...]:
    statuses: list[NativeCutoverGraphGateStatus] = []
    for gate in gates:
        matching_bindings = tuple(
            sorted(
                binding.work_item_workflow_binding_id
                for binding in work_bindings
                if _binding_matches_gate(binding=binding, gate=gate)
            )
        )
        coverage_reason = None
        coverage_state = "covered"
        if not matching_bindings:
            coverage_state = "missing"
            coverage_reason = f"cutover_gate:{gate.gate_key}:binding_missing"
        statuses.append(
            NativeCutoverGraphGateStatus(
                cutover_gate_id=gate.cutover_gate_id,
                gate_key=gate.gate_key,
                gate_name=gate.gate_name,
                gate_kind=gate.gate_kind,
                gate_status=gate.gate_status,
                target_kind=gate.target_kind,
                target_ref=gate.target_ref,
                required_evidence_refs=_flatten_text_values(gate.required_evidence),
                binding_ids=matching_bindings,
                coverage_state=coverage_state,
                coverage_reason=coverage_reason,
            )
        )
    return tuple(statuses)


def _binding_statuses(
    *,
    run_id: str,
    gates: Sequence[CutoverGateAuthorityRecord],
    work_bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> tuple[NativeCutoverGraphBindingStatus, ...]:
    statuses: list[NativeCutoverGraphBindingStatus] = []
    gate_by_id = {gate.cutover_gate_id: gate for gate in gates}
    for binding in work_bindings:
        linked_gate_keys = tuple(
            gate.gate_key
            for gate in gates
            if _binding_matches_gate(binding=binding, gate=gate)
        )
        linkage_state = "linked"
        linkage_reason = None
        if linked_gate_keys:
            linkage_state = "linked"
        elif binding.workflow_run_id == run_id:
            linkage_state = "run_bound"
        elif binding.cutover_gate_id is not None and binding.cutover_gate_id not in gate_by_id:
            linkage_state = "orphaned"
            linkage_reason = f"work_binding:{binding.work_item_workflow_binding_id}:cutover_gate_missing"
        else:
            linkage_state = "orphaned"
            linkage_reason = f"work_binding:{binding.work_item_workflow_binding_id}:unlinked"
        statuses.append(
            NativeCutoverGraphBindingStatus(
                work_item_workflow_binding_id=binding.work_item_workflow_binding_id,
                binding_kind=binding.binding_kind,
                binding_status=binding.binding_status,
                source_kind=binding.source_kind,
                source_id=binding.source_id,
                cutover_gate_id=binding.cutover_gate_id,
                workflow_class_id=binding.workflow_class_id,
                schedule_definition_id=binding.schedule_definition_id,
                workflow_run_id=binding.workflow_run_id,
                bound_by_decision_id=binding.bound_by_decision_id,
                linked_gate_keys=_dedupe(linked_gate_keys),
                linkage_state=linkage_state,
                linkage_reason=linkage_reason,
            )
        )
    return tuple(statuses)


def _status_state(
    *,
    graph_topology: GraphTopologyReadModel,
    graph_lineage: GraphLineageReadModel,
    cutover_gates: Sequence[NativeCutoverGraphGateStatus],
    work_bindings: Sequence[NativeCutoverGraphBindingStatus],
) -> str:
    planning_graph_complete = (
        graph_topology.completeness.is_complete and graph_lineage.completeness.is_complete
    )
    gate_gap = any(gate.coverage_state != "covered" for gate in cutover_gates)
    binding_gap = any(binding.linkage_state == "orphaned" for binding in work_bindings)
    if not planning_graph_complete:
        return "blocked"
    if gate_gap or binding_gap:
        return "stale"
    return "fresh"


def _status_reason(missing_refs: tuple[str, ...]) -> str | None:
    if not missing_refs:
        return None
    return ", ".join(missing_refs)


def cutover_graph_status_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    operator_control: OperatorControlAuthority,
    work_bindings: Sequence[WorkItemWorkflowBindingRecord],
) -> NativeCutoverGraphStatusReadModel:
    """Build one fail-closed cutover/planning graph status surface."""

    ordered_rows = _sorted_evidence_rows(canonical_evidence)
    graph_topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=ordered_rows,
    )
    graph_lineage = graph_lineage_run(
        run_id=run_id,
        canonical_evidence=ordered_rows,
    )

    relevant_bindings = tuple(
        binding
        for binding in work_bindings
        if _binding_is_relevant(binding=binding, gates=operator_control.cutover_gates, run_id=run_id)
    )
    relevant_bindings = tuple(
        sorted(
            relevant_bindings,
            key=lambda binding: (binding.created_at, binding.work_item_workflow_binding_id),
        )
    )
    cutover_gates = _gate_statuses(
        gates=operator_control.cutover_gates,
        work_bindings=relevant_bindings,
    )
    binding_statuses = _binding_statuses(
        run_id=run_id,
        gates=operator_control.cutover_gates,
        work_bindings=relevant_bindings,
    )

    missing_refs = list(graph_topology.completeness.missing_evidence_refs)
    missing_refs.extend(graph_lineage.completeness.missing_evidence_refs)
    for gate in cutover_gates:
        if gate.coverage_reason is not None:
            missing_refs.append(gate.coverage_reason)
    for binding in binding_statuses:
        if binding.linkage_reason is not None:
            missing_refs.append(binding.linkage_reason)

    missing_refs_tuple = _dedupe(missing_refs)
    return NativeCutoverGraphStatusReadModel(
        run_id=run_id,
        request_id=graph_lineage.request_id,
        watermark=graph_lineage.watermark,
        evidence_refs=graph_lineage.evidence_refs,
        graph_topology=graph_topology,
        graph_lineage=graph_lineage,
        cutover_gates=cutover_gates,
        work_bindings=binding_statuses,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        status_state=_status_state(
            graph_topology=graph_topology,
            graph_lineage=graph_lineage,
            cutover_gates=cutover_gates,
            work_bindings=binding_statuses,
        ),
        status_reason=_status_reason(missing_refs_tuple),
    )


def render_cutover_graph_status(view: NativeCutoverGraphStatusReadModel) -> str:
    """Render the cutover graph status as a machine-readable line surface."""

    lines = [
        "kind: cutover_graph_status",
        f"run_id: {view.run_id}",
        f"request_id: {_format_optional_text(view.request_id)}",
        f"completeness.is_complete: {_format_bool(view.completeness.is_complete)}",
    ]
    _append_indexed_lines(
        lines,
        "completeness.missing_evidence_refs",
        view.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"watermark.evidence_seq: {_format_optional_int(view.watermark.evidence_seq)}",
            f"watermark.source: {view.watermark.source}",
            f"status.state: {view.status_state}",
            f"status.reason: {_format_optional_text(view.status_reason)}",
        ]
    )

    lines.extend(
        [
            f"graph_topology.completeness.is_complete: {_format_bool(view.graph_topology.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "graph_topology.completeness.missing_evidence_refs",
        view.graph_topology.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"graph_topology.watermark.evidence_seq: {_format_optional_int(view.graph_topology.watermark.evidence_seq)}",
            f"graph_topology.watermark.source: {view.graph_topology.watermark.source}",
            f"graph_topology.evidence_refs_count: {len(view.graph_topology.evidence_refs)}",
        ]
    )
    _append_indexed_lines(lines, "graph_topology.evidence_refs", view.graph_topology.evidence_refs)
    lines.extend(
        [
            f"graph_topology.admitted_definition_ref: {_format_optional_text(view.graph_topology.admitted_definition_ref)}",
            f"graph_topology.nodes_count: {len(view.graph_topology.nodes)}",
            f"graph_topology.edges_count: {len(view.graph_topology.edges)}",
            f"graph_topology.runtime_node_order_count: {len(view.graph_topology.runtime_node_order)}",
        ]
    )
    for index, node in enumerate(view.graph_topology.nodes):
        lines.extend(
            [
                f"graph_topology.nodes[{index}].node_id: {node.node_id}",
                f"graph_topology.nodes[{index}].node_type: {node.node_type}",
                f"graph_topology.nodes[{index}].display_name: {node.display_name}",
                f"graph_topology.nodes[{index}].position_index: {node.position_index}",
            ]
        )
    for index, edge in enumerate(view.graph_topology.edges):
        lines.extend(
            [
                f"graph_topology.edges[{index}].edge_id: {edge.edge_id}",
                f"graph_topology.edges[{index}].edge_type: {edge.edge_type}",
                f"graph_topology.edges[{index}].from_node_id: {edge.from_node_id}",
                f"graph_topology.edges[{index}].to_node_id: {edge.to_node_id}",
                f"graph_topology.edges[{index}].position_index: {edge.position_index}",
            ]
        )
    _append_indexed_lines(
        lines,
        "graph_topology.runtime_node_order",
        view.graph_topology.runtime_node_order,
    )

    lines.extend(
        [
            f"graph_lineage.completeness.is_complete: {_format_bool(view.graph_lineage.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "graph_lineage.completeness.missing_evidence_refs",
        view.graph_lineage.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"graph_lineage.watermark.evidence_seq: {_format_optional_int(view.graph_lineage.watermark.evidence_seq)}",
            f"graph_lineage.watermark.source: {view.graph_lineage.watermark.source}",
            f"graph_lineage.evidence_refs_count: {len(view.graph_lineage.evidence_refs)}",
        ]
    )
    _append_indexed_lines(lines, "graph_lineage.evidence_refs", view.graph_lineage.evidence_refs)
    lines.extend(
        [
            f"graph_lineage.claim_received_ref: {_format_optional_text(view.graph_lineage.claim_received_ref)}",
            f"graph_lineage.admitted_definition_ref: {_format_optional_text(view.graph_lineage.admitted_definition_ref)}",
            f"graph_lineage.admitted_definition_hash: {_format_optional_text(view.graph_lineage.admitted_definition_hash)}",
            f"graph_lineage.nodes_count: {len(view.graph_lineage.nodes)}",
            f"graph_lineage.edges_count: {len(view.graph_lineage.edges)}",
            f"graph_lineage.runtime_node_order_count: {len(view.graph_lineage.runtime_node_order)}",
            f"graph_lineage.current_state: {_format_optional_text(view.graph_lineage.current_state)}",
            f"graph_lineage.terminal_reason: {_format_optional_text(view.graph_lineage.terminal_reason)}",
        ]
    )
    for index, node in enumerate(view.graph_lineage.nodes):
        lines.extend(
            [
                f"graph_lineage.nodes[{index}].node_id: {node.node_id}",
                f"graph_lineage.nodes[{index}].node_type: {node.node_type}",
                f"graph_lineage.nodes[{index}].display_name: {node.display_name}",
                f"graph_lineage.nodes[{index}].position_index: {node.position_index}",
            ]
        )
    for index, edge in enumerate(view.graph_lineage.edges):
        lines.extend(
            [
                f"graph_lineage.edges[{index}].edge_id: {edge.edge_id}",
                f"graph_lineage.edges[{index}].edge_type: {edge.edge_type}",
                f"graph_lineage.edges[{index}].from_node_id: {edge.from_node_id}",
                f"graph_lineage.edges[{index}].to_node_id: {edge.to_node_id}",
                f"graph_lineage.edges[{index}].position_index: {edge.position_index}",
            ]
        )
    _append_indexed_lines(
        lines,
        "graph_lineage.runtime_node_order",
        view.graph_lineage.runtime_node_order,
    )

    lines.extend(
        [
            "cutover_gates.kind: native_cutover_gate_status",
            f"cutover_gates.count: {len(view.cutover_gates)}",
        ]
    )
    for index, gate in enumerate(view.cutover_gates):
        lines.extend(
            [
                f"cutover_gates[{index}].cutover_gate_id: {gate.cutover_gate_id}",
                f"cutover_gates[{index}].gate_key: {gate.gate_key}",
                f"cutover_gates[{index}].gate_name: {gate.gate_name}",
                f"cutover_gates[{index}].gate_kind: {gate.gate_kind}",
                f"cutover_gates[{index}].gate_status: {gate.gate_status}",
                f"cutover_gates[{index}].target_kind: {gate.target_kind}",
                f"cutover_gates[{index}].target_ref: {gate.target_ref}",
                f"cutover_gates[{index}].required_evidence_refs_count: {len(gate.required_evidence_refs)}",
            ]
        )
        _append_indexed_lines(
            lines,
            f"cutover_gates[{index}].required_evidence_refs",
            gate.required_evidence_refs,
        )
        lines.extend(
            [
                f"cutover_gates[{index}].binding_ids_count: {len(gate.binding_ids)}",
            ]
        )
        _append_indexed_lines(lines, f"cutover_gates[{index}].binding_ids", gate.binding_ids)
        lines.extend(
            [
                f"cutover_gates[{index}].coverage_state: {gate.coverage_state}",
                f"cutover_gates[{index}].coverage_reason: {_format_optional_text(gate.coverage_reason)}",
            ]
        )

    lines.extend(
        [
            "work_bindings.kind: native_work_binding_status",
            f"work_bindings.count: {len(view.work_bindings)}",
        ]
    )
    for index, binding in enumerate(view.work_bindings):
        lines.extend(
            [
                f"work_bindings[{index}].work_item_workflow_binding_id: {binding.work_item_workflow_binding_id}",
                f"work_bindings[{index}].binding_kind: {binding.binding_kind}",
                f"work_bindings[{index}].binding_status: {binding.binding_status}",
                f"work_bindings[{index}].source.kind: {binding.source_kind}",
                f"work_bindings[{index}].source.id: {binding.source_id}",
                f"work_bindings[{index}].source.cutover_gate_id: {_format_optional_text(binding.cutover_gate_id)}",
                f"work_bindings[{index}].target.workflow_class_id: {_format_optional_text(binding.workflow_class_id)}",
                f"work_bindings[{index}].target.schedule_definition_id: {_format_optional_text(binding.schedule_definition_id)}",
                f"work_bindings[{index}].target.workflow_run_id: {_format_optional_text(binding.workflow_run_id)}",
                f"work_bindings[{index}].bound_by_decision_id: {_format_optional_text(binding.bound_by_decision_id)}",
                f"work_bindings[{index}].linked_gate_keys_count: {len(binding.linked_gate_keys)}",
            ]
        )
        _append_indexed_lines(
            lines,
            f"work_bindings[{index}].linked_gate_keys",
            binding.linked_gate_keys,
        )
        lines.extend(
            [
                f"work_bindings[{index}].linkage_state: {binding.linkage_state}",
                f"work_bindings[{index}].linkage_reason: {_format_optional_text(binding.linkage_reason)}",
            ]
        )
    return "\n".join(lines)


def _receipt_rows(canonical_evidence: Sequence[EvidenceRow]) -> tuple[EvidenceRow, ...]:
    return tuple(
        sorted(
            (
                row
                for row in canonical_evidence
                if isinstance(row, EvidenceRow)
                and row.kind == "receipt"
                and isinstance(row.record, ReceiptV1)
            ),
            key=lambda item: (item.evidence_seq, item.row_id),
        )
    )


def _first_text(value: Mapping[str, object], key: str) -> str | None:
    field_value = value.get(key)
    if isinstance(field_value, str) and field_value:
        return field_value
    return None


def _status_snapshot_from_mapping(
    status_snapshot: Mapping[str, object] | None,
) -> NativeCutoverStatusSnapshot:
    if not isinstance(status_snapshot, Mapping):
        return NativeCutoverStatusSnapshot()
    raw_run = status_snapshot.get("run")
    if isinstance(raw_run, Mapping):
        run = raw_run
    else:
        run = status_snapshot
    return NativeCutoverStatusSnapshot(
        run_id=_first_text(run, "run_id"),
        workflow_id=_first_text(run, "workflow_id"),
        workflow_definition_id=_first_text(run, "workflow_definition_id"),
        request_id=_first_text(run, "request_id"),
        current_state=_first_text(run, "current_state"),
        terminal_reason_code=_first_text(run, "terminal_reason_code"),
        last_event_id=_first_text(run, "last_event_id"),
    )


def _proof_missings(*completeness: ProjectionCompleteness) -> tuple[str, ...]:
    missings: list[str] = []
    for item in completeness:
        missings.extend(item.missing_evidence_refs)
    return _dedupe(missings)


@dataclass(frozen=True, slots=True)
class NativeCutoverStatusSnapshot:
    """Frontdoor status snapshot for one run."""

    run_id: str | None = None
    workflow_id: str | None = None
    workflow_definition_id: str | None = None
    request_id: str | None = None
    current_state: str | None = None
    terminal_reason_code: str | None = None
    last_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class NativeCutoverReceiptSnapshot:
    """Receipt summary derived from canonical evidence."""

    workflow_id: str | None
    request_id: str | None
    row_count: int
    receipt_count: int
    latest_evidence_seq: int | None
    evidence_refs: tuple[str, ...]
    receipt_ids: tuple[str, ...]
    receipt_types: tuple[str, ...]
    completeness: ProjectionCompleteness


@dataclass(frozen=True, slots=True)
class NativeCutoverScoreboardReadModel:
    """One operator-readable cutover scoreboard."""

    run_id: str
    request_id: str | None
    watermark: ProjectionWatermark
    receipts: NativeCutoverReceiptSnapshot
    status: NativeCutoverStatusSnapshot
    operator_proofs: NativeOperatorStatusReadModel
    completeness: ProjectionCompleteness
    readiness_state: str
    readiness_reason: str | None


def _receipt_snapshot(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    operator_proofs: NativeOperatorStatusReadModel,
) -> NativeCutoverReceiptSnapshot:
    ordered_rows = tuple(
        sorted(
            (
                row
                for row in canonical_evidence
                if isinstance(row, EvidenceRow)
            ),
            key=lambda item: (item.evidence_seq, item.row_id),
        )
    )
    receipt_rows = _receipt_rows(ordered_rows)
    if receipt_rows:
        first_receipt = receipt_rows[0].record
        workflow_id = first_receipt.workflow_id
        request_id = first_receipt.request_id
    else:
        workflow_id = None
        request_id = None

    missing_refs: list[str] = []
    if not ordered_rows:
        missing_refs.append(f"run:{run_id}:evidence_missing")
    if len(receipt_rows) == 0:
        missing_refs.append("receipts:missing")
    if len(receipt_rows) * 2 != len(ordered_rows):
        missing_refs.append("receipts:bundle_count_drift")

    receipt_ids = tuple(row.record.receipt_id for row in receipt_rows)
    receipt_types = tuple(row.record.receipt_type for row in receipt_rows)
    latest_evidence_seq = (
        ordered_rows[-1].evidence_seq if ordered_rows else operator_proofs.watermark.evidence_seq
    )
    if latest_evidence_seq is None:
        latest_evidence_seq = operator_proofs.watermark.evidence_seq

    return NativeCutoverReceiptSnapshot(
        workflow_id=workflow_id,
        request_id=request_id,
        row_count=len(ordered_rows),
        receipt_count=len(receipt_rows),
        latest_evidence_seq=latest_evidence_seq,
        evidence_refs=tuple(row.row_id for row in ordered_rows),
        receipt_ids=receipt_ids,
        receipt_types=receipt_types,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs,
            missing_evidence_refs=_dedupe(missing_refs),
        ),
    )


def _status_missing_refs(
    *,
    run_id: str,
    canonical_workflow_id: str | None,
    canonical_request_id: str | None,
    status: NativeCutoverStatusSnapshot,
) -> tuple[str, ...]:
    missing_refs: list[str] = []
    if status.run_id is None:
        missing_refs.append("status:run_id_missing")
    elif status.run_id != run_id:
        missing_refs.append("status:run_id_mismatch")

    if status.current_state is None:
        missing_refs.append("status:current_state_missing")

    if canonical_request_id is None:
        missing_refs.append("status:canonical_request_missing")
    elif status.request_id is not None and status.request_id != canonical_request_id:
        missing_refs.append("status:request_id_mismatch")

    if status.workflow_id is None:
        missing_refs.append("status:workflow_id_missing")
    elif canonical_workflow_id is not None and status.workflow_id != canonical_workflow_id:
        missing_refs.append("status:workflow_id_mismatch")

    return _dedupe(missing_refs)


def _readiness_state(
    *,
    status: NativeCutoverStatusSnapshot,
    receipts: NativeCutoverReceiptSnapshot,
    operator_proofs: NativeOperatorStatusReadModel,
    completeness: ProjectionCompleteness,
) -> str:
    if completeness.is_complete:
        return "ready"
    if (
        receipts.completeness.is_complete
        and operator_proofs.completeness.is_complete
        and status.run_id is None
        and status.request_id is None
        and status.current_state is None
        and status.workflow_id is None
        and status.terminal_reason_code is None
    ):
        return "partially_ready"
    return "blocked"


def _readiness_reason(missing_refs: tuple[str, ...]) -> str | None:
    if not missing_refs:
        return None
    return ", ".join(missing_refs)


def cutover_scoreboard_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    status_snapshot: Mapping[str, object] | None,
    support: NativeOperatorSupportSnapshot,
) -> NativeCutoverScoreboardReadModel:
    """Build one fail-closed cutover scoreboard from canonical evidence."""

    from .operator_dashboard import operator_status_run

    ordered_rows = tuple(
        sorted(
            (
                row
                for row in canonical_evidence
                if isinstance(row, EvidenceRow)
            ),
            key=lambda item: (item.evidence_seq, item.row_id),
        )
    )
    inspect_view = inspect_run(run_id=run_id, canonical_evidence=ordered_rows)
    replay_view = replay_run(run_id=run_id, canonical_evidence=ordered_rows)
    operator_proofs = operator_status_run(
        run_id=run_id,
        canonical_evidence=ordered_rows,
        support=support,
    )
    status = _status_snapshot_from_mapping(status_snapshot)
    receipts = _receipt_snapshot(
        run_id=run_id,
        canonical_evidence=ordered_rows,
        operator_proofs=operator_proofs,
    )

    missing_refs = _proof_missings(
        inspect_view.completeness,
        replay_view.completeness,
        operator_proofs.completeness,
        receipts.completeness,
    )
    missing_refs = _dedupe(
        missing_refs
        + _status_missing_refs(
            run_id=run_id,
            canonical_workflow_id=receipts.workflow_id,
            canonical_request_id=operator_proofs.request_id,
            status=status,
        )
    )

    return NativeCutoverScoreboardReadModel(
        run_id=run_id,
        request_id=operator_proofs.request_id,
        watermark=operator_proofs.watermark,
        receipts=receipts,
        status=status,
        operator_proofs=operator_proofs,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs,
            missing_evidence_refs=missing_refs,
        ),
        readiness_state=_readiness_state(
            status=status,
            receipts=receipts,
            operator_proofs=operator_proofs,
            completeness=ProjectionCompleteness(
                is_complete=not missing_refs,
                missing_evidence_refs=missing_refs,
            ),
        ),
        readiness_reason=_readiness_reason(missing_refs),
    )


def render_cutover_scoreboard(view: NativeCutoverScoreboardReadModel) -> str:
    """Render the scoreboard as a machine-readable line-oriented surface."""

    lines = [
        "kind: cutover_scoreboard",
        f"run_id: {view.run_id}",
        f"request_id: {_format_optional_text(view.request_id)}",
        f"watermark.evidence_seq: {_format_optional_int(view.watermark.evidence_seq)}",
        f"watermark.source: {view.watermark.source}",
        f"completeness.is_complete: {_format_bool(view.completeness.is_complete)}",
    ]
    _append_indexed_lines(
        lines,
        "completeness.missing_evidence_refs",
        view.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"readiness.state: {view.readiness_state}",
            f"readiness.reason: {_format_optional_text(view.readiness_reason)}",
        ]
    )

    lines.extend(
        [
            "receipts.kind: native_receipts",
            f"receipts.workflow_id: {_format_optional_text(view.receipts.workflow_id)}",
            f"receipts.request_id: {_format_optional_text(view.receipts.request_id)}",
            f"receipts.row_count: {view.receipts.row_count}",
            f"receipts.receipt_count: {view.receipts.receipt_count}",
            f"receipts.latest_evidence_seq: {_format_optional_int(view.receipts.latest_evidence_seq)}",
            f"receipts.completeness.is_complete: {_format_bool(view.receipts.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "receipts.completeness.missing_evidence_refs",
        view.receipts.completeness.missing_evidence_refs,
    )
    _append_indexed_lines(lines, "receipts.evidence_refs", view.receipts.evidence_refs)
    _append_indexed_lines(lines, "receipts.receipt_ids", view.receipts.receipt_ids)
    _append_indexed_lines(lines, "receipts.receipt_types", view.receipts.receipt_types)

    lines.extend(
        [
            "status.kind: native_frontdoor_status",
            f"status.run_id: {_format_optional_text(view.status.run_id)}",
            f"status.workflow_id: {_format_optional_text(view.status.workflow_id)}",
            f"status.workflow_definition_id: {_format_optional_text(view.status.workflow_definition_id)}",
            f"status.request_id: {_format_optional_text(view.status.request_id)}",
            f"status.current_state: {_format_optional_text(view.status.current_state)}",
            f"status.terminal_reason_code: {_format_optional_text(view.status.terminal_reason_code)}",
            f"status.last_event_id: {_format_optional_text(view.status.last_event_id)}",
        ]
    )

    lines.extend(
        [
            "operator_proofs.kind: native_operator_status",
            f"operator_proofs.completeness.is_complete: {_format_bool(view.operator_proofs.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "operator_proofs.completeness.missing_evidence_refs",
        view.operator_proofs.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"operator_proofs.watermark.evidence_seq: {_format_optional_int(view.operator_proofs.watermark.evidence_seq)}",
            f"operator_proofs.watermark.source: {view.operator_proofs.watermark.source}",
            f"operator_proofs.outbox_depth: {view.operator_proofs.outbox_depth}",
            f"operator_proofs.outbox.latest_evidence_seq: {_format_optional_int(view.operator_proofs.outbox_latest_evidence_seq)}",
            f"operator_proofs.subscription.checkpoint_id: {_format_optional_text(view.operator_proofs.checkpoint_id)}",
            f"operator_proofs.subscription.subscription_id: {_format_optional_text(view.operator_proofs.subscription_id)}",
            f"operator_proofs.subscription.last_evidence_seq: {_format_optional_int(view.operator_proofs.subscription_last_evidence_seq)}",
            f"operator_proofs.subscription.lag_evidence_seq: {_format_optional_int(view.operator_proofs.subscription_lag_evidence_seq)}",
            f"operator_proofs.subscription.checkpoint_status: {_format_optional_text(view.operator_proofs.checkpoint_status)}",
        ]
    )

    lines.extend(
        [
            f"operator_proofs.graph_topology.completeness.is_complete: {_format_bool(view.operator_proofs.graph_topology.completeness.is_complete)}",
            f"operator_proofs.graph_lineage.completeness.is_complete: {_format_bool(view.operator_proofs.graph_lineage.completeness.is_complete)}",
        ]
    )
    return "\n".join(lines)
