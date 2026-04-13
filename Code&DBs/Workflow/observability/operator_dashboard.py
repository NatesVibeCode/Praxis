"""Combined operator dashboard surfaces over route, workflow-class resolution, and support state.

This module consolidates the native operator cockpit with the lighter operator
status view so the dashboard-facing read APIs live behind one module boundary.
The surfaces remain read-only and continue stitching existing authorities
without introducing new truth.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg

from authority.workflow_class_resolution import WorkflowClassResolutionDecision
from receipts import EvidenceRow
from registry.endpoint_failover import (
    PostgresProviderFailoverAndEndpointAuthorityRepository,
    ProviderEndpointAuthoritySelector,
    ProviderEndpointBindingAuthorityRecord,
    ProviderFailoverAndEndpointAuthority,
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    ProviderFailoverAuthoritySelector,
    ProviderFailoverBindingAuthorityRecord,
)
from registry.provider_routing import ProviderRouteAuthority
from runtime._helpers import _append_indexed_lines, _dedupe, _format_bool, _json_compatible
from storage.postgres import resolve_workflow_database_url

from .graph_lineage import graph_lineage_run
from .graph_topology import graph_topology_run
from .operator_topology import NativeCutoverGraphStatusReadModel, render_cutover_graph_status
from .read_models import (
    GraphLineageReadModel,
    GraphTopologyReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
)

__all__ = [
    "NativeOperatorStatusReadModel",
    "NativeOperatorSupportSnapshot",
    "load_native_operator_support",
    "operator_status_run",
    "render_operator_status",
    "NativeOperatorCockpitDispatchReadModel",
    "NativeOperatorCockpitError",
    "NativeOperatorCockpitFailoverEffectiveSliceReadModel",
    "NativeOperatorCockpitFailoverReadModel",
    "NativeOperatorCockpitFailoverSelectorContract",
    "NativeOperatorCockpitProvenance",
    "NativeOperatorCockpitReadModel",
    "NativeOperatorCockpitRouteReadModel",
    "operator_cockpit_run",
    "operator_cockpit_run_with_failover_contract",
    "render_operator_cockpit",
]


class NativeOperatorCockpitError(RuntimeError):
    """Raised when the cockpit cannot be built safely."""

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


def _normalize_aware_datetime(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise NativeOperatorCockpitError(
            "operator_cockpit.invalid_datetime",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise NativeOperatorCockpitError(
            "operator_cockpit.invalid_datetime",
            f"{field_name} must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _normalize_as_of(value: datetime) -> datetime:
    return _normalize_aware_datetime(value, field_name="as_of")


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NativeOperatorCockpitError(
            "operator_cockpit.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _format_optional_text(value: str | None) -> str:
    return value if value is not None and value != "" else "-"


def _format_optional_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def _format_optional_float(value: float | int | None) -> str:
    return "-" if value is None else str(value)


def _format_optional_datetime(value: datetime | None) -> str:
    return "-" if value is None else value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class NativeOperatorSupportSnapshot:
    """Support-state snapshot for one run's outbox and subscription checkpoint."""

    outbox_depth: int
    outbox_latest_evidence_seq: int | None
    checkpoint_id: str | None = None
    subscription_id: str | None = None
    subscription_last_evidence_seq: int | None = None
    checkpoint_status: str | None = None


@dataclass(frozen=True, slots=True)
class NativeOperatorStatusReadModel:
    """One native operator status view."""

    graph_topology: GraphTopologyReadModel
    graph_lineage: GraphLineageReadModel
    run_id: str
    request_id: str | None
    completeness: ProjectionCompleteness
    watermark: ProjectionWatermark
    evidence_refs: tuple[str, ...]
    outbox_depth: int
    outbox_latest_evidence_seq: int | None
    checkpoint_id: str | None
    subscription_id: str | None
    subscription_last_evidence_seq: int | None
    subscription_lag_evidence_seq: int | None
    checkpoint_status: str | None


async def load_native_operator_support(
    *,
    run_id: str,
    env: Mapping[str, str] | None = None,
    database_url: str | None = None,
) -> NativeOperatorSupportSnapshot:
    """Read the canonical outbox depth and latest subscription checkpoint for one run."""

    resolved_database_url = (
        database_url if database_url is not None else resolve_workflow_database_url(env=env)
    )
    conn = await asyncpg.connect(resolved_database_url)
    try:
        outbox_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)::bigint AS outbox_depth,
                MAX(evidence_seq)::bigint AS outbox_latest_evidence_seq
            FROM workflow_outbox
            WHERE run_id = $1
            """,
            run_id,
        )
        checkpoint_row = await conn.fetchrow(
            """
            SELECT
                checkpoint_id,
                subscription_id,
                last_evidence_seq,
                checkpoint_status
            FROM subscription_checkpoints
            WHERE run_id = $1
            ORDER BY checkpointed_at DESC, checkpoint_id DESC
            LIMIT 1
            """,
            run_id,
        )
        outbox_depth = int(outbox_row["outbox_depth"])
        outbox_latest_evidence_seq = outbox_row["outbox_latest_evidence_seq"]
        if outbox_latest_evidence_seq is not None:
            outbox_latest_evidence_seq = int(outbox_latest_evidence_seq)
        if checkpoint_row is None:
            return NativeOperatorSupportSnapshot(
                outbox_depth=outbox_depth,
                outbox_latest_evidence_seq=outbox_latest_evidence_seq,
            )
        subscription_last_evidence_seq = checkpoint_row["last_evidence_seq"]
        if subscription_last_evidence_seq is not None:
            subscription_last_evidence_seq = int(subscription_last_evidence_seq)
        return NativeOperatorSupportSnapshot(
            outbox_depth=outbox_depth,
            outbox_latest_evidence_seq=outbox_latest_evidence_seq,
            checkpoint_id=str(checkpoint_row["checkpoint_id"]),
            subscription_id=str(checkpoint_row["subscription_id"]),
            subscription_last_evidence_seq=subscription_last_evidence_seq,
            checkpoint_status=str(checkpoint_row["checkpoint_status"]),
        )
    finally:
        await conn.close()


def operator_status_run(
    *,
    run_id: str,
    canonical_evidence: Sequence[EvidenceRow],
    support: NativeOperatorSupportSnapshot,
) -> NativeOperatorStatusReadModel:
    """Build one fail-closed operator status view from canonical evidence and checkpoints."""

    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    lineage = graph_lineage_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    missing_refs = list(topology.completeness.missing_evidence_refs)
    missing_refs.extend(lineage.completeness.missing_evidence_refs)

    if support.outbox_depth < 0:
        raise ValueError("outbox_depth must be a non-negative integer")

    expected_outbox_depth = len(lineage.evidence_refs)
    if support.outbox_depth != expected_outbox_depth:
        missing_refs.append("outbox:depth_drift")

    outbox_latest_evidence_seq = support.outbox_latest_evidence_seq
    if support.outbox_depth > 0 and outbox_latest_evidence_seq is None:
        missing_refs.append("outbox:latest_evidence_seq_missing")
    if (
        outbox_latest_evidence_seq is not None
        and lineage.watermark.evidence_seq is not None
        and outbox_latest_evidence_seq != lineage.watermark.evidence_seq
    ):
        missing_refs.append("outbox:watermark_drift")

    if support.checkpoint_id is None:
        missing_refs.append("subscription:checkpoint_missing")
    if support.subscription_id is None:
        missing_refs.append("subscription:subscription_id_missing")
    if support.subscription_last_evidence_seq is None:
        missing_refs.append("subscription:last_evidence_seq_missing")

    subscription_lag_evidence_seq = None
    if (
        outbox_latest_evidence_seq is not None
        and support.subscription_last_evidence_seq is not None
    ):
        if support.subscription_last_evidence_seq > outbox_latest_evidence_seq:
            missing_refs.append("subscription:lag_inverted")
        else:
            subscription_lag_evidence_seq = (
                outbox_latest_evidence_seq - support.subscription_last_evidence_seq
            )

    missing_refs_tuple = _dedupe(missing_refs)
    return NativeOperatorStatusReadModel(
        run_id=run_id,
        request_id=lineage.request_id,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        watermark=lineage.watermark,
        evidence_refs=lineage.evidence_refs,
        graph_topology=topology,
        graph_lineage=lineage,
        outbox_depth=support.outbox_depth,
        outbox_latest_evidence_seq=outbox_latest_evidence_seq,
        checkpoint_id=support.checkpoint_id,
        subscription_id=support.subscription_id,
        subscription_last_evidence_seq=support.subscription_last_evidence_seq,
        subscription_lag_evidence_seq=subscription_lag_evidence_seq,
        checkpoint_status=support.checkpoint_status,
    )


def render_operator_status(view: NativeOperatorStatusReadModel) -> str:
    """Render the status surface as machine-readable line output."""

    lines = [
        "kind: operator_status",
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
            f"evidence_refs_count: {len(view.evidence_refs)}",
        ]
    )
    _append_indexed_lines(lines, "evidence_refs", view.evidence_refs)

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
            f"graph_topology.nodes_count: {len(view.graph_topology.nodes)}",
            f"graph_topology.edges_count: {len(view.graph_topology.edges)}",
            f"graph_topology.runtime_node_order_count: {len(view.graph_topology.runtime_node_order)}",
            f"graph_topology.admitted_definition_ref: {_format_optional_text(view.graph_topology.admitted_definition_ref)}",
        ]
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
            f"graph_lineage.claim_received_ref: {_format_optional_text(view.graph_lineage.claim_received_ref)}",
            f"graph_lineage.admitted_definition_ref: {_format_optional_text(view.graph_lineage.admitted_definition_ref)}",
            f"graph_lineage.admitted_definition_hash: {_format_optional_text(view.graph_lineage.admitted_definition_hash)}",
            f"graph_lineage.current_state: {_format_optional_text(view.graph_lineage.current_state)}",
            f"graph_lineage.terminal_reason: {_format_optional_text(view.graph_lineage.terminal_reason)}",
        ]
    )

    lines.extend(
        [
            f"outbox.depth: {view.outbox_depth}",
            f"outbox.latest_evidence_seq: {_format_optional_int(view.outbox_latest_evidence_seq)}",
            f"subscription.checkpoint_id: {_format_optional_text(view.checkpoint_id)}",
            f"subscription.subscription_id: {_format_optional_text(view.subscription_id)}",
            f"subscription.last_evidence_seq: {_format_optional_int(view.subscription_last_evidence_seq)}",
            f"subscription.lag_evidence_seq: {_format_optional_int(view.subscription_lag_evidence_seq)}",
            f"subscription.checkpoint_status: {_format_optional_text(view.checkpoint_status)}",
        ]
    )
    return "\n".join(lines)


def _json_text(value: object) -> str:
    return json.dumps(_json_compatible(value), sort_keys=True, separators=(",", ":"))


def _route_missing_refs(authority: ProviderRouteAuthority) -> tuple[str, ...]:
    missing_refs: list[str] = []
    if not authority.provider_route_health_windows:
        missing_refs.append("route:health_windows_missing")
    if not authority.provider_budget_windows:
        missing_refs.append("route:budget_windows_missing")
    if not authority.route_eligibility_states:
        missing_refs.append("route:eligibility_states_missing")
    return _dedupe(missing_refs)


def _route_integrity_missing_refs(authority: ProviderRouteAuthority) -> tuple[str, ...]:
    """Mirror runtime-grade route-authority validation before calling a cockpit complete."""

    missing_refs: list[str] = []
    known_window_refs: set[str] = set()

    for candidate_ref, records in authority.provider_route_health_windows.items():
        normalized_candidate_ref = candidate_ref.strip() if candidate_ref.strip() else candidate_ref
        for index, record in enumerate(records):
            if record.candidate_ref != normalized_candidate_ref:
                missing_refs.append(
                    f"route:health_windows[{candidate_ref}].record[{index}]:candidate_ref_mismatch"
                )
            if record.provider_route_health_window_id:
                known_window_refs.add(record.provider_route_health_window_id)

    for provider_policy_id, records in authority.provider_budget_windows.items():
        normalized_provider_policy_id = (
            provider_policy_id.strip() if provider_policy_id.strip() else provider_policy_id
        )
        for index, record in enumerate(records):
            if record.provider_policy_id != normalized_provider_policy_id:
                missing_refs.append(
                    f"route:budget_windows[{provider_policy_id}].record[{index}]:provider_policy_id_mismatch"
                )
            if record.provider_budget_window_id:
                known_window_refs.add(record.provider_budget_window_id)

    for candidate_ref, records in authority.route_eligibility_states.items():
        normalized_candidate_ref = candidate_ref.strip() if candidate_ref.strip() else candidate_ref
        for index, record in enumerate(records):
            if record.candidate_ref != normalized_candidate_ref:
                missing_refs.append(
                    f"route:eligibility_states[{candidate_ref}].record[{index}]:candidate_ref_mismatch"
                )
            if not record.source_window_refs:
                missing_refs.append(
                    f"route:eligibility_states[{candidate_ref}].record[{index}]:source_window_refs_missing"
                )
                continue
            unknown_window_refs = tuple(
                ref for ref in record.source_window_refs if ref not in known_window_refs
            )
            if unknown_window_refs:
                missing_refs.append(
                    f"route:eligibility_states[{candidate_ref}].record[{index}]:unknown_source_window_refs"
                )

    return _dedupe(missing_refs)


def _route_completeness(authority: ProviderRouteAuthority) -> ProjectionCompleteness:
    missing_refs = list(_route_missing_refs(authority))
    missing_refs.extend(_route_integrity_missing_refs(authority))
    missing_refs_tuple = _dedupe(missing_refs)
    return ProjectionCompleteness(
        is_complete=not missing_refs_tuple,
        missing_evidence_refs=missing_refs_tuple,
    )


def _require_route_authority(value: object) -> ProviderRouteAuthority:
    if isinstance(value, ProviderRouteAuthority):
        return value
    if value is None:
        raise NativeOperatorCockpitError(
            "operator_cockpit.route_authority_missing",
            "route authority is required for cockpit truth",
            details={"value_type": "NoneType"},
        )
    raise NativeOperatorCockpitError(
        "operator_cockpit.invalid_route_authority",
        "route_authority must be a ProviderRouteAuthority",
        details={"value_type": type(value).__name__},
    )


def _dispatch_completeness(_: WorkflowClassResolutionDecision) -> ProjectionCompleteness:
    return ProjectionCompleteness(is_complete=True, missing_evidence_refs=())


def _cockpit_state(
    *,
    route: ProjectionCompleteness,
    failover: ProjectionCompleteness | None,
    dispatch: ProjectionCompleteness,
    cutover: NativeCutoverGraphStatusReadModel,
    snapshot_is_coherent: bool,
) -> str:
    if (
        snapshot_is_coherent
        and route.is_complete
        and (failover is None or failover.is_complete)
        and dispatch.is_complete
        and cutover.completeness.is_complete
    ):
        return cutover.status_state
    if cutover.status_state == "blocked":
        return "blocked"
    return "stale"


def _cockpit_reason(
    *,
    route: ProjectionCompleteness,
    failover: ProjectionCompleteness | None,
    dispatch: ProjectionCompleteness,
    cutover: NativeCutoverGraphStatusReadModel,
    snapshot_missing_refs: Sequence[str] = (),
) -> str | None:
    missing_refs = list(route.missing_evidence_refs)
    if failover is not None:
        missing_refs.extend(failover.missing_evidence_refs)
    missing_refs.extend(dispatch.missing_evidence_refs)
    missing_refs.extend(cutover.completeness.missing_evidence_refs)
    missing_refs.extend(snapshot_missing_refs)
    missing_refs_tuple = _dedupe(missing_refs)
    if missing_refs_tuple:
        return ", ".join(missing_refs_tuple)
    return cutover.status_reason


def _sorted_groups(
    mapping: Mapping[str, tuple[object, ...]]
) -> tuple[tuple[str, tuple[object, ...]], ...]:
    return tuple(sorted(mapping.items(), key=lambda item: item[0]))


def _authority_slice_key(
    record: ProviderFailoverBindingAuthorityRecord | ProviderEndpointBindingAuthorityRecord,
) -> tuple[datetime, datetime | None, str]:
    return (
        _normalize_aware_datetime(record.effective_from, field_name="effective_from"),
        (
            None
            if record.effective_to is None
            else _normalize_aware_datetime(record.effective_to, field_name="effective_to")
        ),
        record.decision_ref,
    )


def _format_authority_slice_key(
    slice_key: tuple[datetime, datetime | None, str] | None,
) -> str:
    if slice_key is None:
        return "-"
    effective_from, effective_to, decision_ref = slice_key
    return (
        f"effective_from={effective_from.isoformat()},"
        f"effective_to={'' if effective_to is None else effective_to.isoformat()},"
        f"decision_ref={decision_ref}"
    )


def _normalized_optional_selector_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _default_failover_selector_contract() -> NativeOperatorCockpitFailoverSelectorContract:
    return NativeOperatorCockpitFailoverSelectorContract(
        model_profile_id=None,
        provider_policy_id=None,
        binding_scope=None,
        endpoint_ref=None,
        endpoint_kind=None,
    )


def _normalize_failover_selector_contract(
    contract: NativeOperatorCockpitFailoverSelectorContract | None,
) -> tuple[NativeOperatorCockpitFailoverSelectorContract, tuple[str, ...]]:
    if contract is None:
        return _default_failover_selector_contract(), (
            "failover:selector_missing",
            "failover:endpoint_selector_missing",
        )

    model_profile_id = _normalized_optional_selector_text(contract.model_profile_id)
    provider_policy_id = _normalized_optional_selector_text(contract.provider_policy_id)
    binding_scope = _normalized_optional_selector_text(contract.binding_scope)
    endpoint_ref = _normalized_optional_selector_text(contract.endpoint_ref)
    endpoint_kind = _normalized_optional_selector_text(contract.endpoint_kind)
    missing_refs: list[str] = []

    if model_profile_id is None:
        missing_refs.append("failover:selector_model_profile_missing")
    if provider_policy_id is None:
        missing_refs.append("failover:selector_provider_policy_missing")
    if binding_scope is None:
        missing_refs.append("failover:selector_binding_scope_missing")

    if endpoint_ref is None and endpoint_kind is None:
        missing_refs.append("failover:endpoint_selector_missing")
    elif endpoint_ref is not None and endpoint_kind is not None:
        missing_refs.append("failover:endpoint_selector_ambiguous")

    return (
        NativeOperatorCockpitFailoverSelectorContract(
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            binding_scope=binding_scope,
            endpoint_ref=endpoint_ref,
            endpoint_kind=endpoint_kind,
        ),
        _dedupe(missing_refs),
    )


def _selected_failover_binding(
    failover_records: Sequence[ProviderFailoverBindingAuthorityRecord],
) -> tuple[ProviderFailoverBindingAuthorityRecord | None, tuple[str, ...]]:
    if not failover_records:
        return None, ("failover:bindings_missing",)

    selected_position_index = min(record.position_index for record in failover_records)
    selected_bindings = tuple(
        record for record in failover_records if record.position_index == selected_position_index
    )
    if len(selected_bindings) != 1:
        return None, ("failover:selected_candidate_ambiguous",)
    return selected_bindings[0], ()


def _failover_missing_refs_from_repository_error(
    error: ProviderFailoverAndEndpointAuthorityRepositoryError,
) -> tuple[str, ...]:
    reason_map = {
        "endpoint_failover.invalid_selector": (
            "failover:selector_invalid",
        ),
        "endpoint_failover.failover_missing": (
            "failover:bindings_missing",
        ),
        "endpoint_failover.ambiguous_failover_slice": (
            "failover:effective_slice_ambiguous",
        ),
        "endpoint_failover.endpoint_missing": (
            "failover:endpoint_binding_missing",
        ),
        "endpoint_failover.ambiguous_endpoint_slice": (
            "failover:endpoint_slice_ambiguous",
        ),
        "endpoint_failover.read_failed": (
            "failover:authority_read_failed",
        ),
    }
    return reason_map.get(error.reason_code, ("failover:authority_load_failed",))


def _route_snapshot_markers(authority: ProviderRouteAuthority) -> tuple[datetime, ...]:
    markers: list[datetime] = []
    for grouped_rows in (
        authority.provider_route_health_windows,
        authority.provider_budget_windows,
        authority.route_eligibility_states,
    ):
        for group_key, group in _sorted_groups(grouped_rows):
            if not group:
                continue
            latest = group[0]
            markers.append(
                _normalize_aware_datetime(
                    latest.created_at,
                    field_name=f"route[{group_key}].latest.created_at",
                )
            )
    return tuple(markers)


def _snapshot_coherence_missing_refs(
    *,
    route_authority: ProviderRouteAuthority,
    dispatch_resolution: WorkflowClassResolutionDecision,
    cutover_status: NativeCutoverGraphStatusReadModel,
    as_of: datetime,
) -> tuple[str, ...]:
    missing_refs: list[str] = []

    route_markers = _route_snapshot_markers(route_authority)
    if route_markers:
        if any(marker != route_markers[0] for marker in route_markers[1:]):
            missing_refs.append("route:snapshot_mixed_time")
        elif route_markers[0] != as_of:
            missing_refs.append("route:snapshot_as_of_mismatch")
    else:
        missing_refs.append("route:snapshot_unavailable")

    dispatch_as_of = _normalize_aware_datetime(
        dispatch_resolution.as_of,
        field_name="dispatch.as_of",
    )
    if dispatch_as_of != as_of:
        missing_refs.append("dispatch:snapshot_as_of_mismatch")

    watermark_sources = (
        cutover_status.watermark.source,
        cutover_status.graph_topology.watermark.source,
        cutover_status.graph_lineage.watermark.source,
    )
    if any(marker != watermark_sources[0] for marker in watermark_sources[1:]):
        missing_refs.append("cutover:snapshot_watermark_source_mismatch")

    watermark_evidence_seqs = (
        cutover_status.watermark.evidence_seq,
        cutover_status.graph_topology.watermark.evidence_seq,
        cutover_status.graph_lineage.watermark.evidence_seq,
    )
    if any(marker is None for marker in watermark_evidence_seqs):
        missing_refs.append("cutover:snapshot_watermark_missing")
    elif any(marker != watermark_evidence_seqs[0] for marker in watermark_evidence_seqs[1:]):
        missing_refs.append("cutover:snapshot_watermark_mismatch")

    return _dedupe(missing_refs)


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitProvenance:
    """Shared provenance envelope for the stitched cockpit."""

    as_of: datetime
    route_authority: str = (
        "registry.provider_routing.load_provider_route_authority_snapshot"
    )
    failover_authority: str | None = None
    dispatch_authority: str = "authority.workflow_class_resolution.load_workflow_class_resolution_runtime"
    cutover_authority: str = "observability.operator_topology.cutover_graph_status_run"
    stitched_sections: tuple[str, ...] = ("route", "dispatch", "cutover")

    def to_json(self) -> dict[str, Any]:
        section_authorities = {
            "route": self.route_authority,
            "dispatch": self.dispatch_authority,
            "cutover": self.cutover_authority,
        }
        if self.failover_authority is not None:
            section_authorities["failover"] = self.failover_authority
        return {
            "kind": "operator_cockpit_provenance",
            "as_of": self.as_of.isoformat(),
            "section_authorities": section_authorities,
            "stitched_sections": list(self.stitched_sections),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitRouteReadModel:
    """Route authority section for the cockpit."""

    authority: ProviderRouteAuthority
    completeness: ProjectionCompleteness

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "provider_route_control_tower",
            "completeness": _json_compatible(self.completeness),
            "provider_route_health_windows": _json_compatible(
                self.authority.provider_route_health_windows
            ),
            "provider_budget_windows": _json_compatible(self.authority.provider_budget_windows),
            "route_eligibility_states": _json_compatible(
                self.authority.route_eligibility_states
            ),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitDispatchReadModel:
    """Workflow-class resolution section for the cockpit."""

    resolution: WorkflowClassResolutionDecision
    completeness: ProjectionCompleteness

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "workflow_class_resolution",
            "completeness": _json_compatible(self.completeness),
            "resolution": _json_compatible(self.resolution),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitFailoverSelectorContract:
    """Bounded selector tuple the cockpit owns for the failover section."""

    model_profile_id: str | None
    provider_policy_id: str | None
    binding_scope: str | None
    endpoint_ref: str | None = None
    endpoint_kind: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "model_profile_id": self.model_profile_id,
            "provider_policy_id": self.provider_policy_id,
            "binding_scope": self.binding_scope,
            "endpoint_ref": self.endpoint_ref,
            "endpoint_kind": self.endpoint_kind,
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitFailoverEffectiveSliceReadModel:
    """One bounded failover effective slice plus the endpoint bound onto it."""

    model_profile_id: str
    provider_policy_id: str
    binding_scope: str
    slice_binding_count: int
    slice_candidate_refs: tuple[str, ...]
    selected_candidate_ref: str
    selected_position_index: int
    selected_failover_role: str
    selected_trigger_rule: str
    selected_provider_failover_binding_id: str
    failover_decision_ref: str
    failover_created_at: datetime
    failover_slice_key: str
    endpoint_binding_id: str | None
    endpoint_ref: str | None
    endpoint_kind: str | None
    endpoint_uri: str | None
    endpoint_decision_ref: str | None
    endpoint_created_at: datetime | None
    endpoint_slice_key: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "model_profile_id": self.model_profile_id,
            "provider_policy_id": self.provider_policy_id,
            "binding_scope": self.binding_scope,
            "slice_binding_count": self.slice_binding_count,
            "slice_candidate_refs": list(self.slice_candidate_refs),
            "selected_candidate_ref": self.selected_candidate_ref,
            "selected_position_index": self.selected_position_index,
            "selected_failover_role": self.selected_failover_role,
            "selected_trigger_rule": self.selected_trigger_rule,
            "selected_provider_failover_binding_id": self.selected_provider_failover_binding_id,
            "failover_decision_ref": self.failover_decision_ref,
            "failover_created_at": self.failover_created_at.isoformat(),
            "failover_slice_key": self.failover_slice_key,
            "endpoint_binding_id": self.endpoint_binding_id,
            "endpoint_ref": self.endpoint_ref,
            "endpoint_kind": self.endpoint_kind,
            "endpoint_uri": self.endpoint_uri,
            "endpoint_decision_ref": self.endpoint_decision_ref,
            "endpoint_created_at": (
                None if self.endpoint_created_at is None else self.endpoint_created_at.isoformat()
            ),
            "endpoint_slice_key": self.endpoint_slice_key,
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitFailoverReadModel:
    """Failover freshness and provenance from the bounded authority seam."""

    selector_contract: NativeOperatorCockpitFailoverSelectorContract
    selector_as_of: datetime
    completeness: ProjectionCompleteness
    loaded_failover_selector_count: int
    loaded_endpoint_selector_count: int
    freshness_state: str
    freshness_reason: str | None
    effective_slice: NativeOperatorCockpitFailoverEffectiveSliceReadModel | None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "provider_failover_and_endpoint_authority",
            "selector_contract": self.selector_contract.to_json(),
            "selector_as_of": self.selector_as_of.isoformat(),
            "completeness": _json_compatible(self.completeness),
            "loaded_failover_selector_count": self.loaded_failover_selector_count,
            "loaded_endpoint_selector_count": self.loaded_endpoint_selector_count,
            "freshness_state": self.freshness_state,
            "freshness_reason": self.freshness_reason,
            "effective_slice": (
                None if self.effective_slice is None else self.effective_slice.to_json()
            ),
        }


@dataclass(frozen=True, slots=True)
class NativeOperatorCockpitReadModel:
    """One bounded operator cockpit over route, workflow-class resolution, and cutover truth."""

    provenance: NativeOperatorCockpitProvenance
    run_id: str
    request_id: str | None
    as_of: datetime
    watermark: ProjectionWatermark
    route: NativeOperatorCockpitRouteReadModel
    failover: NativeOperatorCockpitFailoverReadModel | None
    dispatch: NativeOperatorCockpitDispatchReadModel
    cutover_status: NativeCutoverGraphStatusReadModel
    completeness: ProjectionCompleteness
    status_state: str
    status_reason: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "operator_cockpit",
            "provenance": self.provenance.to_json(),
            "run_id": self.run_id,
            "request_id": self.request_id,
            "as_of": self.as_of.isoformat(),
            "watermark": _json_compatible(self.watermark),
            "route": self.route.to_json(),
            **(
                {}
                if self.failover is None
                else {"failover": self.failover.to_json()}
            ),
            "dispatch": self.dispatch.to_json(),
            "cutover_status": {
                "kind": "cutover_graph_status",
                **_json_compatible(self.cutover_status),
            },
            "completeness": _json_compatible(self.completeness),
            "status_state": self.status_state,
            "status_reason": self.status_reason,
        }


def _render_route_section(
    lines: list[str],
    *,
    route: NativeOperatorCockpitRouteReadModel,
) -> None:
    authority = route.authority
    health_groups = _sorted_groups(authority.provider_route_health_windows)
    budget_groups = _sorted_groups(authority.provider_budget_windows)
    eligibility_groups = _sorted_groups(authority.route_eligibility_states)

    lines.extend(
        [
            "route.kind: provider_route_control_tower",
            f"route.completeness.is_complete: {_format_bool(route.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "route.completeness.missing_evidence_refs",
        route.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"route.health_windows_group_count: {len(health_groups)}",
            f"route.health_windows_total_count: {sum(len(group) for _, group in health_groups)}",
        ]
    )
    for index, (candidate_ref, group) in enumerate(health_groups):
        latest = group[0]
        lines.extend(
            [
                f"route.health_windows[{index}].candidate_ref: {candidate_ref}",
                f"route.health_windows[{index}].count: {len(group)}",
                f"route.health_windows[{index}].latest.provider_route_health_window_id: {latest.provider_route_health_window_id}",
                f"route.health_windows[{index}].latest.provider_ref: {latest.provider_ref}",
                f"route.health_windows[{index}].latest.health_status: {latest.health_status}",
                f"route.health_windows[{index}].latest.health_score: {_format_optional_float(latest.health_score)}",
                f"route.health_windows[{index}].latest.sample_count: {latest.sample_count}",
                f"route.health_windows[{index}].latest.failure_rate: {_format_optional_float(latest.failure_rate)}",
                f"route.health_windows[{index}].latest.latency_p95_ms: {_format_optional_int(latest.latency_p95_ms)}",
                f"route.health_windows[{index}].latest.observed_window_started_at: {latest.observed_window_started_at.isoformat()}",
                f"route.health_windows[{index}].latest.observed_window_ended_at: {latest.observed_window_ended_at.isoformat()}",
                f"route.health_windows[{index}].latest.observation_ref: {latest.observation_ref}",
                f"route.health_windows[{index}].latest.created_at: {latest.created_at.isoformat()}",
            ]
        )

    lines.extend(
        [
            f"route.budget_windows_group_count: {len(budget_groups)}",
            f"route.budget_windows_total_count: {sum(len(group) for _, group in budget_groups)}",
        ]
    )
    for index, (provider_policy_id, group) in enumerate(budget_groups):
        latest = group[0]
        lines.extend(
            [
                f"route.budget_windows[{index}].provider_policy_id: {provider_policy_id}",
                f"route.budget_windows[{index}].count: {len(group)}",
                f"route.budget_windows[{index}].latest.provider_budget_window_id: {latest.provider_budget_window_id}",
                f"route.budget_windows[{index}].latest.provider_ref: {latest.provider_ref}",
                f"route.budget_windows[{index}].latest.budget_scope: {latest.budget_scope}",
                f"route.budget_windows[{index}].latest.budget_status: {latest.budget_status}",
                f"route.budget_windows[{index}].latest.window_started_at: {latest.window_started_at.isoformat()}",
                f"route.budget_windows[{index}].latest.window_ended_at: {latest.window_ended_at.isoformat()}",
                f"route.budget_windows[{index}].latest.request_limit: {_format_optional_int(latest.request_limit)}",
                f"route.budget_windows[{index}].latest.requests_used: {latest.requests_used}",
                f"route.budget_windows[{index}].latest.token_limit: {_format_optional_int(latest.token_limit)}",
                f"route.budget_windows[{index}].latest.tokens_used: {latest.tokens_used}",
                f"route.budget_windows[{index}].latest.spend_limit_usd: {latest.spend_limit_usd}",
                f"route.budget_windows[{index}].latest.spend_used_usd: {latest.spend_used_usd}",
                f"route.budget_windows[{index}].latest.decision_ref: {latest.decision_ref}",
                f"route.budget_windows[{index}].latest.created_at: {latest.created_at.isoformat()}",
            ]
        )

    lines.extend(
        [
            f"route.eligibility_states_group_count: {len(eligibility_groups)}",
            f"route.eligibility_states_total_count: {sum(len(group) for _, group in eligibility_groups)}",
        ]
    )
    for index, (candidate_ref, group) in enumerate(eligibility_groups):
        latest = group[0]
        lines.extend(
            [
                f"route.eligibility_states[{index}].candidate_ref: {candidate_ref}",
                f"route.eligibility_states[{index}].count: {len(group)}",
                f"route.eligibility_states[{index}].latest.route_eligibility_state_id: {latest.route_eligibility_state_id}",
                f"route.eligibility_states[{index}].latest.model_profile_id: {latest.model_profile_id}",
                f"route.eligibility_states[{index}].latest.provider_policy_id: {latest.provider_policy_id}",
                f"route.eligibility_states[{index}].latest.eligibility_status: {latest.eligibility_status}",
                f"route.eligibility_states[{index}].latest.reason_code: {latest.reason_code}",
                f"route.eligibility_states[{index}].latest.evaluated_at: {latest.evaluated_at.isoformat()}",
                f"route.eligibility_states[{index}].latest.expires_at: {_format_optional_text(latest.expires_at.isoformat() if latest.expires_at is not None else None)}",
                f"route.eligibility_states[{index}].latest.decision_ref: {latest.decision_ref}",
                f"route.eligibility_states[{index}].latest.created_at: {latest.created_at.isoformat()}",
            ]
        )
        _append_indexed_lines(
            lines,
            f"route.eligibility_states[{index}].latest.source_window_refs",
            latest.source_window_refs,
        )


def _render_dispatch_section(
    lines: list[str],
    *,
    dispatch: NativeOperatorCockpitDispatchReadModel,
) -> None:
    resolution = dispatch.resolution
    class_record = resolution.workflow_class
    lane_policy = resolution.lane_policy
    lines.extend(
        [
            "dispatch.kind: workflow_class_resolution",
            f"dispatch.completeness.is_complete: {_format_bool(dispatch.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "dispatch.completeness.missing_evidence_refs",
        dispatch.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"dispatch.as_of: {resolution.as_of.isoformat()}",
            f"dispatch.workflow_class_id: {class_record.workflow_class_id}",
            f"dispatch.class_name: {class_record.class_name}",
            f"dispatch.class_kind: {class_record.class_kind}",
            f"dispatch.workflow_lane_id: {class_record.workflow_lane_id}",
            f"workflow.review_required: {_format_bool(class_record.review_required)}",
            f"dispatch.decision_ref: {class_record.decision_ref}",
            f"dispatch.workflow_lane_policy_id: {lane_policy.workflow_lane_policy_id}",
            f"dispatch.policy_scope: {lane_policy.policy_scope}",
            f"dispatch.work_kind: {lane_policy.work_kind}",
            f"dispatch.lane_policy.decision_ref: {lane_policy.decision_ref}",
            f"dispatch.queue_shape_json: {_json_text(class_record.queue_shape)}",
            f"dispatch.throttle_policy_json: {_json_text(class_record.throttle_policy)}",
            f"dispatch.match_rules_json: {_json_text(lane_policy.match_rules)}",
            f"dispatch.lane_parameters_json: {_json_text(lane_policy.lane_parameters)}",
        ]
        )


def _selector_contract_from_loaded_authority(
    authority: ProviderFailoverAndEndpointAuthority,
) -> NativeOperatorCockpitFailoverSelectorContract:
    failover_groups = tuple(authority.provider_failover_bindings.items())
    endpoint_groups = tuple(authority.provider_endpoint_bindings.items())
    failover_selector = failover_groups[0][0] if len(failover_groups) == 1 else None
    endpoint_selector = endpoint_groups[0][0] if len(endpoint_groups) == 1 else None
    return NativeOperatorCockpitFailoverSelectorContract(
        model_profile_id=(
            None if failover_selector is None else failover_selector.model_profile_id
        ),
        provider_policy_id=(
            None if failover_selector is None else failover_selector.provider_policy_id
        ),
        binding_scope=(None if failover_selector is None else failover_selector.binding_scope),
        endpoint_ref=(None if endpoint_selector is None else endpoint_selector.endpoint_ref),
        endpoint_kind=(None if endpoint_selector is None else endpoint_selector.endpoint_kind),
    )


def _build_failover_read_model(
    authority: ProviderFailoverAndEndpointAuthority,
    *,
    as_of: datetime,
    selector_contract: NativeOperatorCockpitFailoverSelectorContract | None = None,
    additional_missing_refs: Sequence[str] = (),
    expect_failover_selector: bool = True,
    expect_endpoint_selector: bool = True,
) -> NativeOperatorCockpitFailoverReadModel:
    if not isinstance(authority, ProviderFailoverAndEndpointAuthority):
        raise NativeOperatorCockpitError(
            "operator_cockpit.invalid_failover_authority",
            "failover_authority must be a ProviderFailoverAndEndpointAuthority",
            details={"value_type": type(authority).__name__},
        )

    failover_groups = tuple(authority.provider_failover_bindings.items())
    endpoint_groups = tuple(authority.provider_endpoint_bindings.items())
    normalized_contract = (
        _selector_contract_from_loaded_authority(authority)
        if selector_contract is None
        else selector_contract
    )
    missing_refs: list[str] = list(additional_missing_refs)
    freshness_reason: str | None = None
    effective_slice: NativeOperatorCockpitFailoverEffectiveSliceReadModel | None = None

    if not failover_groups and expect_failover_selector:
        missing_refs.append("failover:bindings_missing")
    elif len(failover_groups) > 1:
        missing_refs.append("failover:selector_ambiguous")

    if not endpoint_groups and expect_endpoint_selector:
        missing_refs.append("failover:endpoint_binding_missing")
    elif len(endpoint_groups) > 1:
        missing_refs.append("failover:endpoint_selector_ambiguous")

    failover_records: tuple[ProviderFailoverBindingAuthorityRecord, ...] = ()
    endpoint_binding: ProviderEndpointBindingAuthorityRecord | None = None

    if len(failover_groups) == 1:
        failover_selector, failover_records = failover_groups[0]
        if failover_selector.as_of != as_of:
            missing_refs.append("failover:selector_as_of_mismatch")
    if len(endpoint_groups) == 1:
        endpoint_selector, endpoint_binding = endpoint_groups[0]
        if endpoint_selector.as_of != as_of:
            missing_refs.append("failover:endpoint_selector_as_of_mismatch")

    if failover_records:
        slice_keys = {_authority_slice_key(record) for record in failover_records}
        if len(slice_keys) != 1:
            missing_refs.append("failover:effective_slice_ambiguous")

        selected_binding, selection_missing_refs = _selected_failover_binding(failover_records)
        if selection_missing_refs:
            missing_refs.extend(selection_missing_refs)
        elif selected_binding is not None:
            failover_slice_key = _authority_slice_key(selected_binding)
            endpoint_slice_key: tuple[datetime, datetime | None, str] | None = None

            if endpoint_binding is not None:
                endpoint_slice_key = _authority_slice_key(endpoint_binding)
                if endpoint_binding.candidate_ref != selected_binding.candidate_ref:
                    missing_refs.append("failover:endpoint_candidate_mismatch")
                if endpoint_slice_key != failover_slice_key:
                    missing_refs.append("failover:endpoint_slice_stale")
                    freshness_reason = (
                        "active endpoint binding did not share the failover effective slice: "
                        f"failover={_format_authority_slice_key(failover_slice_key)}; "
                        f"endpoint={_format_authority_slice_key(endpoint_slice_key)}"
                    )

            effective_slice = NativeOperatorCockpitFailoverEffectiveSliceReadModel(
                model_profile_id=selected_binding.model_profile_id,
                provider_policy_id=selected_binding.provider_policy_id,
                binding_scope=selected_binding.binding_scope,
                slice_binding_count=len(failover_records),
                slice_candidate_refs=tuple(
                    binding.candidate_ref for binding in failover_records
                ),
                selected_candidate_ref=selected_binding.candidate_ref,
                selected_position_index=selected_binding.position_index,
                selected_failover_role=selected_binding.failover_role,
                selected_trigger_rule=selected_binding.trigger_rule,
                selected_provider_failover_binding_id=selected_binding.provider_failover_binding_id,
                failover_decision_ref=selected_binding.decision_ref,
                failover_created_at=_normalize_aware_datetime(
                    selected_binding.created_at,
                    field_name="selected_failover_binding.created_at",
                ),
                failover_slice_key=_format_authority_slice_key(failover_slice_key),
                endpoint_binding_id=(
                    None if endpoint_binding is None else endpoint_binding.provider_endpoint_binding_id
                ),
                endpoint_ref=(None if endpoint_binding is None else endpoint_binding.endpoint_ref),
                endpoint_kind=(None if endpoint_binding is None else endpoint_binding.endpoint_kind),
                endpoint_uri=(None if endpoint_binding is None else endpoint_binding.endpoint_uri),
                endpoint_decision_ref=(
                    None if endpoint_binding is None else endpoint_binding.decision_ref
                ),
                endpoint_created_at=(
                    None
                    if endpoint_binding is None
                    else _normalize_aware_datetime(
                        endpoint_binding.created_at,
                        field_name="endpoint_binding.created_at",
                    )
                ),
                endpoint_slice_key=_format_authority_slice_key(endpoint_slice_key),
            )

    missing_refs_tuple = _dedupe(missing_refs)
    if freshness_reason is None and not missing_refs_tuple and effective_slice is not None:
        freshness_reason = None
    elif freshness_reason is None and missing_refs_tuple:
        freshness_reason = ", ".join(missing_refs_tuple)

    return NativeOperatorCockpitFailoverReadModel(
        selector_contract=normalized_contract,
        selector_as_of=as_of,
        completeness=ProjectionCompleteness(
            is_complete=not missing_refs_tuple,
            missing_evidence_refs=missing_refs_tuple,
        ),
        loaded_failover_selector_count=len(failover_groups),
        loaded_endpoint_selector_count=len(endpoint_groups),
        freshness_state=(
            "fresh" if not missing_refs_tuple and effective_slice is not None else "stale"
        ),
        freshness_reason=freshness_reason,
        effective_slice=effective_slice,
    )


def _render_failover_section(
    lines: list[str],
    *,
    failover: NativeOperatorCockpitFailoverReadModel,
) -> None:
    lines.extend(
        [
            "failover.kind: provider_failover_and_endpoint_authority",
            "failover.selector.model_profile_id: "
            f"{_format_optional_text(failover.selector_contract.model_profile_id)}",
            "failover.selector.provider_policy_id: "
            f"{_format_optional_text(failover.selector_contract.provider_policy_id)}",
            "failover.selector.binding_scope: "
            f"{_format_optional_text(failover.selector_contract.binding_scope)}",
            f"failover.selector.as_of: {failover.selector_as_of.isoformat()}",
            "failover.selector.endpoint_ref: "
            f"{_format_optional_text(failover.selector_contract.endpoint_ref)}",
            "failover.selector.endpoint_kind: "
            f"{_format_optional_text(failover.selector_contract.endpoint_kind)}",
            f"failover.completeness.is_complete: {_format_bool(failover.completeness.is_complete)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "failover.completeness.missing_evidence_refs",
        failover.completeness.missing_evidence_refs,
    )
    lines.extend(
        [
            f"failover.loaded_failover_selector_count: {failover.loaded_failover_selector_count}",
            f"failover.loaded_endpoint_selector_count: {failover.loaded_endpoint_selector_count}",
            f"failover.freshness.state: {failover.freshness_state}",
            f"failover.freshness.reason: {_format_optional_text(failover.freshness_reason)}",
        ]
    )

    effective_slice = failover.effective_slice
    lines.extend(
        [
            "failover.effective_slice.present: "
            f"{_format_bool(effective_slice is not None)}",
            "failover.effective_slice.model_profile_id: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.model_profile_id)}",
            "failover.effective_slice.provider_policy_id: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.provider_policy_id)}",
            "failover.effective_slice.binding_scope: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.binding_scope)}",
            "failover.effective_slice.slice_binding_count: "
            f"{_format_optional_int(None if effective_slice is None else effective_slice.slice_binding_count)}",
            "failover.effective_slice.selected_candidate_ref: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.selected_candidate_ref)}",
            "failover.effective_slice.selected_position_index: "
            f"{_format_optional_int(None if effective_slice is None else effective_slice.selected_position_index)}",
            "failover.effective_slice.selected_failover_role: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.selected_failover_role)}",
            "failover.effective_slice.selected_trigger_rule: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.selected_trigger_rule)}",
            "failover.effective_slice.selected_provider_failover_binding_id: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.selected_provider_failover_binding_id)}",
            "failover.effective_slice.failover_decision_ref: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.failover_decision_ref)}",
            "failover.effective_slice.failover_created_at: "
            f"{_format_optional_datetime(None if effective_slice is None else effective_slice.failover_created_at)}",
            "failover.effective_slice.failover_slice_key: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.failover_slice_key)}",
            "failover.effective_slice.endpoint_binding_id: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_binding_id)}",
            "failover.effective_slice.endpoint_ref: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_ref)}",
            "failover.effective_slice.endpoint_kind: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_kind)}",
            "failover.effective_slice.endpoint_uri: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_uri)}",
            "failover.effective_slice.endpoint_decision_ref: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_decision_ref)}",
            "failover.effective_slice.endpoint_created_at: "
            f"{_format_optional_datetime(None if effective_slice is None else effective_slice.endpoint_created_at)}",
            "failover.effective_slice.endpoint_slice_key: "
            f"{_format_optional_text(None if effective_slice is None else effective_slice.endpoint_slice_key)}",
        ]
    )
    _append_indexed_lines(
        lines,
        "failover.effective_slice.slice_candidate_refs",
        () if effective_slice is None else effective_slice.slice_candidate_refs,
    )


async def _load_bounded_failover_read_model(
    *,
    conn: asyncpg.Connection,
    as_of: datetime,
    failover_contract: NativeOperatorCockpitFailoverSelectorContract | None,
) -> NativeOperatorCockpitFailoverReadModel:
    normalized_contract, selector_missing_refs = _normalize_failover_selector_contract(
        failover_contract
    )
    repository = PostgresProviderFailoverAndEndpointAuthorityRepository(conn)
    provider_failover_bindings: dict[
        ProviderFailoverAuthoritySelector,
        tuple[ProviderFailoverBindingAuthorityRecord, ...],
    ] = {}
    provider_endpoint_bindings: dict[
        ProviderEndpointAuthoritySelector,
        ProviderEndpointBindingAuthorityRecord,
    ] = {}
    missing_refs = list(selector_missing_refs)
    failover_records: tuple[ProviderFailoverBindingAuthorityRecord, ...] = ()

    if not selector_missing_refs or not any(
        ref.startswith("failover:selector_") for ref in selector_missing_refs
    ):
        if (
            normalized_contract.model_profile_id is not None
            and normalized_contract.provider_policy_id is not None
            and normalized_contract.binding_scope is not None
        ):
            failover_selector = ProviderFailoverAuthoritySelector(
                model_profile_id=normalized_contract.model_profile_id,
                provider_policy_id=normalized_contract.provider_policy_id,
                binding_scope=normalized_contract.binding_scope,
                as_of=as_of,
            )
            try:
                failover_records = await repository.fetch_provider_failover_bindings(
                    selector=failover_selector
                )
                provider_failover_bindings[failover_selector] = failover_records
            except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
                missing_refs.extend(_failover_missing_refs_from_repository_error(exc))

    selected_binding, selection_missing_refs = _selected_failover_binding(failover_records)
    if selection_missing_refs and failover_records:
        missing_refs.extend(selection_missing_refs)

    endpoint_selector_missing = (
        "failover:endpoint_selector_missing" in selector_missing_refs
        or "failover:endpoint_selector_ambiguous" in selector_missing_refs
    )
    if (
        selected_binding is not None
        and not endpoint_selector_missing
        and normalized_contract.provider_policy_id is not None
        and normalized_contract.binding_scope is not None
    ):
        endpoint_selector = ProviderEndpointAuthoritySelector(
            provider_policy_id=normalized_contract.provider_policy_id,
            candidate_ref=selected_binding.candidate_ref,
            binding_scope=normalized_contract.binding_scope,
            as_of=as_of,
            endpoint_ref=normalized_contract.endpoint_ref,
            endpoint_kind=normalized_contract.endpoint_kind,
        )
        try:
            provider_endpoint_bindings[endpoint_selector] = await repository.fetch_endpoint_binding(
                selector=endpoint_selector
            )
        except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
            missing_refs.extend(_failover_missing_refs_from_repository_error(exc))

    authority = ProviderFailoverAndEndpointAuthority(
        provider_failover_bindings=provider_failover_bindings,
        provider_endpoint_bindings=provider_endpoint_bindings,
    )
    expect_failover_selector = (
        normalized_contract.model_profile_id is not None
        and normalized_contract.provider_policy_id is not None
        and normalized_contract.binding_scope is not None
    )
    expect_endpoint_selector = (
        (normalized_contract.endpoint_ref is None)
        != (normalized_contract.endpoint_kind is None)
    )
    return _build_failover_read_model(
        authority,
        as_of=as_of,
        selector_contract=normalized_contract,
        additional_missing_refs=_dedupe(missing_refs),
        expect_failover_selector=expect_failover_selector,
        expect_endpoint_selector=expect_endpoint_selector,
    )


def _operator_cockpit_read_model(
    *,
    run_id: str,
    as_of: datetime,
    route_authority: ProviderRouteAuthority,
    failover: NativeOperatorCockpitFailoverReadModel | None,
    dispatch_resolution: WorkflowClassResolutionDecision,
    cutover_status: NativeCutoverGraphStatusReadModel,
) -> NativeOperatorCockpitReadModel:
    """Build one fail-closed cockpit from route, workflow-class resolution, and cutover truth."""

    normalized_run_id = _require_text(run_id, field_name="run_id")
    normalized_as_of = _normalize_as_of(as_of)
    normalized_route_authority = _require_route_authority(route_authority)
    if cutover_status is None:
        raise NativeOperatorCockpitError(
            "operator_cockpit.cutover_status_missing",
            "cutover status is required for cockpit truth",
            details={"value_type": "NoneType"},
        )
    if not isinstance(cutover_status, NativeCutoverGraphStatusReadModel):
        raise NativeOperatorCockpitError(
            "operator_cockpit.invalid_cutover_status",
            "cutover_status must be a NativeCutoverGraphStatusReadModel",
            details={"value_type": type(cutover_status).__name__},
        )
    if cutover_status.run_id != normalized_run_id:
        raise NativeOperatorCockpitError(
            "operator_cockpit.run_id_mismatch",
            "cutover status must belong to the requested run_id",
            details={
                "run_id": normalized_run_id,
                "cutover_run_id": cutover_status.run_id,
            },
        )
    snapshot_refs = _snapshot_coherence_missing_refs(
        route_authority=normalized_route_authority,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
        as_of=normalized_as_of,
    )

    route = NativeOperatorCockpitRouteReadModel(
        authority=normalized_route_authority,
        completeness=_route_completeness(normalized_route_authority),
    )
    dispatch = NativeOperatorCockpitDispatchReadModel(
        resolution=dispatch_resolution,
        completeness=_dispatch_completeness(dispatch_resolution),
    )
    completeness_refs = list(route.completeness.missing_evidence_refs)
    completeness_refs.extend(dispatch.completeness.missing_evidence_refs)
    completeness_refs.extend(cutover_status.completeness.missing_evidence_refs)
    completeness_refs.extend(snapshot_refs)
    completeness_tuple = _dedupe(completeness_refs)

    return NativeOperatorCockpitReadModel(
        provenance=NativeOperatorCockpitProvenance(
            as_of=normalized_as_of,
            failover_authority=(
                None
                if failover is None
                else "registry.endpoint_failover.load_provider_failover_and_endpoint_authority"
            ),
            stitched_sections=(
                ("route", "dispatch", "cutover")
                if failover is None
                else ("route", "failover", "dispatch", "cutover")
            ),
        ),
        run_id=normalized_run_id,
        request_id=cutover_status.request_id,
        as_of=normalized_as_of,
        watermark=cutover_status.watermark,
        route=route,
        failover=failover,
        dispatch=dispatch,
        cutover_status=cutover_status,
        completeness=ProjectionCompleteness(
            is_complete=not completeness_tuple,
            missing_evidence_refs=completeness_tuple,
        ),
        status_state=_cockpit_state(
            route=route.completeness,
            failover=None,
            dispatch=dispatch.completeness,
            cutover=cutover_status,
            snapshot_is_coherent=not snapshot_refs,
        ),
        status_reason=_cockpit_reason(
            route=route.completeness,
            failover=None,
            dispatch=dispatch.completeness,
            cutover=cutover_status,
            snapshot_missing_refs=snapshot_refs,
        ),
    )


def operator_cockpit_run(
    *,
    run_id: str,
    as_of: datetime,
    route_authority: ProviderRouteAuthority,
    failover_authority: ProviderFailoverAndEndpointAuthority | None = None,
    dispatch_resolution: WorkflowClassResolutionDecision,
    cutover_status: NativeCutoverGraphStatusReadModel,
) -> NativeOperatorCockpitReadModel:
    normalized_as_of = _normalize_as_of(as_of)
    failover = (
        None
        if failover_authority is None
        else _build_failover_read_model(failover_authority, as_of=normalized_as_of)
    )
    return _operator_cockpit_read_model(
        run_id=run_id,
        as_of=normalized_as_of,
        route_authority=route_authority,
        failover=failover,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
    )


async def operator_cockpit_run_with_failover_contract(
    *,
    conn: asyncpg.Connection,
    run_id: str,
    as_of: datetime,
    route_authority: ProviderRouteAuthority,
    failover_contract: NativeOperatorCockpitFailoverSelectorContract | None,
    dispatch_resolution: WorkflowClassResolutionDecision,
    cutover_status: NativeCutoverGraphStatusReadModel,
) -> NativeOperatorCockpitReadModel:
    normalized_as_of = _normalize_as_of(as_of)
    failover = await _load_bounded_failover_read_model(
        conn=conn,
        as_of=normalized_as_of,
        failover_contract=failover_contract,
    )
    return _operator_cockpit_read_model(
        run_id=run_id,
        as_of=normalized_as_of,
        route_authority=route_authority,
        failover=failover,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
    )


def render_operator_cockpit(view: NativeOperatorCockpitReadModel) -> str:
    """Render the cockpit as a machine-readable line-oriented surface."""

    lines = [
        "kind: operator_cockpit",
        f"run_id: {view.run_id}",
        f"request_id: {_format_optional_text(view.request_id)}",
        f"as_of: {view.as_of.isoformat()}",
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
            f"status.state: {view.status_state}",
            f"status.reason: {_format_optional_text(view.status_reason)}",
        ]
    )

    _render_route_section(lines, route=view.route)
    if view.failover is not None:
        _render_failover_section(lines, failover=view.failover)
    _render_dispatch_section(lines, dispatch=view.dispatch)

    for line in render_cutover_graph_status(view.cutover_status).splitlines():
        lines.append(f"cutover.{line}")

    return "\n".join(lines)
