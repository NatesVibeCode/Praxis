"""CQRS commands for Object Truth MDM/source-authority evidence."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.object_truth.mdm import build_mdm_resolution_packet
from storage.postgres.object_truth_repository import persist_mdm_resolution_packet


EntityType = Literal["person", "organization", "account", "location", "asset"]


class RecordObjectTruthMdmResolutionCommand(BaseModel):
    """Record one receipt-backed MDM resolution packet."""

    client_ref: str
    entity_type: EntityType
    as_of: str
    identity_clusters: list[dict[str, Any]] = Field(default_factory=list)
    field_comparisons: list[dict[str, Any]] = Field(default_factory=list)
    normalization_rules: list[dict[str, Any]] = Field(default_factory=list)
    authority_evidence: list[dict[str, Any]] = Field(default_factory=list)
    hierarchy_signals: list[dict[str, Any]] = Field(default_factory=list)
    typed_gaps: list[dict[str, Any]] = Field(default_factory=list)
    packet_ref: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("client_ref", "as_of", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("client_ref and as_of must be non-empty strings")
        return value.strip()

    @field_validator("packet_ref", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator(
        "identity_clusters",
        "field_comparisons",
        "normalization_rules",
        "authority_evidence",
        "hierarchy_signals",
        "typed_gaps",
        mode="before",
    )
    @classmethod
    def _normalize_record_list(cls, value: object) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("MDM record groups must be lists of JSON objects")
        return [dict(item) for item in value]

    @model_validator(mode="after")
    def _validate_packet_contents(self) -> "RecordObjectTruthMdmResolutionCommand":
        if not self.identity_clusters:
            raise ValueError("identity_clusters are required")
        if not self.field_comparisons:
            raise ValueError("field_comparisons are required")
        return self


def handle_object_truth_mdm_resolution_record(
    command: RecordObjectTruthMdmResolutionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    typed_gaps = _merge_typed_gaps(command.typed_gaps, command.field_comparisons)
    packet = build_mdm_resolution_packet(
        client_ref=command.client_ref,
        entity_type=command.entity_type,
        as_of=command.as_of,
        identity_clusters=command.identity_clusters,
        field_comparisons=command.field_comparisons,
        normalization_rules=command.normalization_rules,
        authority_evidence=command.authority_evidence,
        hierarchy_signals=command.hierarchy_signals,
        typed_gaps=typed_gaps,
        packet_ref=command.packet_ref,
    )
    persisted = persist_mdm_resolution_packet(
        conn,
        packet=packet,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = {
        "packet_ref": packet["packet_ref"],
        "resolution_packet_digest": packet["resolution_packet_digest"],
        "client_ref": packet["client_ref"],
        "entity_type": packet["entity_type"],
        "identity_cluster_count": len(packet["identity_clusters"]),
        "field_comparison_count": len(packet["field_comparisons"]),
        "authority_evidence_count": len(packet["authority_evidence"]),
        "typed_gap_count": len(packet["typed_gaps"]),
    }
    return {
        "ok": True,
        "operation": "object_truth_mdm_resolution_record",
        "packet": packet,
        "packet_ref": packet["packet_ref"],
        "resolution_packet_digest": packet["resolution_packet_digest"],
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _merge_typed_gaps(
    explicit_gaps: list[dict[str, Any]],
    field_comparisons: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for gap in explicit_gaps:
        key = str(gap.get("gap_digest") or gap.get("gap_id") or gap)
        if key not in seen:
            seen.add(key)
            merged.append(dict(gap))
    for comparison in field_comparisons:
        for gap in comparison.get("typed_gaps") or []:
            if not isinstance(gap, dict):
                continue
            key = str(gap.get("gap_digest") or gap.get("gap_id") or gap)
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(gap))
    return merged


__all__ = [
    "RecordObjectTruthMdmResolutionCommand",
    "handle_object_truth_mdm_resolution_record",
]
