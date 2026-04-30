from __future__ import annotations

from types import SimpleNamespace

from runtime.object_truth.mdm import (
    build_cluster_member,
    build_field_value_candidate,
    build_identity_cluster,
    build_match_signal,
    build_source_authority_evidence,
    compare_field_candidates,
)
from runtime.operations.commands import object_truth_mdm as commands
from runtime.operations.queries import object_truth_mdm as queries


AS_OF = "2026-04-30T16:00:00Z"


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _mdm_evidence() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    salesforce = build_cluster_member(
        entity_type="organization",
        source_system="salesforce",
        source_record_id="sf-001",
        source_object_ref="account",
        source_record={"tax_id": "12-3456789", "legal_name": "ACME, Inc."},
    )
    hubspot = build_cluster_member(
        entity_type="organization",
        source_system="hubspot",
        source_record_id="hs-001",
        source_object_ref="company",
        source_record={"tax_id": "123456789", "legal_name": "Acme Incorporated"},
    )
    signal = build_match_signal(
        signal_class="exact_identifier",
        left_member_ref=str(salesforce["source_record_ref"]),
        right_member_ref=str(hubspot["source_record_ref"]),
        field_name="tax_id",
        evidence_value="123456789",
    )
    cluster = build_identity_cluster(
        entity_type="organization",
        member_records=[salesforce, hubspot],
        match_signals=[signal],
        created_at=AS_OF,
        updated_at=AS_OF,
    )
    authority = build_source_authority_evidence(
        entity_type="organization",
        field_name="legal_name",
        source_system="hubspot",
        authority_rank=1,
        authority_scope={"business_domain": "customer_master"},
        authority_reason="Approved source of record",
        evidence_type="system_stewardship_assignment",
        evidence_reference="policy.source_authority.hubspot.legal_name",
        approved_by="operator:nate",
        approved_at="2026-04-01T00:00:00Z",
        review_interval_days=90,
    )
    candidate = build_field_value_candidate(
        entity_type="organization",
        field_name="legal_name",
        source_system="hubspot",
        source_record_id="hs-001",
        source_value_raw="Acme Incorporated",
        observed_at=AS_OF,
        loaded_at=AS_OF,
        as_of=AS_OF,
        field_volatility="stable",
    )
    comparison = compare_field_candidates(
        entity_type="organization",
        canonical_record_id="canonical.organization.acme",
        canonical_field="legal_name",
        candidates=[candidate],
        authority_evidence=[authority],
        as_of=AS_OF,
        cluster_id=str(cluster["cluster_id"]),
    )
    return cluster, comparison, authority


def test_mdm_resolution_record_builds_packet_persists_and_emits_event(monkeypatch) -> None:
    cluster, comparison, authority = _mdm_evidence()
    persist_calls: list[dict[str, object]] = []

    def _persist(conn, *, packet, observed_by_ref=None, source_ref=None):
        persist_calls.append(
            {
                "packet": packet,
                "observed_by_ref": observed_by_ref,
                "source_ref": source_ref,
            }
        )
        return {
            "packet": {"packet_ref": packet["packet_ref"]},
            "identity_cluster_count": len(packet["identity_clusters"]),
            "field_comparison_count": len(packet["field_comparisons"]),
            "authority_evidence_count": len(packet["authority_evidence"]),
            "typed_gap_count": len(packet["typed_gaps"]),
        }

    monkeypatch.setattr(commands, "persist_mdm_resolution_packet", _persist)

    result = commands.handle_object_truth_mdm_resolution_record(
        commands.RecordObjectTruthMdmResolutionCommand(
            client_ref="client.acme",
            entity_type="organization",
            as_of=AS_OF,
            identity_clusters=[cluster],
            field_comparisons=[comparison],
            authority_evidence=[authority],
            observed_by_ref="operator:nate",
            source_ref="phase_03_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "object_truth_mdm_resolution_record"
    assert result["packet_ref"].startswith("object_truth_mdm_packet.")
    assert result["event_payload"]["identity_cluster_count"] == 1
    assert result["event_payload"]["field_comparison_count"] == 1
    assert persist_calls[0]["observed_by_ref"] == "operator:nate"
    assert persist_calls[0]["packet"]["resolution_packet_digest"] == result["resolution_packet_digest"]


def test_mdm_resolution_record_requires_packet_evidence() -> None:
    try:
        commands.RecordObjectTruthMdmResolutionCommand(
            client_ref="client.acme",
            entity_type="organization",
            as_of=AS_OF,
            identity_clusters=[],
            field_comparisons=[],
        )
    except ValueError as exc:
        assert "identity_clusters are required" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("MDM resolution record should require evidence")


def test_mdm_resolution_read_lists_and_describes(monkeypatch) -> None:
    monkeypatch.setattr(
        queries,
        "list_mdm_resolution_packets",
        lambda conn, client_ref=None, entity_type=None, limit=50: [
            {"packet_ref": "packet.1", "client_ref": client_ref, "entity_type": entity_type}
        ],
    )
    monkeypatch.setattr(
        queries,
        "load_mdm_resolution_packet",
        lambda conn, packet_ref, include_records=True: {
            "packet_ref": packet_ref,
            "identity_clusters": [{}] if include_records else [],
        },
    )

    listed = queries.handle_object_truth_mdm_resolution_read(
        queries.QueryObjectTruthMdmResolutionRead(
            action="list",
            client_ref="client.acme",
            entity_type="organization",
        ),
        _subsystems(),
    )
    described = queries.handle_object_truth_mdm_resolution_read(
        queries.QueryObjectTruthMdmResolutionRead(
            action="describe",
            packet_ref="packet.1",
        ),
        _subsystems(),
    )

    assert listed["count"] == 1
    assert listed["items"][0]["client_ref"] == "client.acme"
    assert described["ok"] is True
    assert described["packet"]["packet_ref"] == "packet.1"
