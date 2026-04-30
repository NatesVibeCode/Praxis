from __future__ import annotations

from runtime.object_truth.mdm import (
    build_anti_match_signal,
    build_cluster_member,
    build_field_value_candidate,
    build_identity_cluster,
    build_match_signal,
    build_mdm_resolution_packet,
    build_normalization_rule_record,
    build_reversible_source_link,
    build_source_authority_evidence,
    compare_field_candidates,
    normalize_field_value,
)


AS_OF = "2026-04-30T12:00:00Z"


def _organization_members() -> tuple[dict[str, object], dict[str, object]]:
    salesforce = build_cluster_member(
        entity_type="organization",
        source_system="salesforce",
        source_record_id="001",
        source_object_ref="account",
        source_record={
            "tax_id": "12-3456789",
            "legal_name": "ACME, Inc.",
        },
    )
    netsuite = build_cluster_member(
        entity_type="organization",
        source_system="netsuite",
        source_record_id="CUST-77",
        source_object_ref="customer",
        source_record={
            "tax_id": "123456789",
            "legal_name": "Acme Incorporated",
        },
    )
    return salesforce, netsuite


def _authority(
    *,
    entity_type: str = "account",
    field_name: str = "status",
    source_system: str = "erp",
    rank: int = 1,
) -> dict[str, object]:
    return build_source_authority_evidence(
        entity_type=entity_type,
        field_name=field_name,
        source_system=source_system,
        authority_rank=rank,
        authority_scope={"business_domain": "customer_master", "environment": "prod"},
        authority_reason="Approved field source of record",
        evidence_type="system_stewardship_assignment",
        evidence_reference=f"policy.source_authority.{source_system}.{field_name}",
        approved_by="operator:nate",
        approved_at="2026-04-01T00:00:00Z",
        review_interval_days=90,
    )


def test_identity_cluster_is_order_stable_and_auto_accepts_strong_evidence() -> None:
    salesforce, netsuite = _organization_members()
    left_ref = str(salesforce["source_record_ref"])
    right_ref = str(netsuite["source_record_ref"])
    signals = [
        build_match_signal(
            signal_class="exact_identifier",
            left_member_ref=left_ref,
            right_member_ref=right_ref,
            field_name="tax_id",
            evidence_value="123456789",
        ),
        build_match_signal(
            signal_class="strong_quasi_identifier",
            left_member_ref=left_ref,
            right_member_ref=right_ref,
            field_name="legal_name",
            evidence_value="acme",
        ),
        build_match_signal(
            signal_class="source_provenance_confidence",
            left_member_ref=left_ref,
            right_member_ref=right_ref,
            field_name="source_system",
            evidence_value={"salesforce": "crm", "netsuite": "erp"},
            confidence=0.8,
        ),
    ]

    cluster = build_identity_cluster(
        entity_type="organization",
        member_records=[salesforce, netsuite],
        match_signals=signals,
        created_at=AS_OF,
        updated_at=AS_OF,
    )
    reordered = build_identity_cluster(
        entity_type="organization",
        member_records=[netsuite, salesforce],
        match_signals=list(reversed(signals)),
        created_at=AS_OF,
        updated_at=AS_OF,
    )

    assert cluster["cluster_id"] == reordered["cluster_id"]
    assert cluster["identity_cluster_digest"] == reordered["identity_cluster_digest"]
    assert cluster["review_status"] == "auto-accepted"
    assert cluster["cluster_confidence"] >= 0.82


def test_blocking_anti_match_forces_split_required() -> None:
    salesforce, netsuite = _organization_members()
    left_ref = str(salesforce["source_record_ref"])
    right_ref = str(netsuite["source_record_ref"])
    anti_match = build_anti_match_signal(
        signal_class="mutually_exclusive_official_identifier",
        left_member_ref=left_ref,
        right_member_ref=right_ref,
        field_name="tax_id",
        evidence_value={"left": "12-3456789", "right": "98-7654321"},
        reason="Two active official tax IDs cannot identify one legal organization",
    )

    cluster = build_identity_cluster(
        entity_type="organization",
        member_records=[salesforce, netsuite],
        match_signals=[
            build_match_signal(
                signal_class="weak_descriptive_similarity",
                left_member_ref=left_ref,
                right_member_ref=right_ref,
                field_name="legal_name",
                evidence_value="acme",
            )
        ],
        anti_match_signals=[anti_match],
        created_at=AS_OF,
        updated_at=AS_OF,
    )

    assert cluster["review_status"] == "split-required"
    assert cluster["anti_match_signals"][0]["blocking"] is True


def test_normalization_rule_and_source_link_preserve_raw_values() -> None:
    rule = build_normalization_rule_record(
        entity_type="organization",
        field_name="legal_name",
        input_pattern="text",
        normalization_steps=[
            "strip",
            "collapse_whitespace",
            "remove_punctuation",
            "casefold",
            "remove_legal_suffix",
        ],
        output_type="text",
        reversible=True,
        loss_risk="lossy-source-preserved",
        exception_policy="preserve raw and emit non-normalizable gap",
        test_examples=[{"input": "ACME, Incorporated.", "output": "acme"}],
        locale_assumptions=["US English legal suffixes"],
    )
    normalized = normalize_field_value(
        entity_type="organization",
        field_name="legal_name",
        source_value_raw="  ACME, Incorporated.  ",
        rule=rule,
    )

    assert normalized["source_value_raw"] == "  ACME, Incorporated.  "
    assert normalized["source_value_normalized"] == "acme"
    assert normalized["loss_risk"] == "lossy-source-preserved"

    link = build_reversible_source_link(
        canonical_record_id="canonical.organization.acme",
        canonical_field="legal_name",
        source_system="salesforce",
        source_record_id="001",
        source_field="Name",
        source_value_raw=normalized["source_value_raw"],
        source_value_normalized=normalized["source_value_normalized"],
        transform_chain=normalized["transform_chain"],
        selection_reason="top_field_authority",
        authority_basis={"authority_rank": 1, "authority_evidence_digest": "authority.digest"},
        observed_at="2026-04-30T08:00:00-04:00",
        loaded_at=AS_OF,
    )

    assert link["source_value_raw"] == "  ACME, Incorporated.  "
    assert link["source_value_normalized"] == "acme"
    assert link["observed_at"] == AS_OF
    assert link["source_link_digest"]


def test_field_comparison_selects_authoritative_stale_value_and_emits_tradeoff_gap() -> None:
    erp_candidate = build_field_value_candidate(
        entity_type="account",
        field_name="status",
        source_system="erp",
        source_record_id="A-100",
        source_field="account_status",
        source_value_raw="Inactive",
        observed_at="2024-01-01T00:00:00Z",
        loaded_at="2024-01-01T00:05:00Z",
        as_of=AS_OF,
        source_update_cadence_hours=24,
        field_volatility="high-change",
    )
    crm_candidate = build_field_value_candidate(
        entity_type="account",
        field_name="status",
        source_system="crm",
        source_record_id="001",
        source_field="Status",
        source_value_raw="Active",
        observed_at="2026-04-30T11:00:00Z",
        loaded_at="2026-04-30T11:05:00Z",
        as_of=AS_OF,
        source_update_cadence_hours=1,
        field_volatility="high-change",
    )

    comparison = compare_field_candidates(
        entity_type="account",
        canonical_record_id="canonical.account.acme",
        canonical_field="status",
        candidates=[crm_candidate, erp_candidate],
        authority_evidence=[
            _authority(source_system="crm", rank=2),
            _authority(source_system="erp", rank=1),
        ],
        as_of=AS_OF,
    )

    assert comparison["selection_state"] == "selected"
    assert comparison["selected_canonical_value"] == "inactive"
    assert comparison["selected_source_link"]["source_system"] == "erp"
    assert comparison["conflict_flag"] is True
    assert comparison["consensus_flag"] is False
    assert {gap["gap_type"] for gap in comparison["typed_gaps"]} == {"stale-value"}
    assert comparison["rejected_candidate_values"][0]["rejection_reason"] == "lower_field_authority"


def test_field_without_authority_policy_stays_unresolved_even_with_consensus() -> None:
    salesforce_phone = build_field_value_candidate(
        entity_type="person",
        field_name="phone",
        source_system="salesforce",
        source_record_id="003",
        source_value_raw="(303) 555-0100",
        observed_at="2026-04-30T10:00:00Z",
        loaded_at="2026-04-30T10:01:00Z",
        as_of=AS_OF,
        field_volatility="high-change",
    )
    hubspot_phone = build_field_value_candidate(
        entity_type="person",
        field_name="phone",
        source_system="hubspot",
        source_record_id="contact-9",
        source_value_raw="3035550100",
        observed_at="2026-04-30T10:00:00Z",
        loaded_at="2026-04-30T10:01:00Z",
        as_of=AS_OF,
        field_volatility="high-change",
    )

    comparison = compare_field_candidates(
        entity_type="person",
        canonical_record_id="canonical.person.alex",
        canonical_field="phone",
        candidates=[salesforce_phone, hubspot_phone],
        authority_evidence=[],
        as_of=AS_OF,
    )

    assert comparison["consensus_flag"] is True
    assert comparison["selection_state"] == "unresolved"
    assert comparison["selected_canonical_value"] is None
    assert comparison["typed_gaps"][0]["gap_type"] == "policy-missing"


def test_resolution_packet_digest_is_stable_across_input_order() -> None:
    salesforce, netsuite = _organization_members()
    signal = build_match_signal(
        signal_class="exact_identifier",
        left_member_ref=str(salesforce["source_record_ref"]),
        right_member_ref=str(netsuite["source_record_ref"]),
        field_name="tax_id",
        evidence_value="123456789",
    )
    cluster = build_identity_cluster(
        entity_type="organization",
        member_records=[salesforce, netsuite],
        match_signals=[signal],
        created_at=AS_OF,
        updated_at=AS_OF,
    )
    authority = _authority(entity_type="organization", field_name="legal_name", source_system="netsuite")
    candidate = build_field_value_candidate(
        entity_type="organization",
        field_name="legal_name",
        source_system="netsuite",
        source_record_id="CUST-77",
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
    )
    rule = build_normalization_rule_record(
        entity_type="organization",
        field_name="legal_name",
        input_pattern="text",
        normalization_steps=["strip", "collapse_whitespace", "remove_punctuation", "casefold", "remove_legal_suffix"],
        output_type="text",
        reversible=True,
        loss_risk="lossy-source-preserved",
        exception_policy="preserve raw",
        test_examples=[{"input": "Acme Inc.", "output": "acme"}],
    )

    packet = build_mdm_resolution_packet(
        client_ref="client.acme",
        entity_type="organization",
        as_of=AS_OF,
        identity_clusters=[cluster],
        field_comparisons=[comparison],
        normalization_rules=[rule],
        authority_evidence=[authority],
        typed_gaps=comparison["typed_gaps"],
    )
    same_packet = build_mdm_resolution_packet(
        client_ref="client.acme",
        entity_type="organization",
        as_of=AS_OF,
        normalization_rules=[rule],
        field_comparisons=[comparison],
        identity_clusters=[cluster],
        typed_gaps=list(reversed(comparison["typed_gaps"])),
        authority_evidence=[authority],
    )

    assert packet["packet_ref"] == same_packet["packet_ref"]
    assert packet["resolution_packet_digest"] == same_packet["resolution_packet_digest"]
