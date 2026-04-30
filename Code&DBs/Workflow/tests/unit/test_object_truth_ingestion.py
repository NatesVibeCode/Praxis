from __future__ import annotations

import pytest

from core.object_truth_ops import ObjectTruthOperationError
from runtime.object_truth.ingestion import (
    build_ingestion_replay_fixture,
    build_raw_payload_reference,
    build_readiness_inputs,
    build_redacted_preview,
    build_sample_capture_record,
    build_source_query_evidence,
    build_system_snapshot_record,
    normalize_ingestion_source_metadata,
)


def _snapshot() -> dict[str, object]:
    return build_system_snapshot_record(
        client_ref="client.acme",
        system_ref="salesforce",
        integration_id="integration.salesforce.prod",
        connector_ref="connector.salesforce",
        environment_ref="prod",
        auth_context={"tenant": "acme", "scopes": ["read:account", "read:contact"]},
        captured_at="2026-04-30T12:00:00-04:00",
        capture_receipt_id="receipt.capture.1",
        schema_snapshot_count=1,
        sample_count=1,
        metadata={"region": "us-east"},
    )


def _source_evidence() -> dict[str, object]:
    return build_source_query_evidence(
        system_ref="salesforce",
        object_ref="account",
        source_query={
            "fields": ["id", "name", "updated_at"],
            "where": {"is_deleted": False},
        },
        cursor_value={"page": 2, "offset": 200},
        window_kind="source_updated_at",
        window_start="2026-04-29T00:00:00Z",
        window_end="2026-04-30T00:00:00Z",
        limit=100,
    )


def test_system_snapshot_source_query_and_sample_capture_are_deterministic() -> None:
    snapshot = _snapshot()
    same_snapshot = build_system_snapshot_record(
        client_ref="client.acme",
        system_ref="salesforce",
        integration_id="integration.salesforce.prod",
        connector_ref="connector.salesforce",
        environment_ref="prod",
        auth_context={"scopes": ["read:account", "read:contact"], "tenant": "acme"},
        captured_at="2026-04-30T16:00:00Z",
        capture_receipt_id="receipt.capture.1",
        schema_snapshot_count=1,
        sample_count=1,
        metadata={"region": "us-east"},
    )

    assert snapshot["system_snapshot_id"] == same_snapshot["system_snapshot_id"]
    assert snapshot["captured_at"] == "2026-04-30T16:00:00Z"
    assert "scopes" not in snapshot

    source_evidence = _source_evidence()
    same_source_evidence = build_source_query_evidence(
        system_ref="salesforce",
        object_ref="account",
        source_query={
            "where": {"is_deleted": False},
            "fields": ["id", "name", "updated_at"],
        },
        cursor_value={"offset": 200, "page": 2},
        window_kind="source_updated_at",
        window_start="2026-04-29T00:00:00Z",
        window_end="2026-04-30T00:00:00Z",
        limit=100,
    )

    assert source_evidence["source_evidence_digest"] == same_source_evidence["source_evidence_digest"]
    assert source_evidence["cursor_ref"] == same_source_evidence["cursor_ref"]
    assert "cursor_value" not in source_evidence

    sample = build_sample_capture_record(
        system_snapshot_id=str(snapshot["system_snapshot_id"]),
        schema_snapshot_digest="schema.digest.account",
        system_ref="salesforce",
        object_ref="account",
        sample_strategy="recent",
        source_evidence=source_evidence,
        sample_size_requested=10,
        sample_payloads=[
            {"id": "001", "name": "Acme"},
            {"name": "Beta", "id": "002"},
        ],
        receipt_id="receipt.sample.1",
    )
    same_sample = build_sample_capture_record(
        system_snapshot_id=str(snapshot["system_snapshot_id"]),
        schema_snapshot_digest="schema.digest.account",
        system_ref="salesforce",
        object_ref="account",
        sample_strategy="recent",
        source_evidence=source_evidence,
        sample_size_requested=10,
        sample_payloads=[
            {"name": "Acme", "id": "001"},
            {"id": "002", "name": "Beta"},
        ],
        receipt_id="receipt.sample.1",
    )

    assert sample["sample_id"] == same_sample["sample_id"]
    assert sample["sample_size_returned"] == 2
    assert sample["status"] == "captured"
    assert "sample_payloads" not in sample


def test_redacted_preview_hides_sensitive_values_while_preserving_shape() -> None:
    preview = build_redacted_preview(
        {
            "name": "Acme",
            "email": "owner@example.com",
            "api_token": "secret-token",
            "notes": "private renewal terms",
            "billing": {"city": "Denver", "tax_id": "12-3456789"},
            "contacts": [{"email": "a@example.com", "role": "admin"}],
        },
        policy={
            "public_fields": ["name", "billing.city"],
            "restricted_fields": ["billing.tax_id"],
        },
    )

    preview_json = preview["preview_json"]
    assert preview_json["name"] == "Acme"
    assert preview_json["billing"]["city"] == "Denver"
    assert preview_json["email"]["redacted"] is True
    assert preview_json["api_token"]["classification"] == "restricted"
    assert preview_json["notes"]["classification"] == "confidential"
    assert preview_json["billing"]["tax_id"]["classification"] == "restricted"
    assert preview_json["contacts"][0]["email"]["redacted"] is True
    assert preview_json["contacts"][0]["role"] == "admin"
    assert preview["redaction_count"] == 5

    by_path = {item["field_path"]: item for item in preview["field_classifications"]}
    assert by_path["billing.tax_id"]["classification"] == "restricted"
    assert by_path["contacts[0].email"]["classification"] == "confidential"


def test_raw_payload_reference_omits_content_unless_policy_approves_inline_storage() -> None:
    raw_payload = {"id": "001", "email": "owner@example.com"}

    reference = build_raw_payload_reference(
        raw_payload=raw_payload,
        raw_payload_ref="vault://object-truth/raw/account/001",
        privacy_classification="confidential",
        retention_policy_ref="retention.object_truth.redacted_hashes",
    )

    assert reference["raw_payload_ref"] == "vault://object-truth/raw/account/001"
    assert reference["raw_payload_hash"]
    assert reference["normalized_payload_hash"]
    assert reference["inline_payload_stored"] is False
    assert "raw_payload_json" not in reference

    with pytest.raises(ObjectTruthOperationError) as excinfo:
        build_raw_payload_reference(
            raw_payload=raw_payload,
            privacy_classification="confidential",
            inline_payload_approved=True,
        )

    assert excinfo.value.reason_code == "object_truth.raw_payload_policy_missing"

    approved = build_raw_payload_reference(
        raw_payload=raw_payload,
        privacy_classification="confidential",
        privacy_policy_ref="privacy.object_truth.raw_payload.approved",
        retention_policy_ref="retention.object_truth.short_lived",
        inline_payload_approved=True,
    )
    assert approved["inline_payload_stored"] is True
    assert approved["raw_payload_json"] == raw_payload


def test_source_metadata_normalization_maps_aliases_and_unknowns() -> None:
    raw_reference = build_raw_payload_reference(
        raw_payload={"id": "001", "email": "owner@example.com"},
        raw_payload_ref="vault://object-truth/raw/account/001",
        privacy_classification="confidential",
        retention_policy_ref="retention.object_truth.redacted_hashes",
    )
    preview = build_redacted_preview({"email": "owner@example.com"})

    metadata = normalize_ingestion_source_metadata(
        {
            "id": "001",
            "createdAt": "2026-04-29T10:00:00-05:00",
            "updated_at": "unknown",
            "updated_by": "user:17",
            "etag": "v1",
            "extra": {"b": 2, "a": 1},
        },
        raw_payload_reference=raw_reference,
        redacted_preview=preview,
    )
    same_metadata = normalize_ingestion_source_metadata(
        {
            "extra": {"a": 1, "b": 2},
            "etag": "v1",
            "updated_by": "user:17",
            "updated_at": "unknown",
            "createdAt": "2026-04-29T15:00:00Z",
            "id": "001",
        },
        raw_payload_reference=raw_reference,
        redacted_preview=preview,
    )

    assert metadata["external_record_id"] == "001"
    assert metadata["source_created_at"] == "2026-04-29T15:00:00Z"
    assert metadata["source_updated_at"] is None
    assert metadata["source_actor_ref"] == "user:17"
    assert metadata["source_version_ref"] == "v1"
    assert metadata["privacy_classification"] == "confidential"
    assert metadata["metadata_json"] == {"extra": {"a": 1, "b": 2}}
    assert metadata["source_metadata_digest"] == same_metadata["source_metadata_digest"]

    numeric_id_metadata = normalize_ingestion_source_metadata({"id": 0})
    assert numeric_id_metadata["external_record_id"] == "0"


def test_replay_fixture_and_readiness_inputs_are_order_stable() -> None:
    snapshot = _snapshot()
    source_evidence = _source_evidence()
    sample_one = build_sample_capture_record(
        system_snapshot_id=str(snapshot["system_snapshot_id"]),
        schema_snapshot_digest="schema.digest.account",
        system_ref="salesforce",
        object_ref="account",
        sample_strategy="fixture",
        source_evidence=source_evidence,
        sample_size_requested=1,
        sample_payloads=[{"id": "001", "name": "Acme"}],
    )
    sample_two = build_sample_capture_record(
        system_snapshot_id=str(snapshot["system_snapshot_id"]),
        schema_snapshot_digest="schema.digest.account",
        system_ref="salesforce",
        object_ref="account",
        sample_strategy="recent",
        source_evidence=source_evidence,
        sample_size_requested=1,
        sample_payloads=[{"id": "002", "name": "Beta"}],
    )

    fixture = build_ingestion_replay_fixture(
        system_snapshot=snapshot,
        samples=[sample_two, sample_one],
    )
    same_fixture = build_ingestion_replay_fixture(
        system_snapshot=snapshot,
        samples=[sample_one, sample_two],
    )

    assert fixture["fixture_digest"] == same_fixture["fixture_digest"]
    assert fixture["sample_strategies"] == ["fixture", "recent"]
    assert fixture["object_refs"] == ["account"]

    readiness = build_readiness_inputs(
        system_snapshots=[snapshot],
        sample_records=[sample_two, sample_one],
        required_connector_refs=["connector.manual"],
        required_source_refs=["source.fixture"],
    )

    assert readiness["operation_name"] == "object_truth_readiness"
    assert readiness["tool_ref"] == "praxis_object_truth_readiness"
    assert readiness["payload"] == {
        "client_payload_mode": "redacted_hashes",
        "planned_fanout": 2,
        "include_counts": True,
    }
    assert readiness["fail_closed_states"] == ["blocked", "unknown", "revoked"]
    assert readiness["ingestion_requirements"]["connector_refs"] == [
        "connector.manual",
        "connector.salesforce",
    ]
    assert readiness["ingestion_requirements"]["sample_count"] == 2
