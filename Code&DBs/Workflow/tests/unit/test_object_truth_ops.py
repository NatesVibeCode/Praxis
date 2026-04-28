from __future__ import annotations

import pytest

from core.object_truth_ops import (
    ObjectTruthOperationError,
    build_identity,
    build_object_version,
    build_task_environment_contract,
    compare_object_versions,
    extract_field_observations,
    normalize_schema_snapshot,
)


def test_schema_snapshot_digest_is_stable_across_field_order() -> None:
    left = normalize_schema_snapshot(
        [
            {"field_path": "email", "field_kind": "text", "required": True},
            {"field_path": "name", "field_kind": "text"},
        ],
        system_ref="salesforce",
        object_ref="contact",
    )
    right = normalize_schema_snapshot(
        [
            {"field_path": "name", "field_kind": "text"},
            {"field_path": "email", "field_kind": "text", "required": True},
        ],
        system_ref="salesforce",
        object_ref="contact",
    )

    assert left["fields"] == right["fields"]
    assert left["schema_digest"] == right["schema_digest"]


def test_field_observations_capture_nested_paths_and_redact_secrets() -> None:
    observations = extract_field_observations(
        {
            "id": "001",
            "billing": {"city": "Denver"},
            "api_token": "super-secret",
            "contacts": [{"email": "a@example.com"}],
        }
    )

    by_path = {item["field_path"]: item for item in observations}
    assert by_path["billing"]["field_kind"] == "object"
    assert by_path["billing.city"]["redacted_value_preview"] == "Denver"
    assert by_path["api_token"]["sensitive"] is True
    assert by_path["api_token"]["redacted_value_preview"] == "[REDACTED]"
    assert by_path["contacts"]["cardinality_kind"] == "many"


def test_identity_digest_is_stable_and_requires_all_fields() -> None:
    left = build_identity({"id": "001", "email": "a@example.com"}, ["id", "email"])
    right = build_identity({"email": "a@example.com", "id": "001"}, ["id", "email"])

    assert left["identity_digest"] == right["identity_digest"]

    with pytest.raises(ObjectTruthOperationError) as excinfo:
        build_identity({"id": "001"}, ["id", "email"])

    assert excinfo.value.reason_code == "object_truth.identity_missing_fields"
    assert excinfo.value.details["missing_fields"] == ["email"]


def test_object_version_comparison_reports_deltas_and_freshness_without_deciding_truth() -> None:
    left = build_object_version(
        system_ref="salesforce",
        object_ref="account",
        record={"id": "001", "name": "Acme", "billing": {"city": "Denver"}},
        identity_fields=["id"],
        source_metadata={"updated_at": "2026-04-28T10:00:00Z"},
    )
    right = build_object_version(
        system_ref="hubspot",
        object_ref="company",
        record={"id": "001", "name": "Acme Corp", "billing": {"city": "Denver"}, "tier": "enterprise"},
        identity_fields=["id"],
        source_metadata={"updated_at": "2026-04-27T10:00:00Z"},
    )

    comparison = compare_object_versions(left, right)

    assert comparison["freshness"]["state"] == "left_newer"
    assert comparison["summary"] == {
        "matching_fields": 3,
        "different_fields": 1,
        "missing_left_fields": 1,
        "missing_right_fields": 0,
    }
    by_path = {item["field_path"]: item for item in comparison["field_comparisons"]}
    assert by_path["name"]["status"] == "different"
    assert by_path["tier"]["status"] == "missing_left"


def test_task_environment_contract_digest_is_append_only_and_order_stable() -> None:
    left = build_task_environment_contract(
        task_type="object_truth.compare_samples",
        authority_inputs={"object_ref": "account", "sample_size": 1000},
        allowed_model_routes=["openai/gpt-5.4", "openai/gpt-5.4-mini"],
        tool_refs=["object_truth.compare", "object_truth.observe"],
        object_version_refs=["object_version:1", "object_version:2"],
        sop_refs=["sop:account-cleanup"],
        policy_refs=["policy:object-truth-deterministic-substrate"],
    )
    right = build_task_environment_contract(
        task_type="object_truth.compare_samples",
        authority_inputs={"sample_size": 1000, "object_ref": "account"},
        allowed_model_routes=["openai/gpt-5.4", "openai/gpt-5.4-mini"],
        tool_refs=["object_truth.compare", "object_truth.observe"],
        object_version_refs=["object_version:1", "object_version:2"],
        sop_refs=["sop:account-cleanup"],
        policy_refs=["policy:object-truth-deterministic-substrate"],
    )
    appended = build_task_environment_contract(
        task_type="object_truth.compare_samples",
        authority_inputs={"sample_size": 1000, "object_ref": "account"},
        allowed_model_routes=["openai/gpt-5.4", "openai/gpt-5.4-mini"],
        tool_refs=["object_truth.compare", "object_truth.observe"],
        object_version_refs=["object_version:1", "object_version:2"],
        sop_refs=["sop:account-cleanup"],
        policy_refs=["policy:object-truth-deterministic-substrate"],
        previous_contract_digest=left["contract_digest"],
        failure_pattern_refs=["failure-pattern:missing-identity"],
    )

    assert left["contract_digest"] == right["contract_digest"]
    assert appended["previous_contract_digest"] == left["contract_digest"]
    assert appended["contract_digest"] != left["contract_digest"]
