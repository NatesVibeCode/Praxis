from __future__ import annotations

import pytest

from runtime.operations.queries.object_truth import QueryObserveRecord, handle_observe_record


def test_observe_record_query_handler_returns_deterministic_object_version() -> None:
    query = QueryObserveRecord(
        system_ref=" salesforce ",
        object_ref="account",
        record={
            "id": "001",
            "name": "Acme",
            "billing": {"city": "Denver"},
            "api_token": "secret-value",
        },
        identity_fields=["id"],
        source_metadata={"updated_at": "2026-04-28T10:00:00Z"},
    )

    result = handle_observe_record(query, subsystems=None)

    assert result["ok"] is True
    assert result["operation"] == "object_truth_observe_record"
    assert result["stats"]["field_observation_count"] == 5
    object_version = result["object_version"]
    assert object_version["system_ref"] == "salesforce"
    assert object_version["identity"]["identity_values"] == {"id": "001"}
    by_path = {item["field_path"]: item for item in object_version["field_observations"]}
    assert by_path["api_token"]["redacted_value_preview"] == "[REDACTED]"
    assert object_version["hierarchy_signals"]["has_nested_objects"] is True


def test_observe_record_query_requires_identity_fields() -> None:
    with pytest.raises(ValueError, match="identity_fields"):
        QueryObserveRecord(
            system_ref="salesforce",
            object_ref="account",
            record={"id": "001"},
            identity_fields=[],
        )
