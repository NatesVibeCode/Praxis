from __future__ import annotations

from surfaces.mcp.tools import object_truth


def test_object_truth_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_observe_record"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth(
        {
            "system_ref": "salesforce",
            "object_ref": "account",
            "record": {"id": "001", "name": "Acme"},
            "identity_fields": ["id"],
            "source_metadata": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_observe_record"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_observe_record"
    assert captured["payload"] == {
        "system_ref": "salesforce",
        "object_ref": "account",
        "record": {"id": "001", "name": "Acme"},
        "identity_fields": ["id"],
    }


def test_object_truth_store_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_store_observed_record"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_store(
        {
            "system_ref": "salesforce",
            "object_ref": "account",
            "record": {"id": "001", "name": "Acme"},
            "identity_fields": ["id"],
            "observed_by_ref": "operator:nate",
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_store_observed_record"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_store_observed_record"
    assert captured["payload"] == {
        "system_ref": "salesforce",
        "object_ref": "account",
        "record": {"id": "001", "name": "Acme"},
        "identity_fields": ["id"],
        "observed_by_ref": "operator:nate",
    }


def test_object_truth_store_schema_snapshot_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_store_schema_snapshot"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_store_schema_snapshot(
        {
            "system_ref": "salesforce",
            "object_ref": "account",
            "raw_schema": {"fields": [{"name": "id", "type": "string"}]},
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_store_schema_snapshot"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_store_schema_snapshot"
    assert captured["payload"] == {
        "system_ref": "salesforce",
        "object_ref": "account",
        "raw_schema": {"fields": [{"name": "id", "type": "string"}]},
    }


def test_object_truth_compare_versions_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_compare_versions"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_compare_versions(
        {
            "left_object_version_digest": "left",
            "right_object_version_digest": "right",
        }
    )

    assert result == {"ok": True, "operation": "object_truth_compare_versions"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_compare_versions"
    assert captured["payload"] == {
        "left_object_version_digest": "left",
        "right_object_version_digest": "right",
    }


def test_object_truth_readiness_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_readiness", "state": "ready"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_readiness(
        {
            "client_payload_mode": "redacted_hashes",
            "planned_fanout": 2,
            "privacy_policy_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_readiness", "state": "ready"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_readiness"
    assert captured["payload"] == {
        "client_payload_mode": "redacted_hashes",
        "planned_fanout": 2,
    }


def test_object_truth_record_comparison_run_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_record_comparison_run"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_record_comparison_run(
        {
            "left_object_version_digest": "left",
            "right_object_version_digest": "right",
            "observed_by_ref": "operator:nate",
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_record_comparison_run"}
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"
    }
    assert captured["operation_name"] == "object_truth_record_comparison_run"
    assert captured["payload"] == {
        "left_object_version_digest": "left",
        "right_object_version_digest": "right",
        "observed_by_ref": "operator:nate",
    }


def test_object_truth_ingestion_sample_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_ingestion_sample_record"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_ingestion_sample_record(
        {
            "client_ref": "client.acme",
            "system_ref": "salesforce",
            "integration_id": "integration.salesforce.prod",
            "connector_ref": "connector.salesforce",
            "environment_ref": "sandbox",
            "object_ref": "account",
            "schema_snapshot_digest": "schema.digest.account",
            "captured_at": "2026-04-30T16:00:00Z",
            "capture_receipt_id": "receipt.capture.1",
            "identity_fields": ["id"],
            "sample_payloads": [{"id": "001"}],
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_ingestion_sample_record"}
    assert captured["operation_name"] == "object_truth_ingestion_sample_record"
    assert captured["payload"] == {
        "client_ref": "client.acme",
        "system_ref": "salesforce",
        "integration_id": "integration.salesforce.prod",
        "connector_ref": "connector.salesforce",
        "environment_ref": "sandbox",
        "object_ref": "account",
        "schema_snapshot_digest": "schema.digest.account",
        "captured_at": "2026-04-30T16:00:00Z",
        "capture_receipt_id": "receipt.capture.1",
        "identity_fields": ["id"],
        "sample_payloads": [{"id": "001"}],
    }


def test_object_truth_ingestion_sample_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_ingestion_sample_read"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_ingestion_sample_read(
        {
            "action": "describe",
            "sample_id": "sample.1",
            "client_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_ingestion_sample_read"}
    assert captured["operation_name"] == "object_truth_ingestion_sample_read"
    assert captured["payload"] == {
        "action": "describe",
        "sample_id": "sample.1",
    }


def test_object_truth_mdm_resolution_record_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_mdm_resolution_record"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_mdm_resolution_record(
        {
            "client_ref": "client.acme",
            "entity_type": "organization",
            "as_of": "2026-04-30T16:00:00Z",
            "identity_clusters": [{"cluster_id": "cluster.1"}],
            "field_comparisons": [{"field_comparison_digest": "comparison.digest"}],
            "source_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_mdm_resolution_record"}
    assert captured["operation_name"] == "object_truth_mdm_resolution_record"
    assert captured["payload"] == {
        "client_ref": "client.acme",
        "entity_type": "organization",
        "as_of": "2026-04-30T16:00:00Z",
        "identity_clusters": [{"cluster_id": "cluster.1"}],
        "field_comparisons": [{"field_comparison_digest": "comparison.digest"}],
    }


def test_object_truth_mdm_resolution_read_mcp_tool_uses_gateway(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        object_truth,
        "workflow_database_env",
        lambda: {"WORKFLOW_DATABASE_URL": "postgresql://authority.example/praxis"},
    )

    def _execute(*, env, operation_name, payload):
        captured["env"] = env
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "operation": "object_truth_mdm_resolution_read"}

    monkeypatch.setattr(object_truth, "execute_operation_from_env", _execute)

    result = object_truth.tool_praxis_object_truth_mdm_resolution_read(
        {
            "action": "describe",
            "packet_ref": "packet.1",
            "client_ref": None,
        }
    )

    assert result == {"ok": True, "operation": "object_truth_mdm_resolution_read"}
    assert captured["operation_name"] == "object_truth_mdm_resolution_read"
    assert captured["payload"] == {
        "action": "describe",
        "packet_ref": "packet.1",
    }
