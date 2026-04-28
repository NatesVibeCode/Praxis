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
