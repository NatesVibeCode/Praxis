from __future__ import annotations

from surfaces.mcp.catalog import get_tool_catalog


def _data_definition():
    get_tool_catalog.cache_clear()
    return get_tool_catalog()["praxis_data"]


def test_praxis_data_schema_projects_runtime_action_requirements() -> None:
    definition = _data_definition()
    schema = definition.input_schema

    assert "input" in schema["properties"]
    assert "secondary_input" in schema["properties"]
    assert "operation" in schema["properties"]
    assert schema["$defs"]["input_source"]["anyOf"] == [
        {"required": ["input"]},
        {"required": ["input_path"]},
        {"required": ["records"]},
    ]

    action_requirements = definition.action_requirements
    assert action_requirements["filter"]["required"] == ["predicates"]
    assert action_requirements["normalize"]["required"] == ["rules"]
    assert action_requirements["approve"]["required"] == ["approved_by", "approval_reason"]
    assert action_requirements["apply"]["required"] == ["keys"]
    assert action_requirements["sync"]["required"] == ["keys"]

    capability_by_action = {
        capability["action"]: capability
        for capability in definition.capability_rows()
    }
    assert capability_by_action["filter"]["requiredArgs"] == ["predicates"]
    assert capability_by_action["normalize"]["requiredArgs"] == ["rules"]
    assert capability_by_action["approve"]["requiredArgs"] == ["approved_by", "approval_reason"]
    assert capability_by_action["apply"]["requiredArgs"] == ["keys"]
    assert capability_by_action["sync"]["requiredArgs"] == ["keys"]


def test_praxis_data_schema_keeps_alternative_sources_machine_readable() -> None:
    schema = _data_definition().input_schema

    filter_branch = next(
        branch
        for branch in schema["allOf"]
        if branch["if"]["properties"]["action"].get("const") == "filter"
    )
    assert filter_branch["then"]["allOf"] == [
        {"$ref": "#/$defs/input_source"},
        {"required": ["predicates"]},
    ]

    join_branch = next(
        branch
        for branch in schema["allOf"]
        if branch["if"]["properties"]["action"].get("const") == "join"
    )
    assert {"$ref": "#/$defs/input_source"} in join_branch["then"]["allOf"]
    assert {"$ref": "#/$defs/secondary_input_source"} in join_branch["then"]["allOf"]
    assert {
        "anyOf": [
            {"required": ["keys"]},
            {"required": ["left_keys", "right_keys"]},
        ],
    } in join_branch["then"]["allOf"]

    workflow_branch = next(
        branch
        for branch in schema["allOf"]
        if branch["if"]["properties"]["action"].get("enum") == ["run", "workflow_spec", "launch"]
    )
    assert workflow_branch["then"] == {"$ref": "#/$defs/workflow_job_source"}
