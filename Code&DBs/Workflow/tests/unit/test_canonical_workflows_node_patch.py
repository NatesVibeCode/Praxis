import pytest

from runtime.canonical_workflows import (
    WorkflowRuntimeBoundaryError,
    _apply_node_field_patch,
)


def _definition() -> dict:
    return {
        "draft_flow": [
            {
                "id": "node-1",
                "title": "Research app",
                "summary": "Find integration surface",
                "depends_on": [],
            }
        ],
        "execution_setup": {
            "phases": [
                {
                    "step_id": "node-1",
                    "agent_route": "auto/research",
                    "system_prompt": "Search once.",
                    "required_inputs": ["app_domain"],
                    "outputs": ["candidate_docs"],
                    "persistence_targets": ["integration_research"],
                    "agent_tool_plan": {
                        "tool_name": "praxis_search",
                        "operation": "search",
                        "repeats": 4,
                    },
                }
            ]
        },
    }


def test_apply_node_field_patch_updates_exact_node_field() -> None:
    patched = _apply_node_field_patch(
        _definition(),
        row={"compiled_spec": {}},
        subpath="nodes/node-1/prompt",
        body={"value": "Search docs, API references, auth pages, and SDK examples."},
    )

    phase = patched["execution_setup"]["phases"][0]
    assert phase["step_id"] == "node-1"
    assert phase["system_prompt"] == "Search docs, API references, auth pages, and SDK examples."
    assert phase["agent_tool_plan"]["tool_name"] == "praxis_search"


def test_apply_node_field_patch_rejects_multi_field_body() -> None:
    with pytest.raises(WorkflowRuntimeBoundaryError, match="exactly one field"):
        _apply_node_field_patch(
            _definition(),
            row={"compiled_spec": {}},
            subpath="nodes/node-1",
            body={"prompt": "Search docs.", "outputs": ["candidate_docs"]},
        )

