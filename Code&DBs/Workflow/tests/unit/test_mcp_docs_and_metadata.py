from __future__ import annotations

from pathlib import Path

from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.docs import render_mcp_markdown


def test_every_tool_has_complete_cli_metadata() -> None:
    catalog = get_tool_catalog()

    for definition in catalog.values():
        assert definition.cli_surface
        assert definition.cli_tier in {"stable", "advanced", "session"}
        assert definition.cli_entrypoint
        assert definition.cli_describe_command
        assert definition.cli_when_to_use
        assert definition.cli_when_not_to_use
        assert definition.cli_examples
        assert definition.risk_levels


def test_checked_in_mcp_docs_match_generated_catalog() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    docs_path = repo_root / "docs" / "MCP.md"

    assert docs_path.read_text(encoding="utf-8") == render_mcp_markdown()


def test_catalog_examples_match_current_tool_contracts() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    catalog = get_tool_catalog()

    artifacts = catalog["praxis_artifacts"].example_input()
    governance = catalog["praxis_governance"].example_input()
    graph = catalog["praxis_graph"].example_input()
    heal = catalog["praxis_heal"].example_input()
    roadmap = catalog["praxis_operator_roadmap_view"].example_input()
    validate = catalog["praxis_workflow_validate"].example_input()
    wave = catalog["praxis_wave"].example_input()

    assert artifacts["action"] == "list"
    assert "sandbox_id" not in artifacts
    assert governance["text"] == "Ship the API key in the test fixture"
    assert "prompt" not in governance
    assert graph["depth"] == 1
    assert "entity_id" not in graph
    assert heal["job_label"] == "build"
    assert roadmap == {}
    assert (repo_root / validate["spec_path"]).is_file()
    assert wave["action"] == "next"
    assert "wave_id" not in wave


def test_tool_display_name_omits_redundant_praxis_prefix() -> None:
    definition = get_tool_catalog()["praxis_workflow"]

    assert definition.display_name == "Workflow"
    assert definition.integration_row()["name"] == "Workflow"
