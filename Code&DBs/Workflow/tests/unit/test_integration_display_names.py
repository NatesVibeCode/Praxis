from __future__ import annotations

from runtime.integrations.display_names import (
    base_integration_name,
    display_name_for_integration,
)
from runtime.integrations.integration_registry import list_integrations


def test_display_name_for_praxis_tool_uses_single_brand_prefix() -> None:
    row = {
        "id": "praxis_workflow",
        "name": "Praxis Workflow",
        "manifest_source": "mcp_tool",
        "mcp_server_id": "praxis-workflow-mcp",
    }

    assert base_integration_name(row) == "Workflow"
    assert display_name_for_integration(row) == "Praxis: Workflow"


def test_list_integrations_exposes_branded_display_name_for_praxis_tools() -> None:
    class _Conn:
        def execute(self, query: str) -> list[dict[str, object]]:
            assert "FROM integration_registry ir" in query
            return [
                {
                    "id": "praxis_workflow",
                    "name": "Workflow",
                    "description": "Run and inspect workflows",
                    "provider": "mcp",
                    "capabilities": [{"action": "kickoff", "description": "Start a run"}],
                    "auth_status": "connected",
                    "manifest_source": "mcp_tool",
                    "connector_slug": None,
                    "health_status": None,
                    "error_rate": None,
                }
            ]

    rows = list_integrations(_Conn())

    assert rows == [
        {
            "id": "praxis_workflow",
            "name": "Workflow",
            "display_name": "Praxis: Workflow",
            "description": "Run and inspect workflows",
            "provider": "mcp",
            "auth_status": "connected",
            "source": "mcp_tool",
            "catalog_dispatch": False,
            "health_status": None,
            "error_rate": None,
            "actions": [{"action": "kickoff", "description": "Start a run"}],
        }
    ]
