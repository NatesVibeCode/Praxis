from __future__ import annotations

import json
from pathlib import Path

from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.tools import connector


def test_connector_build_uses_manifest_builder_template_and_workflow_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    template_dir = tmp_path / "config" / "cascade" / "specs"
    template_dir.mkdir(parents=True)
    template_path = template_dir / "W_integration_builder_template.queue.json"
    template_path.write_text(
        json.dumps(
            {
                "name": "Integration Builder - <<INTEGRATION_NAME>>",
                "queue_id": "integration_builder_<<INTEGRATION_SLUG>>",
                "jobs": [
                    {
                        "label": "research_api",
                        "prompt": "Read <<AUTH_DOCS_URL>> and use <<SECRET_ENV_VAR>>",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    launched_paths: list[str] = []

    monkeypatch.setattr(connector, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(connector, "_TEMPLATE_PATH", template_path)
    monkeypatch.setattr(
        connector,
        "_SPECS_DIR",
        tmp_path / "artifacts" / "workflow" / "integration_builder",
    )
    monkeypatch.setattr(
        connector,
        "_launch_workflow",
        lambda spec_path: launched_paths.append(spec_path) or {"run_id": "run-connector"},
    )

    result = connector.tool_praxis_connector(
        {
            "action": "build",
            "app_name": "Example CRM",
            "auth_docs_url": "https://docs.example.com/api",
            "secret_env_var": "EXAMPLE_CRM_TOKEN",
        },
    )

    spec_path = Path(result["workflow_spec_path"])
    assert spec_path == tmp_path / "artifacts" / "workflow" / "integration_builder" / "connector_example_crm.queue.json"
    assert launched_paths == [str(spec_path)]
    assert not (tmp_path / "config" / "specs").exists()
    assert not (tmp_path / "artifacts" / "connectors").exists()

    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    assert spec["name"] == "Integration Builder - Example CRM"
    assert spec["queue_id"] == "integration_builder_example_crm"
    assert "https://docs.example.com/api" in spec["jobs"][0]["prompt"]
    assert "EXAMPLE_CRM_TOKEN" in spec["jobs"][0]["prompt"]


def test_connector_catalog_projects_action_requirements() -> None:
    get_tool_catalog.cache_clear()
    definition = get_tool_catalog()["praxis_connector"]
    capability_by_action = {
        capability["action"]: capability
        for capability in definition.capability_rows()
    }

    assert capability_by_action["build"]["requiredArgs"] == ["app_name"]
    assert capability_by_action["get"]["requiredArgs"] == ["app_slug"]
    assert capability_by_action["register"]["requiredArgs"] == ["app_slug"]
    assert capability_by_action["verify"]["requiredArgs"] == ["app_slug"]
