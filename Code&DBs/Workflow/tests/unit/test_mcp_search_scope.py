from __future__ import annotations

from surfaces.mcp.runtime_context import workflow_mcp_request_context
from surfaces.mcp.tools import search


def test_praxis_search_clamps_paths_to_workflow_shard(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute(_subs, *, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"results": [], "_meta": {}}

    monkeypatch.setattr(search, "execute_operation_from_subsystems", _fake_execute)

    with workflow_mcp_request_context(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        allowed_tools=["praxis_search"],
        expires_at=9999999999,
        source_refs=["BUG-123"],
        access_policy={
            "resolved_read_scope": ["Code&DBs/Workflow/runtime/sandbox_runtime.py"],
            "write_scope": ["Code&DBs/Workflow/tests/unit/test_sandbox_runtime.py"],
        },
    ):
        payload = search.tool_praxis_search(
            {
                "query": "workspace materialization",
                "scope": {"paths": ["Code&DBs/Workflow/**"]},
            }
        )

    assert payload["_meta"]["dispatch_path"] == "gateway"
    assert captured["operation_name"] == "search.federated"
    assert captured["payload"]["scope"]["paths"] == [
        "Code&DBs/Workflow/runtime/sandbox_runtime.py",
        "Code&DBs/Workflow/tests/unit/test_sandbox_runtime.py",
    ]


def test_praxis_search_rejects_paths_outside_workflow_shard() -> None:
    with workflow_mcp_request_context(
        run_id="run.alpha",
        workflow_id="workflow.alpha",
        job_label="job.alpha",
        allowed_tools=["praxis_search"],
        expires_at=9999999999,
        access_policy={
            "resolved_read_scope": ["Code&DBs/Workflow/runtime/sandbox_runtime.py"],
        },
    ):
        payload = search.tool_praxis_search(
            {
                "query": "secrets",
                "scope": {"paths": ["Code&DBs/Workflow/surfaces/**"]},
            }
        )

    assert payload["ok"] is False
    assert payload["reason_code"] == "workflow_mcp.search_scope_outside_shard"
