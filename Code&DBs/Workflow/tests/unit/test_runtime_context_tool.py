from __future__ import annotations

from types import SimpleNamespace

from surfaces.mcp.tools import runtime_context


def test_praxis_context_shard_summary_reads_current_workflow_session(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime_context,
        "get_current_workflow_mcp_context",
        lambda: SimpleNamespace(
            run_id="run.alpha",
            workflow_id="workflow.alpha",
            job_label="job-alpha",
        ),
    )
    monkeypatch.setattr(runtime_context._subs, "get_pg_conn", lambda: object())
    monkeypatch.setattr(
        runtime_context,
        "load_workflow_job_runtime_context",
        lambda _conn, *, run_id, job_label: {
            "run_id": run_id,
            "job_label": job_label,
            "workflow_id": "workflow.alpha",
            "execution_context_shard": {
                "write_scope": ["runtime/example.py"],
                "resolved_read_scope": ["runtime/support.py"],
                "blast_radius": ["runtime/downstream.py"],
                "test_scope": ["tests/test_example.py"],
                "verify_refs": ["verify.spec.global"],
                "context_sections": [{"name": "FILE: runtime/support.py", "content": "def helper():\n    return 1\n"}],
            },
            "execution_bundle": {"tool_bucket": "build"},
        },
    )

    payload = runtime_context.tool_praxis_context_shard({"view": "summary", "include_bundle": True})

    assert payload["run_id"] == "run.alpha"
    assert payload["job_label"] == "job-alpha"
    assert payload["write_scope"] == ["runtime/example.py"]
    assert payload["context_section_names"] == ["FILE: runtime/support.py"]
    assert payload["execution_bundle"]["tool_bucket"] == "build"
