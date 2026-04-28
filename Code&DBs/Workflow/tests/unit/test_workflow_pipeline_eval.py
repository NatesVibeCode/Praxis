from __future__ import annotations

from types import SimpleNamespace

from runtime.workflow.pipeline_eval import evaluate_pipeline_preview


def _spec(*, jobs: list[dict]):
    return SimpleNamespace(
        name="Eval spec",
        workflow_id="workflow.eval",
        jobs=jobs,
    )


def _preview_job(
    *,
    label: str,
    prompt: str,
    write_scope: list[str],
    result_kind: str = "artifact_bundle",
    submit_tool: str = "praxis_submit_artifact_bundle",
    mcp_tools: list[str] | None = None,
    scope_resolution_error: str | None = None,
):
    shard = {"write_scope": write_scope}
    if scope_resolution_error:
        shard["scope_resolution_error"] = scope_resolution_error
    bundle = {
        "access_policy": {"write_scope": write_scope, "workspace_mode": "docker_packet_only"},
        "completion_contract": {
            "submission_required": True,
            "result_kind": result_kind,
            "submit_tool_names": [submit_tool],
        },
        "mcp_tool_names": mcp_tools or [submit_tool, "praxis_get_submission"],
    }
    return {
        "label": label,
        "prompt": prompt,
        "task_type": "review",
        "execution_context_shard": shard,
        "execution_bundle": bundle,
        "completion_contract": bundle["completion_contract"],
        "mcp_tool_names": bundle["mcp_tool_names"],
    }


def test_pipeline_eval_blocks_scratch_scope_for_durable_artifact() -> None:
    prompt = (
        "Use praxis workflow discover. Do not edit code. Write "
        "Code&DBs/Workflow/artifacts/workflow/demo/PLAN.md"
    )
    spec = _spec(jobs=[{
        "label": "Plan packet",
        "agent": "openai/gpt-5.4-mini",
        "task_type": "review",
        "prompt": prompt,
        "verify_command": "test -s Code&DBs/Workflow/artifacts/workflow/demo/PLAN.md",
    }])
    preview = {
        "spec_name": "Eval spec",
        "workflow_id": "workflow.eval",
        "total_jobs": 1,
        "jobs": [
            _preview_job(
                label="Plan packet",
                prompt=prompt,
                write_scope=["scratch/workflow_eval"],
                result_kind="code_change",
                submit_tool="praxis_submit_code_change",
                mcp_tools=["praxis_query", "praxis_submit_code_change", "praxis_get_submission"],
            )
        ],
    }

    result = evaluate_pipeline_preview(spec, validation_result={"valid": True}, preview_payload=preview)
    kinds = {finding.kind for finding in result.findings}

    assert not result.ok
    assert "artifact_path_outside_write_scope" in kinds
    assert "scratch_fallback_with_artifact_paths" in kinds
    assert "artifact_job_uses_code_change_submission" in kinds
    assert "prompt_tool_not_allowed" in kinds


def test_pipeline_eval_blocks_scoped_broad_tool_instruction() -> None:
    prompt = "Use praxis workflow query, then write artifacts/workflow/demo/PLAN.md"
    spec = _spec(jobs=[{
        "label": "Plan packet",
        "agent": "openai/gpt-5.4-mini",
        "task_type": "review",
        "prompt": prompt,
    }])
    preview = {
        "spec_name": "Eval spec",
        "workflow_id": "workflow.eval",
        "total_jobs": 1,
        "jobs": [
            _preview_job(
                label="Plan packet",
                prompt=prompt,
                write_scope=["artifacts/workflow/demo/PLAN.md"],
                mcp_tools=["praxis_query", "praxis_submit_artifact_bundle", "praxis_get_submission"],
            )
        ],
    }

    result = evaluate_pipeline_preview(spec, validation_result={"valid": True}, preview_payload=preview)
    kinds = {finding.kind for finding in result.findings}

    assert not result.ok
    assert "prompt_tool_scope_not_enforced" in kinds


def test_pipeline_eval_allows_matching_artifact_bundle_contract() -> None:
    prompt = "Write artifacts/workflow/demo/PLAN.md"
    spec = _spec(jobs=[{
        "label": "Plan packet",
        "agent": "openai/gpt-5.4-mini",
        "task_type": "review",
        "prompt": prompt,
    }])
    preview = {
        "spec_name": "Eval spec",
        "workflow_id": "workflow.eval",
        "total_jobs": 1,
        "jobs": [
            _preview_job(
                label="Plan packet",
                prompt=prompt,
                write_scope=["artifacts/workflow/demo/PLAN.md"],
                mcp_tools=["praxis_submit_artifact_bundle", "praxis_get_submission"],
            )
        ],
    }

    result = evaluate_pipeline_preview(spec, validation_result={"valid": True}, preview_payload=preview)

    assert result.ok
    assert result.error_count == 0
