"""Workflow-to-workflow invocation.

Direct servicebus operation: looks up a saved workflow by workflow_id, submits
its compiled_spec as a new workflow run, waits for completion, returns output.
No LLM in the loop.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

MAX_WAIT_SECONDS = 600  # 10 minutes
POLL_INTERVAL = 3       # seconds


def invoke_workflow(args: dict, pg: Any) -> dict:
    """Invoke a saved workflow by workflow_id.

    args:
        workflow_id: str (required) — lookup by ID
        inputs: dict (optional) — injected into first job's prompt as context
        parent_run_id: str (optional) — the invoking run's ID
        trigger_depth: int (optional) — current nesting depth

    Returns IntegrationResult with child run output.
    """
    workflow_id = str(args.get("workflow_id") or "").strip()
    inputs = args.get("inputs", {})
    parent_run_id = args.get("parent_run_id")
    trigger_depth = args.get("trigger_depth", 0)

    if not workflow_id:
        return {
            "status": "failed",
            "data": None,
            "summary": "workflow_id required.",
            "error": "missing_workflow_id",
        }

    # 1. Look up the workflow by canonical authority key.
    rows = pg.execute(
        "SELECT id, name, definition, compiled_spec, invocation_count, last_invoked_at FROM workflows WHERE id = $1",
        workflow_id,
    )

    if not rows:
        return {
            "status": "failed",
            "data": None,
            "summary": f"Workflow not found: {workflow_id}",
            "error": "workflow_not_found",
        }

    wf = rows[0]
    from runtime.operating_model_planner import current_compiled_spec, missing_execution_plan_message

    definition_row = wf.get("definition")
    compiled_spec_row = wf.get("compiled_spec")
    spec_raw = current_compiled_spec(definition_row, compiled_spec_row)
    if not spec_raw:
        return {
            "status": "failed",
            "data": None,
            "summary": missing_execution_plan_message(wf["name"]),
            "error": "no_current_plan",
        }

    # 2. Inject inputs into first job's prompt (if provided)
    if inputs and spec_raw.get("jobs"):
        first_job = spec_raw["jobs"][0]
        input_block = "\n\n## Inputs from Parent Workflow\n" + json.dumps(inputs, indent=2, default=str)
        first_job["prompt"] = first_job.get("prompt", "") + input_block

    # 3. Submit as a new workflow run — direct Postgres insert, no LLM
    try:
        from runtime.workflow.unified import submit_workflow_inline

        result = submit_workflow_inline(
            pg,
            spec_raw,
            parent_run_id=parent_run_id,
            trigger_depth=trigger_depth + 1,
            packet_provenance={
                "source_kind": "workflow_invoke",
                "workflow_row": wf,
                "definition_row": definition_row,
                "compiled_spec_row": spec_raw,
                "file_inputs": {
                    "inputs": inputs,
                },
            },
        )
        child_run_id = result["run_id"]
        packet_reuse_provenance = result.get("packet_reuse_provenance")
    except RuntimeError as exc:
        # Trigger depth exceeded
        return {
            "status": "failed",
            "data": None,
            "summary": str(exc),
            "error": "trigger_depth_exceeded",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "data": None,
            "summary": f"Failed to submit child workflow: {exc}",
            "error": "submit_error",
        }

    # 4. Update invocation tracking
    pg.execute(
        "UPDATE workflows SET invocation_count = invocation_count + 1, last_invoked_at = now() WHERE id = $1",
        wf["id"],
    )

    # 5. Wait for child to complete — poll Postgres, no LLM
    logger.info("Waiting for child workflow %s (parent=%s, depth=%d)",
                child_run_id, parent_run_id, trigger_depth + 1)

    deadline = time.monotonic() + MAX_WAIT_SECONDS
    child_status = "running"

    while time.monotonic() < deadline:
        rows = pg.execute(
            "SELECT current_state FROM workflow_runs WHERE run_id = $1", child_run_id,
        )
        if rows and rows[0]["current_state"] in ("succeeded", "failed", "dead_letter", "cancelled"):
            child_status = rows[0]["current_state"]
            break
        time.sleep(POLL_INTERVAL)
    else:
        return {
            "status": "failed",
            "data": {"child_run_id": child_run_id},
            "summary": f"Child workflow {child_run_id} timed out after {MAX_WAIT_SECONDS}s.",
            "error": "invoke_timeout",
        }

    # 6. Collect child output
    job_rows = pg.execute(
        """SELECT label, status, stdout_preview, duration_ms, cost_usd
           FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at""",
        child_run_id,
    )

    jobs_output = []
    for j in (job_rows or []):
        jobs_output.append({
            "label": j["label"],
            "status": j["status"],
            "output": j.get("stdout_preview", "")[:1000],
            "duration_ms": j.get("duration_ms", 0),
        })

    last_invoked_at = wf.get("last_invoked_at")
    if hasattr(last_invoked_at, "isoformat"):
        last_invoked_at = last_invoked_at.isoformat()
    elif last_invoked_at is not None:
        last_invoked_at = str(last_invoked_at)

    return {
        "status": "succeeded" if child_status == "succeeded" else "failed",
        "data": {
            "child_run_id": child_run_id,
            "child_status": child_status,
            "workflow_id": wf["id"],
            "workflow_name": wf["name"],
            "packet_reuse_provenance": packet_reuse_provenance,
            "invocation_count": int(wf.get("invocation_count") or 0),
            "last_invoked_at": last_invoked_at,
            "jobs": jobs_output,
        },
        "summary": f"Workflow '{wf['name']}' → {child_status} ({len(jobs_output)} jobs, run {child_run_id})",
        "error": None if child_status == "succeeded" else "child_failed",
    }
