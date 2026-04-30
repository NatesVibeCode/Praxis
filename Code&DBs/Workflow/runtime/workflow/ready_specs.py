"""Ready-spec evaluator: fires queued workflow specs from workflow_spec_ready."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from runtime.system_events import emit_system_event

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


def evaluate_ready_specs(conn: SyncPostgresConnection) -> int:
    """Find and fire due specs from workflow_spec_ready.
    
    Returns the number of specs fired.
    """
    from runtime.control_commands import (
        request_workflow_submit_command,
        render_workflow_submit_response,
    )
    from runtime.workflow_spec import WorkflowSpec
    from runtime.workspace_paths import repo_root as workspace_repo_root

    repo_root = workspace_repo_root()

    # Find due specs
    rows = conn.execute(
        """
        SELECT spec_id, spec_path
        FROM workflow_spec_ready
        WHERE status = 'staged'
          AND (scheduled_at IS NULL OR scheduled_at <= now())
        ORDER BY scheduled_at NULLS FIRST, created_at
        LIMIT 10
        """
    )
    if not rows:
        return 0

    fired_count = 0
    for row in rows:
        spec_id = str(row["spec_id"])
        spec_path = str(row["spec_path"])

        try:
            # Load spec to get metadata
            spec = WorkflowSpec.load(spec_path)
            
            # Submit through service bus
            command = request_workflow_submit_command(
                conn,
                requested_by_kind="system",
                requested_by_ref=f"workflow.ready_spec:{spec_id}",
                spec_path=spec_path,
                repo_root=str(repo_root),
                idempotency_key=f"ready_spec:{spec_id}",
            )
            result = render_workflow_submit_response(
                command,
                spec_name=spec.name,
                total_jobs=len(spec.jobs),
            )
            
            run_id = result.get("run_id")
            if run_id:
                # Update status in ready table
                conn.execute(
                    """
                    UPDATE workflow_spec_ready
                    SET status = 'fired',
                        run_id = $2,
                        fired_at = now(),
                        updated_at = now()
                    WHERE spec_id = $1
                    """,
                    spec_id,
                    run_id,
                )
                fired_count += 1
                logger.info("Fired ready spec %s -> run %s", spec_id, run_id)
                emit_system_event(
                    conn,
                    event_type="workflow.ready_spec.fired",
                    source_id=spec_id,
                    source_type="workflow_spec_ready",
                    payload={"spec_id": spec_id, "spec_path": spec_path, "run_id": run_id, "spec_name": spec.name},
                )
            else:
                error_code = result.get("error_code") or "submit_failed"
                logger.error("Failed to fire ready spec %s: %s", spec_id, error_code)
                conn.execute(
                    """
                    UPDATE workflow_spec_ready
                    SET status = 'failed',
                        updated_at = now()
                    WHERE spec_id = $1
                    """,
                    spec_id,
                )
                emit_system_event(
                    conn,
                    event_type="workflow.ready_spec.failed",
                    source_id=spec_id,
                    source_type="workflow_spec_ready",
                    payload={"spec_id": spec_id, "spec_path": spec_path, "error_code": error_code},
                )

        except Exception as exc:
            logger.error("Error firing ready spec %s: %s", spec_id, exc, exc_info=True)
            conn.execute(
                """
                UPDATE workflow_spec_ready
                SET status = 'failed',
                    updated_at = now()
                WHERE spec_id = $1
                """,
                spec_id,
            )
            emit_system_event(
                conn,
                event_type="workflow.ready_spec.failed",
                source_id=spec_id,
                source_type="workflow_spec_ready",
                payload={"spec_id": spec_id, "spec_path": spec_path, "error": f"{type(exc).__name__}: {exc}"},
            )

    return fired_count
