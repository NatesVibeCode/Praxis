"""Verify adapter — DAG node that runs DB-backed post-dispatch verification.

Resolves verification bindings through `verification_registry`, then executes
typed argv commands in the workspace after the file_writer node has applied
code blocks. Stops on first failure.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .deterministic import DeterministicTaskRequest, DeterministicTaskResult, BaseNodeAdapter


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class VerifyAdapter(BaseNodeAdapter):
    """Run post-dispatch verification commands."""

    executor_type = "adapter.verifier"

    def execute(
        self, *, request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = dict(request.input_payload)

        bindings = payload.get("bindings", [])
        workdir = payload.get("workdir")

        if not bindings:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs={"bindings": 0},
                outputs={"all_passed": True, "results": [], "skipped": True},
                started_at=started_at,
                finished_at=_utc_now(),
            )

        try:
            from runtime.verification import resolve_verify_commands, run_verify
            from storage.postgres.connection import ensure_postgres_available

            conn = ensure_postgres_available()
            results = list(run_verify(resolve_verify_commands(conn, list(bindings)), workdir=workdir))
        except Exception as exc:
            results = [{
                "label": "verification_registry",
                "command": "verification_registry",
                "exit_code": -1,
                "output": str(exc),
                "passed": False,
            }]
            all_passed = False
        else:
            all_passed = all(result.passed for result in results)

        status = "succeeded" if all_passed else "failed"
        failure_code = None if all_passed else "verifier.check_failed"

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status=status,
            reason_code="adapter.execution_succeeded" if all_passed else "verifier.failed",
            executor_type=self.executor_type,
            inputs={"bindings": len(bindings)},
            outputs={
                "all_passed": all_passed,
                "results": [
                    result.to_json() if hasattr(result, "to_json") else result
                    for result in results
                ],
            },
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code=failure_code,
        )
