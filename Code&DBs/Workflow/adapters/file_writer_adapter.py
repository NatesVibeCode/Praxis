"""File writer adapter — workflow node that applies code blocks to the filesystem.

The graph owns all I/O. This node receives parsed code blocks from the
output_parser node and writes them to disk with path validation and
atomic writes. The model never touches the filesystem.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .deterministic import DeterministicTaskRequest, DeterministicTaskResult, BaseNodeAdapter
from runtime.output_writer import apply_structured_output


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FileWriterAdapter(BaseNodeAdapter):
    """Write parsed code blocks to the filesystem."""

    executor_type = "adapter.file_writer"

    def execute(
        self, *, request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = self._merge_inputs(request)

        workspace_root = payload.get("workspace_root", "")

        # code_blocks come from the LLM adapter's structured_output
        code_blocks = payload.get("code_blocks", [])
        if not code_blocks:
            so = payload.get("structured_output")
            if isinstance(so, dict):
                code_blocks = so.get("code_blocks", [])

        if not code_blocks or not workspace_root:
            return DeterministicTaskResult(
                node_id=request.node_id,
                task_name=request.task_name,
                status="succeeded",
                reason_code="adapter.execution_succeeded",
                executor_type=self.executor_type,
                inputs={"code_blocks": 0, "workspace_root": workspace_root},
                outputs={"write_manifest": {"total_files": 0, "skipped": True}},
                started_at=started_at,
                finished_at=_utc_now(),
            )

        manifest = apply_structured_output(code_blocks, workspace_root=workspace_root)

        status = "succeeded" if manifest.all_succeeded else "failed"
        failure_code = None if manifest.all_succeeded else "file_writer.partial_failure"

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status=status,
            reason_code="adapter.execution_succeeded" if status == "succeeded" else "file_writer.write_failed",
            executor_type=self.executor_type,
            inputs={"code_blocks": len(code_blocks), "workspace_root": workspace_root},
            outputs={
                "write_manifest": {
                    "total_files": manifest.total_files,
                    "total_bytes": manifest.total_bytes,
                    "all_succeeded": manifest.all_succeeded,
                    "results": [
                        {
                            "file_path": r.file_path,
                            "action": r.action,
                            "bytes_written": r.bytes_written,
                            "success": r.success,
                            "error": r.error,
                        }
                        for r in manifest.results
                    ],
                },
            },
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code=failure_code,
        )
