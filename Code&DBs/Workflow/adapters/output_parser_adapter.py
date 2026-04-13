"""Output parser adapter — DAG node that extracts structured output from model response.

Sits between the LLM node and the file_writer node. Parses the model's
stdout (which may be JSON, NDJSON, or fenced code blocks) into a canonical
StructuredOutput with typed code blocks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .deterministic import DeterministicTaskRequest, DeterministicTaskResult, BaseNodeAdapter
from .structured_output import parse_model_output


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class OutputParserAdapter(BaseNodeAdapter):
    """Parse model completion into structured code blocks."""

    executor_type = "adapter.output_parser"

    def execute(
        self, *, request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = self._merge_inputs(request)
        completion = str(payload.get("completion", ""))

        default_path = ""
        scope_write = payload.get("scope_write")
        if isinstance(scope_write, list) and scope_write:
            default_path = scope_write[0]

        structured = parse_model_output(completion, default_file_path=default_path)

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"completion_length": len(completion)},
            outputs={
                "has_code": structured.has_code,
                "parse_strategy": structured.parse_strategy,
                "explanation": structured.explanation,
                "code_blocks": [
                    {
                        "file_path": cb.file_path,
                        "content": cb.content,
                        "language": cb.language,
                        "action": cb.action,
                    }
                    for cb in structured.code_blocks
                ],
                "file_paths": list(structured.file_paths),
            },
            started_at=started_at,
            finished_at=_utc_now(),
        )
