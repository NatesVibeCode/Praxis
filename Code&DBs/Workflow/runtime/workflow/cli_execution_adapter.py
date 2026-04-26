"""Workflow adapter bridge for CLI execution.

This is not a CLI runner. It adapts deterministic graph nodes onto the
canonical workflow execution backend.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from adapters.deterministic import (
    BaseNodeAdapter,
    DeterministicTaskRequest,
    DeterministicTaskResult,
)

from .execution_backends import execute_cli


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sandbox_policy(payload: dict[str, Any]) -> SimpleNamespace:
    raw = payload.get("sandbox_profile")
    profile = raw if isinstance(raw, dict) else {}
    return SimpleNamespace(
        network_policy=str(profile.get("network_policy") or "provider_only"),
        workspace_materialization=str(profile.get("workspace_materialization") or "copy"),
        secret_allowlist=tuple(profile.get("secret_allowlist") or ()),
    )


def _agent_config(payload: dict[str, Any]) -> SimpleNamespace:
    provider = str(payload.get("provider_slug") or "").strip().lower()
    model = str(payload.get("model_slug") or payload.get("model") or "").strip()
    return SimpleNamespace(
        provider=provider,
        model=model,
        wrapper_command=None,
        docker_image=None,
        timeout_seconds=int(payload.get("timeout") or 900),
        max_output_tokens=int(payload.get("max_tokens") or 4096),
        execution_transport="cli",
        sandbox_provider=str(
            (payload.get("sandbox_profile") or {}).get("sandbox_provider")
            if isinstance(payload.get("sandbox_profile"), dict)
            else ""
        ).strip()
        or "docker_local",
        sandbox_policy=_sandbox_policy(payload),
    )


class WorkflowCLIExecutionAdapter(BaseNodeAdapter):
    """Delegate `cli_llm` graph nodes to the canonical workflow CLI backend."""

    executor_type = "adapter.cli_llm"
    _prefer_docker = True

    def execute(
        self,
        *,
        request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = self._merge_inputs(request)
        prompt = str(payload.get("prompt") or "").strip()
        workdir = str(payload.get("workdir") or "").strip() or "."
        result = execute_cli(
            _agent_config(payload),
            prompt,
            workdir,
            execution_bundle=(
                payload.get("execution_bundle")
                if isinstance(payload.get("execution_bundle"), dict)
                else None
            ),
        )
        status = str(result.get("status") or "failed")
        error_code = str(result.get("error_code") or "").strip()
        stdout = str(result.get("stdout") or "")
        outputs = {
            "completion": stdout,
            "provider_slug": payload.get("provider_slug"),
            "model_slug": payload.get("model_slug") or payload.get("model"),
            **result,
        }
        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status=status,
            reason_code=error_code or (
                "adapter.execution_succeeded"
                if status == "succeeded"
                else "cli_adapter.execution_failed"
            ),
            executor_type=self.executor_type,
            inputs={
                "task_name": request.task_name,
                "input_payload": payload,
                "execution_boundary_ref": request.execution_boundary_ref,
            },
            outputs=outputs,
            started_at=started_at,
            finished_at=_utc_now(),
            failure_code=None if status == "succeeded" else error_code or "cli_adapter.execution_failed",
        )
