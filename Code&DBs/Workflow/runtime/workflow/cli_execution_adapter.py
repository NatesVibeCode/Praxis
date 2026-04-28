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
    _translate_host_path_to_container,
)

from .execution_backends import execute_cli
from .execution_bundle import build_execution_bundle


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


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, (list, tuple)):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            result.append(text)
    return result


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _scope_from_payload(payload: dict[str, Any]) -> dict[str, list[str]]:
    return {
        "write_scope": _string_list(payload.get("write_scope")) or _string_list(
            payload.get("scope_write")
        ),
        "declared_read_scope": _string_list(payload.get("declared_read_scope"))
        or _string_list(payload.get("scope_read")),
        "resolved_read_scope": _string_list(payload.get("resolved_read_scope")),
        "test_scope": _string_list(payload.get("test_scope")),
        "blast_radius": _string_list(payload.get("blast_radius")),
    }


def _merge_payload_scope_into_bundle(
    bundle: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Merge dynamic context-node scope output into a prebuilt bundle.

    Graph compilation may persist an initial execution_bundle before the
    context node resolves read/test/blast scope. The CLI adapter sees the
    merged dependency payload at execution time, so it is the last safe point
    to make the sandbox access_policy match the prompt manifest.
    """

    merged = dict(bundle)
    access_policy = dict(merged.get("access_policy") or {})
    for key, additions in _scope_from_payload(payload).items():
        if not additions:
            continue
        access_policy[key] = _dedupe_strings(
            _string_list(access_policy.get(key)) + additions
        )
    if access_policy:
        merged["access_policy"] = access_policy
    return merged


def _execution_bundle(payload: dict[str, Any]) -> dict[str, Any] | None:
    existing = payload.get("execution_bundle")
    if isinstance(existing, dict):
        return _merge_payload_scope_into_bundle(existing, payload)
    scope = _scope_from_payload(payload)
    write_scope = scope["write_scope"]
    declared_read_scope = scope["declared_read_scope"]
    resolved_read_scope = scope["resolved_read_scope"]
    test_scope = scope["test_scope"]
    blast_radius = scope["blast_radius"]
    if not any(
        (write_scope, declared_read_scope, resolved_read_scope, test_scope, blast_radius)
    ):
        return None
    return build_execution_bundle(
        run_id=str(payload.get("run_id") or "").strip() or None,
        workflow_id=str(payload.get("workflow_id") or "").strip() or None,
        job_label=str(payload.get("task_name") or payload.get("job_label") or "job"),
        prompt=str(payload.get("prompt") or ""),
        task_type=str(payload.get("task_type") or "").strip() or None,
        capabilities=_string_list(payload.get("capabilities")),
        allowed_tools=_string_list(payload.get("allowed_tools")),
        explicit_mcp_tools=_string_list(payload.get("mcp_tools")),
        explicit_skill_refs=_string_list(payload.get("skill_refs")),
        write_scope=write_scope,
        declared_read_scope=declared_read_scope,
        resolved_read_scope=resolved_read_scope,
        test_scope=test_scope,
        blast_radius=blast_radius,
        verify_refs=_string_list(payload.get("verify_refs")),
        sandbox_profile=(
            payload.get("sandbox_profile")
            if isinstance(payload.get("sandbox_profile"), dict)
            else None
        ),
    )


def _execution_workdir(payload: dict[str, Any]) -> str:
    raw = str(payload.get("workdir") or "").strip() or "."
    translated = _translate_host_path_to_container(raw)
    return str(translated or raw)


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
        workdir = _execution_workdir(payload)
        execution_bundle = _execution_bundle(payload)
        result = execute_cli(
            _agent_config(payload),
            prompt,
            workdir,
            execution_bundle=execution_bundle,
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
