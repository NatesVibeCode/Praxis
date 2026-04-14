"""Context compiler adapter — workflow node that reads files and compiles the prompt.

This is the first node in a dispatch graph. It reads target files via
scope_resolver, compiles context sections, and renders the full prompt
via prompt_renderer. The LLM node downstream receives the compiled
prompt — it never reads files itself.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from runtime._helpers import _fail

from .deterministic import DeterministicTaskRequest, DeterministicTaskResult


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ContextCompilerAdapter:
    """Compile context from files and render the prompt for the LLM node."""

    executor_type = "adapter.context_compiler"

    def __init__(
        self,
        *,
        shadow_packet_config: Mapping[str, Any] | None = None,
        conn_factory=None,
    ) -> None:
        self._shadow_packet_config = (
            dict(shadow_packet_config)
            if isinstance(shadow_packet_config, Mapping)
            else None
        )
        self._conn_factory = conn_factory

    def execute(
        self, *, request: DeterministicTaskRequest,
    ) -> DeterministicTaskResult:
        started_at = _utc_now()
        payload = dict(request.input_payload)

        prompt = payload.get("prompt", "")
        scope_read = payload.get("scope_read")
        scope_write = payload.get("scope_write")
        workdir = payload.get("workdir")
        system_prompt = payload.get("system_prompt")
        context_sections = payload.get("context_sections") or []
        resolved_scope_read: list[str] = []
        resolved_test_scope: list[str] = []
        resolved_blast_radius: list[str] = []

        # Resolve scope: read target files + their imports
        if scope_write and workdir:
            try:
                from runtime.scope_resolver import resolve_scope
                resolution = resolve_scope(scope_write, root_dir=workdir)
                resolved_scope_read = list(getattr(resolution, "computed_read_scope", []) or [])
                resolved_test_scope = list(getattr(resolution, "test_scope", []) or [])
                resolved_blast_radius = list(getattr(resolution, "blast_radius", []) or [])
                if resolution.context_sections:
                    context_sections = list(resolution.context_sections)
            except Exception as exc:
                import sys
                print(f"[context] scope resolution failed: {exc}", file=sys.stderr)

        # If no context from scope resolver, read files directly
        if not context_sections and scope_write and workdir:
            for fpath in scope_write:
                abs_path = os.path.join(workdir, fpath)
                try:
                    with open(abs_path) as fh:
                        context_sections.append({
                            "name": f"FILE: {fpath}",
                            "content": fh.read(),
                        })
                        resolved_scope_read.append(fpath)
                except OSError:
                    pass

        # Render the prompt with context
        from runtime.prompt_renderer import render_prompt

        class _Spec:
            adapter_type = "cli_llm"

        spec = _Spec()
        spec.prompt = prompt
        spec.provider_slug = payload.get("provider_slug", "anthropic")
        spec.model_slug = payload.get("model_slug")
        spec.system_prompt = system_prompt
        spec.context_sections = context_sections if context_sections else None

        rendered = render_prompt(spec)
        normalized_scope_read = _normalize_paths(scope_read) + [
            str(path).strip()
            for path in resolved_scope_read
            if str(path).strip()
        ]
        normalized_scope_write = _normalize_paths(scope_write)
        outputs = {
            "context_sections": [dict(s) for s in rendered.context_sections],
            "token_estimate": rendered.total_tokens_est,
            "scope_read": _ordered_unique(normalized_scope_read),
            "scope_write": _ordered_unique(normalized_scope_write),
            "test_scope": _ordered_unique([str(path).strip() for path in resolved_test_scope if str(path).strip()]),
            "blast_radius": _ordered_unique(
                [str(path).strip() for path in resolved_blast_radius if str(path).strip()]
            ),
        }
        if self._shadow_packet_config is None:
            outputs["system_message"] = rendered.system_message
            outputs["user_message"] = rendered.user_message

        if self._shadow_packet_config is not None:
            try:
                from runtime.shadow_execution_packet import (
                    ShadowExecutionPacketError,
                    build_shadow_execution_packet,
                    persist_shadow_execution_packet,
                )

                packet = build_shadow_execution_packet(
                    rendered=rendered,
                    payload=payload,
                    shadow_packet_config=self._shadow_packet_config,
                    scope_read=outputs["scope_read"],
                    scope_write=outputs["scope_write"],
                    test_scope=outputs["test_scope"],
                    blast_radius=outputs["blast_radius"],
                )
                packet = persist_shadow_execution_packet(
                    packet,
                    conn_factory=self._conn_factory,
                )
                outputs["execution_packet_ref"] = packet["packet_revision"]
                outputs["execution_packet_hash"] = packet["packet_hash"]
                outputs["shadow_execution_packet_ref"] = packet["packet_revision"]
                outputs["shadow_execution_packet_hash"] = packet["packet_hash"]
            except ShadowExecutionPacketError as exc:
                return _fail(
                    exc.reason_code,
                    str(exc),
                    request=request,
                    failure_code=exc.reason_code,
                    started_at=started_at,
                    executor_type=self.executor_type,
                    inputs={
                        "prompt_length": len(prompt),
                        "context_sections": len(context_sections),
                    },
                )

        return DeterministicTaskResult(
            node_id=request.node_id,
            task_name=request.task_name,
            status="succeeded",
            reason_code="adapter.execution_succeeded",
            executor_type=self.executor_type,
            inputs={"prompt_length": len(prompt), "context_sections": len(context_sections)},
            outputs=outputs,
            started_at=started_at,
            finished_at=_utc_now(),
        )


def _normalize_paths(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        return [text] if text else []
    if not isinstance(raw, (list, tuple, set)):
        return []
    normalized: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
