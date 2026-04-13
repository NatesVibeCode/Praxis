"""Tools: praxis_context_shard — bounded workflow runtime shard access."""

from __future__ import annotations

from typing import Any

from runtime.workflow.job_runtime_context import load_workflow_job_runtime_context

from ..helpers import _serialize
from ..runtime_context import get_current_workflow_mcp_context
from ..subsystems import _subs


def _strip_empty(obj: Any) -> Any:
    """Recursively remove None, empty strings, empty lists, and empty dicts."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            stripped = _strip_empty(v)
            if stripped is None:
                continue
            if isinstance(stripped, (str, list, dict)) and not stripped:
                continue
            result[k] = stripped
        return result or None
    if isinstance(obj, list):
        result_list = [_strip_empty(v) for v in obj]
        result_list = [v for v in result_list if v is not None]
        return result_list if result_list else None
    return obj


def tool_praxis_context_shard(params: dict) -> dict:
    context = get_current_workflow_mcp_context()
    if context is None:
        return {"error": "workflow MCP session context is unavailable"}

    run_id = context.run_id
    if not run_id:
        return {"error": "workflow MCP session is missing run_id authority"}

    record = load_workflow_job_runtime_context(
        _subs.get_pg_conn(),
        run_id=run_id,
        job_label=context.job_label,
    )
    if record is None:
        return {
            "error": "workflow runtime context is unavailable",
            "run_id": run_id,
            "job_label": context.job_label,
        }

    view = str(params.get("view") or "full").strip().lower() or "full"
    section_name = str(params.get("section_name") or "").strip()
    # Default False — the bundle duplicates scope data already in the shard
    include_bundle = bool(params.get("include_bundle", False))
    execution_context_shard = dict(record.get("execution_context_shard") or {})
    execution_bundle = dict(record.get("execution_bundle") or {})

    if view == "summary":
        payload: dict[str, Any] = {
            "run_id": run_id,
            "job_label": context.job_label,
        }
        if context.workflow_id or record.get("workflow_id"):
            payload["workflow_id"] = record.get("workflow_id") or context.workflow_id
        for key in ("write_scope", "resolved_read_scope", "blast_radius", "test_scope", "verify_refs"):
            val = execution_context_shard.get(key) or []
            if val:
                payload[key] = val
        section_names = [
            str(section.get("name") or "").strip()
            for section in execution_context_shard.get("context_sections") or []
            if isinstance(section, dict) and str(section.get("name") or "").strip()
        ]
        if section_names:
            payload["context_section_names"] = section_names
        if include_bundle:
            cleaned_bundle = _strip_empty(execution_bundle)
            if cleaned_bundle:
                payload["execution_bundle"] = cleaned_bundle
        return _serialize(payload)

    if view == "sections":
        sections = [
            dict(section)
            for section in execution_context_shard.get("context_sections") or []
            if isinstance(section, dict)
        ]
        if section_name:
            sections = [s for s in sections if str(s.get("name") or "").strip() == section_name]
        payload = {
            "run_id": run_id,
            "job_label": context.job_label,
            "context_sections": sections,
        }
        if context.workflow_id or record.get("workflow_id"):
            payload["workflow_id"] = record.get("workflow_id") or context.workflow_id
        if include_bundle:
            cleaned_bundle = _strip_empty(execution_bundle)
            if cleaned_bundle:
                payload["execution_bundle"] = cleaned_bundle
        return _serialize(payload)

    # Full view — strip empty fields from the shard before returning
    cleaned_shard = _strip_empty(execution_context_shard) or {}
    payload = {
        "run_id": run_id,
        "job_label": context.job_label,
        "execution_context_shard": cleaned_shard,
    }
    if context.workflow_id or record.get("workflow_id"):
        payload["workflow_id"] = record.get("workflow_id") or context.workflow_id
    if include_bundle:
        cleaned_bundle = _strip_empty(execution_bundle)
        if cleaned_bundle:
            payload["execution_bundle"] = cleaned_bundle
    return _serialize(payload)


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_context_shard": (
        tool_praxis_context_shard,
        {
            "description": (
                "Return the bounded execution shard for the current workflow MCP session. "
                "This is only valid inside workflow Docker jobs using the signed MCP bridge."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "view": {
                        "type": "string",
                        "enum": ["full", "summary", "sections"],
                        "default": "full",
                    },
                    "section_name": {
                        "type": "string",
                        "description": "Optional section name filter when view=sections.",
                    },
                    "include_bundle": {
                        "type": "boolean",
                        "description": "Include the execution bundle alongside the shard. Defaults to false — the bundle duplicates scope data already present in the shard.",
                        "default": False,
                    },
                },
                "required": [],
            },
        },
    ),
}

