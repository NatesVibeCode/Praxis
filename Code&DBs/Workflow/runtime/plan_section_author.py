"""Layer 4 (Author) — section + cluster author building blocks."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPacket, SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms

logger = logging.getLogger(__name__)


def _section_author_timeout_seconds() -> float:
    raw = os.environ.get("WORKFLOW_SECTION_AUTHOR_TIMEOUT_S", "").strip()
    if not raw:
        return 180.0
    try:
        return max(10.0, float(raw))
    except ValueError:
        return 180.0


@dataclass(frozen=True)
class AuthoredPacket:
    label: str
    stage: str
    description: str
    prompt: str
    write: list[str]
    agent: str
    task_type: str
    capabilities: list[str]
    consumes: list[str]
    produces: list[str]
    depends_on: list[str]
    gates: list[dict[str, Any]]
    parameters: dict[str, Any]
    workdir: str | None
    on_failure: str
    on_success: str
    timeout: int
    budget: dict[str, Any] | None
    raw_llm_response: str
    provider_slug: str
    model_slug: str
    usage: dict[str, int] = field(default_factory=dict)
    proposed_pills: list[dict[str, Any]] = field(default_factory=list)
    pill_audit_local: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label, "stage": self.stage, "description": self.description,
            "prompt": self.prompt, "write": list(self.write), "agent": self.agent,
            "task_type": self.task_type, "capabilities": list(self.capabilities),
            "consumes": list(self.consumes), "produces": list(self.produces),
            "depends_on": list(self.depends_on),
            "gates": [dict(gate) for gate in self.gates],
            "parameters": dict(self.parameters), "workdir": self.workdir,
            "on_failure": self.on_failure, "on_success": self.on_success,
            "timeout": self.timeout,
            "budget": dict(self.budget) if self.budget else None,
            "raw_llm_response": self.raw_llm_response,
            "provider_slug": self.provider_slug, "model_slug": self.model_slug,
            "usage": dict(self.usage),
            "proposed_pills": list(self.proposed_pills),
            "pill_audit_local": list(self.pill_audit_local),
        }


@dataclass(frozen=True)
class AuthorError:
    label: str
    error: str
    reason_code: str
    raw_llm_response: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label, "error": self.error,
            "reason_code": self.reason_code, "raw_llm_response": self.raw_llm_response,
        }


@dataclass(frozen=True)
class SectionSandbox:
    plan_field_schema: list[dict[str, Any]]
    stage_io: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_field_schema": list(self.plan_field_schema),
            "stage_io": dict(self.stage_io),
        }


def _load_plan_field_schema(conn: Any) -> list[dict[str, Any]]:
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds
    out: list[dict[str, Any]] = []
    try:
        rows = list_object_kinds(conn, category="plan_field")
    except DataDictionaryBoundaryError:
        return out
    for row in rows:
        object_kind = str(row.get("object_kind") or "")
        field_name = object_kind.split(":", 1)[1] if ":" in object_kind else object_kind
        metadata = row.get("metadata") or {}
        out.append({
            "field": field_name, "label": row.get("label"),
            "summary": row.get("summary"), "metadata": metadata,
        })
    out.sort(key=lambda entry: (entry["metadata"].get("order") or 999, entry["field"]))
    return out


def build_shared_prefix(atoms: "SuggestedAtoms", sandbox: SectionSandbox) -> str:
    """Byte-identical prefix used by the synthesis call AND every fork-out call.

    The provider's prefix cache (DeepSeek/Together auto, OpenAI auto, etc.) hits
    only when subsequent calls share an exact-prefix-match. Synthesis is the
    cache-priming call — its purpose is to write this prefix once. Fork-out
    calls then shift the cache hit and pay full price only on their per-call
    suffix delta.

    Determinism rules:
      - JSON dicts serialized with sort_keys=True
      - No timestamps, no random ids, no per-call values inside the prefix
      - Pill / schema lists stable-sorted before serialization
    """
    pills_view = sorted(
        [
            {
                "ref": f"{p.object_kind}.{p.field_path}",
                "field_kind": p.field_kind,
                "source": "bound",
            }
            for p in atoms.pills.bound[:6]
        ]
        + [
            {
                "ref": p.ref,
                "field_kind": p.field_kind,
                "label": p.label,
                "summary": p.summary,
                "source": "suggested",
            }
            for p in atoms.suggested_pills[:8]
        ],
        key=lambda r: r["ref"],
    )

    field_lines: list[str] = []
    for entry in sorted(sandbox.plan_field_schema, key=lambda e: e["field"]):
        meta = entry.get("metadata", {})
        flags: list[str] = []
        if meta.get("required"):
            flags.append("REQUIRED")
        if meta.get("type"):
            flags.append(f"type={meta['type']}")
        if meta.get("forbid_workspace_root"):
            flags.append("no-workspace-root")
        field_lines.append(f"  - {entry['field']}: {entry['summary']} [{', '.join(flags)}]")

    sections: list[str] = [
        "PROMPT:",
        atoms.intent,
        "",
        "DATA PILLS:",
        json.dumps(pills_view, indent=2, sort_keys=True),
        "",
        "DATA DICTIONARY (registered stages):",
        json.dumps(sandbox.stage_io, indent=2, sort_keys=True),
        "",
        "DATA DICTIONARY (PlanPacket fields):",
        "\n".join(field_lines),
    ]
    return "\n".join(sections)


def build_section_sandbox(conn: Any) -> SectionSandbox:
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds

    stage_io: dict[str, dict[str, Any]] = {}
    try:
        for row in list_object_kinds(conn, category="stage"):
            kind = str(row.get("object_kind") or "")
            stage_name = kind.split(":", 1)[1] if ":" in kind else kind
            metadata = row.get("metadata") or {}
            stage_io[stage_name] = {
                "produces": list(metadata.get("produces") or []),
                "consumes": list(metadata.get("consumes") or []),
            }
    except DataDictionaryBoundaryError:
        pass

    return SectionSandbox(
        plan_field_schema=_load_plan_field_schema(conn),
        stage_io=stage_io,
    )


def _strip_json_fences(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _coerce_packet_response(
    *, target: SkeletalPacket, parsed: dict[str, Any], raw: str,
    provider_slug: str, model_slug: str,
    usage: dict[str, int] | None = None,
) -> AuthoredPacket:
    def floor_union(llm_value: Any, floor: list[str]) -> list[str]:
        out = list(floor)
        if isinstance(llm_value, list):
            for item in llm_value:
                if isinstance(item, str) and item not in out:
                    out.append(item)
        return out

    consumes = floor_union(parsed.get("consumes"), target.consumes_floor)
    produces = floor_union(parsed.get("produces"), target.produces_floor)
    capabilities = floor_union(parsed.get("capabilities"), target.capabilities_floor)

    gates_out: list[dict[str, Any]] = []
    seen: set[str] = set()
    if isinstance(parsed.get("gates"), list):
        for gate in parsed["gates"]:
            if isinstance(gate, dict) and isinstance(gate.get("gate_id"), str):
                normalized = dict(gate)
                normalized.setdefault("params", {})
                gates_out.append(normalized)
                seen.add(gate["gate_id"])
            elif isinstance(gate, str):
                gates_out.append({"gate_id": gate, "params": {}})
                seen.add(gate)
    for gate in target.gates_scaffold:
        if gate.gate_id in seen:
            continue
        gates_out.append(gate.to_dict())

    write_raw = parsed.get("write")
    if isinstance(write_raw, list) and write_raw:
        write = [str(item) for item in write_raw]
    else:
        # Deterministic write-scope floor (2026-04-26): the validator rejects
        # empty write and rejects workspace-root scopes. Without a floor here,
        # every fork-author response that omits "write" lands as 2 errors per
        # packet (plan_field.required_missing + plan_field.workspace_root).
        # Stage-and-label scoped artifacts/ path satisfies both constraints
        # deterministically without burning an LLM round-trip.
        write = [f"artifacts/{target.stage}/{target.label}/"]

    return AuthoredPacket(
        label=target.label, stage=target.stage,
        description=str(parsed.get("description") or target.description),
        prompt=str(parsed.get("prompt") or ""),
        write=write,
        agent=str(parsed.get("agent") or f"auto/{target.stage}"),
        task_type=str(parsed.get("task_type") or target.stage),
        capabilities=capabilities, consumes=consumes, produces=produces,
        depends_on=list(target.depends_on), gates=gates_out,
        parameters=dict(parsed.get("parameters") or {}),
        workdir=parsed.get("workdir"),
        on_failure=str(parsed.get("on_failure") or "abort"),
        on_success=str(parsed.get("on_success") or "continue"),
        timeout=int(parsed.get("timeout") or 300),
        budget=dict(parsed["budget"]) if isinstance(parsed.get("budget"), dict) else None,
        raw_llm_response=raw, provider_slug=provider_slug, model_slug=model_slug,
        usage=dict(usage or {}),
        proposed_pills=[
            p for p in (parsed.get("proposed_pills") or []) if isinstance(p, dict)
        ],
        pill_audit_local=[
            p for p in (parsed.get("pill_audit_local") or []) if isinstance(p, dict)
        ],
    )


def _resolve_routes_for_task_type(task_type: str) -> list[tuple[str, str]]:
    """Return route list for any task_type, ordered by rank."""
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    try:
        rows = pg.fetch(
            """
            SELECT provider_slug, model_slug
              FROM task_type_routing
             WHERE task_type = $1
               AND permitted = true
               AND route_source = 'explicit'
             ORDER BY rank ASC, updated_at DESC
            """,
            task_type,
        )
    except Exception:
        rows = []
    routes = [
        (str(row["provider_slug"]), str(row["model_slug"]))
        for row in rows or []
        if str(row.get("provider_slug") or "").strip()
        and str(row.get("model_slug") or "").strip()
    ]
    if routes:
        return routes
    from runtime.compiler_llm import _resolve_app_compile_routes
    return _resolve_app_compile_routes()


def _resolve_section_author_routes() -> list[tuple[str, str]]:
    """Legacy alias — returns plan_section_author rows."""
    return _resolve_routes_for_task_type("plan_section_author")
