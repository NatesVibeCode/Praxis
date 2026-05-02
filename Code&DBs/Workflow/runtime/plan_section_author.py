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
    # Wall-clock cap for one LLM call in the synthesis / fork-author chain.
    # Sized for the generous 50K-token budget on each call: a reasoning model
    # may legitimately take 5-8 minutes to think through a complex section.
    # Override via WORKFLOW_SECTION_AUTHOR_TIMEOUT_S (env, seconds).
    raw = os.environ.get("WORKFLOW_SECTION_AUTHOR_TIMEOUT_S", "").strip()
    if not raw:
        return 600.0
    try:
        return max(10.0, float(raw))
    except ValueError:
        return 600.0


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
    # Integration-agent fields. Populated when `agent` starts with
    # `integration/`. The runtime reads these as top-level workflow-job
    # fields when the packet ships through compose_plan_to_definition.
    integration_id: str | None = None
    integration_action: str | None = None
    integration_args: dict[str, Any] = field(default_factory=dict)
    # Per-packet observability for experiment reporting.
    wall_ms: int | None = None
    latency_ms: int | None = None
    finish_reason: str | None = None
    content_len: int | None = None
    reasoning_len: int | None = None

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
            "integration_id": self.integration_id,
            "integration_action": self.integration_action,
            "integration_args": dict(self.integration_args),
            "wall_ms": self.wall_ms,
            "latency_ms": self.latency_ms,
            "finish_reason": self.finish_reason,
            "content_len": self.content_len,
            "reasoning_len": self.reasoning_len,
        }


@dataclass(frozen=True)
class AuthorError:
    label: str
    error: str
    reason_code: str
    raw_llm_response: str | None
    # Per-packet observability — populated even when the call failed so
    # experiment reports can show "leg 3 failed at 22s after a 4096-token
    # length-cap loop" rather than just "leg 3 failed".
    wall_ms: int | None = None
    latency_ms: int | None = None
    finish_reason: str | None = None
    content_len: int | None = None
    reasoning_len: int | None = None
    provider_slug: str | None = None
    model_slug: str | None = None
    usage: dict[str, int] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label, "error": self.error,
            "reason_code": self.reason_code, "raw_llm_response": self.raw_llm_response,
            "wall_ms": self.wall_ms,
            "latency_ms": self.latency_ms,
            "finish_reason": self.finish_reason,
            "content_len": self.content_len,
            "reasoning_len": self.reasoning_len,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "usage": dict(self.usage) if self.usage else None,
        }


@dataclass(frozen=True)
class SectionSandbox:
    plan_field_schema: list[dict[str, Any]]
    stage_io: dict[str, dict[str, Any]] = field(default_factory=dict)
    integrations: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_field_schema": list(self.plan_field_schema),
            "stage_io": dict(self.stage_io),
            "integrations": list(self.integrations),
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


def _load_available_integrations(conn: Any) -> list[dict[str, Any]]:
    """Pull connected integration agents from `integration_registry` so the
    fork-out author can see them as alternatives to LLM agent slugs.

    Each integration becomes a deterministic agent the LLM can route to via
    `agent=integration/<id>/<action>`. Returns at most one row per id, with
    the action list flattened to {action, description} pairs.
    """
    out: list[dict[str, Any]] = []
    if conn is None:
        return out
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, description, capabilities
                FROM integration_registry
                WHERE auth_status = 'connected'
                ORDER BY id
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("integration registry lookup failed in section sandbox: %s", exc)
        return out
    for row in rows or []:
        if isinstance(row, dict):
            integration_id = str(row.get("id") or "")
            name = str(row.get("name") or "")
            description = str(row.get("description") or "")
            capabilities = row.get("capabilities") or []
        else:
            integration_id = str(row[0] or "")
            name = str(row[1] or "")
            description = str(row[2] or "")
            capabilities = row[3] or []
        if not integration_id:
            continue
        if isinstance(capabilities, str):
            try:
                capabilities = json.loads(capabilities)
            except json.JSONDecodeError:
                capabilities = []
        actions: list[dict[str, str]] = []
        if isinstance(capabilities, list):
            for cap in capabilities:
                if not isinstance(cap, dict):
                    continue
                action = str(cap.get("action") or "").strip()
                if not action:
                    continue
                actions.append({
                    "action": action,
                    "description": str(cap.get("description") or "").strip(),
                })
        if not actions:
            continue
        out.append({
            "id": integration_id,
            "name": name,
            "description": description,
            "actions": actions,
        })
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

    # Render registered integration agents so the LLM has visibility into the
    # deterministic-agent path. Without this, the only agents the LLM
    # considers are LLM-flavoured (auto/build, auto/refactor, ...). With it,
    # work like dedupe / validate / reconcile / redact can route to
    # integration/praxis_data/<action> for free, exact, receipt-backed
    # execution.
    integration_lines: list[str] = []
    for integration in sorted(sandbox.integrations, key=lambda i: i.get("id") or ""):
        integration_id = integration.get("id") or ""
        if not integration_id:
            continue
        name = integration.get("name") or integration_id
        description = (integration.get("description") or "").strip()
        integration_lines.append(f"  • {integration_id} — {name}")
        if description:
            integration_lines.append(f"      {description}")
        for action_entry in integration.get("actions") or []:
            action = action_entry.get("action") or ""
            action_desc = (action_entry.get("description") or "").strip()
            if not action:
                continue
            agent_slug = f"integration/{integration_id}/{action}"
            if action_desc:
                integration_lines.append(f"      - {agent_slug} — {action_desc}")
            else:
                integration_lines.append(f"      - {agent_slug}")

    sections: list[str] = [
        "ROLE:",
        (
            "You are authoring ONE PlanPacket inside a multi-step plan that the Praxis "
            "runtime will execute. Each packet you author is a JOB SPEC: at run time, "
            "the runtime resolves placeholders, picks an executor based on `task_type`, "
            "feeds it the rendered `prompt`, and lets it write only inside `write`. "
            "Bad packet = bad runtime behavior — be precise."
        ),
        "",
        "PROMPT (the operator intent — the WHOLE plan must serve this):",
        atoms.intent,
        "",
        "DATA PILLS (typed authority handles available to packets in this plan):",
        json.dumps(pills_view, indent=2, sort_keys=True),
        "",
        "DATA DICTIONARY (registered stages — consumes/produces are the FLOOR a packet of that stage MUST satisfy; you may NARROW produces to what THIS packet actually emits, never expand or drop floor types):",
        json.dumps(sandbox.stage_io, indent=2, sort_keys=True),
        "",
        "DATA DICTIONARY (PlanPacket fields — what each field means at runtime):",
        "\n".join(field_lines),
        "",
        "AVAILABLE INTEGRATION AGENTS (deterministic, non-LLM — prefer these for record-level data work, parsing, validation, dedupe, reconcile, redact, aggregate, join, merge, repair-loop, etc.; they are FREE, EXACT, and RECEIPT-BACKED — no tokens, no drift, fully replayable. Only use an LLM agent (auto/build, auto/refactor, ...) when the work genuinely needs inference or generation):",
        ("\n".join(integration_lines) if integration_lines else "  (none registered)"),
        "",
        "AUTHORING CONVENTIONS (the runtime enforces these — invented variations break execution):",
        (
            "  • Placeholder syntax — `{{name}}` (DOUBLE braces). The runtime engine\n"
            "    resolves `{{name}}` against the packet's `parameters` dict at run\n"
            "    time. Single-brace `{name}` is NOT a placeholder; it stays literal.\n"
            "    Dotted access works: `{{leads.count}}` resolves nested keys.\n"
            "    Defaults work: `{{key|default:fallback}}`.\n"
            "    Example:\n"
            "      \"parameters\": {\n"
            "        \"tickets\": \"{{fetch_tickets.output}}\",\n"
            "        \"severity_field\": \"issues.severity\"\n"
            "      },\n"
            "      \"prompt\": \"Classify {{tickets}} by {{severity_field}}…\"\n"
            "\n"
            "  • Pill references — pills are typed authority handles. Reference one\n"
            "    by storing its `ref` (e.g. `\"issues.severity\"`) as a STRING value\n"
            "    in `parameters` under a key the prompt then reads via `{{key}}`.\n"
            "    Don't try to template the pill ref itself — the runtime resolves\n"
            "    `parameters` first, then renders `prompt` with those values.\n"
            "\n"
            "  • Upstream output — `{{<upstream_label>.output}}` reads the JSON\n"
            "    output of an earlier packet you declared in `depends_on`. Don't\n"
            "    invent a `.output` for a packet you didn't depend on — the\n"
            "    placeholder will fail to resolve at runtime.\n"
            "\n"
            "  • `consumes` / `produces` — start from the stage floor; you may NARROW\n"
            "    `produces` to only the types this packet actually emits (e.g. a build\n"
            "    packet that only emits an execution_receipt should drop code_change_candidate/diff\n"
            "    if it doesn't really produce them); you may NOT drop a type the runtime\n"
            "    floor requires nor invent un-registered types. The downstream type-flow\n"
            "    gate JOINs producers→consumers by these arrays — over-claiming poisons\n"
            "    the type graph.\n"
            "\n"
            "  • `task_type` — the routing key the executor uses to pick an LLM/agent\n"
            "    via `task_type_routing`. Default to the stage name (`research`,\n"
            "    `review`, `build`, `fix`, `test`); pick a more specific registered\n"
            "    task_type only when one fits and you're sure it's registered.\n"
            "    `task_type` is NOT the same as `stage`.\n"
            "\n"
            "  • `agent` — `auto/<stage>` is fine and means \"runtime picks based on\n"
            "    task_type_routing.\" Only override with a specific agent slug if you\n"
            "    have a concrete reason (e.g. operator pinned a particular composer).\n"
            "\n"
            "  • Integration agents (deterministic, non-LLM) — when this packet's\n"
            "    work matches an entry in AVAILABLE INTEGRATION AGENTS, set\n"
            "    `agent=integration/<id>/<action>` AND emit three sibling fields\n"
            "    so the runtime executes via the integration handler instead of\n"
            "    spawning an LLM:\n"
            "      \"agent\": \"integration/praxis_data/dedupe\",\n"
            "      \"integration_id\": \"praxis_data\",\n"
            "      \"integration_action\": \"dedupe\",\n"
            "      \"integration_args\": { /* op-specific args, e.g. */\n"
            "        \"input_path\": \"artifacts/data/users.csv\",\n"
            "        \"keys\": [\"email\"]\n"
            "      }\n"
            "    Capability hints with prefix `data_op` in atoms.step_types are\n"
            "    a strong signal to use praxis_data. Do not invent integration\n"
            "    ids or actions outside AVAILABLE INTEGRATION AGENTS.\n"
            "\n"
            "  • `gates` — list each gate by `gate_id`. The required gates per stage\n"
            "    are listed in DATA DICTIONARY (registered stages). For\n"
            "    `type_flow_gate`, `params={}` is correct — the gate reads\n"
            "    consumes/produces from the packet itself; do NOT invent param schemas.\n"
            "    For `write_scope_gate`, `params={}` similarly — the gate reads `write`.\n"
            "    Other gates: emit `params={}` unless you know the param contract.\n"
            "\n"
            "  • `write` — packet-relative paths under `artifacts/<stage>/<label>/`\n"
            "    by convention. Never the workspace root (`.`, `./`, `,`).\n"
            "\n"
            "  • `pill_audit_local` — for EACH pill in DATA PILLS that you actually\n"
            "    referenced (either by storing its ref as a parameter value or\n"
            "    citing it in the prompt body), emit\n"
            "    `{ref, verdict: confirmed|misattributed, reason}`. Reject\n"
            "    (misattributed) when a pill matched on prose-similarity but isn't\n"
            "    semantically right for THIS packet — e.g., a `capability_catalog.route`\n"
            "    surfaced for a Slack-routing intent is misattributed (route here\n"
            "    refers to model routing, not message dispatch). Only audit pills you\n"
            "    considered; an empty list means you didn't reference any."
        ),
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
        integrations=_load_available_integrations(conn),
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
    wall_ms: int | None = None,
    latency_ms: int | None = None,
    finish_reason: str | None = None,
    content_len: int | None = None,
    reasoning_len: int | None = None,
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

    # Deterministic write-scope floor (2026-04-26). The validator rejects
    # empty write AND rejects workspace-root scopes (",", ".", "./", []).
    # When LLM omits write OR emits only workspace-root markers, fall back
    # to artifacts/<stage>/<label>/ — stage-and-label scoped, not workspace-
    # root, so both validator gates pass without an LLM round-trip.
    _WORKSPACE_ROOT_TOKENS = {",", ".", "./"}
    write_raw = parsed.get("write")
    write = [str(item) for item in write_raw] if isinstance(write_raw, list) else []
    write = [w for w in write if w and w not in _WORKSPACE_ROOT_TOKENS]
    if not write:
        write = [f"artifacts/{target.stage}/{target.label}/"]

    # Same pattern for parameters — required field, falls back to a stage-
    # scoped scaffold when LLM omits or only emits empty marks.
    params_raw = parsed.get("parameters")
    if isinstance(params_raw, dict) and params_raw:
        parameters = dict(params_raw)
    else:
        parameters = {"stage": target.stage, "label": target.label}

    agent_slug = str(parsed.get("agent") or f"auto/{target.stage}")
    integration_id_raw = parsed.get("integration_id")
    integration_action_raw = parsed.get("integration_action")
    integration_args_raw = parsed.get("integration_args")
    integration_id: str | None = None
    integration_action: str | None = None
    integration_args: dict[str, Any] = {}
    if isinstance(integration_id_raw, str) and integration_id_raw.strip():
        integration_id = integration_id_raw.strip()
    if isinstance(integration_action_raw, str) and integration_action_raw.strip():
        integration_action = integration_action_raw.strip()
    if isinstance(integration_args_raw, dict):
        integration_args = dict(integration_args_raw)
    # When the agent slug declares integration shape but the LLM omitted the
    # sibling fields, derive them from the slug. This catches "agent emitted
    # but integration_* dropped" failure modes without forcing a re-author.
    if (
        agent_slug.startswith("integration/")
        and (integration_id is None or integration_action is None)
    ):
        parts = agent_slug.split("/", 2)
        if len(parts) == 3:
            if integration_id is None:
                integration_id = parts[1] or None
            if integration_action is None:
                integration_action = parts[2] or None

    return AuthoredPacket(
        label=target.label, stage=target.stage,
        description=str(parsed.get("description") or target.description),
        prompt=str(parsed.get("prompt") or ""),
        write=write,
        agent=agent_slug,
        task_type=str(parsed.get("task_type") or target.stage),
        capabilities=capabilities, consumes=consumes, produces=produces,
        depends_on=list(target.depends_on), gates=gates_out,
        parameters=parameters,
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
        integration_id=integration_id,
        integration_action=integration_action,
        integration_args=integration_args,
        wall_ms=wall_ms,
        latency_ms=latency_ms,
        finish_reason=finish_reason,
        content_len=content_len,
        reasoning_len=reasoning_len,
    )


def _resolve_routes_for_task_type(task_type: str) -> list[tuple[str, str]]:
    """Return matrix-gated routes for a task_type, ordered by rank."""
    from runtime.compiler_llm import resolve_matrix_gated_routes, _resolve_app_compile_routes

    routes = resolve_matrix_gated_routes(task_type)
    if routes:
        return routes
    return _resolve_app_compile_routes()


def _resolve_section_author_routes() -> list[tuple[str, str]]:
    """Legacy alias — returns plan_section_author rows."""
    return _resolve_routes_for_task_type("plan_section_author")
