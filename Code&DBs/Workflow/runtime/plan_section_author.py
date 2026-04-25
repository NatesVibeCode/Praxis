"""Layer 4 (Author): per-section parallel LLM filling of skeletal packets.

The synthesizer (Layer 0.5) hands us a SkeletalPlan with depends_on /
consumes / produces / capabilities / gate scaffolds already wired
deterministically. Each packet is missing the menu-level fields a Moon
operator would otherwise have to fill: prompt, write_scope, agent,
gate parameters, parameter bindings, on_failure / on_success policy,
timeout, budget.

This layer fans out one LLM call per packet (concurrency-capped) using
the same provider-resolution as ``runtime.compiler_llm.call_llm_compile``
(``task_type_routing`` row for ``auto/compile``, DeepSeek-via-OpenRouter
under current operator standing orders). Each call sees:

  - the full atoms (pills, parameters)
  - its own packet skeleton + one-line summaries of upstream / downstream
  - the data-dictionary plan_field row schema (target shape)
  - the available tool / capability / gate catalog
  - the standing orders relevant to compile (provider routing)

and returns one ``AuthoredPacket`` JSON with every required plan_field
populated — no TBD, no 'auto' unless the catalog admits it, no empty
arrays where a value is required.

Honest scope: never picks cross-packet structure (depends_on,
consumes/produces direction, gate kinds). Only fills inside one packet.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPacket, SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms

logger = logging.getLogger(__name__)


_DEFAULT_CONCURRENCY = 4
_AUTHOR_TASK_TYPE = "compile"


def _section_author_timeout_seconds() -> float:
    """Per-section/cluster author HTTP timeout. Larger than the legacy compile
    timeout because the section prompt carries the data-dictionary sandbox +
    neighbors, and a cluster prompt may carry many sibling packets at once.
    DeepSeek wall-clock for a single packet is 20-50s on a warm route; a
    12-packet cluster needs 2-3x that. 180s default."""
    raw = os.environ.get("WORKFLOW_SECTION_AUTHOR_TIMEOUT_S", "").strip()
    if not raw:
        return 180.0
    try:
        value = float(raw)
    except ValueError:
        return 180.0
    return max(10.0, value)


# Provider-routing standing order text shipped in the sandbox.
_PROVIDER_ROUTING_STANDING_ORDER = (
    "CLI is the provider execution lane for every use case, always. API is "
    "opt-in only. Always label provider-routing discussion as CLI or API. "
    "Direct DeepSeek API is research-only; OpenRouter route is UI-compile-only. "
    "Anthropic access is CLI-only (no direct anthropic API). Gemini 2.5 (any "
    "variant) is forbidden for real work."
)


@dataclass(frozen=True)
class SectionSandbox:
    """Compact per-call context. Built once in the orchestrator, shared across calls."""

    tools: list[dict[str, Any]]
    skills: list[dict[str, str]]
    stage_io: dict[str, dict[str, Any]]
    standing_orders: str
    plan_field_schema: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "tools": list(self.tools),
            "skills": list(self.skills),
            "stage_io": dict(self.stage_io),
            "standing_orders": self.standing_orders,
            "plan_field_schema": list(self.plan_field_schema),
        }


def build_section_sandbox(conn: Any) -> SectionSandbox:
    """Load tools / skills / stage I/O / standing orders / plan_field schema once."""
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds

    tools: list[dict[str, Any]] = []
    try:
        for row in list_object_kinds(conn, category="tool"):
            kind = str(row.get("object_kind") or "")
            if kind.startswith("tool:praxis_"):
                tools.append(
                    {
                        "name": kind.split(":", 1)[1],
                        "summary": (row.get("summary") or "")[:160],
                    }
                )
    except DataDictionaryBoundaryError:
        pass

    stage_io: dict[str, dict[str, Any]] = {}
    try:
        for row in list_object_kinds(conn, category="stage"):
            kind = str(row.get("object_kind") or "")
            stage_name = kind.split(":", 1)[1] if ":" in kind else kind
            metadata = row.get("metadata") or {}
            stage_io[stage_name] = {
                "produces": list(metadata.get("produces") or []),
                "consumes": list(metadata.get("consumes") or []),
                "required_gates": list(metadata.get("required_gates") or []),
                "capabilities": list(metadata.get("capabilities") or []),
            }
    except DataDictionaryBoundaryError:
        pass

    skills = _scan_repo_skills()
    plan_field_schema = _load_plan_field_schema(conn)

    return SectionSandbox(
        tools=tools,
        skills=skills,
        stage_io=stage_io,
        standing_orders=_PROVIDER_ROUTING_STANDING_ORDER,
        plan_field_schema=plan_field_schema,
    )


def _scan_repo_skills(skills_root: str = "Skills") -> list[dict[str, str]]:
    """Scan repo Skills/<name>/SKILL.md files; return slim {name, summary} entries."""
    import pathlib

    out: list[dict[str, str]] = []
    root = pathlib.Path(skills_root)
    if not root.exists():
        return out
    for skill_dir in sorted(root.iterdir()):
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            continue
        try:
            text = skill_md.read_text(encoding="utf-8")
        except OSError:
            continue
        summary = ""
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("---") or stripped.startswith("#"):
                continue
            if stripped.startswith("description:"):
                summary = stripped.split(":", 1)[1].strip().strip("'\"")
                break
            summary = stripped
            break
        out.append({"name": skill_dir.name, "summary": summary[:200]})
    return out


@dataclass(frozen=True)
class AuthoredPacket:
    """One packet with every menu-level field filled."""

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

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "stage": self.stage,
            "description": self.description,
            "prompt": self.prompt,
            "write": list(self.write),
            "agent": self.agent,
            "task_type": self.task_type,
            "capabilities": list(self.capabilities),
            "consumes": list(self.consumes),
            "produces": list(self.produces),
            "depends_on": list(self.depends_on),
            "gates": [dict(gate) for gate in self.gates],
            "parameters": dict(self.parameters),
            "workdir": self.workdir,
            "on_failure": self.on_failure,
            "on_success": self.on_success,
            "timeout": self.timeout,
            "budget": dict(self.budget) if self.budget else None,
            "raw_llm_response": self.raw_llm_response,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
        }


@dataclass(frozen=True)
class AuthorError:
    """A packet that failed authoring; surfaces the error for retry or operator review."""

    label: str
    error: str
    reason_code: str
    raw_llm_response: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "error": self.error,
            "reason_code": self.reason_code,
            "raw_llm_response": self.raw_llm_response,
        }


@dataclass(frozen=True)
class AuthoredPlan:
    """Output of the parallel author fan-out."""

    packets: list[AuthoredPacket]
    errors: list[AuthorError]
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "packets": [p.to_dict() for p in self.packets],
            "errors": [e.to_dict() for e in self.errors],
            "notes": list(self.notes),
        }


def _load_plan_field_schema(conn: Any) -> list[dict[str, Any]]:
    """Read plan_field rows ordered by metadata.order."""
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
        out.append(
            {
                "field": field_name,
                "label": row.get("label"),
                "summary": row.get("summary"),
                "metadata": metadata,
            }
        )
    out.sort(key=lambda entry: (entry["metadata"].get("order") or 999, entry["field"]))
    return out


def _build_section_prompt(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    target: SkeletalPacket,
    sandbox: SectionSandbox,
) -> str:
    """Lean per-section prompt. Carries only what the LLM needs to author one packet."""

    target_view = {
        "label": target.label,
        "stage": target.stage,
        "description": target.description,
        "consumes_floor": list(target.consumes_floor),
        "produces_floor": list(target.produces_floor),
        "capabilities_floor": list(target.capabilities_floor),
        "depends_on": list(target.depends_on),
        "scaffolded_gate_ids": [gate.gate_id for gate in target.gates_scaffold],
    }
    upstream = [
        {"label": p.label, "stage": p.stage}
        for p in skeleton.packets
        if p.label in target.depends_on
    ]
    downstream = [
        {"label": p.label, "stage": p.stage}
        for p in skeleton.packets
        if target.label in p.depends_on
    ]

    pills_view = [
        {
            "ref": pill.ref,
            "summary": (pill.description or "")[:140],
            "score": round(pill.score, 1),
        }
        for pill in atoms.pills.suggested[:6]
    ]
    parameters_view = [
        {"name": p.name, "type_hint": p.type_hint} for p in atoms.parameters
    ]

    field_lines: list[str] = []
    for entry in sandbox.plan_field_schema:
        meta = entry.get("metadata", {})
        flags: list[str] = []
        if meta.get("required"):
            flags.append("REQUIRED")
        if meta.get("type"):
            flags.append(f"type={meta['type']}")
        if meta.get("picker_source"):
            flags.append(f"pick={meta['picker_source']}")
        if meta.get("floor_from"):
            flags.append(f"floor={meta['floor_from']}")
        if meta.get("forbid_workspace_root"):
            flags.append("no-workspace-root")
        if meta.get("forbid_placeholders"):
            flags.append(f"no-placeholders={meta['forbid_placeholders']}")
        field_lines.append(
            f"  - {entry['field']}: {entry['summary']} [{', '.join(flags)}]"
        )

    catalog_view = {
        "tools": [tool["name"] for tool in sandbox.tools[:30]],
        "skills": [
            f"{skill['name']}: {skill['summary'][:120]}" for skill in sandbox.skills[:20]
        ],
        "stage_io": sandbox.stage_io,
    }

    sections: list[str] = [
        f"TASK: Fill section '{target.label}' of a workflow plan.",
        "",
        "Cross-packet structure (depends_on, gate ids, consumes/produces floor) is "
        "already wired by the deterministic synthesizer. Keep what is given. You "
        "fill the menu-level fields inside this one packet.",
        "",
        "SOURCE PROSE:",
        atoms.intent,
        "",
        "YOUR PACKET:",
        json.dumps(target_view, indent=2),
        "",
        f"NEIGHBORS:  upstream={upstream}  downstream={downstream}",
        "",
        "PILLS YOU MAY REFERENCE (top 6 from data dictionary):",
        json.dumps(pills_view, indent=2),
        "",
        "WORKFLOW PARAMETERS:",
        json.dumps(parameters_view, indent=2),
        "",
        "FIELDS TO FILL (every REQUIRED field must be set):",
        "\n".join(field_lines),
        "",
        "CATALOG (sandbox):",
        json.dumps(catalog_view, indent=2),
        "",
        f"STANDING ORDER (provider routing): {sandbox.standing_orders}",
        "",
        "RULES:",
        "  - Reference parameters by {name} (e.g. {app_name}).",
        "  - Reference upstream outputs by packet label.",
        "  - Never write TBD / TODO / FIXME / 'auto' (unless 'pick' admits 'auto').",
        "  - write must be precise globs; never ['.'] (workspace-root is forbidden).",
        "  - Keep depends_on as given; the synthesizer wired it.",
        "  - capabilities / consumes / produces begin at the floor; you may add "
        "but never drop floor entries.",
        "  - gates MUST be a list of OBJECTS, one per scaffolded gate id. "
        "Required shape per entry: {\"gate_id\": \"<one of scaffolded_gate_ids "
        "above>\", \"params\": {<gate-specific keys, derived from THIS packet's "
        "intent — never copy placeholder values>}}. Bare strings are not "
        "acceptable. If a gate has no real params for this packet, emit an "
        "empty params dict: {\"gate_id\":\"<id>\",\"params\":{}}. Never "
        "fabricate ref ids you did not see in the skeleton or pills.",
        "  - Prefer auto/<task_type> for agent unless catalog forces otherwise.",
        "",
        "OUTPUT: a single PlanPacket JSON object. No markdown fences. No prose "
        "outside JSON. No commentary.",
    ]
    return "\n".join(sections)


def _coerce_packet_response(
    *,
    target: SkeletalPacket,
    parsed: dict[str, Any],
    raw: str,
    provider_slug: str,
    model_slug: str,
) -> AuthoredPacket:
    """Apply skeleton floors over the LLM output (LLM may not drop floor values)."""

    def floor_union(field_name: str, llm_value: Any, floor: list[str]) -> list[str]:
        out = list(floor)
        if isinstance(llm_value, list):
            for item in llm_value:
                if isinstance(item, str) and item not in out:
                    out.append(item)
        return out

    consumes = floor_union("consumes", parsed.get("consumes"), target.consumes_floor)
    produces = floor_union("produces", parsed.get("produces"), target.produces_floor)
    capabilities = floor_union("capabilities", parsed.get("capabilities"), target.capabilities_floor)

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
                # LLM degraded gates to bare strings; rehydrate to object form.
                gates_out.append({"gate_id": gate, "params": {}})
                seen.add(gate)
    for gate in target.gates_scaffold:
        if gate.gate_id in seen:
            continue
        gates_out.append(gate.to_dict())

    write = parsed.get("write")
    if not isinstance(write, list) or not write:
        write = []
    elif write == ["."] or write == ["./"]:
        write = []  # forbid workspace-root; downstream validator surfaces

    return AuthoredPacket(
        label=target.label,
        stage=target.stage,
        description=str(parsed.get("description") or target.description),
        prompt=str(parsed.get("prompt") or ""),
        write=[str(item) for item in write],
        agent=str(parsed.get("agent") or f"auto/{target.stage}"),
        task_type=str(parsed.get("task_type") or target.stage),
        capabilities=capabilities,
        consumes=consumes,
        produces=produces,
        depends_on=list(target.depends_on),
        gates=gates_out,
        parameters=dict(parsed.get("parameters") or {}),
        workdir=parsed.get("workdir"),
        on_failure=str(parsed.get("on_failure") or "abort"),
        on_success=str(parsed.get("on_success") or "continue"),
        timeout=int(parsed.get("timeout") or 300),
        budget=dict(parsed["budget"]) if isinstance(parsed.get("budget"), dict) else None,
        raw_llm_response=raw,
        provider_slug=provider_slug,
        model_slug=model_slug,
    )


def _strip_json_fences(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _resolve_section_author_routes() -> list[tuple[str, str]]:
    """Return route list for the per-section author task_type.

    Reads task_type_routing rows for ``plan_section_author`` (registered by
    migration 249) ordered by rank. Falls back to the compile route list
    when no rows exist (fresh-clone bootstrap path).
    """
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    pool = get_workflow_pool()
    pg = SyncPostgresConnection(pool)
    try:
        rows = pg.fetch(
            """
            SELECT provider_slug, model_slug
              FROM task_type_routing
             WHERE task_type = 'plan_section_author'
               AND permitted = true
               AND route_source = 'explicit'
             ORDER BY rank ASC, updated_at DESC
            """
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


def _call_section_llm(prompt: str, *, hydrate_env: Any | None = None) -> tuple[str, str, str]:
    """Return (raw_content, provider_slug, model_slug) using the section-author route."""
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )

    if hydrate_env is not None:
        hydrate_env()

    timeout = int(_section_author_timeout_seconds())
    last_error: Exception | None = None
    for provider_slug, model_slug in _resolve_section_author_routes():
        try:
            endpoint = resolve_api_endpoint(provider_slug, model_slug)
            if not endpoint:
                raise RuntimeError(f"no registered endpoint for {provider_slug}/{model_slug}")
            protocol_family = resolve_api_protocol_family(provider_slug)
            if not protocol_family:
                raise RuntimeError(f"no protocol_family for {provider_slug}")

            env = dict(os.environ)
            api_key: str | None = None
            for env_var in resolve_api_key_env_vars(provider_slug):
                candidate = resolve_secret(env_var, env=env)
                if candidate and candidate.strip():
                    api_key = candidate.strip()
                    break
            if not api_key:
                raise RuntimeError(f"no API key for {provider_slug} (Keychain + env both empty)")

            request = LLMRequest(
                endpoint_uri=str(endpoint),
                api_key=api_key,
                provider_slug=provider_slug,
                model_slug=model_slug,
                messages=({"role": "user", "content": prompt},),
                protocol_family=str(protocol_family),
                timeout_seconds=timeout,
                retry_attempts=0,
            )
            response = call_llm(request)
            return response.content, provider_slug, model_slug
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Section-author route failed for %s/%s: %s",
                provider_slug,
                model_slug,
                exc,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError("task_type_routing returned no llm_task routes for compile")


def author_plan_section(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    target: SkeletalPacket,
    sandbox: SectionSandbox,
    hydrate_env: Any | None = None,
) -> AuthoredPacket | AuthorError:
    """Author one packet section. Returns AuthoredPacket on success, AuthorError on failure."""
    prompt = _build_section_prompt(
        atoms=atoms,
        skeleton=skeleton,
        target=target,
        sandbox=sandbox,
    )
    try:
        raw, provider_slug, model_slug = _call_section_llm(prompt, hydrate_env=hydrate_env)
    except Exception as exc:
        return AuthorError(
            label=target.label,
            error=str(exc),
            reason_code="section_author.llm_call_failed",
            raw_llm_response=None,
        )

    try:
        parsed = json.loads(_strip_json_fences(raw))
    except json.JSONDecodeError as exc:
        return AuthorError(
            label=target.label,
            error=f"section author returned non-JSON: {exc}",
            reason_code="section_author.parse_failed",
            raw_llm_response=raw,
        )
    if not isinstance(parsed, dict):
        return AuthorError(
            label=target.label,
            error=f"section author returned {type(parsed).__name__}, expected object",
            reason_code="section_author.shape_invalid",
            raw_llm_response=raw,
        )

    return _coerce_packet_response(
        target=target,
        parsed=parsed,
        raw=raw,
        provider_slug=provider_slug,
        model_slug=model_slug,
    )


def author_plan_sections_parallel(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    conn: Any,
    concurrency: int = _DEFAULT_CONCURRENCY,
    hydrate_env: Any | None = None,
) -> AuthoredPlan:
    """Fan out per-packet authoring across N workers; return merged AuthoredPlan."""
    sandbox = build_section_sandbox(conn)
    notes: list[str] = []
    if not sandbox.plan_field_schema:
        notes.append(
            "no plan_field rows registered in the data dictionary "
            "(category='plan_field'); apply migration 247"
        )
    if not skeleton.packets:
        notes.append("skeleton has no packets — nothing to author")
        return AuthoredPlan(packets=[], errors=[], notes=notes)

    successes: list[AuthoredPacket] = []
    failures: list[AuthorError] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = {
            pool.submit(
                author_plan_section,
                atoms=atoms,
                skeleton=skeleton,
                target=target,
                sandbox=sandbox,
                hydrate_env=hydrate_env,
            ): target.label
            for target in skeleton.packets
        }
        for future in concurrent.futures.as_completed(futures):
            label = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                failures.append(
                    AuthorError(
                        label=label,
                        error=f"{type(exc).__name__}: {exc}",
                        reason_code="section_author.unhandled",
                        raw_llm_response=None,
                    )
                )
                continue
            if isinstance(result, AuthorError):
                failures.append(result)
            else:
                successes.append(result)

    successes.sort(key=lambda p: [pp.label for pp in skeleton.packets].index(p.label))
    return AuthoredPlan(packets=successes, errors=failures, notes=notes)
