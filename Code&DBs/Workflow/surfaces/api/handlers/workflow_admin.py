"""Administrative handlers for the workflow HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from runtime.dependency_contract import dependency_truth_report
from runtime.engineering_observability import (
    build_bug_scoreboard,
    build_code_hotspots,
    build_platform_observability,
)
from storage.postgres.connection import resolve_workflow_database_url
from surfaces.api import operator_read, operator_write
from surfaces.api.handlers import workflow_launcher
from surfaces.mcp.catalog import get_tool_catalog

from ._shared import (
    REPO_ROOT,
    RouteEntry,
    _ClientError,
    _exact,
    _query_params,
    _read_json_body,
    _serialize,
)
from .workflow_run import _handle_status


def _tool_definition(tool_name: str):
    definition = get_tool_catalog().get(tool_name)
    if definition is None:
        raise KeyError(f"unknown MCP tool: {tool_name}")
    return definition


def _tool_surface_hint(
    tool_name: str,
    *,
    http_hint: str | None = None,
    suffix: str | None = None,
) -> str:
    definition = _tool_definition(tool_name)
    parts = [
        f"CLI `{definition.cli_entrypoint}`",
        f"MCP `{definition.name}`",
    ]
    if http_hint:
        parts.append(f"HTTP {http_hint}")
    text = f"Use {'; '.join(parts)}. {definition.cli_when_to_use}"
    if suffix:
        text += f" {suffix}"
    text += f" Schema/help: `{definition.cli_describe_command}`."
    return text


def _cli_surface_hint(
    command: str,
    *,
    description: str,
    suffix: str | None = None,
) -> str:
    text = f"Use CLI `{command}`. {description}"
    if suffix:
        text += f" {suffix}"
    return text


def _handle_orient(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    """Return everything an agent needs to start operating."""

    endpoints = {
        "/orient": "Full orientation context for cold-start agents",
        "/mcp": "Bounded MCP JSON-RPC bridge for Praxis Engine workflow jobs and other HTTP clients",
        "/workflow-runs": "Run or dry-run a workflow spec",
        "/workflow-validate": "Validate a workflow spec without running",
        "/status": "Recent workflow status from receipts",
        "/query": "Removed wrapper surface; use praxis workflow query/discover/recall/tools instead",
        "/bugs": "File, list, search, or resolve bugs",
        "/health": "Preflight probes + operator snapshot",
        "/recall": "Search the knowledge graph",
        "/ingest": "Ingest content into the knowledge graph",
        "/graph": "Entity neighbors and blast radius",
        "/wave": "Workflow wave operations (observe/start/next/record)",
        "/receipts": "Search canonical workflow receipts + token burn analytics",
        "/constraints": "Mined failure constraints (list or scope-match)",
        "/friction": "Friction ledger (guardrail bounces, warnings, stats)",
        "/heal": "Failure diagnosis + recovery recommendations",
        "/artifacts": "Sandbox artifact store (list/search/diff/stats)",
        "/governance": "Secret scanning + scope compliance checks",
        "/heartbeat": "Memory maintenance cycle + latest results",
        "/session": "Session carry-forward packs (latest/validate)",
        "/decompose": "Sprint decomposition (objective→micro-sprints)",
        "/research": "Research sessions (search local knowledge)",
        "/operator_view": "Observability views (operator status/scoreboard/graph/issue backlog)",
        "/api/operator/task-route-eligibility": "Write a timed provider/model route eligibility window",
        "/api/operator/transport-support": "Read provider/model transport support before run time",
        "/api/operator/native-primary-cutover-gate": "Admit a native-primary cutover gate through operator control",
        "/api/operator/roadmap-write": "Preview, validate, or commit roadmap rows through one shared validation gate",
        "/api/operator/work-item-closeout": "Preview or commit proof-backed bug and roadmap closeout through one shared reconciliation gate",
        "/api/operator/roadmap-view": "Read one roadmap subtree and its dependency edges from DB-backed authority",
        "/api/operator/provider-onboarding": "Seed a provider profile, model catalog rows, benchmark metadata, and verification in one wizard",
    }

    health_payload = _handle_health(subs, {})
    dependency_truth = dependency_truth_report(scope="all")

    recent_activity: dict[str, Any]
    try:
        ingester = subs.get_receipt_ingester()
        receipts = ingester.load_recent(since_hours=24)
        pass_rate = ingester.compute_pass_rate(receipts)
        top_failures = ingester.top_failure_codes(receipts)
        recent_activity = {
            "total_workflows_24h": len(receipts),
            "pass_rate": round(pass_rate, 4),
            "top_failure_codes": top_failures,
        }
    except Exception as exc:
        recent_activity = {"error": f"Could not load receipts: {exc}"}

    try:
        bug_tracker = subs.get_bug_tracker()
    except Exception:
        bug_tracker = None

    engineering_observability = {
        "code_hotspots": build_code_hotspots(
            repo_root=REPO_ROOT,
            bug_tracker=bug_tracker,
            roots=("runtime", "surfaces/api", "surfaces/cli"),
            limit=10,
        ),
        "bug_scoreboard": build_bug_scoreboard(
            bug_tracker=bug_tracker,
            repo_root=REPO_ROOT,
            limit=10,
        ),
        "platform_observability": build_platform_observability(
            platform_payload=health_payload,
        ),
    }

    discover_tool = _tool_definition("praxis_discover")
    recall_tool = _tool_definition("praxis_recall")
    query_tool = _tool_definition("praxis_query")
    workflow_tool = _tool_definition("praxis_workflow")
    health_tool = _tool_definition("praxis_health")
    bugs_tool = _tool_definition("praxis_bugs")
    tool_count = len(get_tool_catalog())

    return {
        "platform": "praxis-workflow",
        "brand": "Praxis Engine",
        "version": "1.0.0",
        "instruction_authority": {
            "kind": "orient_instruction_authority",
            "authority": "surfaces.api.handlers.workflow_admin._handle_orient",
            "lane": "native_operator",
            "packet_read_order": [
                "roadmap_truth",
                "queue_refs",
                "current_state_notes",
                "health",
                "recent_activity",
            ],
            "downstream_truth_surfaces": {
                "roadmap_truth": "/api/operator/roadmap-view",
                "queue_refs_and_current_state_notes": (
                    "surfaces.api.native_operator_surface.query_native_operator_surface"
                ),
                "run_status": "/api/workflow-runs/{run_id}/status",
            },
            "directive": (
                "Treat /orient as the canonical instruction authority for this lane. "
                "Downstream packets should read roadmap-backed truth, queue refs, and current-state "
                "notes before using repo files or prior chat state."
            ),
        },
        "capabilities": [
            "workflow_runs",
            "workflow_validate",
            "status",
            "query",
            "bugs",
            "health",
            "recall",
            "ingest",
            "graph",
            "wave",
            "receipts",
            "constraints",
            "friction",
            "heal",
            "artifacts",
            "governance",
            "heartbeat",
            "session",
            "decompose",
            "research",
            "operator_view",
            "provider_onboarding",
        ],
        "endpoints": endpoints,
        "status": health_payload.get("operator_snapshot"),
        "health": health_payload.get("preflight"),
        "proof_metrics": health_payload.get("proof_metrics"),
        "schema_authority": health_payload.get("schema_authority"),
        "lane_recommendation": health_payload.get("lane_recommendation"),
        "dependency_truth": dependency_truth,
        "recent_activity": recent_activity,
        "engineering_observability": engineering_observability,
        "search_surfaces": {
            "architecture_scan": _cli_surface_hint(
                "workflow architecture scan",
                description=(
                    "Exact static architecture scan for raw SQL literals in front-door modules and "
                    "front-door imports reaching into `runtime.*` or `storage.postgres.*`."
                ),
                suffix="Use this before fuzzy retrieval when you need proof of boundary drift, not semantic candidates.",
            ),
            "code_discovery": _tool_surface_hint(
                "praxis_discover",
                http_hint="`/query` with `find <term>`",
                suffix=(
                    "This is hybrid retrieval, not vector-only: AST fingerprint vectors plus Postgres full-text "
                    "search fused into one ranking. Always search here before building new code. Reindex after code changes with "
                    "`workflow discover reindex --yes` or `praxis_discover(action='reindex')`."
                ),
            ),
            "knowledge_graph": _tool_surface_hint(
                "praxis_recall",
                http_hint="`/recall`",
                suffix="Use it for decisions, patterns, and prior context. Results may come from text match, graph traversal, or vector similarity, not code similarity.",
            ),
            "bugs": _tool_surface_hint(
                "praxis_bugs",
                http_hint="`/bugs` with `action=search`",
                suffix="Search is backed by Postgres FTS and may blend in vector ranking when the embedding lane is available. Prefer read/search before filing or resolving so duplicates stay down.",
            ),
            "receipts": _tool_surface_hint(
                "praxis_receipts",
                http_hint="`/receipts`",
                suffix="Use it for canonical workflow evidence, exact-ish receipt search, and token-burn analysis.",
            ),
        },
        "cli_surface": {
            "kind": "catalog_backed_cli",
            "preferred": True,
            "tool_count": tool_count,
            "directive": (
                "Prefer the catalog-backed `workflow` CLI as the default operator surface. "
                "Use discovery commands first, then switch to direct aliases for day-to-day reads."
            ),
            "discovery_commands": [
                {
                    "command": "workflow tools list",
                    "description": "List the full catalog-backed CLI surface with tier, risk, and recommended alias data.",
                    "examples": [
                        "workflow tools list",
                        "workflow tools list --surface query --json",
                    ],
                },
                {
                    "command": "workflow tools search <text>",
                    "description": "Search the tool catalog by intent, noun, or task before guessing a command name. Add `--exact` when you already know the alias, tool name, or entrypoint.",
                    "examples": [
                        "workflow tools search failure",
                        "workflow tools search roadmap",
                        "workflow tools search architecture",
                        "workflow tools search workflow query --exact",
                    ],
                },
                {
                    "command": "workflow architecture scan",
                    "description": "Run an exact static architecture scan for raw SQL drift and front-door imports that reach into runtime or storage authority.",
                    "examples": [
                        "workflow architecture scan",
                        "workflow architecture scan --scope surfaces --json",
                    ],
                },
                {
                    "command": "workflow tools describe <tool|alias>",
                    "description": "Inspect one tool's schema, risk, badges, and example payloads before calling it.",
                    "examples": [
                        "workflow tools describe praxis_query",
                        "workflow tools describe praxis_workflow",
                    ],
                },
                {
                    "command": "workflow tools call <tool|alias> --input-json '{...}'",
                    "description": "Use the generic direct-call surface when no friendly alias fits or when you want exact schema control.",
                    "examples": [
                        "workflow tools call praxis_query --input-json '{\"question\":\"what is failing right now?\"}'",
                        "workflow tools call praxis_health --input-json '{}'",
                    ],
                },
            ],
            "recommended_reads": [
                {
                    "tool": query_tool.name,
                    "command": query_tool.cli_entrypoint,
                    "description": "Best first stop for natural-language questions when you are not yet sure which specialist surface you need.",
                    "examples": [
                        "workflow query 'what is failing right now?'",
                        "workflow query 'which runs are stuck?'",
                        "workflow query 'show me quality metrics'",
                    ],
                },
                {
                    "tool": health_tool.name,
                    "command": health_tool.cli_entrypoint,
                    "description": "Run the preflight and operator snapshot when the platform feels degraded or before dispatching work.",
                    "examples": [
                        "workflow health",
                        "workflow health --json",
                    ],
                },
                {
                    "tool": discover_tool.name,
                    "command": discover_tool.cli_entrypoint,
                    "description": "Search for existing code by behavior before adding new functions, modules, or patterns. Uses hybrid ranking, not vector-only similarity.",
                    "examples": [
                        "workflow discover 'retry logic with exponential backoff'",
                        "workflow discover 'Postgres connection pooling' --kind function",
                        "workflow discover 'parse JSON from stdin' --kind function --limit 5",
                        "workflow discover stats",
                    ],
                },
                {
                    "tool": recall_tool.name,
                    "command": recall_tool.cli_entrypoint,
                    "description": "Search the knowledge graph for decisions, patterns, and prior context instead of code similarity. Ranking can come from text, graph, or vector retrieval.",
                    "examples": [
                        "workflow recall 'provider routing' --type decision",
                        "workflow recall 'dispatch run completion trigger retirement'",
                        "workflow recall 'workflow_runs' --type table --limit 5",
                    ],
                },
                {
                    "tool": bugs_tool.name,
                    "command": bugs_tool.cli_entrypoint,
                    "description": "Inspect bug state before filing or resolving so you reuse existing evidence and avoid duplicates.",
                    "examples": [
                        "workflow bugs list --limit 10",
                        "workflow bugs search routing",
                        "workflow bugs stats",
                    ],
                },
            ],
            "guardrails": [
                "Write and dispatch tools require explicit confirmation (`--yes`).",
                "Session tools require a workflow token and only work inside an active workflow MCP session.",
                "Workflow launches are kickoff-first: run once, then inspect status/stream separately.",
            ],
        },
        "instructions": (
            "You are operating the Praxis Engine autonomous engineering control plane.\n"
            "Prefer the catalog-backed `workflow` CLI as the default human/operator surface.\n"
            f"There are currently {tool_count} catalog-backed tools. Start with `workflow tools list`, "
            "`workflow tools search <text>`, and `workflow tools describe <tool|alias>` when you need the current "
            "surface instead of memorizing a static list. Use `--exact` when you already know the alias or entrypoint.\n"
            f"For common reads, go straight to `{query_tool.cli_entrypoint}`, `{health_tool.cli_entrypoint}`, "
            f"`{discover_tool.cli_entrypoint}`, `{recall_tool.cli_entrypoint}`, `{bugs_tool.cli_entrypoint}`, "
            "and `workflow architecture scan` when you need exact boundary evidence.\n"
            "Use `workflow tools call <tool|alias> --input-json '{...}'` as the generic fallback when no direct alias fits.\n"
            "CLI guardrails are intentional: write/dispatch flows require `--yes`, and session-only tools require a "
            "workflow token.\n"
            "SEARCH BEFORE YOU BUILD: Before writing any new function, module, class, or pattern, "
            f"use `{discover_tool.cli_entrypoint}` in the CLI, `{discover_tool.name}` via MCP, or `/query` with "
            "`find <term>` over HTTP to check if similar code already exists. The codebase is large — duplicating "
            "existing infrastructure wastes time and creates maintenance burden. Use "
            f"`{recall_tool.cli_entrypoint}` / `{recall_tool.name}` for architectural decisions and patterns.\n"
            "When the question is about exact boundary drift — raw SQL in front doors, or front-door imports that "
            "reach into runtime/storage — use `workflow architecture scan` instead of the router.\n"
            f"Use `{query_tool.cli_entrypoint}` or `/query` for natural-language questions and first-pass routing.\n"
            "Use /workflow-runs to enqueue a workflow spec run. Treat it as fire-and-observe, never wait-for-completion.\n"
            "The launch call should stay short so the client can keep issuing new commands while execution happens elsewhere.\n"
            "For HTTP clients, POST /workflow-runs to get run_id, then use the dedicated channels "
            "GET /api/workflow-runs/{run_id}/stream for live SSE updates and GET /api/workflow-runs/{run_id}/status "
            "for snapshots.\n"
            f"For MCP use, prefer `{workflow_tool.name}(action='run', spec_path='...')`. It returns run_id plus "
            "stream_url and status_url. Follow with action='status', inspect, cancel, or retry as needed.\n"
            "Legacy inline wait can still exist for streaming MCP callers, but the supported mental model is kickoff "
            "first and observation on separate status/stream channels.\n"
            "When checking status, read health.likely_failed + health.signals + health.resource_telemetry "
            "(tokens_total, tokens_per_minute, heartbeat freshness) as the expected-failure heuristic.\n"
            "For MCP status calls, use kill_if_idle=true if a running run is idle and unhealthy.\n"
            "CPU is currently proxied through heartbeat + throughput signals, not native CPU telemetry.\n"
            "Use `/health` or `workflow health` to check system state first when the platform feels degraded."
        ),
        "infrastructure": {
            "service_manager": "scripts/praxis",
            "compatibility_alias": "scripts/praxis-ctl",
            "commands": {
                "install": "praxis install — install the single Praxis background agent (auto-start on login)",
                "launch": "praxis launch — auto-heal services, ensure /app build, prove launcher readiness, and open the launcher",
                "status": "praxis status — show all services, PIDs, ports, and semantic readiness",
                "restart": "praxis restart [postgres|api|workflow-api|worker|scheduler] — restart services",
                "stop": "praxis stop — stop all services",
                "logs": "praxis logs [postgres|api|workflow-api|worker|scheduler] — tail logs",
            },
            "services": [
                {"label": "com.praxis.engine", "keep_alive": True, "contains": ["postgres", "api-server", "workflow-worker", "scheduler"]},
                {"label": "com.praxis.postgres", "port": 5432, "managed_by": "com.praxis.engine"},
                {"label": "com.praxis.api-server", "port": 8420, "managed_by": "com.praxis.engine"},
                {"label": "com.praxis.workflow-worker", "managed_by": "com.praxis.engine"},
                {"label": "com.praxis.scheduler", "interval_sec": 60, "managed_by": "com.praxis.engine"},
            ],
            "notes": "scripts/praxis is the preferred launcher entrypoint; scripts/praxis-ctl remains a compatibility alias. launchd only sees the single com.praxis.engine background item, while Praxis supervises postgres, api-server, workflow-worker, and scheduler internally. The launcher front door is http://127.0.0.1:8420/app and the always-on MCP bridge is served from the API surface.",
        },
    }


def _parse_optional_iso_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise _ClientError(f"{field_name} must be an ISO-8601 datetime string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _ClientError(f"{field_name} must be an ISO-8601 datetime string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _ClientError(f"{field_name} must include a timezone offset")
    return parsed


def _handle_task_route_eligibility_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    provider_slug = body.get("provider_slug")
    if not isinstance(provider_slug, str) or not provider_slug.strip():
        raise _ClientError("provider_slug is required")

    task_type = body.get("task_type")
    if task_type is not None and (not isinstance(task_type, str) or not task_type.strip()):
        raise _ClientError("task_type must be a non-empty string when provided")

    model_slug = body.get("model_slug")
    if model_slug is not None and (not isinstance(model_slug, str) or not model_slug.strip()):
        raise _ClientError("model_slug must be a non-empty string when provided")

    reason_code = body.get("reason_code", "operator_control")
    if not isinstance(reason_code, str) or not reason_code.strip():
        raise _ClientError("reason_code must be a non-empty string")

    rationale = body.get("rationale")
    if rationale is not None and (not isinstance(rationale, str) or not rationale.strip()):
        raise _ClientError("rationale must be a non-empty string when provided")

    decision_ref = body.get("decision_ref")
    if decision_ref is not None and (
        not isinstance(decision_ref, str) or not decision_ref.strip()
    ):
        raise _ClientError("decision_ref must be a non-empty string when provided")

    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    return operator_write.set_task_route_eligibility_window(
        provider_slug=provider_slug,
        eligibility_status=body.get("eligibility_status", "rejected"),
        effective_to=_parse_optional_iso_datetime(
            body.get("effective_to"),
            field_name="effective_to",
        ),
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=_parse_optional_iso_datetime(
            body.get("effective_from"),
            field_name="effective_from",
        ),
        decision_ref=decision_ref,
        env=env,
    )


def _parse_optional_mapping(value: object, *, field_name: str) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise _ClientError(f"{field_name} must be an object")
    return value


def _parse_optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise _ClientError(f"{field_name} must be a non-empty string when provided")
    return value.strip()


def _handle_native_primary_cutover_gate_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    decided_by = body.get("decided_by")
    if not isinstance(decided_by, str) or not decided_by.strip():
        raise _ClientError("decided_by is required")

    decision_source = body.get("decision_source")
    if not isinstance(decision_source, str) or not decision_source.strip():
        raise _ClientError("decision_source is required")

    rationale = body.get("rationale")
    if not isinstance(rationale, str) or not rationale.strip():
        raise _ClientError("rationale is required")

    roadmap_item_id = _parse_optional_text(
        body.get("roadmap_item_id"),
        field_name="roadmap_item_id",
    )
    workflow_class_id = _parse_optional_text(
        body.get("workflow_class_id"),
        field_name="workflow_class_id",
    )
    schedule_definition_id = _parse_optional_text(
        body.get("schedule_definition_id"),
        field_name="schedule_definition_id",
    )
    if sum(1 for value in (roadmap_item_id, workflow_class_id, schedule_definition_id) if value) != 1:
        raise _ClientError("exactly one of roadmap_item_id, workflow_class_id, or schedule_definition_id is required")

    title = _parse_optional_text(body.get("title"), field_name="title")
    gate_name = _parse_optional_text(body.get("gate_name"), field_name="gate_name")
    gate_policy = _parse_optional_mapping(body.get("gate_policy"), field_name="gate_policy")
    required_evidence = _parse_optional_mapping(
        body.get("required_evidence"),
        field_name="required_evidence",
    )

    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    return operator_write.admit_native_primary_cutover_gate(
        decided_by=decided_by.strip(),
        decision_source=decision_source.strip(),
        rationale=rationale.strip(),
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=_parse_optional_iso_datetime(
            body.get("decided_at"),
            field_name="decided_at",
        ),
        opened_at=_parse_optional_iso_datetime(
            body.get("opened_at"),
            field_name="opened_at",
        ),
        created_at=_parse_optional_iso_datetime(
            body.get("created_at"),
            field_name="created_at",
        ),
        updated_at=_parse_optional_iso_datetime(
            body.get("updated_at"),
            field_name="updated_at",
        ),
        env=env,
    )


def _handle_transport_support(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    # --- input validation (handler responsibility) ---
    provider_filter = body.get("provider_slug")
    if provider_filter is not None and (not isinstance(provider_filter, str) or not provider_filter.strip()):
        raise _ClientError("provider_slug must be a non-empty string when provided")
    model_filter = body.get("model_slug")
    if model_filter is not None and (not isinstance(model_filter, str) or not model_filter.strip()):
        raise _ClientError("model_slug must be a non-empty string when provided")
    raw_jobs = body.get("jobs")
    if raw_jobs is not None and not isinstance(raw_jobs, list):
        raise _ClientError("jobs must be a list when provided")
    if isinstance(raw_jobs, list):
        for index, raw_job in enumerate(raw_jobs):
            if not isinstance(raw_job, dict):
                raise _ClientError(f"jobs[{index}] must be an object")

    runtime_profile_ref = (
        body.get("runtime_profile_ref").strip()
        if isinstance(body.get("runtime_profile_ref"), str) and body.get("runtime_profile_ref").strip()
        else "praxis"
    )

    return operator_read.query_transport_support(
        health_mod=subs.get_health_mod(),
        pg=subs.get_pg_conn(),
        provider_filter=provider_filter.strip() if isinstance(provider_filter, str) else None,
        model_filter=model_filter.strip() if isinstance(model_filter, str) else None,
        runtime_profile_ref=runtime_profile_ref,
        jobs=raw_jobs if isinstance(raw_jobs, list) else None,
    )


def _parse_optional_string_list(value: object, *, field_name: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise _ClientError(f"{field_name} must be a list of non-empty strings")
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise _ClientError(f"{field_name}[{index}] must be a non-empty string")
        normalized.append(item.strip())
    return normalized


def _parse_optional_bool(value: object, *, field_name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise _ClientError(f"{field_name} must be a boolean when provided")
    return value


def _handle_roadmap_write_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    title = body.get("title")
    if not isinstance(title, str) or not title.strip():
        raise _ClientError("title is required")

    intent_brief = body.get("intent_brief")
    if not isinstance(intent_brief, str) or not intent_brief.strip():
        raise _ClientError("intent_brief is required")

    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    return operator_write.roadmap_write(
        action=body.get("action", "preview"),
        title=title,
        intent_brief=intent_brief,
        template=body.get("template", "single_capability"),
        priority=body.get("priority", "p2"),
        parent_roadmap_item_id=body.get("parent_roadmap_item_id"),
        slug=body.get("slug"),
        depends_on=_parse_optional_string_list(
            body.get("depends_on"),
            field_name="depends_on",
        ),
        source_bug_id=body.get("source_bug_id"),
        registry_paths=_parse_optional_string_list(
            body.get("registry_paths"),
            field_name="registry_paths",
        ),
        decision_ref=body.get("decision_ref"),
        item_kind=body.get("item_kind"),
        tier=body.get("tier"),
        phase_ready=_parse_optional_bool(
            body.get("phase_ready"),
            field_name="phase_ready",
        ),
        approval_tag=body.get("approval_tag"),
        reference_doc=body.get("reference_doc"),
        outcome_gate=body.get("outcome_gate"),
        env=env,
    )


def _handle_roadmap_view_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    root_roadmap_item_id = body.get("root_roadmap_item_id")
    if not isinstance(root_roadmap_item_id, str) or not root_roadmap_item_id.strip():
        raise _ClientError("root_roadmap_item_id is required")

    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    return operator_read.query_roadmap_tree(
        root_roadmap_item_id=root_roadmap_item_id,
        env=env,
    )


def _handle_work_item_closeout_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    return operator_write.reconcile_work_item_closeout(
        action=body.get("action", "preview"),
        bug_ids=_parse_optional_string_list(
            body.get("bug_ids"),
            field_name="bug_ids",
        ),
        roadmap_item_ids=_parse_optional_string_list(
            body.get("roadmap_item_ids"),
            field_name="roadmap_item_ids",
        ),
        env=env,
    )


def _handle_provider_onboarding_post(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    del subs
    from registry.provider_onboarding import normalize_provider_onboarding_spec, run_provider_onboarding

    raw_spec = body.get("spec") if isinstance(body.get("spec"), dict) else body
    try:
        spec = normalize_provider_onboarding_spec(raw_spec)
    except Exception as exc:
        raise _ClientError(str(exc)) from exc

    env = {"WORKFLOW_DATABASE_URL": resolve_workflow_database_url()}
    result = run_provider_onboarding(
        database_url=env["WORKFLOW_DATABASE_URL"],
        spec=spec,
        dry_run=bool(body.get("dry_run", False)),
    )
    return _serialize(result)


def _handle_health(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    hs_mod = subs.get_health_mod()
    from adapters import provider_registry as provider_registry_mod
    dependency_truth = dependency_truth_report(scope="all")

    db_url = resolve_workflow_database_url()
    probes: list[Any] = [
        hs_mod.PostgresProbe(db_url),
        hs_mod.PostgresConnectivityProbe(db_url),
        hs_mod.DiskSpaceProbe(str(REPO_ROOT)),
    ]
    for provider_slug in provider_registry_mod.registered_providers():
        for adapter_type in ("cli_llm", "llm_task"):
            if not provider_registry_mod.supports_adapter(provider_slug, adapter_type):
                continue
            probes.append(hs_mod.ProviderTransportProbe(provider_slug, adapter_type))

    runner = hs_mod.PreflightRunner(probes)
    preflight = runner.run()

    panel = subs.get_operator_panel()
    snap = panel.snapshot()
    lane = panel.recommend_lane()
    proof_payload: dict[str, Any]
    try:
        from runtime.receipt_store import proof_metrics

        proof_payload = proof_metrics(
            since_hours=int(body.get("since_hours") or 0),
        )
    except Exception as exc:
        proof_payload = {"error": f"Could not compute proof metrics: {exc}"}

    schema_authority: dict[str, Any]
    try:
        from storage.dev_postgres import local_postgres_health

        status = local_postgres_health()
        schema_authority = {
            "schema_bootstrapped": status.schema_bootstrapped,
            "missing_schema_objects": list(status.missing_schema_objects),
            "compile_artifact_authority_ready": status.compile_artifact_authority_ready,
            "compile_index_authority_ready": status.compile_index_authority_ready,
            "execution_packet_authority_ready": status.execution_packet_authority_ready,
            "repo_snapshot_authority_ready": status.repo_snapshot_authority_ready,
            "verification_registry_ready": status.verification_registry_ready,
            "verifier_authority_ready": status.verifier_authority_ready,
            "healer_authority_ready": status.healer_authority_ready,
        }
    except Exception as exc:
        schema_authority = {
            "error": f"Could not resolve schema authority readiness: {exc}",
        }

    return {
        "preflight": {
            "overall": preflight.overall.value,
            "checks": [
                {
                    "name": check.name,
                    "passed": check.passed,
                    "message": check.message,
                    "duration_ms": round(check.duration_ms, 2),
                    "status": check.status or ("ok" if check.passed else "failed"),
                    "details": check.details,
                }
                for check in preflight.checks
            ],
            "timestamp": preflight.timestamp.isoformat(),
        },
        "operator_snapshot": _serialize(snap),
        "proof_metrics": proof_payload,
        "schema_authority": schema_authority,
        "dependency_truth": dependency_truth,
        "lane_recommendation": {
            "recommended_posture": lane.recommended_posture,
            "confidence": lane.confidence,
            "reasons": list(lane.reasons),
            "degraded_cause": lane.degraded_cause,
        },
    }


def _handle_governance(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    action = body.get("action", "scan_prompt")
    gov = subs.get_governance_filter()

    if action == "scan_prompt":
        text = body.get("text", "")
        if not text:
            raise _ClientError("text is required for scan_prompt")
        result = gov.scan_prompt(text)
        if result.passed:
            return {"passed": True, "findings_count": 0}
        return {
            "passed": False,
            "blocked_reason": result.blocked_reason,
            "findings": [
                {
                    "pattern": finding.pattern_name,
                    "line": finding.line_number,
                    "severity": finding.severity,
                    "redacted": finding.redacted_match,
                }
                for finding in result.findings
            ],
        }
    if action == "scan_scope":
        result = gov.scan_scope(
            body.get("write_paths", []),
            body.get("allowed_paths") or None,
        )
        if result.passed:
            return {"passed": True}
        return {
            "passed": False,
            "blocked_reason": result.blocked_reason,
            "out_of_scope": list(result.out_of_scope_paths),
        }
    raise _ClientError(f"Unknown governance action: {action}")


def _handle_root_get(request: Any, path: str) -> None:
    request._send_json(
        200,
        {
            "service": "praxis-workflow-api",
            "version": "2.0.0",
            "hint": "POST to /orient to get started",
        },
    )


def _handle_platform_overview_get(request: Any, path: str) -> None:
    try:
        pg = request.subsystems.get_pg_conn()
        from runtime.receipt_store import list_receipts as _list_receipts

        status_data = _handle_status(request.subsystems, {"since_hours": 24})
        recent_records = _list_receipts(limit=20)
        recent = [
            {
                "label": record.label,
                "agent": record.agent,
                "status": record.status,
                "timestamp": record.timestamp,
            }
            for record in recent_records
        ]
        models = pg.execute(
            """
            SELECT DISTINCT ON (provider_slug, model_slug)
                   provider_slug || '/' || model_slug AS name,
                   capability_tags AS tags,
                   route_tier,
                   latency_class
            FROM provider_model_candidates
            WHERE status = 'active'
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
            """
        )
        bug_sev = pg.execute(
            "SELECT severity as code, COUNT(*) as count FROM bugs GROUP BY severity ORDER BY count DESC LIMIT 8"
        )
        request._send_json(
            200,
            {
                "pass_rate": status_data.get("pass_rate", 0),
                "total_workflows": status_data.get("total_workflows", 0),
                "total_tables": int(
                    pg.fetchval("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'")
                ),
                "total_bugs": int(pg.fetchval("SELECT COUNT(*) FROM bugs")),
                "open_bugs": int(pg.fetchval("SELECT COUNT(*) FROM bugs WHERE status = 'OPEN'")),
                "total_workflow_runs": int(pg.fetchval("SELECT COUNT(*) FROM public.workflow_runs")),
                "total_registry_items": int(pg.fetchval("SELECT COUNT(*) FROM platform_registry")),
                "recent_workflows": [
                    {
                        "label": row["label"],
                        "agent": row["agent"],
                        "status": row["status"],
                        "timestamp": str(row["timestamp"]) if row["timestamp"] else "",
                    }
                    for row in recent
                ],
                "active_models": [
                    {
                        "name": row["name"],
                        "tags": row["tags"],
                        "route_tier": row["route_tier"],
                        "latency_class": row["latency_class"],
                    }
                    for row in models
                ],
                "bug_severity": [dict(row) for row in bug_sev],
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}", "error_code": "internal_error"})


def _handle_workflow_templates_get(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        params = _query_params(request.path)
        query = (params.get("q", [""])[0]).strip()

        columns = (
            "id, name, description, category, trigger_type, input_schema, output_schema, steps, mcp_tool_refs"
        )
        if query:
            rows = pg.execute(
                f"""SELECT {columns}
                       FROM registry_workflows
                      WHERE search_vector @@ plainto_tsquery('english', $1)
                         OR name ILIKE '%' || $1 || '%'
                         OR description ILIKE '%' || $1 || '%'
                      ORDER BY name
                      LIMIT 20""",
                query,
            )
        else:
            rows = pg.execute(
                f"SELECT {columns} FROM registry_workflows ORDER BY name LIMIT 20"
            )

        request._send_json(
            200,
            {
                "templates": [
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "description": row.get("description") or "",
                        "category": row.get("category") or "",
                        "trigger_type": row.get("trigger_type") or "",
                        "input_schema": row.get("input_schema") or {},
                        "output_schema": row.get("output_schema") or {},
                        "steps": row.get("steps") or [],
                        "mcp_tool_refs": row.get("mcp_tool_refs") or [],
                    }
                    for row in rows
                ],
                "count": len(rows),
                "query": query,
            },
        )
    except Exception as exc:
        request._send_json(500, {"error": str(exc)})


def _handle_launcher_status_get(request: Any, path: str) -> None:
    del path
    try:
        request._send_json(200, workflow_launcher.launcher_status_payload())
    except workflow_launcher.LauncherAuthorityError as exc:
        request._send_json(503, {"error": str(exc)})


def _handle_launcher_recover_post(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
    except Exception as exc:
        request._send_json(400, {"error": f"Invalid JSON: {exc}"})
        return

    action = body.get("action", "launch")
    service = body.get("service")
    run_id = body.get("run_id")
    open_browser = bool(body.get("open_browser", False))

    if not isinstance(action, str) or not action.strip():
        request._send_json(400, {"error": "action must be a non-empty string"})
        return
    if service is not None and (not isinstance(service, str) or not service.strip()):
        request._send_json(400, {"error": "service must be a non-empty string when provided"})
        return
    if run_id is not None and (not isinstance(run_id, str) or not run_id.strip()):
        request._send_json(400, {"error": "run_id must be a non-empty string when provided"})
        return

    try:
        status_code, payload = workflow_launcher.launcher_recover_payload(
            action=action,
            service=service,
            run_id=run_id,
            open_browser=open_browser,
        )
    except ValueError as exc:
        request._send_json(400, {"error": str(exc)})
        return
    except workflow_launcher.LauncherAuthorityError as exc:
        request._send_json(503, {"error": str(exc)})
        return

    request._send_json(status_code, payload)


ADMIN_POST_ROUTES: list[RouteEntry] = [
    (_exact("/api/launcher/recover"), _handle_launcher_recover_post),
]

ADMIN_GET_ROUTES: list[RouteEntry] = [
    (_exact("/"), _handle_root_get),
    (_exact("/api/platform-overview"), _handle_platform_overview_get),
    (_exact("/api/launcher/status"), _handle_launcher_status_get),
    (_exact("/api/workflow-templates"), _handle_workflow_templates_get),
]

ADMIN_ROUTES: dict[str, object] = {
    "/orient": _handle_orient,
    "/health": _handle_health,
    "/governance": _handle_governance,
    "/api/operator/task-route-eligibility": _handle_task_route_eligibility_post,
    "/api/operator/native-primary-cutover-gate": _handle_native_primary_cutover_gate_post,
    "/api/operator/transport-support": _handle_transport_support,
    "/api/operator/roadmap-write": _handle_roadmap_write_post,
    "/api/operator/work-item-closeout": _handle_work_item_closeout_post,
    "/api/operator/roadmap-view": _handle_roadmap_view_post,
    "/api/operator/provider-onboarding": _handle_provider_onboarding_post,
}


__all__ = [
    "ADMIN_GET_ROUTES",
    "ADMIN_POST_ROUTES",
    "ADMIN_ROUTES",
    "_handle_health",
]
