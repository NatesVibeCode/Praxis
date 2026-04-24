"""Administrative handlers for the workflow HTTP API."""

from __future__ import annotations

from typing import Any

from ._shared import (
    REPO_ROOT,
    RouteEntry,
    _ClientError,
    _exact,
    _query_params,
    _read_json_body,
    _serialize,
)


def dependency_truth_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.dependency_contract import dependency_truth_report as _dependency_truth_report

    return _dependency_truth_report(*args, **kwargs)


def build_code_hotspots(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.engineering_observability import build_code_hotspots as _build_code_hotspots

    return _build_code_hotspots(*args, **kwargs)


def build_bug_scoreboard(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.engineering_observability import build_bug_scoreboard as _build_bug_scoreboard

    return _build_bug_scoreboard(*args, **kwargs)


def build_platform_observability(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.engineering_observability import (
        build_platform_observability as _build_platform_observability,
    )

    return _build_platform_observability(*args, **kwargs)


def build_content_health_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.missing_detector import build_content_health_report as _build_content_health_report

    return _build_content_health_report(*args, **kwargs)


def provider_registry_health(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from registry.provider_execution_registry import registry_health as _provider_registry_health

    return _provider_registry_health(*args, **kwargs)


def query_transport_support(*args: Any, **kwargs: Any) -> Any:
    from surfaces.api.operator_read import query_transport_support as _query_transport_support

    return _query_transport_support(*args, **kwargs)


def build_transport_support_summary(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from surfaces.api.operator_read import (
        build_transport_support_summary as _build_transport_support_summary,
    )

    return _build_transport_support_summary(*args, **kwargs)


def surface_usage_recorder_health() -> dict[str, Any]:
    from ._surface_usage import surface_usage_recorder_health as _surface_usage_recorder_health

    return _surface_usage_recorder_health()


def workflow_database_status(*args: Any, **kwargs: Any) -> Any:
    from surfaces._boot import workflow_database_status as _workflow_database_status

    return _workflow_database_status(*args, **kwargs)


def native_instance_contract(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.instance import native_instance_contract as _native_instance_contract

    return _native_instance_contract(*args, **kwargs)


def build_orient_primitive_contracts(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from runtime.primitive_contracts import (
        build_orient_primitive_contracts as _build_orient_primitive_contracts,
    )

    return _build_orient_primitive_contracts(*args, **kwargs)


def _tool_definition(tool_name: str):
    from surfaces.mcp.catalog import get_tool_catalog

    definition = get_tool_catalog().get(tool_name)
    if definition is None:
        raise KeyError(f"unknown MCP tool: {tool_name}")
    return definition


def _workflow_env(subs: Any) -> dict[str, str]:
    from surfaces._boot import resolve_surface_env

    postgres_env = getattr(subs, "_postgres_env", None)
    env = dict(postgres_env() or {}) if callable(postgres_env) else None
    try:
        return resolve_surface_env(
            repo_root=getattr(subs, "_repo_root", None),
            workflow_root=getattr(subs, "_workflow_root", None),
            env=env,
        )
    except RuntimeError as exc:
        raise RuntimeError("workflow surface is missing an explicit Postgres env authority") from exc


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


def _build_standing_orders(subs: Any) -> list[dict[str, Any]]:
    """Return active architecture-policy rows as boot-time directives.

    Every harness that orients via /orient receives these as standing orders.
    Queries operator_decisions directly via the sync pg connection so this stays
    callable from the sync handler dispatcher without crossing async boundaries.
    """

    try:
        pg = subs.get_pg_conn()
    except Exception as exc:  # noqa: BLE001 — orient must not crash on auxiliary reads
        return [{"error": f"standing_orders unavailable: {exc}"}]

    try:
        rows = pg.fetch(
            """
            SELECT
                decision_scope_ref,
                decision_key,
                title,
                rationale,
                decided_by,
                decision_source,
                effective_from,
                effective_to
            FROM operator_decisions
            WHERE decision_kind = 'architecture_policy'
              AND decision_status IN ('decided', 'active')
              AND effective_from <= now()
              AND (effective_to IS NULL OR effective_to > now())
            ORDER BY effective_from DESC, decided_at DESC
            LIMIT 50
            """
        )
    except Exception as exc:  # noqa: BLE001 — orient must not crash on auxiliary reads
        return [{"error": f"standing_orders unavailable: {exc}"}]

    directives: list[dict[str, Any]] = []
    for row in rows:
        effective_from = row["effective_from"]
        effective_to = row["effective_to"]
        directive = {
            "authority_domain": row["decision_scope_ref"],
            "policy_slug": row["decision_key"],
            "title": row["title"],
            "rationale": row["rationale"],
            "decided_by": row["decided_by"],
            "decision_source": row["decision_source"],
            "effective_from": effective_from.isoformat() if effective_from else None,
            "effective_to": effective_to.isoformat() if effective_to else None,
        }
        directives.append({k: v for k, v in directive.items() if v is not None})
    return directives


def _build_orient_tool_guidance(
    *,
    discover_tool: Any,
    recall_tool: Any,
    query_tool: Any,
    workflow_tool: Any,
    health_tool: Any,
    bugs_tool: Any,
    tool_count: int,
) -> dict[str, Any]:
    """Return structured tool-selection guidance for the orient authority envelope."""

    return {
        "kind": "orient_tool_guidance",
        "authority": "surfaces.mcp.catalog.get_tool_catalog",
        "policy_decision_ref": (
            "operator_decision.architecture_policy.orient.authority_envelope_tool_guidance"
        ),
        "policy_decision_key": "architecture-policy::orient::authority-envelope-tool-guidance",
        "preferred_operator_surface": {
            "kind": "catalog_backed_cli",
            "command_prefix": "workflow",
            "tool_count": tool_count,
            "generic_call": "workflow tools call <tool|alias|entrypoint> --input-json '{...}'",
            "unified_http_call": "POST /api/operate",
            "unified_http_catalog": "GET /api/operate/catalog",
        },
        "catalog": {
            "list_command": "workflow tools list",
            "search_command": "workflow tools search <text>",
            "schema_command": "workflow tools describe <tool|alias|entrypoint>",
            "http_catalog": "GET /api/operate/catalog",
            "directive": "Inspect the live catalog before guessing tool names or schemas.",
        },
        "primary_reads": [
            {
                "intent": "unknown or broad system question",
                "tool": query_tool.name,
                "command": query_tool.cli_entrypoint,
                "schema": query_tool.cli_describe_command,
            },
            {
                "intent": "current platform health or degraded state",
                "tool": health_tool.name,
                "command": health_tool.cli_entrypoint,
                "schema": health_tool.cli_describe_command,
            },
            {
                "intent": "code behavior before building",
                "tool": discover_tool.name,
                "command": discover_tool.cli_entrypoint,
                "schema": discover_tool.cli_describe_command,
            },
            {
                "intent": "architecture memory and decisions",
                "tool": recall_tool.name,
                "command": recall_tool.cli_entrypoint,
                "schema": recall_tool.cli_describe_command,
            },
            {
                "intent": "bug state, replay packets, and duplicate checks",
                "tool": bugs_tool.name,
                "command": bugs_tool.cli_entrypoint,
                "schema": bugs_tool.cli_describe_command,
            },
        ],
        "dispatch": {
            "tool": workflow_tool.name,
            "command": workflow_tool.cli_entrypoint,
            "schema": workflow_tool.cli_describe_command,
            "model": "kickoff first; inspect status or stream separately",
        },
        "guardrails": {
            "write_dispatch_requires_yes": True,
            "session_tools_require_workflow_token": True,
            "search_before_build": True,
        },
    }


def _build_orient_authority_envelope(
    subs: Any,
    *,
    standing_orders: list[dict[str, Any]],
    health_payload: dict[str, Any],
    dependency_truth: dict[str, Any],
    tool_guidance: dict[str, Any],
    fast: bool = False,
) -> dict[str, Any]:
    """Return the explicit authority envelope projected by /orient."""

    workflow_env: dict[str, str] = {}
    workflow_env_error: str | None = None
    try:
        workflow_env = _workflow_env(subs)
    except Exception as exc:  # noqa: BLE001 — orient must report drift instead of hiding it
        workflow_env_error = f"{type(exc).__name__}: {exc}"

    if fast:
        native_instance = {"status": "skipped", "reason": "orient_fast_path"}
    else:
        if workflow_env_error is None:
            try:
                native_instance = native_instance_contract(
                    env=workflow_env,
                )
            except Exception as exc:  # noqa: BLE001 — orient must report drift instead of hiding it
                workflow_env_error = f"{type(exc).__name__}: {exc}"
                native_instance = {
                    "error": f"native_instance unavailable: {workflow_env_error}",
                }
        else:
            native_instance = {
                "error": f"native_instance unavailable: {workflow_env_error}",
            }
    primitive_contracts = build_orient_primitive_contracts(
        workflow_env=workflow_env,
        native_instance=native_instance,
        workflow_env_error=workflow_env_error,
    )

    return {
        "kind": "orient_authority_envelope",
        "version": "1.0",
        "mandatory": True,
        "authority": "surfaces.api.handlers.workflow_admin._handle_orient",
        "policy_decision_ref": (
            "operator_decision.architecture_policy.orient.mandatory_authority_envelope"
        ),
        "policy_decision_key": "architecture-policy::orient::mandatory-authority-envelope",
        "native_instance": native_instance,
        "standing_orders_ref": "/orient#standing_orders",
        "standing_orders_count": len(
            [
                order
                for order in standing_orders
                if isinstance(order, dict) and "error" not in order
            ]
        ),
        "health_ref": "/orient#health",
        "health_overall": (health_payload.get("preflight") or {}).get("overall"),
        "lane_recommendation": health_payload.get("lane_recommendation") or {},
        "tool_guidance": tool_guidance,
        "tool_guidance_ref": "/orient#tool_guidance",
        "primitive_contracts": primitive_contracts,
        "primitive_contracts_ref": "/orient#primitive_contracts",
        "dependency_truth": {
            "ok": dependency_truth.get("ok"),
            "missing_count": dependency_truth.get("missing_count"),
        },
        "surface_refs": {
            "orient": "/orient",
            "workflow_health": "/health",
            "operator_status_snapshot": "/api/status",
            "native_operator_surface": (
                "surfaces.api.native_operator_surface.query_native_operator_surface"
            ),
            "context_shard": "praxis_context_shard",
        },
        "scope_source": {
            "default": "/orient#authority_envelope.native_instance",
            "workflow_session": "praxis_context_shard when a signed workflow token is present",
        },
        "enforcement": {
            "new_entrypoints": (
                "consume or project this envelope instead of resolving runtime authority independently"
            ),
            "drift_signal": (
                "native_instance.error or mismatched downstream native_instance payloads must be treated "
                "as authority drift"
            ),
            "primitive_contracts": (
                "operation posture, runtime binding, state semantics, proof refs, and failure identity "
                "must be consumed from /orient or their named underlying authorities"
            ),
        },
    }


def _handle_orient(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    """Return everything an agent needs to start operating."""

    fast_orient = bool(body.get("fast") or body.get("skip_engineering_observability"))
    compact_orient = bool(body.get("compact"))
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
        "/api/status": "Catalog-backed operator status snapshot with queue and failure breakdown.",
        "/api/launcher/status": "Launcher readiness plus runtime target and sandbox contract status.",
        "/api/setup/doctor": "Runtime-target setup doctor with sandbox, DB, API, workspace, and image contract.",
        "/api/setup/plan": "Runtime-target setup plan showing authority rows, services, images, and cleanup checks.",
        "/api/setup/apply": "Runtime-target setup apply gate; API/MCP own setup authority, SSH is build/deploy transport only.",
        "/api/setup/graph": "Onboarding gate-probe graph: per-gate status, observed state, and copy-pasteable remediation hints.",
        "/api/operator/graph": "Catalog-backed cross-domain operator graph projection.",
        "/api/operator/issue-backlog": "Catalog-backed canonical issue backlog read.",
        "/api/operator/replay-ready-bugs": "Catalog-backed replay-ready bug backlog read.",
        "/api/operator/runs/{run_id}/status": "Catalog-backed run-scoped operator status view.",
        "/api/operator/runs/{run_id}/scoreboard": "Catalog-backed cutover scoreboard for one run.",
        "/api/operator/runs/{run_id}/graph": "Catalog-backed run graph topology view.",
        "/api/operator/runs/{run_id}/lineage": "Catalog-backed run lineage view.",
        "/api/operator/task-route-eligibility": "Write a timed provider/model route eligibility window",
        "/api/operator/transport-support": "Read provider/model transport support before run time",
        "/api/operator/native-primary-cutover-gate": "Admit a native-primary cutover gate through operator control",
        "/api/operator/roadmap-write": "Preview, validate, or commit roadmap rows through one shared validation gate",
        "/api/operator/work-item-closeout": "Preview or commit proof-backed bug and roadmap closeout through one shared reconciliation gate",
        "/api/operator/roadmap/tree/{root_roadmap_item_id}": "Read one roadmap subtree and its dependency edges from DB-backed authority",
        "/api/operator/provider-onboarding": "Seed a provider profile, model catalog rows, benchmark metadata, and verification in one wizard",
        "/api/operator/decision": "Record one canonical operator decision row through the operation catalog",
        "/api/operator/decisions": "List canonical operator decisions through the operation catalog",
        "/api/operator/architecture-policy": "Record one architecture-policy operator decision through the operation catalog",
        "/api/operator/functional-area": "Record one functional area row through the operation catalog",
        "/api/operator/object-relation": "Record one operator object relation through the operation catalog",
        "/api/circuits": "Read or override provider circuit breaker state through the operation catalog",
        "/api/circuits/history": "Read durable provider circuit override history through the operation catalog",
    }

    if fast_orient:
        health_payload = {
            "preflight": {"overall": "skipped"},
            "operator_snapshot": {},
            "proof_metrics": {"status": "skipped", "reason": "orient_fast_path"},
            "schema_authority": {"status": "skipped", "reason": "orient_fast_path"},
            "lane_recommendation": {},
        }
    else:
        health_payload = _handle_health(subs, {})
    if fast_orient:
        dependency_truth = {"ok": True, "status": "skipped", "reason": "orient_fast_path"}
    else:
        dependency_truth = dependency_truth_report(scope="all")

    recent_activity: dict[str, Any]
    if fast_orient:
        recent_activity = {"status": "skipped", "reason": "orient_fast_path"}
    else:
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

    if fast_orient:
        engineering_observability = {
            "code_hotspots": {"status": "skipped", "reason": "orient_fast_path"},
            "bug_scoreboard": {"status": "skipped", "reason": "orient_fast_path"},
            "platform_observability": {"status": "skipped", "reason": "orient_fast_path"},
        }
    else:
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

    if fast_orient:
        from types import SimpleNamespace
        from surfaces.mcp.catalog import get_tool_catalog

        tool_count = len(get_tool_catalog())
        query_tool = SimpleNamespace(name="praxis_query", cli_entrypoint="workflow query")
        health_tool = SimpleNamespace(name="praxis_health", cli_entrypoint="workflow health")
        discover_tool = SimpleNamespace(name="praxis_discover", cli_entrypoint="workflow discover")
        recall_tool = SimpleNamespace(name="praxis_recall", cli_entrypoint="workflow recall")
        bugs_tool = SimpleNamespace(name="praxis_bugs", cli_entrypoint="workflow bugs")
        workflow_tool = SimpleNamespace(name="praxis_workflow", cli_entrypoint="workflow run")
        tool_guidance = {
            "kind": "catalog_backed_cli",
            "preferred": True,
            "status": "skipped",
            "reason": "orient_fast_path",
            "recommended_reads": [
                {"tool": "praxis_query", "command": "workflow query"},
                {"tool": "praxis_health", "command": "workflow health"},
                {"tool": "praxis_discover", "command": "workflow discover"},
                {"tool": "praxis_recall", "command": "workflow recall"},
                {"tool": "praxis_bugs", "command": "workflow bugs"},
            ],
        }
    else:
        from surfaces.mcp.catalog import get_tool_catalog

        discover_tool = _tool_definition("praxis_discover")
        recall_tool = _tool_definition("praxis_recall")
        query_tool = _tool_definition("praxis_query")
        workflow_tool = _tool_definition("praxis_workflow")
        health_tool = _tool_definition("praxis_health")
        bugs_tool = _tool_definition("praxis_bugs")
        tool_count = len(get_tool_catalog())
        tool_guidance = _build_orient_tool_guidance(
            discover_tool=discover_tool,
            recall_tool=recall_tool,
            query_tool=query_tool,
            workflow_tool=workflow_tool,
            health_tool=health_tool,
            bugs_tool=bugs_tool,
            tool_count=tool_count,
        )

    try:
        standing_orders = _build_standing_orders(subs)
    except Exception as exc:  # noqa: BLE001 — orient must not crash on auxiliary reads
        standing_orders = [{"error": f"standing_orders unavailable: {exc}"}]
    if compact_orient:
        standing_orders = [
            {
                **order,
                "rationale": (
                    str(order.get("rationale") or "")[:360] + "...[truncated]"
                    if len(str(order.get("rationale") or "")) > 360
                    else order.get("rationale")
                ),
            }
            if isinstance(order, dict)
            else order
            for order in standing_orders
        ]
    authority_envelope = _build_orient_authority_envelope(
        subs,
        standing_orders=standing_orders,
        health_payload=health_payload,
        dependency_truth=dependency_truth,
        tool_guidance=tool_guidance,
        fast=fast_orient,
    )
    try:
        from runtime.setup_wizard import setup_payload

        runtime_setup = setup_payload("doctor", repo_root=REPO_ROOT, authority_surface="api")
    except Exception as exc:  # noqa: BLE001 — orient must report setup drift instead of hiding it
        runtime_setup = {"error": f"runtime setup unavailable: {exc}"}

    return {
        "platform": "praxis-workflow",
        "brand": "Praxis Engine",
        "version": "1.0.0",
        "instruction_authority": {
            "kind": "orient_instruction_authority",
            "authority": "surfaces.api.handlers.workflow_admin._handle_orient",
            "lane": "native_operator",
            "packet_read_order": [
                "standing_orders",
                "authority_envelope",
                "tool_guidance",
                "primitive_contracts",
                "roadmap_truth",
                "queue_refs",
                "current_state_notes",
                "health",
                "recent_activity",
            ],
            "downstream_truth_surfaces": {
                "standing_orders": "/orient#standing_orders",
                "tool_guidance": "/orient#tool_guidance",
                "primitive_contracts": "/orient#primitive_contracts",
                "roadmap_truth": "/api/operator/roadmap/tree/{root_roadmap_item_id}",
                "queue_refs_and_current_state_notes": (
                    "surfaces.api.native_operator_surface.query_native_operator_surface"
                ),
                "run_status": "/api/workflow-runs/{run_id}/status",
            },
            "directive": (
                "Treat /orient as the canonical instruction authority for this lane. "
                "Read standing_orders first — they are active architecture-policy rows "
                "from operator authority and bind this session's behavior. "
                "Downstream packets should read roadmap-backed truth, queue refs, and current-state "
                "notes before using repo files or prior chat state."
            ),
        },
        "standing_orders": standing_orders,
        "authority_envelope": authority_envelope,
        "native_instance": authority_envelope.get("native_instance"),
        "runtime_setup": runtime_setup,
        "runtime_target": runtime_setup.get("runtime_target")
        if isinstance(runtime_setup, dict)
        else None,
        "empty_thin_sandbox_default": runtime_setup.get("empty_thin_sandbox_default")
        if isinstance(runtime_setup, dict)
        else None,
        "tool_guidance": tool_guidance,
        "primitive_contracts": authority_envelope.get("primitive_contracts"),
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
            "operator_decisions",
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
                    "command": "workflow tools describe <tool|alias|entrypoint>",
                    "description": "Inspect one tool's schema, risk, badges, and example payloads before calling it.",
                    "examples": [
                        "workflow tools describe praxis_query",
                        "workflow tools describe praxis_workflow",
                    ],
                },
                {
                    "command": "workflow tools call <tool|alias|entrypoint> --input-json '{...}'",
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
            "`workflow tools search <text>`, and `workflow tools describe <tool|alias|entrypoint>` when you need the current "
            "surface instead of memorizing a static list. Use `--exact` when you already know the alias or entrypoint.\n"
            f"For common reads, go straight to `{query_tool.cli_entrypoint}`, `{health_tool.cli_entrypoint}`, "
            f"`{discover_tool.cli_entrypoint}`, `{recall_tool.cli_entrypoint}`, `{bugs_tool.cli_entrypoint}`, "
            "and `workflow architecture scan` when you need exact boundary evidence.\n"
            "Use `workflow tools call <tool|alias|entrypoint> --input-json '{...}'` as the generic fallback when no direct alias fits.\n"
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
                "launch": "praxis launch — start cockpit Docker services, probe launcher readiness, and open the launcher",
                "doctor": "praxis doctor --json — emit launcher readiness as JSON",
                "status": "praxis status — show Docker service state and semantic readiness",
                "restart": "praxis restart [semantic|api|workflow-api|worker|scheduler] — restart services",
                "stop": "praxis stop — stop all services",
                "logs": "praxis logs [semantic|api|workflow-api|worker|scheduler] — tail logs",
            },
            "services": [
                {"label": "postgres", "port": 5432, "managed_by": "external"},
                {"label": "semantic-backend", "port": 8421, "managed_by": "docker-compose"},
                {"label": "api-server", "port": 8420, "managed_by": "docker-compose"},
                {"label": "workflow-worker", "managed_by": "docker-compose profile: worker"},
                {"label": "scheduler", "interval_sec": 60, "managed_by": "docker-compose"},
            ],
            "notes": "scripts/praxis is the preferred launcher entrypoint; scripts/praxis-ctl remains a compatibility alias. Docker Compose owns cockpit services by default; workflow-worker is an explicit execution-node profile and Postgres is external authority via WORKFLOW_DATABASE_URL. Native launchd install/setup control has been removed. Launcher and docs endpoints are projected by /orient#primitive_contracts.runtime_binding.http_endpoints.",
        },
}


def _handle_health(subs: Any, body: dict[str, Any]) -> dict[str, Any]:
    skip_transport_support = bool(body.get("skip_transport_support"))
    hs_mod = subs.get_health_mod()
    dependency_truth = dependency_truth_report(scope="all")

    db_url = _workflow_env(subs)["WORKFLOW_DATABASE_URL"]
    probes: list[Any] = [
        hs_mod.PostgresProbe(db_url),
        hs_mod.PostgresConnectivityProbe(db_url),
        hs_mod.DiskSpaceProbe(str(REPO_ROOT)),
    ]
    surface_usage_recorder = surface_usage_recorder_health()
    if surface_usage_recorder.get("authority_ready") is False:
        probes.append(
            hs_mod.StaticHealthProbe(
                name="surface_usage_recorder",
                passed=False,
                message=f"surface usage recorder degraded: {surface_usage_recorder.get('last_error') or 'unknown error'}",
                status="failed",
                details=surface_usage_recorder,
            )
        )
    if skip_transport_support:
        transport_support = []
        transport_support_summary = {
            "default_provider_slug": "",
            "default_adapter_type": "",
            "registered_providers": [],
            "providers": [],
            "support_basis": "skipped:orient_fast_path",
            "probe_targets": [],
        }
        provider_registry = {"status": "skipped", "reason": "orient_fast_path"}
    else:
        transport_support = query_transport_support(
            health_mod=hs_mod,
            pg=subs.get_pg_conn(),
        )
        transport_support_summary = build_transport_support_summary(transport_support)
        try:
            provider_registry = provider_registry_health()
        except Exception as exc:
            provider_registry_error = f"{type(exc).__name__}: {exc}"
            provider_registry = {
                "status": "load_failed",
                "error": provider_registry_error,
                "authority_available": False,
                "fallback_active": False,
            }
            probes.append(
                hs_mod.StaticHealthProbe(
                    name="provider_registry",
                    passed=False,
                    message=f"provider registry load failed: {provider_registry_error}",
                    status="failed",
                    details=provider_registry,
                )
            )
    for provider_slug, adapter_type in transport_support_summary.get("probe_targets", []):
        probes.append(hs_mod.ProviderTransportProbe(provider_slug, adapter_type))

    runner = hs_mod.PreflightRunner(probes)
    preflight = runner.run()

    panel = subs.get_operator_panel()
    snap = panel.snapshot()
    lane = panel.recommend_lane()
    proof_payload: dict[str, Any]
    if skip_transport_support:
        proof_payload = {"status": "skipped", "reason": "orient_fast_path"}
        content_health = {"status": "skipped", "reason": "orient_fast_path"}
    else:
        try:
            from runtime.receipt_store import proof_metrics

            proof_payload = proof_metrics(
                since_hours=int(body.get("since_hours") or 0),
            )
        except Exception as exc:
            proof_payload = {"error": f"Could not compute proof metrics: {exc}"}

        try:
            memory_engine = getattr(subs, "get_memory_engine", lambda: None)()
            content_health = build_content_health_report(memory_engine)
        except Exception as exc:
            content_health = {"status": "error", "reason": str(exc)}

    schema_authority: dict[str, Any]
    if skip_transport_support:
        schema_authority = {"status": "skipped", "reason": "orient_fast_path"}
    else:
        try:
            status = workflow_database_status(env=_workflow_env(subs))
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
        "content_health": content_health,
        "schema_authority": schema_authority,
        "surface_usage_recorder": surface_usage_recorder,
        "dependency_truth": dependency_truth,
        "transport_support_summary": {
            "default_provider_slug": transport_support_summary["default_provider_slug"],
            "default_adapter_type": transport_support_summary["default_adapter_type"],
            "registered_providers": list(transport_support_summary["registered_providers"]),
            "providers": list(transport_support_summary["providers"]),
            "support_basis": transport_support_summary["support_basis"],
            "provider_registry_status": provider_registry.get("status"),
            "provider_registry_authority_available": provider_registry.get("authority_available"),
            "provider_registry_fallback_active": provider_registry.get("fallback_active"),
        },
        "provider_registry": provider_registry,
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
        from .workflow_run import _handle_status

        status_data = _handle_status(request.subsystems, {"since_hours": 24})
        recent_records = _list_receipts(limit=20)
        from runtime.primitive_contracts import bug_status_sql_in_literal

        degraded_sources: dict[str, str] = {}

        def _fetch_count(query: str, source: str) -> int:
            try:
                return int(pg.fetchval(query) or 0)
            except Exception as exc:  # noqa: BLE001 - overview must degrade per source.
                degraded_sources[source] = f"{type(exc).__name__}: {exc}"
                return 0

        def _execute_rows(query: str, source: str) -> list[Any]:
            try:
                return list(pg.execute(query))
            except Exception as exc:  # noqa: BLE001 - overview must keep other ticket metrics usable.
                degraded_sources[source] = f"{type(exc).__name__}: {exc}"
                return []

        recent = [
            {
                "label": record.label,
                "agent": record.agent,
                "status": record.status,
                "timestamp": record.timestamp,
            }
            for record in recent_records
        ]
        models = _execute_rows(
            """
            SELECT DISTINCT ON (provider_slug, model_slug)
                   provider_slug || '/' || model_slug AS name,
                   capability_tags AS tags,
                   route_tier,
                   latency_class
            FROM provider_model_candidates
            WHERE status = 'active'
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
            """,
            "provider_model_candidates",
        )
        bug_sev = _execute_rows(
            "SELECT severity as code, COUNT(*) as count FROM bugs GROUP BY severity ORDER BY count DESC LIMIT 8",
            "bugs_by_severity",
        )
        request._send_json(
            200,
            {
                "pass_rate": status_data.get("pass_rate", 0),
                "total_workflows": status_data.get("total_workflows", 0),
                "total_tables": _fetch_count(
                    "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'",
                    "pg_tables",
                ),
                "total_bugs": _fetch_count("SELECT COUNT(*) FROM bugs", "bugs_total"),
                "open_bugs": _fetch_count(
                    "SELECT COUNT(*) FROM bugs WHERE "
                    + bug_status_sql_in_literal("open"),
                    "bugs_open",
                ),
                "total_workflow_runs": _fetch_count(
                    "SELECT COUNT(*) FROM public.workflow_runs",
                    "workflow_runs",
                ),
                "total_registry_items": _fetch_count(
                    "SELECT COUNT(*) FROM platform_registry",
                    "platform_registry",
                ),
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
                "observability_state": "degraded" if degraded_sources else "ready",
                "degraded_sources": degraded_sources,
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
        from surfaces.api.handlers import workflow_launcher

        request._send_json(200, workflow_launcher.launcher_status_payload())
    except workflow_launcher.LauncherAuthorityError as exc:
        request._send_json(503, {"error": str(exc)})


def _handle_setup_get(request: Any, path: str) -> None:
    try:
        mode = path.rsplit("/", 1)[-1]
        if mode == "graph":
            from runtime.setup_wizard import setup_graph_payload

            request._send_json(
                200,
                setup_graph_payload(repo_root=REPO_ROOT, authority_surface="api"),
            )
            return
        if mode not in {"doctor", "plan"}:
            request._send_json(404, {"error": "unknown setup mode"})
            return
        from runtime.setup_wizard import setup_payload

        request._send_json(200, setup_payload(mode, repo_root=REPO_ROOT, authority_surface="api"))
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


def _handle_setup_apply_post(request: Any, path: str) -> None:
    del path
    try:
        body = _read_json_body(request)
        from runtime.setup_wizard import setup_payload

        approved = bool(body.get("yes") or body.get("apply") or body.get("approved"))
        payload = setup_payload("apply", repo_root=REPO_ROOT, apply=approved, authority_surface="api")
        status_code = 200 if payload.get("ok") else (501 if approved else 409)
        request._send_json(status_code, payload)
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})


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

    from surfaces.api.handlers import workflow_launcher

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
    (_exact("/api/setup/apply"), _handle_setup_apply_post),
    (_exact("/api/launcher/recover"), _handle_launcher_recover_post),
]

ADMIN_GET_ROUTES: list[RouteEntry] = [
    (_exact("/"), _handle_root_get),
    (_exact("/api/platform-overview"), _handle_platform_overview_get),
    (_exact("/api/launcher/status"), _handle_launcher_status_get),
    (_exact("/api/setup/doctor"), _handle_setup_get),
    (_exact("/api/setup/plan"), _handle_setup_get),
    (_exact("/api/setup/graph"), _handle_setup_get),
    (_exact("/api/workflow-templates"), _handle_workflow_templates_get),
]

ADMIN_ROUTES: dict[str, object] = {
    "/orient": _handle_orient,
    "/health": _handle_health,
    "/governance": _handle_governance,
}


__all__ = [
    "ADMIN_GET_ROUTES",
    "ADMIN_POST_ROUTES",
    "ADMIN_ROUTES",
    "_handle_health",
]
