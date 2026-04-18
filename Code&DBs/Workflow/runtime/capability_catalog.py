"""DB-backed capability catalog authority for compile-time selection."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from runtime.integrations.display_names import display_name_for_integration

logger = logging.getLogger(__name__)


class CapabilityCatalogError(RuntimeError):
    """Raised when capability authority rows are missing or malformed."""


class _TaskCapability:
    mechanical_edit = "mechanical_edit"
    code_generation = "code_generation"
    code_review = "code_review"
    architecture = "architecture"
    analysis = "analysis"
    creative = "creative"
    research = "research"
    debug = "debug"


_TASK_CAPABILITY_ROWS: tuple[dict[str, Any], ...] = (
    {
        "capability_ref": "cap-task-code-generation",
        "capability_slug": _TaskCapability.code_generation,
        "capability_kind": "task",
        "title": "Code generation",
        "summary": "Implement or extend code from requirements.",
        "description": "Use when the work is primarily about building new code, tests, scripts, or implementation changes.",
        "route": "task/code_generation",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "build",
            "implement",
            "create",
            "generate",
            "write",
            "function",
            "class",
            "module",
            "script",
            "test",
            "code",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.code_generation.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-mechanical-edit",
        "capability_slug": _TaskCapability.mechanical_edit,
        "capability_kind": "task",
        "title": "Mechanical edit",
        "summary": "Perform bounded code edits and structural refactors.",
        "description": "Use when the work is mostly renames, formatting, file-local fixes, or mechanical transformations.",
        "route": "task/mechanical_edit",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "fix",
            "edit",
            "rename",
            "format",
            "refactor",
            "mechanical",
            "cleanup",
            "patch",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.mechanical_edit.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-code-review",
        "capability_slug": _TaskCapability.code_review,
        "capability_kind": "task",
        "title": "Code review",
        "summary": "Inspect code for correctness, regressions, and risk.",
        "description": "Use when the work is reviewing existing code, checking for bugs, or assessing quality.",
        "route": "task/code_review",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "review",
            "audit",
            "check",
            "lint",
            "inspect",
            "quality",
            "regression",
            "risk",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.code_review.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-architecture",
        "capability_slug": _TaskCapability.architecture,
        "capability_kind": "task",
        "title": "Architecture",
        "summary": "Design systems, contracts, and multi-component changes.",
        "description": "Use when the work is mainly system design, schema definition, planning, or interface shaping.",
        "route": "task/architecture",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "design",
            "architect",
            "plan",
            "schema",
            "structure",
            "contract",
            "interface",
            "system",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.architecture.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-analysis",
        "capability_slug": _TaskCapability.analysis,
        "capability_kind": "task",
        "title": "Analysis",
        "summary": "Evaluate, compare, score, and synthesize information.",
        "description": "Use when the work is mainly analysis, ranking, scoring, or comparative evaluation.",
        "route": "task/analysis",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "analyze",
            "analyse",
            "evaluate",
            "assess",
            "score",
            "rank",
            "compare",
            "synthesize",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.analysis.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-creative",
        "capability_slug": _TaskCapability.creative,
        "capability_kind": "task",
        "title": "Creative writing",
        "summary": "Draft communication and creative text artifacts.",
        "description": "Use when the work is composing outreach, narrative copy, or creative communication.",
        "route": "task/creative",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "draft",
            "email",
            "outreach",
            "compose",
            "copywrite",
            "copy",
            "message",
            "creative",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.creative.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-research",
        "capability_slug": _TaskCapability.research,
        "capability_kind": "task",
        "title": "Research",
        "summary": "Gather information and synthesize findings.",
        "description": "Use when the work is searching, discovering, summarizing, or gathering evidence.",
        "route": "task/research",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "research",
            "discover",
            "search",
            "find",
            "summarize",
            "summarise",
            "gather",
            "evidence",
            "brief",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.research.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
    {
        "capability_ref": "cap-task-debug",
        "capability_slug": _TaskCapability.debug,
        "capability_kind": "task",
        "title": "Debugging",
        "summary": "Diagnose failures and trace problems to root cause.",
        "description": "Use when the work is investigating errors, tracing failures, or diagnosing runtime issues.",
        "route": "task/debug",
        "engines": ["minimal_intent_compile"],
        "signals": [
            "debug",
            "diagnose",
            "trace",
            "failure",
            "bug",
            "error",
            "fix",
            "broken",
        ],
        "reference_slugs": [],
        "enabled": True,
        "binding_revision": "binding.capability_catalog.task.debug.20260409",
        "decision_ref": "decision.capability_catalog.task.default",
    },
)


def build_capability_catalog_rows(integrations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the canonical capability catalog seed set from live integrations."""
    catalog: list[dict[str, Any]] = [dict(row) for row in _TASK_CAPABILITY_ROWS] + [
        {
            "capability_ref": "cap-research-local-knowledge",
            "capability_slug": "research/local-knowledge",
            "capability_kind": "memory",
            "title": "Local knowledge recall",
            "summary": "Search prior findings and saved research before going outbound.",
            "description": "Uses praxis_research and the local research runtime to search existing findings and compile briefs before new work starts.",
            "route": "praxis_research",
            "engines": ["praxis_research", "memory.research_runtime"],
            "signals": ["research", "findings", "knowledge", "brief", "prior", "existing", "history", "recall"],
            "reference_slugs": [],
            "enabled": True,
            "binding_revision": "binding.capability_catalog.bootstrap.20260408",
            "decision_ref": "decision.capability_catalog.bootstrap.20260408",
        },
        {
            "capability_ref": "cap-research-fan-out",
            "capability_slug": "research/fan-out",
            "capability_kind": "fanout",
            "title": "Parallel research fan-out (API burst)",
            "summary": "Burst N parallel Haiku workers over a research prompt.",
            "description": "Count-based SLM burst via runtime fan_out dispatch. API providers only; CLI adapters are rejected because they break under concurrency bursts.",
            "route": "workflow.fanout",
            "engines": ["fan_out_dispatch", "claude-haiku-4-5-20251001"],
            "signals": ["parallel", "fan out", "burst", "haiku", "workers", "broad", "sweep"],
            "reference_slugs": [],
            "enabled": True,
            "binding_revision": "binding.capability_catalog.bootstrap.20260408",
            "decision_ref": "decision.capability_catalog.bootstrap.20260408",
        },
        {
            "capability_ref": "cap-research-loop",
            "capability_slug": "research/loop",
            "capability_kind": "loop",
            "title": "Research loop (item-based)",
            "summary": "Run a research step over each item in a list.",
            "description": "Item-based parallel map via runtime loop dispatch. Any provider is allowed; one spec per item with templated prompt substitution.",
            "route": "workflow.loop",
            "engines": ["loop_dispatch"],
            "signals": ["for each", "loop", "iterate", "per item", "per lead", "per url", "map"],
            "reference_slugs": [],
            "enabled": True,
            "binding_revision": "binding.capability_catalog.loop_split.20260418",
            "decision_ref": "decision.capability_catalog.loop_split.20260418",
        },
        {
            "capability_ref": "cap-research-gemini-cli",
            "capability_slug": "research/gemini-cli",
            "capability_kind": "cli",
            "title": "Internet docs research",
            "summary": "Use the Gemini CLI lane for outbound web and official-docs research when local context is not enough.",
            "description": "Uses the Gemini CLI provider lane exposed by the runtime planner and executor for outbound internet scans, official API docs lookup, and source-backed research passes.",
            "route": "google/gemini-cli",
            "engines": ["gemini-cli"],
            "signals": [
                "gemini",
                "cli",
                "scan",
                "external",
                "web",
                "search",
                "browse",
                "online",
                "internet",
                "brave",
                "api docs",
                "official docs",
                "documentation",
                "source urls",
            ],
            "reference_slugs": [],
            "enabled": True,
            "binding_revision": "binding.capability_catalog.bootstrap.20260408",
            "decision_ref": "decision.capability_catalog.bootstrap.20260408",
        },
    ]

    for integration in integrations:
        integration_id = _slugify(integration.get("id") or integration.get("name"))
        integration_name = _as_text(integration.get("display_name")) or display_name_for_integration(integration)
        if not integration_id:
            continue
        for capability in integration.get("capabilities", []):
            action = _slugify(capability.get("action")) if isinstance(capability, dict) else _slugify(capability)
            description = (
                _as_text(capability.get("description")) if isinstance(capability, dict) else ""
            ) or _as_text(integration.get("description"))
            if not action or not _looks_like_research_action(action, description):
                continue
            catalog.append(
                {
                    "capability_ref": f"cap-{integration_id}-{action}",
                    "capability_slug": f"tool/{integration_id}/{action}",
                    "capability_kind": "integration",
                    "title": f"{integration_name} {action.replace('-', ' ').replace('_', ' ')}",
                    "summary": description or f"Use {integration_name} for {action.replace('-', ' ')}.",
                    "description": description or f"Connected integration capability @{integration_id}/{action}.",
                    "route": f"@{integration_id}/{action}",
                    "engines": [integration_name],
                    "signals": [
                        integration_id,
                        integration_name.lower(),
                        action.replace("-", " "),
                        (description or "").lower(),
                    ],
                    "reference_slugs": [f"@{integration_id}/{action}", f"@{integration_id}"],
                    "enabled": True,
                    "binding_revision": f"binding.capability_catalog.integration.{integration_id}.{action}",
                    "decision_ref": "decision.capability_catalog.integration.seed",
                }
            )

    return catalog


def sync_capability_catalog(
    conn: Any,
    *,
    integrations: list[dict[str, Any]] | None = None,
) -> int:
    """Best-effort upsert of capability authority rows."""
    if conn is None:
        return 0

    try:
        integrations = integrations if integrations is not None else _load_integrations(conn)
    except Exception as exc:
        logger.warning("capability catalog source load failed: %s", exc)
        return 0

    if not _capability_catalog_columns(conn):
        return 0

    rows = build_capability_catalog_rows(integrations)
    if not rows:
        return 0

    try:
        conn.execute_many(
            """
            INSERT INTO capability_catalog (
                capability_ref,
                capability_slug,
                capability_kind,
                title,
                summary,
                description,
                route,
                engines,
                signals,
                reference_slugs,
                enabled,
                binding_revision,
                decision_ref
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8::jsonb, $9::jsonb, $10::jsonb, $11, $12, $13
            )
            ON CONFLICT (capability_slug) DO UPDATE SET
                capability_kind = EXCLUDED.capability_kind,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                description = EXCLUDED.description,
                route = EXCLUDED.route,
                engines = EXCLUDED.engines,
                signals = EXCLUDED.signals,
                reference_slugs = EXCLUDED.reference_slugs,
                enabled = EXCLUDED.enabled,
                binding_revision = EXCLUDED.binding_revision,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            [
                (
                    row["capability_ref"],
                    row["capability_slug"],
                    row["capability_kind"],
                    row["title"],
                    row["summary"],
                    row["description"],
                    row["route"],
                    json.dumps(row["engines"]),
                    json.dumps(row["signals"]),
                    json.dumps(row["reference_slugs"]),
                    row["enabled"],
                    row["binding_revision"],
                    row["decision_ref"],
                )
                for row in rows
            ],
        )
    except Exception as exc:
        logger.warning("capability catalog sync failed: %s", exc)
        return 0

    return len(rows)


def load_capability_catalog(conn: Any) -> list[dict[str, Any]]:
    """Load canonical capability authority rows from Postgres."""
    if conn is None:
        raise CapabilityCatalogError("capability catalog requires Postgres authority")

    rows = conn.execute(
        """
        SELECT capability_ref,
               capability_slug,
               capability_kind,
               title,
               summary,
               description,
               route,
               engines,
               signals,
               reference_slugs,
               enabled,
               binding_revision,
               decision_ref
          FROM capability_catalog
         WHERE enabled = TRUE
         ORDER BY capability_kind, title, capability_slug
        """
    )
    catalog = [_row_to_entry(dict(row)) for row in rows or []]
    if not catalog:
        raise CapabilityCatalogError("capability_catalog authority rows are missing")
    return catalog


def select_capability_catalog_entries(
    conn: Any,
    *,
    description: str,
    stage: str | None = None,
    label: str | None = None,
    write_scope: list[str] | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Select task capabilities from the catalog for a minimal intent."""
    catalog = load_capability_catalog(conn)
    entries = _rank_capability_entries(
        catalog,
        description=description,
        stage=stage,
        label=label,
        write_scope=write_scope,
        limit=limit,
    )
    if not entries:
        raise CapabilityCatalogError(
            "capability_catalog has no matching task authority rows for the requested intent"
        )
    return entries


def _row_to_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _as_text(row.get("capability_ref")),
        "capability_ref": _as_text(row.get("capability_ref")),
        "slug": _as_text(row.get("capability_slug")),
        "capability_slug": _as_text(row.get("capability_slug")),
        "kind": _as_text(row.get("capability_kind")),
        "capability_kind": _as_text(row.get("capability_kind")),
        "title": _as_text(row.get("title")),
        "summary": _as_text(row.get("summary")),
        "description": _as_text(row.get("description")),
        "route": _as_text(row.get("route")),
        "engines": _json_list(row.get("engines")),
        "signals": _json_list(row.get("signals")),
        "reference_slugs": _json_list(row.get("reference_slugs")),
        "enabled": bool(row.get("enabled")),
        "binding_revision": _as_text(row.get("binding_revision")),
        "decision_ref": _as_text(row.get("decision_ref")),
    }


def _rank_capability_entries(
    catalog: list[dict[str, Any]],
    *,
    description: str,
    stage: str | None,
    label: str | None,
    write_scope: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    tokens = _tokenize(
        " ".join(
            part
            for part in (
                stage or "",
                label or "",
                description or "",
                " ".join(write_scope or ()),
            )
            if part
        )
    )
    if not tokens:
        return []

    scored: list[tuple[int, str, dict[str, Any]]] = []
    for row in catalog:
        if row.get("capability_kind") != "task":
            continue
        row_tokens = _capability_tokens(row)
        overlap = tokens & row_tokens
        if not overlap:
            continue
        score = len(overlap) * 10
        if stage and stage in row_tokens:
            score += 15
        slug = _as_text(row.get("capability_slug"))
        if slug in tokens:
            score += 20
        if _as_text(row.get("title")).lower() in " ".join(sorted(tokens)):
            score += 5
        scored.append((score, slug, row))

    scored.sort(key=lambda item: (-item[0], item[1]))
    ranked = [dict(item[2]) for item in scored[: max(limit, 1)]]
    if ranked:
        return ranked

    default_entry = next(
        (dict(row) for row in catalog if row.get("capability_slug") == _TaskCapability.code_generation),
        None,
    )
    return [default_entry] if default_entry else []


def _capability_catalog_columns(conn: Any) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_name = 'capability_catalog'
        """
    )
    return {_as_text(row.get("column_name")) for row in rows or [] if _as_text(row.get("column_name"))}


def _load_integrations(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, provider, capabilities, auth_status, description
          FROM integration_registry
         ORDER BY name
        """
    )
    integrations: list[dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        raw_capabilities = item.get("capabilities")
        if isinstance(raw_capabilities, str):
            try:
                raw_capabilities = json.loads(raw_capabilities)
            except (json.JSONDecodeError, TypeError):
                raw_capabilities = []
        capabilities: list[dict[str, Any]] = []
        for capability in raw_capabilities or []:
            if isinstance(capability, str):
                action = _slugify(capability)
                if action:
                    capabilities.append({"action": action})
                continue
            if isinstance(capability, dict):
                action = _slugify(capability.get("action"))
                if not action:
                    continue
                capabilities.append(
                    {
                        "action": action,
                        "description": _as_text(capability.get("description")),
                        "inputs": capability.get("inputs") if isinstance(capability.get("inputs"), list) else [],
                        "requiredArgs": capability.get("requiredArgs")
                        if isinstance(capability.get("requiredArgs"), list)
                        else [],
                    }
                )
        integrations.append(
            {
                "id": _slugify(item.get("id")),
                "name": _as_text(item.get("name")),
                "provider": _as_text(item.get("provider")),
                "auth_status": _as_text(item.get("auth_status")),
                "description": _as_text(item.get("description")),
                "capabilities": capabilities,
            }
        )
    return integrations


def _looks_like_research_action(action: str, description: str) -> bool:
    haystack = f"{action} {description}".lower()
    return any(
        token in haystack
        for token in (
            "search",
            "query",
            "intel",
            "research",
            "review opportunities",
            "review-opportunities",
            "find",
            "receipt",
        )
    )


def _slugify(value: object) -> str:
    text = _as_text(value).strip().lower()
    if not text:
        return ""
    slug = []
    last_was_dash = False
    for char in text:
        if char.isalnum():
            slug.append(char)
            last_was_dash = False
        elif char in {" ", "-", "_", "/", "."}:
            if not last_was_dash:
                slug.append("-")
                last_was_dash = True
        elif char == "@":
            continue
    return "".join(slug).strip("-")


def _as_text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _json_list(value: object) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
    return list(value) if isinstance(value, list) else []


def _capability_tokens(row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in ("capability_slug", "capability_kind", "title", "summary", "description", "route"):
        tokens.update(_tokenize(_as_text(row.get(key))))
    for signal in _json_list(row.get("signals")):
        tokens.update(_tokenize(_as_text(signal)))
    return tokens


def _tokenize(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9_/-]+", (text or "").lower()) if token}


__all__ = [
    "CapabilityCatalogError",
    "build_capability_catalog_rows",
    "load_capability_catalog",
    "select_capability_catalog_entries",
    "sync_capability_catalog",
]
