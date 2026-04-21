"""Atlas graph read model for the Praxis knowledge graph.

This module is the runtime authority for Atlas graph assembly. UI and export
surfaces should consume this read model instead of rebuilding graph state in a
script or scraping the generated HTML artifact.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime._workflow_database import resolve_runtime_database_url
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

REPO_ROOT = Path(__file__).resolve().parents[3]
HEURISTIC_MAP_PATH = Path("/tmp/atlas_heuristic_map.json")
FRESHNESS_LAG_TOLERANCE_SECONDS = 2.0

# One color per area. Keep this here so the API, app, and static export share
# the same visual semantics.
AREA_COLORS = {
    "compiler": "#7aa2f7",
    "scheduler": "#bb9af7",
    "sandbox": "#9ece6a",
    "routing": "#e0af68",
    "circuits": "#f7768e",
    "outbox": "#2ac3de",
    "receipts": "#73daca",
    "memory": "#c0caf5",
    "bugs": "#db4b4b",
    "roadmap": "#ff9e64",
    "authority": "#a9b1d6",
    "build": "#b4f9f8",
    "governance": "#e0dc8f",
    "heal": "#9ece6a",
    "discover": "#41a6b5",
    "debate": "#f7768e",
    "moon": "#ad8ee6",
    "mcp": "#7dcfff",
    "cli": "#cfc9c2",
    "integrations": "#ff9e64",
}
UNOWNED_COLOR = "#4a5068"

SCHEMA_AREA_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "authority",
        (
            "operator_",
            "authority_",
            "object_",
            "semantic_",
            "registry_",
            "persona_",
            "native_runtime",
            "workspace_authority",
            "runtime_profile_authority",
        ),
    ),
    ("build", ("workflow_build_", "review_", "app_manifest", "manifest", "shape_family")),
    (
        "scheduler",
        (
            "workflow_run",
            "run_node",
            "run_edge",
            "schedule_",
            "recurring_",
            "control_command",
            "workflow_chain",
            "workflow_job_runtime",
        ),
    ),
    ("compiler", ("workflow_definition", "workflow_version", "uploaded_file", "compiler_", "workflow_trigger")),
    ("sandbox", ("sandbox_", "execution_packet", "fork_", "worktree")),
    ("routing", ("provider_", "model_", "route_", "adapter_", "task_type_", "market_")),
    ("circuits", ("quality_", "failure_", "gate_", "eligibility", "health")),
    (
        "outbox",
        (
            "outbox",
            "event_log",
            "workflow_event",
            "system_event",
            "subscription_",
            "idempotency_ledger",
        ),
    ),
    ("receipts", ("receipt", "provenance", "probe_")),
    ("memory", ("memory_", "context_", "reference_catalog", "semantic_predicate", "semantic_assertion")),
    ("bugs", ("bug", "issue", "evidence_link")),
    ("roadmap", ("roadmap_", "cutover_", "work_item_")),
    ("governance", ("credential_", "promotion_", "policy", "verification_", "verifier_")),
    ("heal", ("healing_", "healer_", "retry")),
    ("discover", ("discover", "search", "compile_index")),
    ("debate", ("debate_", "adversarial")),
    ("moon", ("moon_", "dashboard", "surface_catalog")),
    ("mcp", ("mcp_", "operation_catalog", "tool_", "capability_catalog", "registry_calculation")),
    ("cli", ("cli_", "agent_", "render", "native_operator")),
    ("integrations", ("integration", "connector_", "webhook_", "oauth", "api_schema")),
)

AUTHORITY_PROJECTION_SOURCE_CLOCK_SQL = """
SELECT max(source_updated_at) AS source_updated_at
  FROM (
    SELECT max(updated_at) AS source_updated_at FROM roadmap_items
    UNION ALL SELECT max(created_at) FROM roadmap_items
    UNION ALL SELECT max(completed_at) FROM roadmap_items
    UNION ALL SELECT max(created_at) FROM roadmap_item_dependencies
    UNION ALL SELECT max(updated_at) FROM operator_object_relations
    UNION ALL SELECT max(created_at) FROM operator_object_relations
    UNION ALL SELECT max(updated_at) FROM bugs
    UNION ALL SELECT max(created_at) FROM bugs
    UNION ALL SELECT max(updated_at) FROM workflow_build_intents
    UNION ALL SELECT max(created_at) FROM workflow_build_intents
    UNION ALL SELECT max(created_at) FROM bug_evidence_links
    UNION ALL SELECT max(updated_at) FROM workflow_chains
    UNION ALL SELECT max(created_at) FROM workflow_chains
    UNION ALL SELECT max(started_at) FROM workflow_chains
    UNION ALL SELECT max(updated_at) FROM workflow_chain_waves
    UNION ALL SELECT max(created_at) FROM workflow_chain_waves
    UNION ALL SELECT max(started_at) FROM workflow_chain_waves
    UNION ALL SELECT max(completed_at) FROM workflow_chain_waves
    UNION ALL SELECT max(updated_at) FROM workflow_chain_wave_runs
    UNION ALL SELECT max(created_at) FROM workflow_chain_wave_runs
    UNION ALL SELECT max(started_at) FROM workflow_chain_wave_runs
    UNION ALL SELECT max(completed_at) FROM workflow_chain_wave_runs
    UNION ALL SELECT max(updated_at) FROM issues
    UNION ALL SELECT max(created_at) FROM issues
    UNION ALL SELECT max(updated_at) FROM operator_decisions
    UNION ALL SELECT max(created_at) FROM operator_decisions
    UNION ALL SELECT max(decided_at) FROM operator_decisions
  ) source_clocks
"""


def infer_schema_area(node_id: str, label: str, etype: str, preview: str, source: str) -> str | None:
    if etype != "table" and not node_id.startswith("table:"):
        return None
    haystack = " ".join((node_id, label, preview, source)).lower()
    for area, markers in SCHEMA_AREA_RULES:
        if any(marker in haystack for marker in markers):
            return area
    return None


def fetch_entities(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT id, entity_type, name, COALESCE(LEFT(content, 300), '') AS content_preview,
                   COALESCE(source, '') AS source
              FROM memory_entities
             WHERE NOT archived
               AND name IS NOT NULL AND name <> ''
               AND entity_type NOT IN ('task', 'fact')
            """
        )
    ]


def fetch_edges(conn: SyncPostgresConnection, known_ids: set[str]) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT source_id, target_id, relation_type, weight
          FROM memory_edges
         WHERE active = true
        """
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        if r["source_id"] in known_ids and r["target_id"] in known_ids:
            out.append(dict(r))
    return out


def fetch_capabilities(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT capability_slug, capability_kind, title, summary, route
              FROM capability_catalog
             WHERE enabled = true
            """
        )
    ]


def fetch_functional_areas(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT area_slug, title, summary
              FROM functional_areas
             WHERE area_status = 'active'
            """
        )
    ]


def fetch_area_relations(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT source_kind, source_ref, target_ref, relation_kind
              FROM operator_object_relations
             WHERE target_kind = 'functional_area'
               AND relation_status = 'active'
            """
        )
    ]


def fetch_tools() -> list[dict[str, Any]]:
    try:
        from surfaces.mcp.catalog import get_tool_catalog

        out: list[dict[str, Any]] = []
        for slug, definition in get_tool_catalog().items():
            out.append(
                {
                    "slug": slug,
                    "display_name": definition.display_name,
                    "description": definition.description,
                    "surface": getattr(definition, "surface", ""),
                    "tier": getattr(definition, "cli_tier", ""),
                }
            )
        return out
    except Exception as exc:
        print(f"[atlas] catalog import failed ({exc}); using CLI fallback", file=sys.stderr)

    result = subprocess.run(
        ["praxis", "workflow", "tools", "list"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    out: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.startswith("praxis_"):
            continue
        parts = line.split()
        if not parts:
            continue
        out.append(
            {
                "slug": parts[0],
                "display_name": parts[0].removeprefix("praxis_").replace("_", " ").title(),
                "description": " ".join(parts[6:])[:200] if len(parts) > 6 else "",
                "surface": parts[3] if len(parts) > 3 else "",
                "tier": parts[4] if len(parts) > 4 else "",
            }
        )
    return out


def _workflow_database_url() -> str:
    return str(resolve_runtime_database_url(repo_root=REPO_ROOT))


def _as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: Any) -> str | None:
    timestamp = _as_utc(value)
    return None if timestamp is None else timestamp.isoformat()


def _seconds_between(start: Any, end: Any) -> float | None:
    start_at = _as_utc(start)
    end_at = _as_utc(end)
    if start_at is None or end_at is None:
        return None
    return max(0.0, (end_at - start_at).total_seconds())


def classify_graph_freshness(
    *,
    authority_source_updated_at: Any,
    authority_projection_last_run_at: Any,
    authority_projection_edge_count: int,
) -> tuple[str, float | None]:
    if authority_projection_edge_count <= 0 or _as_utc(authority_projection_last_run_at) is None:
        return "unknown", None
    lag_seconds = _seconds_between(authority_projection_last_run_at, authority_source_updated_at)
    if lag_seconds is not None and lag_seconds > FRESHNESS_LAG_TOLERANCE_SECONDS:
        return "projection_lagging", lag_seconds
    return "fresh", 0.0 if lag_seconds is not None else None


def fetch_graph_freshness(conn: SyncPostgresConnection) -> dict[str, Any]:
    memory_entities_updated_at = conn.fetchval(
        "SELECT max(updated_at) FROM memory_entities WHERE archived = false"
    )
    memory_edges_updated_at = conn.fetchval(
        """
        SELECT max(GREATEST(created_at, COALESCE(last_validated_at, created_at)))
          FROM memory_edges
         WHERE active = true
        """
    )
    authority_projection_last_run_at = conn.fetchval(
        """
        SELECT max(last_validated_at)
          FROM memory_edges
         WHERE active = true
           AND authority_class = 'canonical'
           AND provenance_kind = 'schema_projection'
        """
    )
    authority_projection_edge_count = int(
        conn.fetchval(
            """
            SELECT count(*)
              FROM memory_edges
             WHERE active = true
               AND authority_class = 'canonical'
               AND provenance_kind = 'schema_projection'
            """
        )
        or 0
    )
    authority_source_updated_at = conn.fetchval(AUTHORITY_PROJECTION_SOURCE_CLOCK_SQL)
    graph_freshness_state, projection_lag_seconds = classify_graph_freshness(
        authority_source_updated_at=authority_source_updated_at,
        authority_projection_last_run_at=authority_projection_last_run_at,
        authority_projection_edge_count=authority_projection_edge_count,
    )
    return {
        "graph_freshness_state": graph_freshness_state,
        "memory_entities_max_updated_at": _iso(memory_entities_updated_at),
        "memory_edges_max_updated_at": _iso(memory_edges_updated_at),
        "authority_projection_last_run_at": _iso(authority_projection_last_run_at),
        "authority_projection_source_max_updated_at": _iso(authority_source_updated_at),
        "authority_projection_lag_seconds": projection_lag_seconds,
        "authority_projection_edge_count": authority_projection_edge_count,
        "authority_projection_last_run_source": (
            "memory_edges.last_validated_at where authority_class='canonical' "
            "and provenance_kind='schema_projection'"
        ),
    }


def unknown_graph_freshness(error: Exception | None = None) -> dict[str, Any]:
    freshness: dict[str, Any] = {
        "graph_freshness_state": "unknown",
        "memory_entities_max_updated_at": None,
        "memory_edges_max_updated_at": None,
        "authority_projection_last_run_at": None,
        "authority_projection_source_max_updated_at": None,
        "authority_projection_lag_seconds": None,
        "authority_projection_edge_count": 0,
        "authority_projection_last_run_source": (
            "memory_edges.last_validated_at where authority_class='canonical' "
            "and provenance_kind='schema_projection'"
        ),
    }
    if error is not None:
        freshness["freshness_error"] = f"{type(error).__name__}: {error}"
    return freshness


def _connect(database_url: str | None = None) -> SyncPostgresConnection:
    return SyncPostgresConnection(
        get_workflow_pool(
            env={"WORKFLOW_DATABASE_URL": database_url or _workflow_database_url()},
        )
    )


def build_graph(*, database_url: str | None = None) -> dict[str, list[dict[str, Any]]]:
    conn = _connect(database_url)
    entities = fetch_entities(conn)
    capabilities = fetch_capabilities(conn)
    areas = fetch_functional_areas(conn)
    area_relations = fetch_area_relations(conn)
    entity_ids = {e["id"] for e in entities}
    edges = fetch_edges(conn, entity_ids)

    tools = fetch_tools()

    hmap: dict[str, dict[str, str]] = {"tools": {}, "tables": {}}
    if HEURISTIC_MAP_PATH.is_file():
        hmap = json.loads(HEURISTIC_MAP_PATH.read_text(encoding="utf-8"))

    node_area: dict[str, str] = {}

    for r in area_relations:
        area = str(r["target_ref"]).removeprefix("functional_area.")
        kind = str(r["source_kind"])
        ref = str(r["source_ref"])
        if kind in (
            "bug",
            "roadmap_item",
            "repo_path",
            "operator_decision",
            "functional_area",
            "workflow",
            "workflow_build_intent",
        ):
            node_area.setdefault(f"{kind}::{ref}", area)

    for tool_slug, area in hmap.get("tools", {}).items():
        node_area.setdefault(f"tool::{tool_slug}", area)
    for table_name, area in hmap.get("tables", {}).items():
        node_area.setdefault(f"table:{table_name}", area)

    nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()

    for area_row in areas:
        area_slug = str(area_row["area_slug"])
        area_id = f"area::{area_slug}"
        color = AREA_COLORS.get(area_slug, "#888")
        nodes.append(
            {
                "data": {
                    "id": area_id,
                    "label": area_row["title"],
                    "type": "functional_area",
                    "area": area_slug,
                    "preview": area_row["summary"],
                    "color": color,
                    "is_area": True,
                }
            }
        )
        node_ids.add(area_id)

    def add_node(node_id: str, label: str, etype: str, preview: str, source: str) -> None:
        if node_id in node_ids:
            return
        area = node_area.get(node_id) or infer_schema_area(node_id, label, etype, preview, source)
        color = AREA_COLORS.get(area, UNOWNED_COLOR) if area else UNOWNED_COLOR
        nodes.append(
            {
                "data": {
                    "id": node_id,
                    "label": label,
                    "type": etype,
                    "area": area or "",
                    "preview": preview,
                    "source": source,
                    "color": color,
                }
            }
        )
        node_ids.add(node_id)

    for entity in entities:
        add_node(
            str(entity["id"]),
            str(entity["name"] or entity["id"])[:96],
            str(entity["entity_type"]),
            str(entity["content_preview"]),
            str(entity["source"]),
        )

    for capability in capabilities:
        capability_slug = str(capability["capability_slug"])
        add_node(
            f"capability::{capability_slug}",
            str(capability["title"]),
            "capability",
            f"{capability['capability_kind']} | {capability['route']} | {capability['summary']}",
            "capability_catalog",
        )

    for tool in tools:
        tool_slug = str(tool["slug"])
        add_node(
            f"tool::{tool_slug}",
            str(tool["display_name"]),
            "tool",
            str(tool["description"]),
            f"mcp::{tool['surface']}::{tool['tier']}",
        )

    edge_rows: list[dict[str, Any]] = []
    for edge in edges:
        if edge["source_id"] not in node_ids or edge["target_id"] not in node_ids:
            continue
        relation = str(edge["relation_type"])
        if relation == "belongs_to_area":
            continue
        edge_rows.append(
            {
                "data": {
                    "id": f"{edge['source_id']}|{relation}|{edge['target_id']}",
                    "source": edge["source_id"],
                    "target": edge["target_id"],
                    "label": relation,
                    "weight": float(edge["weight"] or 1.0),
                }
            }
        )

    degree: dict[str, int] = {}
    for edge in edge_rows:
        source = str(edge["data"]["source"])
        target = str(edge["data"]["target"])
        degree[source] = degree.get(source, 0) + 1
        degree[target] = degree.get(target, 0) + 1

    area_member_count: dict[str, int] = {}
    for node in nodes:
        data = node["data"]
        if data.get("is_area"):
            continue
        area = str(data.get("area") or "")
        if area:
            area_member_count[area] = area_member_count.get(area, 0) + 1

    for node in nodes:
        data = node["data"]
        node_id = str(data["id"])
        if data.get("is_area"):
            count = area_member_count.get(str(data["area"]), 1)
            data["degree"] = count
            data["size"] = max(42, min(140, 34 + 7 * count**0.55))
        else:
            node_degree = max(1, degree.get(node_id, 1))
            data["degree"] = node_degree
            data["size"] = max(6, min(32, 6 + 2.2 * node_degree**0.75))

    final_nodes: list[dict[str, Any]] = []
    for node in nodes:
        data = node["data"]
        node_id = str(data["id"])
        if data.get("is_area"):
            final_nodes.append(node)
            continue
        has_area = bool(data.get("area"))
        has_edges = node_id in degree
        if has_area or has_edges:
            final_nodes.append(node)

    kept_ids = {str(node["data"]["id"]) for node in final_nodes}
    final_edges = [
        edge
        for edge in edge_rows
        if edge["data"]["source"] in kept_ids and edge["data"]["target"] in kept_ids
    ]

    node_area_lookup: dict[str, str] = {}
    for node in final_nodes:
        data = node["data"]
        if data.get("is_area"):
            continue
        area = str(data.get("area") or "")
        if area:
            node_area_lookup[str(data["id"])] = area

    aggregate_counts: dict[tuple[str, str, str], int] = {}
    for edge in final_edges:
        data = edge["data"]
        src_area = node_area_lookup.get(str(data["source"]))
        tgt_area = node_area_lookup.get(str(data["target"]))
        if not src_area or not tgt_area or src_area == tgt_area:
            continue
        relation = str(data["label"])
        key = (src_area, tgt_area, relation)
        aggregate_counts[key] = aggregate_counts.get(key, 0) + 1

    for (src_area, tgt_area, relation), count in aggregate_counts.items():
        src_id = f"area::{src_area}"
        tgt_id = f"area::{tgt_area}"
        if src_id not in kept_ids or tgt_id not in kept_ids:
            continue
        final_edges.append(
            {
                "data": {
                    "id": f"{src_id}|agg_{relation}|{tgt_id}",
                    "source": src_id,
                    "target": tgt_id,
                    "label": relation,
                    "weight": float(count),
                    "is_aggregate": True,
                },
                "classes": "aggregate",
            }
        )

    return {"nodes": final_nodes, "edges": final_edges}


def build_atlas_payload(*, database_url: str | None = None) -> dict[str, Any]:
    graph = build_graph(database_url=database_url)
    warnings: list[str] = []
    try:
        freshness = fetch_graph_freshness(_connect(database_url))
    except Exception as exc:
        freshness = unknown_graph_freshness(exc)
        warnings.append(f"atlas_freshness_unavailable:{type(exc).__name__}")
    areas = []
    for node in graph["nodes"]:
        data = node.get("data", {})
        if not data.get("is_area"):
            continue
        areas.append(
            {
                "slug": data.get("area") or "",
                "title": data.get("label") or "",
                "summary": data.get("preview") or "",
                "color": data.get("color") or AREA_COLORS.get(str(data.get("area") or ""), UNOWNED_COLOR),
                "member_count": int(data.get("degree") or 0),
            }
        )
    real_edges = [edge for edge in graph["edges"] if not edge.get("data", {}).get("is_aggregate")]
    aggregate_edges = [edge for edge in graph["edges"] if edge.get("data", {}).get("is_aggregate")]
    return {
        "ok": True,
        "nodes": graph["nodes"],
        "edges": graph["edges"],
        "areas": sorted(areas, key=lambda area: str(area["slug"])),
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "node_count": len(graph["nodes"]),
            "edge_count": len(real_edges),
            "aggregate_edge_count": len(aggregate_edges),
            "source_authority": "Praxis.db",
            "graph_freshness_state": freshness["graph_freshness_state"],
            "memory_entities_max_updated_at": freshness["memory_entities_max_updated_at"],
            "memory_edges_max_updated_at": freshness["memory_edges_max_updated_at"],
            "authority_projection_last_run_at": freshness["authority_projection_last_run_at"],
            "authority_projection_source_max_updated_at": (
                freshness["authority_projection_source_max_updated_at"]
            ),
            "authority_projection_lag_seconds": freshness["authority_projection_lag_seconds"],
            "authority_projection_edge_count": freshness["authority_projection_edge_count"],
            "freshness": freshness,
        },
        "warnings": warnings,
    }
