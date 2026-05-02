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
from runtime.workspace_paths import repo_root as workspace_repo_root, scratch_path
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

REPO_ROOT = workspace_repo_root()
HEURISTIC_MAP_PATH = scratch_path("atlas_heuristic_map")
FRESHNESS_LAG_TOLERANCE_SECONDS = 2.0
ACTIVITY_HALF_LIFE_SECONDS = 7 * 24 * 60 * 60
MIN_ACTIVITY_SCORE = 0.08

# Atlas is a living map, not a categorical dashboard. Keep area color payloads
# monochrome so downstream renderers cannot quietly reintroduce the old rainbow
# browser language.
AREA_COLORS = {
    "compiler": "#b7b0a3",
    "scheduler": "#d8d2c5",
    "sandbox": "#8c8a84",
    "routing": "#e0af68",
    "circuits": "#f3efe6",
    "outbox": "#8c8a84",
    "receipts": "#d8d2c5",
    "memory": "#b7b0a3",
    "bugs": "#f3efe6",
    "roadmap": "#d8d2c5",
    "authority": "#b7b0a3",
    "build": "#8c8a84",
    "governance": "#d8d2c5",
    "heal": "#8c8a84",
    "discover": "#b7b0a3",
    "debate": "#f3efe6",
    "canvas": "#d8d2c5",
    "mcp": "#b7b0a3",
    "cli": "#cfc9c2",
    "integrations": "#e0af68",
}
UNOWNED_COLOR = "#6d6b67"

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
    ("canvas", ("canvas_", "dashboard", "surface_catalog")),
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


def normalize_relation_label(label: str) -> str:
    normalized = label.strip().lower()
    return {
        "derives_from": "derived_from",
        "derived_from": "derived_from",
    }.get(normalized, normalized)


def fetch_entities(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT id, entity_type, name, COALESCE(LEFT(content, 300), '') AS content_preview,
                   COALESCE(source, '') AS source,
                   updated_at
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
        SELECT source_id, target_id, relation_type, weight,
               GREATEST(created_at, COALESCE(last_validated_at, created_at)) AS updated_at
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


def fetch_data_dictionary_objects(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT object_kind, label, category, summary, origin_ref
              FROM data_dictionary_objects
             ORDER BY object_kind
            """
        )
    ]


def fetch_data_dictionary_lineage(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT src_object_kind, src_field_path, dst_object_kind, dst_field_path,
                   edge_kind, effective_source, confidence
              FROM data_dictionary_lineage_effective
             WHERE src_field_path = ''
               AND dst_field_path = ''
            """
        )
    ]


def fetch_surface_catalog_items(conn: SyncPostgresConnection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.fetch(
            """
            SELECT catalog_item_id, surface_name, label, family, status, drop_kind,
                   action_value, gate_family, description, truth_category,
                   surface_tier, binding_revision, decision_ref
              FROM surface_catalog_registry
             WHERE enabled = true
             ORDER BY surface_name, display_order, catalog_item_id
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
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
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


def compute_activity_score(updated_at: Any, *, reference_at: Any | None = None) -> float:
    """Map a durable timestamp into a bounded liveness signal for Atlas."""
    updated = _as_utc(updated_at)
    if updated is None:
        return MIN_ACTIVITY_SCORE
    reference = _as_utc(reference_at) or datetime.now(timezone.utc)
    age_seconds = max(0.0, (reference - updated).total_seconds())
    score = 1.0 / (1.0 + age_seconds / ACTIVITY_HALF_LIFE_SECONDS)
    return round(max(MIN_ACTIVITY_SCORE, min(1.0, score)), 4)


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
    reference_at = datetime.now(timezone.utc)
    conn = _connect(database_url)
    entities = fetch_entities(conn)
    capabilities = fetch_capabilities(conn)
    areas = fetch_functional_areas(conn)
    area_relations = fetch_area_relations(conn)
    data_dictionary_objects = fetch_data_dictionary_objects(conn)
    data_dictionary_lineage = fetch_data_dictionary_lineage(conn)
    surface_catalog_items = fetch_surface_catalog_items(conn)
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
                    "updated_at": None,
                    "activity_score": MIN_ACTIVITY_SCORE,
                    "is_area": True,
                }
            }
        )
        node_ids.add(area_id)

    def add_node(
        node_id: str,
        label: str,
        etype: str,
        preview: str,
        source: str,
        *,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if node_id in node_ids:
            return
        extra_data = dict(extra or {})
        explicit_area = str(extra_data.get("area") or "").strip()
        updated_at = extra_data.pop("updated_at", None)
        activity_score = extra_data.pop("activity_score", None)
        area = (
            node_area.get(node_id)
            or explicit_area
            or infer_schema_area(node_id, label, etype, preview, source)
        )
        color = AREA_COLORS.get(area, UNOWNED_COLOR) if area else UNOWNED_COLOR
        data = {
            "id": node_id,
            "label": label,
            "type": etype,
            "area": area or "",
            "preview": preview,
            "source": source,
            "authority_source": source,
            "color": color,
            "updated_at": _iso(updated_at),
            "activity_score": (
                round(float(activity_score), 4)
                if activity_score is not None
                else compute_activity_score(updated_at, reference_at=reference_at)
            ),
        }
        if extra_data:
            data.update(extra_data)
        nodes.append({"data": data})
        node_ids.add(node_id)

    for entity in entities:
        add_node(
            str(entity["id"]),
            str(entity["name"] or entity["id"])[:96],
            str(entity["entity_type"]),
            str(entity["content_preview"]),
            str(entity["source"]),
            extra={"updated_at": entity.get("updated_at")},
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

    for dictionary_object in data_dictionary_objects:
        object_kind = str(dictionary_object["object_kind"])
        category = str(dictionary_object["category"] or "object")
        raw_label = str(dictionary_object["label"] or "").strip()
        label = raw_label or object_kind.split(":", 1)[-1]
        origin_ref = dictionary_object.get("origin_ref")
        origin_preview = json.dumps(origin_ref, sort_keys=True) if origin_ref else ""
        summary = str(dictionary_object["summary"] or "").strip()
        preview = summary or origin_preview
        add_node(
            object_kind,
            label[:96],
            category,
            preview[:300],
            "data_dictionary_objects",
            extra={
                "object_kind": object_kind,
                "category": category,
                "definition_summary": summary,
                "authority_source": "data_dictionary_objects",
            },
        )

    for surface_item in surface_catalog_items:
        item_id = str(surface_item["catalog_item_id"])
        surface_name = str(surface_item["surface_name"] or "surface")
        action_value = surface_item.get("action_value")
        gate_family = surface_item.get("gate_family")
        route_ref = str(action_value or gate_family or "")
        preview_parts = [
            str(surface_item["description"] or "").strip(),
            f"truth={surface_item['truth_category']}",
            f"tier={surface_item['surface_tier']}",
            f"route={route_ref}" if route_ref else "",
        ]
        add_node(
            f"surface_catalog::{item_id}",
            f"{surface_name}: {surface_item['label']}"[:96],
            "surface_catalog_item",
            " | ".join(part for part in preview_parts if part)[:300],
            "surface_catalog_registry",
            extra={
                "area": surface_name if surface_name in AREA_COLORS else "canvas",
                "authority_source": "surface_catalog_registry",
                "object_kind": f"surface_catalog:{item_id}",
                "category": str(surface_item["drop_kind"]),
                "surface_name": surface_name,
                "route_ref": route_ref,
                "binding_revision": str(surface_item["binding_revision"]),
                "decision_ref": str(surface_item["decision_ref"]),
            },
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
    seen_edge_ids: set[str] = set()
    for edge in edges:
        if edge["source_id"] not in node_ids or edge["target_id"] not in node_ids:
            continue
        relation = normalize_relation_label(str(edge["relation_type"]))
        if relation == "belongs_to_area":
            continue
        edge_id = f"{edge['source_id']}|{relation}|{edge['target_id']}"
        if edge_id in seen_edge_ids:
            continue
        seen_edge_ids.add(edge_id)
        edge_rows.append(
            {
                "data": {
                    "id": edge_id,
                    "source": edge["source_id"],
                    "target": edge["target_id"],
                    "label": relation,
                    "weight": float(edge["weight"] or 1.0),
                    "updated_at": _iso(edge.get("updated_at")),
                    "activity_score": compute_activity_score(
                        edge.get("updated_at"),
                        reference_at=reference_at,
                    ),
                }
            }
        )

    for lineage in data_dictionary_lineage:
        source = str(lineage["src_object_kind"])
        target = str(lineage["dst_object_kind"])
        if source not in node_ids or target not in node_ids:
            continue
        relation = normalize_relation_label(str(lineage["edge_kind"]))
        edge_id = f"{source}|{relation}|{target}"
        if edge_id in seen_edge_ids:
            continue
        seen_edge_ids.add(edge_id)
        edge_rows.append(
            {
                "data": {
                    "id": edge_id,
                    "source": source,
                    "target": target,
                    "label": relation,
                    "weight": float(lineage["confidence"] or 1.0),
                    "authority_source": "data_dictionary_lineage_effective",
                    "relation_source": str(lineage["effective_source"] or ""),
                    "updated_at": None,
                    "activity_score": MIN_ACTIVITY_SCORE,
                }
            }
        )

    for surface_item in surface_catalog_items:
        item_id = str(surface_item["catalog_item_id"])
        surface_name = str(surface_item["surface_name"] or "canvas")
        source = f"surface_catalog::{item_id}"
        target = f"area::{surface_name if surface_name in AREA_COLORS else 'canvas'}"
        if source not in node_ids or target not in node_ids:
            continue
        edge_id = f"{source}|belongs_to_surface|{target}"
        if edge_id in seen_edge_ids:
            continue
        seen_edge_ids.add(edge_id)
        edge_rows.append(
            {
                "data": {
                    "id": edge_id,
                    "source": source,
                    "target": target,
                    "label": "belongs_to_surface",
                    "weight": 1.0,
                    "authority_source": "surface_catalog_registry",
                    "updated_at": None,
                    "activity_score": MIN_ACTIVITY_SCORE,
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
    area_activity_score: dict[str, float] = {}
    area_updated_at: dict[str, datetime] = {}
    for node in nodes:
        data = node["data"]
        if data.get("is_area"):
            continue
        area = str(data.get("area") or "")
        if area:
            area_member_count[area] = area_member_count.get(area, 0) + 1
            area_activity_score[area] = max(
                area_activity_score.get(area, MIN_ACTIVITY_SCORE),
                float(data.get("activity_score") or MIN_ACTIVITY_SCORE),
            )
            node_updated_at = _as_utc(data.get("updated_at"))
            if node_updated_at is not None:
                current_updated_at = area_updated_at.get(area)
                if current_updated_at is None or node_updated_at > current_updated_at:
                    area_updated_at[area] = node_updated_at

    for node in nodes:
        data = node["data"]
        node_id = str(data["id"])
        if data.get("is_area"):
            area = str(data["area"])
            count = area_member_count.get(area, 1)
            data["degree"] = count
            data["size"] = max(42, min(140, 34 + 7 * count**0.55))
            data["activity_score"] = round(area_activity_score.get(area, MIN_ACTIVITY_SCORE), 4)
            data["updated_at"] = _iso(area_updated_at.get(area))
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
                    "updated_at": None,
                    "activity_score": MIN_ACTIVITY_SCORE,
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


def _node_text(data: dict[str, Any]) -> str:
    return " ".join(
        str(data.get(key) or "")
        for key in (
            "id",
            "label",
            "type",
            "area",
            "preview",
            "source",
            "authority_source",
            "object_kind",
            "category",
            "definition_summary",
            "surface_name",
            "route_ref",
            "decision_ref",
        )
    ).lower()


def _node_summary(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": data.get("id"),
        "label": data.get("label") or data.get("id"),
        "kind": data.get("object_kind") or data.get("category") or data.get("type"),
        "area": data.get("area") or None,
        "authority_source": data.get("authority_source") or data.get("source") or None,
        "summary": data.get("definition_summary") or data.get("preview") or "",
        "route_ref": data.get("route_ref") or None,
        "decision_ref": data.get("decision_ref") or None,
    }


def build_ui_experience_graph(
    payload: dict[str, Any],
    *,
    focus: str | None = None,
    surface_name: str | None = None,
    limit: int = 80,
) -> dict[str, Any]:
    """Project Atlas into an LLM-sized UI experience graph.

    Atlas remains the spatial graph payload. This projection is for agents:
    bounded, relationship-first, and explicit about which DB authority owns a
    screen, control, or schema object.
    """

    max_items = max(1, min(int(limit or 80), 250))
    query = str(focus or "").strip().lower()
    surface_filter = str(surface_name or "").strip().lower()
    raw_nodes = payload.get("nodes") if isinstance(payload, dict) else []
    raw_edges = payload.get("edges") if isinstance(payload, dict) else []
    nodes: dict[str, dict[str, Any]] = {}
    for node in raw_nodes if isinstance(raw_nodes, list) else []:
        data = node.get("data") if isinstance(node, dict) else None
        if isinstance(data, dict) and data.get("id"):
            nodes[str(data["id"])] = data

    def node_matches(data: dict[str, Any]) -> bool:
        if data.get("is_area"):
            return False
        if surface_filter and str(data.get("surface_name") or data.get("area") or "").lower() != surface_filter:
            return False
        return not query or query in _node_text(data)

    matching_ids = {node_id for node_id, data in nodes.items() if node_matches(data)}
    relationships: list[dict[str, Any]] = []
    related_ids: set[str] = set(matching_ids)
    for edge in raw_edges if isinstance(raw_edges, list) else []:
        data = edge.get("data") if isinstance(edge, dict) else None
        if not isinstance(data, dict) or data.get("is_aggregate"):
            continue
        source_id = str(data.get("source") or "")
        target_id = str(data.get("target") or "")
        source = nodes.get(source_id)
        target = nodes.get(target_id)
        if not source or not target:
            continue
        edge_text = " ".join(
            str(data.get(key) or "")
            for key in ("id", "label", "authority_source", "relation_source")
        ).lower()
        edge_matches = bool(query and query in edge_text)
        if matching_ids and source_id not in matching_ids and target_id not in matching_ids and not edge_matches:
            continue
        if not matching_ids and query and not edge_matches:
            continue
        related_ids.update((source_id, target_id))
        relationships.append(
            {
                "source": _node_summary(source),
                "relation": data.get("label") or "related",
                "target": _node_summary(target),
                "authority_source": data.get("authority_source") or data.get("relation_source") or "memory_edges",
                "relation_source": data.get("relation_source") or None,
                "weight": data.get("weight"),
            }
        )

    surface_controls = [
        _node_summary(data)
        for data in nodes.values()
        if str(data.get("type") or "") == "surface_catalog_item"
        and (not surface_filter or str(data.get("surface_name") or "").lower() == surface_filter)
        and (not query or query in _node_text(data) or str(data.get("id") or "") in related_ids)
    ]
    authority_objects = [
        _node_summary(data)
        for node_id, data in nodes.items()
        if node_id in related_ids
        and str(data.get("authority_source") or "") == "data_dictionary_objects"
    ]
    experience_nodes = [
        _node_summary(data)
        for node_id, data in nodes.items()
        if node_id in related_ids
        and str(data.get("authority_source") or "") != "data_dictionary_objects"
        and not data.get("is_area")
    ]

    return {
        "view": "ui_experience_graph",
        "consumer": "llm",
        "source_authority": "Praxis.db via Atlas read model",
        "filters": {
            "focus": focus,
            "surface_name": surface_name,
            "limit": max_items,
        },
        "counts": {
            "atlas_nodes": len(nodes),
            "atlas_edges": len(raw_edges) if isinstance(raw_edges, list) else 0,
            "matched_nodes": len(matching_ids),
            "related_nodes": len(related_ids),
            "relationships": len(relationships),
            "surface_controls": len(surface_controls),
            "authority_objects": len(authority_objects),
        },
        "surfaces": sorted(
            {
                str(data.get("surface_name") or data.get("area") or "")
                for data in nodes.values()
                if str(data.get("type") or "") == "surface_catalog_item"
            }
            - {""}
        ),
        "surface_controls": surface_controls[:max_items],
        "experience_nodes": experience_nodes[:max_items],
        "authority_objects": authority_objects[:max_items],
        "relationships": relationships[:max_items],
        "agent_guidance": [
            "Use surface_catalog_registry rows as the authority for Canvas controls and gate visibility.",
            "Use data_dictionary_objects and data_dictionary_lineage_effective for object and relationship semantics.",
            "Treat React/CSS files as renderers of this authority, not as the source of truth.",
        ],
    }
