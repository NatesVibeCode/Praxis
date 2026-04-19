#!/usr/bin/env python3
"""Generate a single-file HTML atlas of the Praxis knowledge graph.

Output: artifacts/atlas.html (a self-contained page with cytoscape.js from CDN).

Design: Obsidian-style graph view with compound-node area containers,
zoom-gated labels, hover neighborhood focus, and semantic color-by-area.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime._workflow_database import resolve_runtime_database_url

HEURISTIC_MAP_PATH = Path("/tmp/atlas_heuristic_map.json")

# One color per area — semantic palette, muted pastels.
AREA_COLORS = {
    "compiler":     "#7aa2f7",
    "scheduler":    "#bb9af7",
    "sandbox":      "#9ece6a",
    "routing":      "#e0af68",
    "circuits":     "#f7768e",
    "outbox":       "#2ac3de",
    "receipts":     "#73daca",
    "memory":       "#c0caf5",
    "bugs":         "#db4b4b",
    "roadmap":      "#ff9e64",
    "authority":    "#a9b1d6",
    "build":        "#b4f9f8",
    "governance":   "#e0dc8f",
    "heal":         "#9ece6a",
    "discover":     "#41a6b5",
    "debate":       "#f7768e",
    "moon":         "#ad8ee6",
    "mcp":          "#7dcfff",
    "cli":          "#cfc9c2",
    "integrations": "#ff9e64",
}
UNOWNED_COLOR = "#4a5068"
EDGE_COLOR = "#3a3f4b"


def fetch_entities(cur) -> list[dict]:
    cur.execute(
        """
        SELECT id, entity_type, name, COALESCE(LEFT(content, 300), '') AS content_preview,
               COALESCE(source, '') AS source
          FROM memory_entities
         WHERE NOT archived
           AND name IS NOT NULL AND name <> ''
           AND entity_type NOT IN ('task', 'fact')
        """
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_edges(cur, known_ids: set[str]) -> list[dict]:
    cur.execute(
        """
        SELECT source_id, target_id, relation_type, weight
          FROM memory_edges
         WHERE active = true
        """
    )
    out = []
    for r in cur.fetchall():
        if r["source_id"] in known_ids and r["target_id"] in known_ids:
            out.append(dict(r))
    return out


def fetch_capabilities(cur) -> list[dict]:
    cur.execute(
        """
        SELECT capability_slug, capability_kind, title, summary, route
          FROM capability_catalog
         WHERE enabled = true
        """
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_functional_areas(cur) -> list[dict]:
    cur.execute(
        """
        SELECT area_slug, title, summary
          FROM functional_areas
         WHERE area_status = 'active'
        """
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_area_relations(cur) -> list[dict]:
    cur.execute(
        """
        SELECT source_kind, source_ref, target_ref, relation_kind
          FROM operator_object_relations
         WHERE target_kind = 'functional_area'
           AND relation_status = 'active'
        """
    )
    return [dict(r) for r in cur.fetchall()]


def fetch_tools() -> list[dict]:
    try:
        from surfaces.mcp.catalog import get_tool_catalog

        out = []
        for slug, definition in get_tool_catalog().items():
            out.append({
                "slug": slug,
                "display_name": definition.display_name,
                "description": definition.description,
                "surface": getattr(definition, "surface", ""),
                "tier": getattr(definition, "cli_tier", ""),
            })
        return out
    except Exception as exc:
        print(f"[atlas] catalog import failed ({exc}); using CLI fallback", file=sys.stderr)
        import subprocess

        result = subprocess.run(
            ["praxis", "workflow", "tools", "list"],
            capture_output=True, text=True, timeout=30,
        )
        out = []
        for line in result.stdout.splitlines():
            if not line.startswith("praxis_"):
                continue
            parts = line.split()
            if not parts:
                continue
            out.append({
                "slug": parts[0],
                "display_name": parts[0].removeprefix("praxis_").replace("_", " ").title(),
                "description": " ".join(parts[6:])[:200] if len(parts) > 6 else "",
                "surface": parts[3] if len(parts) > 3 else "",
                "tier": parts[4] if len(parts) > 4 else "",
            })
        return out


def _workflow_database_url() -> str:
    configured = str(os.environ.get("WORKFLOW_DATABASE_URL") or "").strip()
    if configured:
        return configured
    return str(resolve_runtime_database_url(repo_root=REPO_ROOT))


def build_graph(*, database_url: str | None = None) -> dict:
    conn = psycopg2.connect(
        database_url or _workflow_database_url(),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    try:
        with conn.cursor() as cur:
            entities = fetch_entities(cur)
            capabilities = fetch_capabilities(cur)
            areas = fetch_functional_areas(cur)
            area_relations = fetch_area_relations(cur)
            entity_ids = {e["id"] for e in entities}
            edges = fetch_edges(cur, entity_ids)
    finally:
        conn.close()

    tools = fetch_tools()

    # Load heuristic map for tools + tables.
    hmap = {"tools": {}, "tables": {}}
    if HEURISTIC_MAP_PATH.is_file():
        hmap = json.loads(HEURISTIC_MAP_PATH.read_text())

    # Build: node_id -> area. Sourced from operator_object_relations directly
    # (not memory_edges) because the canonical prefix used in memory_edges is
    # e.g. "roadmap_item::foo" while the authority still refers to
    # source_kind='roadmap_item' with source_ref='foo'. Combining the two
    # prefixes here keeps the mapping precise.
    node_area: dict[str, str] = {}

    for r in area_relations:
        area = r["target_ref"].removeprefix("functional_area.")
        kind = r["source_kind"]
        ref = r["source_ref"]
        if kind in ("bug", "roadmap_item", "repo_path", "operator_decision",
                    "functional_area", "workflow", "workflow_build_intent"):
            node_area.setdefault(f"{kind}::{ref}", area)

    for tool_slug, area in hmap.get("tools", {}).items():
        node_area.setdefault(f"tool::{tool_slug}", area)
    for table_name, area in hmap.get("tables", {}).items():
        node_area.setdefault(f"table:{table_name}", area)

    nodes: list[dict] = []
    node_ids: set[str] = set()

    # Area compound parents
    area_ids = set()
    for a in areas:
        aid = f"area::{a['area_slug']}"
        color = AREA_COLORS.get(a["area_slug"], "#888")
        nodes.append({
            "data": {
                "id": aid,
                "label": a["title"],
                "type": "functional_area",
                "area": a["area_slug"],
                "preview": a["summary"],
                "color": color,
                "is_area": True,
            }
        })
        area_ids.add(aid)
        node_ids.add(aid)

    def add_node(node_id: str, label: str, etype: str, preview: str, source: str) -> None:
        if node_id in node_ids:
            return
        area = node_area.get(node_id)
        color = AREA_COLORS.get(area, UNOWNED_COLOR) if area else UNOWNED_COLOR
        data = {
            "id": node_id,
            "label": label,
            "type": etype,
            "area": area or "",
            "preview": preview,
            "source": source,
            "color": color,
        }
        nodes.append({"data": data})
        node_ids.add(node_id)

    for e in entities:
        add_node(
            e["id"],
            e["name"] or e["id"][:24],
            e["entity_type"],
            e["content_preview"],
            e["source"],
        )

    for c in capabilities:
        cid = f"capability::{c['capability_slug']}"
        add_node(cid, c["title"], "capability",
                 f"{c['capability_kind']} • {c['route']} • {c['summary']}",
                 "capability_catalog")

    for t in tools:
        tid = f"tool::{t['slug']}"
        add_node(tid, t["display_name"], "tool", t["description"],
                 f"mcp::{t['surface']}::{t['tier']}")

    # Authority-projected entities (roadmap_items, bugs, etc.) now live in
    # memory_entities via the authority-to-memory projection, so they flow in
    # through fetch_entities. No manual stubs needed — avoids prefix collisions.

    # Structural edges (memory_edges). Skip belongs_to_area — redundant with
    # area membership and just contributes visual noise (~half of all edges).
    edge_rows: list[dict] = []
    for e in edges:
        if e["source_id"] not in node_ids or e["target_id"] not in node_ids:
            continue
        rel = e["relation_type"]
        if rel == "belongs_to_area":
            continue
        edge_rows.append({
            "data": {
                "id": f"{e['source_id']}|{rel}|{e['target_id']}",
                "source": e["source_id"],
                "target": e["target_id"],
                "label": rel,
                "weight": float(e["weight"] or 1.0),
            }
        })

    # Compute degree for node sizing.
    degree: dict[str, int] = {}
    for e in edge_rows:
        degree[e["data"]["source"]] = degree.get(e["data"]["source"], 0) + 1
        degree[e["data"]["target"]] = degree.get(e["data"]["target"], 0) + 1

    # Area sizing: real member count of the area (not parent_child_count, which
    # was always zero because no node sets `parent`).
    area_member_count: dict[str, int] = {}
    for n in nodes:
        if n["data"].get("is_area"):
            continue
        a = n["data"].get("area")
        if a:
            area_member_count[a] = area_member_count.get(a, 0) + 1

    for n in nodes:
        nid = n["data"]["id"]
        if n["data"].get("is_area"):
            c = area_member_count.get(n["data"]["area"], 1)
            n["data"]["degree"] = c
            n["data"]["size"] = max(42, min(140, 34 + 7 * c ** 0.55))
        else:
            d = max(1, degree.get(nid, 1))
            n["data"]["degree"] = d
            n["data"]["size"] = max(6, min(32, 6 + 2.2 * d ** 0.75))

    # Drop standalone nodes with no area AND no edges — pure noise.
    final_nodes = []
    for n in nodes:
        nid = n["data"]["id"]
        if n["data"].get("is_area"):
            final_nodes.append(n)
            continue
        has_area = bool(n["data"].get("area"))
        has_edges = nid in degree
        if has_area or has_edges:
            final_nodes.append(n)

    kept_ids = {n["data"]["id"] for n in final_nodes}
    final_edges = [e for e in edge_rows if e["data"]["source"] in kept_ids and e["data"]["target"] in kept_ids]

    # (Pull edges removed — the JS uses a two-pass layout: fcose on areas +
    # aggregate edges for overview, then per-area local layout on expand.
    # Pull edges were only needed by the old single-fcose-on-everything layout.)

    # Aggregate cross-area edges: for every real edge whose endpoints live in
    # different areas, emit one area↔area edge per (src_area, tgt_area, rel)
    # triple weighted by count. Used for the overview layer (progressive
    # disclosure) so closed areas still reveal their outbound dependencies.
    node_area_lookup: dict[str, str] = {}
    for n in final_nodes:
        if n["data"].get("is_area"):
            continue
        a = n["data"].get("area")
        if a:
            node_area_lookup[n["data"]["id"]] = a

    aggregate_counts: dict[tuple[str, str, str], int] = {}
    for e in final_edges:
        d = e["data"]
        if d.get("is_pull"):
            continue
        src_area = node_area_lookup.get(d["source"])
        tgt_area = node_area_lookup.get(d["target"])
        if not src_area or not tgt_area or src_area == tgt_area:
            continue
        key = (src_area, tgt_area, d["label"])
        aggregate_counts[key] = aggregate_counts.get(key, 0) + 1

    for (src_area, tgt_area, rel), count in aggregate_counts.items():
        src_id = f"area::{src_area}"
        tgt_id = f"area::{tgt_area}"
        if src_id not in kept_ids or tgt_id not in kept_ids:
            continue
        final_edges.append({
            "data": {
                "id": f"{src_id}|agg_{rel}|{tgt_id}",
                "source": src_id,
                "target": tgt_id,
                "label": rel,
                "weight": float(count),
                "is_aggregate": True,
            },
            "classes": "aggregate",
        })

    return {"nodes": final_nodes, "edges": final_edges}


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Praxis System Atlas</title>
<script src="https://unpkg.com/cytoscape@3.29.2/dist/cytoscape.min.js"></script>
<script src="https://unpkg.com/layout-base@2.0.1/layout-base.js"></script>
<script src="https://unpkg.com/cose-base@2.2.0/cose-base.js"></script>
<script src="https://unpkg.com/cytoscape-fcose@2.2.0/cytoscape-fcose.js"></script>
<style>
  :root {
    --bg: #16161e;
    --bg-panel: #1a1b26;
    --bg-elev: #1f2335;
    --border: #292e42;
    --text: #c0caf5;
    --text-dim: #565f89;
    --accent: #7aa2f7;
  }
  html, body { margin: 0; padding: 0; height: 100%; font-family: -apple-system, system-ui, sans-serif; background: var(--bg); color: var(--text); }
  #app { display: grid; grid-template-columns: 240px 1fr 300px; grid-template-rows: 44px 1fr; height: 100vh; }
  header { grid-column: 1 / 4; padding: 0 18px; background: var(--bg-panel); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 18px; }
  header h1 { font-size: 14px; margin: 0; font-weight: 500; letter-spacing: 0.3px; }
  header .counts { font-size: 11px; color: var(--text-dim); font-family: ui-monospace, monospace; }
  header .hint { font-size: 11px; color: var(--text-dim); font-style: italic; }
  #sidebar { background: var(--bg-panel); padding: 14px; overflow-y: auto; border-right: 1px solid var(--border); font-size: 12px; }
  #sidebar h2 { font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-dim); margin: 14px 0 6px; font-weight: 600; }
  #sidebar h2:first-child { margin-top: 0; }
  #sidebar label { display: flex; align-items: center; padding: 3px 0; cursor: pointer; gap: 7px; }
  #sidebar label:hover { color: white; }
  #sidebar input[type=search] { width: 100%; padding: 6px 9px; border-radius: 5px; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 12px; box-sizing: border-box; outline: none; }
  #sidebar input[type=search]:focus { border-color: var(--accent); }
  .swatch { width: 10px; height: 10px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
  .edge-swatch { width: 22px; height: 2px; display: inline-block; flex-shrink: 0; position: relative; }
  .edge-swatch.dashed { background: transparent; border-top: 2px dashed currentColor; height: 0; }
  .edge-swatch.dotted { background: transparent; border-top: 2px dotted currentColor; height: 0; }
  #cy { background: var(--bg); width: 100%; height: 100%; }
  #detail { background: var(--bg-panel); padding: 14px; border-left: 1px solid var(--border); overflow-y: auto; font-size: 12px; }
  #detail h2 { font-size: 14px; margin: 0 0 6px; font-weight: 500; }
  #detail .tags { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
  #detail .tag { font-size: 10px; padding: 2px 7px; border-radius: 8px; background: var(--bg-elev); color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.3px; }
  #detail .tag.area { color: var(--accent); }
  #detail .src { color: var(--text-dim); font-size: 10px; margin-bottom: 10px; font-family: ui-monospace, monospace; word-break: break-all; }
  #detail pre { white-space: pre-wrap; word-break: break-word; font-size: 11px; background: var(--bg); padding: 9px; border-radius: 5px; max-height: 260px; overflow-y: auto; border: 1px solid var(--border); line-height: 1.5; }
  #detail .empty { color: var(--text-dim); font-style: italic; font-size: 12px; }
  #detail .neighbors { margin-top: 14px; }
  #detail .neighbors .nrow { padding: 4px 0; font-size: 11px; display: flex; align-items: center; gap: 6px; cursor: pointer; }
  #detail .neighbors .nrow:hover { color: white; }
  button { background: var(--bg-elev); color: var(--text); border: 1px solid var(--border); padding: 5px 9px; border-radius: 4px; font-size: 11px; cursor: pointer; }
  button:hover { background: var(--border); color: white; }
  .toolbar { margin-left: auto; display: flex; gap: 6px; }
</style>
</head>
<body>
<div id="app">
  <header>
    <h1>Praxis System Atlas</h1>
    <span class="counts" id="counts"></span>
    <span class="hint" id="mode-hint">click an area to expand</span>
    <div class="toolbar">
      <button id="btn-expand-all">Expand all</button>
      <button id="btn-collapse-all">Collapse all</button>
      <button id="btn-fit">Fit</button>
      <button id="btn-relayout">Re-layout</button>
      <button id="btn-labels">Labels: auto</button>
    </div>
  </header>
  <aside id="sidebar">
    <h2>Search</h2>
    <input type="search" id="search" placeholder="name or id…">
    <h2>Relations</h2>
    <div id="relation-filters"></div>
    <h2>Areas</h2>
    <div id="area-filters"></div>
    <h2>Types</h2>
    <div id="type-filters"></div>
  </aside>
  <div id="cy"></div>
  <aside id="detail">
    <p class="empty">Click a node to inspect.<br><br>Hover a node to focus its neighborhood.</p>
  </aside>
</div>
<script>
const GRAPH = __GRAPH_JSON__;
const AREA_COLORS = __AREA_COLORS__;

// Counts
const typeCounts = {};
const areaCounts = {};
const relationCounts = {};
GRAPH.nodes.forEach(n => {
  const t = n.data.type;
  typeCounts[t] = (typeCounts[t] || 0) + 1;
  const a = n.data.area || '(unowned)';
  areaCounts[a] = (areaCounts[a] || 0) + 1;
});
GRAPH.edges.forEach(e => {
  if (e.data.is_pull || e.data.is_aggregate) return;
  const r = e.data.label;
  if (!r) return;
  relationCounts[r] = (relationCounts[r] || 0) + 1;
});
const realEdges = GRAPH.edges.filter(e => !e.data.is_pull && !e.data.is_aggregate).length;
const aggEdges = GRAPH.edges.filter(e => e.data.is_aggregate).length;
document.getElementById('counts').textContent =
  GRAPH.nodes.length + ' nodes • ' + realEdges + ' edges • ' + aggEdges + ' inter-area';

const RELATION_STYLES = {
  depends_on:       { color: '#7aa2f7', style: 'solid'  },
  parent_of:        { color: '#bb9af7', style: 'solid'  },
  derived_from:     { color: '#9ece6a', style: 'dashed' },
  implements_build: { color: '#e0af68', style: 'solid'  },
  resolves_bug:     { color: '#f7768e', style: 'dotted' },
};

function mkCheckbox(container, key, count, color, dataKey) {
  const label = document.createElement('label');
  const cb = document.createElement('input');
  cb.type = 'checkbox'; cb.checked = true; cb.dataset[dataKey] = key;
  cb.addEventListener('change', applyFilters);
  const sw = document.createElement('span');
  sw.className = 'swatch'; if (color) sw.style.background = color;
  label.appendChild(cb); label.appendChild(sw);
  label.appendChild(document.createTextNode(key + ' (' + count + ')'));
  container.appendChild(label);
}

function mkRelCheckbox(container, key, count) {
  const rs = RELATION_STYLES[key] || { color: '#7e869e', style: 'solid' };
  const label = document.createElement('label');
  const cb = document.createElement('input');
  cb.type = 'checkbox'; cb.checked = true; cb.dataset.relation = key;
  cb.addEventListener('change', applyFilters);
  const sw = document.createElement('span');
  sw.className = 'edge-swatch' + (rs.style !== 'solid' ? ' ' + rs.style : '');
  if (rs.style === 'solid') sw.style.background = rs.color;
  else sw.style.color = rs.color;
  label.appendChild(cb); label.appendChild(sw);
  label.appendChild(document.createTextNode(key + ' (' + count + ')'));
  container.appendChild(label);
}

const areaBox = document.getElementById('area-filters');
Object.keys(areaCounts).sort().forEach(a => mkCheckbox(areaBox, a, areaCounts[a], AREA_COLORS[a] || '#4a5068', 'area'));

const typeBox = document.getElementById('type-filters');
Object.keys(typeCounts).sort().forEach(t => mkCheckbox(typeBox, t, typeCounts[t], null, 'type'));

const relBox = document.getElementById('relation-filters');
Object.keys(relationCounts).sort().forEach(r => mkRelCheckbox(relBox, r, relationCounts[r]));

const cy = cytoscape({
  container: document.getElementById('cy'),
  elements: [...GRAPH.nodes, ...GRAPH.edges],
  style: [
    { selector: 'node', style: {
      'background-color': 'data(color)',
      'background-opacity': 0.85,
      'label': 'data(label)',
      'color': '#c0caf5',
      'font-size': '9px',
      'font-family': 'ui-monospace, monospace',
      'text-valign': 'bottom',
      'text-halign': 'center',
      'text-margin-y': 3,
      'width': 'data(size)',
      'height': 'data(size)',
      'border-width': 0,
      'text-outline-width': 3,
      'text-outline-color': '#16161e',
      'text-outline-opacity': 1,
      'min-zoomed-font-size': 10,
      'text-opacity': 0,
    }},
    { selector: 'node[?is_area]', style: {
      'background-color': 'data(color)',
      'background-opacity': 0.10,
      'border-width': 2,
      'border-color': 'data(color)',
      'border-opacity': 0.6,
      'shape': 'round-rectangle',
      'label': 'data(label)',
      'font-size': '15px',
      'font-weight': 700,
      'color': 'data(color)',
      'text-valign': 'center',
      'text-halign': 'center',
      'text-margin-y': 0,
      'text-opacity': 1,
      'text-outline-width': 4,
      'text-outline-color': '#16161e',
      'text-outline-opacity': 1,
      'min-zoomed-font-size': 0,
      'padding': '22px',
      'z-index': 1,
    }},
    { selector: 'node[?is_area].expanded', style: {
      'background-opacity': 0,
      'border-opacity': 0.3,
      'border-style': 'dashed',
      'text-valign': 'top',
      'text-margin-y': -8,
      'font-size': '14px',
      'z-index': 0,
    }},
    { selector: 'node:selected', style: {
      'border-width': 2,
      'border-color': '#f7768e',
      'text-opacity': 1,
    }},
    { selector: 'node.hover', style: {
      'text-opacity': 1,
      'z-index': 10,
    }},
    { selector: 'edge', style: {
      'width': 1,
      'line-color': '#7e869e',
      'curve-style': 'straight',
      'opacity': 0.7,
      'target-arrow-shape': 'none',
    }},
    { selector: 'edge[label = "depends_on"]', style: {
      'line-color': '#7aa2f7',
      'width': 1.1,
      'target-arrow-shape': 'triangle-backcurve',
      'target-arrow-color': '#7aa2f7',
      'arrow-scale': 0.65,
      'curve-style': 'straight',
    }},
    { selector: 'edge[label = "parent_of"]', style: {
      'line-color': '#bb9af7',
      'width': 1.6,
      'opacity': 0.8,
    }},
    { selector: 'edge[label = "derived_from"]', style: {
      'line-color': '#9ece6a',
      'line-style': 'dashed',
      'width': 1.0,
      'target-arrow-shape': 'triangle-backcurve',
      'target-arrow-color': '#9ece6a',
      'arrow-scale': 0.6,
      'curve-style': 'straight',
    }},
    { selector: 'edge[label = "implements_build"]', style: {
      'line-color': '#e0af68',
      'width': 1.3,
      'target-arrow-shape': 'triangle-backcurve',
      'target-arrow-color': '#e0af68',
      'arrow-scale': 0.65,
      'curve-style': 'straight',
    }},
    { selector: 'edge[label = "resolves_bug"]', style: {
      'line-color': '#f7768e',
      'line-style': 'dotted',
      'width': 1.2,
      'target-arrow-shape': 'triangle-backcurve',
      'target-arrow-color': '#f7768e',
      'arrow-scale': 0.65,
      'curve-style': 'straight',
    }},
    { selector: 'edge[?is_aggregate]', style: {
      'width': 'mapData(weight, 1, 40, 1.8, 7)',
      'opacity': 0.7,
      'curve-style': 'bezier',
      'control-point-step-size': 40,
      'target-arrow-shape': 'triangle-backcurve',
      'arrow-scale': 0.9,
      'z-index': 2,
    }},
    { selector: 'edge.highlight', style: {
      'opacity': 1,
      'width': 2.2,
      'z-index': 5,
    }},
    { selector: 'edge.pull', style: {
      'opacity': 0,
      'events': 'no',
      'width': 0.1,
    }},
    { selector: '.faded', style: { 'opacity': 0.08 }},
    { selector: 'edge.faded', style: { 'opacity': 0.04 }},
  ],
  // Preset layout — positions computed in cy.ready() via a two-pass approach:
  //  1. fcose on area-nodes + aggregate edges only (overview layer, spread out)
  //  2. members start stacked at their area's position; layoutArea() spreads
  //     them on first expand.
  layout: { name: 'preset' },
  wheelSensitivity: 0.25,
  minZoom: 0.1,
  maxZoom: 4,
});

// Progressive disclosure state
const expandedAreas = new Set();

function syncExpandedClass() {
  cy.nodes('[?is_area]').forEach(a => {
    if (expandedAreas.has(a.data('area'))) a.addClass('expanded');
    else a.removeClass('expanded');
  });
}

// Zoom-gated labels (area labels are always shown)
let labelMode = 'auto'; // auto | always | off
function applyLabelMode() {
  const zoom = cy.zoom();
  const show = labelMode === 'always' || (labelMode === 'auto' && zoom > 1.3);
  cy.nodes().not('[?is_area]').not('.hover').style('text-opacity', show ? 1 : 0);
}
cy.on('zoom', applyLabelMode);

document.getElementById('btn-labels').addEventListener('click', () => {
  labelMode = labelMode === 'auto' ? 'always' : labelMode === 'always' ? 'off' : 'auto';
  document.getElementById('btn-labels').textContent = 'Labels: ' + labelMode;
  applyLabelMode();
});
document.getElementById('btn-fit').addEventListener('click', () => {
  const visible = cy.nodes().filter(n => n.style('display') !== 'none');
  cy.fit(visible.length ? visible : cy.nodes('[?is_area]'), 60);
});

document.getElementById('btn-expand-all').addEventListener('click', () => {
  cy.nodes('[?is_area]').forEach(a => {
    const slug = a.data('area');
    if (!expandedAreas.has(slug)) {
      expandedAreas.add(slug);
      layoutArea(slug);
    }
  });
  applyFilters();
  syncExpandedClass();
  updateModeHint();
});
document.getElementById('btn-collapse-all').addEventListener('click', () => {
  expandedAreas.clear();
  applyFilters();
  syncExpandedClass();
  updateModeHint();
});

function updateModeHint() {
  const hint = document.getElementById('mode-hint');
  const total = cy.nodes('[?is_area]').length;
  const n = expandedAreas.size;
  if (n === 0) hint.textContent = 'click an area to expand';
  else if (n === total) hint.textContent = 'all ' + total + ' areas expanded';
  else hint.textContent = n + ' / ' + total + ' areas expanded';
}

// Hover neighborhood focus — works for both area (overview) and node (expanded) layers.
cy.on('mouseover', 'node', evt => {
  const n = evt.target;
  const visibleEdges = n.connectedEdges().not('.pull').filter(e => e.style('display') !== 'none');
  const neighbors = visibleEdges.connectedNodes();
  const hood = n.union(neighbors).union(visibleEdges);
  cy.elements().not(hood).not('.pull').addClass('faded');
  hood.removeClass('faded');
  visibleEdges.addClass('highlight');
  n.addClass('hover');
  hood.nodes().style('text-opacity', 1);
});
cy.on('mouseout', 'node', evt => {
  cy.elements().removeClass('faded');
  cy.edges().removeClass('highlight');
  cy.nodes().removeClass('hover');
  applyLabelMode();
});

// Tracks areas that have been laid out at least once so re-expand is instant.
const laidOutAreas = new Set();

function layoutArea(areaSlug) {
  const areaNode = cy.$('#area\\:\\:' + areaSlug.replace(/[^\w-]/g, ''));
  const members = cy.nodes().filter(n => !n.data('is_area') && n.data('area') === areaSlug);
  if (members.length === 0) return;
  const center = areaNode.position();
  // Scale the local layout's bounding box to member count so large areas don't
  // overlap their neighbors. Roughly: radius grows with sqrt(count).
  const radius = Math.max(80, 22 * Math.sqrt(members.length));
  const bb = {
    x1: center.x - radius, y1: center.y - radius,
    x2: center.x + radius, y2: center.y + radius,
  };
  const name = members.length > 30 ? 'grid' : (members.length > 10 ? 'concentric' : 'circle');
  const opts = {
    name, boundingBox: bb, animate: true, animationDuration: 350,
    fit: false, avoidOverlap: true, padding: 10,
  };
  if (name === 'concentric') {
    opts.concentric = n => n.degree(true);
    opts.levelWidth = () => 2;
    opts.minNodeSpacing = 18;
  }
  if (name === 'circle') opts.radius = radius * 0.8;
  members.layout(opts).run();
  laidOutAreas.add(areaSlug);
}

// Click: area → toggle expand; non-area → detail pane
cy.on('tap', 'node', evt => {
  const d = evt.target.data();
  if (d.is_area) {
    if (expandedAreas.has(d.area)) {
      expandedAreas.delete(d.area);
    } else {
      expandedAreas.add(d.area);
      layoutArea(d.area);
    }
    syncExpandedClass();
    applyFilters();
    updateModeHint();
    return;
  }
  renderDetail(d, evt.target);
});
cy.on('tap', evt => {
  if (evt.target === cy) {
    document.getElementById('detail').innerHTML = '<p class="empty">Click an area to expand its members.<br>Click a node to inspect.<br>Hover to focus neighborhood.</p>';
  }
});

function renderDetail(d, node) {
  const box = document.getElementById('detail');
  box.innerHTML = '';
  const h = document.createElement('h2'); h.textContent = d.label; box.appendChild(h);
  const tags = document.createElement('div'); tags.className = 'tags';
  if (d.area) { const t = document.createElement('span'); t.className = 'tag area'; t.textContent = d.area; tags.appendChild(t); }
  const tt = document.createElement('span'); tt.className = 'tag'; tt.textContent = d.type; tags.appendChild(tt);
  box.appendChild(tags);
  const s = document.createElement('div'); s.className = 'src';
  s.textContent = d.id + (d.source ? ' • ' + d.source : '');
  box.appendChild(s);
  if (d.preview) { const p = document.createElement('pre'); p.textContent = d.preview; box.appendChild(p); }
  const neigh = node.neighborhood('node').not('[?is_area]');
  if (neigh.length) {
    const nw = document.createElement('div'); nw.className = 'neighbors';
    const nh = document.createElement('h2'); nh.textContent = 'Connected (' + neigh.length + ')';
    nh.style.fontSize = '10px'; nh.style.letterSpacing = '0.8px'; nh.style.textTransform = 'uppercase';
    nh.style.color = 'var(--text-dim)'; nh.style.margin = '0 0 6px'; nw.appendChild(nh);
    neigh.forEach(nn => {
      const row = document.createElement('div'); row.className = 'nrow';
      const sw = document.createElement('span'); sw.className = 'swatch';
      sw.style.background = nn.data('color');
      row.appendChild(sw);
      row.appendChild(document.createTextNode(nn.data('label')));
      row.addEventListener('click', () => {
        cy.$(':selected').unselect();
        nn.select();
        cy.animate({ center: { eles: nn }, zoom: 1.6 }, { duration: 400 });
        renderDetail(nn.data(), nn);
      });
      nw.appendChild(row);
    });
    box.appendChild(nw);
  }
}

// Filters
document.getElementById('search').addEventListener('input', applyFilters);

function applyFilters() {
  const activeAreas = new Set([...document.querySelectorAll('#area-filters input:checked')].map(i => i.dataset.area));
  const activeTypes = new Set([...document.querySelectorAll('#type-filters input:checked')].map(i => i.dataset.type));
  const activeRelations = new Set([...document.querySelectorAll('#relation-filters input:checked')].map(i => i.dataset.relation));
  const q = (document.getElementById('search').value || '').toLowerCase().trim();

  cy.nodes().forEach(n => {
    const d = n.data();
    if (d.is_area) {
      n.style('display', activeAreas.has(d.area) ? 'element' : 'none');
      return;
    }
    const areaOk = activeAreas.has(d.area || '(unowned)');
    const typeOk = activeTypes.has(d.type);
    const searchMatch = q && ((d.label || '').toLowerCase().includes(q) || (d.id || '').toLowerCase().includes(q));
    // Overview layer: non-area nodes hidden until their area is expanded
    // (or the user is actively searching for them).
    const expanded = d.area && expandedAreas.has(d.area);
    const visible = areaOk && typeOk && (expanded || searchMatch);
    n.style('display', visible ? 'element' : 'none');
  });

  cy.edges().forEach(e => {
    const d = e.data();
    if (d.is_pull) {
      // Invisible layout-only edges: keep in graph, but styled to 0 opacity.
      e.style('display', 'element');
      return;
    }
    const sVis = e.source().style('display') !== 'none';
    const tVis = e.target().style('display') !== 'none';
    if (!sVis || !tVis) {
      e.style('display', 'none');
      return;
    }
    const relOk = !d.label || activeRelations.has(d.label);
    if (!relOk) { e.style('display', 'none'); return; }
    if (d.is_aggregate) {
      // Hide aggregate when both endpoint areas are fully expanded —
      // the underlying real edges take over and carry the same signal.
      const srcArea = e.source().data('area');
      const tgtArea = e.target().data('area');
      const bothExpanded = expandedAreas.has(srcArea) && expandedAreas.has(tgtArea);
      e.style('display', bothExpanded ? 'none' : 'element');
      return;
    }
    e.style('display', 'element');
  });
}

// Overview layout: fcose on area nodes + aggregate edges only.
// Non-area nodes get stacked at their area's position until expand.
function layoutOverview(animate) {
  const areaEles = cy.nodes('[?is_area]').union(cy.edges('[?is_aggregate]'));
  const layout = areaEles.layout({
    name: 'fcose',
    animate: !!animate,
    animationDuration: animate ? 500 : 0,
    randomize: true,
    nodeRepulsion: 150000,
    idealEdgeLength: 260,
    edgeElasticity: 0.1,
    gravity: 0.05,
    numIter: 3500,
    packComponents: true,
    uniformNodeDimensions: false,
    nodeSeparation: 160,
    fit: false,
  });
  layout.run();
  // Stack members at their area position; layoutArea() spreads on expand.
  cy.nodes('[?is_area]').forEach(a => {
    const p = a.position();
    cy.nodes().filter(n => !n.data('is_area') && n.data('area') === a.data('area'))
      .forEach(m => m.position({ x: p.x, y: p.y }));
  });
  // Un-laid-out status so re-expand re-runs local layout.
  laidOutAreas.clear();
  // Previously-expanded areas need a layout re-run after re-positioning.
  expandedAreas.forEach(slug => layoutArea(slug));
}

cy.ready(() => {
  layoutOverview(false);
  applyFilters();
  syncExpandedClass();
  updateModeHint();
  cy.fit(cy.nodes('[?is_area]'), 80);
  applyLabelMode();
});

// Re-layout button re-runs overview (then re-expands any open areas).
document.getElementById('btn-relayout').onclick = null;
document.getElementById('btn-relayout').addEventListener('click', () => {
  layoutOverview(true);
  setTimeout(() => cy.fit(cy.nodes('[?is_area]'), 80), 600);
});
</script>
</body>
</html>
"""


def main() -> None:
    graph = build_graph()
    html = HTML_TEMPLATE.replace(
        "__GRAPH_JSON__", json.dumps(graph, default=str)
    ).replace(
        "__AREA_COLORS__", json.dumps(AREA_COLORS)
    )
    out_dir = REPO_ROOT / "artifacts"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "atlas.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"  nodes: {len(graph['nodes'])} (incl {sum(1 for n in graph['nodes'] if n['data'].get('is_area'))} areas)")
    print(f"  edges: {len(graph['edges'])}")


if __name__ == "__main__":
    main()
