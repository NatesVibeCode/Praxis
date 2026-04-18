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

DB_URL = os.environ.get(
    "WORKFLOW_DATABASE_URL",
    "postgresql://postgres@praxis-postgres-1.orb.local:5432/praxis",
)
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


def build_graph() -> dict:
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
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

    # Only structural edges (memory_edges). Area membership is compound-parenting, not edges.
    edge_rows = [
        {
            "data": {
                "id": f"{e['source_id']}|{e['relation_type']}|{e['target_id']}",
                "source": e["source_id"],
                "target": e["target_id"],
                "label": e["relation_type"],
                "weight": float(e["weight"] or 1.0),
            }
        }
        for e in edges
        if e["source_id"] in node_ids and e["target_id"] in node_ids
    ]

    # Compute degree for node sizing.
    degree: dict[str, int] = {}
    for e in edge_rows:
        degree[e["data"]["source"]] = degree.get(e["data"]["source"], 0) + 1
        degree[e["data"]["target"]] = degree.get(e["data"]["target"], 0) + 1

    # Plus sibling count as pseudo-degree for compound children (so orphan tables in an area still get size).
    parent_child_count: dict[str, int] = {}
    for n in nodes:
        p = n["data"].get("parent")
        if p:
            parent_child_count[p] = parent_child_count.get(p, 0) + 1

    for n in nodes:
        nid = n["data"]["id"]
        if n["data"].get("is_area"):
            # Area size ~ log(child count)
            c = parent_child_count.get(nid, 1)
            n["data"]["degree"] = c
            n["data"]["size"] = max(30, min(110, 28 + 6 * c ** 0.55))
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

    # Invisible area-pull edges: gives fcose gravitational pull so same-area nodes cluster.
    for n in final_nodes:
        if n["data"].get("is_area"):
            continue
        area = n["data"].get("area")
        if not area:
            continue
        area_id = f"area::{area}"
        if area_id not in kept_ids:
            continue
        final_edges.append({
            "data": {
                "id": f"{n['data']['id']}|pull|{area_id}",
                "source": n["data"]["id"],
                "target": area_id,
                "is_pull": True,
                "weight": 1.0,
            },
            "classes": "pull",
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
  #sidebar { background: var(--bg-panel); padding: 14px; overflow-y: auto; border-right: 1px solid var(--border); font-size: 12px; }
  #sidebar h2 { font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-dim); margin: 14px 0 6px; font-weight: 600; }
  #sidebar h2:first-child { margin-top: 0; }
  #sidebar label { display: flex; align-items: center; padding: 3px 0; cursor: pointer; gap: 7px; }
  #sidebar label:hover { color: white; }
  #sidebar input[type=search] { width: 100%; padding: 6px 9px; border-radius: 5px; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 12px; box-sizing: border-box; outline: none; }
  #sidebar input[type=search]:focus { border-color: var(--accent); }
  .swatch { width: 10px; height: 10px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
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
    <div class="toolbar">
      <button id="btn-fit">Fit</button>
      <button id="btn-relayout">Re-layout</button>
      <button id="btn-labels">Labels: auto</button>
    </div>
  </header>
  <aside id="sidebar">
    <h2>Search</h2>
    <input type="search" id="search" placeholder="name or id…">
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
GRAPH.nodes.forEach(n => {
  const t = n.data.type;
  typeCounts[t] = (typeCounts[t] || 0) + 1;
  const a = n.data.area || '(unowned)';
  areaCounts[a] = (areaCounts[a] || 0) + 1;
});
document.getElementById('counts').textContent =
  GRAPH.nodes.length + ' nodes • ' + GRAPH.edges.length + ' edges';

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

const areaBox = document.getElementById('area-filters');
Object.keys(areaCounts).sort().forEach(a => mkCheckbox(areaBox, a, areaCounts[a], AREA_COLORS[a] || '#4a5068', 'area'));

const typeBox = document.getElementById('type-filters');
Object.keys(typeCounts).sort().forEach(t => mkCheckbox(typeBox, t, typeCounts[t], null, 'type'));

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
      'background-opacity': 0,
      'border-width': 0,
      'shape': 'round-rectangle',
      'label': 'data(label)',
      'font-size': '16px',
      'font-weight': 700,
      'color': 'data(color)',
      'text-valign': 'top',
      'text-halign': 'center',
      'text-margin-y': -6,
      'text-opacity': 0.85,
      'text-outline-width': 6,
      'text-outline-color': '#16161e',
      'text-outline-opacity': 1,
      'min-zoomed-font-size': 0,
      'padding': '22px',
      'z-index': 0,
      'events': 'no',
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
      'width': 'mapData(weight, 0, 2, 0.8, 2)',
      'line-color': '#7e869e',
      'curve-style': 'straight',
      'opacity': 0.75,
      'target-arrow-shape': 'none',
    }},
    { selector: 'edge.highlight', style: {
      'line-color': '#7aa2f7',
      'opacity': 1,
      'width': 1.8,
      'z-index': 5,
    }},
    { selector: 'edge.pull', style: {
      'opacity': 0,
      'events': 'no',
      'width': 0.1,
    }},
    { selector: '.faded', style: { 'opacity': 0.12 }},
    { selector: 'edge.faded', style: { 'opacity': 0.05 }},
  ],
  layout: {
    name: 'fcose',
    animate: false,
    randomize: true,
    nodeRepulsion: 8500,
    idealEdgeLength: 55,
    edgeElasticity: 0.25,
    gravity: 0.2,
    gravityRangeCompound: 1.8,
    gravityCompound: 1.2,
    numIter: 4000,
    tile: false,
    packComponents: true,
    nodeSeparation: 90,
    uniformNodeDimensions: true,
  },
  wheelSensitivity: 0.25,
  minZoom: 0.1,
  maxZoom: 4,
});

// Zoom-gated labels
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
document.getElementById('btn-fit').addEventListener('click', () => cy.fit(null, 40));
document.getElementById('btn-relayout').addEventListener('click', () => cy.layout({
  name: 'fcose', animate: true, randomize: false, nodeRepulsion: 7500,
  idealEdgeLength: 45, edgeElasticity: 0.3, numIter: 2500,
}).run());

// Hover neighborhood focus
cy.on('mouseover', 'node', evt => {
  if (evt.target.data('is_area')) return;
  const n = evt.target;
  // Exclude invisible pull edges from neighborhood
  const visibleEdges = n.connectedEdges().not('.pull');
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

// Click detail
cy.on('tap', 'node', evt => {
  const d = evt.target.data();
  if (d.is_area) return;
  renderDetail(d, evt.target);
});
cy.on('tap', evt => {
  if (evt.target === cy) {
    document.getElementById('detail').innerHTML = '<p class="empty">Click a node to inspect.<br><br>Hover a node to focus its neighborhood.</p>';
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
  const q = (document.getElementById('search').value || '').toLowerCase().trim();

  cy.nodes().forEach(n => {
    const d = n.data();
    if (d.is_area) { n.style('display', activeAreas.has(d.area) ? 'element' : 'none'); return; }
    const areaOk = activeAreas.has(d.area || '(unowned)');
    const typeOk = activeTypes.has(d.type);
    const searchOk = !q || (d.label || '').toLowerCase().includes(q) || (d.id || '').toLowerCase().includes(q);
    n.style('display', areaOk && typeOk && searchOk ? 'element' : 'none');
  });
  cy.edges().forEach(e => {
    const visible = e.source().style('display') !== 'none' && e.target().style('display') !== 'none';
    e.style('display', visible ? 'element' : 'none');
  });
}

function repositionAreaLabels() {
  // After layout, move each area node to the centroid of same-area nodes.
  const groups = {};
  cy.nodes().forEach(n => {
    if (n.data('is_area')) return;
    const a = n.data('area');
    if (!a) return;
    const p = n.position();
    (groups[a] ||= []).push(p);
  });
  cy.nodes('[?is_area]').forEach(area => {
    const pts = groups[area.data('area')];
    if (!pts || !pts.length) return;
    const sx = pts.reduce((s, p) => s + p.x, 0) / pts.length;
    const sy = pts.reduce((s, p) => s + p.y, 0) / pts.length;
    area.position({ x: sx, y: sy });
  });
}

cy.ready(() => {
  repositionAreaLabels();
  cy.fit(null, 60);
  const z = cy.zoom();
  if (z < 0.7) cy.zoom({ level: 0.7, renderedPosition: { x: cy.width()/2, y: cy.height()/2 } });
  applyLabelMode();
});
// Also reposition after manual re-layout
const origRelayout = document.getElementById('btn-relayout').onclick;
document.getElementById('btn-relayout').addEventListener('click', () => {
  setTimeout(repositionAreaLabels, 1500);
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
