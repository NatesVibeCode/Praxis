#!/usr/bin/env python3
"""Generate a single-file HTML atlas of the Praxis knowledge graph.

Output: artifacts/atlas.html (a self-contained page with cytoscape.js from CDN).

Design: Obsidian-style graph view with compound-node area containers,
zoom-gated labels, hover neighborhood focus, and semantic color-by-area.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

from runtime import atlas_graph as atlas_graph_read_model


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
    --bg: #090a0d;
    --bg-panel: #11131a;
    --bg-elev: #171a23;
    --bg-control: #1b2030;
    --border: #2b3140;
    --border-strong: #485064;
    --text: #f3efe6;
    --text-soft: #d8d2c5;
    --text-dim: #9299ab;
    --accent: #f3efe6;
    --accent-cool: #7dcfff;
  }
  html, body { margin: 0; padding: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; background: var(--bg); color: var(--text); }
  #app { display: grid; grid-template-columns: minmax(248px, 18vw) minmax(0, 1fr) minmax(300px, 23vw); grid-template-rows: 52px minmax(0, 1fr); height: 100vh; min-width: 920px; }
  header { grid-column: 1 / 4; padding: 0 18px; background: rgba(17, 19, 26, 0.96); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 14px; box-shadow: 0 14px 30px rgba(0, 0, 0, 0.22); z-index: 5; }
  header h1 { font-size: 15px; margin: 0; font-weight: 700; letter-spacing: 0; color: var(--text); white-space: nowrap; }
  header .counts { font-size: 11px; color: var(--text-dim); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  header .hint { font-size: 11px; color: var(--accent-cool); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  #sidebar { background: rgba(17, 19, 26, 0.98); padding: 14px; overflow-y: auto; border-right: 1px solid var(--border); font-size: 12px; scrollbar-gutter: stable; }
  #sidebar h2 { font-size: 10px; text-transform: uppercase; letter-spacing: 0.12em; color: var(--text-dim); margin: 16px 0 7px; font-weight: 700; }
  #sidebar h2:first-child { margin-top: 0; }
  #sidebar label { display: flex; align-items: center; min-height: 24px; padding: 2px 0; cursor: pointer; gap: 8px; color: var(--text-soft); }
  #sidebar label:hover { color: white; }
  #sidebar input[type=checkbox] { accent-color: var(--accent); }
  #sidebar input[type=search] { width: 100%; padding: 8px 10px; border-radius: 7px; border: 1px solid var(--border); background: #0c0e13; color: var(--text); font-size: 12px; box-sizing: border-box; outline: none; }
  #sidebar input[type=search]:focus { border-color: var(--border-strong); box-shadow: 0 0 0 3px rgba(125, 207, 255, 0.08); }
  .swatch { width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex-shrink: 0; box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.16); }
  .edge-swatch { width: 22px; height: 2px; display: inline-block; flex-shrink: 0; position: relative; }
  .edge-swatch.dashed { background: transparent; border-top: 2px dashed currentColor; height: 0; }
  .edge-swatch.dotted { background: transparent; border-top: 2px dotted currentColor; height: 0; }
  #cy {
    background:
      linear-gradient(rgba(255, 255, 255, 0.028) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255, 255, 255, 0.028) 1px, transparent 1px),
      var(--bg);
    background-size: 32px 32px;
    width: 100%;
    height: 100%;
    min-width: 0;
    min-height: 0;
  }
  #cy.atlas-load-error { display: grid; place-items: center; padding: 28px; color: var(--text-dim); background: var(--bg); box-sizing: border-box; }
  #detail { background: rgba(17, 19, 26, 0.98); padding: 18px; border-left: 1px solid var(--border); overflow-y: auto; font-size: 12px; scrollbar-gutter: stable; }
  #detail h2 { font-size: 16px; margin: 0 0 8px; font-weight: 700; line-height: 1.25; color: var(--text); }
  #detail .tags { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
  #detail .tag { font-size: 10px; padding: 3px 7px; border-radius: 7px; background: var(--bg-elev); color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.08em; border: 1px solid rgba(255, 255, 255, 0.06); }
  #detail .tag.area { color: var(--accent); }
  #detail .src { color: var(--text-dim); font-size: 10px; margin-bottom: 10px; font-family: ui-monospace, monospace; word-break: break-all; }
  #detail pre { white-space: pre-wrap; word-break: break-word; font-size: 11px; background: #0c0e13; padding: 10px; border-radius: 7px; max-height: 300px; overflow-y: auto; border: 1px solid var(--border); line-height: 1.55; color: var(--text-soft); }
  #detail .empty { color: var(--text-dim); font-size: 12px; line-height: 1.6; }
  #detail .neighbors { margin-top: 14px; }
  #detail .neighbors .nrow { padding: 5px 0; font-size: 11px; display: flex; align-items: center; gap: 7px; cursor: pointer; color: var(--text-soft); }
  #detail .neighbors .nrow:hover { color: white; }
  button { background: var(--bg-control); color: var(--text-soft); border: 1px solid var(--border); padding: 6px 10px; border-radius: 7px; font-size: 11px; font-weight: 650; cursor: pointer; }
  button:hover { border-color: var(--border-strong); background: #22283a; color: white; }
  .toolbar { margin-left: auto; display: flex; gap: 6px; }
  .atlas-error-title { color: var(--text); font-weight: 700; margin-bottom: 6px; }
  .atlas-error-copy { max-width: 46ch; line-height: 1.55; text-align: center; }
  @media (max-width: 1040px) {
    #app { min-width: 0; grid-template-columns: 220px minmax(0, 1fr); grid-template-rows: 52px minmax(0, 1fr) 220px; }
    header { grid-column: 1 / 3; }
    #detail { grid-column: 1 / 3; border-left: 0; border-top: 1px solid var(--border); }
    .toolbar button { padding-inline: 8px; }
  }
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

function notifyHost(payload) {
  try {
    if (window.parent && window.parent !== window) {
      window.parent.postMessage({ type: 'praxis-atlas-status', ...payload }, window.location.origin);
    }
  } catch (_) {
    // Host status is best-effort only; the atlas must still render standalone.
  }
}

function showAtlasError(title, detail) {
  const cyBox = document.getElementById('cy');
  const detailBox = document.getElementById('detail');
  if (cyBox) {
    cyBox.classList.add('atlas-load-error');
    cyBox.innerHTML =
      '<div><div class="atlas-error-title"></div><div class="atlas-error-copy"></div></div>';
    cyBox.querySelector('.atlas-error-title').textContent = title;
    cyBox.querySelector('.atlas-error-copy').textContent = detail;
  }
  if (detailBox) {
    detailBox.innerHTML = '<h2></h2><p class="empty"></p>';
    detailBox.querySelector('h2').textContent = title;
    detailBox.querySelector('.empty').textContent = detail;
  }
  notifyHost({ ok: false, detail: title + ': ' + detail });
}

if (typeof cytoscape !== 'function') {
  showAtlasError(
    'Atlas runtime unavailable',
    'The graph library did not load, so the generated atlas cannot draw its canvas.'
  );
  throw new Error('Praxis atlas runtime unavailable: cytoscape missing');
}

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

function stableHash(value) {
  let hash = 2166136261;
  const text = String(value || '');
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seedCirclePositions(nodes, radiusBase, spread) {
  const sorted = nodes.sort((a, b) => String(a.id()).localeCompare(String(b.id())));
  const count = Math.max(sorted.length, 1);
  const golden = Math.PI * (3 - Math.sqrt(5));
  sorted.forEach((node, index) => {
    const h = stableHash(node.id());
    const ring = 0.78 + ((h % 17) / 100);
    const radius = radiusBase + spread * Math.sqrt((index + 1) / count);
    const angle = index * golden + ((h % 29) / 29) * 0.22;
    node.position({
      x: Math.cos(angle) * radius * ring,
      y: Math.sin(angle) * radius * ring,
    });
  });
}

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
      'background-opacity': 0.92,
      'label': 'data(label)',
      'color': '#f3efe6',
      'font-size': '10px',
      'font-family': 'ui-monospace, SFMono-Regular, Menlo, monospace',
      'text-valign': 'bottom',
      'text-halign': 'center',
      'text-margin-y': 5,
      'width': 'data(size)',
      'height': 'data(size)',
      'border-width': 1,
      'border-color': 'rgba(255, 255, 255, 0.22)',
      'border-opacity': 0.35,
      'text-outline-width': 3,
      'text-outline-color': '#090a0d',
      'text-outline-opacity': 1,
      'min-zoomed-font-size': 8,
      'text-opacity': 0,
    }},
    { selector: 'node[?is_area]', style: {
      'background-color': 'data(color)',
      'background-opacity': 0.14,
      'border-width': 2,
      'border-color': 'data(color)',
      'border-opacity': 0.74,
      'shape': 'round-rectangle',
      'label': 'data(label)',
      'font-size': '16px',
      'font-weight': 700,
      'color': 'data(color)',
      'text-valign': 'center',
      'text-halign': 'center',
      'text-margin-y': 0,
      'text-opacity': 1,
      'text-outline-width': 5,
      'text-outline-color': '#090a0d',
      'text-outline-opacity': 1,
      'min-zoomed-font-size': 0,
      'padding': '30px',
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
    { selector: 'node.fallback-node', style: {
      'width': 'mapData(degree, 1, 8, 14, 30)',
      'height': 'mapData(degree, 1, 8, 14, 30)',
      'background-opacity': 0.96,
      'border-width': 1,
      'border-color': '#f3efe6',
      'border-opacity': 0.28,
      'font-size': '9px',
      'text-margin-y': 5,
      'min-zoomed-font-size': 7,
      'z-index': 3,
    }},
    { selector: 'node.hover', style: {
      'text-opacity': 1,
      'z-index': 10,
    }},
    { selector: 'edge', style: {
      'width': 1.2,
      'line-color': '#8f98ad',
      'curve-style': 'straight',
      'opacity': 0.72,
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
      'width': 'mapData(weight, 1, 40, 2.0, 8)',
      'opacity': 0.82,
      'curve-style': 'bezier',
      'control-point-step-size': 40,
      'target-arrow-shape': 'triangle-backcurve',
      'arrow-scale': 0.9,
      'z-index': 2,
    }},
    { selector: 'edge.fallback-edge', style: {
      'width': 'mapData(weight, 1, 10, 1.1, 3.2)',
      'opacity': 0.46,
      'curve-style': 'bezier',
      'control-point-step-size': 28,
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
window.__PRAXIS_ATLAS_CY__ = cy;

// Progressive disclosure state
const expandedAreas = new Set();
let overviewLayoutGeneration = 0;

function fitVisibleGraph(padding) {
  const visible = cy.nodes().filter(n => n.style('display') !== 'none');
  const target = visible.length ? visible : cy.nodes('[?is_area]');
  if (target.length) {
    cy.fit(target, padding);
  }
}

function hasAreaOverview() {
  return cy.nodes('[?is_area]').length > 0;
}

function syncFallbackMode() {
  const fallback = !hasAreaOverview();
  cy.nodes().not('[?is_area]').toggleClass('fallback-node', fallback);
  cy.edges().not('.pull').toggleClass('fallback-edge', fallback);
  return fallback;
}

function atlasAreaPositions() {
  return cy.nodes('[?is_area]').map(n => ({
    id: n.id(),
    x: Math.round(n.position('x')),
    y: Math.round(n.position('y')),
  })).sort((a, b) => a.id.localeCompare(b.id));
}

function markAtlasReady() {
  window.__PRAXIS_ATLAS_READY__ = true;
  window.__PRAXIS_ATLAS_AREA_POSITIONS__ = atlasAreaPositions();
  notifyHost({
    ok: true,
    nodes: GRAPH.nodes.length,
    edges: realEdges,
    aggregate_edges: aggEdges,
  });
}

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
  const fallback = !hasAreaOverview();
  const show = labelMode === 'always' || (labelMode === 'auto' && (fallback || zoom > 1.3));
  cy.nodes().not('[?is_area]').not('.hover').style('text-opacity', show ? 1 : 0);
}
cy.on('zoom', applyLabelMode);

document.getElementById('btn-labels').addEventListener('click', () => {
  labelMode = labelMode === 'auto' ? 'always' : labelMode === 'always' ? 'off' : 'auto';
  document.getElementById('btn-labels').textContent = 'Labels: ' + labelMode;
  applyLabelMode();
});
document.getElementById('btn-fit').addEventListener('click', () => {
  fitVisibleGraph(60);
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
  if (!hasAreaOverview()) {
    hint.textContent = 'table dependency fallback';
    return;
  }
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

function layoutFallback(animate) {
  syncFallbackMode();
  seedCirclePositions(cy.nodes().not('[?is_area]'), 120, 520);
  let layout;
  try {
    layout = cy.elements().layout({
    name: 'fcose',
    animate: !!animate,
    animationDuration: animate ? 400 : 0,
    randomize: false,
    nodeRepulsion: 90000,
    idealEdgeLength: 160,
    edgeElasticity: 0.12,
    gravity: 0.08,
    numIter: 2200,
    packComponents: true,
    uniformNodeDimensions: false,
    nodeSeparation: 120,
    fit: false,
    });
  } catch (error) {
    showAtlasError('Atlas layout failed', error instanceof Error ? error.message : String(error));
    return;
  }
  layout.one('layoutstop', () => {
    cy.resize();
    fitVisibleGraph(70);
    applyLabelMode();
    markAtlasReady();
  });
  layout.run();
}

function syncOverviewState() {
  syncFallbackMode();
  cy.nodes('[?is_area]').forEach(a => {
    const p = a.position();
    cy.nodes().filter(n => !n.data('is_area') && n.data('area') === a.data('area'))
      .forEach(m => m.position({ x: p.x, y: p.y }));
  });
  laidOutAreas.clear();
  expandedAreas.forEach(slug => layoutArea(slug));
  syncExpandedClass();
  applyFilters();
  updateModeHint();
  cy.resize();
  fitVisibleGraph(80);
  applyLabelMode();
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
  const hasAreaFilters = document.querySelectorAll('#area-filters input').length > 0;
  const activeTypes = new Set([...document.querySelectorAll('#type-filters input:checked')].map(i => i.dataset.type));
  const activeRelations = new Set([...document.querySelectorAll('#relation-filters input:checked')].map(i => i.dataset.relation));
  const q = (document.getElementById('search').value || '').toLowerCase().trim();
  const overviewMode = hasAreaOverview();

  cy.nodes().forEach(n => {
    const d = n.data();
    if (d.is_area) {
      n.style('display', activeAreas.has(d.area) ? 'element' : 'none');
      return;
    }
    const areaOk = !hasAreaFilters || activeAreas.has(d.area || '(unowned)');
    const typeOk = activeTypes.has(d.type);
    const searchMatch = q && ((d.label || '').toLowerCase().includes(q) || (d.id || '').toLowerCase().includes(q));
    // Overview layer: non-area nodes hidden until their area is expanded
    // (or the user is actively searching for them).
    const expanded = !overviewMode || (d.area && expandedAreas.has(d.area));
    const visible = areaOk && typeOk && (overviewMode ? (expanded || searchMatch) : (searchMatch || !q));
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
  if (!hasAreaOverview()) {
    layoutFallback(animate);
    return;
  }
  const generation = ++overviewLayoutGeneration;
  const areaEles = cy.nodes('[?is_area]').union(cy.edges('[?is_aggregate]'));
  seedCirclePositions(cy.nodes('[?is_area]'), 130, 580);
  let layout;
  try {
    layout = areaEles.layout({
    name: 'fcose',
    animate: !!animate,
    animationDuration: animate ? 500 : 0,
    randomize: false,
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
  } catch (error) {
    showAtlasError('Atlas layout failed', error instanceof Error ? error.message : String(error));
    return;
  }
  layout.one('layoutstop', () => {
    if (generation !== overviewLayoutGeneration) return;
    syncOverviewState();
    markAtlasReady();
  });
  layout.run();
}

cy.ready(() => {
  syncFallbackMode();
  layoutOverview(false);
  applyFilters();
  syncExpandedClass();
  updateModeHint();
  applyLabelMode();
  requestAnimationFrame(() => {
    cy.resize();
    if (!hasAreaOverview()) {
      fitVisibleGraph(70);
    }
  });
});

window.addEventListener('resize', () => {
  cy.resize();
  fitVisibleGraph(80);
});

// Re-layout button re-runs overview (then re-expands any open areas).
document.getElementById('btn-relayout').onclick = null;
document.getElementById('btn-relayout').addEventListener('click', () => {
  layoutOverview(true);
});
</script>
</body>
</html>
"""


def main() -> None:
    graph = atlas_graph_read_model.build_graph()
    html = HTML_TEMPLATE.replace(
        "__GRAPH_JSON__", json.dumps(graph, default=str)
    ).replace(
        "__AREA_COLORS__", json.dumps(atlas_graph_read_model.AREA_COLORS)
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
