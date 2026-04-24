import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent } from 'react';
import cytoscape, { type Core, type ElementDefinition, type NodeSingular } from 'cytoscape';
import fcose from 'cytoscape-fcose';
import './AtlasPage.css';

interface AtlasElementData {
  id: string;
  label?: string;
  source?: string;
  target?: string;
  type?: string;
  area?: string;
  preview?: string;
  color?: string;
  degree?: number;
  size?: number;
  weight?: number;
  authority_source?: string;
  relation_source?: string;
  object_kind?: string;
  category?: string;
  definition_summary?: string;
  surface_name?: string;
  route_ref?: string;
  binding_revision?: string;
  decision_ref?: string;
  updated_at?: string | null;
  activity_score?: number;
  is_area?: boolean;
  is_aggregate?: boolean;
  display_color?: string;
}

interface AtlasElement {
  data: AtlasElementData;
  classes?: string;
}

interface AtlasArea {
  slug: string;
  title: string;
  summary: string;
  color: string;
  member_count: number;
}

type GraphFreshnessState = 'fresh' | 'projection_lagging' | 'unknown' | string;

interface AtlasFreshness {
  graph_freshness_state?: GraphFreshnessState;
  memory_entities_max_updated_at?: string | null;
  memory_edges_max_updated_at?: string | null;
  authority_projection_last_run_at?: string | null;
  authority_projection_source_max_updated_at?: string | null;
  authority_projection_lag_seconds?: number | null;
  authority_projection_edge_count?: number;
  authority_projection_last_run_source?: string;
  freshness_error?: string;
}

interface AtlasMetadata extends AtlasFreshness {
  generated_at?: string;
  node_count: number;
  edge_count: number;
  aggregate_edge_count: number;
  source_authority: string;
  freshness?: AtlasFreshness;
}

interface AtlasPayload {
  ok: boolean;
  nodes: AtlasElement[];
  edges: AtlasElement[];
  areas: AtlasArea[];
  metadata: AtlasMetadata;
  warnings: string[];
  error?: string;
  detail?: string;
}

type ScopeKind = 'all' | 'area' | 'kind' | 'recent' | 'stale' | 'orphans' | 'search';

interface Scope {
  kind: ScopeKind;
  value: string;
  raw: string;
}

const SCOPE_ALL: Scope = { kind: 'all', value: '', raw: '' };
const ATLAS_POLL_MS = 30_000;
const ATLAS_GRAPH_TIMEOUT_MS = 15_000;
const ATLAS_RENDER_READY_TIMEOUT_MS = 4_000;
const ATLAS_EASING = 'ease-in-out-cubic';
const FLASH_MS = 1500;
const NEUTRAL_TONES = ['#f3efe6', '#d8d2c5', '#b7b0a3', '#8c8a84', '#6d6b67'];
const DEFAULT_GLOW = '#b7b0a3';

const TYPE_GLOW: Record<string, string> = {
  bug: '#f3efe6',
  issue: '#f3efe6',
  roadmap_item: '#d8d2c5',
  operator_decision: '#b7b0a3',
  decision: '#b7b0a3',
  workflow: '#f3efe6',
  workflow_build_intent: '#f3efe6',
  capability: '#d8d2c5',
  tool: '#d8d2c5',
  table: '#b7b0a3',
  surface_catalog_item: '#8c8a84',
  functional_area: '#f3efe6',
};

let fcoseRegistered = false;

declare global {
  interface Window {
    __PRAXIS_ATLAS_READY__?: boolean;
    __PRAXIS_ATLAS_VISIBLE_COUNTS__?: { nodes: number; edges: number };
  }
}

function ensureFcoseRegistered() {
  if (fcoseRegistered) return;
  cytoscape.use(fcose);
  fcoseRegistered = true;
}

function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function toneFor(data: AtlasElementData): string {
  const key = data.area || data.type || data.object_kind || data.id;
  return NEUTRAL_TONES[stableHash(key) % NEUTRAL_TONES.length];
}

function glowFor(data: AtlasElementData): string {
  return TYPE_GLOW[data.type || ''] || TYPE_GLOW[data.object_kind || ''] || toneFor(data) || DEFAULT_GLOW;
}

function formatWhen(value?: string | null) {
  if (!value) return 'unknown';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function freshnessTone(state: GraphFreshnessState | undefined) {
  if (state === 'fresh') return 'ok';
  if (state === 'projection_lagging') return 'warn';
  return 'unknown';
}

function freshnessTooltip(payload: AtlasPayload) {
  const f = payload.metadata.freshness || payload.metadata;
  const lag = f.authority_projection_lag_seconds;
  return [
    `Generated ${formatWhen(payload.metadata.generated_at)}`,
    `Entities ${formatWhen(f.memory_entities_max_updated_at)}`,
    `Edges ${formatWhen(f.memory_edges_max_updated_at)}`,
    `Projection ${formatWhen(f.authority_projection_last_run_at)}`,
    typeof lag === 'number' ? `Lag ${Math.round(lag)}s` : null,
    f.freshness_error ? `Error ${f.freshness_error}` : null,
    `${payload.metadata.node_count} nodes / ${payload.metadata.edge_count} edges`,
  ].filter(Boolean).join('\n');
}

function seedCirclePositions(nodes: cytoscape.NodeCollection, radius: number) {
  const sorted = nodes.sort((a, b) => String(a.id()).localeCompare(String(b.id())));
  const count = Math.max(sorted.length, 1);
  const golden = Math.PI * (3 - Math.sqrt(5));
  sorted.forEach((node, index) => {
    const h = stableHash(node.id());
    const ring = 0.82 + ((h % 17) / 100);
    const r = radius * Math.sqrt((index + 1) / count);
    const angle = index * golden + ((h % 29) / 29) * 0.22;
    node.position({ x: Math.cos(angle) * r * ring, y: Math.sin(angle) * r * ring });
  });
}

function memberClusterRadius(count: number): number {
  if (count <= 1) return 52;
  return Math.min(360, Math.max(88, 46 + 34 * Math.sqrt(count)));
}

function memberCloudPositions(
  members: cytoscape.NodeCollection,
  center: { x: number; y: number },
  radius: number,
  areaSlug: string,
) {
  const positions = new Map<string, { x: number; y: number }>();
  const sorted = members.sort((a, b) => {
    const degreeDelta = b.degree(true) - a.degree(true);
    if (degreeDelta !== 0) return degreeDelta;
    return String(a.id()).localeCompare(String(b.id()));
  });
  const count = Math.max(sorted.length, 1);
  const golden = Math.PI * (3 - Math.sqrt(5));
  sorted.forEach((node, index) => {
    const h = stableHash(`${areaSlug}:${node.id()}`);
    const jitter = (h % 997) / 997;
    const progress = count === 1 ? 0 : (index + 0.35) / count;
    const lane = 0.84 + ((h % 23) / 100);
    const angle = index * golden + jitter * 0.46;
    const distance = Math.sqrt(progress) * radius * lane;
    positions.set(node.id(), {
      x: center.x + Math.cos(angle) * distance,
      y: center.y + Math.sin(angle) * distance,
    });
  });
  return positions;
}

function toElementDefinitions(payload: AtlasPayload): ElementDefinition[] {
  return [
    ...payload.nodes.map((node) => ({
      data: {
        ...node.data,
        display_color: toneFor(node.data),
      },
      classes: node.classes,
    })),
    ...payload.edges.map((edge) => ({ data: edge.data, classes: edge.classes })),
  ] as ElementDefinition[];
}

function parseScope(raw: string): Scope {
  const trimmed = raw.trim();
  if (!trimmed) return SCOPE_ALL;
  const lower = trimmed.toLowerCase();
  if (lower === 'recent' || lower === 'live') return { kind: 'recent', value: '', raw: trimmed };
  if (lower === 'stale' || lower === 'cold') return { kind: 'stale', value: '', raw: trimmed };
  if (lower === 'orphans' || lower === 'orphan') return { kind: 'orphans', value: '', raw: trimmed };
  const areaMatch = lower.match(/^area:(.+)$/);
  if (areaMatch) return { kind: 'area', value: areaMatch[1].trim(), raw: trimmed };
  const kindMatch = lower.match(/^kind:(.+)$/);
  if (kindMatch) return { kind: 'kind', value: kindMatch[1].trim(), raw: trimmed };
  return { kind: 'search', value: lower, raw: trimmed };
}

function scopeLabel(scope: Scope): string {
  if (scope.kind === 'all') return 'all';
  if (scope.kind === 'area') return `area · ${scope.value}`;
  if (scope.kind === 'kind') return `kind · ${scope.value}`;
  if (scope.kind === 'recent') return 'recent';
  if (scope.kind === 'stale') return 'stale';
  if (scope.kind === 'orphans') return 'orphans';
  return `"${scope.raw}"`;
}

function nodeMatchesScope(data: AtlasElementData, scope: Scope, degree: number): boolean {
  if (data.is_area) return false;
  if (scope.kind === 'all') return true;
  if (scope.kind === 'area') return (data.area || '').toLowerCase() === scope.value;
  if (scope.kind === 'kind') {
    return [data.type, data.object_kind, data.category]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(scope.value));
  }
  if (scope.kind === 'recent') return (data.activity_score ?? 0) >= 0.5;
  if (scope.kind === 'stale') return (data.activity_score ?? 0) < 0.2 && Boolean(data.updated_at);
  if (scope.kind === 'orphans') return degree <= 1;
  if (scope.kind === 'search') {
    const haystack = [
      data.label,
      data.id,
      data.type,
      data.area,
      data.authority_source,
      data.object_kind,
      data.definition_summary,
      data.route_ref,
      data.decision_ref,
    ].filter(Boolean).join(' ').toLowerCase();
    return haystack.includes(scope.value);
  }
  return true;
}

function nodeKindLabel(data: AtlasElementData) {
  return data.object_kind || data.category || data.type || 'unknown';
}

function nodeAuthorityLabel(data: AtlasElementData) {
  return data.authority_source || data.source || 'unknown';
}

function useAtlasGraph() {
  const [payload, setPayload] = useState<AtlasPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState(Date.now());

  const refresh = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    setError(null);
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), ATLAS_GRAPH_TIMEOUT_MS);
    try {
      const response = await fetch('/api/atlas/graph', {
        cache: 'no-store',
        signal: controller.signal,
      });
      const body = await response.json().catch(() => null) as AtlasPayload | null;
      if (!response.ok || !body?.ok) {
        throw new Error(body?.detail || body?.error || `Atlas graph request failed with HTTP ${response.status}`);
      }
      setPayload(body);
      setLastRefresh(Date.now());
    } catch (err) {
      if (!silent) setPayload(null);
      if (err instanceof Error && err.name === 'AbortError') {
        setError(`Atlas graph request timed out after ${Math.ceil(ATLAS_GRAPH_TIMEOUT_MS / 1000)}s.`);
      } else {
        setError(err instanceof Error ? err.message : 'Atlas graph is unavailable.');
      }
    } finally {
      window.clearTimeout(timeout);
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    const id = window.setInterval(() => { refresh(true); }, ATLAS_POLL_MS);
    return () => window.clearInterval(id);
  }, [refresh]);

  return { payload, loading, error, refresh, lastRefresh };
}

export function AtlasPage() {
  const { payload, loading, error, refresh, lastRefresh } = useAtlasGraph();
  const graphRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const expandedAreasRef = useRef<Set<string>>(new Set());
  const scopeRef = useRef<Scope>(SCOPE_ALL);
  const selectedNodeIdRef = useRef<string | null>(null);
  const previousUpdatedAtRef = useRef<Map<string, string>>(new Map());
  const changedNodeIdsRef = useRef<Set<string>>(new Set());

  const [expandedAreas, setExpandedAreas] = useState<Set<string>>(new Set());
  const [scope, setScope] = useState<Scope>(SCOPE_ALL);
  const [commandOpen, setCommandOpen] = useState(false);
  const [commandDraft, setCommandDraft] = useState('');
  const [selectedNode, setSelectedNode] = useState<AtlasElementData | null>(null);
  const [selectedNeighbors, setSelectedNeighbors] = useState<AtlasElementData[]>([]);
  const [cardPosition, setCardPosition] = useState<{ x: number; y: number } | null>(null);
  const [renderIssue, setRenderIssue] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  const nodeById = useMemo(() => {
    const index = new Map<string, AtlasElementData>();
    payload?.nodes.forEach((node) => index.set(node.data.id, node.data));
    return index;
  }, [payload]);

  const degreeMap = useMemo(() => {
    const d = new Map<string, number>();
    payload?.edges.forEach((edge) => {
      if (edge.data.is_aggregate) return;
      if (edge.data.source) d.set(edge.data.source, (d.get(edge.data.source) || 0) + 1);
      if (edge.data.target) d.set(edge.data.target, (d.get(edge.data.target) || 0) + 1);
    });
    return d;
  }, [payload]);

  const activeCount = useMemo(() => {
    if (!payload) return 0;
    return payload.nodes.reduce((count, node) => {
      const data = node.data;
      if (data.is_area) return count;
      const degree = degreeMap.get(data.id) || 0;
      return nodeMatchesScope(data, scope, degree) ? count + 1 : count;
    }, 0);
  }, [degreeMap, payload, scope]);

  const hotCount = useMemo(() => {
    if (!payload) return 0;
    return payload.nodes.reduce((count, node) => (
      !node.data.is_area && (node.data.activity_score ?? 0) >= 0.5 ? count + 1 : count
    ), 0);
  }, [payload]);

  const applyLabelMode = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const zoom = cy.zoom();
    cy.nodes().not('[?is_area]').forEach((node) => {
      const hovered = node.hasClass('hover') || node.selected();
      node.style('text-opacity', hovered || zoom > 1.35 ? 1 : 0);
    });
  }, []);

  const setGlobalReady = useCallback((cy: Core, readyState: boolean) => {
    const visibleNodes = cy.nodes().filter((node) => node.style('display') !== 'none').length;
    const visibleEdges = cy.edges().filter((edge) => edge.style('display') !== 'none').length;
    window.__PRAXIS_ATLAS_READY__ = readyState;
    window.__PRAXIS_ATLAS_VISIBLE_COUNTS__ = { nodes: visibleNodes, edges: visibleEdges };
  }, []);

  const fitVisibleGraph = useCallback((padding = 90) => {
    const cy = cyRef.current;
    if (!cy) return;
    const expanded = expandedAreasRef.current;
    if (expanded.size > 0) {
      const expandedTarget = cy.nodes().filter((node) => {
        const data = node.data() as AtlasElementData;
        if (data.is_area) return node.style('display') !== 'none';
        return expanded.has(data.area || '') && node.style('display') !== 'none';
      });
      if (expandedTarget.length) {
        cy.fit(expandedTarget, padding);
        setGlobalReady(cy, true);
        return;
      }
    }
    const visible = cy.nodes().filter((node) => node.style('display') !== 'none');
    const target = visible.length ? visible : cy.nodes('[?is_area]');
    if (target.length) cy.fit(target, padding);
    setGlobalReady(cy, true);
  }, [setGlobalReady]);

  const setAreaVisualState = useCallback((areaNode: NodeSingular, expanded: boolean, animate: boolean) => {
    const collapsedSize = Number(areaNode.data('collapsed_size') || areaNode.data('size') || 92);
    const targetSize = collapsedSize;
    areaNode.toggleClass('expanded', expanded);
    areaNode.stop(true, false);
    if (!animate) {
      areaNode.style({ width: targetSize, height: targetSize });
      return;
    }
    areaNode.animate(
      { style: { width: targetSize, height: targetSize } },
      { duration: 420, easing: ATLAS_EASING },
    );
  }, []);

  const layoutArea = useCallback((areaSlug: string) => {
    const cy = cyRef.current;
    if (!cy) return;
    const areaNode = cy.getElementById(`area::${areaSlug}`);
    const members = cy.nodes().filter((node) => !node.data('is_area') && node.data('area') === areaSlug);
    if (!areaNode.length || !members.length) return;
    const center = areaNode.position();
    const radius = memberClusterRadius(members.length);
    const positions = memberCloudPositions(members, center, radius, areaSlug);
    members.layout({
      name: 'preset',
      positions: (nodeId: string | NodeSingular) => {
        const id = typeof nodeId === 'string' ? nodeId : nodeId.id();
        return positions.get(id) || center;
      },
      animate: true,
      animationDuration: 460,
      animationEasing: ATLAS_EASING,
      fit: false,
      padding: 24,
    } as cytoscape.LayoutOptions).run();
  }, []);

  const displaceAreaOrbit = useCallback((focusArea: string) => {
    const cy = cyRef.current;
    if (!cy) return;
    const focusNode = cy.getElementById(`area::${focusArea}`);
    if (!focusNode.length) return;
    const center = focusNode.position();
    const memberCount = cy.nodes().filter((node) => !node.data('is_area') && node.data('area') === focusArea).length;
    const orbitRadius = memberClusterRadius(memberCount) + 290;
    const otherAreas = cy.nodes('[?is_area]')
      .filter((node) => node.data('area') !== focusArea && node.style('display') !== 'none')
      .sort((a, b) => String(a.id()).localeCompare(String(b.id())));
    const count = Math.max(otherAreas.length, 1);
    otherAreas.forEach((node, index) => {
      const hashOffset = (stableHash(`${focusArea}:${node.id()}`) % 19) / 19;
      const angle = -Math.PI / 2 + (index / count) * Math.PI * 2 + hashOffset * 0.08;
      const xRadius = orbitRadius * 1.18;
      const yRadius = orbitRadius * 0.88;
      node.animate(
        {
          position: {
            x: center.x + Math.cos(angle) * xRadius,
            y: center.y + Math.sin(angle) * yRadius,
          },
        },
        { duration: 460, easing: ATLAS_EASING },
      );
    });
  }, []);

  const applyScopeVisuals = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const currentScope = scopeRef.current;
    const expanded = expandedAreasRef.current;

    cy.nodes().forEach((node) => {
      const data = node.data() as AtlasElementData;
      if (data.is_area) {
        const matchesAreaScope = currentScope.kind !== 'area'
          || (data.area || '').toLowerCase() === currentScope.value;
        node.style('display', matchesAreaScope ? 'element' : 'none');
        node.toggleClass('scope-dim', currentScope.kind !== 'all' && currentScope.kind !== 'area');
        setAreaVisualState(node, expanded.has(data.area || ''), false);
        return;
      }
      const degree = degreeMap.get(data.id) || 0;
      const inScope = nodeMatchesScope(data, currentScope, degree);
      const areaExpanded = expanded.has(data.area || '');
      const isLandmark = (data.activity_score ?? 0) >= 0.86 || degree >= 8;
      const isTexturePoint = stableHash(data.id) % 100 < 18;
      const visible = areaExpanded
        ? isLandmark || isTexturePoint
        : currentScope.kind !== 'all' && inScope;
      node.style('display', visible ? 'element' : 'none');
      node.toggleClass('scope-hit', inScope && currentScope.kind !== 'all');
      node.toggleClass('scope-dim', !inScope && currentScope.kind !== 'all');
      node.toggleClass('expanded-member', visible && areaExpanded);
      node.toggleClass(
        'landmark',
        visible && areaExpanded && isLandmark,
      );
    });

    cy.edges().forEach((edge) => {
      const data = edge.data() as AtlasElementData;
      const sourceVisible = edge.source().style('display') !== 'none';
      const targetVisible = edge.target().style('display') !== 'none';
      if (!sourceVisible || !targetVisible) {
        edge.style('display', 'none');
        return;
      }
      if (data.is_aggregate) {
        const sourceArea = edge.source().data('area');
        const targetArea = edge.target().data('area');
        const bothExpanded = expanded.has(sourceArea) && expanded.has(targetArea);
        edge.style('display', bothExpanded ? 'none' : 'element');
        return;
      }
      edge.style('display', 'element');
    });

    applyLabelMode();
    setGlobalReady(cy, true);
  }, [applyLabelMode, degreeMap, setAreaVisualState, setGlobalReady]);

  const syncExpandedAreas = useCallback((next: Set<string>) => {
    const focusArea = Array.from(next)[0] || '';
    const focused = focusArea ? new Set([focusArea]) : new Set<string>();
    expandedAreasRef.current = focused;
    const cy = cyRef.current;
    if (cy) {
      cy.nodes('[?is_area]').forEach((node) => {
        setAreaVisualState(node, focused.has(node.data('area')), true);
      });
    }
    setExpandedAreas(new Set(focused));
    applyScopeVisuals();
    if (focusArea) {
      layoutArea(focusArea);
      displaceAreaOrbit(focusArea);
    }
    window.setTimeout(() => fitVisibleGraph(52), 480);
  }, [applyScopeVisuals, displaceAreaOrbit, fitVisibleGraph, layoutArea, setAreaVisualState]);

  const updateCardPosition = useCallback((nodeId: string) => {
    const cy = cyRef.current;
    if (!cy) return;
    const node = cy.getElementById(nodeId);
    if (!node.length) return;
    const rendered = node.renderedPosition();
    setCardPosition({ x: rendered.x, y: rendered.y });
  }, []);

  const selectNode = useCallback((nodeId: string, animate = true) => {
    const data = nodeById.get(nodeId);
    if (!data || data.is_area) return;
    const neighbors = payload?.edges
      .filter((edge) => !edge.data.is_aggregate && (edge.data.source === nodeId || edge.data.target === nodeId))
      .map((edge) => nodeById.get(edge.data.source === nodeId ? edge.data.target || '' : edge.data.source || ''))
      .filter((node): node is AtlasElementData => Boolean(node && !node.is_area)) || [];
    const cy = cyRef.current;
    if (cy) {
      const node = cy.getElementById(nodeId);
      if (node.length) {
        cy.$(':selected').unselect();
        node.select();
        const area = String(data.area || '');
        if (area) {
          const next = new Set([area]);
          expandedAreasRef.current = next;
          setExpandedAreas(new Set(next));
          layoutArea(area);
          displaceAreaOrbit(area);
        }
        applyScopeVisuals();
        if (animate) cy.animate({ center: { eles: node }, zoom: 1.55 }, { duration: 300 });
        updateCardPosition(nodeId);
      }
    }
    selectedNodeIdRef.current = nodeId;
    setSelectedNode(data);
    setSelectedNeighbors(neighbors);
  }, [applyScopeVisuals, displaceAreaOrbit, layoutArea, nodeById, payload, updateCardPosition]);

  const dismissSelection = useCallback(() => {
    const cy = cyRef.current;
    if (cy) cy.$(':selected').unselect();
    selectedNodeIdRef.current = null;
    setSelectedNode(null);
    setSelectedNeighbors([]);
    setCardPosition(null);
  }, []);

  const applyScope = useCallback((next: Scope) => {
    scopeRef.current = next;
    setScope(next);
    const cy = cyRef.current;
    if (!cy) return;
    applyScopeVisuals();

    if (next.kind === 'area' && next.value) {
      const expanded = new Set([next.value]);
      syncExpandedAreas(expanded);
      const areaNode = cy.getElementById(`area::${next.value}`);
      if (areaNode.length) cy.animate({ center: { eles: areaNode }, zoom: 1.08 }, { duration: 320 });
      return;
    }

    if (next.kind === 'search' && next.value) {
      const match = cy.nodes().filter((node) => {
        const data = node.data() as AtlasElementData;
        if (data.is_area) return false;
        const degree = degreeMap.get(data.id) || 0;
        return nodeMatchesScope(data, next, degree);
      }).first();
      if (match.length) selectNode(match.id());
      return;
    }

    window.setTimeout(() => fitVisibleGraph(52), 60);
  }, [applyScopeVisuals, degreeMap, fitVisibleGraph, selectNode, syncExpandedAreas]);

  useEffect(() => {
    if (!payload) return;
    const previous = previousUpdatedAtRef.current;
    const next = new Map<string, string>();
    const changed = new Set<string>();
    payload.nodes.forEach((node) => {
      const data = node.data;
      if (data.is_area || !data.updated_at) return;
      next.set(data.id, data.updated_at);
      const prev = previous.get(data.id);
      if ((prev && prev !== data.updated_at) || (!prev && previous.size > 0)) changed.add(data.id);
    });
    previousUpdatedAtRef.current = next;
    changedNodeIdsRef.current = changed;
  }, [payload]);

  useEffect(() => {
    if (!selectedNodeIdRef.current || !payload) return;
    const latest = nodeById.get(selectedNodeIdRef.current);
    if (!latest) {
      dismissSelection();
      return;
    }
    setSelectedNode(latest);
    const neighbors = payload.edges
      .filter((edge) => !edge.data.is_aggregate && (
        edge.data.source === latest.id || edge.data.target === latest.id
      ))
      .map((edge) => nodeById.get(edge.data.source === latest.id ? edge.data.target || '' : edge.data.source || ''))
      .filter((node): node is AtlasElementData => Boolean(node && !node.is_area));
    setSelectedNeighbors(neighbors);
  }, [dismissSelection, nodeById, payload]);

  useEffect(() => {
    if (!payload || !graphRef.current) return undefined;
    ensureFcoseRegistered();
    setReady(false);
    setRenderIssue(null);
    expandedAreasRef.current = new Set();
    scopeRef.current = SCOPE_ALL;
    setScope(SCOPE_ALL);
    setExpandedAreas(new Set());

    const cy = cytoscape({
      container: graphRef.current,
      elements: toElementDefinitions(payload),
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#050505',
            'background-opacity': 0.08,
            'border-width': 1,
            'border-color': 'data(display_color)',
            'border-opacity': 'mapData(activity_score, 0, 1, 0.2, 0.82)' as unknown as number,
            label: 'data(label)',
            color: '#f3efe6',
            'font-size': '10px',
            'font-family': 'ui-monospace, SFMono-Regular, Menlo, monospace',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'text-margin-y': 5,
            width: 'data(size)',
            height: 'data(size)',
            'text-outline-width': 3,
            'text-outline-color': '#050505',
            'text-outline-opacity': 1,
            'min-zoomed-font-size': 8,
            'text-opacity': 0,
            'overlay-color': 'data(display_color)',
            'overlay-opacity': 'mapData(activity_score, 0, 1, 0.02, 0.18)' as unknown as number,
            'overlay-padding': 8,
            'transition-property': 'opacity, border-opacity, overlay-opacity, width, height',
            'transition-duration': 220,
          },
        },
        {
          selector: 'node[!is_area]',
          style: {
            width: 'mapData(size, 6, 32, 9, 28)',
            height: 'mapData(size, 6, 32, 9, 28)',
            'z-index': 4,
          },
        },
        ...Object.entries(TYPE_GLOW).map(([type, hex]) => ({
          selector: `node[type = "${type}"]`,
          style: {
            'border-color': hex,
            'overlay-color': hex,
          },
        })),
        {
          selector: 'node[?is_area]',
          style: {
            'background-color': '#050505',
            'background-opacity': 0,
            'border-width': 2.5,
            'border-color': 'data(display_color)',
            'border-opacity': 'mapData(activity_score, 0, 1, 0.5, 1)' as unknown as number,
            'overlay-opacity': 0,
            'overlay-padding': 0,
            shape: 'ellipse',
            label: 'data(label)',
            'font-size': '17px',
            'font-weight': 700,
            color: '#f3efe6',
            'text-valign': 'center',
            'text-halign': 'center',
            'text-opacity': 1,
            'text-outline-width': 7,
            'text-outline-color': '#050505',
            'text-outline-opacity': 1,
            'min-zoomed-font-size': 0,
            'text-wrap': 'wrap',
            'text-max-width': '118px',
            padding: '24px',
            'text-background-color': '#050505',
            'text-background-opacity': 0,
            'text-background-padding': '0px',
            'text-background-shape': 'roundrectangle',
            'z-index': 1,
          },
        },
        {
          selector: 'node[?is_area].expanded',
          style: {
            'background-opacity': 0,
            'border-style': 'dashed',
            'border-opacity': 0.5,
            'text-valign': 'top',
            'text-margin-y': 16,
            'font-size': '14px',
            'text-max-width': '150px',
            'z-index': 0,
          },
        },
        {
          selector: 'node.expanded-member',
          style: {
            width: 5,
            height: 5,
            'background-opacity': 0.025,
            'border-opacity': 0.22,
            'overlay-opacity': 0.01 as unknown as number,
          },
        },
        {
          selector: 'node.landmark',
          style: {
            width: 'mapData(size, 6, 32, 12, 26)',
            height: 'mapData(size, 6, 32, 12, 26)',
            'background-opacity': 0.1,
            'border-opacity': 0.78,
            'overlay-opacity': 0.12 as unknown as number,
            'font-size': '9px',
            'text-opacity': 1,
            'text-wrap': 'wrap',
            'text-max-width': '76px',
            'text-margin-y': 6,
            'z-index': 8,
          },
        },
        {
          selector: 'node:selected',
          style: {
            'border-width': 2,
            'border-color': '#ffffff',
            'border-opacity': 1,
            'text-opacity': 1,
            'overlay-opacity': 0.32 as unknown as number,
          },
        },
        {
          selector: 'node.hover',
          style: {
            'text-opacity': 1,
            'border-opacity': 1,
            'z-index': 10,
          },
        },
        {
          selector: 'node.scope-hit',
          style: {
            'border-opacity': 1,
            'overlay-opacity': 0.26 as unknown as number,
          },
        },
        {
          selector: 'node.scope-dim',
          style: {
            'border-opacity': 0.1,
            'overlay-opacity': 0.01 as unknown as number,
          },
        },
        {
          selector: 'node.flash',
          style: {
            'border-color': '#ffffff',
            'border-opacity': 1,
            'overlay-color': '#ffffff',
            'overlay-opacity': 0.5 as unknown as number,
          },
        },
        {
          selector: 'edge',
          style: {
            width: 1,
            'line-color': '#5f5b53',
            'line-opacity': 0.36,
            'curve-style': 'bezier',
            'control-point-step-size': 30,
            'target-arrow-shape': 'none',
            'transition-property': 'opacity, width, line-color',
            'transition-duration': 180,
          },
        },
        {
          selector: 'edge[?is_aggregate]',
          style: {
            width: 'mapData(weight, 1, 40, 1, 4.8)',
            'line-opacity': 0.48,
            'curve-style': 'bezier',
            'control-point-step-size': 80,
            'z-index': 2,
          },
        },
        {
          selector: 'edge.highlight',
          style: {
            'line-color': '#f3efe6',
            'line-opacity': 0.96,
            width: 2,
            'z-index': 5,
          },
        },
        { selector: 'node.faded', style: { opacity: 0.08 } },
        { selector: 'edge.faded', style: { opacity: 0.04 } },
      ] as unknown as cytoscape.StylesheetJson,
      layout: { name: 'preset' },
      minZoom: 0.08,
      maxZoom: 4,
    });

    cyRef.current = cy;
    setGlobalReady(cy, false);
    cy.nodes('[?is_area]').forEach((node) => {
      node.data('collapsed_size', Number(node.data('size') || 92));
      setAreaVisualState(node, false, false);
    });

    const flashIds = changedNodeIdsRef.current;
    flashIds.forEach((id) => {
      const node = cy.getElementById(id);
      if (!node.length) return;
      node.addClass('flash');
      window.setTimeout(() => node.removeClass('flash'), FLASH_MS);
    });

    cy.on('zoom', applyLabelMode);
    cy.on('mouseover', 'node', (event) => {
      const node = event.target as NodeSingular;
      const visibleEdges = node.connectedEdges().filter((edge) => edge.style('display') !== 'none');
      const neighbors = visibleEdges.connectedNodes();
      const hood = node.union(neighbors).union(visibleEdges);
      cy.elements().not(hood).addClass('faded');
      hood.removeClass('faded');
      visibleEdges.addClass('highlight');
      node.addClass('hover');
      hood.nodes().style('text-opacity', 1);
    });
    cy.on('mouseout', 'node', (event) => {
      const node = event.target as NodeSingular;
      node.removeClass('hover');
      cy.elements().removeClass('faded');
      cy.edges().removeClass('highlight');
      applyLabelMode();
    });
    cy.on('tap', 'node', (event) => {
      const node = event.target as NodeSingular;
      const data = node.data() as AtlasElementData;
      if (data.is_area) {
        const area = data.area || '';
        const next = expandedAreasRef.current.has(area) ? new Set<string>() : new Set([area]);
        cy.elements().removeClass('faded');
        cy.edges().removeClass('highlight');
        cy.nodes().removeClass('hover');
        syncExpandedAreas(next);
        return;
      }
      selectNode(node.id(), false);
    });
    cy.on('pan zoom position', () => {
      if (selectedNodeIdRef.current) updateCardPosition(selectedNodeIdRef.current);
    });
    cy.on('tap', (event) => {
      if (event.target === cy) dismissSelection();
    });

    const readyTimeout = window.setTimeout(() => {
      if (cyRef.current !== cy || ready) return;
      applyScopeVisuals();
      cy.resize();
      fitVisibleGraph(52);
      setReady(true);
      setGlobalReady(cy, true);
    }, ATLAS_RENDER_READY_TIMEOUT_MS);

    try {
      const areaNodes = cy.nodes('[?is_area]');
      seedCirclePositions(areaNodes, 460);
      const overview = areaNodes.union(cy.edges('[?is_aggregate]')).layout({
        name: 'fcose',
        animate: false,
        randomize: false,
        nodeRepulsion: 125000,
        idealEdgeLength: 210,
        edgeElasticity: 0.1,
        gravity: 0.05,
        numIter: 3000,
        packComponents: true,
        uniformNodeDimensions: false,
        nodeSeparation: 115,
        fit: false,
      } as cytoscape.LayoutOptions);
      overview.one('layoutstop', () => {
        window.clearTimeout(readyTimeout);
        cy.nodes('[?is_area]').forEach((areaNode) => {
          const position = areaNode.position();
          cy.nodes().filter((node) => !node.data('is_area') && node.data('area') === areaNode.data('area'))
            .forEach((member) => {
              member.position(position);
            });
        });
        applyScopeVisuals();
        cy.resize();
        fitVisibleGraph(52);
        setReady(true);
        setGlobalReady(cy, true);
        if (selectedNodeIdRef.current) selectNode(selectedNodeIdRef.current, false);
      });
      overview.run();
    } catch (err) {
      window.clearTimeout(readyTimeout);
      applyScopeVisuals();
      cy.resize();
      fitVisibleGraph(52);
      setReady(true);
      setGlobalReady(cy, true);
      setRenderIssue(err instanceof Error ? err.message : 'Atlas layout failed.');
    }

    const onResize = () => {
      cy.resize();
      fitVisibleGraph(52);
    };
    window.addEventListener('resize', onResize);

    return () => {
      window.clearTimeout(readyTimeout);
      window.removeEventListener('resize', onResize);
      cy.destroy();
      if (cyRef.current === cy) cyRef.current = null;
      window.__PRAXIS_ATLAS_READY__ = false;
    };
  }, [
    applyLabelMode,
    applyScopeVisuals,
    dismissSelection,
    fitVisibleGraph,
    payload,
    ready,
    selectNode,
    setAreaVisualState,
    setGlobalReady,
    syncExpandedAreas,
    updateCardPosition,
  ]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (commandOpen) {
          setCommandOpen(false);
          return;
        }
        if (selectedNode) {
          dismissSelection();
          return;
        }
        if (scope.kind !== 'all') {
          applyScope(SCOPE_ALL);
        }
        return;
      }
      if (event.defaultPrevented) return;
      const target = event.target as HTMLElement | null;
      const isInput = target && (
        target.tagName === 'INPUT'
        || target.tagName === 'TEXTAREA'
        || target.isContentEditable
      );
      if (isInput) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setCommandDraft(scope.raw);
        setCommandOpen(true);
        return;
      }
      if (event.key === '/') {
        event.preventDefault();
        setCommandDraft(scope.raw);
        setCommandOpen(true);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [applyScope, commandOpen, dismissSelection, scope, selectedNode]);

  const onCommandSubmit = (event: FormEvent) => {
    event.preventDefault();
    const next = parseScope(commandDraft);
    applyScope(next);
    setCommandOpen(false);
  };

  const onCommandClear = () => {
    setCommandDraft('');
    applyScope(SCOPE_ALL);
    setCommandOpen(false);
  };

  if (loading) {
    return (
      <div className="atlas-page atlas-page--blank">
        <div className="atlas-page__status-hint">opening atlas</div>
      </div>
    );
  }

  if (error || !payload) {
    return (
      <div className="atlas-page atlas-page--blank">
        <div className="atlas-page__status-hint">atlas unavailable</div>
        <div className="atlas-page__status-detail">{error || 'empty payload'}</div>
        <button type="button" className="atlas-page__retry" onClick={() => refresh()}>retry</button>
      </div>
    );
  }

  const freshnessState = payload.metadata.freshness?.graph_freshness_state
    || payload.metadata.graph_freshness_state
    || 'unknown';
  const tone = freshnessTone(freshnessState);
  const refreshedAgoSec = Math.max(0, Math.round((Date.now() - lastRefresh) / 1000));
  const areaMatch = scope.kind === 'area'
    ? payload.areas.find((area) => area.slug.toLowerCase() === scope.value)
    : null;

  return (
    <section className="atlas-page" aria-label="Praxis Atlas">
      <div className="atlas-page__canvas" ref={graphRef} />

      <div className="atlas-hud atlas-hud--tl">
        <button
          type="button"
          className={`atlas-hud__scope atlas-hud__scope--${scope.kind}`}
          onClick={() => {
            setCommandDraft(scope.raw);
            setCommandOpen(true);
          }}
          title="Change scope"
        >
          <span className="atlas-hud__dot" aria-hidden="true" />
          <span>{scopeLabel(scope)}</span>
          {scope.kind !== 'all' && (
            <span
              className="atlas-hud__clear"
              role="button"
              tabIndex={0}
              onClick={(event) => {
                event.stopPropagation();
                applyScope(SCOPE_ALL);
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  applyScope(SCOPE_ALL);
                }
              }}
            >
              x
            </span>
          )}
        </button>
      </div>

      <div
        className={`atlas-hud atlas-hud--tr atlas-hud__freshness atlas-hud__freshness--${tone}`}
        title={freshnessTooltip(payload)}
      >
        <span className="atlas-hud__dot" aria-hidden="true" />
        <span className="atlas-hud__freshness-label">{freshnessState}</span>
      </div>

      <div className="atlas-hud atlas-hud--br">
        <span className="atlas-hud__count">{activeCount}</span>
        <span className="atlas-hud__count-label">{scope.kind === 'all' ? 'nodes' : 'in scope'}</span>
        <span className="atlas-hud__sep">/</span>
        <span className="atlas-hud__hot">{hotCount} live</span>
      </div>

      <div className="atlas-hud atlas-hud--bl">
        <kbd>/</kbd>
        <span>scope</span>
        <span className="atlas-hud__sep">/</span>
        <span className="atlas-hud__ago">{refreshedAgoSec}s ago</span>
      </div>

      {payload.warnings.length > 0 && (
        <div className="atlas-banner">{payload.warnings.join(' / ')}</div>
      )}

      {commandOpen && (
        <div
          className="atlas-command"
          onClick={(event) => {
            if (event.target === event.currentTarget) setCommandOpen(false);
          }}
        >
          <form className="atlas-command__box" onSubmit={onCommandSubmit}>
            <input
              autoFocus
              type="text"
              value={commandDraft}
              onChange={(event) => setCommandDraft(event.target.value)}
              placeholder="bug id, area:scheduler, kind:table, recent, stale, orphans"
            />
            <div className="atlas-command__hints">
              <span>enter</span>
              <span>/</span>
              <button type="button" onClick={onCommandClear}>clear</button>
              <span>/</span>
              <span>esc</span>
            </div>
          </form>
        </div>
      )}

      {selectedNode && (
        <AtlasFocusCard
          node={selectedNode}
          neighbors={selectedNeighbors}
          position={cardPosition}
          onNeighborClick={selectNode}
          onDismiss={dismissSelection}
        />
      )}

      {(!ready || renderIssue) && (
        <div className="atlas-page__status-hint atlas-page__status-hint--overlay">
          {renderIssue || 'rendering'}
        </div>
      )}

      {areaMatch && (
        <div className="atlas-area-hint" title={areaMatch.summary}>
          {areaMatch.title} / {areaMatch.member_count} members
        </div>
      )}

      {expandedAreas.size > 0 && scope.kind === 'all' && (
        <div className="atlas-area-hint atlas-area-hint--secondary">
          {expandedAreas.size} area{expandedAreas.size === 1 ? '' : 's'} open
        </div>
      )}
    </section>
  );
}

interface FocusCardProps {
  node: AtlasElementData;
  neighbors: AtlasElementData[];
  position: { x: number; y: number } | null;
  onNeighborClick: (id: string) => void;
  onDismiss: () => void;
}

function AtlasFocusCard({ node, neighbors, position, onNeighborClick, onDismiss }: FocusCardProps) {
  const width = 330;
  const style: CSSProperties = position
    ? {
      left: Math.max(16, Math.min(window.innerWidth - width - 16, position.x + 24)),
      top: Math.max(16, Math.min(window.innerHeight - 240, position.y + 24)),
    }
    : { right: 24, top: 24 };
  const glow = glowFor(node);
  const liveness = node.activity_score == null
    ? 'unknown'
    : node.activity_score >= 0.5
      ? 'live'
      : node.activity_score >= 0.2
        ? 'recent'
        : 'cold';

  return (
    <div className="atlas-card" style={style} role="dialog" aria-label={node.label || node.id}>
      <div className="atlas-card__head" style={{ borderColor: glow }}>
        <div className="atlas-card__title">{node.label || node.id}</div>
        <button type="button" className="atlas-card__close" onClick={onDismiss} aria-label="close">x</button>
      </div>
      <div className="atlas-card__meta">
        {node.area && <span>{node.area}</span>}
        <span>{nodeKindLabel(node)}</span>
        <span className="atlas-card__liveness" title={node.updated_at || 'unknown'}>{liveness}</span>
      </div>
      <div className="atlas-card__id">{node.id}</div>
      {(node.definition_summary || node.preview) && (
        <div className="atlas-card__body">{node.definition_summary || node.preview}</div>
      )}
      <div className="atlas-card__authority">{nodeAuthorityLabel(node)}</div>
      {neighbors.length > 0 && (
        <div className="atlas-card__neighbors">
          {neighbors.slice(0, 12).map((neighbor) => (
            <button key={neighbor.id} type="button" onClick={() => onNeighborClick(neighbor.id)} title={neighbor.id}>
              {neighbor.label || neighbor.id}
            </button>
          ))}
          {neighbors.length > 12 && <span className="atlas-card__more">+{neighbors.length - 12} more</span>}
        </div>
      )}
    </div>
  );
}
