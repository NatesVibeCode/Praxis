import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cytoscape, {
  type Core,
  type EdgeSingular,
  type ElementDefinition,
  type NodeSingular,
} from 'cytoscape';
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
  is_area?: boolean;
  is_aggregate?: boolean;
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

type LabelMode = 'auto' | 'always' | 'off';
type AtlasViewMode = 'graph' | 'table';

interface AtlasTableRow {
  edge: AtlasElementData;
  source: AtlasElementData;
  target: AtlasElementData;
}

const ATLAS_EASING = 'ease-in-out-cubic';
const ATLAS_GRAPH_TIMEOUT_MS = 15_000;
const ATLAS_RENDER_READY_TIMEOUT_MS = 4_000;

const RELATION_STYLES: Record<string, { color: string; style: 'solid' | 'dashed' | 'dotted' }> = {
  depends_on: { color: '#7aa2f7', style: 'solid' },
  parent_of: { color: '#bb9af7', style: 'solid' },
  derived_from: { color: '#9ece6a', style: 'dashed' },
  implements_build: { color: '#e0af68', style: 'solid' },
  resolves_bug: { color: '#f7768e', style: 'dotted' },
};

function freshnessLabel(state: GraphFreshnessState | undefined) {
  if (state === 'fresh') return 'Fresh';
  if (state === 'projection_lagging') return 'Projection lagging';
  return 'Freshness unknown';
}

function freshnessClassName(state: GraphFreshnessState | undefined) {
  if (state === 'fresh' || state === 'projection_lagging') return state;
  return 'unknown';
}

function formatAtlasTimestamp(value?: string | null) {
  if (!value) return 'unavailable';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  });
}

function freshnessTitle(metadata: AtlasMetadata) {
  const freshness = metadata.freshness || metadata;
  const lag = freshness.authority_projection_lag_seconds;
  const parts = [
    `Generated: ${formatAtlasTimestamp(metadata.generated_at)}`,
    `Memory entities: ${formatAtlasTimestamp(freshness.memory_entities_max_updated_at)}`,
    `Memory edges: ${formatAtlasTimestamp(freshness.memory_edges_max_updated_at)}`,
    `Projection run: ${formatAtlasTimestamp(freshness.authority_projection_last_run_at)}`,
    `Authority source: ${formatAtlasTimestamp(freshness.authority_projection_source_max_updated_at)}`,
    `Projection edges: ${freshness.authority_projection_edge_count ?? 0}`,
  ];
  if (typeof lag === 'number') parts.push(`Projection lag: ${Math.round(lag)}s`);
  if (freshness.freshness_error) parts.push(`Freshness error: ${freshness.freshness_error}`);
  return parts.join('\n');
}

let fcoseRegistered = false;

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

function seedCirclePositions(nodes: cytoscape.NodeCollection, radiusBase: number, spread: number) {
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

function memberClusterRadius(count: number): number {
  if (count <= 1) return 54;
  return Math.min(330, Math.max(82, 38 + 30 * Math.sqrt(count)));
}

function areaBackdropSize(memberCount: number, collapsedSize: number): number {
  const radius = memberClusterRadius(memberCount);
  return Math.max(collapsedSize, Math.min(520, radius * 2 + 78));
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

function areaPositions(cy: Core) {
  return cy.nodes('[?is_area]').map((node) => ({
    id: node.id(),
    x: Math.round(node.position('x')),
    y: Math.round(node.position('y')),
  })).sort((a, b) => a.id.localeCompare(b.id));
}

function toElementDefinitions(payload: AtlasPayload): ElementDefinition[] {
  return [
    ...payload.nodes.map((node) => ({
      data: node.data,
      classes: node.classes,
    })),
    ...payload.edges.map((edge) => ({
      data: edge.data,
      classes: edge.classes,
    })),
  ] as ElementDefinition[];
}

function atlasNodeMatchesFilters(
  data: AtlasElementData,
  areas: Set<string>,
  types: Set<string>,
  query: string,
) {
  if (data.is_area) return false;
  const areaOk = areas.has(data.area || '(unowned)');
  const typeOk = types.has(data.type || 'unknown');
  const haystack = [
    data.label,
    data.id,
    data.type,
    data.area,
    data.source,
    data.authority_source,
    data.object_kind,
    data.definition_summary,
    data.route_ref,
  ].filter(Boolean).join(' ').toLowerCase();
  const searchOk = !query || haystack.includes(query);
  return areaOk && typeOk && searchOk;
}

function nodeAuthorityLabel(data: AtlasElementData) {
  return data.authority_source || data.source || 'unknown';
}

function nodeKindLabel(data: AtlasElementData) {
  return data.object_kind || data.category || data.type || 'unknown';
}

function useAtlasGraph() {
  const [payload, setPayload] = useState<AtlasPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
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
    } catch (err) {
      setPayload(null);
      if (err instanceof Error && err.name === 'AbortError') {
        setError(`Atlas graph request timed out after ${Math.ceil(ATLAS_GRAPH_TIMEOUT_MS / 1000)}s.`);
      } else {
        setError(err instanceof Error ? err.message : 'Atlas graph is unavailable.');
      }
    } finally {
      window.clearTimeout(timeout);
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { payload, loading, error, refresh };
}

function setGlobalAtlasDebug(cy: Core, ready: boolean) {
  window.__PRAXIS_ATLAS_READY__ = ready;
  window.__PRAXIS_ATLAS_AREA_POSITIONS__ = ready ? areaPositions(cy) : [];
}

declare global {
  interface Window {
    __PRAXIS_ATLAS_READY__?: boolean;
    __PRAXIS_ATLAS_AREA_POSITIONS__?: Array<{ id: string; x: number; y: number }>;
  }
}

export function AtlasPage() {
  const { payload, loading, error, refresh } = useAtlasGraph();
  const graphRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const expandedAreasRef = useRef<Set<string>>(new Set());
  const filtersRef = useRef({ areas: new Set<string>(), types: new Set<string>(), relations: new Set<string>() });
  const searchRef = useRef('');
  const labelModeRef = useRef<LabelMode>('auto');

  const [expandedAreas, setExpandedAreas] = useState<Set<string>>(new Set());
  const [activeAreas, setActiveAreas] = useState<Set<string>>(new Set());
  const [activeTypes, setActiveTypes] = useState<Set<string>>(new Set());
  const [activeRelations, setActiveRelations] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState('');
  const [labelMode, setLabelMode] = useState<LabelMode>('auto');
  const [viewMode, setViewMode] = useState<AtlasViewMode>('graph');
  const [selectedNode, setSelectedNode] = useState<AtlasElementData | null>(null);
  const [selectedNeighbors, setSelectedNeighbors] = useState<AtlasElementData[]>([]);
  const [renderIssue, setRenderIssue] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  const nodeById = useMemo(() => {
    const index = new Map<string, AtlasElementData>();
    payload?.nodes.forEach((node) => {
      index.set(node.data.id, node.data);
    });
    return index;
  }, [payload]);

  const areaCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    payload?.nodes.forEach((node) => {
      const area = node.data.area || '(unowned)';
      counts[area] = (counts[area] || 0) + 1;
    });
    return counts;
  }, [payload]);

  const typeCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    payload?.nodes.forEach((node) => {
      const type = node.data.type || 'unknown';
      counts[type] = (counts[type] || 0) + 1;
    });
    return counts;
  }, [payload]);

  const relationCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    payload?.edges.forEach((edge) => {
      if (edge.data.is_aggregate) return;
      const relation = edge.data.label;
      if (!relation) return;
      counts[relation] = (counts[relation] || 0) + 1;
    });
    return counts;
  }, [payload]);

  const tableRows = useMemo<AtlasTableRow[]>(() => {
    if (!payload) return [];
    const query = search.toLowerCase().trim();
    return payload.edges
      .filter((edge) => !edge.data.is_aggregate)
      .map((edge) => {
        const source = nodeById.get(edge.data.source || '');
        const target = nodeById.get(edge.data.target || '');
        if (!source || !target) return null;
        const relation = edge.data.label || '';
        if (relation && !activeRelations.has(relation)) return null;
        const sourceInScope = atlasNodeMatchesFilters(source, activeAreas, activeTypes, '');
        const targetInScope = atlasNodeMatchesFilters(target, activeAreas, activeTypes, '');
        if (!sourceInScope && !targetInScope) return null;
        const sourceMatches = atlasNodeMatchesFilters(source, activeAreas, activeTypes, query);
        const targetMatches = atlasNodeMatchesFilters(target, activeAreas, activeTypes, query);
        const edgeMatches = !query || [
          relation,
          edge.data.id,
          edge.data.authority_source,
          edge.data.relation_source,
        ].filter(Boolean).join(' ').toLowerCase().includes(query);
        if (!sourceMatches && !targetMatches && !edgeMatches) return null;
        return { edge: edge.data, source, target };
      })
      .filter((row): row is AtlasTableRow => Boolean(row))
      .sort((a, b) => {
        const sourceDelta = (a.source.label || a.source.id).localeCompare(b.source.label || b.source.id);
        if (sourceDelta !== 0) return sourceDelta;
        return (a.target.label || a.target.id).localeCompare(b.target.label || b.target.id);
      });
  }, [activeAreas, activeRelations, activeTypes, nodeById, payload, search]);

  const areaColor = useCallback((area: string) => {
    return payload?.areas.find((item) => item.slug === area)?.color || '#4a5068';
  }, [payload]);

  const selectNodeById = useCallback((nodeId: string, zoom = 1.55) => {
    const data = nodeById.get(nodeId);
    if (!data || data.is_area) return;
    const neighbors = payload?.edges
      .filter((edge) => !edge.data.is_aggregate && (edge.data.source === nodeId || edge.data.target === nodeId))
      .map((edge) => nodeById.get(edge.data.source === nodeId ? edge.data.target || '' : edge.data.source || ''))
      .filter((neighbor): neighbor is AtlasElementData => Boolean(neighbor && !neighbor.is_area)) || [];
    const cy = cyRef.current;
    if (cy) {
      const node = cy.getElementById(nodeId);
      if (node.length) {
        cy.$(':selected').unselect();
        node.select();
        cy.animate({ center: { eles: node }, zoom }, { duration: 300 });
      }
    }
    setSelectedNode(data);
    setSelectedNeighbors(neighbors);
  }, [nodeById, payload]);

  const fitVisibleGraph = useCallback((padding = 70) => {
    const cy = cyRef.current;
    if (!cy) return;
    const visible = cy.nodes().filter((node) => node.style('display') !== 'none');
    const target = visible.length ? visible : cy.nodes('[?is_area]');
    if (target.length) cy.fit(target, padding);
  }, []);

  const applyLabelMode = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const fallback = cy.nodes('[?is_area]').length === 0;
    const show = labelModeRef.current === 'always'
      || (labelModeRef.current === 'auto' && (fallback || cy.zoom() > 1.3));
    cy.nodes().not('[?is_area]').not('.hover').style('text-opacity', show ? 1 : 0);
  }, []);

  const setAreaVisualState = useCallback((areaNode: NodeSingular, expanded: boolean, animate: boolean) => {
    const collapsedSize = Number(areaNode.data('collapsed_size') || areaNode.data('size') || 92);
    const memberCount = Number(areaNode.data('degree') || 1);
    const targetSize = expanded ? areaBackdropSize(memberCount, collapsedSize) : collapsedSize;
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
    const options: cytoscape.LayoutOptions = {
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
    };
    members.layout(options).run();
  }, []);

  const applyFilters = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const { areas, types, relations } = filtersRef.current;
    const expanded = expandedAreasRef.current;
    const query = searchRef.current.toLowerCase().trim();
    const overviewMode = cy.nodes('[?is_area]').length > 0;

    cy.nodes().forEach((node) => {
      const data = node.data() as AtlasElementData;
      if (data.is_area) {
        node.style('display', areas.has(data.area || '') ? 'element' : 'none');
        return;
      }
      const areaOk = areas.has(data.area || '(unowned)');
      const typeOk = types.has(data.type || 'unknown');
      const searchMatch = Boolean(query)
        && `${data.label || ''} ${data.id || ''}`.toLowerCase().includes(query);
      const areaExpanded = !overviewMode || (data.area && expanded.has(data.area));
      const visible = areaOk && typeOk && (overviewMode ? (areaExpanded || searchMatch) : (searchMatch || !query));
      node.toggleClass('expanded-member', Boolean(visible && overviewMode && areaExpanded));
      node.style('display', visible ? 'element' : 'none');
    });

    cy.edges().forEach((edge) => {
      const data = edge.data() as AtlasElementData;
      const sourceVisible = edge.source().style('display') !== 'none';
      const targetVisible = edge.target().style('display') !== 'none';
      if (!sourceVisible || !targetVisible) {
        edge.style('display', 'none');
        return;
      }
      const relationOk = !data.label || relations.has(data.label);
      if (!relationOk) {
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
  }, [applyLabelMode]);

  const syncExpandedAreas = useCallback((next: Set<string>) => {
    expandedAreasRef.current = next;
    const cy = cyRef.current;
    if (cy) {
      cy.nodes('[?is_area]').forEach((node) => {
        const area = node.data('area');
        setAreaVisualState(node, next.has(area), true);
      });
    }
    setExpandedAreas(new Set(next));
    applyFilters();
    next.forEach((area) => layoutArea(area));
    window.setTimeout(() => fitVisibleGraph(82), 480);
  }, [applyFilters, fitVisibleGraph, layoutArea, setAreaVisualState]);

  const layoutOverview = useCallback((animate: boolean) => {
    const cy = cyRef.current;
    if (!cy) return;
    const areaNodes = cy.nodes('[?is_area]');
    seedCirclePositions(areaNodes, 130, 580);
    const layout = areaNodes.union(cy.edges('[?is_aggregate]')).layout({
      name: 'fcose',
      animate,
      animationDuration: animate ? 620 : 0,
      animationEasing: ATLAS_EASING,
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
    } as cytoscape.LayoutOptions);
    layout.one('layoutstop', () => {
      cy.nodes('[?is_area]').forEach((areaNode) => {
        setAreaVisualState(areaNode, expandedAreasRef.current.has(areaNode.data('area')), animate);
        const position = areaNode.position();
        cy.nodes().filter((node) => !node.data('is_area') && node.data('area') === areaNode.data('area'))
          .forEach((member) => {
            member.position(position);
          });
      });
      expandedAreasRef.current.forEach((area) => layoutArea(area));
      applyFilters();
      cy.resize();
      fitVisibleGraph(80);
      applyLabelMode();
      setReady(true);
      setGlobalAtlasDebug(cy, true);
    });
    layout.run();
  }, [applyFilters, applyLabelMode, fitVisibleGraph, layoutArea, setAreaVisualState]);

  useEffect(() => {
    if (!payload) return;
    setActiveAreas(new Set(Object.keys(areaCounts)));
    setActiveTypes(new Set(Object.keys(typeCounts)));
    setActiveRelations(new Set(Object.keys(relationCounts)));
    setExpandedAreas(new Set());
    setSelectedNode(null);
    setSelectedNeighbors([]);
  }, [areaCounts, payload, relationCounts, typeCounts]);

  useEffect(() => {
    filtersRef.current = { areas: activeAreas, types: activeTypes, relations: activeRelations };
    applyFilters();
  }, [activeAreas, activeRelations, activeTypes, applyFilters]);

  useEffect(() => {
    searchRef.current = search;
    applyFilters();
  }, [applyFilters, search]);

  useEffect(() => {
    labelModeRef.current = labelMode;
    applyLabelMode();
  }, [applyLabelMode, labelMode]);

  useEffect(() => {
    expandedAreasRef.current = expandedAreas;
  }, [expandedAreas]);

  useEffect(() => {
    if (!payload || !graphRef.current) return undefined;
    ensureFcoseRegistered();
    setReady(false);
    setRenderIssue(null);
    filtersRef.current = {
      areas: new Set(Object.keys(areaCounts)),
      types: new Set(Object.keys(typeCounts)),
      relations: new Set(Object.keys(relationCounts)),
    };
    expandedAreasRef.current = new Set();
    searchRef.current = '';
    labelModeRef.current = 'auto';

    const cy = cytoscape({
      container: graphRef.current,
      elements: toElementDefinitions(payload),
      style: [
        {
          selector: 'node',
          style: {
            'background-color': 'data(color)',
            'background-opacity': 0.9,
            label: 'data(label)',
            color: '#f3efe6',
            'font-size': '10px',
            'font-family': 'ui-monospace, SFMono-Regular, Menlo, monospace',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'text-margin-y': 5,
            width: 'data(size)',
            height: 'data(size)',
            'border-width': 1,
            'border-color': 'rgba(255, 255, 255, 0.22)',
            'border-opacity': 0.35,
            'text-outline-width': 3,
            'text-outline-color': '#090a0d',
            'text-outline-opacity': 1,
            'min-zoomed-font-size': 8,
            'text-opacity': 0,
            'transition-property': 'opacity, background-opacity, border-opacity, width, height',
            'transition-duration': 180,
          },
        },
        {
          selector: 'node[!is_area]',
          style: {
            width: 'mapData(size, 6, 32, 8, 35)',
            height: 'mapData(size, 6, 32, 8, 35)',
            'z-index': 4,
          },
        },
        {
          selector: 'node.expanded-member',
          style: {
            'background-opacity': 0.96,
            'border-opacity': 0.55,
          },
        },
        {
          selector: 'node[?is_area]',
          style: {
            'background-color': 'data(color)',
            'background-opacity': 0.1,
            'border-width': 2,
            'border-color': 'data(color)',
            'border-opacity': 0.62,
            shape: 'ellipse',
            label: 'data(label)',
            'font-size': '16px',
            'font-weight': 700,
            color: 'data(color)',
            'text-valign': 'center',
            'text-halign': 'center',
            'text-opacity': 1,
            'text-outline-width': 5,
            'text-outline-color': '#090a0d',
            'min-zoomed-font-size': 0,
            padding: '30px',
            'text-background-color': '#090a0d',
            'text-background-opacity': 0.62,
            'text-background-padding': '5px',
            'text-background-shape': 'roundrectangle',
            'z-index': 1,
          },
        },
        {
          selector: 'node[?is_area].expanded',
          style: {
            'background-opacity': 0.035,
            'border-opacity': 0.34,
            'border-style': 'dashed',
            'text-valign': 'top',
            'text-margin-y': 12,
            'font-size': '14px',
            'z-index': 0,
          },
        },
        {
          selector: 'node:selected',
          style: {
            'border-width': 2,
            'border-color': '#f7768e',
            'text-opacity': 1,
          },
        },
        {
          selector: 'node.hover',
          style: {
            'text-opacity': 1,
            'z-index': 10,
          },
        },
        {
          selector: 'edge',
          style: {
            width: 1.2,
            'line-color': '#8f98ad',
            'curve-style': 'bezier',
            'control-point-step-size': 26,
            opacity: 0.6,
            'target-arrow-shape': 'none',
            'line-cap': 'round',
            'transition-property': 'opacity, width, line-color',
            'transition-duration': 160,
          },
        },
        {
          selector: 'edge[label = "depends_on"]',
          style: {
            'line-color': '#7aa2f7',
            width: 1.1,
            'target-arrow-shape': 'triangle-backcurve',
            'target-arrow-color': '#7aa2f7',
            'arrow-scale': 0.65,
          },
        },
        {
          selector: 'edge[label = "derived_from"]',
          style: {
            'line-color': '#9ece6a',
            'line-style': 'dashed',
            width: 1,
            'target-arrow-shape': 'triangle-backcurve',
            'target-arrow-color': '#9ece6a',
            'arrow-scale': 0.6,
          },
        },
        {
          selector: 'edge[?is_aggregate]',
          style: {
            width: 'mapData(weight, 1, 40, 1.6, 6.5)',
            opacity: 0.7,
            'curve-style': 'bezier',
            'control-point-step-size': 64,
            'target-arrow-shape': 'triangle-backcurve',
            'arrow-scale': 0.8,
            'z-index': 2,
          },
        },
        {
          selector: 'edge.highlight',
          style: {
            opacity: 1,
            width: 2.2,
            'z-index': 5,
          },
        },
        { selector: '.faded', style: { opacity: 0.08 } },
        { selector: 'edge.faded', style: { opacity: 0.04 } },
      ],
      layout: { name: 'preset' },
      wheelSensitivity: 0.25,
      minZoom: 0.1,
      maxZoom: 4,
    });

    cyRef.current = cy;
    cy.nodes('[?is_area]').forEach((node) => {
      node.data('collapsed_size', Number(node.data('size') || 92));
      setAreaVisualState(node, false, false);
    });
    setGlobalAtlasDebug(cy, false);

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
        const next = new Set(expandedAreasRef.current);
        if (next.has(area)) next.delete(area);
        else next.add(area);
        cy.elements().removeClass('faded');
        cy.edges().removeClass('highlight');
        cy.nodes().removeClass('hover');
        syncExpandedAreas(next);
        return;
      }
      cy.$(':selected').unselect();
      node.select();
      const neighbors = node.neighborhood('node').filter((neighbor) => !neighbor.data('is_area'))
        .map((neighbor) => neighbor.data() as AtlasElementData);
      setSelectedNode(data);
      setSelectedNeighbors(neighbors);
    });
    cy.on('tap', (event) => {
      if (event.target === cy) {
        cy.$(':selected').unselect();
        setSelectedNode(null);
        setSelectedNeighbors([]);
      }
    });

    const readyTimeout = window.setTimeout(() => {
      if (cyRef.current !== cy || ready) return;
      applyFilters();
      cy.resize();
      fitVisibleGraph(80);
      applyLabelMode();
      setReady(true);
      setGlobalAtlasDebug(cy, true);
    }, ATLAS_RENDER_READY_TIMEOUT_MS);
    cy.one('layoutstop', () => window.clearTimeout(readyTimeout));

    try {
      layoutOverview(false);
    } catch (err) {
      window.clearTimeout(readyTimeout);
      applyFilters();
      cy.resize();
      fitVisibleGraph(80);
      applyLabelMode();
      setReady(true);
      setGlobalAtlasDebug(cy, true);
      setRenderIssue(err instanceof Error ? err.message : 'Atlas layout failed.');
    }

    const onResize = () => {
      cy.resize();
      fitVisibleGraph(80);
    };
    window.addEventListener('resize', onResize);

    return () => {
      window.clearTimeout(readyTimeout);
      window.removeEventListener('resize', onResize);
      cy.destroy();
      if (cyRef.current === cy) cyRef.current = null;
    };
  }, [
    applyLabelMode,
    areaCounts,
    fitVisibleGraph,
    layoutOverview,
    payload,
    relationCounts,
    syncExpandedAreas,
    typeCounts,
  ]);

  const toggleValue = (current: Set<string>, value: string) => {
    const next = new Set(current);
    if (next.has(value)) next.delete(value);
    else next.add(value);
    return next;
  };

  const relationItems = Object.keys(relationCounts).sort();
  const areaItems = Object.keys(areaCounts).sort();
  const typeItems = Object.keys(typeCounts).sort();
  const modeHint = payload
    ? expandedAreas.size === 0
      ? 'overview'
      : expandedAreas.size === payload.areas.length
        ? `all ${payload.areas.length} areas expanded`
        : `${expandedAreas.size} / ${payload.areas.length} areas expanded`
    : 'loading graph';
  const freshnessState = payload?.metadata.freshness?.graph_freshness_state
    || payload?.metadata.graph_freshness_state
    || 'unknown';
  const freshnessClass = freshnessClassName(freshnessState);

  if (loading) {
    return (
      <div className="app-shell__fallback">
        <div className="app-shell__fallback-kicker">Loading</div>
        <div className="app-shell__fallback-title">Opening Atlas...</div>
      </div>
    );
  }

  if (error || !payload) {
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">Atlas unavailable</div>
        <div className="app-shell__fallback-title">Could not load graph authority.</div>
        <p className="app-shell__fallback-copy">{error || 'The Atlas graph payload was empty.'}</p>
        <button type="button" className="app-shell__crash-action" onClick={refresh}>Retry</button>
      </div>
    );
  }

  return (
    <section className="atlas-page" aria-label="Praxis knowledge atlas">
      <header className="atlas-page__header">
        <div className="atlas-page__identity">
          <h1>Praxis System Atlas</h1>
          <div className="atlas-page__meta">
            <span>{payload.metadata.node_count} nodes</span>
            <span>{payload.metadata.edge_count} edges</span>
            <span>{payload.metadata.aggregate_edge_count} inter-area</span>
            <span>{payload.metadata.source_authority}</span>
          </div>
        </div>
        <div
          className={`atlas-page__freshness atlas-page__freshness--${freshnessClass}`}
          title={freshnessTitle(payload.metadata)}
        >
          <span aria-hidden="true" />
          {freshnessLabel(freshnessState)}
        </div>
        <div className="atlas-page__hint">{modeHint}</div>
        <div className="atlas-page__toolbar" aria-label="Atlas graph controls">
          <button
            type="button"
            className={viewMode === 'graph' ? 'atlas-page__toolbar-active' : ''}
            onClick={() => setViewMode('graph')}
          >
            Graph
          </button>
          <button
            type="button"
            className={viewMode === 'table' ? 'atlas-page__toolbar-active' : ''}
            onClick={() => setViewMode('table')}
          >
            Table
          </button>
          <button type="button" onClick={() => syncExpandedAreas(new Set(payload.areas.map((area) => area.slug)))}>
            Expand all
          </button>
          <button type="button" onClick={() => syncExpandedAreas(new Set())}>
            Collapse all
          </button>
          <button type="button" onClick={() => fitVisibleGraph(60)}>
            Fit
          </button>
          <button type="button" onClick={() => layoutOverview(true)}>
            Re-layout
          </button>
          <button
            type="button"
            onClick={() => setLabelMode((current) => (current === 'auto' ? 'always' : current === 'always' ? 'off' : 'auto'))}
          >
            Labels: {labelMode}
          </button>
        </div>
      </header>

      <aside className="atlas-page__sidebar" aria-label="Atlas filters">
        <section className="atlas-filter">
          <h2>Search</h2>
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="name or id..."
          />
        </section>
        <section className="atlas-filter">
          <h2>Relations</h2>
          {relationItems.map((relation) => {
            const style = RELATION_STYLES[relation] || { color: '#8f98ad', style: 'solid' as const };
            return (
              <label key={relation}>
                <input
                  type="checkbox"
                  checked={activeRelations.has(relation)}
                  onChange={() => setActiveRelations((current) => toggleValue(current, relation))}
                />
                <span
                  className={`atlas-edge-swatch atlas-edge-swatch--${style.style}`}
                  style={style.style === 'solid' ? { backgroundColor: style.color } : { color: style.color }}
                />
                <span>{relation} ({relationCounts[relation]})</span>
              </label>
            );
          })}
        </section>
        <section className="atlas-filter">
          <h2>Areas</h2>
          {areaItems.map((area) => (
            <label key={area}>
              <input
                type="checkbox"
                checked={activeAreas.has(area)}
                onChange={() => setActiveAreas((current) => toggleValue(current, area))}
              />
              <span className="atlas-swatch" style={{ backgroundColor: areaColor(area), color: areaColor(area) }} />
              <span>{area} ({areaCounts[area]})</span>
            </label>
          ))}
        </section>
        <section className="atlas-filter">
          <h2>Types</h2>
          {typeItems.map((type) => (
            <label key={type}>
              <input
                type="checkbox"
                checked={activeTypes.has(type)}
                onChange={() => setActiveTypes((current) => toggleValue(current, type))}
              />
              <span>{type} ({typeCounts[type]})</span>
            </label>
          ))}
        </section>
      </aside>

      <main className="atlas-page__workspace" aria-label="Atlas workspace">
        <div
          className={`atlas-page__graph${viewMode === 'graph' ? '' : ' atlas-page__graph--hidden'}`}
          ref={graphRef}
          aria-hidden={viewMode !== 'graph'}
        />
        <div
          className={`atlas-page__table${viewMode === 'table' ? '' : ' atlas-page__table--hidden'}`}
          aria-hidden={viewMode !== 'table'}
        >
          <table>
            <thead>
              <tr>
                <th scope="col">Source</th>
                <th scope="col">Relation</th>
                <th scope="col">Target</th>
                <th scope="col">Kind</th>
                <th scope="col">Authority</th>
              </tr>
            </thead>
            <tbody>
              {tableRows.map((row) => {
                const selected = selectedNode?.id === row.source.id || selectedNode?.id === row.target.id;
                return (
                  <tr
                    key={row.edge.id}
                    className={selected ? 'atlas-page__table-row--selected' : ''}
                    onClick={() => selectNodeById(row.source.id)}
                  >
                    <td>
                      <button type="button" onClick={(event) => {
                        event.stopPropagation();
                        selectNodeById(row.source.id);
                      }}>
                        {row.source.label || row.source.id}
                      </button>
                      <span>{row.source.id}</span>
                    </td>
                    <td>
                      <strong>{row.edge.label || 'related'}</strong>
                      <span>{row.edge.relation_source || row.edge.authority_source || 'canonical'}</span>
                    </td>
                    <td>
                      <button type="button" onClick={(event) => {
                        event.stopPropagation();
                        selectNodeById(row.target.id);
                      }}>
                        {row.target.label || row.target.id}
                      </button>
                      <span>{row.target.id}</span>
                    </td>
                    <td>
                      <strong>{nodeKindLabel(row.source)}</strong>
                      <span>{nodeKindLabel(row.target)}</span>
                    </td>
                    <td>
                      <strong>{nodeAuthorityLabel(row.source)}</strong>
                      <span>{nodeAuthorityLabel(row.target)}</span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {tableRows.length === 0 && (
            <div className="atlas-page__table-empty">No relationships match the current filters.</div>
          )}
        </div>
      </main>

      <aside className="atlas-page__detail" aria-label="Atlas detail">
        {selectedNode ? (
          <>
            <h2>{selectedNode.label || selectedNode.id}</h2>
            <div className="atlas-page__tags">
              {selectedNode.area && <span className="atlas-page__tag atlas-page__tag--area">{selectedNode.area}</span>}
              {selectedNode.type && <span className="atlas-page__tag">{selectedNode.type}</span>}
            </div>
            <div className="atlas-page__source">
              {selectedNode.id}{selectedNode.authority_source || selectedNode.source ? ` | ${nodeAuthorityLabel(selectedNode)}` : ''}
            </div>
            {(selectedNode.object_kind || selectedNode.route_ref || selectedNode.decision_ref) && (
              <dl className="atlas-page__facts">
                {selectedNode.object_kind && (
                  <>
                    <dt>Object</dt>
                    <dd>{selectedNode.object_kind}</dd>
                  </>
                )}
                {selectedNode.route_ref && (
                  <>
                    <dt>Route</dt>
                    <dd>{selectedNode.route_ref}</dd>
                  </>
                )}
                {selectedNode.decision_ref && (
                  <>
                    <dt>Decision</dt>
                    <dd>{selectedNode.decision_ref}</dd>
                  </>
                )}
              </dl>
            )}
            {selectedNode.preview && <pre>{selectedNode.preview}</pre>}
            {selectedNeighbors.length > 0 && (
              <div className="atlas-page__neighbors">
                <h3>Connected ({selectedNeighbors.length})</h3>
                {selectedNeighbors.map((neighbor) => (
                  <button
                    key={neighbor.id}
                    type="button"
                    onClick={() => {
                      const cy = cyRef.current;
                      if (!cy) return;
                      const node = cy.getElementById(neighbor.id);
                      if (!node.length) return;
                      cy.$(':selected').unselect();
                      node.select();
                      cy.animate({ center: { eles: node }, zoom: 1.6 }, { duration: 300 });
                      setSelectedNode(node.data() as AtlasElementData);
                    }}
                  >
                    <span
                      className="atlas-swatch"
                      style={{ backgroundColor: neighbor.color || '#4a5068', color: neighbor.color || '#4a5068' }}
                    />
                    <span>{neighbor.label || neighbor.id}</span>
                  </button>
                ))}
              </div>
            )}
          </>
        ) : (
          <p className="atlas-page__empty">
            No node selected.
          </p>
        )}
        {payload.warnings.length > 0 && (
          <div className="atlas-page__warnings">
            <h3>Warnings</h3>
            {payload.warnings.map((warning) => <p key={warning}>{warning}</p>)}
          </div>
        )}
      </aside>

      {(!ready || renderIssue) && (
        <div className="atlas-page__status" aria-live="polite">
          <strong>{renderIssue ? 'Atlas render check' : 'Rendering Atlas'}</strong>
          <span>{renderIssue || 'Waiting for the graph canvas to report ready.'}</span>
        </div>
      )}
    </section>
  );
}
