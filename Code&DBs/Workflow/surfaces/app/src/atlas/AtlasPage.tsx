import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent } from 'react';
import { AtlasConstellation } from './AtlasConstellation';
import { AtlasContactSheet } from './AtlasContactSheet';
import { AtlasLedger } from './AtlasLedger';
import './AtlasPage.css';

// ── View state ────────────────────────────────────────────────────────
// Three views: constellation (spatial, default), contact (card grid), ledger
// (three-pane reader for drill-down). Click an area in constellation or contact
// to open the ledger focused on that area.

type AtlasView = 'constellation' | 'contact' | 'ledger';

function readViewFromUrl(): AtlasView {
  if (typeof window === 'undefined') return 'constellation';
  const params = new URLSearchParams(window.location.search);
  const v = params.get('view');
  if (v === 'contact') return 'contact';
  if (v === 'ledger') return 'ledger';
  return 'constellation';
}

function syncViewToUrl(view: AtlasView, area?: string | null): void {
  if (typeof window === 'undefined') return;
  const url = new URL(window.location.href);
  if (view === 'constellation') {
    url.searchParams.delete('view');
  } else {
    url.searchParams.set('view', view);
  }
  if (area && view === 'ledger') {
    url.searchParams.set('area', area);
  } else {
    url.searchParams.delete('area');
  }
  window.history.replaceState(window.history.state, '', url.toString());
}

// ── Types ─────────────────────────────────────────────────────────────

export interface AtlasElementData {
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
  display_size?: number;
  signal_activity?: number;
  signal_authority?: number;
  signal_data?: number;
  signal_dependency?: number;
  signal_risk?: number;
  signal_stale?: number;
  semantic_role?: SemanticObjectRole;
  semantic_rank?: number;
  original_id?: string;
  node_kind?: 'area' | 'object' | 'class_label';
}

export interface AtlasElement {
  data: AtlasElementData;
  classes?: string;
}

export interface AtlasArea {
  slug: string;
  title: string;
  summary: string;
  color: string;
  member_count: number;
}

type GraphFreshnessState = 'fresh' | 'projection_lagging' | 'unknown' | string;
type AtlasViewMode = 'overview' | 'area_focus' | 'item_inspect';
export type SemanticObjectRole = 'authority' | 'data' | 'dependency' | 'risk' | 'live' | 'stale';

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

export interface AtlasPayload {
  ok: boolean;
  nodes: AtlasElement[];
  edges: AtlasElement[];
  areas: AtlasArea[];
  metadata: AtlasMetadata;
  warnings: string[];
  error?: string;
  detail?: string;
}

interface AtlasGraphEvent {
  authority_id?: string;
  captured_at?: string;
  changed_paths?: string[];
  event_type?: string;
  node_id?: string;
  run_id?: string;
  source_ids?: string[];
  workflow_id?: string;
}

export interface AreaSignal {
  slug: string;
  title: string;
  summary: string;
  memberCount: number;
  authorityCount: number;
  dataCount: number;
  dependencyCount: number;
  riskCount: number;
  liveCount: number;
  staleCount: number;
  activityScore: number;
  semanticWeight: number;
  displaySize: number;
}

export interface DependencySignal {
  id: string;
  sourceArea: string;
  targetArea: string;
  relation: string;
  weight: number;
  activityScore: number;
}

export interface SemanticObject {
  node: AtlasElementData;
  role: SemanticObjectRole;
  rank: number;
  dependencyCount: number;
}

export interface SemanticModel {
  areaSignals: Map<string, AreaSignal>;
  areas: AreaSignal[];
  dependencies: DependencySignal[];
  nodeById: Map<string, AtlasElementData>;
  rawEdgesByNode: Map<string, AtlasElementData[]>;
  semanticObjectsByArea: Map<string, SemanticObject[]>;
}

// ── Constants ─────────────────────────────────────────────────────────

const ATLAS_GRAPH_TIMEOUT_MS = 15_000;
const ATLAS_GRAPH_STREAM_PATH = '/api/atlas/graph/stream';
const ATLAS_LIVE_MARK_MS = 4200;
const MAX_OVERVIEW_DEPENDENCIES = 34;
const MAX_FOCUS_DEPENDENCIES = 7;
const MAX_FOCUS_OBJECTS = 10;
const MAX_SELECTED_EDGES = 4;
const MIN_ACTIVITY_SCORE = 0.08;

const ROLE_COLORS: Record<SemanticObjectRole, string> = {
  authority: '#f3eee4',
  data: '#f3eee4',
  dependency: '#f3eee4',
  risk: '#f85149',
  live: '#d29922',
  stale: '#9b9488',
};

const AREA_TONES = ['#f3eee4', '#d8d2c5', '#b7b0a3', '#9b9488', '#706b62', '#3a3a3a'];

declare global {
  interface Window {
    __PRAXIS_ATLAS_READY__?: boolean;
    __PRAXIS_ATLAS_VISIBLE_COUNTS__?: { nodes: number; edges: number };
  }
}

// ── Utility functions ─────────────────────────────────────────────────

function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function areaColor(area: string) {
  return AREA_TONES[stableHash(area) % AREA_TONES.length];
}

function objectId(id: string) {
  return `object::${id}`;
}

function areaId(slug: string) {
  return `area::${slug}`;
}

function freshnessTone(state: GraphFreshnessState | undefined) {
  if (state === 'fresh') return 'ok';
  if (state === 'projection_lagging') return 'warn';
  return 'unknown';
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

function haystack(data: AtlasElementData) {
  return [
    data.id,
    data.label,
    data.type,
    data.object_kind,
    data.category,
    data.area,
    data.authority_source,
    data.source,
    data.definition_summary,
    data.preview,
  ].filter(Boolean).join(' ').toLowerCase();
}

function semanticRoleFor(data: AtlasElementData): SemanticObjectRole {
  const text = haystack(data);
  const authority = String(data.authority_source || data.source || '').toLowerCase();
  const type = String(data.type || data.category || '').toLowerCase();
  const objectKind = String(data.object_kind || data.id || '').toLowerCase();
  const activity = data.activity_score ?? MIN_ACTIVITY_SCORE;

  if (text.includes('bug') || text.includes('issue') || text.includes('risk') || text.includes('failure')) {
    return 'risk';
  }

  if (
    objectKind.includes('operator_decision')
    || objectKind.includes('authority_')
    || objectKind.includes('registry')
    || type.includes('decision')
    || authority.includes('operator_decisions')
  ) {
    return 'authority';
  }

  if (
    authority === 'data_dictionary_objects'
    || type === 'table'
    || objectKind.startsWith('table:')
  ) {
    return 'data';
  }

  if (
    type.includes('surface_catalog')
    || type.includes('capability')
    || type.includes('tool')
    || text.includes('connector')
    || text.includes('dependency')
  ) {
    return 'dependency';
  }

  if (activity >= 0.5) return 'live';
  if (activity < 0.18 && Boolean(data.updated_at)) return 'stale';
  return 'data';
}

function roleLabel(role: SemanticObjectRole) {
  if (role === 'authority') return 'authority';
  if (role === 'data') return 'data';
  if (role === 'dependency') return 'dependency';
  if (role === 'risk') return 'risk';
  if (role === 'live') return 'live';
  return 'stale';
}

function objectRank(data: AtlasElementData, dependencyCount: number) {
  const role = semanticRoleFor(data);
  const roleScore: Record<SemanticObjectRole, number> = {
    authority: 460,
    risk: 430,
    data: 330,
    dependency: 290,
    live: 230,
    stale: 120,
  };
  return roleScore[role]
    + dependencyCount * 24
    + Math.round((data.activity_score ?? MIN_ACTIVITY_SCORE) * 130)
    + Math.min(44, Number(data.size || 0));
}

function nodeKindLabel(data: AtlasElementData) {
  return data.object_kind || data.category || data.type || 'object';
}

function semanticDisplayLabel(data: AtlasElementData) {
  const raw = data.label || data.object_kind || data.type || data.id;
  const clean = raw
    .replace(/^table:/i, '')
    .replace(/^memory:/i, '')
    .replace(/[_:/.-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const words = clean.split(' ').filter(Boolean);
  if (words.length <= 3) return clean;
  return words.slice(0, 3).join(' ');
}

function authorityLabel(data: AtlasElementData) {
  return data.authority_source || data.source || 'Praxis.db';
}

function outcomeLine(data: AtlasElementData, dependencyCount: number) {
  const role = semanticRoleFor(data);
  if (role === 'authority') return 'Source-of-truth object for nearby system behavior.';
  if (role === 'risk') return 'Risk or defect object; inspect before trusting this area.';
  if (role === 'dependency') return 'Bridge object; it explains how this area touches another system.';
  if (role === 'live') return `Recently active object with ${dependencyCount} visible dependency${dependencyCount === 1 ? '' : 'ies'}.`;
  if (role === 'stale') return 'Cold object; verify whether it still belongs in this semantic map.';
  return 'Data dictionary object; use it to understand what this area owns.';
}

// ── Payload / event helpers ───────────────────────────────────────────

function atlasPayloadSourceIds(payload: AtlasPayload | null) {
  const ids = new Set<string>();
  if (!payload) return ids;
  payload.nodes.forEach((node) => {
    [node.data.id, node.data.original_id, node.data.area, node.data.source, node.data.target,
      node.data.route_ref, node.data.binding_revision, node.data.decision_ref,
    ].forEach((value) => { if (value) ids.add(value); });
  });
  payload.edges.forEach((edge) => {
    [edge.data.id, edge.data.original_id, edge.data.source, edge.data.target, edge.data.area,
      edge.data.route_ref, edge.data.binding_revision, edge.data.decision_ref,
    ].forEach((value) => { if (value) ids.add(value); });
  });
  payload.areas.forEach((area) => {
    ids.add(area.slug);
    ids.add(areaId(area.slug));
  });
  return ids;
}

function atlasEventSourceIds(event: AtlasGraphEvent) {
  return [
    ...(event.source_ids || []),
    ...(event.changed_paths || []),
    event.workflow_id,
    event.run_id,
    event.node_id,
    event.authority_id,
  ].filter((value): value is string => Boolean(value));
}

function atlasEventHitsPayload(event: AtlasGraphEvent, payload: AtlasPayload | null) {
  const graphIds = atlasPayloadSourceIds(payload);
  return atlasEventSourceIds(event).some((sourceId) => graphIds.has(sourceId));
}

// ── Data hook ─────────────────────────────────────────────────────────

function useAtlasGraph() {
  const [payload, setPayload] = useState<AtlasPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState(Date.now());
  const [changedSourceIds, setChangedSourceIds] = useState<Set<string>>(() => new Set());
  const payloadRef = useRef<AtlasPayload | null>(null);
  const liveMarkTimerRef = useRef<number | null>(null);

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
      payloadRef.current = body;
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
    if (typeof window === 'undefined' || typeof window.EventSource !== 'function') return undefined;
    const source = new window.EventSource(ATLAS_GRAPH_STREAM_PATH);
    source.onmessage = (event) => {
      try {
        const graphEvent = JSON.parse(event.data) as AtlasGraphEvent;
        if (!atlasEventHitsPayload(graphEvent, payloadRef.current)) return;
        const sourceIds = atlasEventSourceIds(graphEvent);
        setChangedSourceIds(new Set(sourceIds));
        void refresh(true);
        if (liveMarkTimerRef.current !== null) {
          window.clearTimeout(liveMarkTimerRef.current);
        }
        liveMarkTimerRef.current = window.setTimeout(() => {
          setChangedSourceIds(new Set());
          liveMarkTimerRef.current = null;
        }, ATLAS_LIVE_MARK_MS);
      } catch {
        // Ignore malformed stream payloads.
      }
    };
    source.onerror = () => {
      // EventSource owns reconnect.
    };
    return () => {
      source.close();
      if (liveMarkTimerRef.current !== null) {
        window.clearTimeout(liveMarkTimerRef.current);
        liveMarkTimerRef.current = null;
      }
    };
  }, [refresh]);

  return { payload, loading, error, refresh, lastRefresh, changedSourceIds };
}

// ── Model builders (exported for tests) ───────────────────────────────

function buildSemanticModel(payload: AtlasPayload): SemanticModel {
  const nodeById = new Map<string, AtlasElementData>();
  const rawEdgesByNode = new Map<string, AtlasElementData[]>();
  const areaSignals = new Map<string, AreaSignal>();
  const dependencyByArea = new Map<string, number>();
  const dependencySignals: DependencySignal[] = [];

  payload.nodes.forEach((node) => {
    nodeById.set(node.data.id, node.data);
  });

  payload.areas.forEach((area) => {
    areaSignals.set(area.slug, {
      slug: area.slug,
      title: area.title,
      summary: area.summary,
      memberCount: area.member_count,
      authorityCount: 0,
      dataCount: 0,
      dependencyCount: 0,
      riskCount: 0,
      liveCount: 0,
      staleCount: 0,
      activityScore: MIN_ACTIVITY_SCORE,
      semanticWeight: Math.max(1, area.member_count),
      displaySize: 88,
    });
  });

  payload.edges.forEach((edge) => {
    const data = edge.data;
    if (!data.source || !data.target) return;

    if (!data.is_aggregate) {
      rawEdgesByNode.set(data.source, [...(rawEdgesByNode.get(data.source) || []), data]);
      rawEdgesByNode.set(data.target, [...(rawEdgesByNode.get(data.target) || []), data]);
      return;
    }

    const sourceArea = nodeById.get(data.source)?.area || '';
    const targetArea = nodeById.get(data.target)?.area || '';
    if (!sourceArea || !targetArea || sourceArea === targetArea) return;
    const weight = Number(data.weight || 1);
    dependencySignals.push({
      id: data.id,
      sourceArea,
      targetArea,
      relation: data.label || 'depends_on',
      weight,
      activityScore: data.activity_score ?? MIN_ACTIVITY_SCORE,
    });
    dependencyByArea.set(sourceArea, (dependencyByArea.get(sourceArea) || 0) + weight);
    dependencyByArea.set(targetArea, (dependencyByArea.get(targetArea) || 0) + weight);
  });

  const semanticObjectsByArea = new Map<string, SemanticObject[]>();
  payload.nodes.forEach((node) => {
    const data = node.data;
    if (data.is_area || !data.area) return;
    const area = areaSignals.get(data.area);
    if (!area) return;

    const dependencyCount = rawEdgesByNode.get(data.id)?.length || Number(data.degree || 0) || 0;
    const role = semanticRoleFor(data);
    const semanticObject: SemanticObject = {
      node: data,
      role,
      rank: objectRank(data, dependencyCount),
      dependencyCount,
    };

    semanticObjectsByArea.set(data.area, [...(semanticObjectsByArea.get(data.area) || []), semanticObject]);
    area.activityScore = Math.max(area.activityScore, data.activity_score ?? MIN_ACTIVITY_SCORE);
    if (role === 'authority') area.authorityCount += 1;
    if (role === 'data') area.dataCount += 1;
    if (role === 'dependency') area.dependencyCount += 1;
    if (role === 'risk') area.riskCount += 1;
    if ((data.activity_score ?? MIN_ACTIVITY_SCORE) >= 0.5) area.liveCount += 1;
    if ((data.activity_score ?? MIN_ACTIVITY_SCORE) < 0.18 && Boolean(data.updated_at)) area.staleCount += 1;
  });

  areaSignals.forEach((area) => {
    area.dependencyCount += Math.round(dependencyByArea.get(area.slug) || 0);
    area.semanticWeight = area.memberCount * 0.35
      + area.authorityCount * 2.2
      + area.dataCount * 1.4
      + area.dependencyCount * 1.1
      + area.riskCount * 3.2
      + area.liveCount * 1.2;
    area.displaySize = clamp(66 + Math.sqrt(Math.max(1, area.semanticWeight)) * 9, 72, 166);
  });

  semanticObjectsByArea.forEach((objects, area) => {
    semanticObjectsByArea.set(area, objects.sort((a, b) => b.rank - a.rank));
  });

  dependencySignals.sort((a, b) => b.weight - a.weight || b.activityScore - a.activityScore);

  return {
    areaSignals,
    areas: [...areaSignals.values()].sort((a, b) => b.semanticWeight - a.semanticWeight),
    dependencies: dependencySignals,
    nodeById,
    rawEdgesByNode,
    semanticObjectsByArea,
  };
}

// buildElements is kept for backward compatibility with tests.
// The new D2/D4/D5 views consume SemanticModel directly.
function areaPosition(area: AreaSignal, index: number, total: number) {
  if (index === 0) return { x: 0, y: 0 };
  const innerCount = Math.min(8, Math.max(1, total - 1));
  const outerIndex = index - 1;
  const inner = outerIndex < innerCount;
  const ringIndex = inner ? outerIndex : outerIndex - innerCount;
  const count = inner ? innerCount : Math.max(1, total - 1 - innerCount);
  const radius = inner ? 315 : 540;
  const angle = -Math.PI / 2 + (ringIndex / count) * Math.PI * 2 + (stableHash(area.slug) % 17) * 0.01;
  return {
    x: Math.cos(angle) * radius * (inner ? 1.08 : 1.16),
    y: Math.sin(angle) * radius * (inner ? 0.78 : 0.86),
  };
}

function outerAreaPosition(area: AreaSignal, focusArea: string, index: number, total: number) {
  const angle = -Math.PI / 2 + (index / Math.max(1, total)) * Math.PI * 2 + (stableHash(`${focusArea}:${area.slug}`) % 23) * 0.008;
  return {
    x: Math.cos(angle) * 520,
    y: Math.sin(angle) * 345,
  };
}

function classRole(role: SemanticObjectRole): 'authority' | 'data' | 'dependency' | 'risk' {
  if (role === 'authority') return 'authority';
  if (role === 'dependency') return 'dependency';
  if (role === 'risk' || role === 'stale') return 'risk';
  return 'data';
}

function objectPosition(object: SemanticObject, indexInClass: number, classCount: number) {
  const centers: Record<ReturnType<typeof classRole>, { x: number; y: number }> = {
    authority: { x: -280, y: -220 },
    data: { x: 260, y: -220 },
    dependency: { x: -280, y: 220 },
    risk: { x: 260, y: 220 },
  };
  const role = classRole(object.role);
  const center = centers[role];
  const columns = classCount <= 4 ? classCount : 4;
  const col = indexInClass % Math.max(1, columns);
  const row = Math.floor(indexInClass / Math.max(1, columns));
  const xOffset = (col - (columns - 1) / 2) * 110;
  const yOffset = row * 90;
  return {
    x: center.x + xOffset,
    y: center.y + yOffset,
  };
}

function inspectNeighborPosition(index: number) {
  const positions = [
    { x: 180, y: -180 },
    { x: 280, y: -60 },
    { x: 280, y: 80 },
    { x: 180, y: 200 },
  ];
  return positions[index] || { x: 260, y: -120 + index * 90 };
}

function selectFocusObjects(model: SemanticModel, areaSlug: string) {
  const objects = model.semanticObjectsByArea.get(areaSlug) || [];
  const selected: SemanticObject[] = [];
  const byRole = new Map<SemanticObjectRole, SemanticObject[]>();

  objects.forEach((object) => {
    byRole.set(object.role, [...(byRole.get(object.role) || []), object]);
  });

  (['authority', 'data', 'dependency', 'risk', 'live', 'stale'] as SemanticObjectRole[]).forEach((role) => {
    const cap = role === 'data' ? 4 : role === 'authority' ? 2 : 2;
    selected.push(...(byRole.get(role) || []).slice(0, cap));
  });

  const seen = new Set(selected.map((object) => object.node.id));
  objects.forEach((object) => {
    if (selected.length >= MAX_FOCUS_OBJECTS) return;
    if (!seen.has(object.node.id)) {
      selected.push(object);
      seen.add(object.node.id);
    }
  });

  return selected.slice(0, MAX_FOCUS_OBJECTS);
}

function selectedEdges(model: SemanticModel, selectedId: string | null) {
  if (!selectedId) return [];
  return [...(model.rawEdgesByNode.get(selectedId) || [])]
    .map((edge) => {
      const neighborId = edge.source === selectedId ? edge.target : edge.source;
      const neighbor = neighborId ? model.nodeById.get(neighborId) : undefined;
      const dependencyCount = neighbor ? model.rawEdgesByNode.get(neighbor.id)?.length || Number(neighbor.degree || 0) || 0 : 0;
      const rank = neighbor ? objectRank(neighbor, dependencyCount) : 0;
      return { edge, neighbor, rank };
    })
    .filter((item): item is { edge: AtlasElementData; neighbor: AtlasElementData; rank: number } => Boolean(item.neighbor && !item.neighbor.is_area))
    .sort((a, b) => b.rank - a.rank)
    .slice(0, MAX_SELECTED_EDGES);
}

interface BuildElement {
  data: Record<string, unknown>;
  position?: { x: number; y: number };
  classes?: string;
}

function buildElements(
  model: SemanticModel,
  mode: AtlasViewMode,
  focusArea: string | null,
  selectedId: string | null,
  changedSourceIds: Set<string> = new Set(),
): BuildElement[] {
  if (mode === 'overview' || !focusArea) {
    const areaIndex = new Map(model.areas.map((area, index) => [area.slug, index]));
    const total = model.areas.length;
    const nodes = model.areas.map((area) => ({
      data: {
        id: areaId(area.slug),
        label: area.title,
        area: area.slug,
        node_kind: 'area',
        display_size: area.displaySize,
        display_color: areaColor(area.slug),
        signal_activity: area.activityScore,
        signal_authority: area.authorityCount,
        signal_data: area.dataCount,
        signal_dependency: area.dependencyCount,
        signal_risk: area.riskCount,
        signal_stale: area.staleCount,
      },
      position: areaPosition(area, areaIndex.get(area.slug) || 0, total),
      classes: `area-node area-overview ${changedSourceIds.has(area.slug) || changedSourceIds.has(areaId(area.slug)) ? 'atlas-live-changed' : ''}`,
    }));

    const edges = model.dependencies.slice(0, MAX_OVERVIEW_DEPENDENCIES).map((dep) => ({
      data: {
        id: `overview::${dep.id}`,
        source: areaId(dep.sourceArea),
        target: areaId(dep.targetArea),
        label: dep.relation,
        display_color: areaColor(dep.sourceArea),
        weight: dep.weight,
        signal_activity: dep.activityScore,
      },
      classes: 'overview-dependency',
    }));

    return [...nodes, ...edges];
  }

  const focusSignal = model.areaSignals.get(focusArea);
  const focusDeps = model.dependencies
    .filter((dep) => dep.sourceArea === focusArea || dep.targetArea === focusArea)
    .slice(0, MAX_FOCUS_DEPENDENCIES);
  const outerAreas = model.areas.filter((area) => area.slug !== focusArea);
  const inspecting = mode === 'item_inspect' && Boolean(selectedId);
  const focusObjects = inspecting ? [] : selectFocusObjects(model, focusArea);
  const selectedEdgeItems = selectedEdges(model, selectedId);
  const objectMap = new Map<string, SemanticObject>();
  const objectPositions = new Map<string, { x: number; y: number }>();
  const classIndex = new Map<ReturnType<typeof classRole>, number>();
  const classCounts = focusObjects.reduce((counts, object) => {
    const klass = classRole(object.role);
    counts.set(klass, (counts.get(klass) || 0) + 1);
    return counts;
  }, new Map<ReturnType<typeof classRole>, number>());

  focusObjects.forEach((object) => {
    objectMap.set(object.node.id, object);
    const klass = classRole(object.role);
    const index = classIndex.get(klass) || 0;
    classIndex.set(klass, index + 1);
    objectPositions.set(object.node.id, objectPosition(object, index, classCounts.get(klass) || 1));
  });

  if (selectedId && !objectMap.has(selectedId)) {
    const selectedNode = model.nodeById.get(selectedId);
    if (selectedNode) {
      const dependencyCount = model.rawEdgesByNode.get(selectedId)?.length || Number(selectedNode.degree || 0) || 0;
      const object = {
        node: selectedNode,
        role: semanticRoleFor(selectedNode),
        rank: objectRank(selectedNode, dependencyCount),
        dependencyCount,
      };
      objectMap.set(selectedId, object);
      objectPositions.set(selectedId, inspecting ? { x: -98, y: 0 } : { x: 0, y: 0 });
    }
  }

  selectedEdgeItems.forEach((item, index) => {
    if (!objectMap.has(item.neighbor.id)) {
      const role = semanticRoleFor(item.neighbor);
      objectMap.set(item.neighbor.id, {
        node: item.neighbor,
        role,
        rank: objectRank(item.neighbor, item.rank),
        dependencyCount: model.rawEdgesByNode.get(item.neighbor.id)?.length || Number(item.neighbor.degree || 0) || 0,
      });
      const base = objectPositions.get(selectedId || '') || { x: -98, y: 0 };
      const offset = inspectNeighborPosition(index);
      objectPositions.set(item.neighbor.id, {
        x: base.x + offset.x,
        y: base.y + offset.y,
      });
    }
  });

  const areaNodes: BuildElement[] = [
    {
      data: {
        id: areaId(focusArea),
        label: focusSignal?.title || focusArea,
        area: focusArea,
        node_kind: 'area',
        display_color: areaColor(focusArea),
        display_size: 1,
      },
      position: { x: 0, y: 0 },
      classes: 'area-node focus-anchor',
    },
    ...outerAreas.map((area, index) => ({
      data: {
        id: areaId(area.slug),
        label: area.title,
        area: area.slug,
        node_kind: 'area',
        display_color: areaColor(area.slug),
        display_size: clamp(area.displaySize * 0.54, 38, 72),
        signal_activity: area.activityScore,
      },
      position: outerAreaPosition(area, focusArea, index, outerAreas.length),
      classes: `area-node area-outer ${focusDeps.some((dep) => dep.sourceArea === area.slug || dep.targetArea === area.slug) ? 'dependency-source' : ''} ${changedSourceIds.has(area.slug) || changedSourceIds.has(areaId(area.slug)) ? 'atlas-live-changed' : ''}`,
    })),
  ];

  const objectNodes: BuildElement[] = [...objectMap.values()].map((object) => {
    const position = objectPositions.get(object.node.id) || { x: 0, y: 0 };
    const labelled = object.node.id === selectedId;
    return {
      data: {
        ...object.node,
        id: objectId(object.node.id),
        label: semanticDisplayLabel(object.node),
        original_id: object.node.id,
        node_kind: 'object',
        semantic_role: object.role,
        semantic_rank: object.rank,
        display_size: clamp(26 + Math.sqrt(object.rank) * 0.9, 32, 54),
      },
      position,
      classes: `semantic-object role-${object.role} class-${classRole(object.role)} ${labelled ? 'labelled-object' : ''} ${object.node.id === selectedId ? 'selected-object' : ''} ${changedSourceIds.has(object.node.id) || changedSourceIds.has(objectId(object.node.id)) ? 'atlas-live-changed' : ''}`,
    };
  });

  const classLabelNodes: BuildElement[] = inspecting ? [] : [
    ['authority', 'authority', -280, -290],
    ['data', 'data dictionary', 260, -290],
    ['dependency', 'active dependencies', -280, 150],
    ['risk', 'risk / stale', 260, 150],
  ].flatMap(([key, label, x, y]) => {
    const count = classCounts.get(key as ReturnType<typeof classRole>) || 0;
    if (count <= 0) return [];
    return [{
      data: {
        id: `class-label::${focusArea}::${key}`,
        label: `${label} ${count}`,
        node_kind: 'class_label',
        display_size: 1,
      },
      position: { x: Number(x), y: Number(y) },
      classes: 'class-label',
    }];
  });

  const dependencyEdges: BuildElement[] = inspecting ? [] : focusDeps.map((dep) => ({
    data: {
      id: `focus::${dep.id}`,
      source: areaId(dep.sourceArea),
      target: areaId(dep.targetArea),
      label: dep.relation,
      display_color: areaColor(dep.sourceArea === focusArea ? dep.targetArea : dep.sourceArea),
      weight: dep.weight,
      signal_activity: dep.activityScore,
    },
    classes: 'focus-dependency',
  }));

  const inspectEdges: BuildElement[] = selectedEdgeItems.map((item) => ({
    data: {
      id: `inspect::${item.edge.id}`,
      source: objectId(selectedId || ''),
      target: objectId(item.neighbor.id),
      label: item.edge.label || 'related',
      weight: item.edge.weight || 1,
      signal_activity: item.edge.activity_score ?? MIN_ACTIVITY_SCORE,
    },
    classes: 'selection-edge',
  }));

  const objectEdges: BuildElement[] = [];
  const renderedEdgeIds = new Set<string>();

  if (!inspecting) {
    [...objectMap.values()].forEach((object) => {
      const rawEdges = model.rawEdgesByNode.get(object.node.id) || [];
      rawEdges.forEach((edge) => {
        if (edge.source && edge.target && objectMap.has(edge.source) && objectMap.has(edge.target)) {
          if (!renderedEdgeIds.has(edge.id)) {
            renderedEdgeIds.add(edge.id);
            objectEdges.push({
              data: {
                id: `object-edge::${edge.id}`,
                source: objectId(edge.source),
                target: objectId(edge.target),
                label: edge.label || 'related',
                weight: edge.weight || 1,
                signal_activity: edge.activity_score ?? MIN_ACTIVITY_SCORE,
              },
              classes: 'object-dependency',
            });
          }
        }
      });
    });
  }

  return [...areaNodes, ...classLabelNodes, ...objectNodes, ...dependencyEdges, ...inspectEdges, ...objectEdges];
}

export { buildElements, buildSemanticModel };

// ── Page component ────────────────────────────────────────────────────

export function AtlasPage() {
  const { payload, loading, error, refresh, lastRefresh, changedSourceIds } = useAtlasGraph();
  const [view, setView] = useState<AtlasView>(() => readViewFromUrl());
  const [focusArea, setFocusArea] = useState<string | null>(() => {
    if (typeof window === 'undefined') return null;
    return new URLSearchParams(window.location.search).get('area');
  });
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [commandOpen, setCommandOpen] = useState(false);
  const [commandDraft, setCommandDraft] = useState('');
  const [previousView, setPreviousView] = useState<AtlasView>('constellation');

  const model = useMemo(() => (payload ? buildSemanticModel(payload) : null), [payload]);
  const selectedNode = selectedNodeId && model ? model.nodeById.get(selectedNodeId) || null : null;
  const selectedEdgeItems = useMemo(() => (model ? selectedEdges(model, selectedNodeId) : []), [model, selectedNodeId]);

  const changedSlugs = useMemo(() => {
    const slugs = new Set<string>();
    changedSourceIds.forEach((id) => {
      if (id.startsWith('area::')) slugs.add(id.slice(6));
      else slugs.add(id);
    });
    return slugs;
  }, [changedSourceIds]);

  const switchView = useCallback((next: AtlasView, area?: string | null) => {
    if (next === 'ledger' && view !== 'ledger') setPreviousView(view);
    setView(next);
    syncViewToUrl(next, area);
  }, [view]);

  const openArea = useCallback((slug: string) => {
    setFocusArea(slug);
    setSelectedNodeId(null);
    switchView('ledger', slug);
  }, [switchView]);

  const backFromLedger = useCallback(() => {
    setSelectedNodeId(null);
    switchView(previousView);
  }, [previousView, switchView]);

  const inspectNode = useCallback((id: string) => {
    const node = model?.nodeById.get(id);
    if (!node || node.is_area) return;
    if (node.area && node.area !== focusArea) setFocusArea(node.area);
    setSelectedNodeId(id);
  }, [focusArea, model]);

  const submitCommand = useCallback((event?: FormEvent) => {
    event?.preventDefault();
    if (!model) return;
    const raw = commandDraft.trim();
    const query = raw.toLowerCase();
    if (!query) {
      setCommandOpen(false);
      return;
    }
    const areaQuery = query.match(/^area:(.+)$/)?.[1]?.trim();
    const areaMatch = model.areas.find((area) => (
      area.slug.toLowerCase() === (areaQuery || query)
      || area.title.toLowerCase().includes(areaQuery || query)
    ));
    if (areaMatch) {
      openArea(areaMatch.slug);
      setCommandOpen(false);
      return;
    }
    const nodeMatch = [...model.nodeById.values()].find((node) => (
      !node.is_area && haystack(node).includes(query)
    ));
    if (nodeMatch) {
      if (nodeMatch.area) openArea(nodeMatch.area);
      inspectNode(nodeMatch.id);
    }
    setCommandOpen(false);
  }, [commandDraft, inspectNode, model, openArea]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const isInput = target && (
        target.tagName === 'INPUT'
        || target.tagName === 'TEXTAREA'
        || target.isContentEditable
      );
      if (event.key === 'Escape') {
        if (commandOpen) {
          setCommandOpen(false);
          return;
        }
        if (view === 'ledger') {
          backFromLedger();
          return;
        }
        return;
      }
      if (isInput || event.defaultPrevented) return;
      if (event.key === '/' || ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k')) {
        event.preventDefault();
        setCommandDraft('');
        setCommandOpen(true);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [backFromLedger, commandOpen, view]);

  useEffect(() => {
    window.__PRAXIS_ATLAS_READY__ = true;
    return () => { window.__PRAXIS_ATLAS_READY__ = false; };
  }, []);

  if (loading) {
    return (
      <div className="atlas-page atlas-page--blank">
        <div className="atlas-page__status-hint">opening atlas</div>
      </div>
    );
  }

  if (error || !payload || !model) {
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

  return (
    <section className="atlas-page" aria-label="Praxis Atlas">
      {/* ── HUD top-left: view label ── */}
      <div className="atlas-hud atlas-hud--tl atlas-mode-chip">
        <span className="atlas-hud__dot" aria-hidden="true" />
        <span>
          {view === 'constellation' ? 'constellation' : view === 'contact' ? 'contact sheet' : focusArea || 'ledger'}
        </span>
      </div>

      {/* ── View toggle ── */}
      <div className="atlas-view-toggle" role="tablist" aria-label="Atlas view">
        <span
          role="tab"
          aria-selected={view === 'constellation'}
          className={view === 'constellation' ? 'prx-radio-pill checked' : 'prx-radio-pill'}
          onClick={() => switchView('constellation')}
          style={{ cursor: 'pointer' }}
        >
          map
        </span>
        <span
          role="tab"
          aria-selected={view === 'contact'}
          className={view === 'contact' ? 'prx-radio-pill checked' : 'prx-radio-pill'}
          onClick={() => switchView('contact')}
          style={{ cursor: 'pointer' }}
        >
          contact
        </span>
        {view === 'ledger' && (
          <span
            role="tab"
            aria-selected
            className="prx-radio-pill checked"
            style={{ cursor: 'pointer' }}
          >
            ledger
          </span>
        )}
      </div>

      {/* ── HUD top-right: freshness ── */}
      <div
        className={`atlas-hud atlas-hud--tr atlas-hud__freshness atlas-hud__freshness--${tone}`}
        title={freshnessTooltip(payload)}
      >
        <span className="atlas-hud__dot" aria-hidden="true" />
        <span className="atlas-hud__freshness-label">{freshnessState}</span>
      </div>

      {/* ── Back button in ledger ── */}
      {view === 'ledger' && (
        <button type="button" className="atlas-collapse-control" onClick={backFromLedger}>
          ← back
        </button>
      )}

      {/* ── HUD bottom ── */}
      <div className="atlas-hud atlas-hud--bl">
        <kbd>/</kbd>
        <span>find</span>
        <span className="atlas-hud__sep">/</span>
        <span className="atlas-hud__ago">{refreshedAgoSec}s ago</span>
      </div>

      <div className="atlas-semantic-strip">
        <span><strong>{model.areas.length}</strong>areas</span>
        <span><strong>{payload.metadata.node_count}</strong>objects</span>
        <span><strong>{model.dependencies.length}</strong>deps</span>
        <span><strong>{model.areas.reduce((sum, a) => sum + a.liveCount, 0)}</strong>live</span>
      </div>

      {/* ── Warnings banner ── */}
      {payload.warnings.length > 0 && (
        <div className="atlas-banner">{payload.warnings.join(' / ')}</div>
      )}

      {/* ── Command palette ── */}
      {commandOpen && (
        <div
          className="atlas-command"
          onClick={(event) => {
            if (event.target === event.currentTarget) setCommandOpen(false);
          }}
        >
          <form className="atlas-command__box" onSubmit={submitCommand}>
            <input
              autoFocus
              type="text"
              value={commandDraft}
              onChange={(event) => setCommandDraft(event.target.value)}
              placeholder="area:authority, table name, bug id, surface, dependency"
            />
            <div className="atlas-command__hints">
              <span>enter</span>
              <span>/</span>
              <span>esc</span>
            </div>
          </form>
        </div>
      )}

      {/* ── Views ── */}
      {view === 'constellation' && (
        <AtlasConstellation
          areas={model.areas}
          dependencies={model.dependencies}
          changedSlugs={changedSlugs}
          onAreaClick={openArea}
        />
      )}

      {view === 'contact' && (
        <AtlasContactSheet
          areas={model.areas}
          changedSlugs={changedSlugs}
          onAreaClick={openArea}
        />
      )}

      {view === 'ledger' && (
        <AtlasLedger
          model={model}
          selectedArea={focusArea}
          onAreaSelect={(slug) => {
            setFocusArea(slug);
            setSelectedNodeId(null);
            syncViewToUrl('ledger', slug);
          }}
          onNodeSelect={inspectNode}
          selectedNodeId={selectedNodeId}
          onBack={backFromLedger}
        />
      )}

      {/* ── Focus card for selected node (in ledger view) ── */}
      {view === 'ledger' && selectedNode && (
        <AtlasFocusCard
          node={selectedNode}
          edges={selectedEdgeItems.map((item) => ({ edge: item.edge, neighbor: item.neighbor }))}
          onNeighborClick={(id) => inspectNode(id)}
          onDismiss={() => setSelectedNodeId(null)}
        />
      )}
    </section>
  );
}

// ── Focus card ────────────────────────────────────────────────────────

interface FocusCardProps {
  node: AtlasElementData;
  edges: { edge: AtlasElementData; neighbor: AtlasElementData }[];
  onNeighborClick: (id: string) => void;
  onDismiss: () => void;
}

function AtlasFocusCard({ node, edges, onNeighborClick, onDismiss }: FocusCardProps) {
  const role = semanticRoleFor(node);
  const color = ROLE_COLORS[role];
  const liveness = node.activity_score == null
    ? 'unknown'
    : node.activity_score >= 0.5
      ? 'live'
      : node.activity_score >= 0.2
        ? 'recent'
        : 'cold';
  const style: CSSProperties = { borderColor: color };

  return (
    <div className="atlas-card" role="dialog" aria-label={node.label || node.id}>
      <div className="atlas-card__head" style={style}>
        <div className="atlas-card__title">{node.label || node.id}</div>
        <button type="button" className="atlas-card__close" onClick={onDismiss} aria-label="close">x</button>
      </div>
      <div className="atlas-card__meta">
        {node.area && <span>{node.area}</span>}
        <span>{roleLabel(role)}</span>
        <span>{nodeKindLabel(node)}</span>
        <span className="atlas-card__liveness" title={node.updated_at || 'unknown'}>{liveness}</span>
      </div>
      <div className="atlas-card__id">{node.id}</div>
      <div className="atlas-card__outcome">{outcomeLine(node, edges.length)}</div>
      {(node.definition_summary || node.preview) && (
        <div className="atlas-card__body">{node.definition_summary || node.preview}</div>
      )}
      <div className="atlas-card__authority">{authorityLabel(node)}</div>
      <AtlasProvenanceChain node={node} />
      {edges.length > 0 && (
        <div className="atlas-card__neighbors">
          {edges.map(({ edge, neighbor }) => (
            <button key={edge.id} type="button" onClick={() => onNeighborClick(neighbor.id)} title={neighbor.id}>
              <span>{edge.label || 'related'}</span>
              {neighbor.label || neighbor.id}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

interface AtlasProvenanceProps {
  node: AtlasElementData;
}

function AtlasProvenanceChain({ node }: AtlasProvenanceProps) {
  const rings: { label: string; what: string; tone?: 'warn' | 'err' }[] = [];

  if (node.authority_source) rings.push({ label: 'authority', what: node.authority_source });
  if (node.binding_revision) rings.push({ label: 'binding', what: node.binding_revision });
  if (node.decision_ref) rings.push({ label: 'decision', what: node.decision_ref });
  if (node.relation_source) rings.push({ label: 'relation', what: node.relation_source });
  if (node.updated_at) rings.push({ label: 'updated', what: node.updated_at });
  if ((node.signal_risk ?? 0) > 0) rings.push({ label: 'risk', what: 'signal_risk · attend', tone: 'err' });

  if (rings.length === 0) return null;

  return (
    <div className="atlas-card__provenance">
      <div className="atlas-card__provenance-cap">PROVENANCE</div>
      <div className="prx-chain">
        {rings.map((r, i) => (
          <div key={`${r.label}-${i}`} className="ev" data-tone={r.tone}>
            <div className="hd">{r.label}</div>
            <div className="what">{r.what}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
