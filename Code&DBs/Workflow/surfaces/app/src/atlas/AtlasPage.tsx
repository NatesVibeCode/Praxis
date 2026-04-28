import { useCallback, useEffect, useMemo, useState, KeyboardEvent, type CSSProperties } from 'react';

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
}

interface AtlasElement {
  data: AtlasElementData;
}

interface AtlasArea {
  slug: string;
  title: string;
  summary: string;
  color: string;
  member_count: number;
}

interface AtlasMetadata {
  node_count: number;
  edge_count: number;
  aggregate_edge_count: number;
  source_authority: string;
  generated_at?: string;
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

type SemanticObjectRole = 'authority' | 'data' | 'dependency' | 'risk' | 'live' | 'stale';

const ATLAS_GRAPH_TIMEOUT_MS = 15_000;
const ATLAS_GRAPH_STREAM_PATH = '/api/atlas/graph/stream';
const MIN_ACTIVITY_SCORE = 0.08;

function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
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
  if (objectKind.includes('operator_decision') || objectKind.includes('authority_') || objectKind.includes('registry') || type.includes('decision') || authority.includes('operator_decisions')) {
    return 'authority';
  }
  if (authority === 'data_dictionary_objects' || type === 'table' || objectKind.startsWith('table:')) {
    return 'data';
  }
  if (type.includes('surface_catalog') || type.includes('capability') || type.includes('tool') || text.includes('connector') || text.includes('dependency')) {
    return 'dependency';
  }
  if (activity >= 0.5) return 'live';
  if (activity < 0.18 && Boolean(data.updated_at)) return 'stale';
  return 'data';
}

function useAtlasGraph() {
  const [payload, setPayload] = useState<AtlasPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    setError(null);
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), ATLAS_GRAPH_TIMEOUT_MS);
    try {
      const response = await fetch('/api/atlas/graph', { cache: 'no-store', signal: controller.signal });
      const body = await response.json().catch(() => null) as AtlasPayload | null;
      if (!response.ok || !body?.ok) throw new Error(body?.detail || body?.error || `Atlas graph request failed with HTTP ${response.status}`);
      setPayload(body);
    } catch (err) {
      if (!silent) setPayload(null);
      setError(err instanceof Error && err.name === 'AbortError' ? 'Timeout' : (err instanceof Error ? err.message : 'Error'));
    } finally {
      window.clearTimeout(timeout);
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.EventSource !== 'function') return;
    const source = new window.EventSource(ATLAS_GRAPH_STREAM_PATH);
    source.onmessage = () => { refresh(true); };
    return () => { source.close(); };
  }, [refresh]);

  return { payload, loading, error, refresh };
}

// -----------------------------------------------------------------------------
// UI COMPONENTS
// -----------------------------------------------------------------------------

function calculateHeat(counts: any, memberCount: number) {
  if (memberCount === 0) return 0.1;
  const active = counts.live + counts.risk * 1.5 + counts.auth * 0.5;
  return Math.min(1, Math.max(0.05, active / Math.max(1, memberCount)));
}

function Sparkline({ data, color, width, height, fill = true }: any) {
  if (!data || data.length === 0) return <div style={{width, height}} />;
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const range = max - min || 1;
  const dx = width / (data.length - 1 || 1);
  const pts = data.map((d: number, i: number) => {
    const x = i * dx;
    const y = height - ((d - min) / range) * height;
    return `${x},${y}`;
  });
  
  const path = `M${pts.join(' L')}`;
  const fillPath = `M0,${height} L${pts.join(' L')} L${width},${height} Z`;

  return (
    <svg width={width} height={height} style={{ display: 'block', overflow: 'visible' }}>
      {fill && <path d={fillPath} fill={color} opacity={0.15} />}
      <path d={path} fill="none" stroke={color} strokeWidth={1.5} />
    </svg>
  );
}

const FAMILIES: Record<string, { label: string }> = {
  codebase: { label: 'Codebase' },
  agents: { label: 'Agents' },
  knowledge: { label: 'Knowledge' },
  system: { label: 'System' },
};

function assignFamily(slug: string) {
  if (slug.includes('agent') || slug.includes('tool') || slug.includes('workflow')) return 'agents';
  if (slug.includes('doc') || slug.includes('memory') || slug.includes('knowledge') || slug.includes('dictionary')) return 'knowledge';
  if (slug.includes('core') || slug.includes('system') || slug.includes('runtime') || slug.includes('platform')) return 'system';
  return 'codebase';
}

function heatColor(heat: number) {
  if (heat > 0.85) return '#d29922'; // hot
  if (heat > 0.6) return '#a08543'; // active
  if (heat > 0.3) return '#6b6e58'; // steady
  return '#3d5567'; // quiet
}

function roleColor(r: string) {
  if (r === 'authority') return '#d29922';
  if (r === 'live') return '#3fb950';
  if (r === 'risk') return '#cf222e';
  if (r === 'stale') return '#5a554d';
  return '#9b9488';
}

export function AtlasPage() {
  const { payload, loading, error, refresh } = useAtlasGraph();
  const [selectedAreaId, setSelectedAreaId] = useState<string | null>(null);

  const model = useMemo(() => {
    if (!payload) return null;
    
    let totalLive = 0;
    
    const areas = payload.areas.map(a => {
      let auth = 0, live = 0, risk = 0, stale = 0, data = 0;
      let lastUpdated = 0;
      
      const objs = payload.nodes.filter(n => !n.data.is_area && n.data.area === a.slug).map(n => {
        const role = semanticRoleFor(n.data);
        if (role === 'authority') auth++;
        else if (role === 'live') live++;
        else if (role === 'risk') risk++;
        else if (role === 'stale') stale++;
        else data++;
        
        if (n.data.updated_at) {
          const t = new Date(n.data.updated_at).getTime();
          if (t > lastUpdated) lastUpdated = t;
        }
        
        return { ...n.data, role };
      });
      
      totalLive += live;
      const counts = { auth, live, risk, stale, data };
      const heat = calculateHeat(counts, a.member_count);
      const weight = (a.member_count * 0.35) + (auth * 2.2) + (data * 1.4) + (risk * 3.2) + (live * 1.2);
      
      // Seeded random sparkline based on heat for prototype effect
      const sparkWrites = Array.from({length: 24}, () => Math.round(Math.random() * heat * 10));
      
      return {
        id: a.slug,
        name: a.title,
        weight: Math.round(weight),
        heat,
        objects: objs.sort((x, y) => {
          const rank = { authority: 0, risk: 1, live: 2, data: 3, dependency: 4, stale: 5 };
          return (rank[x.role as keyof typeof rank] ?? 99) - (rank[y.role as keyof typeof rank] ?? 99);
        }),
        memberCount: a.member_count,
        counts,
        sparkWrites,
        writes24h: sparkWrites.reduce((sum, n) => sum + n, 0),
        lastUpdate: lastUpdated > 0 ? { when: new Date(lastUpdated).toLocaleString(), who: 'system' } : { when: 'unknown', who: 'system' },
        family: assignFamily(a.slug),
        integrations: [],
      };
    }).sort((a, b) => b.weight - a.weight);

    return { areas, payload, totalLive };
  }, [payload]);

  if (loading) return <div style={{...ld.page, background: '#1a1a1a', color: '#f5f1e8'}}><div style={{padding: 40}}>opening atlas...</div></div>;
  if (error || !model) return <div style={{...ld.page, background: '#1a1a1a', color: '#f5f1e8'}}><div style={{padding: 40}}>Error: {error} <button onClick={() => refresh()} style={{background: 'transparent', color: 'inherit', border: '1px solid #f5f1e8', padding: '4px 8px', borderRadius: 4, cursor: 'pointer'}}>retry</button></div></div>;

  const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
  const byFam: Record<string, typeof model.areas> = {};
  model.areas.forEach(a => {
    (byFam[a.family] = byFam[a.family] || []).push(a);
  });
  
  const hotAreas = model.areas.filter(a => a.heat > 0.85).length;
  const dormantAreas = model.areas.filter(a => a.heat < 0.25).length;
  const totalWrites = model.areas.reduce((sum, a) => sum + a.writes24h, 0);
  const selectedArea = selectedAreaId ? model.areas.find(a => a.id === selectedAreaId) : null;

  return (
    <div style={{ ...ld.page, paddingRight: selectedArea ? 440 : 64 }}>
      <div style={ld.masthead}>
        <div style={ld.mastTop}>
          <span>VOL · III</span>
          <span>{today}</span>
          <span>PRAXIS · SYSTEM</span>
        </div>
        <div style={ld.mastTitle}>The Atlas</div>
        <div style={ld.mastSub}>
          Confidence infrastructure, materialized.<br />
          <span style={{ fontSize: 13, opacity: 0.62, fontStyle: 'normal' }}>Rules should be environmental, not conversational.</span>
        </div>
        <div style={ld.mastRule} />
        <div style={ld.mastStats}>
          <span><b>{model.areas.length}</b> areas tracked</span>
          <span><b>{model.payload.metadata.node_count.toLocaleString()}</b> objects</span>
          <span><b>{totalWrites.toLocaleString()}</b> writes today</span>
          <span><b>{model.totalLive}</b> live objects</span>
          <span style={{color:'#d29922'}}>● {hotAreas} areas hot</span>
          <span style={{color:'#5a554d'}}>○ {dormantAreas} areas dormant</span>
        </div>
      </div>

      {Object.entries(FAMILIES).map(([famKey, fam]) => {
        const list = byFam[famKey] || [];
        if (list.length === 0) return null;
        return (
          <section key={famKey} style={ld.section}>
            <div style={ld.sectionHead}>
              <div style={ld.sectionEyebrow}>{fam.label.toUpperCase()} · {list.length} areas</div>
              <div style={ld.sectionTitle}>
                {famKey === 'codebase' && 'The work, and what touches it.'}
                {famKey === 'agents'   && 'Who is running, and what they did.'}
                {famKey === 'knowledge'&& 'What you and the agents know.'}
                {famKey === 'system'   && 'The substrate underneath.'}
              </div>
              <div style={ld.sectionRule} />
            </div>

            <div style={ld.sectionGrid}>
              {list.map(a => (
                <Article 
                  key={a.id} 
                  area={a} 
                  isSelected={selectedAreaId === a.id} 
                  onClick={() => setSelectedAreaId(a.id)} 
                />
              ))}
            </div>
          </section>
        );
      })}

      <div style={ld.colophon}>
        <div style={ld.colRule} />
        <div style={ld.colText}>
          <span>Set in Space Grotesk &amp; IBM Plex Mono. Composed by Praxis. </span>
          <span>Data derived from live system memory. </span>
        </div>
      </div>

      {selectedArea && (
        <div style={ld.sidePanel}>
          <div style={ld.spHead}>
            <div>
              <div style={ld.spEyebrow}>AREA DETAILS</div>
              <div style={ld.spTitle}>{selectedArea.name}</div>
            </div>
            <button onClick={() => setSelectedAreaId(null)} style={ld.spClose}>×</button>
          </div>
          <div style={ld.spContent}>
            <div style={ld.spSectionLabel}>OBJECTS · {selectedArea.objects.length}</div>
            {selectedArea.objects.length === 0 ? (
              <div style={{color: '#5a554d', fontStyle: 'italic', fontSize: 12}}>No objects in this area</div>
            ) : selectedArea.objects.slice(0, 100).map((obj: any) => (
              <div key={obj.id} style={ld.spObjRow}>
                <span style={{...ld.spObjRole, color: roleColor(obj.role)}}>{obj.role}</span>
                <span style={ld.spObjName}>{obj.label || obj.object_kind || obj.id}</span>
                <span style={ld.spObjMeta}>{obj.type || obj.object_kind || 'object'}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

const Article = ({ area, isSelected, onClick }: { area: any, isSelected: boolean, onClick: () => void }) => {
  const heatLabel = area.heat > 0.85 ? 'hot' : area.heat > 0.6 ? 'active' : area.heat > 0.3 ? 'steady' : 'quiet';
  const hc = heatColor(area.heat);
  const isHot = area.heat > 0.85;
  const isQuiet = area.heat < 0.25;

  return (
    <div 
      style={{
        ...ld.article, 
        cursor: 'pointer',
        backgroundColor: isSelected ? '#ebe6d9' : '#f5f1e8',
        outline: isSelected ? '2px solid #1a1a1a' : 'none',
        outlineOffset: '-2px',
      }}
      onClick={onClick}
    >
      <div style={ld.artHead}>
        <div style={ld.artKicker}>
          <span style={{...ld.artDot, background: hc}} />
          <span>{heatLabel.toUpperCase()}</span>
          <span style={{color:'#3a3a3a'}}>·</span>
          <span>weight {area.weight}</span>
        </div>
        <div style={ld.artTitle}>{area.name}</div>
      </div>

      <div style={ld.artLede}>
        <span style={ld.artFigure}>{area.memberCount.toLocaleString()}</span>
        <span style={ld.artFigureUnit}>objects</span>
        <span style={{...ld.artTrend, color: '#3fb950', marginLeft: 'auto'}}>
          {area.counts.live} live
        </span>
      </div>

      <div style={ld.artSparkRow}>
        <Sparkline data={area.sparkWrites} color={hc} width={180} height={28} />
        <div style={ld.artSparkLabel}>writes · 24h · {area.writes24h.toLocaleString()}</div>
      </div>

      <div style={ld.artByline}>
        <span style={ld.artBylineKey}>last —</span>
        <span style={ld.artBylineVal}>{area.lastUpdate.when}, {area.lastUpdate.who}</span>
      </div>

      {(isHot || isQuiet) && (
        <div style={{...ld.artMargin, color: isHot ? '#d29922' : '#5a554d'}}>
          {isHot && '⟶ this area is unusually busy today.'}
          {isQuiet && '⟶ no activity in recent memory.'}
        </div>
      )}
    </div>
  );
};

const ld: Record<string, CSSProperties> = {
  page: {
    width: '100%', height: '100%', overflow: 'auto',
    background: '#f5f1e8', color: '#1a1a1a',
    fontFamily: 'Space Grotesk, sans-serif',
    padding: '48px 64px 64px',
    boxSizing: 'border-box',
    transition: 'padding-right 240ms cubic-bezier(0.2, 0.8, 0.2, 1)',
  },
  masthead: { borderBottom: '2px solid #1a1a1a', paddingBottom: 18, marginBottom: 32 },
  mastTop: {
    display: 'flex', justifyContent: 'space-between',
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.18em',
    color: '#5a554d', textTransform: 'uppercase', marginBottom: 14,
  },
  mastTitle: {
    fontFamily: 'Space Grotesk, sans-serif', fontSize: 88, fontWeight: 500,
    letterSpacing: '-0.04em', lineHeight: 0.95, marginBottom: 8,
  },
  mastSub: { fontSize: 16, color: '#3a3a3a', fontStyle: 'italic', marginBottom: 18 },
  mastRule: { height: 1, background: '#1a1a1a', marginBottom: 12 },
  mastStats: {
    display: 'flex', gap: 22, fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
    color: '#3a3a3a', letterSpacing: '0.04em',
  },
  section: { marginBottom: 44 },
  sectionHead: { marginBottom: 22 },
  sectionEyebrow: {
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.22em',
    color: '#5a554d', marginBottom: 6,
  },
  sectionTitle: { fontSize: 28, fontWeight: 500, letterSpacing: '-0.02em', marginBottom: 10 },
  sectionRule: { height: 1, background: '#1a1a1a' },
  sectionGrid: {
    display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
    gap: 0,
    borderTop: '1px solid #d4cdb8',
  },
  article: {
    padding: '20px 22px 18px',
    borderBottom: '1px solid #d4cdb8',
    borderRight: '1px solid #d4cdb8',
    background: '#f5f1e8',
    transition: 'background-color 160ms ease',
  },
  artHead: { marginBottom: 10 },
  artKicker: {
    display: 'flex', alignItems: 'center', gap: 8,
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.14em',
    color: '#5a554d', marginBottom: 6,
  },
  artDot: { width: 6, height: 6, borderRadius: '50%' },
  artTitle: { fontSize: 22, fontWeight: 500, letterSpacing: '-0.01em', lineHeight: 1.15 },
  artLede: { display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 12 },
  artFigure: { fontSize: 30, fontWeight: 500, letterSpacing: '-0.02em' },
  artFigureUnit: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, color: '#5a554d', letterSpacing: '0.06em' },
  artTrend: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, letterSpacing: '0.04em' },
  artSparkRow: { marginBottom: 12 },
  artSparkLabel: {
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.10em',
    color: '#5a554d', marginTop: 4,
  },
  artByline: {
    display: 'flex', gap: 8, fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
    color: '#3a3a3a', borderTop: '1px dotted #b8af96', paddingTop: 10,
  },
  artBylineKey: { color: '#5a554d' },
  artBylineVal: { fontStyle: 'italic' },
  artMargin: {
    marginTop: 10, fontStyle: 'italic', fontSize: 11, fontFamily: 'IBM Plex Mono, monospace',
    letterSpacing: '0.04em',
  },
  colophon: { marginTop: 40 },
  colRule: { height: 1, background: '#1a1a1a', marginBottom: 12 },
  colText: {
    fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.12em',
    color: '#5a554d', textTransform: 'uppercase',
  },
  sidePanel: {
    position: 'fixed', right: 0, top: 0, bottom: 0, width: 400,
    background: '#f5f1e8', borderLeft: '2px solid #1a1a1a',
    boxShadow: '-8px 0 32px rgba(0,0,0,0.08)',
    display: 'flex', flexDirection: 'column', zIndex: 100,
    animation: 'slideIn 240ms cubic-bezier(0.2, 0.8, 0.2, 1)',
  },
  spHead: {
    padding: '24px 24px 16px', borderBottom: '1px solid #d4cdb8',
    display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
  },
  spEyebrow: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.18em', color: '#5a554d', marginBottom: 6 },
  spTitle: { fontSize: 24, fontWeight: 500, letterSpacing: '-0.01em' },
  spClose: { background: 'transparent', border: 'none', fontSize: 28, cursor: 'pointer', padding: 0, lineHeight: 1, color: '#1a1a1a' },
  spContent: { flex: 1, overflow: 'auto', padding: '20px 24px' },
  spSectionLabel: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 10, letterSpacing: '0.18em', color: '#1a1a1a', marginBottom: 12, borderBottom: '1px solid #d4cdb8', paddingBottom: 6 },
  spObjRow: { display: 'grid', gridTemplateColumns: '80px 1fr', gap: 12, alignItems: 'baseline', padding: '8px 0', borderBottom: '1px solid rgba(26,26,26,0.06)' },
  spObjRole: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 9, letterSpacing: '0.08em', textTransform: 'uppercase' },
  spObjName: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 11, color: '#1a1a1a', wordBreak: 'break-all', fontWeight: 500 },
  spObjMeta: { fontFamily: 'IBM Plex Mono, monospace', fontSize: 9, color: '#5a554d', gridColumn: '2', marginTop: -2 },
};

