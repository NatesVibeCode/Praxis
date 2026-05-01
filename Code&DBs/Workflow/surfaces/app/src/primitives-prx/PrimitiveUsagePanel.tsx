import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { StatusRail } from '../primitives/StructuralPrimitives';
import type { PrimitiveTelemetryPayload, TrackedEventName } from './telemetry';

interface PrimitiveUsagePanelProps {
  /** Polling interval in ms. 0 disables polling (live-events only). Default 2000. */
  pollMs?: number;
  /** Max rows shown. Default 100. */
  limit?: number;
  /** Endpoint base. Default `/api/ui/telemetry`. */
  endpoint?: string;
  /** When true, renders inline rather than as a fixed-position overlay. */
  inline?: boolean;
}

const EVENT_GLYPH: Partial<Record<TrackedEventName, string>> = {
  'prx:row-select': '▣',
  'prx:dispatch': '›',
  'prx:wizard-submit': '↵',
  'prx:tool-run': '⊞',
  'prx:cmd-run': '›',
  'prx:tab-select': '⌐',
  'prx:flow-node-select': '◇',
  'prx:step-run': '↦',
  'prx:transport': '◀▶',
  'prx:workflow-control': '◫',
  'prx:prompt-ref-insert': '@',
  'prx:prompt-change': '·',
  'prx:form-change': '·',
};

function summarize(p: PrimitiveTelemetryPayload): string {
  const detail = p.detail as Record<string, unknown> | undefined;
  if (!detail) return '';
  if (typeof detail.action === 'string') return String(detail.action);
  if (typeof detail.op === 'string') return String(detail.op);
  if (typeof detail.value === 'string') return String(detail.value);
  if (typeof detail.name === 'string') return String(detail.name);
  return '';
}

function relTime(iso?: string): string {
  if (!iso) return '';
  const ts = Date.parse(iso);
  if (Number.isNaN(ts)) return '';
  const ms = Date.now() - ts;
  if (ms < 1000) return 'just now';
  if (ms < 60_000) return `${Math.floor(ms / 1000)}s ago`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`;
  return `${Math.floor(ms / 3_600_000)}h ago`;
}

/**
 * Live primitive-usage panel.
 *
 * Polls /api/ui/telemetry/recent and renders each row as a prx-runlog
 * row. Subscribes to `prx:*` events on the document so newly-fired
 * events appear immediately without waiting for the next poll.
 *
 * Use `<PrimitiveUsageOverlay>` for a toggleable fixed-position version.
 */
export function PrimitiveUsagePanel({
  pollMs = 2000,
  limit = 100,
  endpoint = '/api/ui/telemetry',
  inline = false,
}: PrimitiveUsagePanelProps) {
  const [events, setEvents] = useState<PrimitiveTelemetryPayload[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState<string>('');
  const seen = useRef<Set<string>>(new Set());
  const containerRef = useRef<HTMLDivElement | null>(null);

  const fingerprint = (p: PrimitiveTelemetryPayload) =>
    `${p.ts}|${p.event_name}|${p.surface_id ?? ''}`;

  const append = useCallback((batch: PrimitiveTelemetryPayload[]) => {
    setEvents((prev) => {
      const next = [...prev];
      for (const row of batch) {
        const fp = fingerprint(row);
        if (seen.current.has(fp)) continue;
        seen.current.add(fp);
        next.push(row);
      }
      // Keep newest at top
      next.sort((a, b) => (b.ts ?? '').localeCompare(a.ts ?? ''));
      return next.slice(0, limit);
    });
  }, [limit]);

  const reload = useCallback(async () => {
    try {
      const r = await fetch(`${endpoint}/recent?limit=${limit}`);
      if (!r.ok) {
        setError(`status ${r.status}`);
        return;
      }
      const body = await r.json();
      const list: PrimitiveTelemetryPayload[] = Array.isArray(body?.events) ? body.events : [];
      append(list);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, [endpoint, limit, append]);

  // Poll
  useEffect(() => {
    if (pollMs <= 0) return;
    if (paused) return;
    let cancelled = false;
    const tick = async () => {
      if (cancelled) return;
      await reload();
    };
    tick();
    const id = setInterval(tick, pollMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pollMs, paused, reload]);

  // Live subscribe — append in-flight events immediately
  useEffect(() => {
    if (paused) return;
    const TRACKED: TrackedEventName[] = [
      'prx:row-select', 'prx:dispatch', 'prx:wizard-submit', 'prx:tool-run',
      'prx:cmd-run', 'prx:tab-select', 'prx:flow-node-select', 'prx:step-run',
      'prx:transport', 'prx:workflow-control', 'prx:prompt-ref-insert',
      'prx:prompt-change', 'prx:form-change',
    ];
    const handler = (e: Event) => {
      const ce = e as CustomEvent;
      const surface = (() => {
        let cur = e.target as Element | null;
        while (cur) {
          if (cur.id) return cur.id;
          cur = cur.parentElement;
        }
        return null;
      })();
      const mode: PrimitiveTelemetryPayload['mode'] =
        document.body.classList.contains('high-contrast') ? 'high-contrast'
        : document.body.classList.contains('print') ? 'print'
        : document.body.classList.contains('lite') ? 'lite'
        : 'firmware';
      append([{
        event_name: e.type as TrackedEventName,
        surface_id: surface,
        detail: ce.detail,
        ts: new Date().toISOString(),
        mode,
      }]);
    };
    TRACKED.forEach(evt => document.addEventListener(evt, handler));
    return () => TRACKED.forEach(evt => document.removeEventListener(evt, handler));
  }, [paused, append]);

  const filtered = useMemo(() => {
    if (!filter) return events;
    const f = filter.toLowerCase();
    return events.filter(e =>
      (e.event_name || '').toLowerCase().includes(f) ||
      (e.surface_id || '').toLowerCase().includes(f) ||
      JSON.stringify(e.detail || '').toLowerCase().includes(f),
    );
  }, [events, filter]);

  const counts = useMemo(() => {
    const map = new Map<string, number>();
    events.forEach(e => map.set(e.event_name, (map.get(e.event_name) || 0) + 1));
    return [...map.entries()].sort((a, b) => b[1] - a[1]);
  }, [events]);

  const wrapperStyle: React.CSSProperties = inline
    ? { width: '100%' }
    : {
        position: 'fixed',
        bottom: 16, right: 16,
        width: 480,
        maxHeight: 'min(70vh, 560px)',
        zIndex: 200,
        display: 'flex',
        flexDirection: 'column',
      };

  return (
    <div
      ref={containerRef}
      className="prx-friction"
      style={wrapperStyle}
      data-testid="prx-primitive-usage-panel"
      aria-label="Primitive usage telemetry"
      role="region"
    >
      <div
        className="hd"
        style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', padding: '8px 14px' }}
      >
        <span>primitive usage · {events.length} events{error ? ` · err: ${error}` : ''}</span>
        <span style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
          <input
            type="search"
            placeholder="filter…"
            value={filter}
            aria-label="Filter telemetry events"
            onChange={(e) => setFilter(e.target.value)}
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 10,
              padding: '3px 6px',
              background: 'var(--bg-input)',
              color: 'var(--text)',
              border: '1px solid var(--border-faint)',
              borderRadius: 3,
              width: 90,
              letterSpacing: 0,
              textTransform: 'none',
            }}
          />
          <button
            type="button"
            onClick={() => setPaused(p => !p)}
            aria-pressed={paused}
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 10,
              padding: '3px 6px',
              background: 'transparent',
              color: paused ? 'var(--warning)' : 'var(--text-muted)',
              border: '1px solid var(--border-faint)',
              borderRadius: 3,
              cursor: 'pointer',
              letterSpacing: 0.04,
              textTransform: 'uppercase',
            }}
          >
            {paused ? '▶ resume' : '⏸ pause'}
          </button>
          <button
            type="button"
            onClick={() => { setEvents([]); seen.current.clear(); }}
            aria-label="Clear telemetry view"
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 10,
              padding: '3px 6px',
              background: 'transparent',
              color: 'var(--text-muted)',
              border: '1px solid var(--border-faint)',
              borderRadius: 3,
              cursor: 'pointer',
              letterSpacing: 0.04,
              textTransform: 'uppercase',
            }}
          >
            ↻ clear
          </button>
        </span>
      </div>
      {counts.length > 0 && (
        <StatusRail
          style={{ flexWrap: 'wrap', rowGap: 4, padding: '6px 14px' }}
          items={counts.slice(0, 6).map(([name, count]) => ({
            label: (
              <span style={{ textTransform: 'none', fontSize: 10 }}>
                {EVENT_GLYPH[name as TrackedEventName] ?? '·'} {name.replace(/^prx:/, '')}
              </span>
            ),
            value: count,
          }))}
        />
      )}
      <div className="prx-runlog" style={{ overflowY: 'auto', flex: 1, fontSize: 11 }}>
        {filtered.length === 0 ? (
          <div style={{ padding: '16px', color: 'var(--text-muted)', fontSize: 11 }}>
            {paused ? 'paused — resume to receive events' : 'waiting for primitive events…'}
          </div>
        ) : (
          filtered.map((e) => (
            <div className="row" key={fingerprint(e)} style={{ gridTemplateColumns: '70px 130px 1fr 90px' }}>
              <span className="ts">{relTime(e.ts)}</span>
              <span className="actor" title={e.surface_id || ''}>
                {EVENT_GLYPH[e.event_name as TrackedEventName] ?? '·'} {e.event_name.replace(/^prx:/, '')}
              </span>
              <span className="what">
                <span style={{ color: 'var(--text-muted)' }}>{e.surface_id ?? '—'}</span>
                {summarize(e) && <em style={{ marginLeft: 6 }}> · {summarize(e)}</em>}
              </span>
              <span
                className="stat"
                data-tone={e.mode === 'firmware' ? 'dim' : e.mode === 'lite' ? 'ok' : 'warn'}
              >
                {e.mode}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
