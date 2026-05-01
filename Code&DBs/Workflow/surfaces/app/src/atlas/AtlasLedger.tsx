import { useCallback, useEffect, useMemo, useRef } from 'react';
import { AuthorityBreadcrumb } from '../primitives';
import type { AreaSignal, SemanticModel, SemanticObject } from './AtlasPage';

interface LedgerProps {
  model: SemanticModel;
  selectedArea: string | null;
  onAreaSelect: (slug: string) => void;
  onNodeSelect: (id: string) => void;
  selectedNodeId: string | null;
  onBack: () => void;
}

function fmt(n: number): string {
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + 'k';
  return String(Math.round(n));
}

function heatColor(score: number): string {
  if (score > 0.85) return 'var(--warning)';
  if (score > 0.6) return 'var(--accent)';
  if (score > 0.3) return 'var(--text-muted)';
  return 'var(--border)';
}

function roleTone(role: string): string {
  if (role === 'risk') return 'var(--danger)';
  if (role === 'live') return 'var(--warning)';
  if (role === 'authority') return 'var(--success)';
  return 'var(--text-muted)';
}

export function AtlasLedger({
  model,
  selectedArea,
  onAreaSelect,
  onNodeSelect,
  selectedNodeId,
  onBack,
}: LedgerProps) {
  const areaListRef = useRef<HTMLDivElement>(null);
  const ranked = useMemo(
    () => [...model.areas].sort((a, b) => b.semanticWeight - a.semanticWeight),
    [model.areas],
  );

  const sel = selectedArea ? model.areaSignals.get(selectedArea) ?? ranked[0] ?? null : ranked[0] ?? null;
  const effectiveSlug = sel?.slug ?? null;

  const objects = useMemo(
    () => (effectiveSlug ? model.semanticObjectsByArea.get(effectiveSlug) ?? [] : []),
    [effectiveSlug, model.semanticObjectsByArea],
  );

  const touches = useMemo(() => {
    if (!effectiveSlug) return [];
    const byKey = new Map<string, { slug: string; weight: number; relation: string }>();
    model.dependencies
      .filter((d) => d.sourceArea === effectiveSlug || d.targetArea === effectiveSlug)
      .forEach((d) => {
        const slug = d.sourceArea === effectiveSlug ? d.targetArea : d.sourceArea;
        const key = `${slug}::${d.relation}`;
        const existing = byKey.get(key);
        if (!existing || d.weight > existing.weight) {
          byKey.set(key, { slug, weight: d.weight, relation: d.relation });
        }
      });
    return [...byKey.values()].sort((a, b) => b.weight - a.weight);
  }, [effectiveSlug, model.dependencies]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      const idx = ranked.findIndex((a) => a.slug === effectiveSlug);
      if (e.key === 'j' || e.key === 'ArrowDown') {
        const next = ranked[Math.min(idx + 1, ranked.length - 1)];
        if (next) onAreaSelect(next.slug);
        e.preventDefault();
      } else if (e.key === 'k' || e.key === 'ArrowUp') {
        const prev = ranked[Math.max(idx - 1, 0)];
        if (prev) onAreaSelect(prev.slug);
        e.preventDefault();
      } else if (e.key === 'Escape') {
        onBack();
        e.preventDefault();
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [ranked, effectiveSlug, onAreaSelect, onBack]);

  useEffect(() => {
    if (!effectiveSlug || !areaListRef.current) return;
    const row = areaListRef.current.querySelector<HTMLElement>(`[data-slug="${CSS.escape(effectiveSlug)}"]`);
    row?.scrollIntoView({ block: 'nearest' });
  }, [effectiveSlug]);

  return (
    <div className="atlas-ledger">
      {/* Column 1 — Areas */}
      <div className="atlas-ledger__areas" ref={areaListRef}>
        <div className="atlas-ledger__col-head">
          <span className="atlas-ledger__eyebrow">AREAS</span>
          <span className="atlas-ledger__col-meta">{ranked.length} · WEIGHT ▾</span>
        </div>
        {ranked.map((area) => (
          <div
            key={area.slug}
            data-slug={area.slug}
            className={`atlas-ledger__area-row${area.slug === effectiveSlug ? ' atlas-ledger__area-row--selected' : ''}`}
            onClick={() => onAreaSelect(area.slug)}
          >
            <span className="atlas-ledger__area-weight">
              {String(Math.round(area.semanticWeight)).padStart(2, '0')}
            </span>
            <span className="atlas-ledger__area-name">{area.title}</span>
            <span className="atlas-ledger__area-heat">
              <span
                className="atlas-ledger__area-heat-fill"
                style={{
                  width: `${Math.round(area.activityScore * 100)}%`,
                  background: heatColor(area.activityScore),
                }}
              />
            </span>
          </div>
        ))}
      </div>

      {/* Column 2 — Evidence */}
      <div className="atlas-ledger__evidence">
        <div className="atlas-ledger__col-head">
          {sel ? (
            <AuthorityBreadcrumb
              items={[
                { kind: 'db', cap: 'area', label: sel.title, state: 'live' },
                { kind: 'component', cap: 'lens', label: 'evidence' },
                { kind: 'row', cap: 'state', label: 'verified' },
              ]}
            />
          ) : (
            <>
              <span className="atlas-ledger__eyebrow">EVIDENCE</span>
              <span className="atlas-ledger__col-meta">—</span>
            </>
          )}
        </div>

        {sel && (
          <>
            <div className="atlas-ledger__metrics">
              <LedgerStat label="weight" value={Math.round(sel.semanticWeight)} />
              <LedgerStat label="objects" value={fmt(sel.memberCount)} />
              <LedgerStat
                label="heat"
                value={`${Math.round(sel.activityScore * 100)}/100`}
                color={sel.activityScore > 0.7 ? 'var(--warning)' : undefined}
              />
              <LedgerStat
                label="authority"
                value={sel.authorityCount}
                color={sel.authorityCount > 0 ? 'var(--success)' : undefined}
              />
              <LedgerStat label="data" value={sel.dataCount} />
              <LedgerStat
                label="risk"
                value={sel.riskCount}
                color={sel.riskCount > 0 ? 'var(--danger)' : undefined}
              />
            </div>

            <div className="atlas-ledger__object-head">
              <span className="atlas-ledger__eyebrow">
                OBJECTS · {objects.length}
              </span>
            </div>
            <div className="atlas-ledger__object-list">
              {objects.slice(0, 20).map((obj) => (
                <div
                  key={obj.node.id}
                  className={`atlas-ledger__object-row${obj.node.id === selectedNodeId ? ' atlas-ledger__object-row--selected' : ''}`}
                  onClick={() => onNodeSelect(obj.node.id)}
                >
                  <span
                    className="atlas-ledger__object-role"
                    style={{ color: roleTone(obj.role) }}
                  >
                    {obj.role}
                  </span>
                  <span className="atlas-ledger__object-label">
                    {obj.node.label || obj.node.id}
                  </span>
                  <span className="atlas-ledger__object-score">
                    {Math.round((obj.node.activity_score ?? 0) * 100)}%
                  </span>
                </div>
              ))}
              {objects.length === 0 && (
                <div className="atlas-ledger__empty">no objects in this area</div>
              )}
            </div>
          </>
        )}
      </div>

      {/* Column 3 — Touches (neighboring areas) */}
      <div className="atlas-ledger__touches">
        <div className="atlas-ledger__col-head">
          <span className="atlas-ledger__eyebrow">TOUCHES</span>
          <span className="atlas-ledger__col-meta">{touches.length}</span>
        </div>
        {touches.map((t) => {
          const neighbor = model.areaSignals.get(t.slug);
          return (
            <div
              key={`${t.slug}::${t.relation}`}
              className="atlas-ledger__touch-row"
              onClick={() => onAreaSelect(t.slug)}
            >
              <span className="atlas-ledger__touch-weight">w{Math.round(t.weight)}</span>
              <span className="atlas-ledger__touch-name">
                {neighbor?.title ?? t.slug}
              </span>
              <span className="atlas-ledger__touch-arrow">→</span>
            </div>
          );
        })}
        {touches.length === 0 && (
          <div className="atlas-ledger__empty">no connections</div>
        )}
      </div>
    </div>
  );
}

function LedgerStat({
  label,
  value,
  color,
}: {
  label: string;
  value: string | number;
  color?: string;
}) {
  return (
    <div className="atlas-ledger__stat">
      <span className="atlas-ledger__stat-label">{label}</span>
      <span className="atlas-ledger__stat-value" style={color ? { color } : undefined}>
        {value}
      </span>
    </div>
  );
}
