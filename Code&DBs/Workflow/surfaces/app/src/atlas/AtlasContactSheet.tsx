import { useCallback } from 'react';
import type { AreaSignal } from './AtlasPage';

interface ContactSheetProps {
  areas: AreaSignal[];
  changedSlugs: Set<string>;
  onAreaClick: (slug: string) => void;
}

function fmt(n: number): string {
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + 'k';
  return String(Math.round(n));
}

function heatColor(score: number): string {
  if (score > 0.85) return 'var(--warning)';
  if (score > 0.6) return 'var(--accent)';
  return 'var(--text-muted)';
}

export function AtlasContactSheet({ areas, changedSlugs, onAreaClick }: ContactSheetProps) {
  const sorted = [...areas].sort((a, b) => b.semanticWeight - a.semanticWeight);
  const totalObjects = areas.reduce((sum, a) => sum + a.memberCount, 0);

  const handleClick = useCallback(
    (slug: string) => () => onAreaClick(slug),
    [onAreaClick],
  );

  return (
    <div className="atlas-contact">
      <div className="atlas-contact__grid">
        {sorted.map((area) => {
          const hot = area.activityScore > 0.85;
          const changed = changedSlugs.has(area.slug);
          return (
            <div
              key={area.slug}
              className={`atlas-contact__card ${changed ? 'atlas-contact__card--changed' : ''}`}
              onClick={handleClick(area.slug)}
            >
              {hot && <div className="atlas-contact__heat-strip" />}
              <div className="atlas-contact__card-head">
                <span className="atlas-contact__card-eyebrow">
                  {area.riskCount > 0 ? 'RISK' : area.activityScore > 0.7 ? 'ACTIVE' : 'IDLE'}
                </span>
              </div>
              <div className="atlas-contact__card-title">{area.title}</div>
              <div className="atlas-contact__card-stats">
                <span>{fmt(area.memberCount)} obj</span>
                <span className="atlas-contact__sep">·</span>
                <span>{area.authorityCount} auth</span>
                <span className="atlas-contact__sep">·</span>
                <span>{area.dataCount} data</span>
              </div>
              <div className="atlas-contact__card-gauge">
                <div
                  className="atlas-contact__card-gauge-fill"
                  style={{
                    width: `${Math.round(area.activityScore * 100)}%`,
                    background: heatColor(area.activityScore),
                  }}
                />
              </div>
              <div className="atlas-contact__card-meta">
                <span>w{Math.round(area.semanticWeight)}</span>
                <span>{Math.round(area.activityScore * 100)}% heat</span>
                {area.riskCount > 0 && (
                  <span style={{ color: 'var(--danger)' }}>{area.riskCount} risk</span>
                )}
              </div>
              {area.summary && (
                <div className="atlas-contact__card-summary">{area.summary}</div>
              )}
            </div>
          );
        })}
      </div>

      <div className="atlas-contact__footer">
        <span>{areas.length} AREAS · {fmt(totalObjects)} OBJECTS</span>
        <span style={{ marginLeft: 'auto' }}>SORT · WEIGHT ▾</span>
      </div>
    </div>
  );
}
