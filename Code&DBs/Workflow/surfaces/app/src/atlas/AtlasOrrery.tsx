import { useCallback, useState } from 'react';
import type { AreaSignal } from './AtlasPage';

interface OrreryProps {
  areas: AreaSignal[];
  changedSlugs: Set<string>;
  onAreaClick: (slug: string) => void;
}

const RINGS = [90, 150, 210, 260] as const;
const RING_LABELS = ['MINUTES', 'HOURS', 'DAYS', 'DORMANT'];
const CX = 540;
const CY = 290;

function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function ringForArea(area: AreaSignal): number {
  const s = area.activityScore;
  if (s > 0.7) return RINGS[0];
  if (s > 0.5) return RINGS[1];
  if (s > 0.3) return RINGS[2];
  return RINGS[3];
}

interface Positioned {
  area: AreaSignal;
  x: number;
  y: number;
  ring: number;
}

function positionAreas(areas: AreaSignal[]): Positioned[] {
  const byRing = new Map<number, AreaSignal[]>();
  areas.forEach((a) => {
    const r = ringForArea(a);
    byRing.set(r, [...(byRing.get(r) || []), a]);
  });

  const out: Positioned[] = [];
  byRing.forEach((list, r) => {
    list
      .sort((a, b) => b.semanticWeight - a.semanticWeight)
      .forEach((area, i) => {
        const base = -Math.PI / 2 + (i / Math.max(list.length, 1)) * Math.PI * 2;
        const jitter = (stableHash(area.slug) % 30 - 15) * 0.012;
        const angle = base + jitter;
        out.push({
          area,
          x: CX + Math.cos(angle) * r,
          y: CY + Math.sin(angle) * r,
          ring: r,
        });
      });
  });
  return out;
}

export function AtlasOrrery({ areas, changedSlugs, onAreaClick }: OrreryProps) {
  const [hovered, setHovered] = useState<string | null>(null);
  const positioned = positionAreas(areas);
  const maxWeight = Math.max(...areas.map((a) => a.semanticWeight), 1);
  const ringCounts = RINGS.map((r) => positioned.filter((p) => p.ring === r).length);

  const handleClick = useCallback(
    (slug: string) => (e: React.MouseEvent) => {
      e.stopPropagation();
      onAreaClick(slug);
    },
    [onAreaClick],
  );

  return (
    <div className="atlas-orrery">
      <svg viewBox="0 0 1100 580" className="atlas-orrery__svg">
        {RINGS.map((r, i) => (
          <circle
            key={r}
            cx={CX}
            cy={CY}
            r={r}
            fill="none"
            stroke="rgba(243,238,228,0.06)"
            strokeWidth={1}
            strokeDasharray={i === 0 ? '0' : '2 5'}
          />
        ))}

        <line x1={CX} y1={CY - 280} x2={CX} y2={CY + 280} stroke="rgba(243,238,228,0.04)" />
        <line x1={CX - 300} y1={CY} x2={CX + 300} y2={CY} stroke="rgba(243,238,228,0.04)" />

        {RINGS.map((r, i) => (
          <text
            key={`rl-${r}`}
            x={CX}
            y={CY - r - 6}
            textAnchor="middle"
            className="atlas-orrery__ring-label"
            style={{ fill: i === 0 ? 'var(--text-muted)' : 'var(--border)' }}
          >
            {RING_LABELS[i]}
          </text>
        ))}

        <circle cx={CX} cy={CY} r={5} fill="var(--accent)" />
        <circle cx={CX} cy={CY} r={11} fill="none" stroke="var(--accent)" strokeOpacity={0.3} />
        <text x={CX} y={CY + 26} textAnchor="middle" className="atlas-orrery__you-label">
          YOU
        </text>

        {positioned.map(({ area, x, y }) => {
          const sz = 3 + (area.semanticWeight / maxWeight) * 5;
          const hot = area.activityScore > 0.85;
          const warm = area.activityScore > 0.6;
          const changed = changedSlugs.has(area.slug);
          const isHovered = hovered === area.slug;
          const showLabel = area.activityScore > 0.5 || isHovered;
          return (
            <g
              key={area.slug}
              className="atlas-orrery__node"
              onClick={handleClick(area.slug)}
              onMouseEnter={() => setHovered(area.slug)}
              onMouseLeave={() => setHovered(null)}
              style={{ cursor: 'pointer' }}
            >
              {(hot || changed) && (
                <circle
                  cx={x}
                  cy={y}
                  r={sz + 4}
                  fill={changed ? 'var(--warning)' : hot ? 'var(--warning)' : 'var(--accent)'}
                  opacity={0.2}
                />
              )}
              {isHovered && (
                <circle cx={x} cy={y} r={sz + 6} fill="none" stroke="var(--accent)" strokeOpacity={0.3} />
              )}
              <circle
                cx={x}
                cy={y}
                r={sz}
                fill={hot ? 'var(--warning)' : warm ? 'var(--accent)' : 'var(--text-muted)'}
                opacity={area.activityScore > 0.5 ? 1 : 0.55}
              />
              {showLabel && (
                <text
                  x={x}
                  y={y - sz - 5}
                  textAnchor="middle"
                  className="atlas-orrery__node-label"
                  style={{ opacity: isHovered ? 1 : 0.8 }}
                >
                  {area.title}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <div className="atlas-orrery__footer">
        <span>{ringCounts[0]} MINUTES</span>
        <span>· {ringCounts[1]} HOURS</span>
        <span>· {ringCounts[2]} DAYS</span>
        <span>· {ringCounts[3]} DORMANT</span>
      </div>
    </div>
  );
}
