import { useCallback, useMemo, useState } from 'react';
import type { AreaSignal, DependencySignal } from './AtlasPage';

interface ConstellationProps {
  areas: AreaSignal[];
  dependencies: DependencySignal[];
  changedSlugs: Set<string>;
  onAreaClick: (slug: string) => void;
}

const W = 1100;
const H = 580;
const CX = W / 2;
const CY = H / 2;
const PAD = 60;

function stableHash(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

interface Positioned {
  area: AreaSignal;
  x: number;
  y: number;
  r: number;
}

function layoutConstellation(
  areas: AreaSignal[],
  deps: DependencySignal[],
): Positioned[] {
  if (areas.length === 0) return [];

  const maxWeight = Math.max(...areas.map((a) => a.semanticWeight), 1);
  const nodes: Positioned[] = areas.map((area, i) => {
    const angle = (i / areas.length) * Math.PI * 2 - Math.PI / 2;
    const spread = Math.min(CX - PAD, CY - PAD) * 0.75;
    const jitter = (stableHash(area.slug) % 40 - 20) * 0.6;
    return {
      area,
      x: CX + Math.cos(angle) * (spread + jitter),
      y: CY + Math.sin(angle) * (spread + jitter),
      r: 4 + (area.semanticWeight / maxWeight) * 14,
    };
  });

  const slugIdx = new Map(nodes.map((n, i) => [n.area.slug, i]));

  // simple force iterations: edges attract, all repel
  for (let iter = 0; iter < 120; iter++) {
    const fx = new Float64Array(nodes.length);
    const fy = new Float64Array(nodes.length);

    // repulsion between all pairs
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const dx = nodes[j].x - nodes[i].x;
        const dy = nodes[j].y - nodes[i].y;
        const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
        const repulse = 4000 / (dist * dist);
        const nx = dx / dist;
        const ny = dy / dist;
        fx[i] -= nx * repulse;
        fy[i] -= ny * repulse;
        fx[j] += nx * repulse;
        fy[j] += ny * repulse;
      }
    }

    // attraction along edges
    for (const dep of deps) {
      const si = slugIdx.get(dep.sourceArea);
      const ti = slugIdx.get(dep.targetArea);
      if (si === undefined || ti === undefined) continue;
      const dx = nodes[ti].x - nodes[si].x;
      const dy = nodes[ti].y - nodes[si].y;
      const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
      const attract = dist * 0.008 * Math.min(dep.weight, 5);
      const nx = dx / dist;
      const ny = dy / dist;
      fx[si] += nx * attract;
      fy[si] += ny * attract;
      fx[ti] -= nx * attract;
      fy[ti] -= ny * attract;
    }

    // gravity toward center
    for (let i = 0; i < nodes.length; i++) {
      const dx = CX - nodes[i].x;
      const dy = CY - nodes[i].y;
      fx[i] += dx * 0.003;
      fy[i] += dy * 0.003;
    }

    const cooling = 1 - iter / 140;
    for (let i = 0; i < nodes.length; i++) {
      nodes[i].x += fx[i] * cooling;
      nodes[i].y += fy[i] * cooling;
      // clamp to viewbox
      nodes[i].x = Math.max(PAD + nodes[i].r, Math.min(W - PAD - nodes[i].r, nodes[i].x));
      nodes[i].y = Math.max(PAD + nodes[i].r, Math.min(H - PAD - nodes[i].r, nodes[i].y));
    }
  }

  return nodes;
}

export function AtlasConstellation({
  areas,
  dependencies,
  changedSlugs,
  onAreaClick,
}: ConstellationProps) {
  const [hovered, setHovered] = useState<string | null>(null);

  const positioned = useMemo(
    () => layoutConstellation(areas, dependencies),
    [areas, dependencies],
  );

  const slugPos = useMemo(() => {
    const m = new Map<string, Positioned>();
    positioned.forEach((p) => m.set(p.area.slug, p));
    return m;
  }, [positioned]);

  // dedupe edges for rendering
  const edgesDeduped = useMemo(() => {
    const seen = new Set<string>();
    return dependencies.filter((d) => {
      const key = [d.sourceArea, d.targetArea].sort().join('::');
      if (seen.has(key)) return false;
      seen.add(key);
      return slugPos.has(d.sourceArea) && slugPos.has(d.targetArea);
    });
  }, [dependencies, slugPos]);

  const hoveredNeighbors = useMemo(() => {
    if (!hovered) return new Set<string>();
    const s = new Set<string>();
    dependencies.forEach((d) => {
      if (d.sourceArea === hovered) s.add(d.targetArea);
      if (d.targetArea === hovered) s.add(d.sourceArea);
    });
    return s;
  }, [hovered, dependencies]);

  const handleClick = useCallback(
    (slug: string) => (e: React.MouseEvent) => {
      e.stopPropagation();
      onAreaClick(slug);
    },
    [onAreaClick],
  );

  const maxWeight = Math.max(...areas.map((a) => a.semanticWeight), 1);

  return (
    <div className="atlas-constellation">
      <svg viewBox={`0 0 ${W} ${H}`} className="atlas-constellation__svg">
        {/* edges */}
        {edgesDeduped.map((dep) => {
          const s = slugPos.get(dep.sourceArea)!;
          const t = slugPos.get(dep.targetArea)!;
          const isLit =
            hovered === dep.sourceArea ||
            hovered === dep.targetArea;
          return (
            <line
              key={`${dep.sourceArea}-${dep.targetArea}`}
              x1={s.x}
              y1={s.y}
              x2={t.x}
              y2={t.y}
              stroke={isLit ? 'var(--accent)' : 'var(--border-faint)'}
              strokeWidth={isLit ? 1 : 0.5}
              strokeOpacity={isLit ? 0.6 : 0.25}
              className="atlas-constellation__edge"
            />
          );
        })}

        {/* nodes */}
        {positioned.map(({ area, x, y, r }) => {
          const hot = area.activityScore > 0.85;
          const warm = area.activityScore > 0.6;
          const hasRisk = area.riskCount > 0;
          const changed = changedSlugs.has(area.slug);
          const isHovered = hovered === area.slug;
          const isNeighbor = hoveredNeighbors.has(area.slug);
          const showLabel =
            isHovered ||
            isNeighbor ||
            area.semanticWeight / maxWeight > 0.4;

          let strokeColor = 'var(--text-muted)';
          if (hasRisk) strokeColor = 'var(--danger)';
          else if (hot) strokeColor = 'var(--warning)';
          else if (warm) strokeColor = 'var(--accent)';

          return (
            <g
              key={area.slug}
              className="atlas-constellation__node"
              onClick={handleClick(area.slug)}
              onMouseEnter={() => setHovered(area.slug)}
              onMouseLeave={() => setHovered(null)}
              style={{ cursor: 'pointer' }}
            >
              {/* invisible hit area — generous pad so hover doesn't flicker */}
              <circle
                cx={x}
                cy={y}
                r={Math.max(r + 12, 18)}
                fill="transparent"
              />
              {/* outer glow for hot/changed nodes */}
              <circle
                cx={x}
                cy={y}
                r={r + 6}
                fill="none"
                stroke={hasRisk ? 'var(--danger)' : 'var(--warning)'}
                strokeOpacity={(hot || changed) ? 0.2 : 0}
                strokeWidth={2}
                className="atlas-constellation__glow"
              />
              {/* hover ring — always mounted, opacity transitions */}
              <circle
                cx={x}
                cy={y}
                r={r + 4}
                fill="none"
                stroke="var(--accent)"
                strokeOpacity={isHovered ? 0.4 : isNeighbor ? 0.15 : 0}
                className="atlas-constellation__hover-ring"
              />
              {/* main ring — hollow */}
              <circle
                cx={x}
                cy={y}
                r={r}
                fill="none"
                stroke={strokeColor}
                strokeWidth={isHovered ? 1.8 : 1.2}
                strokeOpacity={area.activityScore > 0.3 ? 0.8 : 0.35}
                className="atlas-constellation__ring"
              />
              {/* tiny center dot for orientation */}
              <circle
                cx={x}
                cy={y}
                r={1.5}
                fill={strokeColor}
                opacity={0.5}
              />
              {/* label */}
              {showLabel && (
                <text
                  x={x}
                  y={y - r - 6}
                  textAnchor="middle"
                  className="atlas-constellation__label"
                  style={{ opacity: isHovered ? 1 : isNeighbor ? 0.7 : 0.55 }}
                >
                  {area.title}
                </text>
              )}
              {/* weight number inside ring for large nodes */}
              {r > 10 && (
                <text
                  x={x}
                  y={y + 3.5}
                  textAnchor="middle"
                  className="atlas-constellation__weight"
                >
                  {Math.round(area.semanticWeight)}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <div className="atlas-constellation__footer">
        <span>{areas.length} AREAS</span>
        <span>· {edgesDeduped.length} CONNECTIONS</span>
        <span>· HOVER TO TRACE</span>
      </div>
    </div>
  );
}
