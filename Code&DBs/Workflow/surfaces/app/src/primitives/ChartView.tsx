import { useMemo } from 'react';

interface ChartViewProps {
  chartType: 'bar' | 'line' | 'pie';
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  cellOpacities?: number[];
}

const W = 320;
const H = 180;
const PAD = { top: 12, right: 12, bottom: 36, left: 44 };

const TONES = ['ok', 'warn', 'err', 'dim', 'live', 'ok'] as const;

function toneVar(index: number): string {
  const tone = TONES[index % TONES.length];
  if (tone === 'ok') return 'var(--success)';
  if (tone === 'warn') return 'var(--warning)';
  if (tone === 'err') return 'var(--danger)';
  if (tone === 'live') return 'var(--accent)';
  return 'var(--text-muted)';
}

export function ChartView({ chartType, data, xKey, yKey, cellOpacities }: ChartViewProps) {
  const values = useMemo(
    () => data.map((d) => Number(d[yKey] ?? 0)),
    [data, yKey],
  );
  const labels = useMemo(
    () => data.map((d) => String(d[xKey] ?? '')),
    [data, xKey],
  );
  const maxVal = Math.max(...values, 1);

  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;

  if (data.length === 0) {
    return (
      <div className="prx-frame" data-tone="dim">
        <span>No data</span>
      </div>
    );
  }

  if (chartType === 'pie') {
    const cx = W / 2, cy = H / 2, r = Math.min(W, H) / 2 - 20;
    const total = values.reduce((a, b) => a + b, 0) || 1;
    let angle = -Math.PI / 2;
    const slices = values.map((v, i) => {
      const sweep = (v / total) * 2 * Math.PI;
      const x1 = cx + r * Math.cos(angle);
      const y1 = cy + r * Math.sin(angle);
      angle += sweep;
      const x2 = cx + r * Math.cos(angle);
      const y2 = cy + r * Math.sin(angle);
      const large = sweep > Math.PI ? 1 : 0;
      const path = `M${cx},${cy} L${x1},${y1} A${r},${r} 0 ${large} 1 ${x2},${y2} Z`;
      return { path, color: toneVar(i), opacity: cellOpacities?.[i] ?? 1 };
    });
    return (
      <svg viewBox={`0 0 ${W} ${H}`} className="prx-chart-svg">
        {slices.map((s, i) => (
          <path key={i} d={s.path} fill={s.color} opacity={s.opacity} stroke="var(--bg-card)" strokeWidth={1} />
        ))}
      </svg>
    );
  }

  if (chartType === 'line') {
    const pts = values.map((v, i) => {
      const x = PAD.left + (i / Math.max(values.length - 1, 1)) * innerW;
      const y = PAD.top + (1 - v / maxVal) * innerH;
      return { x, y };
    });
    const d = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
    return (
      <svg viewBox={`0 0 ${W} ${H}`} className="prx-chart-svg">
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
        <line x1={PAD.left} y1={PAD.top + innerH} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
        <path d={d} fill="none" stroke="var(--accent)" strokeWidth={2} />
        {pts.map((p, i) => (
          <circle key={i} cx={p.x} cy={p.y} r={3} fill="var(--accent)" opacity={cellOpacities?.[i] ?? 1} />
        ))}
        {labels.map((l, i) => {
          const x = PAD.left + (i / Math.max(labels.length - 1, 1)) * innerW;
          return (
            <text key={i} x={x} y={H - 4} textAnchor="middle" fill="var(--text-muted)" fontSize={9}>
              {l.slice(0, 8)}
            </text>
          );
        })}
      </svg>
    );
  }

  const barW = innerW / values.length - 4;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="prx-chart-svg">
      <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
      <line x1={PAD.left} y1={PAD.top + innerH} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border)" strokeWidth={1} />
      {values.map((v, i) => {
        const bh = (v / maxVal) * innerH;
        const x = PAD.left + i * (innerW / values.length) + 2;
        const y = PAD.top + innerH - bh;
        return (
          <g key={i}>
            <rect
              x={x} y={y} width={barW} height={bh}
              fill={toneVar(i)}
              opacity={cellOpacities?.[i] ?? 1}
              rx={2}
            />
            <text x={x + barW / 2} y={H - 4} textAnchor="middle" fill="var(--text-muted)" fontSize={9}>
              {labels[i].slice(0, 8)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
