import React, { useMemo } from 'react';

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

export function ChartView({ chartType, data, xKey, yKey, cellOpacities }: ChartViewProps) {
  const COLORS = ['#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff', '#79c0ff'];

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
      <div style={{
        background: 'var(--bg-card, #161b22)',
        border: '1px solid var(--border, #30363d)',
        borderRadius: 'var(--radius, 6px)',
        padding: 'var(--space-lg, 16px)',
        color: 'var(--text-muted, #8b949e)',
        fontSize: 13,
        textAlign: 'center',
        height: H,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}>
        No data
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
      return { path, color: COLORS[i % COLORS.length], opacity: cellOpacities?.[i] ?? 1 };
    });
    return (
      <svg width={W} height={H} style={{ display: 'block' }}>
        {slices.map((s, i) => (
          <path key={i} d={s.path} fill={s.color} opacity={s.opacity} stroke="var(--bg-card, #161b22)" strokeWidth={1} />
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
      <svg width={W} height={H} style={{ display: 'block' }}>
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top + innerH} stroke="var(--border, #30363d)" strokeWidth={1} />
        <line x1={PAD.left} y1={PAD.top + innerH} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border, #30363d)" strokeWidth={1} />
        <path d={d} fill="none" stroke="#58a6ff" strokeWidth={2} />
        {pts.map((p, i) => (
          <circle key={i} cx={p.x} cy={p.y} r={3} fill="#58a6ff" opacity={cellOpacities?.[i] ?? 1} />
        ))}
        {labels.map((l, i) => {
          const x = PAD.left + (i / Math.max(labels.length - 1, 1)) * innerW;
          return (
            <text key={i} x={x} y={H - 4} textAnchor="middle" fill="#8b949e" fontSize={9}>
              {l.slice(0, 8)}
            </text>
          );
        })}
      </svg>
    );
  }

  // bar (default)
  const barW = innerW / values.length - 4;
  return (
    <svg width={W} height={H} style={{ display: 'block' }}>
      <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top + innerH} stroke="var(--border, #30363d)" strokeWidth={1} />
      <line x1={PAD.left} y1={PAD.top + innerH} x2={PAD.left + innerW} y2={PAD.top + innerH} stroke="var(--border, #30363d)" strokeWidth={1} />
      {values.map((v, i) => {
        const bh = (v / maxVal) * innerH;
        const x = PAD.left + i * (innerW / values.length) + 2;
        const y = PAD.top + innerH - bh;
        return (
          <g key={i}>
            <rect
              x={x} y={y} width={barW} height={bh}
              fill={COLORS[i % COLORS.length]}
              opacity={cellOpacities?.[i] ?? 1}
              rx={2}
            />
            <text x={x + barW / 2} y={H - 4} textAnchor="middle" fill="#8b949e" fontSize={9}>
              {labels[i].slice(0, 8)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
