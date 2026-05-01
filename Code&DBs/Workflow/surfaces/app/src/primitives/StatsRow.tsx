import React from 'react';

interface Stat {
  label: string;
  value?: string | number | null;
  color?: string;
  tone?: 'ok' | 'warn' | 'err';
}

interface StatsRowProps {
  stats: Stat[];
}

/**
 * StatsRow — renders the prx-status-rail CSS structure.
 * Compresses what used to be card-shaped readouts into a one-row rail,
 * the canonical replacement for "status lines pretending to be cards."
 * Public API unchanged. The optional `tone` prop maps to data-tone.
 */
export function StatsRow({ stats }: StatsRowProps) {
  return (
    <div className="prx-status-rail" data-testid="prx-stats-row">
      {stats.map((stat, i) => {
        const sep = i > 0 ? <span className="sep" key={`sep-${i}`}>·</span> : null;
        const valueAttrs: Record<string, string> = {};
        if (stat.tone) valueAttrs['data-tone'] = stat.tone;
        const inlineColor = stat.color ? { color: stat.color } : undefined;
        return (
          <React.Fragment key={i}>
            {sep}
            <span className="item">
              <span className="label">{stat.label}</span>
              <span className="v" {...valueAttrs} style={inlineColor}>
                {stat.value ?? '—'}
              </span>
            </span>
          </React.Fragment>
        );
      })}
    </div>
  );
}
