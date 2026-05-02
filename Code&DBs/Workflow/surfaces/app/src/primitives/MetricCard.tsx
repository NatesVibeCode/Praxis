interface MetricCardProps {
  label?: string;
  value?: string | number | null;
  color?: string;
}

export function MetricCard({ label, value, color }: MetricCardProps) {
  return (
    <div className="prx-roi" data-testid="prx-metric-card">
      <div className="stat">
        {label && <div className="label">{label}</div>}
        <div className="v" style={color ? { color } : undefined}>
          {value ?? '—'}
        </div>
      </div>
    </div>
  );
}
