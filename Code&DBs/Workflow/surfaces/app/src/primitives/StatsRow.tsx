import { StatusRail, type StatusRailItem } from './StructuralPrimitives';

interface Stat {
  label: string;
  value?: string | number | null;
  color?: string;
  tone?: 'ok' | 'warn' | 'err';
}

interface StatsRowProps {
  stats: Stat[];
}

export function StatsRow({ stats }: StatsRowProps) {
  const items: StatusRailItem[] = stats.map((s) => ({
    label: s.label,
    value: s.value ?? '—',
    tone: s.tone,
  }));
  return <StatusRail items={items} />;
}
