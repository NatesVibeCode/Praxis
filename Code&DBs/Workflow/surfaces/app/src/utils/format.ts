/**
 * Text formatting utilities for consistent UI labels.
 */

/** "deal_value" → "Deal Value", "in_progress" → "In Progress" */
export function capitalizeLabel(s: string): string {
  return s
    .replace(/[_-]/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .trim();
}

/** Format a number with locale separators */
export function formatNumber(n: number): string {
  return n.toLocaleString();
}

/** Format currency */
export function formatCurrency(n: number): string {
  return `$${n.toLocaleString()}`;
}

export function getPath(obj: unknown, path: string): unknown {
  return path.split('.').reduce(
    (o, k) => (o && typeof o === 'object' ? (o as Record<string, unknown>)[k] : undefined),
    obj,
  );
}

export function formatValue(raw: unknown, format?: string): string | number {
  if (raw == null) return '\u2014';
  const num = typeof raw === 'number' ? raw : parseFloat(String(raw));
  if (Number.isNaN(num)) return String(raw);

  switch (format) {
    case 'percent':
      return `${(num * 100).toFixed(1)}%`;
    case 'currency':
      return formatCurrency(num);
    default:
      return formatNumber(num);
  }
}
