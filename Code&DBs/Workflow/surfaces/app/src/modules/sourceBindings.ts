export interface SourceBoundConfig {
  objectType?: string;
  endpoint?: string;
  path?: string;
  placeholder?: string;
  disabledMessage?: string;
  emptyMessage?: string;
  emptyDetail?: string;
  columns?: unknown;
}

export function sourceSelectionPath(selectionKey: string | null | undefined): string | null {
  if (selectionKey === null) return null;
  if (selectionKey === undefined) return 'shared.active_source_option';
  const trimmed = selectionKey.trim();
  if (!trimmed) return null;
  return trimmed.startsWith('shared.') ? trimmed : `shared.${trimmed}`;
}

export function activeSourceId(value: unknown): string | null {
  if (!value || typeof value !== 'object') return null;
  const id = (value as Record<string, unknown>).id;
  return typeof id === 'string' && id.trim() ? id : null;
}

export function sourceBindingFor<T extends object>(
  bindings: Record<string, T> | undefined,
  activeSource: unknown,
): T | null {
  const id = activeSourceId(activeSource);
  if (!id || !bindings) return null;
  return bindings[id] ?? null;
}
