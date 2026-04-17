import { useEffect, useState } from 'react';
import { GRID_DATA_SOURCES, normalizeGridEndpoint } from './moduleConfigMetadata';

export interface GridEndpointOption {
  value: string;
  label: string;
  description?: string;
  source: 'favorite' | 'route';
}

const SEED_OPTIONS: GridEndpointOption[] = GRID_DATA_SOURCES.map((source) => ({
  value: source.value,
  label: source.label,
  description: source.value,
  source: 'favorite',
}));

let endpointOptionsCache: GridEndpointOption[] | null = null;
let endpointOptionsPromise: Promise<GridEndpointOption[]> | null = null;

function titleCase(value: string): string {
  return value
    .split(/[\s/_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function humanizeEndpoint(value: string): string {
  const normalized = normalizeGridEndpoint(value);
  if (!normalized) return 'Custom endpoint';
  return normalized
    .split('/')
    .map((part) => titleCase(part))
    .join(' / ');
}

function mergeEndpointOptions(...lists: GridEndpointOption[][]): GridEndpointOption[] {
  const seen = new Map<string, GridEndpointOption>();

  for (const list of lists) {
    for (const item of list) {
      const normalized = normalizeGridEndpoint(item.value);
      if (!normalized) continue;
      if (!seen.has(normalized) || seen.get(normalized)?.source !== 'favorite') {
        seen.set(normalized, { ...item, value: normalized });
      }
    }
  }

  return Array.from(seen.values()).sort((left, right) => {
    if (left.source !== right.source) {
      return left.source === 'favorite' ? -1 : 1;
    }
    return left.label.localeCompare(right.label);
  });
}

async function loadEndpointOptions(): Promise<GridEndpointOption[]> {
  if (endpointOptionsCache) return endpointOptionsCache;
  if (endpointOptionsPromise) return endpointOptionsPromise;

  endpointOptionsPromise = (async () => {
    const response = await fetch('/api/routes?method=GET&path_prefix=/api/&visibility=public');
    if (!response.ok) {
      throw new Error(`Failed to load route catalog: ${response.status} ${response.statusText}`);
    }

    const payload = await response.json() as { routes?: Array<Record<string, unknown>> };
    const routeOptions = (Array.isArray(payload.routes) ? payload.routes : [])
      .map((route) => {
        const rawPath = typeof route.path === 'string' ? route.path : '';
        if (!rawPath.startsWith('/api/') || rawPath.includes('{')) return null;
        const value = normalizeGridEndpoint(rawPath);
        if (!value) return null;

        const summary = typeof route.summary === 'string' && route.summary.trim()
          ? route.summary.trim()
          : typeof route.description === 'string' && route.description.trim()
            ? route.description.trim()
            : rawPath;

        return {
          value,
          label: humanizeEndpoint(value),
          description: summary,
          source: 'route' as const,
        };
      })
      .filter((item): item is NonNullable<typeof item> => item !== null);

    endpointOptionsCache = mergeEndpointOptions(SEED_OPTIONS, routeOptions);
    return endpointOptionsCache;
  })()
    .catch((error) => {
      endpointOptionsCache = SEED_OPTIONS;
      throw error;
    })
    .finally(() => {
      endpointOptionsPromise = null;
    });

  return endpointOptionsPromise;
}

export function useEndpointOptions(): {
  options: GridEndpointOption[];
  loading: boolean;
  error: string | null;
} {
  const [options, setOptions] = useState<GridEndpointOption[]>(endpointOptionsCache ?? SEED_OPTIONS);
  const [loading, setLoading] = useState(endpointOptionsCache === null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    loadEndpointOptions()
      .then((nextOptions) => {
        if (cancelled) return;
        setOptions(nextOptions);
        setError(null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setOptions(SEED_OPTIONS);
        setError(err instanceof Error ? err.message : 'Failed to load endpoint suggestions');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return { options, loading, error };
}
