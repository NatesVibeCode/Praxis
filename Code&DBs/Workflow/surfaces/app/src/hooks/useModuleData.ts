import { useState, useEffect, useCallback, useRef } from 'react';

export function useModuleData<T>(
  endpoint: string,
  options?: { refreshInterval?: number; enabled?: boolean }
): {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
} {
  const enabled = options?.enabled ?? true;
  const refreshInterval = options?.refreshInterval;

  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchData = useCallback(async () => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/${endpoint}`, { signal: controller.signal });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const json = await res.json();
      setData(json);
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === 'AbortError') return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [endpoint]);

  useEffect(() => {
    if (!enabled) {
      setLoading(false);
      return;
    }

    fetchData();

    if (refreshInterval && refreshInterval > 0) {
      const id = setInterval(fetchData, refreshInterval);
      return () => {
        clearInterval(id);
        abortRef.current?.abort();
      };
    }

    return () => {
      abortRef.current?.abort();
    };
  }, [enabled, fetchData, refreshInterval]);

  return { data, loading, error, refetch: fetchData };
}
