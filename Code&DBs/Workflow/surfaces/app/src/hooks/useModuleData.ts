import { useState, useEffect, useCallback, useRef } from 'react';
import { isAbortError } from '../shared/request';

/**
 * Module data source. Legacy string form fetches /api/<endpoint>. The object
 * form with source.projection_ref fetches /api/projections/<projection_ref>
 * and returns envelope.output through the data channel — the typed CQRS path
 * anchored by architecture-policy::surface-catalog::surface-composition-cqrs-
 * direction. Both endpoint and source may coexist during transition; source
 * wins and a console warning fires so the coexistence is visible.
 */
export type ModuleDataSpec =
  | string
  | {
      endpoint?: string;
      source?: { projection_ref?: string };
    };

interface ProjectionEnvelope<T> {
  projection_ref: string;
  output: T | null;
  last_event_id: string | null;
  last_receipt_id: string | null;
  last_refreshed_at: string | null;
  freshness_status: string;
  source_refs: unknown[];
  read_model_object_ref: string | null;
  authority_domain_ref: string | null;
  warnings: string[];
}

function resolveFetchUrl(spec: ModuleDataSpec): string | null {
  if (typeof spec === 'string') {
    return spec ? `/api/${spec}` : null;
  }
  const projectionRef = spec.source?.projection_ref;
  if (projectionRef && spec.endpoint) {
    console.warn(
      `[useModuleData] source.projection_ref (${projectionRef}) wins over endpoint (${spec.endpoint}); drop one to silence.`,
    );
  }
  if (projectionRef) {
    return `/api/projections/${projectionRef}`;
  }
  if (spec.endpoint) {
    return `/api/${spec.endpoint}`;
  }
  return null;
}

function usesProjection(spec: ModuleDataSpec): boolean {
  return typeof spec !== 'string' && Boolean(spec.source?.projection_ref);
}

export function useModuleData<T>(
  spec: ModuleDataSpec,
  options?: { refreshInterval?: number; enabled?: boolean },
): {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
} {
  const enabled = options?.enabled ?? true;
  const refreshInterval = options?.refreshInterval;
  const fetchUrl = resolveFetchUrl(spec);
  const isProjection = usesProjection(spec);

  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(enabled && Boolean(fetchUrl));
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const requestSeqRef = useRef(0);

  const fetchData = useCallback(async () => {
    if (!fetchUrl) {
      requestSeqRef.current += 1;
      abortRef.current?.abort();
      abortRef.current = null;
      setLoading(false);
      return;
    }
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    const isCurrent = () =>
      requestSeqRef.current === requestSeq
      && abortRef.current === controller
      && !controller.signal.aborted;

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(fetchUrl, { signal: controller.signal });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const json = await res.json();
      if (isProjection) {
        const envelope = json as ProjectionEnvelope<T>;
        if (!isCurrent()) return;
        setData((envelope.output ?? null) as T | null);
      } else {
        if (!isCurrent()) return;
        setData(json as T);
      }
    } catch (err: unknown) {
      if (!isCurrent() || isAbortError(err)) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (isCurrent()) {
        setLoading(false);
      }
      if (abortRef.current === controller) {
        abortRef.current = null;
      }
    }
  }, [fetchUrl, isProjection]);

  useEffect(() => {
    if (!enabled || !fetchUrl) {
      requestSeqRef.current += 1;
      abortRef.current?.abort();
      abortRef.current = null;
      setLoading(false);
      return;
    }

    fetchData();

    if (refreshInterval && refreshInterval > 0) {
      const id = setInterval(fetchData, refreshInterval);
      return () => {
        clearInterval(id);
        requestSeqRef.current += 1;
        abortRef.current?.abort();
        abortRef.current = null;
      };
    }

    return () => {
      requestSeqRef.current += 1;
      abortRef.current?.abort();
      abortRef.current = null;
    };
  }, [enabled, fetchData, fetchUrl, refreshInterval]);

  return { data, loading, error, refetch: fetchData };
}
