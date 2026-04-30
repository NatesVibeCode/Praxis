import { useState, useEffect, useCallback, useRef } from 'react';
import type { BuildPayload } from '../types';
import { loadWorkflowBuild, postBuildMutation } from '../buildController';
import { useBuildEvents } from './useBuildEvents';
import { isAbortError } from '../request';

function isRetryableBuildError(message: string | null): boolean {
  if (!message) return false;
  return !/\b(400|401|403|404)\b|not found/i.test(message);
}

export function useBuildPayload(workflowId: string | null) {
  const [payload, setPayload] = useState<BuildPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { latestEvent } = useBuildEvents(workflowId);
  const loadAbortRef = useRef<AbortController | null>(null);
  const requestSeqRef = useRef(0);

  const load = useCallback(async () => {
    loadAbortRef.current?.abort();
    if (!workflowId) {
      loadAbortRef.current = null;
      requestSeqRef.current += 1;
      setPayload(null);
      setLoading(false);
      setError(null);
      return;
    }
    const controller = new AbortController();
    loadAbortRef.current = controller;
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    const isCurrent = () =>
      requestSeqRef.current === requestSeq
      && loadAbortRef.current === controller
      && !controller.signal.aborted;
    setLoading(true);
    setError(null);
    try {
      const data = await loadWorkflowBuild(workflowId, { signal: controller.signal });
      if (!isCurrent()) return;
      setPayload(data);
    } catch (e: any) {
      if (!isCurrent() || isAbortError(e)) return;
      setError(e.message || 'Failed to load build');
    } finally {
      if (isCurrent()) {
        setLoading(false);
      }
      if (loadAbortRef.current === controller) {
        loadAbortRef.current = null;
      }
    }
  }, [workflowId]);

  // Initial load
  useEffect(() => {
    load();
    return () => {
      loadAbortRef.current?.abort();
    };
  }, [load]);

  // Reload on every event from the service bus
  useEffect(() => {
    if (latestEvent) load();
  }, [latestEvent, load]);

  useEffect(() => {
    if (!workflowId || !error || !isRetryableBuildError(error)) return undefined;
    const timer = window.setTimeout(() => {
      void load();
    }, 2500);
    return () => window.clearTimeout(timer);
  }, [error, load, workflowId]);

  const mutate = useCallback(async (subpath: string, body: Record<string, unknown>) => {
    if (!workflowId) return;
    loadAbortRef.current?.abort();
    loadAbortRef.current = null;
    setLoading(false);
    const requestSeq = requestSeqRef.current + 1;
    requestSeqRef.current = requestSeq;
    try {
      const result = await postBuildMutation(workflowId, subpath, body);
      if (requestSeqRef.current !== requestSeq) return result;
      setPayload(result);
      return result;
    } catch (e: any) {
      if (requestSeqRef.current !== requestSeq) throw e;
      setError(e.message || 'Mutation failed');
      throw e;
    }
  }, [workflowId]);

  return { payload, loading, error, mutate, reload: load, setPayload };
}
