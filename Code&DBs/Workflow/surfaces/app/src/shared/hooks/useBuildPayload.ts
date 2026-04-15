import { useState, useEffect, useCallback } from 'react';
import type { BuildPayload } from '../types';
import { loadWorkflowBuild, postBuildMutation } from '../buildController';
import { useBuildEvents } from './useBuildEvents';

export function useBuildPayload(workflowId: string | null) {
  const [payload, setPayload] = useState<BuildPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { latestEvent } = useBuildEvents(workflowId);

  const load = useCallback(async () => {
    if (!workflowId) return;
    setLoading(true);
    setError(null);
    try {
      const data = await loadWorkflowBuild(workflowId);
      setPayload(data);
    } catch (e: any) {
      setError(e.message || 'Failed to load build');
    } finally {
      setLoading(false);
    }
  }, [workflowId]);

  // Initial load
  useEffect(() => {
    load();
  }, [load]);

  // Reload on every event from the service bus
  useEffect(() => {
    if (latestEvent) load();
  }, [latestEvent, load]);

  const mutate = useCallback(async (subpath: string, body: Record<string, unknown>) => {
    if (!workflowId) return;
    try {
      const result = await postBuildMutation(workflowId, subpath, body);
      setPayload(result);
      return result;
    } catch (e: any) {
      setError(e.message || 'Mutation failed');
      throw e;
    }
  }, [workflowId]);

  return { payload, loading, error, mutate, reload: load, setPayload };
}
