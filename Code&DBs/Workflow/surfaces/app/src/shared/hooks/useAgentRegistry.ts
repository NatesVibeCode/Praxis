import { useCallback, useEffect, useState } from 'react';
import { AgentRegistryRow } from '../types';

export function useAgentRegistry() {
  const [agents, setAgents] = useState<AgentRegistryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch('/api/agent_principals?status=any&limit=100');
      let body: any = null;
      try {
        body = await resp.json();
      } catch {
        body = null;
      }
      if (!resp.ok) throw new Error(body?.error || body?.detail || `HTTP ${resp.status}`);
      if (Array.isArray(body?.principals)) {
        setAgents(body.principals);
      } else {
        setAgents([]);
      }
    } catch (e: any) {
      setError(e?.message || 'Failed to load agents');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    reload();
  }, [reload]);

  return { agents, loading, error, reload };
}
