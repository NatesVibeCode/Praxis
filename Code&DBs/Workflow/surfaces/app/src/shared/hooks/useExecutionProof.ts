import { useEffect, useRef, useState } from 'react';
import { ExecutionProof, executionProofPath } from '../../dashboard/proofApi';

export type ExecutionProofStatus = 'idle' | 'loading' | 'ready' | 'error';

export interface UseExecutionProofResult {
  proof: ExecutionProof | null;
  status: ExecutionProofStatus;
  error: string | null;
  refresh: () => void;
}

/**
 * Fetch the run's 4-authority execution proof.
 *
 * Polls every `revalidateMs` (default 30s) while the run is still considered
 * live (`shouldRefresh=true`). When the run is terminal, polling stops — the
 * proof is frozen and the caller can manually `refresh()` if they want.
 *
 * The fetch goes through the project's `window.fetch` patch which injects
 * the `X-Praxis-UI: 1` header — no manual header wiring needed here.
 */
export function useExecutionProof(
  runId: string | null | undefined,
  options: { shouldRefresh?: boolean; revalidateMs?: number } = {},
): UseExecutionProofResult {
  const { shouldRefresh = true, revalidateMs = 30000 } = options;
  const [proof, setProof] = useState<ExecutionProof | null>(null);
  const [status, setStatus] = useState<ExecutionProofStatus>('idle');
  const [error, setError] = useState<string | null>(null);
  const refreshTokenRef = useRef(0);

  const doFetch = (token: number) => {
    if (!runId) return;
    setStatus((prev) => (prev === 'ready' ? 'ready' : 'loading'));
    fetch(executionProofPath(runId))
      .then(async (res) => {
        if (!res.ok) {
          let msg = `HTTP ${res.status}`;
          try {
            const body = await res.json();
            msg = (body && (body.error || body.detail?.error || msg)) as string;
          } catch {
            // body wasn't JSON
          }
          throw new Error(msg);
        }
        return res.json();
      })
      .then((data: ExecutionProof) => {
        if (token !== refreshTokenRef.current) return; // staler request
        setProof(data);
        setStatus('ready');
        setError(null);
      })
      .catch((err: unknown) => {
        if (token !== refreshTokenRef.current) return;
        setError(err instanceof Error ? err.message : String(err));
        setStatus('error');
      });
  };

  useEffect(() => {
    if (!runId) {
      setProof(null);
      setStatus('idle');
      setError(null);
      return;
    }
    const token = ++refreshTokenRef.current;
    doFetch(token);

    if (!shouldRefresh) return;
    const interval = window.setInterval(() => {
      doFetch(++refreshTokenRef.current);
    }, Math.max(5000, revalidateMs));
    return () => {
      window.clearInterval(interval);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, shouldRefresh, revalidateMs]);

  const refresh = () => {
    doFetch(++refreshTokenRef.current);
  };

  return { proof, status, error, refresh };
}
