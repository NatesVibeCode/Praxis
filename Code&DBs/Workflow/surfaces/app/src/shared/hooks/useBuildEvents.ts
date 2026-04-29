import { useState, useEffect, useRef } from 'react';
import type { BuildEvent } from '../types';

export function useBuildEvents(workflowId: string | null) {
  const [events, setEvents] = useState<BuildEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const generationRef = useRef(0);
  const retryDelay = useRef(1000);

  useEffect(() => {
    generationRef.current += 1;
    const generation = generationRef.current;
    setEvents([]);
    setConnected(false);
    setError(null);
    retryDelay.current = 1000;

    const clearReconnect = () => {
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const closeCurrent = () => {
      esRef.current?.close();
      esRef.current = null;
    };

    if (!workflowId || typeof window.EventSource !== 'function') {
      clearReconnect();
      closeCurrent();
      return () => {
        generationRef.current += 1;
        clearReconnect();
        closeCurrent();
      };
    }

    const isCurrent = (es: EventSource) => generationRef.current === generation && esRef.current === es;

    const connect = () => {
      if (generationRef.current !== generation) return;
      clearReconnect();
      const url = `/api/workflows/${workflowId}/build/stream`;
      const es = new EventSource(url);
      esRef.current = es;

      es.onopen = () => {
        if (!isCurrent(es)) return;
        setConnected(true);
        setError(null);
        retryDelay.current = 1000;
      };

      es.onmessage = (e) => {
        if (!isCurrent(es)) return;
        try {
          const event: BuildEvent = JSON.parse(e.data);
          setEvents((prev) => [...prev, event]);
        } catch {
          // ignore malformed events
        }
      };

      es.addEventListener('done', () => {
        if (!isCurrent(es)) return;
        es.close();
        esRef.current = null;
        setConnected(false);
      });

      es.onerror = () => {
        if (!isCurrent(es)) return;
        es.close();
        esRef.current = null;
        setConnected(false);
        setError('Connection lost');
        const delay = retryDelay.current;
        retryDelay.current = Math.min(delay * 2, 30000);
        reconnectTimerRef.current = window.setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      generationRef.current += 1;
      clearReconnect();
      closeCurrent();
    };
  }, [workflowId]);

  return {
    events,
    latestEvent: events.length > 0 ? events[events.length - 1] : null,
    connected,
    error,
  };
}
