/**
 * useShellState — projection-driven shell state hook.
 *
 * Replaces useState<ShellState> ownership in App.tsx. On mount:
 *   1. Resolves the per-tab session_aggregate_ref via sessionAggregate.ts.
 *   2. Loads the route registry via routeRegistry.ts.
 *   3. If the session is fresh, fires shell.session.bootstrapped with the
 *      deep-link parsed from window.location.
 *   4. Fetches initial state from /api/projections/ui_shell_state.live.
 *   5. Subscribes to /api/shell/state/stream (SSE) and reconciles each event.
 *
 * The returned dispatch wraps dispatchShellNavigation AND optimistically
 * applies the diff to local state so UI updates are sub-frame; the SSE
 * reconcile step replaces local state on mismatch (server projection wins).
 *
 * Anchored to decision.shell_navigation_cqrs.20260426.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  dispatchShellNavigation,
  type DispatchResult,
  type ShellNavigationOperation,
} from './dispatchNavigation';
import { getOrCreateSessionAggregate } from './sessionAggregate';
import {
  buildPath,
  buildPathForSurface,
  loadRoutes,
  matchPath,
  type RouteRegistryRow,
} from './routeRegistry';
import {
  createDefaultShellState,
  type ShellState,
} from './state';

export interface UseShellStateResult {
  state: ShellState;
  routes: RouteRegistryRow[];
  sessionAggregateRef: string;
  ready: boolean;
  dispatch: (
    operation: ShellNavigationOperation,
    input: Record<string, unknown>,
    optimisticDiff?: Partial<ShellState>,
  ) => Promise<DispatchResult>;
}

interface ProjectionEnvelope {
  output: ShellState | null;
  last_event_id: string | null;
  freshness_status: string;
}

interface StreamEvent {
  event_type: string;
  event_id: string | null;
  payload: Record<string, unknown>;
}

function applyDiff(state: ShellState, diff: Partial<ShellState> | undefined): ShellState {
  if (!diff) return state;
  return { ...state, ...diff };
}

function inferActiveTabFromRoute(routeId: string, routes: RouteRegistryRow[]): string | null {
  const row = routes.find((r) => r.route_id === routeId);
  if (!row) return null;
  if (row.is_dynamic) return null;
  return row.surface_name;
}

function resolveDeepLink(routes: RouteRegistryRow[]): {
  routeId: string;
  slotValues: Record<string, string | string[]>;
} {
  if (typeof window === 'undefined') {
    return { routeId: 'route.app.dashboard', slotValues: {} };
  }
  const match = matchPath(window.location.pathname, window.location.search);
  if (match) return { routeId: match.route_id, slotValues: match.slot_values };
  return { routeId: 'route.app.dashboard', slotValues: {} };
}

export function useShellState(): UseShellStateResult {
  const [routes, setRoutes] = useState<RouteRegistryRow[]>([]);
  const [state, setState] = useState<ShellState>(() => createDefaultShellState());
  const [sessionRef] = useState<string>(() => getOrCreateSessionAggregate().sessionAggregateRef);
  const [ready, setReady] = useState(false);
  const stateRef = useRef(state);
  const routesRef = useRef(routes);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);
  useEffect(() => {
    routesRef.current = routes;
  }, [routes]);

  // 1. Load registry + bootstrap session + fetch initial projection
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const loaded = await loadRoutes();
      if (cancelled) return;
      setRoutes(loaded);

      const { sessionAggregateRef: sid, isFresh } = getOrCreateSessionAggregate();
      const deepLink = resolveDeepLink(loaded);

      if (isFresh) {
        await dispatchShellNavigation({
          operation: 'shell.session.bootstrapped',
          input: {
            session_aggregate_ref: sid,
            initial_route_id: deepLink.routeId,
            deep_link_search: typeof window !== 'undefined' ? window.location.search : '',
          },
        });
      }

      try {
        const res = await fetch(`/api/projections/ui_shell_state.live?session=${encodeURIComponent(sid)}`);
        if (res.ok) {
          const env = (await res.json()) as ProjectionEnvelope;
          if (env.output && !cancelled) {
            setState((prev) => ({ ...prev, ...env.output }));
          }
        }
      } catch {
        // Projection unavailable — keep default state.
      }
      if (!cancelled) setReady(true);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // 2. SSE subscription
  useEffect(() => {
    if (!ready || typeof window === 'undefined' || typeof window.EventSource !== 'function') {
      return undefined;
    }
    const url = `/api/shell/state/stream?session=${encodeURIComponent(sessionRef)}`;
    const source = new window.EventSource(url);
    source.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data) as StreamEvent;
        applyStreamEvent(parsed);
      } catch {
        // Malformed event — ignore.
      }
    };
    source.onerror = () => {
      // EventSource auto-reconnects.
    };
    return () => {
      source.close();
    };
    function applyStreamEvent(ev: StreamEvent) {
      const payload = ev.payload || {};
      switch (ev.event_type) {
        case 'session.bootstrapped': {
          const initialRoute = String(payload.initial_route_id || 'route.app.dashboard');
          const next = createDefaultShellState();
          next.activeRouteId = initialRoute;
          const tab = inferActiveTabFromRoute(initialRoute, routesRef.current);
          if (tab) next.activeTabId = tab;
          setState(next);
          return;
        }
        case 'surface.opened': {
          const diff = (payload.shell_state_diff as Partial<ShellState>) || {};
          const routeId = String(payload.route_id || '');
          setState((prev) => {
            const merged = applyDiff(prev, diff);
            if (routeId) {
              merged.activeRouteId = routeId;
              if (!('activeTabId' in (diff || {}))) {
                const tab = inferActiveTabFromRoute(routeId, routesRef.current);
                if (tab) merged.activeTabId = tab;
              }
            }
            return merged;
          });
          return;
        }
        case 'tab.closed': {
          const dynamicTabId = String(payload.dynamic_tab_id || '');
          const fallback = String(payload.fallback_route_id || 'route.app.dashboard');
          setState((prev) => {
            const remaining = (prev.dynamicTabs || []).filter((t) => t.id !== dynamicTabId);
            if (prev.activeTabId !== dynamicTabId) {
              return { ...prev, dynamicTabs: remaining };
            }
            const tab = inferActiveTabFromRoute(fallback, routesRef.current);
            return {
              ...prev,
              dynamicTabs: remaining,
              activeRouteId: fallback,
              activeTabId: tab || 'dashboard',
            };
          });
          return;
        }
        // history.popped + draft.guard.consulted are analytic only.
        default:
          return;
      }
    }
  }, [ready, sessionRef]);

  // 3. Dispatch wrapper with optimistic apply + URL push
  const dispatch = useCallback(
    async (
      operation: ShellNavigationOperation,
      input: Record<string, unknown>,
      optimisticDiff?: Partial<ShellState>,
    ): Promise<DispatchResult> => {
      const enrichedInput = { session_aggregate_ref: sessionRef, ...input };

      if (optimisticDiff) {
        setState((prev) => applyDiff(prev, optimisticDiff));
      }

      // URL sync: surface.opened with route_id drives history.
      if (operation === 'shell.surface.opened') {
        const routeId = typeof input.route_id === 'string' ? input.route_id : null;
        const slotValues = (input.slot_values as Record<string, string | string[]>) || {};
        if (routeId && typeof window !== 'undefined') {
          const url = buildPath(routeId, slotValues);
          if (url && url !== window.location.pathname + window.location.search) {
            window.history.pushState({}, '', url);
          }
        }
      }

      return dispatchShellNavigation({ operation, input: enrichedInput });
    },
    [sessionRef],
  );

  return { state, routes, sessionAggregateRef: sessionRef, ready, dispatch };
}

export { buildPath, buildPathForSurface };
