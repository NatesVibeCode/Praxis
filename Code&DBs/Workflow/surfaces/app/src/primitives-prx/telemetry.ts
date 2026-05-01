/**
 * Frontend primitive telemetry bridge.
 *
 * Subscribes to the `prx:*` custom-event bus and dispatches each as a
 * `frontend.primitive.event` operation through the gateway. Gives the
 * design-system owner a usage signal: which primitives fire, how often,
 * which are dead weight.
 *
 * Until the gateway endpoint is wired, the dispatch is a console log.
 * Replace `forwardToGateway` with a fetch to the operation_catalog_gateway
 * once the operation is registered.
 *
 * Standing-order row: telemetry::frontend-primitive-events-via-gateway
 */

const TRACKED_EVENTS = [
  'prx:row-select',
  'prx:dispatch',
  'prx:wizard-submit',
  'prx:tool-run',
  'prx:cmd-run',
  'prx:tab-select',
  'prx:flow-node-select',
  'prx:step-run',
  'prx:transport',
  'prx:workflow-control',
  'prx:prompt-ref-insert',
  'prx:prompt-change',
  'prx:form-change',
] as const;

export type TrackedEventName = typeof TRACKED_EVENTS[number];

export interface PrimitiveTelemetryPayload {
  event_name: TrackedEventName;
  surface_id: string | null;
  detail: unknown;
  ts: string;
  /** Mode at the time of the event (firmware / lite / print / high-contrast) */
  mode: 'firmware' | 'lite' | 'print' | 'high-contrast';
}

type Forwarder = (payload: PrimitiveTelemetryPayload) => void;

const defaultForwarder: Forwarder = (payload) => {
  // Dev/showcase fallback: log to console. Replace with a gateway dispatch
  // (POST /v1/operations/frontend.primitive.event) once the op is registered.
  if (typeof console !== 'undefined' && typeof console.debug === 'function') {
    console.debug('[prx-telemetry]', payload.event_name, payload);
  }
};

let forwarder: Forwarder = defaultForwarder;
let installed = false;
let listener: ((e: Event) => void) | null = null;

function detectMode(): PrimitiveTelemetryPayload['mode'] {
  if (typeof document === 'undefined') return 'firmware';
  const cls = document.body.classList;
  if (cls.contains('high-contrast')) return 'high-contrast';
  if (cls.contains('print')) return 'print';
  if (cls.contains('lite')) return 'lite';
  return 'firmware';
}

function detectSurfaceId(target: EventTarget | null): string | null {
  if (!target || !(target instanceof Element)) return null;
  // Walk up to the nearest id'd ancestor as the "surface"
  let cur: Element | null = target;
  while (cur) {
    if (cur.id) return cur.id;
    cur = cur.parentElement;
  }
  return null;
}

/**
 * Install the telemetry bridge. Idempotent — calling twice is a no-op.
 */
export function installTelemetry(opts: { forward?: Forwarder } = {}): () => void {
  if (installed) return uninstallTelemetry;
  if (opts.forward) forwarder = opts.forward;
  if (typeof document === 'undefined') return uninstallTelemetry;

  listener = (e: Event) => {
    const ce = e as CustomEvent;
    const name = e.type as TrackedEventName;
    const payload: PrimitiveTelemetryPayload = {
      event_name: name,
      surface_id: detectSurfaceId(e.target),
      detail: ce.detail,
      ts: new Date().toISOString(),
      mode: detectMode(),
    };
    try {
      forwarder(payload);
    } catch {
      // never let telemetry break the host page
    }
  };

  TRACKED_EVENTS.forEach((evt) => document.addEventListener(evt, listener as EventListener));
  installed = true;
  return uninstallTelemetry;
}

export function uninstallTelemetry(): void {
  if (!installed || !listener) return;
  TRACKED_EVENTS.forEach((evt) => document.removeEventListener(evt, listener as EventListener));
  installed = false;
  listener = null;
}

/**
 * Override the default forwarder. Call before installTelemetry, or call
 * installTelemetry({ forward }) directly.
 */
export function setForwarder(fn: Forwarder): void {
  forwarder = fn;
}

export const _trackedEvents = TRACKED_EVENTS;
