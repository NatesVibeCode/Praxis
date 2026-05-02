import React from 'react';
import ReactDOM from 'react-dom/client';
import { App } from './App';
import './modules';
import './styles/tokens.css';
import './styles/primitives.css';
import './styles/primitives-ext.css';
import { installTelemetry, PrimitiveUsageOverlay } from './primitives-prx';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
    <PrimitiveUsageOverlay />
  </React.StrictMode>,
);

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => undefined);
  });
}

// Frontend primitive telemetry — every prx:* custom event is POSTed to
// /api/ui/telemetry which appends to ~/.praxis/ui-telemetry.jsonl on the
// API server. Inspect via:
//   curl http://localhost:8420/api/ui/telemetry/recent?limit=50 | jq .
// or open the floating panel in-app (Shift+T or click the ⚙ button bottom-right).
installTelemetry({
  forward: (payload) => {
    try {
      const body = JSON.stringify(payload);
      if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
        const blob = new Blob([body], { type: 'application/json' });
        if (navigator.sendBeacon('/api/ui/telemetry', blob)) return;
      }
      fetch('/api/ui/telemetry', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        keepalive: true,
      }).catch(() => undefined);
    } catch {
      // never let telemetry break the host page
    }
  },
});
