/**
 * Lazy mode-stylesheet loader for the React app.
 *
 * The default firmware + lite stylesheets are bundled with main.tsx.
 * The print + high-contrast stylesheets are mounted only when the
 * toggle activates them — saves ~50 KB on first paint for the
 * majority case where the user stays in firmware or lite.
 *
 * Usage in main.tsx:
 *   import { applyMode } from '@/primitives-prx';
 *   applyMode(localStorage.getItem('prx-mode') ?? 'firmware');
 *
 * Usage on toggle click:
 *   import { applyMode } from '@/primitives-prx';
 *   applyMode(nextMode);
 */

export type Mode = 'firmware' | 'lite' | 'print' | 'high-contrast';

const MODE_STYLESHEET_HREFS: Partial<Record<Mode, string>> = {
  print: '/styles/primitives-print.css',
  'high-contrast': '/styles/primitives-hicontrast.css',
};

const loaded: Set<Mode> = new Set();

function ensureLoaded(mode: Mode): Promise<void> {
  if (loaded.has(mode)) return Promise.resolve();
  const href = MODE_STYLESHEET_HREFS[mode];
  if (!href) {
    loaded.add(mode);
    return Promise.resolve();
  }
  if (typeof document === 'undefined') return Promise.resolve();
  return new Promise((resolve, reject) => {
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = href;
    link.dataset.prxMode = mode;
    link.addEventListener('load', () => {
      loaded.add(mode);
      resolve();
    });
    link.addEventListener('error', () => reject(new Error(`failed to load ${href}`)));
    document.head.appendChild(link);
  });
}

const MODE_CLASSES: Mode[] = ['lite', 'print', 'high-contrast'];

/**
 * Apply a mode. Lazy-loads the stylesheet for print/high-contrast on
 * first activation. Idempotent — calling with the current mode is a no-op.
 */
export async function applyMode(mode: Mode): Promise<void> {
  if (typeof document === 'undefined') return;
  if (mode !== 'firmware') {
    await ensureLoaded(mode);
  }
  MODE_CLASSES.forEach((m) => {
    if (m === mode) document.body.classList.add(m);
    else document.body.classList.remove(m);
  });
  try {
    localStorage.setItem('prx-mode', mode);
  } catch {
    // localStorage may be disabled — non-fatal
  }
}

export function currentMode(): Mode {
  if (typeof document === 'undefined') return 'firmware';
  const cls = document.body.classList;
  if (cls.contains('high-contrast')) return 'high-contrast';
  if (cls.contains('print')) return 'print';
  if (cls.contains('lite')) return 'lite';
  return 'firmware';
}

/** Returns the next mode in the canonical cycle. */
export function nextMode(mode: Mode = currentMode()): Mode {
  const order: Mode[] = ['firmware', 'lite', 'print', 'high-contrast'];
  const idx = order.indexOf(mode);
  return order[(idx + 1) % order.length];
}
