import React, { useEffect, useState } from 'react';
import { PrimitiveUsagePanel } from './PrimitiveUsagePanel';

interface PrimitiveUsageOverlayProps {
  /** Default visibility. Toggle with Shift+T (configurable). Default false. */
  defaultOpen?: boolean;
  /** Hotkey letter (held with Shift). Default 't'. Set null to disable. */
  hotkey?: string | null;
  /** Persist open state across reloads via localStorage. Default true. */
  persist?: boolean;
}

/**
 * Toggleable fixed-position wrapper around PrimitiveUsagePanel.
 * Mounts a floating launcher button bottom-right; click or press Shift+T
 * to open the live telemetry panel. Persists open state to localStorage.
 */
export function PrimitiveUsageOverlay({
  defaultOpen = false,
  hotkey = 't',
  persist = true,
}: PrimitiveUsageOverlayProps) {
  const KEY = 'prx-telemetry-open';
  const [open, setOpen] = useState<boolean>(() => {
    if (!persist || typeof localStorage === 'undefined') return defaultOpen;
    try {
      const v = localStorage.getItem(KEY);
      return v === null ? defaultOpen : v === '1';
    } catch {
      return defaultOpen;
    }
  });

  useEffect(() => {
    if (persist && typeof localStorage !== 'undefined') {
      try { localStorage.setItem(KEY, open ? '1' : '0'); } catch { /* ignore */ }
    }
  }, [open, persist]);

  useEffect(() => {
    if (!hotkey) return;
    const handler = (e: KeyboardEvent) => {
      if (!e.shiftKey || e.metaKey || e.ctrlKey || e.altKey) return;
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || target?.isContentEditable) return;
      if ((e.key || '').toLowerCase() === hotkey.toLowerCase()) {
        e.preventDefault();
        setOpen((v) => !v);
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [hotkey]);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        aria-label={open ? 'Close telemetry panel' : 'Open telemetry panel'}
        aria-expanded={open}
        title={hotkey ? `Toggle telemetry (Shift+${hotkey.toUpperCase()})` : 'Toggle telemetry'}
        style={{
          position: 'fixed',
          bottom: 16,
          right: open ? 504 : 16,
          zIndex: 201,
          width: 36,
          height: 36,
          borderRadius: 18,
          background: open ? 'var(--accent)' : 'var(--bg-card)',
          color: open ? 'var(--text-inverse)' : 'var(--text)',
          border: '1px solid var(--border)',
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 14,
          cursor: 'pointer',
          boxShadow: '0 4px 16px rgba(0,0,0,0.32)',
          transition: 'right 200ms ease, background 120ms ease',
        }}
      >
        ⚙
      </button>
      {open && <PrimitiveUsagePanel />}
    </>
  );
}
