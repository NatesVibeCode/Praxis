/**
 * Per-browser-tab session aggregate UUID.
 *
 * Stored in sessionStorage under praxis.shell.session_aggregate_ref so the
 * ui_shell_state.live projection scopes ShellState per tab. Generated lazily
 * on first read; the bootstrap dispatch is the caller's responsibility (see
 * useShellState.ts).
 *
 * Anchored to decision.shell_navigation_cqrs.20260426.
 */

const STORAGE_KEY = 'praxis.shell.session_aggregate_ref';

function generateUuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  // Fallback for older environments — RFC 4122 v4-ish.
  const bytes = new Uint8Array(16);
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    crypto.getRandomValues(bytes);
  } else {
    for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export interface SessionAggregateState {
  sessionAggregateRef: string;
  isFresh: boolean;
}

export function getOrCreateSessionAggregate(): SessionAggregateState {
  if (typeof window === 'undefined' || typeof window.sessionStorage === 'undefined') {
    return { sessionAggregateRef: generateUuid(), isFresh: true };
  }
  const existing = window.sessionStorage.getItem(STORAGE_KEY);
  if (existing && existing.trim()) {
    return { sessionAggregateRef: existing.trim(), isFresh: false };
  }
  const next = generateUuid();
  window.sessionStorage.setItem(STORAGE_KEY, next);
  return { sessionAggregateRef: next, isFresh: true };
}

export function clearSessionAggregateForTest(): void {
  if (typeof window !== 'undefined' && window.sessionStorage) {
    window.sessionStorage.removeItem(STORAGE_KEY);
  }
}
