/**
 * Single source of truth for job/run status → visual state mapping.
 *
 * Moon's visual language is monochrome + coral-for-failure; status is
 * communicated through fill density and motion, not hue. Every surface that
 * shows a running/idle/succeeded/failed job (run overlay, run panel, dashboard
 * thumbnails, launcher nucleus satellites) routes its raw status string
 * through this mapping so the five visual states stay consistent.
 *
 * Keep this file the ONLY translator. If a new raw status appears (e.g.
 * `retrying`), add it here rather than mapping it in-place at the callsite.
 */

export type MoonStatusState =
  | 'pending'   // queued, not yet noticed
  | 'idle'      // ready, cancelled — quiet terminal or waiting its turn
  | 'active'    // running, claimed — motion, no hue shift
  | 'ok'        // succeeded — ring locks to solid white
  | 'failed';   // failed, blocked, dead_letter, parent_failed — coral

/** Raw job/run statuses emitted by the engine. */
export type RawStatus =
  | 'queued'
  | 'pending'
  | 'ready'
  | 'claimed'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'dead_letter'
  | 'blocked'
  | 'parent_failed'
  | 'cancelled'
  | 'loading'
  | string;

const STATE_MAP: Record<string, MoonStatusState> = {
  queued: 'pending',
  pending: 'pending',
  loading: 'pending',
  ready: 'idle',
  cancelled: 'idle',
  claimed: 'active',
  running: 'active',
  succeeded: 'ok',
  failed: 'failed',
  rejected: 'failed',
  refused: 'failed',
  dead_letter: 'failed',
  blocked: 'failed',
  parent_failed: 'failed',
};

export function statusState(raw: RawStatus | null | undefined): MoonStatusState {
  if (!raw) return 'pending';
  return STATE_MAP[raw] ?? 'pending';
}

/**
 * CSS var driving the primary stroke/fill for a given state. These resolve in
 * moon-tokens.css — consumers never hardcode a hex.
 */
export function statusStrokeVar(state: MoonStatusState): string {
  switch (state) {
    case 'failed':
      return 'var(--moon-status-failed)';
    case 'ok':
      return 'var(--moon-status-ok)';
    case 'active':
      return 'var(--moon-status-active)';
    case 'idle':
      return 'var(--moon-status-idle)';
    case 'pending':
    default:
      return 'var(--moon-status-pending)';
  }
}

/** Low-opacity companion for halo/background tints. */
export function statusSoftVar(state: MoonStatusState): string {
  switch (state) {
    case 'failed':
      return 'var(--moon-status-failed-soft)';
    case 'ok':
      return 'var(--moon-status-ok-soft)';
    case 'active':
      return 'var(--moon-status-active-soft)';
    case 'idle':
      return 'var(--moon-status-idle-soft)';
    case 'pending':
    default:
      return 'var(--moon-status-pending-soft)';
  }
}

/** Human label for badges/pills. */
export function statusLabel(state: MoonStatusState): string {
  switch (state) {
    case 'failed':
      return 'failed';
    case 'ok':
      return 'done';
    case 'active':
      return 'running';
    case 'idle':
      return 'idle';
    case 'pending':
    default:
      return 'pending';
  }
}

export const TERMINAL_STATES: ReadonlySet<MoonStatusState> = new Set(['ok', 'failed', 'idle']);
