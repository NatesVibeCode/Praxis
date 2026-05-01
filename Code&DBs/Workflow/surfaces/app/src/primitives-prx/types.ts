/**
 * Const-asserted enums + typed-data-attr helpers.
 *
 * The `prx-*` CSS classes carry signal in `data-*` attributes (data-tone,
 * data-source, data-state, etc.). Without compile-time enums consumers can
 * write `<span className="prx-chip" data-source="legder">` (typo) and
 * TypeScript happily passes it.
 *
 * These helpers give raw-className consumers the same type safety the
 * React adapters get, without requiring a wrapper component.
 *
 *   import { chipProps, gateProps } from '@/primitives-prx';
 *   <span {...chipProps({ source: 'ledger', tone: 'locked' })}>customer.email</span>
 *
 * Mirrors the values defined in primitives.css. If you add a new variant
 * to the CSS, add it here too.
 */

// ── Enums (const-asserted so TypeScript narrows to the literal union) ──

export const SOURCE_KINDS = ['ledger', 'user-input', 'decision', 'external', 'derived', 'redacted'] as const;
export type SourceKind = typeof SOURCE_KINDS[number];

export const TONES = ['read', 'write', 'locked', 'ok', 'bad'] as const;
export type ChipTone = typeof TONES[number];

export const STATUS_TONES = ['ok', 'warn', 'err', 'dim'] as const;
export type StatusTone = typeof STATUS_TONES[number];

export const GATE_STATES = ['pending', 'approved', 'held', 'refused'] as const;
export type GateState = typeof GATE_STATES[number];

export const RECEIPT_STATES = ['sealed', 'ok', 'refused', 'verify'] as const;
export type ReceiptState = typeof RECEIPT_STATES[number];

export const FLOW_NODE_STATES = ['ok', 'cur', 'err', 'held', 'pending', 'placeholder'] as const;
export type FlowNodeState = typeof FLOW_NODE_STATES[number];

export const LED_TONES = ['live', 'ok', 'err', 'idle'] as const;
export type LedTone = typeof LED_TONES[number];

export const SPINNER_SETS = ['braille', 'quadrant', 'dot', 'bar'] as const;
export type SpinnerSet = typeof SPINNER_SETS[number];

export const ICON_SIZES = ['sm', 'md', 'lg'] as const;
export type IconSize = typeof ICON_SIZES[number];

export const ICON_TONES = ['default', 'ok', 'warn', 'err'] as const;
export type IconTone = typeof ICON_TONES[number];

// ── Typed-data-attr helpers ───────────────────────────────────────────

type ChipPropsInput = { source?: SourceKind; tone?: ChipTone };
export function chipProps(opts: ChipPropsInput = {}) {
  const out: { className: string; 'data-source'?: SourceKind; 'data-tone'?: ChipTone } = {
    className: 'prx-chip',
  };
  if (opts.source) out['data-source'] = opts.source;
  if (opts.tone) out['data-tone'] = opts.tone;
  return out;
}

export function gateProps(state: GateState) {
  return { className: 'prx-gate', 'data-state': state } as const;
}

export function receiptProps(state: ReceiptState) {
  return { className: 'prx-receipt', 'data-state': state } as const;
}

export function flowNodeProps(state: FlowNodeState, num?: number) {
  const out: {
    className: string;
    'data-state': FlowNodeState;
    'data-num'?: string;
  } = { className: 'prx-flow-node', 'data-state': state };
  if (state === 'placeholder' && num !== undefined) {
    out['data-num'] = String(num);
  }
  return out;
}

export function ledProps(tone: LedTone) {
  return { className: 'prx-led', 'data-tone': tone } as const;
}

export function statusCapProps(tone: StatusTone) {
  return { className: 'stat-cap', 'data-tone': tone } as const;
}

export function iconTileProps(opts: { size?: IconSize; tone?: IconTone } = {}) {
  const out: { className: string; 'data-size'?: IconSize; 'data-tone'?: IconTone } = {
    className: 'prx-icon-tile',
  };
  if (opts.size && opts.size !== 'md') out['data-size'] = opts.size;
  if (opts.tone && opts.tone !== 'default') out['data-tone'] = opts.tone;
  return out;
}

// ── Catalog-bound enums (manually mirrored — TODO: generate from migrations) ──
// Until the generated catalog-types.gen.ts ships, these are mirrored by hand
// from operation_catalog_registry constraints. Keep in sync with:
//   Code&DBs/Databases/migrations/workflow/*.sql

export const OPERATION_KINDS = ['query', 'command', 'walk', 'analytics'] as const;
export type OperationKind = typeof OPERATION_KINDS[number];

export const IDEMPOTENCY_POLICIES = ['read_only', 'idempotent', 'none'] as const;
export type IdempotencyPolicy = typeof IDEMPOTENCY_POLICIES[number];
