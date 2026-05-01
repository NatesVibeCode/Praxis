import React, { useEffect, useMemo, useState } from 'react';

import type { IdempotencyPolicy } from './types';
export type { IdempotencyPolicy } from './types';

export interface DispatchEvent {
  op: string;
  payload: Record<string, unknown>;
  hash: string;
  dry: boolean;
  idemPolicy: IdempotencyPolicy;
  cacheHit: boolean;
}

export interface DispatchButtonProps {
  op: string;
  payload: Record<string, unknown>;
  idempotencyPolicy: IdempotencyPolicy;
  /** Fires when the user clicks dispatch. The handler is responsible for any actual gateway call. */
  onDispatch?: (event: DispatchEvent) => void;
  /** Override the default labels by op kind. */
  label?: { ready?: string; dry?: string };
  /** Disable the button. The chevron `▸` suffix appears when this opens a drawer/popover instead of firing immediately. */
  disabled?: boolean;
  /** When true, append a `▸` chevron to indicate this opens a drawer instead of firing immediately. */
  opensDrawer?: boolean;
  /** Tooltip shown when disabled, via [data-tip]. */
  disabledReason?: string;
}

async function sha256Short(s: string): Promise<string> {
  if (typeof window === 'undefined' || !window.crypto?.subtle) return 'sha256:unavailable';
  try {
    const buf = await window.crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
    return (
      'sha256:' +
      Array.from(new Uint8Array(buf))
        .slice(0, 8)
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('')
    );
  } catch {
    return 'sha256:unavailable';
  }
}

function canonicalize(payload: unknown): string {
  // stable stringify — sort object keys
  return JSON.stringify(payload, Object.keys(payload as Record<string, unknown> ?? {}).sort());
}

/**
 * DispatchButton — universal "run through gateway" widget.
 * Renders the prx-dispatch row: op label · live SHA-256 hash · cache
 * hit/miss indicator · dry-run toggle · run button.
 *
 * Stateful: tracks seen hashes to flip the cache indicator on replay,
 * computes the live hash via SubtleCrypto. The `onDispatch` handler is
 * the integration seam — pass it the actual gateway dispatch.
 */
export function DispatchButton({
  op,
  payload,
  idempotencyPolicy,
  onDispatch,
  label,
  disabled,
  opensDrawer,
  disabledReason,
}: DispatchButtonProps) {
  const [hash, setHash] = useState<string>('sha256:—');
  const [seen] = useState<Set<string>>(() => new Set());
  const [cacheHit, setCacheHit] = useState<boolean>(false);
  const [dry, setDry] = useState<boolean>(false);

  const canon = useMemo(() => canonicalize(payload), [payload]);

  useEffect(() => {
    let cancelled = false;
    sha256Short(canon).then((h) => {
      if (cancelled) return;
      setHash(h);
      const replayable = idempotencyPolicy === 'read_only' || idempotencyPolicy === 'idempotent';
      setCacheHit(replayable && seen.has(h));
    });
    return () => {
      cancelled = true;
    };
  }, [canon, idempotencyPolicy, seen]);

  const readyLabel = label?.ready ?? (idempotencyPolicy === 'read_only' ? '↻ read' : '› dispatch');
  const dryLabel = label?.dry ?? '› dry-run';
  const chevron = opensDrawer ? ' ▸' : '';

  function handleClick() {
    seen.add(hash);
    setCacheHit(idempotencyPolicy === 'read_only' || idempotencyPolicy === 'idempotent');
    onDispatch?.({
      op,
      payload,
      hash,
      dry,
      idemPolicy: idempotencyPolicy,
      cacheHit,
    });
  }

  return (
    <div className="prx-dispatch" data-testid="prx-dispatch" data-op={op}>
      <span className="op">{op}</span>
      <span className="hash">
        payload <span className="h">{hash}</span>
      </span>
      <span className="replay" data-state={cacheHit ? 'hit' : 'miss'}>
        {cacheHit ? 'cache hit' : 'cache miss'}
      </span>
      <label className="dry">
        <input
          type="checkbox"
          checked={dry}
          onChange={(e) => setDry(e.target.checked)}
          data-testid="prx-dispatch-dry"
        />
        dry-run
      </label>
      <button
        type="button"
        className={'run' + (dry ? ' dry' : '')}
        onClick={handleClick}
        disabled={disabled}
        data-tip={disabled ? disabledReason : undefined}
        data-testid="prx-dispatch-run"
      >
        {(dry ? dryLabel : readyLabel) + chevron}
      </button>
    </div>
  );
}
