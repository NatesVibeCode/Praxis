import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { BindingLedgerEntry, BindingTarget, BuildPayload } from '../shared/types';
import type { AuthorityActionMeta } from './MoonNodeDetail';

/**
 * MoonBindingReviewQueue — batch review for unresolved binding_ledger entries.
 *
 * Compile produces one suggested candidate per slug (csv-ingestion-agent →
 * task_type_routing:auto/build, #doc-type-document/properties.email →
 * #doc-type-document/properties-email, etc.). Build_state stays "blocked"
 * until each binding is explicitly approved. Per the autonomous-first
 * standing order this should not require per-node clicking through the
 * inspector — the user wants to skim the list and approve in bulk.
 *
 * UX:
 *   - Single panel showing every unresolved binding with its suggested target.
 *   - "Approve all suggested" fires one review_decisions POST per binding in
 *     parallel, then closes the queue when 0 remain.
 *   - Per-row Approve / Skip buttons for the cases where one suggestion is wrong.
 *   - Keyboard: ↑/↓ moves focus, Enter approves the focused row, X skips.
 *
 * Dispatch shape mirrors MoonNodeDetail.tsx (target_kind='binding',
 * target_ref=binding_id, decision='approve'/'reject') so the same
 * /api/workflows/{id}/build/review_decisions sink handles both paths.
 */

interface Props {
  payload: BuildPayload | null;
  onCommitAuthorityAction: (
    subpath: string,
    body: Record<string, unknown>,
    meta: AuthorityActionMeta,
  ) => Promise<void>;
  onClose: () => void;
}

interface PendingBinding {
  binding: BindingLedgerEntry;
  topCandidate: BindingTarget;
}

function bindingsToReview(payload: BuildPayload | null): PendingBinding[] {
  if (!payload) return [];
  const ledger = payload.binding_ledger || [];
  const out: PendingBinding[] = [];
  for (const binding of ledger) {
    if (!binding || typeof binding !== 'object') continue;
    const state = String(binding.state || '').toLowerCase();
    const candidates = Array.isArray(binding.candidate_targets) ? binding.candidate_targets : [];
    if (state === 'accepted' || state === 'rejected') continue;
    if (candidates.length === 0) continue;
    out.push({ binding, topCandidate: candidates[0] });
  }
  return out;
}

function candidateLabel(target: BindingTarget): string {
  return (
    target.label
    || target.target_ref
    || (target.enrichment && target.enrichment.integration_name)
    || 'unknown target'
  );
}

function bindingDisplayLabel(binding: BindingLedgerEntry): string {
  return binding.source_label || binding.binding_id;
}

export function MoonBindingReviewQueue({ payload, onCommitAuthorityAction, onClose }: Props) {
  const pending = useMemo(() => bindingsToReview(payload), [payload]);
  const [focusIndex, setFocusIndex] = useState(0);
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set());
  const [errorByBinding, setErrorByBinding] = useState<Record<string, string>>({});
  const [bulkBusy, setBulkBusy] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (focusIndex >= pending.length) setFocusIndex(Math.max(0, pending.length - 1));
  }, [pending.length, focusIndex]);

  const setBusy = useCallback((id: string, on: boolean) => {
    setBusyIds(prev => {
      const next = new Set(prev);
      if (on) next.add(id); else next.delete(id);
      return next;
    });
  }, []);

  const dispatchDecision = useCallback(
    async (binding: BindingLedgerEntry, candidate: BindingTarget | null, decision: 'approve' | 'reject') => {
      const id = binding.binding_id;
      setBusy(id, true);
      setErrorByBinding(prev => ({ ...prev, [id]: '' }));
      try {
        const request: Record<string, unknown> = {
          target_kind: 'binding',
          target_ref: id,
          decision,
          rationale: decision === 'approve'
            ? 'Approved via batch review queue.'
            : 'Skipped via batch review queue.',
        };
        if (decision === 'approve' && candidate) {
          request.candidate_payload = candidate;
        }
        await onCommitAuthorityAction('review_decisions', request, {
          label: decision === 'approve' ? 'Approve binding (batch)' : 'Skip binding (batch)',
          reason: `${decision === 'approve' ? 'Approve' : 'Skip'} ${bindingDisplayLabel(binding)}.`,
          outcome: decision === 'approve'
            ? `${bindingDisplayLabel(binding)} resolves to ${candidate ? candidateLabel(candidate) : 'unspecified target'}.`
            : `${bindingDisplayLabel(binding)} marked as skipped.`,
          authority: 'build.binding_ledger',
          target: {
            kind: 'binding',
            label: bindingDisplayLabel(binding),
            id,
          },
          changeSummary: [
            'Binding state',
            decision === 'approve' ? 'Accepted' : 'Skipped',
          ],
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setErrorByBinding(prev => ({ ...prev, [id]: message || 'Failed' }));
      } finally {
        setBusy(id, false);
      }
    },
    [onCommitAuthorityAction, setBusy],
  );

  const approveAll = useCallback(async () => {
    if (bulkBusy || pending.length === 0) return;
    setBulkBusy(true);
    try {
      // Sequential — review_decisions is order-sensitive on the server side
      // and parallel writes hit the same workflow_build_review_decisions
      // table; serial is the conservative correct choice.
      for (const { binding, topCandidate } of pending) {
        // eslint-disable-next-line no-await-in-loop
        await dispatchDecision(binding, topCandidate, 'approve');
      }
    } finally {
      setBulkBusy(false);
    }
  }, [bulkBusy, pending, dispatchDecision]);

  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (pending.length === 0) return;
      if (event.key === 'ArrowDown' || event.key === 'j') {
        event.preventDefault();
        setFocusIndex(i => Math.min(pending.length - 1, i + 1));
      } else if (event.key === 'ArrowUp' || event.key === 'k') {
        event.preventDefault();
        setFocusIndex(i => Math.max(0, i - 1));
      } else if (event.key === 'Enter') {
        event.preventDefault();
        const target = pending[focusIndex];
        if (target) void dispatchDecision(target.binding, target.topCandidate, 'approve');
      } else if (event.key.toLowerCase() === 'x') {
        event.preventDefault();
        const target = pending[focusIndex];
        if (target) void dispatchDecision(target.binding, target.topCandidate, 'reject');
      } else if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
      }
    },
    [pending, focusIndex, dispatchDecision, onClose],
  );

  useEffect(() => {
    containerRef.current?.focus();
  }, []);

  if (pending.length === 0) {
    return (
      <div
        ref={containerRef}
        tabIndex={0}
        onKeyDown={onKeyDown}
        className="moon-dock moon-dock--review"
      >
        <div className="moon-dock__header">
          <div className="moon-dock__title">No bindings to review</div>
          <button type="button" onClick={onClose} className="moon-dock__close">×</button>
        </div>
        <div className="moon-dock__item-desc" style={{ padding: 16 }}>
          Every binding the compiler produced is already accepted or rejected.
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      tabIndex={0}
      onKeyDown={onKeyDown}
      className="moon-dock moon-dock--review"
      style={{ outline: 'none' }}
    >
      <div className="moon-dock__header" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: 12 }}>
        <div>
          <div className="moon-dock__title" style={{ fontSize: 14, fontWeight: 600 }}>
            Review bindings ({pending.length})
          </div>
          <div className="moon-dock__item-desc" style={{ fontSize: 11, opacity: 0.7 }}>
            ↑/↓ navigate · Enter approve · X skip · Esc close
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            type="button"
            onClick={approveAll}
            disabled={bulkBusy}
            className="moon-dock-form__btn moon-dock-form__btn--primary"
            style={{ padding: '6px 12px' }}
          >
            {bulkBusy ? 'Approving…' : 'Approve all suggested'}
          </button>
          <button type="button" onClick={onClose} className="moon-dock__close">×</button>
        </div>
      </div>

      <div className="moon-dock__list" role="list" style={{ padding: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
        {pending.map((entry, index) => {
          const focused = index === focusIndex;
          const isBusy = busyIds.has(entry.binding.binding_id);
          const errorMsg = errorByBinding[entry.binding.binding_id];
          return (
            <div
              key={entry.binding.binding_id}
              role="listitem"
              onMouseEnter={() => setFocusIndex(index)}
              className={`moon-dock__item${focused ? ' moon-dock__item--focused' : ''}`}
              style={{
                padding: '10px 12px',
                border: '1px solid',
                borderColor: focused ? 'var(--moon-fg)' : 'var(--moon-border, rgba(255,255,255,0.1))',
                borderRadius: 6,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
                opacity: isBusy ? 0.6 : 1,
              }}
            >
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="moon-dock__item-title" style={{ fontSize: 13, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {bindingDisplayLabel(entry.binding)}
                </div>
                <div className="moon-dock__item-desc" style={{ fontSize: 11, opacity: 0.75, marginTop: 2 }}>
                  → {candidateLabel(entry.topCandidate)}
                  {entry.topCandidate.kind ? ` · ${entry.topCandidate.kind}` : ''}
                </div>
                {errorMsg && (
                  <div style={{ color: 'var(--moon-error, #c98b6f)', fontSize: 11, marginTop: 4 }}>
                    {errorMsg}
                  </div>
                )}
              </div>
              <div style={{ display: 'flex', gap: 6 }}>
                <button
                  type="button"
                  disabled={isBusy}
                  onClick={() => void dispatchDecision(entry.binding, entry.topCandidate, 'approve')}
                  className="moon-dock-form__btn moon-dock-form__btn--primary"
                  style={{ padding: '4px 10px', fontSize: 12 }}
                >
                  Approve
                </button>
                <button
                  type="button"
                  disabled={isBusy}
                  onClick={() => void dispatchDecision(entry.binding, entry.topCandidate, 'reject')}
                  className="moon-dock-form__btn"
                  style={{ padding: '4px 10px', fontSize: 12 }}
                >
                  Skip
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
