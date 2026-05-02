import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { BindingLedgerEntry, BindingTarget, BuildPayload } from '../shared/types';
import type { AuthorityActionMeta } from './CanvasNodeDetail';

/**
 * CanvasBindingReviewQueue — batch review for unresolved binding_ledger entries.
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
 * Dispatch shape mirrors CanvasNodeDetail.tsx (target_kind='binding',
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

interface PendingReviewItem {
  id: string;
  targetKind: 'binding' | 'capability_bundle' | 'workflow_shape';
  targetRef: string;
  slotRef?: string;
  label: string;
  question: string;
  detail: string;
  meta: string;
  candidateLabel: string;
  candidateRef?: string;
  candidatePayload?: Record<string, unknown>;
  authority: string;
  targetLabel: string;
}

function readableSlug(value: string): string {
  const raw = value.trim();
  if (!raw) return 'this item';
  const special: Record<string, string> = {
    'analysis_result': 'analysis result',
    'architecture_plan': 'architecture plan',
    'auto/build': 'build agent',
    'auto/research': 'research agent',
    'auto/review': 'review agent',
    'code_change': 'code change',
    'diff': 'diff',
    'draft': 'draft',
    'evidence_pack': 'evidence pack',
    'execution_receipt': 'execution receipt',
    'input_text': 'input text',
    'requirements': 'requirements',
    'research_findings': 'research findings',
    'review_result': 'review result',
    'summary': 'summary',
    'validated_input': 'validated input',
  };
  const normalized = raw.replace(/^binding:ref-/, '').replace(/^capability_bundle:/, '');
  const mapped = special[raw] || special[normalized];
  if (mapped) return mapped;
  return normalized.replace(/[_.-]+/g, ' ').replace(/\s*\/\s*/g, ' / ').replace(/\s+/g, ' ').trim();
}

function readableTitle(value: string): string {
  const cleaned = readableSlug(value);
  return cleaned
    .split(' ')
    .map((word, index) => {
      if (!word) return word;
      if (/^[A-Z0-9]{2,}$/.test(word)) return word;
      const lower = word.toLowerCase();
      return index === 0 ? `${lower.charAt(0).toUpperCase()}${lower.slice(1)}` : lower;
    })
    .join(' ');
}

function nodeNamesFor(payload: BuildPayload | null, ids: string[] | undefined): string[] {
  if (!payload?.build_graph?.nodes?.length || !ids?.length) return [];
  const byId = new Map(payload.build_graph.nodes.map(node => [node.node_id, readableTitle(node.title || node.node_id)] as const));
  const out: string[] = [];
  for (const id of ids) {
    const name = byId.get(id);
    if (name && !out.includes(name)) out.push(name);
  }
  return out;
}

function compactList(values: string[], fallback: string): string {
  if (values.length === 0) return fallback;
  if (values.length <= 3) return values.join(', ');
  return `${values.slice(0, 3).join(', ')} +${values.length - 3} more`;
}

function bindingQuestion(payload: BuildPayload | null, binding: BindingLedgerEntry, target: BindingTarget): Pick<PendingReviewItem, 'question' | 'detail' | 'meta'> {
  const source = readableSlug(binding.source_label || binding.binding_id);
  const candidate = readableSlug(candidateLabel(target));
  const nodeNames = nodeNamesFor(payload, binding.source_node_ids);
  const scope = compactList(nodeNames, 'this workflow');
  const kind = String(target.kind || binding.source_kind || '').toLowerCase();
  if (kind === 'agent') {
    return {
      question: `Use the ${candidate} for ${scope}?`,
      detail: 'This approves which agent lane may do that work when the workflow runs.',
      meta: binding.source_label || binding.binding_id,
    };
  }
  return {
    question: `Approve the ${source} handoff?`,
    detail: `This lets ${scope} use the "${candidate}" workflow value.`,
    meta: binding.source_label || binding.binding_id,
  };
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

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringValue(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function candidateReviewManifest(payload: BuildPayload | null): Record<string, unknown> {
  return asRecord((payload as unknown as Record<string, unknown> | null)?.candidate_resolution_manifest) || {};
}

function capabilityBundlesToReview(payload: BuildPayload | null): PendingReviewItem[] {
  const manifest = candidateReviewManifest(payload);
  const slots = Array.isArray(manifest.capability_bundle_candidates)
    ? manifest.capability_bundle_candidates
    : [];
  const out: PendingReviewItem[] = [];
  for (const rawSlot of slots) {
    const slot = asRecord(rawSlot);
    if (!slot) continue;
    if (String(slot.approval_state || '').toLowerCase() === 'approved') continue;
    const candidates = Array.isArray(slot.candidates) ? slot.candidates : [];
    const topRankedRef = stringValue(slot.top_ranked_ref);
    const candidate = (
      candidates.map(asRecord).find(item => item && stringValue(item.candidate_ref) === topRankedRef)
      || asRecord(candidates[0])
    );
    if (!candidate) continue;
    const candidateRef = stringValue(candidate.candidate_ref || candidate.target_ref || topRankedRef);
    if (!candidateRef) continue;
    const slotRef = stringValue(slot.slot_ref) || `capability_bundle:${stringValue(slot.family) || 'general'}`;
    const label = stringValue(candidate.label) || candidateRef;
    out.push({
      id: `capability_bundle:${candidateRef}`,
      targetKind: 'capability_bundle',
      targetRef: candidateRef,
      slotRef,
      label: stringValue(slot.family) ? `Capability bundle · ${stringValue(slot.family)}` : 'Capability bundle',
      question: `Use ${readableSlug(label)} as the workflow capability set?`,
      detail: stringValue(slot.family)
        ? `This tells hardening to use the ${readableSlug(stringValue(slot.family))} pattern family.`
        : 'This tells hardening which capability package to use.',
      meta: candidateRef,
      candidateLabel: label,
      candidateRef,
      candidatePayload: asRecord(candidate.payload) || { bundle_ref: candidateRef, label, kind: 'capability_bundle' },
      authority: 'build.capability_bundle',
      targetLabel: label,
    });
  }
  return out;
}

function workflowShapeToReview(payload: BuildPayload | null): PendingReviewItem[] {
  const manifest = candidateReviewManifest(payload);
  const candidates = Array.isArray(manifest.workflow_shape_candidates)
    ? manifest.workflow_shape_candidates
    : [];
  const out: PendingReviewItem[] = [];
  for (const rawCandidate of candidates) {
    const candidate = asRecord(rawCandidate);
    if (!candidate) continue;
    if (String(candidate.approval_state || '').toLowerCase() === 'approved') continue;
    const candidateRef = stringValue(candidate.candidate_ref || candidate.target_ref);
    if (!candidateRef) continue;
    const summary = asRecord(candidate.summary) || {};
    const nodes = typeof summary.node_count === 'number' ? summary.node_count : undefined;
    const edges = typeof summary.edge_count === 'number' ? summary.edge_count : undefined;
    out.push({
      id: `workflow_shape:${candidateRef}`,
      targetKind: 'workflow_shape',
      targetRef: candidateRef,
      slotRef: 'workflow_shape',
      label: 'Workflow shape',
      question: 'Approve this workflow shape?',
      detail: nodes || edges ? `${nodes || 0} steps / ${edges || 0} gates will become the executable shape.` : 'This approves the current graph as the executable shape.',
      meta: candidateRef,
      candidateLabel: nodes || edges ? `${nodes || 0} nodes / ${edges || 0} gates` : 'Current graph',
      candidateRef,
      candidatePayload: {
        ...summary,
        candidate_ref: candidateRef,
        kind: stringValue(candidate.kind) || 'build_graph',
        shape_family_ref: stringValue(candidate.shape_family_ref) || undefined,
      },
      authority: 'build.workflow_shape',
      targetLabel: 'Current graph',
    });
  }
  return out;
}

function reviewItemsToReview(payload: BuildPayload | null): PendingReviewItem[] {
  const bindingItems = bindingsToReview(payload).map(({ binding, topCandidate }) => ({
    id: binding.binding_id,
    targetKind: 'binding' as const,
    targetRef: binding.binding_id,
    slotRef: binding.binding_id,
    label: bindingDisplayLabel(binding),
    ...bindingQuestion(payload, binding, topCandidate),
    candidateLabel: candidateLabel(topCandidate),
    candidateRef: topCandidate.target_ref,
    candidatePayload: topCandidate as Record<string, unknown>,
    authority: 'build.binding_ledger',
    targetLabel: bindingDisplayLabel(binding),
  }));
  return [
    ...bindingItems,
    ...capabilityBundlesToReview(payload),
    ...workflowShapeToReview(payload),
  ];
}

export function reviewReadinessCount(payload: BuildPayload | null): number {
  return reviewItemsToReview(payload).length;
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

export function CanvasBindingReviewQueue({ payload, onCommitAuthorityAction, onClose }: Props) {
  const pending = useMemo(() => reviewItemsToReview(payload), [payload]);
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
    async (item: PendingReviewItem, decision: 'approve' | 'reject') => {
      const id = item.id;
      setBusy(id, true);
      setErrorByBinding(prev => ({ ...prev, [id]: '' }));
      try {
        const request: Record<string, unknown> = {
          target_kind: item.targetKind,
          target_ref: item.targetRef,
          slot_ref: item.slotRef || item.targetRef,
          decision,
          rationale: decision === 'approve'
            ? 'Approved via readiness review queue.'
            : 'Skipped via readiness review queue.',
        };
        if (decision === 'approve') {
          if (item.candidateRef) request.candidate_ref = item.candidateRef;
          if (item.candidatePayload) request.candidate_payload = item.candidatePayload;
        }
        await onCommitAuthorityAction('review_decisions', request, {
          label: decision === 'approve' ? 'Approve readiness item' : 'Skip readiness item',
          reason: `${decision === 'approve' ? 'Approve' : 'Skip'} ${item.label}.`,
          outcome: decision === 'approve'
            ? `${item.label} resolves to ${item.candidateLabel}.`
            : `${item.label} marked as skipped.`,
          authority: item.authority,
          target: {
            kind: item.targetKind,
            label: item.targetLabel,
            id: item.targetRef,
          },
          changeSummary: [
            item.targetKind === 'binding' ? 'Binding state' : 'Review state',
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
      for (const item of pending) {
        // eslint-disable-next-line no-await-in-loop
        await dispatchDecision(item, 'approve');
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
        if (target) void dispatchDecision(target, 'approve');
      } else if (event.key.toLowerCase() === 'x') {
        event.preventDefault();
        const target = pending[focusIndex];
        if (target) void dispatchDecision(target, 'reject');
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
        className="canvas-dock canvas-dock--review canvas-review-queue"
      >
        <div className="canvas-dock__header">
          <div className="canvas-dock__title">No readiness approvals to review</div>
          <button type="button" onClick={onClose} className="canvas-dock__close">×</button>
        </div>
        <div className="canvas-dock__item-desc" style={{ padding: 16 }}>
          Every binding, capability bundle, and workflow shape suggestion is already accepted or rejected.
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      tabIndex={0}
      onKeyDown={onKeyDown}
      className="canvas-dock canvas-dock--review canvas-review-queue"
      style={{ outline: 'none' }}
    >
      <div className="canvas-dock__header canvas-review-queue__header" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: 12 }}>
        <div>
          <div className="canvas-dock__title" style={{ fontSize: 14, fontWeight: 600 }}>
            Review readiness ({pending.length})
          </div>
          <div className="canvas-dock__item-desc" style={{ fontSize: 11, opacity: 0.7 }}>
            ↑/↓ navigate · Enter approve · X skip · Esc close
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            type="button"
            onClick={approveAll}
            disabled={bulkBusy}
            className="canvas-dock-form__btn canvas-dock-form__btn--primary"
            style={{ padding: '6px 12px' }}
          >
            {bulkBusy ? 'Approving...' : 'Approve'}
          </button>
          <button type="button" onClick={onClose} className="canvas-dock__close">×</button>
        </div>
      </div>

      <div className="canvas-dock__list" role="list" style={{ padding: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
        {pending.map((entry, index) => {
          const focused = index === focusIndex;
          const isBusy = busyIds.has(entry.id);
          const errorMsg = errorByBinding[entry.id];
          return (
            <div
              key={entry.id}
              role="listitem"
              onMouseEnter={() => setFocusIndex(index)}
              className={`canvas-dock__item${focused ? ' canvas-dock__item--focused' : ''}`}
              style={{
                padding: '10px 12px',
                border: '1px solid',
                borderColor: focused ? 'var(--canvas-fg)' : 'var(--canvas-border, rgba(255,255,255,0.1))',
                borderRadius: 6,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
                opacity: isBusy ? 0.6 : 1,
              }}
            >
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="canvas-dock__item-title" style={{ fontSize: 13, fontWeight: 600, lineHeight: 1.3 }}>
                  {entry.question}
                </div>
                <div className="canvas-dock__item-desc" style={{ fontSize: 11, opacity: 0.75, marginTop: 2 }}>
                  {entry.detail}
                </div>
                <div className="canvas-dock__item-desc" style={{ fontSize: 10, opacity: 0.48, marginTop: 3 }}>
                  {entry.meta}
                </div>
                {errorMsg && (
                  <div style={{ color: 'var(--canvas-error, #c98b6f)', fontSize: 11, marginTop: 4 }}>
                    {errorMsg}
                  </div>
                )}
              </div>
              <div style={{ display: 'flex', gap: 6 }}>
                <button
                  type="button"
                  disabled={isBusy}
                  onClick={() => void dispatchDecision(entry, 'approve')}
                  className="canvas-dock-form__btn canvas-dock-form__btn--primary"
                  style={{ padding: '4px 10px', fontSize: 12 }}
                >
                  Approve
                </button>
                <button
                  type="button"
                  disabled={isBusy}
                  onClick={() => void dispatchDecision(entry, 'reject')}
                  className="canvas-dock-form__btn"
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
