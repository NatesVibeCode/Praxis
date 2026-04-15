import React, { useCallback, useEffect, useMemo, useState } from 'react';

import {
  fetchCatalogReviewDecisions,
  postCatalogReviewDecision,
  type CatalogReviewDecisionRequest,
} from '../shared/buildController';
import { getCatalogSurfacePolicy, getCatalogTruth, isMoonSurfaceAuthorityItem } from './actionTruth';
import type {
  CatalogItem,
  CatalogSourcePolicy,
  CatalogSurfacePolicyPayload,
  CatalogTruthPayload,
} from './catalog';
import { MoonGlyph } from './MoonGlyph';
import type { GlyphType } from './moonBuildPresenter';

type ReviewScope = 'catalog_item' | 'source_policy';
type ReviewDecision = CatalogReviewDecisionRequest['decision'];

interface CatalogReviewDecision {
  reviewDecisionId: string;
  targetKind: ReviewScope;
  targetRef: string;
  decision: ReviewDecision;
  rationale: string;
  actorType: string;
  approvalMode: string;
  decidedAt: string;
  candidatePayload?: Record<string, unknown>;
}

interface ReviewTarget {
  key: string;
  targetKind: ReviewScope;
  targetRef: string;
  label: string;
  description: string;
  icon: GlyphType;
  truth: CatalogTruthPayload;
  surfacePolicy: CatalogSurfacePolicyPayload;
  status?: CatalogItem['status'];
}

interface ReviewDraft {
  decision: ReviewDecision;
  label: string;
  description: string;
  status: CatalogItem['status'];
  truthCategory: CatalogTruthPayload['category'];
  truthBadge: string;
  truthDetail: string;
  surfaceTier: CatalogSurfacePolicyPayload['tier'];
  surfaceBadge: string;
  surfaceDetail: string;
  hardChoice: string;
  rationale: string;
}

interface Props {
  catalogItems: CatalogItem[];
  sourcePolicies: CatalogSourcePolicy[];
  onCatalogReload: () => Promise<void>;
}

const SOURCE_KIND_LABELS: Record<CatalogSourcePolicy['sourceKind'], string> = {
  capability: 'Capability lanes',
  integration: 'Integration lanes',
  connector: 'Connector lanes',
};

const SOURCE_KIND_ICONS: Record<CatalogSourcePolicy['sourceKind'], GlyphType> = {
  capability: 'classify',
  integration: 'tool',
  connector: 'tool',
};

const DECISION_LABELS: Record<ReviewDecision, string> = {
  approve: 'Approve override',
  widen: 'Widen surface',
  reject: 'Reject draft',
  defer: 'Defer',
  revoke: 'Revoke latest',
};

function tierRank(tier: CatalogSurfacePolicyPayload['tier']): number {
  if (tier === 'hidden') return 0;
  if (tier === 'advanced') return 1;
  return 2;
}

function normalizeReviewDecisions(payload: unknown): CatalogReviewDecision[] {
  if (!Array.isArray(payload)) return [];
  return payload.flatMap((value) => {
    if (!value || typeof value !== 'object') return [];
    const candidate = value as Record<string, unknown>;
    const reviewDecisionId = typeof candidate.review_decision_id === 'string' ? candidate.review_decision_id : '';
    const targetKind = candidate.target_kind === 'catalog_item' || candidate.target_kind === 'source_policy'
      ? candidate.target_kind
      : null;
    const targetRef = typeof candidate.target_ref === 'string' ? candidate.target_ref : '';
    const decision = (
      candidate.decision === 'approve'
      || candidate.decision === 'widen'
      || candidate.decision === 'reject'
      || candidate.decision === 'defer'
      || candidate.decision === 'revoke'
    )
      ? candidate.decision
      : null;
    if (!reviewDecisionId || !targetKind || !targetRef || !decision) return [];
    return [{
      reviewDecisionId,
      targetKind,
      targetRef,
      decision,
      rationale: typeof candidate.rationale === 'string' ? candidate.rationale : '',
      actorType: typeof candidate.actor_type === 'string' ? candidate.actor_type : '',
      approvalMode: typeof candidate.approval_mode === 'string' ? candidate.approval_mode : '',
      decidedAt: typeof candidate.decided_at === 'string' ? candidate.decided_at : '',
      candidatePayload: candidate.candidate_payload && typeof candidate.candidate_payload === 'object'
        ? candidate.candidate_payload as Record<string, unknown>
        : undefined,
    }];
  });
}

function buildDraft(target: ReviewTarget): ReviewDraft {
  return {
    decision: 'approve',
    label: target.label,
    description: target.description,
    status: target.status || 'ready',
    truthCategory: target.truth.category,
    truthBadge: target.truth.badge,
    truthDetail: target.truth.detail,
    surfaceTier: target.surfacePolicy.tier,
    surfaceBadge: target.surfacePolicy.badge,
    surfaceDetail: target.surfacePolicy.detail,
    hardChoice: target.surfacePolicy.hardChoice || '',
    rationale: '',
  };
}

function formatDecisionTimestamp(value: string): string {
  if (!value) return 'Unknown time';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function buildCandidatePayload(target: ReviewTarget, draft: ReviewDraft): Record<string, unknown> {
  const candidatePayload: Record<string, unknown> = {
    truth: {
      category: draft.truthCategory,
      badge: draft.truthBadge.trim(),
      detail: draft.truthDetail.trim(),
    },
    surfacePolicy: {
      tier: draft.surfaceTier,
      badge: draft.surfaceBadge.trim(),
      detail: draft.surfaceDetail.trim(),
      ...(draft.hardChoice.trim() ? { hardChoice: draft.hardChoice.trim() } : {}),
    },
  };
  if (target.targetKind === 'catalog_item') {
    candidatePayload.label = draft.label.trim();
    candidatePayload.description = draft.description.trim();
    candidatePayload.status = draft.status;
  }
  return candidatePayload;
}

export function MoonSurfaceReviewPanel({ catalogItems, sourcePolicies, onCatalogReload }: Props) {
  const [open, setOpen] = useState(false);
  const [scope, setScope] = useState<ReviewScope>('catalog_item');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [draft, setDraft] = useState<ReviewDraft | null>(null);
  const [decisions, setDecisions] = useState<CatalogReviewDecision[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const catalogTargets = useMemo<ReviewTarget[]>(() => (
    catalogItems
      .filter((item) => isMoonSurfaceAuthorityItem(item))
      .map((item) => ({
        key: `catalog_item:${item.id}`,
        targetKind: 'catalog_item' as const,
        targetRef: item.id,
        label: item.label,
        description: item.description || '',
        icon: item.icon,
        truth: getCatalogTruth(item),
        surfacePolicy: getCatalogSurfacePolicy(item),
        status: item.status,
      }))
      .sort((left, right) => (
        tierRank(left.surfacePolicy.tier) - tierRank(right.surfacePolicy.tier)
        || left.label.localeCompare(right.label)
      ))
  ), [catalogItems]);

  const sourceTargets = useMemo<ReviewTarget[]>(() => (
    sourcePolicies
      .map((policy) => ({
        key: `source_policy:${policy.sourceKind}`,
        targetKind: 'source_policy' as const,
        targetRef: policy.sourceKind,
        label: SOURCE_KIND_LABELS[policy.sourceKind],
        description: policy.surfacePolicy?.detail || policy.truth?.detail || `Review DB policy for ${policy.sourceKind} rows.`,
        icon: SOURCE_KIND_ICONS[policy.sourceKind],
        truth: policy.truth || {
          category: 'partial',
          badge: 'Missing',
          detail: 'Source-kind policy is missing truth metadata.',
        },
        surfacePolicy: policy.surfacePolicy || {
          tier: 'hidden',
          badge: 'Missing',
          detail: 'Source-kind policy is missing surface metadata.',
        },
      }))
      .sort((left, right) => left.label.localeCompare(right.label))
  ), [sourcePolicies]);

  const activeTargets = scope === 'catalog_item' ? catalogTargets : sourceTargets;

  useEffect(() => {
    if (!open) return undefined;
    let cancelled = false;
    fetchCatalogReviewDecisions({ surface: 'moon' })
      .then((payload) => {
        if (cancelled) return;
        setDecisions(normalizeReviewDecisions(payload?.review_decisions));
      })
      .catch((nextError: Error) => {
        if (cancelled) return;
        setError(nextError.message || 'Failed to load review decisions');
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  useEffect(() => {
    if (!activeTargets.length) {
      setSelectedKey(null);
      return;
    }
    if (!selectedKey || !activeTargets.some((target) => target.key === selectedKey)) {
      setSelectedKey(activeTargets[0].key);
    }
  }, [activeTargets, selectedKey]);

  const selectedTarget = useMemo(
    () => activeTargets.find((target) => target.key === selectedKey) ?? null,
    [activeTargets, selectedKey],
  );

  useEffect(() => {
    if (!selectedTarget) {
      setDraft(null);
      return;
    }
    setDraft(buildDraft(selectedTarget));
    setError(null);
    setSuccess(null);
  }, [selectedTarget]);

  const decisionsByKey = useMemo(() => Object.fromEntries(
    decisions.map((decision) => [`${decision.targetKind}:${decision.targetRef}`, decision]),
  ), [decisions]);

  const latestDecision = selectedTarget ? decisionsByKey[selectedTarget.key] ?? null : null;
  const reviewCounts = `${catalogTargets.length} catalog rows · ${sourceTargets.length} dynamic lanes`;

  const handleReloadDecisions = useCallback(async () => {
    const payload = await fetchCatalogReviewDecisions({ surface: 'moon' });
    setDecisions(normalizeReviewDecisions(payload?.review_decisions));
  }, []);

  const handleSubmit = useCallback(async () => {
    if (!selectedTarget || !draft) return;
    const overlayDecision = draft.decision === 'approve' || draft.decision === 'widen';
    if (overlayDecision) {
      if (!draft.truthBadge.trim() || !draft.truthDetail.trim() || !draft.surfaceBadge.trim() || !draft.surfaceDetail.trim()) {
        setError('Truth and surface badges need full detail before saving.');
        return;
      }
      if (selectedTarget.targetKind === 'catalog_item' && !draft.label.trim()) {
        setError('Catalog row label cannot be blank.');
        return;
      }
    }

    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      await postCatalogReviewDecision({
        surface_name: 'moon',
        target_kind: selectedTarget.targetKind,
        target_ref: selectedTarget.targetRef,
        decision: draft.decision,
        rationale: draft.rationale.trim() || undefined,
        candidate_payload: overlayDecision ? buildCandidatePayload(selectedTarget, draft) : undefined,
      });
      await Promise.all([
        onCatalogReload(),
        handleReloadDecisions(),
      ]);
      setSuccess(`${selectedTarget.label} review decision recorded.`);
    } catch (nextError: any) {
      setError(nextError?.message || 'Failed to record review decision');
    } finally {
      setLoading(false);
    }
  }, [draft, handleReloadDecisions, onCatalogReload, selectedTarget]);

  return (
    <div className="moon-surface-review">
      <button
        type="button"
        className={`moon-surface-review__toggle${open ? ' moon-surface-review__toggle--open' : ''}`}
        onClick={() => setOpen((current) => !current)}
      >
        <span className="moon-surface-review__toggle-copy">
          <span className="moon-dock__section-label">Surface review</span>
          <span className="moon-action__surface-note">
            Review DB-authored surface policy without shipping a new migration.
          </span>
        </span>
        <span className="moon-surface-review__toggle-meta">{reviewCounts}</span>
      </button>

      {open && (
        <div className="moon-surface-review__body">
          <div className="moon-catalog__filters">
            <button
              type="button"
              className={`moon-catalog__filter${scope === 'catalog_item' ? ' moon-catalog__filter--active' : ''}`}
              onClick={() => setScope('catalog_item')}
            >
              Catalog rows
            </button>
            <button
              type="button"
              className={`moon-catalog__filter${scope === 'source_policy' ? ' moon-catalog__filter--active' : ''}`}
              onClick={() => setScope('source_policy')}
            >
              Dynamic lanes
            </button>
          </div>

          <div className="moon-surface-review__target-list">
            {activeTargets.map((target) => (
              <button
                key={target.key}
                type="button"
                className={`moon-dock__catalog-item moon-dock__catalog-item--${target.truth.category}${selectedKey === target.key ? ' moon-dock__catalog-item--active' : ''}`}
                onClick={() => setSelectedKey(target.key)}
              >
                <MoonGlyph type={target.icon} size={14} />
                <span className="moon-catalog-item__stack">
                  <span className="moon-catalog-item__label">{target.label}</span>
                  <span className="moon-catalog-item__detail">{target.description}</span>
                </span>
                <span className="moon-catalog-item__meta-row">
                  <span className="moon-surface-badge">{target.surfacePolicy.badge}</span>
                  <span className={`moon-truth-badge moon-truth-badge--${target.truth.category}`}>{target.truth.badge}</span>
                </span>
              </button>
            ))}
          </div>

          {!selectedTarget || !draft ? (
            <div className="moon-dock__empty">No reviewable surface targets yet.</div>
          ) : (
            <div className="moon-surface-review__editor">
              <div className="moon-action__surface-card">
                <div className="moon-dock__section-label">Current authority</div>
                <div className="moon-action__surface-note">
                  {selectedTarget.label} currently resolves as <strong>{selectedTarget.surfacePolicy.badge}</strong> and {selectedTarget.truth.badge.toLowerCase()}.
                </div>
                {latestDecision && (
                  <div className="moon-surface-review__latest">
                    <span className="moon-surface-review__latest-badge">{DECISION_LABELS[latestDecision.decision]}</span>
                    <span className="moon-surface-review__latest-copy">
                      Latest review by {latestDecision.actorType || 'operator'} · {formatDecisionTimestamp(latestDecision.decidedAt)}
                    </span>
                    {latestDecision.rationale && (
                      <span className="moon-catalog-item__detail">{latestDecision.rationale}</span>
                    )}
                  </div>
                )}
              </div>

              <label className="moon-dock-form__label" htmlFor="surface-review-decision">Decision</label>
              <select
                id="surface-review-decision"
                className="moon-dock-form__select"
                value={draft.decision}
                onChange={(event) => setDraft((current) => current ? { ...current, decision: event.target.value as ReviewDecision } : current)}
              >
                {Object.entries(DECISION_LABELS).map(([value, label]) => (
                  <option key={value} value={value}>{label}</option>
                ))}
              </select>

              {selectedTarget.targetKind === 'catalog_item' && (
                <>
                  <label className="moon-dock-form__label" htmlFor="surface-review-label">Label</label>
                  <input
                    id="surface-review-label"
                    className="moon-dock-form__input"
                    value={draft.label}
                    onChange={(event) => setDraft((current) => current ? { ...current, label: event.target.value } : current)}
                  />
                  <label className="moon-dock-form__label" htmlFor="surface-review-description">Description</label>
                  <textarea
                    id="surface-review-description"
                    className="moon-dock-form__input moon-surface-review__textarea"
                    value={draft.description}
                    rows={2}
                    onChange={(event) => setDraft((current) => current ? { ...current, description: event.target.value } : current)}
                  />
                  <label className="moon-dock-form__label" htmlFor="surface-review-status">Status</label>
                  <select
                    id="surface-review-status"
                    className="moon-dock-form__select"
                    value={draft.status}
                    onChange={(event) => setDraft((current) => current ? { ...current, status: event.target.value as CatalogItem['status'] } : current)}
                  >
                    <option value="ready">Ready</option>
                    <option value="coming_soon">Coming soon</option>
                  </select>
                </>
              )}

              <div className="moon-surface-review__columns">
                <div className="moon-surface-review__column">
                  <label className="moon-dock-form__label" htmlFor="surface-review-truth-category">Truth category</label>
                  <select
                    id="surface-review-truth-category"
                    className="moon-dock-form__select"
                    value={draft.truthCategory}
                    onChange={(event) => setDraft((current) => current ? { ...current, truthCategory: event.target.value as CatalogTruthPayload['category'] } : current)}
                  >
                    <option value="runtime">Runtime</option>
                    <option value="persisted">Persisted</option>
                    <option value="alias">Alias</option>
                    <option value="partial">Partial</option>
                    <option value="coming_soon">Coming soon</option>
                  </select>
                  <label className="moon-dock-form__label" htmlFor="surface-review-truth-badge">Truth badge</label>
                  <input
                    id="surface-review-truth-badge"
                    className="moon-dock-form__input"
                    value={draft.truthBadge}
                    onChange={(event) => setDraft((current) => current ? { ...current, truthBadge: event.target.value } : current)}
                  />
                  <label className="moon-dock-form__label" htmlFor="surface-review-truth-detail">Truth detail</label>
                  <textarea
                    id="surface-review-truth-detail"
                    className="moon-dock-form__input moon-surface-review__textarea"
                    value={draft.truthDetail}
                    rows={3}
                    onChange={(event) => setDraft((current) => current ? { ...current, truthDetail: event.target.value } : current)}
                  />
                </div>

                <div className="moon-surface-review__column">
                  <label className="moon-dock-form__label" htmlFor="surface-review-surface-tier">Surface tier</label>
                  <select
                    id="surface-review-surface-tier"
                    className="moon-dock-form__select"
                    value={draft.surfaceTier}
                    onChange={(event) => setDraft((current) => current ? { ...current, surfaceTier: event.target.value as CatalogSurfacePolicyPayload['tier'] } : current)}
                  >
                    <option value="primary">Primary</option>
                    <option value="advanced">Advanced</option>
                    <option value="hidden">Hidden</option>
                  </select>
                  <label className="moon-dock-form__label" htmlFor="surface-review-surface-badge">Surface badge</label>
                  <input
                    id="surface-review-surface-badge"
                    className="moon-dock-form__input"
                    value={draft.surfaceBadge}
                    onChange={(event) => setDraft((current) => current ? { ...current, surfaceBadge: event.target.value } : current)}
                  />
                  <label className="moon-dock-form__label" htmlFor="surface-review-surface-detail">Surface detail</label>
                  <textarea
                    id="surface-review-surface-detail"
                    className="moon-dock-form__input moon-surface-review__textarea"
                    value={draft.surfaceDetail}
                    rows={3}
                    onChange={(event) => setDraft((current) => current ? { ...current, surfaceDetail: event.target.value } : current)}
                  />
                </div>
              </div>

              <label className="moon-dock-form__label" htmlFor="surface-review-hard-choice">Hard choice</label>
              <textarea
                id="surface-review-hard-choice"
                className="moon-dock-form__input moon-surface-review__textarea"
                value={draft.hardChoice}
                rows={2}
                onChange={(event) => setDraft((current) => current ? { ...current, hardChoice: event.target.value } : current)}
              />

              <label className="moon-dock-form__label" htmlFor="surface-review-rationale">Rationale</label>
              <textarea
                id="surface-review-rationale"
                className="moon-dock-form__input moon-surface-review__textarea"
                value={draft.rationale}
                rows={2}
                onChange={(event) => setDraft((current) => current ? { ...current, rationale: event.target.value } : current)}
              />

              <div className="moon-dock-form__row">
                <button
                  type="button"
                  className="moon-dock-form__btn"
                  disabled={loading}
                  onClick={handleSubmit}
                >
                  {loading ? 'Saving…' : DECISION_LABELS[draft.decision]}
                </button>
              </div>

              {error && <div className="moon-dock-form__error">{error}</div>}
              {success && <div className="moon-action__success">{success}</div>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
