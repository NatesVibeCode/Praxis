import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { compileDefinition, refineDefinition, commitDefinition, createWorkflow } from '../shared/buildController';
import type { BuildPayload } from '../shared/types';
import { loadCatalogEnvelope, refreshCatalogEnvelope, getCatalogEnvelope, FAMILY_LABELS } from './catalog';
import type { CatalogEnvelope, CatalogItem, CatalogFamily } from './catalog';
import {
  getCatalogSurfacePolicy,
  getCatalogTruth,
  isMoonSurfaceAuthorityItem,
  summarizeCatalogSurface,
  summarizeCatalogTruth,
} from './actionTruth';
import { MoonGlyph } from './MoonGlyph';
import { MoonSurfaceReviewPanel } from './MoonSurfaceReviewPanel';

interface Props {
  workflowId: string | null;
  payload: BuildPayload | null;
  onReload: () => void;
  onClose: () => void;
  onStartCatalogDrag: (event: React.PointerEvent, item: CatalogItem) => void;
  onPayloadChange: (payload: BuildPayload) => void;
  onWorkflowCreated?: (workflowId: string) => void;
  onCatalogChange?: (catalog: CatalogItem[]) => void;
}

const DOCK_FAMILIES: CatalogFamily[] = ['trigger', 'gather', 'think', 'act', 'control'];

export function MoonActionDock({
  workflowId,
  payload,
  onReload,
  onClose,
  onStartCatalogDrag,
  onPayloadChange,
  onWorkflowCreated,
  onCatalogChange,
}: Props) {
  const [prose, setProse] = useState('');
  const [loading, setLoading] = useState(false);
  const [action, setAction] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [catalogEnvelope, setCatalogEnvelope] = useState<CatalogEnvelope>(getCatalogEnvelope());
  const [familyFilter, setFamilyFilter] = useState<CatalogFamily | null>(null);

  useEffect(() => {
    loadCatalogEnvelope().then((nextEnvelope) => {
      setCatalogEnvelope(nextEnvelope);
      onCatalogChange?.(nextEnvelope.items);
    });
  }, [onCatalogChange]);

  const handleCatalogReload = useCallback(async () => {
    const nextEnvelope = await refreshCatalogEnvelope();
    setCatalogEnvelope(nextEnvelope);
    onCatalogChange?.(nextEnvelope.items);
  }, [onCatalogChange]);

  const catalog = catalogEnvelope.items;

  const visibleCatalog = useMemo(() => catalog.filter(c => c.status === 'ready'), [catalog]);
  const moonSurfaceCatalog = useMemo(
    () => visibleCatalog.filter((item) => isMoonSurfaceAuthorityItem(item)),
    [visibleCatalog],
  );
  const catalogSummary = useMemo(() => summarizeCatalogTruth(moonSurfaceCatalog), [moonSurfaceCatalog]);
  const surfaceSummary = useMemo(() => summarizeCatalogSurface(moonSurfaceCatalog), [moonSurfaceCatalog]);
  const visibleCatalogModels = useMemo(
    () => visibleCatalog.map((item) => ({
      item,
      truth: getCatalogTruth(item),
      policy: getCatalogSurfacePolicy(item),
    })),
    [visibleCatalog],
  );
  const filterableFamilies = useMemo(
    () => DOCK_FAMILIES.filter((family) => visibleCatalogModels.some(({ item, policy }) => item.family === family && policy.tier === 'primary')),
    [visibleCatalogModels],
  );
  const filteredCatalog = useMemo(
    () => (familyFilter
      ? visibleCatalogModels.filter(({ item }) => item.family === familyFilter)
      : visibleCatalogModels),
    [familyFilter, visibleCatalogModels],
  );
  const primaryCatalog = filteredCatalog.filter(({ policy }) => policy.tier === 'primary');
  const surfaceStats = useMemo(() => ({
    stepTotal: catalogSummary.nodeTotal,
    stepCore: surfaceSummary.nodeCounts.primary,
    stepOther: Math.max(0, catalogSummary.nodeTotal - surfaceSummary.nodeCounts.primary),
    gateTotal: catalogSummary.edgeTotal,
    gateCore: surfaceSummary.edgeCounts.primary,
    gateOther: Math.max(0, catalogSummary.edgeTotal - surfaceSummary.edgeCounts.primary),
  }), [catalogSummary, surfaceSummary]);

  const hasDefinition = !!(payload?.definition && Object.keys(payload.definition).length > 0);
  const hasGraphSteps = !!payload?.build_graph?.nodes?.some(node => (node.route || '').trim().length > 0);
  const buildState = payload?.build_state || 'draft';

  const adoptBuildPayload = useCallback((nextPayload: BuildPayload) => {
    const workflow = nextPayload.workflow
      ?? payload?.workflow
      ?? (workflowId ? { id: workflowId, name: payload?.workflow?.name || 'Workflow workspace' } : null);
    onPayloadChange({
      ...nextPayload,
      workflow,
    });
  }, [onPayloadChange, payload?.workflow, workflowId]);

  const handleRefine = useCallback(async () => {
    if (!prose.trim() || !payload?.definition) return;
    setLoading(true);
    setAction('refine');
    setError(null);
    setSuccess(null);
    try {
      const result = await refineDefinition(prose.trim(), payload.definition);
      adoptBuildPayload(result);
      setSuccess('Definition refined');
      setProse('');
    } catch (e: any) {
      setError(e.message || 'Refinement failed');
    } finally {
      setLoading(false);
    }
  }, [adoptBuildPayload, payload, prose]);

  const handleCompile = useCallback(async () => {
    if (!prose.trim()) return;
    setLoading(true);
    setAction('compile');
    setError(null);
    setSuccess(null);
    try {
      const result = await compileDefinition(prose.trim(), payload?.workflow?.name);
      adoptBuildPayload(result);
      setSuccess('Compiled');
      setProse('');
    } catch (e: any) {
      setError(e.message || 'Compilation failed');
    } finally {
      setLoading(false);
    }
  }, [adoptBuildPayload, payload?.workflow?.name, prose]);

  const handleCommit = useCallback(async () => {
    if (!hasDefinition && !hasGraphSteps) return;
    setLoading(true);
    setAction('commit');
    setError(null);
    setSuccess(null);
    try {
      const title = payload?.workflow?.name || 'Moon draft';
      const definition = (payload?.definition && Object.keys(payload.definition).length > 0)
        ? payload.definition as Record<string, unknown>
        : undefined;
      const buildGraph = hasGraphSteps ? payload?.build_graph : undefined;
      if (workflowId) {
        await commitDefinition(workflowId, { title, definition, buildGraph });
      } else {
        const created = await createWorkflow(title, { definition, buildGraph });
        const createdWorkflowId = created.id || created.workflow_id;
        if (createdWorkflowId && onWorkflowCreated) onWorkflowCreated(createdWorkflowId);
      }
      setSuccess('Saved');
      onReload();
    } catch (e: any) {
      setError(e.message || 'Save failed');
    } finally {
      setLoading(false);
    }
  }, [workflowId, payload, hasDefinition, hasGraphSteps, onReload, onWorkflowCreated]);

  const renderCatalogButton = useCallback((
    item: CatalogItem,
    detail: string,
    truthBadge: string,
    truthCategory: string,
    surfaceBadge?: string,
  ) => (
    <button
      key={item.id}
      type="button"
      className={`moon-dock__catalog-item moon-dock__catalog-item--${truthCategory}`}
      onPointerDown={e => onStartCatalogDrag(e, item)}
      title={`${item.description || item.label} — ${detail}`}
    >
      <MoonGlyph type={item.icon} size={14} />
      <span className="moon-catalog-item__stack">
        <span className="moon-catalog-item__label">{item.label}</span>
        <span className="moon-catalog-item__detail">{detail}</span>
      </span>
      <span className="moon-catalog-item__meta-row">
        {surfaceBadge && <span className="moon-surface-badge">{surfaceBadge}</span>}
        <span className={`moon-truth-badge moon-truth-badge--${truthCategory}`}>{truthBadge}</span>
      </span>
    </button>
  ), [onStartCatalogDrag]);

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close action dock">&times;</button>
      <div className="moon-dock__title">Action</div>
      <div className="moon-dock__sep" />

      <div className="moon-dock__section-label">
        {hasDefinition ? 'Refine the definition' : 'Describe the workflow'}
      </div>
      <textarea
        className="moon-dock-form__input moon-action__textarea"
        value={prose}
        onChange={e => setProse(e.target.value)}
        placeholder={hasDefinition ? 'Add detail or change direction...' : 'Describe the workflow...'}
        rows={2}
        disabled={loading}
      />
      <div className="moon-dock-form__row">
        {hasDefinition ? (
          <button className="moon-dock-form__btn" onClick={handleRefine} disabled={loading || !prose.trim()}>
            {loading && action === 'refine' ? 'Refining...' : 'Refine'}
          </button>
        ) : (
          <button className="moon-dock-form__btn" onClick={handleCompile} disabled={loading || !prose.trim()}>
            {loading && action === 'compile' ? 'Compiling...' : 'Compile'}
          </button>
        )}
      </div>

      {(hasDefinition || hasGraphSteps) && (
        <div style={{ marginTop: 16 }}>
          <div className="moon-dock__section-label">Save</div>
          <div className="moon-dock__item-desc">State: {buildState}</div>
          <button className="moon-dock-form__btn" onClick={handleCommit} disabled={loading || (!hasDefinition && !hasGraphSteps)}>
            {loading && action === 'commit' ? 'Saving...' : 'Save draft'}
          </button>
        </div>
      )}

      {error && <div className="moon-dock-form__error">{error}</div>}
      {success && <div className="moon-action__success">{success}</div>}

      {/* Draggable catalog — drag items onto chain nodes or edges */}
      <div style={{ marginTop: 20 }}>
        <div className="moon-action__surface-card">
          <div className="moon-dock__section-label">Moon surface</div>
          <div className="moon-action__surface-note">
            Counts reflect first-class Moon primitives only, not every live registry lane.
          </div>
          <div className="moon-action__surface-grid">
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.stepTotal}</span>
              <span className="moon-action__surface-label">step actions</span>
            </div>
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.stepCore}</span>
              <span className="moon-action__surface-label">core now</span>
            </div>
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.stepOther}</span>
              <span className="moon-action__surface-label">off main surface</span>
            </div>
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.gateTotal}</span>
              <span className="moon-action__surface-label">gate types</span>
            </div>
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.gateCore}</span>
              <span className="moon-action__surface-label">execute now</span>
            </div>
            <div className="moon-action__surface-stat">
              <span className="moon-action__surface-value">{surfaceStats.gateOther}</span>
              <span className="moon-action__surface-label">preview or removed</span>
            </div>
          </div>
        </div>
        <MoonSurfaceReviewPanel
          catalogItems={catalogEnvelope.items}
          sourcePolicies={catalogEnvelope.sourcePolicies}
          onCatalogReload={handleCatalogReload}
        />
        <div className="moon-dock__section-label">Catalog — drag onto a step or edge</div>
        <div className="moon-catalog__filters">
          {filterableFamilies.map(f => (
            <button
              key={f}
              className={`moon-catalog__filter${familyFilter === f ? ' moon-catalog__filter--active' : ''}`}
              onClick={() => setFamilyFilter(familyFilter === f ? null : f)}
            >{FAMILY_LABELS[f]}</button>
          ))}
        </div>
        {primaryCatalog.length > 0 && (
          <>
            <div className="moon-dock__section-label">Core now</div>
            <div className="moon-dock__item-desc">These are the curated primitives we trust in the main Moon surface.</div>
            <div className="moon-dock__catalog-grid">
              {primaryCatalog.map(({ item, truth, policy }) => renderCatalogButton(item, policy.detail, truth.badge, truth.category))}
            </div>
          </>
        )}
      </div>

      {payload?.matched_building_blocks && payload.matched_building_blocks.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div className="moon-dock__section-label">
            Composed from {payload.matched_building_blocks.length} building block{payload.matched_building_blocks.length !== 1 ? 's' : ''}
            {payload.composition_plan?.confidence != null && (
              <span style={{ marginLeft: 6, fontSize: 10, color: 'var(--moon-fg-muted)' }}>
                ({Math.round(payload.composition_plan.confidence * 100)}% match)
              </span>
            )}
          </div>
          {payload.matched_building_blocks.slice(0, 5).map((block, i) => (
            <div key={block.id || i} className="moon-dock__item" style={{ padding: '4px 10px' }}>
              <div className="moon-dock__item-title" style={{ fontSize: 12 }}>{block.name}</div>
              <div className="moon-dock__item-desc" style={{ fontSize: 10 }}>{block.category}</div>
            </div>
          ))}
          {payload.matched_building_blocks.length > 5 && (
            <div className="moon-dock__item-desc" style={{ padding: '2px 10px', fontSize: 10 }}>
              +{payload.matched_building_blocks.length - 5} more
            </div>
          )}
        </div>
      )}
    </>
  );
}
