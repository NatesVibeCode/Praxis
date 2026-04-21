import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { compileDefinition, refineDefinition, commitDefinition, createWorkflow, suggestNextSteps } from '../shared/buildController';
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
import { MoonIntegrationsPanel } from './MoonIntegrationsPanel';
import { MoonDataDictionaryPanel } from './MoonDataDictionaryPanel';

interface Props {
  workflowId: string | null;
  payload: BuildPayload | null;
  selectedNodeId?: string | null;
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
  selectedNodeId,
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

  const [suggestedCatalogIds, setSuggestedCatalogIds] = useState<string[]>([]);
  const [suggestedLoading, setSuggestedLoading] = useState(false);

  useEffect(() => {
    if (!workflowId || !selectedNodeId || !payload?.build_graph) {
      setSuggestedCatalogIds([]);
      return undefined;
    }
    let cancelled = false;
    setSuggestedLoading(true);
    suggestNextSteps(workflowId, selectedNodeId, payload.build_graph as any)
      .then((res: any) => {
        if (cancelled) return;
        setSuggestedCatalogIds(res.likely_next_steps.map((s: any) => s.capability_ref || s.id));
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) setSuggestedLoading(false);
      });
    return () => { cancelled = true; };
  }, [workflowId, selectedNodeId, payload?.build_graph]);

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
  const primaryCatalogAll = useMemo(
    () => visibleCatalogModels.filter(({ policy }) => policy.tier === 'primary'),
    [visibleCatalogModels],
  );
  // Count primary catalog items per family — drives the grammar-rail counts.
  const familyCounts = useMemo(() => {
    const out: Record<string, number> = {};
    for (const { item } of primaryCatalogAll) {
      const fam = item.family || 'other';
      out[fam] = (out[fam] ?? 0) + 1;
    }
    return out;
  }, [primaryCatalogAll]);
  // Which families are represented in the "suggested next" set — rail chips
  // for those families get a subtle pulse to tell you where the grammar wants
  // to go next. No color, just motion.
  const suggestedFamilies = useMemo(() => {
    const fams = new Set<string>();
    for (const id of suggestedCatalogIds) {
      const match = visibleCatalogModels.find(m => m.item.id === id
        || m.item.actionValue === id
        || m.item.id.includes(id));
      if (match?.item.family) fams.add(match.item.family);
    }
    return fams;
  }, [suggestedCatalogIds, visibleCatalogModels]);
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
    if (!prose.trim()) return;
    setLoading(true);
    setAction('refine');
    setError(null);
    setSuccess(null);
    try {
      const result = await refineDefinition(prose.trim(), {
        workflowId: payload?.workflow?.id ?? workflowId,
        title: payload?.workflow?.name,
      });
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
      const result = await compileDefinition(prose.trim(), {
        workflowId: payload?.workflow?.id ?? workflowId,
        title: payload?.workflow?.name,
      });
      adoptBuildPayload(result);
      const createdWorkflowId = result.workflow?.id;
      if (createdWorkflowId && createdWorkflowId !== workflowId) onWorkflowCreated?.(createdWorkflowId);
      setSuccess('Compiled');
      setProse('');
    } catch (e: any) {
      setError(e.message || 'Compilation failed');
    } finally {
      setLoading(false);
    }
  }, [adoptBuildPayload, onWorkflowCreated, payload?.workflow?.name, prose, workflowId]);

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
        <div className="moon-action__save-card">
          <div className="moon-dock__section-label">Save</div>
          <div className="moon-action__save-row">
            <span className="moon-action__state-label">State</span>
            <span className="moon-action__state-value">{buildState}</span>
          </div>
          <button className="moon-dock-form__btn moon-action__save-button" onClick={handleCommit} disabled={loading || (!hasDefinition && !hasGraphSteps)}>
            {loading && action === 'commit' ? 'Saving...' : 'Save draft'}
          </button>
        </div>
      )}

      {error && <div className="moon-dock-form__error">{error}</div>}
      {success && <div className="moon-action__success">{success}</div>}

      {/* Draggable catalog — drag items onto chain nodes or edges */}
      <div className="moon-action__catalog-section">
        <MoonSurfaceReviewPanel
          catalogItems={catalogEnvelope.items}
          sourcePolicies={catalogEnvelope.sourcePolicies}
          onCatalogReload={handleCatalogReload}
        />
        <MoonIntegrationsPanel />
        <MoonDataDictionaryPanel />
        <div className="moon-action__catalog-header">
          <div>
            <div className="moon-dock__section-label">Catalog</div>
            <div className="moon-action__catalog-subtitle">
              {primaryCatalogAll.length} primary · {surfaceStats.stepCore}/{surfaceStats.stepTotal} step · {surfaceStats.gateCore}/{surfaceStats.gateTotal} gate
              {(surfaceStats.stepOther + surfaceStats.gateOther) > 0 && ` · ${surfaceStats.stepOther + surfaceStats.gateOther} other`}
            </div>
          </div>
          {familyFilter && (
            <button
              type="button"
              className="moon-grammar-rail__clear"
              onClick={() => setFamilyFilter(null)}
              aria-label="Clear family filter"
            >
              all families
            </button>
          )}
        </div>

        {/* Grammar rail — the workflow vocabulary as a left-to-right flow.
            trigger → gather → think → act → control. Each chip scopes the
            catalog grid. Suggested families get a subtle pulse to bias the
            builder toward the next natural step. */}
        <div className="moon-grammar-rail" role="tablist" aria-label="Catalog families">
          {DOCK_FAMILIES.map((family, i) => {
            const count = familyCounts[family] ?? 0;
            const isActive = familyFilter === family;
            const isAvailable = count > 0 || filterableFamilies.includes(family);
            const isSuggested = suggestedFamilies.has(family);
            const cls = [
              'moon-grammar-rail__chip',
              `moon-grammar-rail__chip--${family}`,
              isActive ? 'moon-grammar-rail__chip--active' : '',
              isSuggested && !isActive ? 'moon-grammar-rail__chip--suggested' : '',
              !isAvailable ? 'moon-grammar-rail__chip--empty' : '',
            ].filter(Boolean).join(' ');
            return (
              <React.Fragment key={family}>
                {i > 0 && <span className="moon-grammar-rail__link" aria-hidden="true" />}
                <button
                  type="button"
                  role="tab"
                  aria-selected={isActive}
                  disabled={!isAvailable}
                  className={cls}
                  onClick={() => setFamilyFilter(isActive ? null : family)}
                  title={`${FAMILY_LABELS[family]} — ${count} item${count === 1 ? '' : 's'}`}
                >
                  <span className="moon-grammar-rail__label">{FAMILY_LABELS[family]}</span>
                  <span className="moon-grammar-rail__count">{count}</span>
                </button>
              </React.Fragment>
            );
          })}
        </div>

        {suggestedLoading && (
          <div className="moon-action__suggestion-loading">
            <span className="moon-spinner" /> Finding suggestions...
          </div>
        )}
        {suggestedCatalogIds.length > 0 && !familyFilter && (
          <div className="moon-action__suggestions">
            <div className="moon-dock__section-label">Suggested next</div>
            <div className="moon-dock__catalog-grid">
              {suggestedCatalogIds.map(id => {
                const model = primaryCatalog.find(m => m.item.id === id || m.item.actionValue === id || m.item.id.includes(id));
                if (!model) return null;
                return renderCatalogButton(model.item, model.policy.detail, model.truth.badge, model.truth.category);
              })}
            </div>
          </div>
        )}

        {primaryCatalog.length > 0 && (
          <>
            <div className="moon-dock__catalog-grid moon-action__primary-catalog-grid">
              {primaryCatalog.map(({ item, truth, policy }) => renderCatalogButton(item, policy.detail, truth.badge, truth.category))}
            </div>
          </>
        )}
      </div>
    </>
  );
}
