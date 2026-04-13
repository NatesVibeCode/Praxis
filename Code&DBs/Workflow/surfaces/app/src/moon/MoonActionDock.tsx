import React, { useState, useCallback, useEffect } from 'react';
import { compileDefinition, refineDefinition, commitDefinition } from '../shared/buildController';
import type { BuildPayload } from '../shared/types';
import { loadCatalog, getCatalog, FAMILY_LABELS } from './catalog';
import type { CatalogItem, CatalogFamily } from './catalog';
import { MoonGlyph } from './MoonGlyph';

interface Props {
  workflowId: string | null;
  payload: BuildPayload | null;
  onReload: () => void;
  onClose: () => void;
}

const DOCK_FAMILIES: CatalogFamily[] = ['trigger', 'gather', 'think', 'act', 'control'];

export function MoonActionDock({ workflowId, payload, onReload, onClose }: Props) {
  const [prose, setProse] = useState('');
  const [loading, setLoading] = useState(false);
  const [action, setAction] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<CatalogItem[]>(getCatalog());
  const [familyFilter, setFamilyFilter] = useState<CatalogFamily | null>(null);

  useEffect(() => { loadCatalog().then(setCatalog); }, []);

  const filteredCatalog = familyFilter
    ? catalog.filter(c => c.family === familyFilter && c.status === 'ready')
    : catalog.filter(c => c.status === 'ready');

  const hasDefinition = !!(payload?.definition && Object.keys(payload.definition).length > 0);
  const buildState = payload?.build_state || 'draft';

  const handleRefine = useCallback(async () => {
    if (!prose.trim() || !payload?.definition) return;
    setLoading(true);
    setAction('refine');
    setError(null);
    setSuccess(null);
    try {
      await refineDefinition(prose.trim(), payload.definition);
      setSuccess('Definition refined');
      setProse('');
      onReload();
    } catch (e: any) {
      setError(e.message || 'Refinement failed');
    } finally {
      setLoading(false);
    }
  }, [prose, payload, onReload]);

  const handleCompile = useCallback(async () => {
    if (!prose.trim()) return;
    setLoading(true);
    setAction('compile');
    setError(null);
    setSuccess(null);
    try {
      await compileDefinition(prose.trim());
      setSuccess('Compiled');
      setProse('');
      onReload();
    } catch (e: any) {
      setError(e.message || 'Compilation failed');
    } finally {
      setLoading(false);
    }
  }, [prose, onReload]);

  const handleCommit = useCallback(async () => {
    if (!workflowId) return;
    setLoading(true);
    setAction('commit');
    setError(null);
    setSuccess(null);
    try {
      const title = payload?.workflow?.name || workflowId;
      const definition = (payload?.definition && Object.keys(payload.definition).length > 0)
        ? payload.definition as Record<string, unknown>
        : { draft_flow: [], execution_setup: { phases: [] } };
      await commitDefinition(workflowId, { title, definition });
      setSuccess('Saved');
      onReload();
    } catch (e: any) {
      setError(e.message || 'Save failed');
    } finally {
      setLoading(false);
    }
  }, [workflowId, payload, onReload]);

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

      {hasDefinition && (
        <div style={{ marginTop: 16 }}>
          <div className="moon-dock__section-label">Save</div>
          <div className="moon-dock__item-desc">State: {buildState}</div>
          <button className="moon-dock-form__btn" onClick={handleCommit} disabled={loading || !workflowId}>
            {loading && action === 'commit' ? 'Saving...' : 'Save draft'}
          </button>
        </div>
      )}

      {error && <div className="moon-dock-form__error">{error}</div>}
      {success && <div className="moon-action__success">{success}</div>}

      {/* Draggable catalog — drag items onto chain nodes or edges */}
      <div style={{ marginTop: 20 }}>
        <div className="moon-dock__section-label">Catalog — drag onto chain</div>
        <div className="moon-catalog__filters">
          {DOCK_FAMILIES.map(f => (
            <button
              key={f}
              className={`moon-catalog__filter${familyFilter === f ? ' moon-catalog__filter--active' : ''}`}
              onClick={() => setFamilyFilter(familyFilter === f ? null : f)}
            >{FAMILY_LABELS[f]}</button>
          ))}
        </div>
        <div className="moon-dock__catalog-grid">
          {filteredCatalog.map(item => (
            <div
              key={item.id}
              className="moon-dock__catalog-item"
              draggable
              onDragStart={e => {
                e.dataTransfer.setData('moon/catalog-id', item.id);
                e.dataTransfer.setData('text/plain', item.label);
                e.dataTransfer.effectAllowed = 'copyLink';
              }}
              title={item.description}
            >
              <MoonGlyph type={item.icon} size={14} />
              <span>{item.label}</span>
            </div>
          ))}
        </div>
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
