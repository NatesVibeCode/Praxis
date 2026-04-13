import React, { useState, useCallback, useEffect } from 'react';
import type { OrbitNode, OrbitEdge, DockContent } from './moonBuildPresenter';
import type { BindingLedgerEntry } from '../shared/types';
import type { CatalogItem } from './catalog';
import { MoonGlyph } from './MoonGlyph';
import { useObjectTypes } from '../shared/hooks/useObjectTypes';
import type { ObjectType } from '../shared/hooks/useObjectTypes';

interface Props {
  node: OrbitNode | null;
  content: DockContent | null;
  workflowId: string | null;
  onMutate: (subpath: string, body: Record<string, unknown>) => Promise<void>;
  onClose: () => void;
  selectedEdge?: OrbitEdge | null;
  edgeFromLabel?: string;
  edgeToLabel?: string;
  onApplyGate?: (edgeId: string, gateFamily: string) => void;
  gateItems?: CatalogItem[];
}

export function MoonNodeDetail({ node, content, workflowId, onMutate, onClose, selectedEdge, edgeFromLabel, edgeToLabel, onApplyGate, gateItems = [] }: Props) {
  const { objectTypes, loading: objectTypesLoading } = useObjectTypes();
  const [objectSearch, setObjectSearch] = useState('');
  const [attachKind, setAttachKind] = useState('reference');
  const [attachRef, setAttachRef] = useState('');
  const [attachLabel, setAttachLabel] = useState('');
  const [attachRole, setAttachRole] = useState('input');
  const [attachPromote, setAttachPromote] = useState(false);
  const [attachLoading, setAttachLoading] = useState(false);
  const [attachError, setAttachError] = useState<string | null>(null);

  const [importLocator, setImportLocator] = useState('');
  const [importLabel, setImportLabel] = useState('');
  const [importLoading, setImportLoading] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);

  const handleAttach = useCallback(async () => {
    if (!node || !attachRef.trim()) return;
    setAttachLoading(true);
    setAttachError(null);
    try {
      await onMutate('attachments', {
        node_id: node.id,
        authority_kind: attachKind,
        authority_ref: attachRef.trim(),
        role: attachRole,
        label: attachLabel.trim() || attachRef.trim(),
        promote_to_state: attachPromote,
      });
      setAttachRef('');
      setAttachLabel('');
    } catch (e: any) {
      setAttachError(e.message || 'Failed to attach');
    } finally {
      setAttachLoading(false);
    }
  }, [node, attachKind, attachRef, attachLabel, attachRole, attachPromote, onMutate]);

  const handleStageImport = useCallback(async () => {
    if (!node || !importLocator.trim()) return;
    setImportLoading(true);
    setImportError(null);
    const label = importLabel.trim() || importLocator.trim().split('/').pop() || 'import';
    try {
      await onMutate('imports', {
        node_id: node.id,
        source_kind: 'net',
        source_locator: importLocator.trim(),
        requested_shape: {
          label,
          target_ref: `#${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`,
          kind: 'type',
        },
        payload: { note: `Requested from ${importLocator.trim()}` },
      });
      setImportLocator('');
      setImportLabel('');
    } catch (e: any) {
      setImportError(e.message || 'Failed to stage import');
    } finally {
      setImportLoading(false);
    }
  }, [node, importLocator, importLabel, onMutate]);

  const handleAdmitImport = useCallback(async (snapshotId: string, shape: Record<string, unknown>) => {
    setImportLoading(true);
    setImportError(null);
    try {
      await onMutate(`imports/${snapshotId}/admit`, {
        admitted_target: {
          target_ref: shape.target_ref || `#${snapshotId}`,
          label: shape.label || snapshotId,
          kind: shape.kind || 'type',
        },
      });
    } catch (e: any) {
      setImportError(e.message || 'Failed to admit import');
    } finally {
      setImportLoading(false);
    }
  }, [onMutate]);

  const handleMaterialize = useCallback(async () => {
    if (!node || !importLocator.trim()) return;
    setImportLoading(true);
    setImportError(null);
    const label = importLabel.trim() || importLocator.trim().split('/').pop() || 'import';
    try {
      await onMutate('materialize-here', {
        node_id: node.id,
        source_kind: 'net',
        source_locator: importLocator.trim(),
        requested_shape: {
          label,
          target_ref: `#${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`,
          kind: 'type',
        },
        authority_kind: attachKind,
        authority_ref: `#${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`,
        role: attachRole,
        label,
        promote_to_state: attachPromote,
      });
      setImportLocator('');
      setImportLabel('');
    } catch (e: any) {
      setImportError(e.message || 'Failed to materialize');
    } finally {
      setImportLoading(false);
    }
  }, [node, importLocator, importLabel, attachKind, attachRole, attachPromote, onMutate]);

  // Sort: unresolved first, then accepted, then rejected
  const bindings = [...(content?.connectBindings || [])].sort((a, b) => {
    const order: Record<string, number> = { unresolved: 0, accepted: 1, rejected: 2 };
    return (order[a.state || 'unresolved'] || 0) - (order[b.state || 'unresolved'] || 0);
  });

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close detail panel">&times;</button>

      {/* Gate config — when an edge is selected */}
      {selectedEdge && onApplyGate ? (
        <>
          <div className="moon-dock__title">Gate</div>
          <div className="moon-dock__subtitle">{edgeFromLabel} &rarr; {edgeToLabel}</div>
          <div className="moon-dock__sep" />
          <div className="moon-dock__section-label">
            {selectedEdge.gateState === 'empty' ? 'Add a gate' : `Gate: ${selectedEdge.gateLabel || selectedEdge.gateState}`}
          </div>
          <div className="moon-dock__catalog-grid">
            {gateItems.map(item => (
              <button
                key={item.id}
                className={`moon-dock__catalog-item${selectedEdge.gateFamily === item.gateFamily ? ' moon-dock__catalog-item--active' : ''}`}
                onClick={() => item.gateFamily && onApplyGate?.(selectedEdge.id, item.gateFamily)}
                draggable
                onDragStart={e => {
                  e.dataTransfer.setData('moon/catalog-id', item.id);
                  e.dataTransfer.setData('text/plain', item.label);
                  e.dataTransfer.effectAllowed = 'copyLink';
                }}
              >
                <MoonGlyph type={item.icon} size={14} />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        </>
      ) : (
        <>
          <div className="moon-dock__title">{node ? node.title : 'Detail'}</div>
        </>
      )}

      {!node && !selectedEdge ? (
        <div className="moon-dock__empty">Select a node or gate.</div>
      ) : node ? (
        <>
          {/* Attached */}
          <div className="moon-dock__sep" />
          <div className="moon-dock__section-label">
            Attached ({content?.contextAttachments?.length || 0})
          </div>
          {(content?.contextAttachments || []).map(a => (
            <div
              key={a.attachment_id}
              className="moon-dock__item"
              draggable
              onDragStart={e => {
                e.dataTransfer.setData('moon/dock', 'context');
                e.dataTransfer.setData('text/plain', a.label || a.authority_ref || '');
                e.dataTransfer.effectAllowed = 'link';
              }}
            >
              <div className="moon-dock__item-title">{a.label || a.authority_ref || 'Reference'}</div>
              <div className="moon-dock__item-desc">{a.authority_kind} / {a.role || 'input'}</div>
            </div>
          ))}

          {/* Imports */}
          <div className="moon-dock__section-label" style={{ marginTop: 16 }}>
            Imports ({content?.imports?.length || 0})
          </div>
          {(content?.imports || []).map(s => (
            <div key={s.snapshot_id} className="moon-dock__item">
              <div className="moon-dock__item-title">{s.source_locator || s.snapshot_id}</div>
              <div className="moon-dock__item-desc">
                {s.approval_state === 'admitted' ? 'Admitted' : 'Staged'}
                {s.approval_state !== 'admitted' && (
                  <button
                    className="moon-dock-form__btn--small"
                    onClick={() => handleAdmitImport(s.snapshot_id, (s as any).requested_shape || {})}
                  >
                    Approve
                  </button>
                )}
              </div>
            </div>
          ))}

          {/* Bindings */}
          <div className="moon-dock__section-label" style={{ marginTop: 16 }}>
            Bindings ({bindings.length})
          </div>
          {bindings.map(binding => (
            <BindingCard key={binding.binding_id} binding={binding} onMutate={onMutate} />
          ))}
          {!bindings.length && (
            <div className="moon-dock__empty">No bindings.</div>
          )}

          {/* DB Objects — drag onto chain nodes to attach */}
          <div className="moon-dock__sep" style={{ marginTop: 20 }} />
          <div className="moon-dock__section-label">
            DB Objects ({objectTypes.length})
          </div>
          {objectTypes.length > 6 && (
            <input
              className="moon-dock-form__input"
              type="text"
              value={objectSearch}
              onChange={e => setObjectSearch(e.target.value)}
              placeholder="Filter objects..."
              style={{ marginBottom: 8, fontSize: 12, padding: '5px 8px' }}
            />
          )}
          {objectTypesLoading ? (
            <div className="moon-dock__empty"><span className="moon-spinner" /> Loading...</div>
          ) : objectTypes.length === 0 ? (
            <div className="moon-dock__empty">No object types registered.</div>
          ) : (
            <div className="moon-dock__catalog-grid">
              {objectTypes
                .filter(ot => !objectSearch || ot.name.toLowerCase().includes(objectSearch.toLowerCase()))
                .slice(0, 20)
                .map(ot => (
                  <div
                    key={ot.type_id}
                    className="moon-dock__catalog-item"
                    draggable
                    onDragStart={e => {
                      e.dataTransfer.setData('moon/object-type-id', ot.type_id);
                      e.dataTransfer.setData('moon/object-type-label', ot.name);
                      e.dataTransfer.setData('text/plain', ot.name);
                      e.dataTransfer.effectAllowed = 'copyLink';
                    }}
                    onClick={() => {
                      if (!node) return;
                      onMutate('attachments', {
                        node_id: node.id,
                        authority_kind: 'object_type',
                        authority_ref: ot.type_id,
                        role: 'input',
                        label: ot.name,
                        promote_to_state: false,
                      }).catch(() => {});
                    }}
                    title={ot.description || `Attach ${ot.name} to this step`}
                  >
                    <span style={{ fontSize: 14 }}>{ot.icon || '◆'}</span>
                    <span>{ot.name}</span>
                  </div>
                ))}
            </div>
          )}

          {/* Attach reference form */}
          <div className="moon-dock__sep" style={{ marginTop: 20 }} />
          <div className="moon-dock__section-label">Attach reference</div>

          <div className="moon-dock-form__row">
            <select className="moon-dock-form__select" value={attachKind} onChange={e => setAttachKind(e.target.value)}>
              <option value="reference">Reference</option>
              <option value="object_type">Object Type</option>
              <option value="object">Object</option>
            </select>
            <select className="moon-dock-form__select" value={attachRole} onChange={e => setAttachRole(e.target.value)}>
              <option value="input">Input</option>
              <option value="evidence">Evidence</option>
              <option value="state_dependency">State dep</option>
              <option value="output">Output</option>
            </select>
          </div>

          <input className="moon-dock-form__input" type="text" value={attachRef} onChange={e => setAttachRef(e.target.value)} placeholder="Authority ref (slug or ID)" />
          <input className="moon-dock-form__input" type="text" value={attachLabel} onChange={e => setAttachLabel(e.target.value)} placeholder="Label (optional)" />

          <label className="moon-dock-form__checkbox">
            <input type="checkbox" checked={attachPromote} onChange={e => setAttachPromote(e.target.checked)} />
            Promote to state node
          </label>

          <button className="moon-dock-form__btn" onClick={handleAttach} disabled={attachLoading || !attachRef.trim()}>
            {attachLoading ? <><span className="moon-spinner" /> Attaching...</> : 'Attach'}
          </button>
          {attachError && <div className="moon-dock-form__error">{attachError}</div>}

          {/* Stage import form */}
          <div className="moon-dock__sep" style={{ marginTop: 20 }} />
          <div className="moon-dock__section-label">Stage import</div>

          <input className="moon-dock-form__input" type="text" value={importLocator} onChange={e => setImportLocator(e.target.value)} placeholder="URL or source locator" />
          <input className="moon-dock-form__input" type="text" value={importLabel} onChange={e => setImportLabel(e.target.value)} placeholder="Label (optional)" />

          <div className="moon-dock-form__row">
            <button className="moon-dock-form__btn" onClick={handleStageImport} disabled={importLoading || !importLocator.trim()}>
              {importLoading ? <><span className="moon-spinner" /> Staging...</> : 'Stage'}
            </button>
            <button className="moon-dock-form__btn" onClick={handleMaterialize} disabled={importLoading || !importLocator.trim()}>
              Materialize here
            </button>
          </div>
          {importError && <div className="moon-dock-form__error">{importError}</div>}
        </>
      ) : null}
    </>
  );
}

function BindingCard({ binding, onMutate }: { binding: BindingLedgerEntry; onMutate: Props['onMutate'] }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showReplace, setShowReplace] = useState(false);
  const [replaceRef, setReplaceRef] = useState('');

  const isAccepted = binding.state === 'accepted';
  const isRejected = binding.state === 'rejected';

  const handleAccept = useCallback(async (target: { target_ref?: string; label?: string; kind?: string }) => {
    setLoading(true);
    setError(null);
    try {
      await onMutate(`bindings/${binding.binding_id}/accept`, {
        accepted_target: target,
        rationale: 'Accepted from Moon Build.',
      });
    } catch (e: any) {
      setError(e.message || 'Failed to accept');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, onMutate]);

  const handleReplace = useCallback(async () => {
    if (!replaceRef.trim()) return;
    setLoading(true);
    setError(null);
    try {
      await onMutate(`bindings/${binding.binding_id}/replace`, {
        accepted_target: {
          target_ref: replaceRef.trim(),
          label: replaceRef.trim(),
          kind: 'custom',
        },
        rationale: 'Replaced from Moon Build.',
      });
      setShowReplace(false);
      setReplaceRef('');
    } catch (e: any) {
      setError(e.message || 'Failed to replace');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, replaceRef, onMutate]);

  const handleReject = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await onMutate(`bindings/${binding.binding_id}/reject`, {
        rationale: 'Rejected from Moon Build.',
      });
    } catch (e: any) {
      setError(e.message || 'Failed to reject');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, onMutate]);

  const stateColor = isAccepted ? '#3fb950' : isRejected ? 'var(--moon-error)' : 'var(--moon-fg-muted)';

  return (
    <div className={`moon-dock__item${isRejected ? ' moon-dock__item--muted' : ''}`}>
      <div className="moon-dock__item-title">
        {binding.source_label || binding.binding_id}
        <span style={{ marginLeft: 8, fontSize: 11, color: stateColor }}>
          {isAccepted && binding.accepted_target?.enrichment
            ? `Connected to ${binding.accepted_target.enrichment.integration_name}`
            : isRejected ? 'Skipped'
            : binding.state || 'Pick a connector'}
        </span>
      </div>

      {isAccepted && binding.accepted_target && (
        <div className="moon-dock__item-desc">
          {binding.accepted_target.enrichment ? (
            <>
              {binding.accepted_target.enrichment.integration_name}
              {binding.accepted_target.enrichment.auth_status && (
                <span style={{ marginLeft: 6, fontSize: 10, color: binding.accepted_target.enrichment.auth_status === 'connected' ? '#3fb950' : 'var(--moon-fg-muted)' }}>
                  ({binding.accepted_target.enrichment.auth_status})
                </span>
              )}
              {binding.accepted_target.enrichment.description && (
                <div style={{ fontSize: 11, color: 'var(--moon-fg-muted)', marginTop: 2 }}>
                  {binding.accepted_target.enrichment.description}
                </div>
              )}
            </>
          ) : (
            <>Bound to: {binding.accepted_target.label || binding.accepted_target.target_ref}</>
          )}
        </div>
      )}

      {/* Candidate targets */}
      {!isAccepted && !isRejected && binding.candidate_targets?.length ? (
        <div style={{ marginTop: 8 }}>
          <div className="moon-dock__section-label">Pick a target:</div>
          {binding.candidate_targets.map((target, i) => (
            <button
              key={i}
              className="moon-dock__item"
              onClick={() => handleAccept(target)}
              disabled={loading}
              draggable={!loading}
              onDragStart={e => {
                e.dataTransfer.setData('moon/dock', 'context');
                e.dataTransfer.setData('text/plain', target.label || target.target_ref || '');
                e.dataTransfer.effectAllowed = 'link';
              }}
              style={{ display: 'block', width: '100%', textAlign: 'left', padding: '6px 10px', marginBottom: 4 }}
            >
              {target.enrichment?.integration_name || target.label || target.target_ref || 'Target'}
              {target.enrichment?.auth_status ? (
                <span className="moon-dock__item-desc" style={{ marginLeft: 6, color: target.enrichment.auth_status === 'connected' ? '#3fb950' : 'var(--moon-fg-muted)' }}>
                  ({target.enrichment.auth_status})
                </span>
              ) : target.kind ? (
                <span className="moon-dock__item-desc" style={{ marginLeft: 6 }}>({target.kind})</span>
              ) : null}
            </button>
          ))}
        </div>
      ) : null}

      {/* Replace / Reject actions */}
      {!isAccepted && !isRejected && (
        <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
          {!showReplace ? (
            <button className="moon-dock-form__btn--small" onClick={() => setShowReplace(true)}>
              Custom target
            </button>
          ) : (
            <>
              <input className="moon-dock-form__input" style={{ marginBottom: 0, flex: 1 }} type="text" value={replaceRef} onChange={e => setReplaceRef(e.target.value)} placeholder="Target ref" />
              <button className="moon-dock-form__btn" onClick={handleReplace} disabled={loading || !replaceRef.trim()}>
                {loading ? '...' : 'Use'}
              </button>
              <button className="moon-dock-form__btn--small" onClick={() => setShowReplace(false)}>Cancel</button>
            </>
          )}
          <button className="moon-dock-form__btn--small" style={{ color: 'var(--moon-error)', borderColor: 'var(--moon-error)' }} onClick={handleReject} disabled={loading}>
            Reject
          </button>
        </div>
      )}

      {error && <div className="moon-dock-form__error">{error}</div>}
    </div>
  );
}
