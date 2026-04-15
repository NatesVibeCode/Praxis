import React, { Suspense, useCallback, useEffect, useState } from 'react';
import type { PraxisSurfaceBundleV4 } from '../praxis/manifest';
import { resolveModule } from '../modules/moduleRegistry';
import { ALL_CELLS, cellIdFromRowCol, getOccupiedCells, parseQuadrantId, parseSpan } from './quadrantUtils';
import { ConfigEditorPanel } from './ConfigEditorPanel';
import { useManifestOverlay } from '../hooks/useManifestOverlay';
import { world } from '../world';
import { ModulePalette } from './ModulePalette';
import { SaveLayoutBar } from './SaveLayoutBar';
import { Toast, useToast } from '../primitives/Toast';
import { DetailSlidePanel } from './DetailSlidePanel';
import { useGridDrag, type GridDragPayload, type GridDragValidator } from './useGridDrag';
import { ModuleActionMenu } from './ModuleActionMenu';
import { UiActionFeed } from '../control/UiActionFeed';
import type { UiActionTarget } from '../control/uiActionLedger';
import { runUiAction, undoUiAction } from '../control/uiActionLedger';
import { gridFieldLabel } from './moduleConfigMetadata';
import './QuadrantGrid.css';

const GRID_UNDO_SCOPE = 'grid.layout';

export interface QuadrantManifest {
  version: number;
  grid: string;
  title?: string;
  quadrants: Record<
    string,
    { module: string; span?: string; config?: Record<string, unknown> }
  >;
}

interface QuadrantSaveTarget {
  manifestId: string;
  name?: string | null;
  description?: string | null;
  bundle?: PraxisSurfaceBundleV4 | null;
  surfaceId?: string | null;
}

// Error boundary so one broken module doesn't take down the grid
class ModuleErrorBoundary extends React.Component<
  { moduleId: string; children: React.ReactNode },
  { error: string | null }
> {
  state = { error: null as string | null };

  static getDerivedStateFromError(err: Error) {
    return { error: err.message };
  }

  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 'var(--space-md)', color: 'var(--danger)', fontSize: 12 }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>{this.props.moduleId}</div>
          <div style={{ color: 'var(--text-muted)' }}>{this.state.error}</div>
        </div>
      );
    }
    return this.props.children;
  }
}

function moduleDisplayName(moduleId: string): string {
  return resolveModule(moduleId)?.name ?? moduleId;
}

function quadrantTarget(quadrantId: string, moduleId?: string): UiActionTarget {
  return {
    kind: 'quadrant',
    label: moduleId ? `${quadrantId} · ${moduleDisplayName(moduleId)}` : quadrantId,
    id: quadrantId,
  };
}

function configValuesEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    return left.every((value, index) => configValuesEqual(value, right[index]));
  }
  if (
    left
    && right
    && typeof left === 'object'
    && typeof right === 'object'
  ) {
    const leftEntries = Object.entries(left as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    const rightEntries = Object.entries(right as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    if (leftEntries.length !== rightEntries.length) return false;
    return leftEntries.every(([key, value], index) => (
      key === rightEntries[index]?.[0]
      && configValuesEqual(value, rightEntries[index]?.[1])
    ));
  }
  return false;
}

function summarizeConfigChanges(
  previousConfig: Record<string, unknown> | undefined,
  nextConfig: Record<string, unknown>,
): string[] {
  const previousKeys = new Set(Object.keys(previousConfig || {}));
  const nextKeys = new Set(Object.keys(nextConfig));
  const changedKeys = new Set<string>();

  for (const key of nextKeys) {
    const before = previousConfig?.[key];
    const after = nextConfig[key];
    if (!configValuesEqual(before, after)) changedKeys.add(key);
  }
  for (const key of previousKeys) {
    if (!nextKeys.has(key)) changedKeys.add(key);
  }

  return Array.from(changedKeys).map(gridFieldLabel).slice(0, 3);
}

export function QuadrantGrid({
  manifest: initialManifest,
  saveTarget,
}: {
  manifest: QuadrantManifest;
  saveTarget?: QuadrantSaveTarget | null;
}) {
  const manifest = useManifestOverlay(initialManifest);
  const occupied = getOccupiedCells(manifest.quadrants);
  const [editingQuadrant, setEditingQuadrant] = useState<{ quadrantId: string; focusKey?: string | null } | null>(null);
  const [showPalette, setShowPalette] = useState(false);
  const [justPlaced, setJustPlaced] = useState<string | null>(null);
  const [actionMenu, setActionMenu] = useState<{
    anchorRect: DOMRect | null;
    quadrantId: string;
    moduleId: string;
    moduleType: string;
  } | null>(null);
  const { show } = useToast();

  const applyQuadrantMutation = useCallback((details: {
    label: string;
    reason: string;
    outcome: string;
    nextQuadrants: QuadrantManifest['quadrants'];
    target?: UiActionTarget | null;
    changeSummary?: string[];
    afterApply?: () => void;
  }) => {
    void (async () => {
      const previousQuadrants = structuredClone(manifest.quadrants);
      const nextQuadrants = structuredClone(details.nextQuadrants);
      const entry = await runUiAction({
        surface: 'grid',
        undoScope: GRID_UNDO_SCOPE,
        category: 'layout',
        label: details.label,
        authority: 'ui.layout.quadrants',
        reason: details.reason,
        outcome: details.outcome,
        target: details.target ?? null,
        changeSummary: details.changeSummary,
        apply: () => {
          world.propose('ui.layout.quadrants', nextQuadrants);
          details.afterApply?.();
        },
        undoDescriptor: {
          kind: 'world.propose',
          path: 'ui.layout.quadrants',
          value: previousQuadrants,
        },
        onUndone: () => {
          setActionMenu(null);
        },
      });

      show(`${details.label}: ${details.outcome}`, 'info', {
        actionLabel: 'Undo',
        durationMs: 5000,
        onAction: () => {
          void (async () => {
            const result = await undoUiAction(entry.id);
            if (!result.ok) {
              show(result.error || 'Undo failed.', 'error');
              return;
            }
            show(`Undid ${details.label}.`, 'success');
          })();
        },
      });
    })();
  }, [manifest.quadrants, show]);

  const saveQuadrantConfig = useCallback((
    quadrantId: string,
    nextConfig: Record<string, unknown>,
    meta?: {
      label: string;
      reason: string;
      outcome: string;
      target?: UiActionTarget | null;
      changeSummary?: string[];
    },
  ) => {
    const current = manifest.quadrants[quadrantId];
    if (!current) return;
    applyQuadrantMutation({
      label: meta?.label ?? 'Update module config',
      reason: meta?.reason ?? `Update settings for quadrant ${quadrantId}.`,
      outcome: meta?.outcome ?? `Quadrant ${quadrantId} now uses the new config draft.`,
      nextQuadrants: {
        ...manifest.quadrants,
        [quadrantId]: {
          ...current,
          config: nextConfig,
        },
      },
      target: meta?.target ?? quadrantTarget(quadrantId, current.module),
      changeSummary: meta?.changeSummary ?? summarizeConfigChanges(current.config, nextConfig),
    });
  }, [applyQuadrantMutation, manifest.quadrants]);

  const removeQuadrant = useCallback((quadrantId: string) => {
    const existing = manifest.quadrants[quadrantId];
    if (!existing) return;
    const updated = { ...manifest.quadrants };
    delete updated[quadrantId];
    applyQuadrantMutation({
      label: 'Remove module',
      reason: `Clear quadrant ${quadrantId} and remove ${existing.module}.`,
      outcome: `Quadrant ${quadrantId} is empty again.`,
      nextQuadrants: updated,
      target: quadrantTarget(quadrantId, existing.module),
      changeSummary: ['Removed module'],
      afterApply: () => {
        setActionMenu(null);
        setEditingQuadrant((current) => (current?.quadrantId === quadrantId ? null : current));
      },
    });
  }, [applyQuadrantMutation, manifest.quadrants]);

  // Span-aware validation: checks if a payload's span fits at the target cell
  const validateDrop: GridDragValidator = useCallback((payload, targetCellId) => {
    const spanStr = (payload.data.span as string) || '1x1';
    const { cols, rows } = parseSpan(spanStr);
    const { row: targetRow, col: targetCol } = parseQuadrantId(targetCellId);

    // Check bounds
    if (targetRow + rows > 4 || targetCol + cols > 4) return false;

    // For cell moves, the source cell's footprint is allowed
    const sourceCellId = payload.kind === 'cell' ? (payload.data.cellId as string) : null;
    const sourceFootprint = new Set<string>();
    if (sourceCellId && manifest.quadrants[sourceCellId]) {
      const srcSpan = manifest.quadrants[sourceCellId].span || '1x1';
      const { cols: sc, rows: sr } = parseSpan(srcSpan);
      const { row: sRow, col: sCol } = parseQuadrantId(sourceCellId);
      for (let r = sRow; r < sRow + sr; r++) {
        for (let c = sCol; c < sCol + sc; c++) {
          const id = cellIdFromRowCol(r, c);
          if (id) sourceFootprint.add(id);
        }
      }
    }

    // Check each cell the new span would cover
    for (let r = targetRow; r < targetRow + rows; r++) {
      for (let c = targetCol; c < targetCol + cols; c++) {
        const id = cellIdFromRowCol(r, c);
        if (!id) return false;
        if (id === targetCellId) continue;
        if (sourceFootprint.has(id)) continue;
        if (occupied.has(id)) return false;
        if (manifest.quadrants[id]) return false;
      }
    }
    return true;
  }, [manifest.quadrants, occupied]);

  const handleDrop = useCallback((payload: GridDragPayload, targetId: string) => {
    if (payload.kind === 'preset') {
      const moduleId = payload.data.module as string;
      const moduleName = resolveModule(moduleId)?.name ?? moduleId;
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: moduleId,
        span: payload.data.span as string,
        config: payload.data.config as Record<string, unknown>,
      };
      applyQuadrantMutation({
        label: 'Add module',
        reason: `Place ${moduleName} in quadrant ${targetId}.`,
        outcome: `${moduleName} now occupies ${targetId}.`,
        nextQuadrants: updated,
        target: quadrantTarget(targetId, moduleId),
        changeSummary: ['Placed module', `Span ${(payload.data.span as string) || '1x1'}`],
        afterApply: () => {
          setShowPalette(false);
          setJustPlaced(targetId);
        },
      });
      return;
    }

    if (payload.kind === 'palette') {
      const moduleId = payload.data.module as string;
      const moduleName = resolveModule(moduleId)?.name ?? moduleId;
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: moduleId,
        span: payload.data.span as string,
      };
      applyQuadrantMutation({
        label: 'Add module',
        reason: `Place ${moduleName} in quadrant ${targetId}.`,
        outcome: `${moduleName} now occupies ${targetId}.`,
        nextQuadrants: updated,
        target: quadrantTarget(targetId, moduleId),
        changeSummary: ['Placed module', `Span ${(payload.data.span as string) || '1x1'}`],
        afterApply: () => {
          setShowPalette(false);
          setJustPlaced(targetId);
        },
      });
      return;
    }

    if (payload.kind === 'object') {
      const objectName = String(payload.data.objectName ?? payload.data.objectTypeId ?? 'object');
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: (payload.data.module as string) || 'object-view',
        span: (payload.data.span as string) || '1x1',
        config: {
          objectTypeId: payload.data.objectTypeId,
          objectName: payload.data.objectName,
        },
      };
      applyQuadrantMutation({
        label: 'Add object module',
        reason: `Place ${objectName} in quadrant ${targetId}.`,
        outcome: `${targetId} now opens ${objectName}.`,
        nextQuadrants: updated,
        target: quadrantTarget(targetId, (payload.data.module as string) || 'object-view'),
        changeSummary: ['Bound object type', objectName],
        afterApply: () => {
          setShowPalette(false);
          setJustPlaced(targetId);
        },
      });
      return;
    }

    if (payload.kind === 'cell') {
      const sourceId = payload.data.cellId as string;
      if (!sourceId || sourceId === targetId) return;

      const updated = { ...manifest.quadrants };
      const sourceDef = updated[sourceId];
      const targetDef = updated[targetId];
      if (!sourceDef) return;

      if (targetDef) {
        updated[sourceId] = { ...targetDef };
        updated[targetId] = { ...sourceDef };
        const sourceName = resolveModule(sourceDef.module)?.name ?? sourceDef.module;
        const targetName = resolveModule(targetDef.module)?.name ?? targetDef.module;
        applyQuadrantMutation({
          label: 'Swap modules',
          reason: `Swap ${sourceName} in ${sourceId} with ${targetName} in ${targetId}.`,
          outcome: `${sourceName} and ${targetName} traded quadrants.`,
          nextQuadrants: updated,
          target: {
            kind: 'quadrants',
            label: `${sourceId} ↔ ${targetId}`,
            id: `${sourceId}:${targetId}`,
          },
          changeSummary: [sourceName, targetName],
          afterApply: () => setJustPlaced(targetId),
        });
      } else {
        updated[targetId] = { ...sourceDef };
        delete updated[sourceId];
        const sourceName = resolveModule(sourceDef.module)?.name ?? sourceDef.module;
        applyQuadrantMutation({
          label: 'Move module',
          reason: `Move ${sourceName} from ${sourceId} to ${targetId}.`,
          outcome: `${sourceName} now occupies ${targetId}.`,
          nextQuadrants: updated,
          target: quadrantTarget(targetId, sourceDef.module),
          changeSummary: [`Moved from ${sourceId}`, `Into ${targetId}`],
          afterApply: () => setJustPlaced(targetId),
        });
      }
    }
  }, [applyQuadrantMutation, manifest.quadrants]);

  // Clear justPlaced after animation completes
  useEffect(() => {
    if (!justPlaced) return;
    const timer = setTimeout(() => setJustPlaced(null), 300);
    return () => clearTimeout(timer);
  }, [justPlaced]);

  const { drag, startDrag } = useGridDrag(handleDrop, validateDrop);

  // Ghost mode: hidden over valid targets (in-cell preview replaces it), red tint over invalid
  const ghostHidden = drag.active && drag.hoveredCell !== null && drag.valid;
  const ghostInvalid = drag.active && drag.hoveredCell !== null && !drag.valid;

  return (
    <div style={{ padding: 'var(--space-lg)', maxWidth: 1600, margin: '0 auto' }}>
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: 'var(--space-lg)' }}>
        {manifest.title && (
          <h1 style={{ fontSize: 20, fontWeight: 700, margin: 0, marginRight: 'var(--space-md)' }}>
            {manifest.title}
          </h1>
        )}
        <button
          type="button"
          onClick={() => setShowPalette(true)}
          style={{
            background: 'var(--accent)',
            color: 'var(--text-inverse)',
            border: 'none',
            borderRadius: '50%',
            width: 28,
            height: 28,
            fontSize: 20,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            padding: 0,
            lineHeight: 1
          }}
          title="Add Module"
        >
          +
        </button>
      </div>

      <SaveLayoutBar manifest={initialManifest} saveTarget={saveTarget} />
      <UiActionFeed
        surface="grid"
        scope={GRID_UNDO_SCOPE}
        subtitle="Latest grid actions with their authority, reason, outcome, and recovery status."
      />

      <ModulePalette isOpen={showPalette} onClose={() => setShowPalette(false)} startDrag={startDrag} />

      <div className="quadrant-grid">
        {ALL_CELLS.map(cellId => {
          if (occupied.has(cellId)) return null;

          const def = manifest.quadrants[cellId];
          const { row, col } = parseQuadrantId(cellId);
          const isHovered = drag.hoveredCell === cellId;

          if (!def) {
            return (
              <div
                key={cellId}
                className={[
                  'quadrant-empty',
                  isHovered ? 'drag-over' : '',
                  isHovered && !drag.valid ? 'invalid' : '',
                ].filter(Boolean).join(' ')}
                data-grid-drop={cellId}
                style={{
                  gridColumn: col + 1,
                  gridRow: row + 1,
                }}
              >
                {isHovered && drag.valid && drag.payload && (
                  <div className="drag-preview-placeholder">
                    <span className="drag-preview-label">{drag.payload.label}</span>
                    <span className="drag-preview-span">{(drag.payload.data.span as string) || '1x1'}</span>
                  </div>
                )}
              </div>
            );
          }

          const { cols, rows } = def.span
            ? parseSpan(def.span)
            : { cols: 1, rows: 1 };

          const moduleDef = resolveModule(def.module);
          const Module = moduleDef?.component;
          const openModuleActions = (anchorRect: DOMRect | null) => {
            setActionMenu({
              anchorRect,
              quadrantId: cellId,
              moduleId: def.module,
              moduleType: moduleDef?.type ?? 'display',
            });
          };

          return (
            <div
              key={cellId}
              className={[
                'quadrant-cell',
                isHovered ? 'drag-over' : '',
                isHovered && !drag.valid ? 'invalid' : '',
                justPlaced === cellId ? 'quadrant-cell-just-placed' : '',
              ].filter(Boolean).join(' ')}
              data-module-focusable="true"
              data-grid-drop={cellId}
              aria-label={moduleDef?.name ?? def.module}
              tabIndex={0}
              style={{
                gridColumn: `${col + 1} / span ${cols}`,
                gridRow: `${row + 1} / span ${rows}`,
                position: 'relative',
                cursor: 'grab',
              }}
              onPointerDown={(e) => {
                if ((e.target as HTMLElement).closest('button, input, textarea, a, [contenteditable]')) return;
                startDrag(e, {
                  kind: 'cell',
                  label: moduleDef?.name ?? def.module,
                  data: { cellId, span: def.span || '1x1' },
                });
              }}
              onContextMenu={(event) => {
                event.preventDefault();
                openModuleActions(new DOMRect(event.clientX, event.clientY, 1, 1));
              }}
              onKeyDown={(event) => {
                if (event.key === 'ContextMenu' || (event.shiftKey && event.key === 'F10')) {
                  event.preventDefault();
                  openModuleActions((event.currentTarget as HTMLDivElement).getBoundingClientRect());
                }
              }}
            >
              <button
                type="button"
                className="gear-icon"
                aria-label={`Open actions for ${moduleDef?.name ?? def.module}`}
                onClick={(event) => {
                  event.stopPropagation();
                  openModuleActions((event.currentTarget as HTMLButtonElement).getBoundingClientRect());
                }}
                style={{
                  position: 'absolute',
                  top: 'var(--space-sm)',
                  right: 'var(--space-sm)',
                  opacity: 0,
                  transition: 'opacity 0.2s',
                  zIndex: 10,
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  fontSize: 16
                }}
              >
                ⋯
              </button>
              {editingQuadrant?.quadrantId === cellId && (
                <div style={{ position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20 }}>
                  <ConfigEditorPanel
                    quadrantId={cellId}
                    moduleId={def.module}
                    config={def.config || {}}
                    focusKey={editingQuadrant.focusKey}
                    onSave={(newConfig) => {
                      saveQuadrantConfig(cellId, newConfig, {
                        label: 'Save module config',
                        reason: `Commit the config editor changes for quadrant ${cellId}.`,
                        outcome: `Quadrant ${cellId} now reflects the edited settings.`,
                      });
                      setEditingQuadrant(null);
                    }}
                    onClose={() => setEditingQuadrant(null)}
                  />
                </div>
              )}
              <ModuleErrorBoundary moduleId={def.module}>
                {Module ? (
                  <Suspense
                    fallback={(
                      <div style={{ padding: 'var(--space-md)', color: 'var(--text-muted)', fontSize: 12 }}>
                        Loading {moduleDef?.name ?? def.module}...
                      </div>
                    )}
                  >
                    <Module
                      quadrantId={cellId}
                      span={{ cols, rows }}
                      config={def.config ?? {}}
                    />
                  </Suspense>
                ) : (
                  <div style={{ padding: 'var(--space-md)', color: 'var(--text-muted)', fontSize: 12 }}>
                    Unknown: {def.module}
                  </div>
                )}
              </ModuleErrorBoundary>
            </div>
          );
        })}
      </div>

      {/* Drag ghost — hidden over valid targets, red over invalid */}
      {drag.active && drag.payload && !ghostHidden && (
        <div
          className={`grid-drag-ghost${ghostInvalid ? ' grid-drag-ghost-invalid' : ''}`}
          style={{ left: drag.ghostX, top: drag.ghostY }}
        >
          {drag.payload.label}
        </div>
      )}

      {actionMenu && manifest.quadrants[actionMenu.quadrantId] && (
        <ModuleActionMenu
          open
          anchorRect={actionMenu.anchorRect}
          quadrantId={actionMenu.quadrantId}
          moduleId={actionMenu.moduleId}
          moduleType={actionMenu.moduleType}
          config={manifest.quadrants[actionMenu.quadrantId]?.config || {}}
          onClose={() => setActionMenu(null)}
          onOpenConfig={(focusKey) => {
            setActionMenu(null);
            setEditingQuadrant({ quadrantId: actionMenu.quadrantId, focusKey });
          }}
          onUpdateConfig={(nextConfig, meta) => {
            saveQuadrantConfig(actionMenu.quadrantId, nextConfig, meta);
          }}
          onRemoveModule={() => removeQuadrant(actionMenu.quadrantId)}
        />
      )}
      <Toast />
      <DetailSlidePanel />
    </div>
  );
}
