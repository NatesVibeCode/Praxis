import React, { Suspense, useCallback, useEffect, useState } from 'react';
import type { PraxisSurfaceBundleV4 } from '../praxis/manifest';
import { resolveModule } from '../modules/moduleRegistry';
import { ALL_CELLS, cellIdFromRowCol, getOccupiedCells, parseQuadrantId, parseSpan } from './quadrantUtils';
import { ConfigEditorPanel } from './ConfigEditorPanel';
import { useManifestOverlay } from '../hooks/useManifestOverlay';
import { world } from '../world';
import { ModulePalette } from './ModulePalette';
import { SaveLayoutBar } from './SaveLayoutBar';
import { Toast } from '../primitives/Toast';
import { ContextMenu } from './ContextMenu';
import { DetailSlidePanel } from './DetailSlidePanel';
import { useGridDrag, type GridDragPayload, type GridDragValidator } from './useGridDrag';
import './QuadrantGrid.css';

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

export function QuadrantGrid({
  manifest: initialManifest,
  saveTarget,
}: {
  manifest: QuadrantManifest;
  saveTarget?: QuadrantSaveTarget | null;
}) {
  const manifest = useManifestOverlay(initialManifest);
  const occupied = getOccupiedCells(manifest.quadrants);
  const [editingQuadrant, setEditingQuadrant] = useState<string | null>(null);
  const [showPalette, setShowPalette] = useState(false);
  const [justPlaced, setJustPlaced] = useState<string | null>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; quadrantId: string; moduleId: string; moduleType: string } | null>(null);

  const handleContextMenuAction = (instruction: string) => {
    window.dispatchEvent(new CustomEvent('fill-refinement', { detail: instruction }));
    setContextMenu(null);
  };

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
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: payload.data.module as string,
        span: payload.data.span as string,
        config: payload.data.config as Record<string, unknown>,
      };
      world.propose('ui.layout.quadrants', updated);
      setShowPalette(false);
      setJustPlaced(targetId);
      return;
    }

    if (payload.kind === 'palette') {
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: payload.data.module as string,
        span: payload.data.span as string,
      };
      world.propose('ui.layout.quadrants', updated);
      setShowPalette(false);
      setJustPlaced(targetId);
      return;
    }

    if (payload.kind === 'object') {
      const updated = { ...manifest.quadrants };
      updated[targetId] = {
        module: (payload.data.module as string) || 'object-view',
        span: (payload.data.span as string) || '1x1',
        config: {
          objectTypeId: payload.data.objectTypeId,
          objectName: payload.data.objectName,
        },
      };
      world.propose('ui.layout.quadrants', updated);
      setShowPalette(false);
      setJustPlaced(targetId);
      return;
    }

    if (payload.kind === 'cell') {
      const sourceId = payload.data.cellId as string;
      if (!sourceId || sourceId === targetId) return;

      const updated = { ...manifest.quadrants };
      const sourceDef = updated[sourceId];
      const targetDef = updated[targetId];

      if (targetDef) {
        updated[sourceId] = { ...targetDef };
        updated[targetId] = { ...sourceDef };
      } else {
        updated[targetId] = { ...sourceDef };
        delete updated[sourceId];
      }

      world.propose('ui.layout.quadrants', updated);
      setJustPlaced(targetId);
    }
  }, [manifest.quadrants]);

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
            >
              <button
                type="button"
                className="gear-icon"
                onClick={() => setEditingQuadrant(cellId)}
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
                ⚙️
              </button>
              {editingQuadrant === cellId && (
                <div style={{ position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20 }}>
                  <ConfigEditorPanel
                    quadrantId={cellId}
                    moduleId={def.module}
                    config={def.config || {}}
                    onSave={(newConfig) => {
                      world.propose(`ui.layout.quadrants.${cellId}.config`, newConfig);
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

      {contextMenu && (
        <ContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          moduleId={contextMenu.moduleId}
          moduleType={contextMenu.moduleType}
          quadrantId={contextMenu.quadrantId}
          onAction={handleContextMenuAction}
          onClose={() => setContextMenu(null)}
        />
      )}
      <Toast />
      <DetailSlidePanel />
    </div>
  );
}
