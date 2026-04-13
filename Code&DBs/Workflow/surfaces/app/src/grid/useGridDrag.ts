/**
 * Pointer-event drag system for the QuadrantGrid workspace.
 *
 * HTML5 DnD is unreliable across z-index layers and React re-renders.
 * This uses pointer capture for reliable drag:
 *   - pointerdown on a source starts tracking
 *   - 5px movement threshold differentiates click from drag
 *   - Ghost element follows the cursor
 *   - Drop targets detected via data-grid-drop attributes + elementsFromPoint
 *   - pointerup triggers the drop callback
 */

import { useCallback, useRef, useState } from 'react';

export interface GridDragPayload {
  /** 'palette' = new module from palette, 'cell' = existing cell move, 'object' = DB object type */
  kind: 'palette' | 'cell' | 'object' | 'preset';
  label: string;
  data: Record<string, unknown>;
}

export interface GridDragState {
  active: boolean;
  payload: GridDragPayload | null;
  ghostX: number;
  ghostY: number;
  hoveredCell: string | null;
  valid: boolean;
}

export type GridDragValidator = (payload: GridDragPayload, targetCellId: string) => boolean;

const DRAG_THRESHOLD = 5;

export function useGridDrag(
  onDrop: (payload: GridDragPayload, targetCellId: string) => void,
  validate?: GridDragValidator,
) {
  const [drag, setDrag] = useState<GridDragState>({
    active: false,
    payload: null,
    ghostX: 0,
    ghostY: 0,
    hoveredCell: null,
    valid: false,
  });

  const startRef = useRef<{ x: number; y: number; payload: GridDragPayload } | null>(null);
  const activeRef = useRef(false);
  const validateRef = useRef(validate);
  validateRef.current = validate;

  const findTarget = useCallback((x: number, y: number): { cellId: string | null; valid: boolean } => {
    const els = document.elementsFromPoint(x, y);
    for (const el of els) {
      const cell = (el as HTMLElement).closest('[data-grid-drop]');
      if (cell) {
        const cellId = cell.getAttribute('data-grid-drop')!;
        const payload = startRef.current?.payload ?? null;
        const isValid = payload && validateRef.current
          ? validateRef.current(payload, cellId)
          : true;
        return { cellId, valid: isValid };
      }
    }
    return { cellId: null, valid: false };
  }, []);

  const handlePointerMove = useCallback((e: PointerEvent) => {
    const start = startRef.current;
    if (!start) return;

    const dx = e.clientX - start.x;
    const dy = e.clientY - start.y;

    if (!activeRef.current) {
      if (Math.abs(dx) < DRAG_THRESHOLD && Math.abs(dy) < DRAG_THRESHOLD) return;
      activeRef.current = true;
      document.body.style.userSelect = 'none';
    }

    const { cellId, valid } = findTarget(e.clientX, e.clientY);
    setDrag({
      active: true,
      payload: start.payload,
      ghostX: e.clientX,
      ghostY: e.clientY,
      hoveredCell: cellId,
      valid,
    });
  }, [findTarget]);

  const handlePointerUp = useCallback((e: PointerEvent) => {
    document.removeEventListener('pointermove', handlePointerMove);
    document.removeEventListener('pointerup', handlePointerUp);
    document.body.style.userSelect = '';

    const wasActive = activeRef.current;
    const payload = startRef.current?.payload ?? null;
    startRef.current = null;
    activeRef.current = false;

    if (wasActive && payload) {
      const { cellId, valid } = findTarget(e.clientX, e.clientY);
      if (cellId && valid) {
        onDrop(payload, cellId);
      }
    }

    setDrag({ active: false, payload: null, ghostX: 0, ghostY: 0, hoveredCell: null, valid: false });
  }, [findTarget, handlePointerMove, onDrop]);

  const startDrag = useCallback((e: React.PointerEvent, payload: GridDragPayload) => {
    if (e.button !== 0) return;
    e.preventDefault();
    startRef.current = { x: e.clientX, y: e.clientY, payload };
    activeRef.current = false;
    document.addEventListener('pointermove', handlePointerMove);
    document.addEventListener('pointerup', handlePointerUp);
  }, [handlePointerMove, handlePointerUp]);

  return { drag, startDrag };
}
