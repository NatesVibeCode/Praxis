/**
 * Pointer-event drag system for Canvas Build.
 *
 * Native HTML5 DnD is unreliable with React re-renders and <button> elements.
 * This uses pointer capture for a rock-solid drag experience:
 *   - pointerdown on a source starts tracking
 *   - 5px movement threshold differentiates click from drag
 *   - Ghost element follows the cursor
 *   - Drop targets detected via data-drop-* attributes + elementFromPoint
 *   - pointerup triggers the drop callback
 */

import { useCallback, useRef, useState } from 'react';

// ---- Payload types ----

export type DragPayloadKind = 'catalog' | 'node' | 'object-type';

export interface DragPayload {
  kind: DragPayloadKind;
  id: string;
  label: string;
}

export interface DropTarget {
  zone: 'node' | 'edge' | 'append';
  id: string;
}

export interface DragState {
  active: boolean;
  payload: DragPayload | null;
  ghostX: number;
  ghostY: number;
  hoveredTarget: DropTarget | null;
}

const DRAG_THRESHOLD = 5;

export function useCanvasDrag(onDrop: (payload: DragPayload, target: DropTarget) => void) {
  const [drag, setDrag] = useState<DragState>({
    active: false,
    payload: null,
    ghostX: 0,
    ghostY: 0,
    hoveredTarget: null,
  });

  const startRef = useRef<{ x: number; y: number; payload: DragPayload } | null>(null);
  const activeRef = useRef(false); // avoid stale closure in global listeners

  // Find the drop target element at a screen coordinate
  const findTarget = useCallback((x: number, y: number): DropTarget | null => {
    const els = document.elementsFromPoint(x, y);
    for (const el of els) {
      const node = (el as HTMLElement).closest('[data-drop-node]');
      if (node) return { zone: 'node', id: node.getAttribute('data-drop-node')! };
      const edge = (el as HTMLElement).closest('[data-drop-edge]');
      if (edge) return { zone: 'edge', id: edge.getAttribute('data-drop-edge')! };
      const append = (el as HTMLElement).closest('[data-drop-append]');
      if (append) return { zone: 'append', id: '__append__' };
    }
    return null;
  }, []);

  const handlePointerMove = useCallback((e: PointerEvent) => {
    const start = startRef.current;
    if (!start) return;

    const dx = e.clientX - start.x;
    const dy = e.clientY - start.y;

    if (!activeRef.current) {
      if (Math.abs(dx) < DRAG_THRESHOLD && Math.abs(dy) < DRAG_THRESHOLD) return;
      activeRef.current = true;
    }

    const target = findTarget(e.clientX, e.clientY);
    setDrag({
      active: true,
      payload: start.payload,
      ghostX: e.clientX,
      ghostY: e.clientY,
      hoveredTarget: target,
    });
  }, [findTarget]);

  const handlePointerUp = useCallback((e: PointerEvent) => {
    document.removeEventListener('pointermove', handlePointerMove);
    document.removeEventListener('pointerup', handlePointerUp);

    const wasActive = activeRef.current;
    const payload = startRef.current?.payload ?? null;
    startRef.current = null;
    activeRef.current = false;

    if (wasActive && payload) {
      const target = findTarget(e.clientX, e.clientY);
      if (target) {
        onDrop(payload, target);
      }
    }

    setDrag({ active: false, payload: null, ghostX: 0, ghostY: 0, hoveredTarget: null });
  }, [findTarget, handlePointerMove, onDrop]);

  const startDrag = useCallback((e: React.PointerEvent, payload: DragPayload) => {
    // Only primary button
    if (e.button !== 0) return;
    e.preventDefault(); // prevent text selection during drag
    startRef.current = { x: e.clientX, y: e.clientY, payload };
    activeRef.current = false;
    document.addEventListener('pointermove', handlePointerMove);
    document.addEventListener('pointerup', handlePointerUp);
  }, [handlePointerMove, handlePointerUp]);

  const cancelDrag = useCallback(() => {
    document.removeEventListener('pointermove', handlePointerMove);
    document.removeEventListener('pointerup', handlePointerUp);
    startRef.current = null;
    activeRef.current = false;
    setDrag({ active: false, payload: null, ghostX: 0, ghostY: 0, hoveredTarget: null });
  }, [handlePointerMove, handlePointerUp]);

  return { drag, startDrag, cancelDrag };
}
