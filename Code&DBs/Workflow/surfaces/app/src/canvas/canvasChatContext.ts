/**
 * Canvas ↔ Chat shared context.
 *
 * CanvasBuildPage writes the active workflow + selection state here whenever
 * it changes; ChatPanel reads it and forwards it as a single
 * ``selection_context`` entry on every send. The chat orchestrator already
 * appends ``selection_context`` items to the user message, AND we thread it
 * down to the canvas_* tool implementations so they can default-target the
 * active workflow when the LLM omits an explicit ``workflow_id``.
 *
 * Backed by the existing app-wide ``world`` observable so we don't introduce
 * a second store. Path: ``canvas.chatContext``.
 */
import { world } from '../world';

export const CANVAS_CHAT_CONTEXT_PATH = 'canvas.chatContext';
export const CANVAS_CHAT_HANDOFF_PATH = 'canvas.chatHandoff';
export const CANVAS_CHAT_CONTEXT_KIND = 'canvas_context';
export const CANVAS_MATERIALIZE_HANDOFF_KIND = 'canvas_materialize_handoff';

export interface CanvasChatContext {
  /** Discriminator for the chat orchestrator's selection_context renderer. */
  kind: typeof CANVAS_CHAT_CONTEXT_KIND;
  workflow_id: string | null;
  workflow_name: string | null;
  selected_node_id: string | null;
  selected_edge_id: string | null;
  view_mode: 'build' | 'run' | null;
  /** Brief one-line hint the LLM uses to orient — kept short on purpose. */
  hint?: string | null;
  materialize_status?: string | null;
  operation_receipt_id?: string | null;
  correlation_id?: string | null;
  graph_summary?: Record<string, unknown> | null;
  /**
   * Read-only snapshot of the graph the operator can currently see.
   *
   * This is intentionally not write authority. It lets Canvas chat reconcile
   * "visible canvas has steps" against a stale/empty persisted read without
   * pretending ephemeral UI state has been saved.
   */
  visible_ui_snapshot?: Record<string, unknown> | null;
}

export interface CanvasChatHandoff {
  kind: typeof CANVAS_MATERIALIZE_HANDOFF_KIND;
  handoff_id: string;
  workflow_id: string | null;
  workflow_name?: string | null;
  phase: 'started' | 'ready' | 'blocked' | 'chat_fallback';
  status_message: string;
  prompt?: string | null;
  operation_receipt_id?: string | null;
  correlation_id?: string | null;
  graph_summary?: Record<string, unknown> | null;
  created_at: string;
}

export function emptyCanvasChatContext(): CanvasChatContext {
  return {
    kind: CANVAS_CHAT_CONTEXT_KIND,
    workflow_id: null,
    workflow_name: null,
    selected_node_id: null,
    selected_edge_id: null,
    view_mode: null,
    hint: null,
    materialize_status: null,
    operation_receipt_id: null,
    correlation_id: null,
    graph_summary: null,
    visible_ui_snapshot: null,
  };
}

/** Write the canvas chat context. Called from CanvasBuildPage on state change. */
export function setCanvasChatContext(patch: Partial<CanvasChatContext>): CanvasChatContext {
  const current = getCanvasChatContext();
  const next: CanvasChatContext = {
    ...current,
    ...patch,
    kind: CANVAS_CHAT_CONTEXT_KIND,
  };
  world.set(CANVAS_CHAT_CONTEXT_PATH, next);
  return next;
}

/** Clear the context (e.g. when leaving Canvas). */
export function clearCanvasChatContext(): void {
  world.set(CANVAS_CHAT_CONTEXT_PATH, emptyCanvasChatContext());
}

export function publishCanvasChatHandoff(
  event: Omit<CanvasChatHandoff, 'kind' | 'created_at'> & { created_at?: string },
): CanvasChatHandoff {
  const next: CanvasChatHandoff = {
    ...event,
    kind: CANVAS_MATERIALIZE_HANDOFF_KIND,
    created_at: event.created_at ?? new Date().toISOString(),
  };
  world.set(CANVAS_CHAT_HANDOFF_PATH, next);
  return next;
}

export function getCanvasChatHandoff(): CanvasChatHandoff | null {
  const raw = world.get(CANVAS_CHAT_HANDOFF_PATH);
  if (raw && typeof raw === 'object' && (raw as CanvasChatHandoff).kind === CANVAS_MATERIALIZE_HANDOFF_KIND) {
    return raw as CanvasChatHandoff;
  }
  return null;
}

export function clearCanvasChatHandoff(): void {
  world.set(CANVAS_CHAT_HANDOFF_PATH, null);
}

export function subscribeCanvasChatHandoff(
  callback: (event: CanvasChatHandoff | null) => void,
): () => void {
  return world.subscribe(CANVAS_CHAT_HANDOFF_PATH, (value) => {
    if (value && typeof value === 'object' && (value as CanvasChatHandoff).kind === CANVAS_MATERIALIZE_HANDOFF_KIND) {
      callback(value as CanvasChatHandoff);
    } else {
      callback(null);
    }
  });
}

/** Snapshot read. Returns null if no context has been set yet. */
export function getCanvasChatContext(): CanvasChatContext {
  const raw = world.get(CANVAS_CHAT_CONTEXT_PATH);
  if (raw && typeof raw === 'object' && (raw as CanvasChatContext).kind === CANVAS_CHAT_CONTEXT_KIND) {
    return raw as CanvasChatContext;
  }
  return emptyCanvasChatContext();
}

/** Subscribe; returns unsubscribe. Called from ChatPanel. */
export function subscribeCanvasChatContext(
  callback: (context: CanvasChatContext) => void,
): () => void {
  return world.subscribe(CANVAS_CHAT_CONTEXT_PATH, (value) => {
    if (value && typeof value === 'object' && (value as CanvasChatContext).kind === CANVAS_CHAT_CONTEXT_KIND) {
      callback(value as CanvasChatContext);
    } else {
      callback(emptyCanvasChatContext());
    }
  });
}

/**
 * Build the selection_context entry array that should accompany a chat
 * sendMessage. Returns ``[]`` when no workflow is active so we don't
 * pollute generic chat conversations with a blank canvas stanza.
 */
export function canvasChatSelectionContext(): Record<string, unknown>[] {
  const ctx = getCanvasChatContext();
  if (!ctx.workflow_id) return [];
  const entry: Record<string, unknown> = {
    kind: ctx.kind,
    workflow_id: ctx.workflow_id,
  };
  if (ctx.workflow_name) entry.workflow_name = ctx.workflow_name;
  if (ctx.selected_node_id) entry.selected_node_id = ctx.selected_node_id;
  if (ctx.selected_edge_id) entry.selected_edge_id = ctx.selected_edge_id;
  if (ctx.view_mode) entry.view_mode = ctx.view_mode;
  if (ctx.hint) entry.hint = ctx.hint;
  if (ctx.materialize_status) entry.materialize_status = ctx.materialize_status;
  if (ctx.operation_receipt_id) entry.operation_receipt_id = ctx.operation_receipt_id;
  if (ctx.correlation_id) entry.correlation_id = ctx.correlation_id;
  if (ctx.graph_summary) entry.graph_summary = ctx.graph_summary;
  if (ctx.visible_ui_snapshot) entry.visible_ui_snapshot = ctx.visible_ui_snapshot;
  return [entry];
}
