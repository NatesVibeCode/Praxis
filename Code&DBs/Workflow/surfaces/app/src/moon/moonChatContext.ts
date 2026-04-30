/**
 * Moon ↔ Chat shared context.
 *
 * MoonBuildPage writes the active workflow + selection state here whenever
 * it changes; ChatPanel reads it and forwards it as a single
 * ``selection_context`` entry on every send. The chat orchestrator already
 * appends ``selection_context`` items to the user message, AND we thread it
 * down to the moon_* tool implementations so they can default-target the
 * active workflow when the LLM omits an explicit ``workflow_id``.
 *
 * Backed by the existing app-wide ``world`` observable so we don't introduce
 * a second store. Path: ``moon.chatContext``.
 */
import { world } from '../world';

export const MOON_CHAT_CONTEXT_PATH = 'moon.chatContext';
export const MOON_CHAT_HANDOFF_PATH = 'moon.chatHandoff';
export const MOON_CHAT_CONTEXT_KIND = 'moon_context';
export const MOON_MATERIALIZE_HANDOFF_KIND = 'moon_materialize_handoff';

export interface MoonChatContext {
  /** Discriminator for the chat orchestrator's selection_context renderer. */
  kind: typeof MOON_CHAT_CONTEXT_KIND;
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
   * This is intentionally not write authority. It lets Moon chat reconcile
   * "visible canvas has steps" against a stale/empty persisted read without
   * pretending ephemeral UI state has been saved.
   */
  visible_ui_snapshot?: Record<string, unknown> | null;
}

export interface MoonChatHandoff {
  kind: typeof MOON_MATERIALIZE_HANDOFF_KIND;
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

export function emptyMoonChatContext(): MoonChatContext {
  return {
    kind: MOON_CHAT_CONTEXT_KIND,
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

/** Write the moon chat context. Called from MoonBuildPage on state change. */
export function setMoonChatContext(patch: Partial<MoonChatContext>): MoonChatContext {
  const current = getMoonChatContext();
  const next: MoonChatContext = {
    ...current,
    ...patch,
    kind: MOON_CHAT_CONTEXT_KIND,
  };
  world.set(MOON_CHAT_CONTEXT_PATH, next);
  return next;
}

/** Clear the context (e.g. when leaving Moon). */
export function clearMoonChatContext(): void {
  world.set(MOON_CHAT_CONTEXT_PATH, emptyMoonChatContext());
}

export function publishMoonChatHandoff(
  event: Omit<MoonChatHandoff, 'kind' | 'created_at'> & { created_at?: string },
): MoonChatHandoff {
  const next: MoonChatHandoff = {
    ...event,
    kind: MOON_MATERIALIZE_HANDOFF_KIND,
    created_at: event.created_at ?? new Date().toISOString(),
  };
  world.set(MOON_CHAT_HANDOFF_PATH, next);
  return next;
}

export function getMoonChatHandoff(): MoonChatHandoff | null {
  const raw = world.get(MOON_CHAT_HANDOFF_PATH);
  if (raw && typeof raw === 'object' && (raw as MoonChatHandoff).kind === MOON_MATERIALIZE_HANDOFF_KIND) {
    return raw as MoonChatHandoff;
  }
  return null;
}

export function clearMoonChatHandoff(): void {
  world.set(MOON_CHAT_HANDOFF_PATH, null);
}

export function subscribeMoonChatHandoff(
  callback: (event: MoonChatHandoff | null) => void,
): () => void {
  return world.subscribe(MOON_CHAT_HANDOFF_PATH, (value) => {
    if (value && typeof value === 'object' && (value as MoonChatHandoff).kind === MOON_MATERIALIZE_HANDOFF_KIND) {
      callback(value as MoonChatHandoff);
    } else {
      callback(null);
    }
  });
}

/** Snapshot read. Returns null if no context has been set yet. */
export function getMoonChatContext(): MoonChatContext {
  const raw = world.get(MOON_CHAT_CONTEXT_PATH);
  if (raw && typeof raw === 'object' && (raw as MoonChatContext).kind === MOON_CHAT_CONTEXT_KIND) {
    return raw as MoonChatContext;
  }
  return emptyMoonChatContext();
}

/** Subscribe; returns unsubscribe. Called from ChatPanel. */
export function subscribeMoonChatContext(
  callback: (context: MoonChatContext) => void,
): () => void {
  return world.subscribe(MOON_CHAT_CONTEXT_PATH, (value) => {
    if (value && typeof value === 'object' && (value as MoonChatContext).kind === MOON_CHAT_CONTEXT_KIND) {
      callback(value as MoonChatContext);
    } else {
      callback(emptyMoonChatContext());
    }
  });
}

/**
 * Build the selection_context entry array that should accompany a chat
 * sendMessage. Returns ``[]`` when no workflow is active so we don't
 * pollute generic chat conversations with a blank moon stanza.
 */
export function moonChatSelectionContext(): Record<string, unknown>[] {
  const ctx = getMoonChatContext();
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
