import { postBuildMutation } from '../shared/buildController';
import { world } from '../world';

export const UI_ACTION_LOG_PATH = 'ui.control.actions';
const MAX_UI_ACTIONS = 8;

export type UiActionStatus = 'applied' | 'undone';
export type UiActionRecovery = 'undo_ready' | 'superseded' | 'recorded' | 'undone';
export type UiActionCategory = 'layout' | 'graph' | 'authority' | 'control';

export interface UiActionTarget {
  kind: string;
  label: string;
  id?: string | null;
}

export interface UiActionEntry {
  id: string;
  surface: string;
  undoScope: string;
  category: UiActionCategory;
  label: string;
  authority: string;
  reason: string;
  outcome: string;
  target: UiActionTarget | null;
  changeSummary: string[];
  status: UiActionStatus;
  undoable: boolean;
  recovery: UiActionRecovery;
  occurredAt: number;
  undoDescriptor: UiActionUndoDescriptor | null;
}

export interface UiActionUndoWorldProposal {
  kind: 'world.propose';
  path: string;
  value: unknown;
}

export interface UiActionUndoWorkflowBuildMutation {
  kind: 'workflow.buildMutation';
  workflowId: string;
  subpath: string;
  body: Record<string, unknown>;
}

export interface UiActionUndoCanvasPayloadRestore {
  kind: 'canvas.payload.restore';
  scope: string;
  payload: unknown;
}

export interface UiActionUndoSequence {
  kind: 'sequence';
  steps: UiActionUndoDescriptor[];
}

export type UiActionUndoDescriptor =
  | UiActionUndoWorldProposal
  | UiActionUndoWorkflowBuildMutation
  | UiActionUndoCanvasPayloadRestore
  | UiActionUndoSequence;

export interface ReversibleUiAction {
  surface: string;
  undoScope?: string;
  category?: UiActionCategory;
  label: string;
  authority: string;
  reason: string;
  outcome: string;
  target?: UiActionTarget | null;
  changeSummary?: string[];
  apply: () => void | Promise<void>;
  undoDescriptor?: UiActionUndoDescriptor | null;
  buildUndoDescriptor?: () => UiActionUndoDescriptor | null | Promise<UiActionUndoDescriptor | null>;
  onUndone?: () => void | Promise<void>;
}

interface UiActionResult {
  ok: boolean;
  entry?: UiActionEntry;
  error?: string;
}

type UiActionUndoExecutor<TKind extends UiActionUndoDescriptor['kind'] = UiActionUndoDescriptor['kind']> = (
  descriptor: Extract<UiActionUndoDescriptor, { kind: TKind }>
) => boolean | void | Promise<boolean | void>;

const undoEffectHandlers = new Map<string, () => void | Promise<void>>();
const undoExecutors = new Map<UiActionUndoDescriptor['kind'], Set<UiActionUndoExecutor>>();
let actionSequence = 0;

function defaultCategory(surface: string): UiActionCategory {
  if (surface === 'grid') return 'layout';
  if (surface === 'canvas') return 'graph';
  return 'control';
}

function normalizeRecovery(entry: Partial<UiActionEntry>): UiActionRecovery {
  if (entry.status === 'undone') return 'undone';
  if (
    entry.recovery === 'undo_ready'
    || entry.recovery === 'superseded'
    || entry.recovery === 'recorded'
    || entry.recovery === 'undone'
  ) {
    return entry.recovery;
  }
  return entry.undoable ? 'undo_ready' : 'recorded';
}

function normalizeUndoDescriptor(value: unknown): UiActionUndoDescriptor | null {
  if (!value || typeof value !== 'object') return null;
  const descriptor = value as { kind?: unknown };
  if (descriptor.kind === 'world.propose') {
    const path = typeof (value as { path?: unknown }).path === 'string'
      ? (value as { path: string }).path.trim()
      : '';
    if (!path) return null;
    return {
      kind: 'world.propose',
      path,
      value: (value as { value?: unknown }).value,
    };
  }
  if (descriptor.kind === 'workflow.buildMutation') {
    const workflowId = typeof (value as { workflowId?: unknown }).workflowId === 'string'
      ? (value as { workflowId: string }).workflowId.trim()
      : '';
    const subpath = typeof (value as { subpath?: unknown }).subpath === 'string'
      ? (value as { subpath: string }).subpath.trim()
      : '';
    const body = (value as { body?: unknown }).body;
    if (!workflowId || !subpath || !body || typeof body !== 'object' || Array.isArray(body)) {
      return null;
    }
    return {
      kind: 'workflow.buildMutation',
      workflowId,
      subpath,
      body: body as Record<string, unknown>,
    };
  }
  if (descriptor.kind === 'canvas.payload.restore') {
    const scope = typeof (value as { scope?: unknown }).scope === 'string'
      ? (value as { scope: string }).scope.trim()
      : '';
    if (!scope) return null;
    return {
      kind: 'canvas.payload.restore',
      scope,
      payload: (value as { payload?: unknown }).payload,
    };
  }
  if (descriptor.kind === 'sequence') {
    const steps = Array.isArray((value as { steps?: unknown }).steps)
      ? (value as { steps: unknown[] }).steps.map(normalizeUndoDescriptor).filter((step): step is UiActionUndoDescriptor => step !== null)
      : [];
    if (steps.length === 0) return null;
    return {
      kind: 'sequence',
      steps,
    };
  }
  return null;
}

function normalizeEntry(entry: Partial<UiActionEntry>): UiActionEntry {
  const undoDescriptor = normalizeUndoDescriptor((entry as { undoDescriptor?: unknown }).undoDescriptor);
  const undoScope = typeof (entry as { undoScope?: unknown }).undoScope === 'string'
    && (entry as { undoScope: string }).undoScope.trim().length > 0
      ? (entry as { undoScope: string }).undoScope
      : String(entry.surface || '');
  const status = entry.status === 'undone' ? 'undone' : 'applied';
  const undoable = Boolean(entry.undoable) && undoDescriptor !== null && status !== 'undone';
  return {
    id: String(entry.id || ''),
    surface: String(entry.surface || ''),
    undoScope,
    category:
      entry.category === 'layout'
      || entry.category === 'graph'
      || entry.category === 'authority'
      || entry.category === 'control'
        ? entry.category
        : defaultCategory(String(entry.surface || '')),
    label: String(entry.label || ''),
    authority: String(entry.authority || ''),
    reason: String(entry.reason || ''),
    outcome: String(entry.outcome || ''),
    target:
      entry.target
      && typeof entry.target === 'object'
      && typeof entry.target.kind === 'string'
      && typeof entry.target.label === 'string'
        ? {
            kind: entry.target.kind,
            label: entry.target.label,
            id: typeof entry.target.id === 'string' ? entry.target.id : null,
          }
        : null,
    changeSummary: Array.isArray(entry.changeSummary)
      ? entry.changeSummary.filter((item): item is string => typeof item === 'string' && item.trim().length > 0).slice(0, 3)
      : [],
    status,
    undoable,
    recovery: normalizeRecovery({ ...entry, status, undoable }),
    occurredAt: typeof entry.occurredAt === 'number' ? entry.occurredAt : Date.now(),
    undoDescriptor,
  };
}

function demoteEntry(entry: UiActionEntry): UiActionEntry {
  if (entry.status === 'undone') {
    return { ...entry, undoable: false, recovery: 'undone' };
  }
  if (entry.recovery === 'recorded') {
    return { ...entry, undoable: false, recovery: 'recorded' };
  }
  return { ...entry, undoable: false, recovery: 'superseded' };
}

function readEntries(): UiActionEntry[] {
  const value = world.get(UI_ACTION_LOG_PATH);
  return Array.isArray(value) ? (value as Partial<UiActionEntry>[]).map(normalizeEntry) : [];
}

function writeEntries(entries: UiActionEntry[]): void {
  world.set(UI_ACTION_LOG_PATH, entries);
}

function trimEntries(entries: UiActionEntry[]): UiActionEntry[] {
  const nextEntries = entries.slice(0, MAX_UI_ACTIONS);
  const liveIds = new Set(nextEntries.map((entry) => entry.id));
  for (const actionId of Array.from(undoEffectHandlers.keys())) {
    if (!liveIds.has(actionId)) undoEffectHandlers.delete(actionId);
  }
  return nextEntries;
}

async function runRegisteredUndoExecutors(descriptor: UiActionUndoDescriptor): Promise<boolean> {
  const executors = undoExecutors.get(descriptor.kind);
  if (!executors || executors.size === 0) return false;
  for (const executor of Array.from(executors)) {
    const handled = await executor(descriptor as never);
    if (handled !== false) return true;
  }
  return false;
}

async function executeUndoDescriptor(descriptor: UiActionUndoDescriptor): Promise<void> {
  if (descriptor.kind === 'sequence') {
    for (const step of descriptor.steps) {
      await executeUndoDescriptor(step);
    }
    return;
  }
  if (await runRegisteredUndoExecutors(descriptor)) return;
  if (descriptor.kind === 'world.propose') {
    world.propose(descriptor.path, descriptor.value);
    return;
  }
  if (descriptor.kind === 'workflow.buildMutation') {
    await postBuildMutation(descriptor.workflowId, descriptor.subpath, descriptor.body);
    return;
  }
  throw new Error('Undo executor unavailable for that action.');
}

export function registerUiActionUndoExecutor<TKind extends UiActionUndoDescriptor['kind']>(
  kind: TKind,
  executor: UiActionUndoExecutor<TKind>,
): () => void {
  const current = undoExecutors.get(kind) ?? new Set<UiActionUndoExecutor>();
  current.add(executor as unknown as UiActionUndoExecutor);
  undoExecutors.set(kind, current);
  return () => {
    const next = undoExecutors.get(kind);
    if (!next) return;
    next.delete(executor as unknown as UiActionUndoExecutor);
    if (next.size === 0) undoExecutors.delete(kind);
  };
}

export async function runUiAction(action: ReversibleUiAction): Promise<UiActionEntry> {
  await action.apply();
  const undoDescriptor = normalizeUndoDescriptor(
    action.undoDescriptor ?? await action.buildUndoDescriptor?.() ?? null,
  );
  const undoable = undoDescriptor !== null;
  const undoScope = action.undoScope?.trim() || action.surface;

  const timestamp = Date.now();
  const nextEntry: UiActionEntry = {
    id: `ui-action-${timestamp}-${++actionSequence}`,
    surface: action.surface,
    undoScope,
    category: action.category ?? defaultCategory(action.surface),
    label: action.label,
    authority: action.authority,
    reason: action.reason,
    outcome: action.outcome,
    target: action.target ?? null,
    changeSummary: Array.isArray(action.changeSummary)
      ? action.changeSummary.filter((item) => typeof item === 'string' && item.trim()).slice(0, 3)
      : [],
    status: 'applied',
    undoable,
    recovery: undoable ? 'undo_ready' : 'recorded',
    occurredAt: timestamp,
    undoDescriptor,
  };

  const nextEntries = trimEntries([
    nextEntry,
    ...readEntries().map((entry) => (entry.undoScope === undoScope ? demoteEntry(entry) : entry)),
  ]);

  if (action.onUndone) {
    undoEffectHandlers.set(nextEntry.id, action.onUndone);
  }
  writeEntries(nextEntries);
  return nextEntry;
}

export async function undoUiAction(actionId: string): Promise<UiActionResult> {
  const entries = readEntries();
  const target = entries.find((entry) => entry.id === actionId);
  if (!target) {
    return { ok: false, error: 'That action is no longer in the control log.' };
  }

  if (!target.undoable || target.status !== 'applied' || !target.undoDescriptor) {
    return { ok: false, error: 'Only the latest applied action in this control lane can be undone.' };
  }

  try {
    await executeUndoDescriptor(target.undoDescriptor);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Undo failed.';
    return { ok: false, error: message };
  }

  const afterUndo = undoEffectHandlers.get(actionId);
  undoEffectHandlers.delete(actionId);
  if (afterUndo) {
    try {
      await afterUndo();
    } catch {}
  }

  const nextEntries: UiActionEntry[] = entries.map((entry) => {
    if (entry.id === actionId) {
      return { ...entry, status: 'undone' as const, undoable: false, recovery: 'undone' as const };
    }
    if (entry.undoScope !== target.undoScope) {
      return entry;
    }
    if (entry.status === 'undone') {
      return { ...entry, undoable: false, recovery: 'undone' as const };
    }
    if (entry.recovery === 'recorded' || !entry.undoDescriptor) {
      return { ...entry, undoable: false, recovery: 'recorded' as const };
    }
    return { ...entry, undoable: false, recovery: 'superseded' as const };
  });

  const nextUndoable = nextEntries.find(
    (entry) => entry.undoScope === target.undoScope && entry.status === 'applied' && entry.undoDescriptor,
  );
  if (nextUndoable) {
    nextUndoable.undoable = true;
    nextUndoable.recovery = 'undo_ready';
  }

  writeEntries(nextEntries);
  return {
    ok: true,
    entry: nextEntries.find((entry) => entry.id === actionId),
  };
}
