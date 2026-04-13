export type EditorMode = 'input' | 'review' | 'committed';
export type EditorSurface = 'definition' | 'plan' | 'run' | 'details';
export type HistoryMode = 'push' | 'replace';

export interface EditorUiContext {
  mode: EditorMode;
  hasDefinition: boolean;
  hasCompiledSpec: boolean;
  hasExecutionSetup: boolean;
  freshPlan: boolean;
}

export interface SurfaceAvailabilityEntry {
  enabled: boolean;
  fallbackSurface: EditorSurface;
}

export type SurfaceAvailability = Record<EditorSurface, SurfaceAvailabilityEntry>;

export interface EditorUiState {
  activeSurface: EditorSurface;
  isEditingPlan: boolean;
}

export type EditorUiAction =
  | { type: 'hydrate'; context: EditorUiContext; requestedSurface?: EditorSurface | null }
  | { type: 'navigate'; context: EditorUiContext; surface: EditorSurface }
  | { type: 'compile'; context: EditorUiContext; requestedSurface?: EditorSurface | null }
  | { type: 'planGenerated'; context: EditorUiContext; requestedSurface?: EditorSurface | null }
  | { type: 'refine'; context: EditorUiContext; requestedSurface?: EditorSurface | null }
  | { type: 'commit'; context: EditorUiContext; requestedSurface?: EditorSurface | null }
  | { type: 'editDescription'; context: EditorUiContext }
  | { type: 'togglePlanEditor'; context: EditorUiContext }
  | { type: 'closePlanEditor'; context: EditorUiContext };

export function parseEditorSurface(value: string | null | undefined): EditorSurface | null {
  if (value === 'definition' || value === 'plan' || value === 'run' || value === 'details') {
    return value;
  }
  return null;
}

export function buildSurfaceAvailability(context: EditorUiContext): SurfaceAvailability {
  const hasDefinition = context.hasDefinition || context.mode === 'input';
  const defaultFallback = context.mode === 'committed' && context.freshPlan ? 'run' : 'definition';

  return {
    definition: {
      enabled: hasDefinition,
      fallbackSurface: 'definition',
    },
    plan: {
      enabled: context.hasDefinition,
      fallbackSurface: 'definition',
    },
    run: {
      enabled: context.hasDefinition,
      fallbackSurface: defaultFallback,
    },
    details: {
      enabled: context.hasDefinition && context.hasExecutionSetup,
      fallbackSurface: 'definition',
    },
  };
}

export function defaultEditorSurface(context: EditorUiContext): EditorSurface {
  if (!context.hasDefinition) return 'definition';
  if (context.mode === 'committed' && context.freshPlan) return 'run';
  return 'definition';
}

export function resolveEditorSurface(
  requestedSurface: EditorSurface | null | undefined,
  context: EditorUiContext,
): EditorSurface {
  const availability = buildSurfaceAvailability(context);
  if (requestedSurface && availability[requestedSurface].enabled) return requestedSurface;
  if (requestedSurface) return availability[requestedSurface].fallbackSurface;
  return defaultEditorSurface(context);
}

function normalizePlanEditorState(
  activeSurface: EditorSurface,
  isEditingPlan: boolean,
  context: EditorUiContext,
): boolean {
  return activeSurface === 'plan' && context.hasCompiledSpec ? isEditingPlan : false;
}

function transition(
  state: EditorUiState,
  context: EditorUiContext,
  requestedSurface: EditorSurface | null | undefined,
  defaultSurface: EditorSurface | null,
  preservePlanEditor: boolean,
): EditorUiState {
  const activeSurface = resolveEditorSurface(requestedSurface ?? defaultSurface, context);
  return {
    activeSurface,
    isEditingPlan: normalizePlanEditorState(
      activeSurface,
      preservePlanEditor ? state.isEditingPlan : false,
      context,
    ),
  };
}

export function createEditorUiState(
  context: EditorUiContext,
  requestedSurface?: EditorSurface | null,
): EditorUiState {
  return {
    activeSurface: resolveEditorSurface(requestedSurface, context),
    isEditingPlan: false,
  };
}

export function editorUiReducer(state: EditorUiState, action: EditorUiAction): EditorUiState {
  switch (action.type) {
    case 'hydrate':
      return transition(state, action.context, action.requestedSurface, state.activeSurface, true);
    case 'navigate':
      return transition(state, action.context, action.surface, action.surface, true);
    case 'compile':
      return transition(state, action.context, action.requestedSurface, 'definition', false);
    case 'planGenerated':
      return transition(state, action.context, action.requestedSurface, 'plan', false);
    case 'refine':
      return transition(state, action.context, action.requestedSurface, 'definition', false);
    case 'commit':
      return transition(state, action.context, action.requestedSurface, 'run', false);
    case 'editDescription':
      return transition(state, action.context, 'definition', 'definition', false);
    case 'togglePlanEditor': {
      const activeSurface = resolveEditorSurface(state.activeSurface, action.context);
      if (activeSurface !== 'plan' || !action.context.hasCompiledSpec) {
        return { activeSurface, isEditingPlan: false };
      }
      return {
        activeSurface,
        isEditingPlan: !state.isEditingPlan,
      };
    }
    case 'closePlanEditor':
      return {
        activeSurface: resolveEditorSurface(state.activeSurface, action.context),
        isEditingPlan: false,
      };
    default:
      return state;
  }
}
