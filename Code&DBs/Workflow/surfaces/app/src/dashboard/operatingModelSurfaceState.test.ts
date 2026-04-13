import {
  createEditorUiState,
  defaultEditorSurface,
  editorUiReducer,
  resolveEditorSurface,
  type EditorUiContext,
} from './operatingModelSurfaceState';

const reviewDefinitionContext: EditorUiContext = {
  mode: 'review',
  hasDefinition: true,
  hasCompiledSpec: false,
  hasExecutionSetup: false,
  freshPlan: false,
};

const reviewPlannedContext: EditorUiContext = {
  mode: 'review',
  hasDefinition: true,
  hasCompiledSpec: true,
  hasExecutionSetup: true,
  freshPlan: true,
};

const committedFreshContext: EditorUiContext = {
  mode: 'committed',
  hasDefinition: true,
  hasCompiledSpec: true,
  hasExecutionSetup: true,
  freshPlan: true,
};

describe('operatingModelSurfaceState', () => {
  it('defaults committed workflows with a fresh plan to the run surface', () => {
    expect(defaultEditorSurface(committedFreshContext)).toBe('run');
    expect(createEditorUiState(committedFreshContext)).toEqual({
      activeSurface: 'run',
      isEditingPlan: false,
    });
  });

  it('falls back to definition when a requested surface is unavailable', () => {
    expect(resolveEditorSurface('details', reviewDefinitionContext)).toBe('definition');
  });

  it('keeps compile transitions on definition and closes advanced JSON editing', () => {
    const state = { activeSurface: 'plan' as const, isEditingPlan: true };

    expect(editorUiReducer(state, {
      type: 'compile',
      context: reviewDefinitionContext,
    })).toEqual({
      activeSurface: 'definition',
      isEditingPlan: false,
    });
  });

  it('moves plan generation to the plan surface and resets advanced JSON editing', () => {
    const state = createEditorUiState(reviewDefinitionContext);

    expect(editorUiReducer(state, {
      type: 'planGenerated',
      context: reviewPlannedContext,
    })).toEqual({
      activeSurface: 'plan',
      isEditingPlan: false,
    });
  });

  it('only opens advanced JSON on the plan surface when a compiled spec exists', () => {
    const plannedState = createEditorUiState(reviewPlannedContext, 'plan');
    expect(editorUiReducer(plannedState, {
      type: 'togglePlanEditor',
      context: reviewPlannedContext,
    })).toEqual({
      activeSurface: 'plan',
      isEditingPlan: true,
    });

    const blockedState = createEditorUiState(reviewDefinitionContext);
    expect(editorUiReducer(blockedState, {
      type: 'togglePlanEditor',
      context: reviewDefinitionContext,
    })).toEqual({
      activeSurface: 'definition',
      isEditingPlan: false,
    });
  });

  it('moves committed workflows to run and closes advanced JSON editing', () => {
    const state = { activeSurface: 'plan' as const, isEditingPlan: true };

    expect(editorUiReducer(state, {
      type: 'commit',
      context: committedFreshContext,
    })).toEqual({
      activeSurface: 'run',
      isEditingPlan: false,
    });
  });
});
