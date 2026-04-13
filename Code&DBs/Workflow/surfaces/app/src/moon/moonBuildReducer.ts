export type DragDropKind = 'node' | 'edge' | 'append';

export interface MoonBuildState {
  activeNodeId: string | null;
  selectedNodeId: string | null;
  selectedEdgeId: string | null;
  openDock: 'action' | 'context' | null;
  releaseOpen: boolean;
  popoutOpen: boolean;
  compileProse: string;
  compilePhase: 'idle' | 'compiling' | 'error';
  compileError: string | null;
  advanceQueued: boolean;
  dragItemId: string | null;
  dragDropKind: DragDropKind | null;
  previewTarget: string | null;
  pendingCatalogId: string | null;  // click-fallback: catalog item awaiting target click
  emptyMode: 'choice' | 'trigger-picker' | 'compose' | null;
  selectedTrigger: { id: string; label: string; icon: string; actionValue: string } | null;
  activeRunId: string | null;
  runViewOpen: boolean;
}

export type MoonBuildAction =
  | { type: 'SELECT_NODE'; nodeId: string | null }
  | { type: 'SELECT_EDGE'; edgeId: string | null }
  | { type: 'SET_ACTIVE'; nodeId: string | null }
  | { type: 'ADVANCE_ACTIVE'; nextUnresolvedId: string | null }
  | { type: 'OPEN_DOCK'; dock: 'action' | 'context' }
  | { type: 'CLOSE_DOCK' }
  | { type: 'TOGGLE_RELEASE' }
  | { type: 'OPEN_POPOUT' }
  | { type: 'CLOSE_POPOUT' }
  | { type: 'SET_PROSE'; prose: string }
  | { type: 'COMPILE_START' }
  | { type: 'COMPILE_SUCCESS' }
  | { type: 'COMPILE_ERROR'; error: string }
  | { type: 'DRAG_START'; itemId: string; dropKind: DragDropKind }
  | { type: 'DRAG_PREVIEW'; targetId: string | null }
  | { type: 'DRAG_END' }
  | { type: 'STAGE_CATALOG'; catalogId: string }
  | { type: 'CLEAR_CATALOG' }
  | { type: 'EMPTY_PICK_TRIGGER' }
  | { type: 'EMPTY_PICK_COMPOSE' }
  | { type: 'EMPTY_RESET' }
  | { type: 'SELECT_TRIGGER'; trigger: { id: string; label: string; icon: string; actionValue: string } }
  | { type: 'DISPATCH_SUCCESS'; runId: string }
  | { type: 'CLOSE_RUN' }
  | { type: 'TOGGLE_RUN_VIEW' }
  | { type: 'RESET' };

export const initialMoonBuildState: MoonBuildState = {
  activeNodeId: null,
  selectedNodeId: null,
  selectedEdgeId: null,
  openDock: null,
  releaseOpen: false,
  popoutOpen: false,
  compileProse: '',
  compilePhase: 'idle',
  compileError: null,
  advanceQueued: false,
  dragItemId: null,
  dragDropKind: null,
  previewTarget: null,
  pendingCatalogId: null,
  emptyMode: 'choice',
  selectedTrigger: null,
  activeRunId: null,
  runViewOpen: false,
};

export function moonBuildReducer(state: MoonBuildState, action: MoonBuildAction): MoonBuildState {
  switch (action.type) {
    case 'SELECT_NODE':
      return {
        ...state,
        selectedNodeId: action.nodeId,
        selectedEdgeId: null,
        popoutOpen: action.nodeId !== null,
      };
    case 'SELECT_EDGE':
      return { ...state, selectedEdgeId: action.edgeId, selectedNodeId: null, popoutOpen: action.edgeId !== null };
    case 'SET_ACTIVE':
      return { ...state, activeNodeId: action.nodeId, advanceQueued: false };
    case 'ADVANCE_ACTIVE':
      return {
        ...state,
        activeNodeId: action.nextUnresolvedId,
        selectedNodeId: action.nextUnresolvedId,
        selectedEdgeId: null,
        popoutOpen: action.nextUnresolvedId !== null,
        advanceQueued: false,
      };
    case 'OPEN_DOCK':
      return { ...state, openDock: action.dock, releaseOpen: false };
    case 'CLOSE_DOCK':
      return { ...state, openDock: null };
    case 'TOGGLE_RELEASE':
      return { ...state, releaseOpen: !state.releaseOpen, openDock: null };
    case 'OPEN_POPOUT':
      return { ...state, popoutOpen: true };
    case 'CLOSE_POPOUT':
      return { ...state, popoutOpen: false };
    case 'SET_PROSE':
      return { ...state, compileProse: action.prose, compileError: null };
    case 'COMPILE_START':
      return { ...state, compilePhase: 'compiling', compileError: null };
    case 'COMPILE_SUCCESS':
      return { ...state, compilePhase: 'idle', compileProse: '', advanceQueued: true, emptyMode: null };
    case 'COMPILE_ERROR':
      return { ...state, compilePhase: 'error', compileError: action.error };
    case 'DRAG_START':
      return { ...state, dragItemId: action.itemId, dragDropKind: action.dropKind };
    case 'DRAG_PREVIEW':
      return { ...state, previewTarget: action.targetId };
    case 'DRAG_END':
      return { ...state, dragItemId: null, dragDropKind: null, previewTarget: null };
    case 'STAGE_CATALOG':
      return { ...state, pendingCatalogId: action.catalogId };
    case 'CLEAR_CATALOG':
      return { ...state, pendingCatalogId: null };
    case 'EMPTY_PICK_TRIGGER':
      return { ...state, emptyMode: 'trigger-picker' as const, selectedTrigger: null };
    case 'EMPTY_PICK_COMPOSE':
      return { ...state, emptyMode: 'compose' as const };
    case 'EMPTY_RESET':
      return { ...state, emptyMode: 'choice' as const, selectedTrigger: null };
    case 'SELECT_TRIGGER':
      return { ...state, selectedTrigger: action.trigger, emptyMode: null };
    case 'DISPATCH_SUCCESS':
      return { ...state, activeRunId: action.runId, runViewOpen: true, releaseOpen: false };
    case 'CLOSE_RUN':
      return { ...state, activeRunId: null, runViewOpen: false };
    case 'TOGGLE_RUN_VIEW':
      return { ...state, runViewOpen: !state.runViewOpen };
    case 'RESET':
      return initialMoonBuildState;
    default:
      return state;
  }
}
