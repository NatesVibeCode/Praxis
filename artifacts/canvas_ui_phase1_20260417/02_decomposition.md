## Proposed Subcomponents

Here are 6 proposed subcomponents and hooks to decompose `CanvasBuildPage`.

### 1. `useCanvasMutations` (Hook)
- **Responsibility:** Centralizes all mutation logic, including graph and authority actions, undo registration, and toast notifications.
- **TypeScript Props Interface:** Not a component, but its returned interface would be:
  ```typescript
  interface CanvasMutations {
    commitGraphAction: (details: GraphActionDetails) => Promise<UiActionEntry | undefined>;
    commitAuthorityAction: (details: AuthorityActionDetails) => Promise<UiActionEntry | undefined>;
    runMutation: (subpath: string, body: Record<string, unknown>) => Promise<BuildPayload | null>;
    mutationError: string | null;
  }

  // (GraphActionDetails and AuthorityActionDetails would be formally defined)
  ```
- **Internal State:** `mutationError`, logic for `runUiAction`, `undoUiAction`, and showing toasts.
- **Target File Path:** `surfaces/app/src/canvas/hooks/useCanvasMutations.ts`

### 2. `useCanvasCatalog` (Hook)
- **Responsibility:** Manages fetching, caching, and providing the action/gate catalog.
- **TypeScript Props Interface:**
  ```typescript
  // Hook returns:
  [
    catalog: CatalogItem[],
    gateCatalogByFamily: Map<string, CatalogItem>,
    reloadCatalog: () => Promise<void>,
  ]
  ```
- **Internal State:** `catalog` array, loading/error states.
- **Target File Path:** `surfaces/app/src/canvas/hooks/useCanvasCatalog.ts`

### 3. `CanvasEmptyState`
- **Responsibility:** Renders and manages the initial state of the page when no graph exists, including the trigger/compose choice and the prose compiler.
- **TypeScript Props Interface:**
  ```typescript
  interface CanvasEmptyStateProps {
    onTriggerSelected: (item: CatalogItem) => void;
    onCompileProse: (prose: string, trigger?: CatalogItem) => void;
  }
  ```
- **Internal State:** `emptyMode` (`'choice' | 'compose' | 'trigger-picker'`), `compileProse` string, `selectedTrigger`.
- **Target File Path:** `surfaces/app/src/canvas/components/CanvasEmptyState.tsx`

### 4. `CanvasGraphCanvas`
- **Responsibility:** Renders the interactive graph of nodes and edges, handles drag-and-drop, and manages canvas-specific geometry like zoom/pan offsets.
- **TypeScript Props Interface:**
  ```typescript
  interface CanvasGraphCanvasProps {
    viewModel: CanvasBuildViewModel; // The result from presentBuild()
    activeNodeId: string | null;
    selectedNodeId: string | null;
    selectedEdgeId: string | null;
    onSelectNode: (nodeId: string | null) => void;
    onSelectEdge: (edgeId: string | null, options?: { openDetail?: boolean }) => void;
    onNodeAction: (nodeId: string, actionValue: string) => void;
    onApplyGate: (edgeId: string, gateFamily: string) => void;
    onAppendNode: (label?: string) => void;
    onReorderNode: (sourceNodeId: string, targetNodeId: string) => void;
  }
  ```
- **Internal State:** `centerRef`, `centerWidth`, `translateX` for canvas positioning. Drag state will be provided by `useCanvasDrag`.
- **Target File Path:** `surfaces/app/src/canvas/components/CanvasGraphCanvas.tsx`

### 5. `CanvasGateControl`
- **Responsibility:** Renders the interactive card for an edge, allowing users to apply a gate or create a branch.
- **TypeScript Props Interface:**
  ```typescript
  interface CanvasGateControlProps {
    edge: OrbitEdge;
    fromNode: OrbitNode | undefined;
    toNode: OrbitNode | undefined;
    gateItem: CatalogItem | null;
    isSelected: boolean;
    onSelect: (edgeId: string) => void;
    onOpenDetail: (edgeId: string) => void;
    onCreateBranch: (edgeId: string, side: 'above' | 'below') => void;
    onApplyFailureGate: (edgeId: string) => void;
  }
  ```
- **Internal State:** None (fully controlled component).
- **Target File Path:** `surfaces/app/src/canvas/components/CanvasGateControl.tsx`

### 6. `useDraftManager` (Hook)
- **Responsibility:** Encapsulates the logic for tracking the "dirty" state of a local draft and communicating it upwards.
- **TypeScript Props Interface:**
  ```typescript
  // Hook signature:
  useDraftManager({
    onDraftStateChange,
    persistedWorkflowId,
    payload,
    // ...other primitives that define dirty state
  });
  ```
- **Internal State:** None; it's a pure `useEffect` hook.
- **Target File Path:** `surfaces/app/src/canvas/hooks/useDraftManager.ts`


## State Ownership After Decomposition

| Current State Variable | New Owner |
|---|---|
| `payload`, `setPayload` | `CanvasBuildPage` (root component, via `useBuildPayload`) |
| `state.emptyMode` | `CanvasEmptyState` |
| `state.compileProse` | `CanvasEmptyState` |
| `state.selectedTrigger` | `CanvasEmptyState` |
| `state.selectedNodeId` | `CanvasBuildPage` (controls selection for all children) |
| `state.activeNodeId` | `CanvasBuildPage` (controls selection for all children) |
| `state.selectedEdgeId` | `CanvasBuildPage` (controls selection for all children) |
| `state.openDock` | `CanvasBuildPage` (manages layout) |
| `state.releaseOpen` | `CanvasBuildPage` (manages layout) |
| `state.runViewOpen` | `CanvasBuildPage` (manages layout) |
| `state.compilePhase` | `CanvasEmptyState` or could be derived from `useCanvasMutations` |
| `catalog`, `setCatalog` | `useCanvasCatalog` hook |
| `canvasGlowProfile` | `useCanvasGlowProfile` hook |
| `mutationError` | `useCanvasMutations` hook |
| `centerWidth`, `translateX` | `CanvasGraphCanvas` component |
| `activeRun` | `CanvasBuildPage` (via `useLiveRunSnapshot`) |
| `viewModel` | `CanvasBuildPage` (derived via `useMemo`) |
| `draftGuardState` | `useDraftManager` hook |

## Migration Order

This order prioritizes extracting self-contained logic (hooks) first to simplify the parent component before extracting UI chunks. Each step is individually testable and shippable.

1.  **Extract `useCanvasCatalog` and `useCanvasGlowProfile` hooks.** These are pure, read-only logic with no dependencies on other complex state. This removes three effects and two `useState` calls from the parent immediately.
2.  **Extract `useCanvasMutations` and `useCanvasUndo` hooks.** This is the most critical step. It isolates the heaviest logic (`commit...Action`, `runMutation`, undo registration) into a testable unit. The parent component's handlers (`handleNodeAction`, `handleCreateBranch`, etc.) will become thin wrappers around the returned mutation functions.
3.  **Extract `CanvasGateControl`.** With mutation logic now in a hook, the inline gate control JSX can be replaced by a clean component. The parent will pass down the relevant edge/node data and the mutation handlers from the `useCanvasMutations` hook.
4.  **Extract `CanvasEmptyState`.** This removes a large, mutually exclusive UI branch and its local state (`emptyMode`, `compileProse`) from the parent, significantly simplifying the main return statement.
5.  **Extract `CanvasGraphCanvas`.** By now, the parent component is much smaller. The graph canvas can be extracted along with its geometry logic (`centerRef`, ResizeObserver). It will receive the `viewModel` and the simplified handlers as props.
6.  **Refactor Docks (`CanvasActionDock`, `CanvasNodeDetail`, etc.).** With the new hooks available, the props passed to these components can be simplified. Instead of passing down many individual callbacks, the parent can pass down the `mutations` object from `useCanvasMutations`, allowing the docks to call `mutations.commitAuthorityAction` directly.

## Coupling Risks

The proposed decomposition directly addresses the five key coupling risks identified in the analysis.

1.  **`commitCanvasGraphAction` mega-closure:** The `useCanvasMutations` hook dissolves this problem entirely. It provides a stable, declarative API (`commitGraphAction({...})`) to the rest of the application, hiding the imperative implementation details of UI actions, toasts, and undo descriptors.
2.  **Inline gate card JSX:** Extracting the `CanvasGateControl` component breaks this coupling. The parent `CanvasGraphCanvas` will simply map over edges and render `<CanvasGateControl>`, passing props. All presentation and interaction logic for the gate card will live in its own file, where it can be tested and styled in isolation.
3.  **Monolithic reducer:** This decomposition eliminates the `canvasBuildReducer`. Its responsibilities are distributed to more appropriate owners: empty state flow logic moves into `CanvasEmptyState`'s local state, selection state remains in the parent `CanvasBuildPage`, and layout state (dock visibility) also stays in the parent. This follows the principle of co-locating state with the components that use it.
4.  **Right dock adapter:** The `useCanvasMutations` hook will be imported and used directly within `CanvasNodeDetail`. This eliminates the need for the four callback props (`onMutate`, `onUpdateBuildGraph`, etc.), completely decoupling the dock's mutation logic from its parent.
5.  **Three-way catalog hydration:** The `useCanvasCatalog` hook provides a single source of truth for the catalog data. It encapsulates the fetching, retrying, and caching logic, presenting a clean `[catalog, reloadCatalog]` API to the rest of the app and removing the tangled `useEffect`s from the page component.

## Open Questions

1.  **ViewModel Generation:** Should the `presentBuild` function and its `useMemo` wrapper be extracted into a dedicated `useCanvasViewModel(payload, selections)` hook? This would further formalize the presentation layer and make the `CanvasBuildPage` component almost entirely a coordinator of hooks and components.
2.  **State Management for Selection:** This plan keeps selection state (`selectedNodeId`, `selectedEdgeId`) in the parent `CanvasBuildPage`. As the app grows, would a shared context (`SelectionContext`) be a cleaner way to provide this state to deeply nested children, or is prop-drilling acceptable for now?
3.  **Hook Granularity:** Is `useCanvasMutations` doing too much by handling graph actions, authority actions, and undo registration? Should it be split into `useCanvasGraphMutations` and `useCanvasAuthorityMutations` for a cleaner separation of concerns, or is the current grouping logical?
4.  **Error Handling Strategy:** The `useCanvasMutations` hook now owns `mutationError`. How should transient UI errors that *don't* come from mutations (e.g., a failed drag operation validation) be handled? Should they also be funneled through this hook, or should components manage their own local error state?
