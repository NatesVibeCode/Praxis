## Decomposition Weaknesses

1. The plan understates the reducer coupling and proposes an unsafe split. It says, "`CanvasEmptyState` ... **Internal State:** `emptyMode` (`'choice' | 'compose' | 'trigger-picker'`), `compileProse` string, `selectedTrigger`" and later "`This decomposition eliminates the `canvasBuildReducer`.`" That is not compatible with the live state machine. In the source, `selectedTrigger` is not empty-state-only state; it participates in draft dirtiness (`draftGuardState`), compile prefixing in `handleCompile`, compile success teardown, and initial trigger-seeded graph creation. `compilePhase`, `compileError`, and `advanceQueued` are also reducer-owned and drive post-compile node advancement. Pulling only part of that flow into `CanvasEmptyState` creates a split-brain state machine.

2. The proposal invents component responsibilities that do not exist in the current code. It claims `CanvasGraphCanvas` "manages canvas-specific geometry like zoom/pan offsets." There is no zoom or pan in `CanvasBuildPage.tsx`; the current behavior is a center-follow translation derived from `centerWidth`, `activeNodeId`, and `CANVAS_LAYOUT`. That is not just cosmetic wording. It signals the plan has not identified the real coupling: active-node centering is tied to root selection state and dock-open layout changes.

3. The proposed `CanvasEmptyState` interface is incomplete. The plan offers:
   "`interface CanvasEmptyStateProps { onTriggerSelected: (item: CatalogItem) => void; onCompileProse: (prose: string, trigger?: CatalogItem) => void; }`"
   That omits `initialMode`, `compilePhase`, `compileError`, the trigger menu anchor, the catalog-backed `MenuPanel` sections, and the reducer transitions that move between `'choice'`, `'selection'`, `'trigger-picker'`, and `'compose'`. The current empty branch is not a leaf widget; it is wired into top-level draft guarding and compile lifecycle.

4. The proposal overstates the benefit of `useCanvasMutations`. It says "`The `useCanvasMutations` hook dissolves this problem entirely.`" It does not. `commitCanvasGraphAction` is coupled to `payload`, `workflowId`, `setPayload`, `canvasUndoScope`, `useToast`, and caller-provided UI callbacks (`afterApply`, `afterUndo`). Moving that code to a hook changes file boundaries, not the dependency surface. The hard part is preserving UI state transitions around mutations; the proposal does not specify how that contract will be stabilized.

5. The plan defines a `CanvasGateControl` component that is too narrow for the actual graph surface. It says the parent "`will simply map over edges and render `<CanvasGateControl>`." That skips the real edge-menu behavior: geometry from `getEdgeGeometry`, hover preview state from `useCanvasDrag`, click-outside dismissal through `shouldKeepEdgeMenusOpen`, selected-edge interaction with the right dock, and branch/failure/empty summaries based on `getCatalogTruth` and `getCatalogSurfacePolicy`. The card is not an isolated presentational fragment.

6. The plan introduces a hook it never scoped. Migration step 1 says, "`Extract `useCanvasCatalog` and `useCanvasGlowProfile` hooks.`" `useCanvasGlowProfile` is not one of the six proposed subcomponents/hooks, has no interface, and no ownership table row. That is a concrete completeness failure in the plan itself.

## Migration Order Risks

1. "`Extract `useCanvasMutations` and `useCanvasUndo` hooks. This is the most critical step.`" This is the wrong first hard extraction. `commitCanvasGraphAction` currently depends on reducer-driven UI callbacks from handlers like `handleNodeAction`, `handleCreateBranch`, `handleApplyGate`, `appendNode`, and `reorderNode`. Extracting mutation infrastructure before defining the state-transition boundary guarantees churn at every caller. The safer prerequisite is to define the reducer/selection contract first, because mutation callbacks are parameterized by it.

2. "`Extract `CanvasEmptyState`. This removes ... its local state (`emptyMode`, `compileProse`) from the parent.`" That migration order is unsafe because `draftGuardState` still depends on `state.compileProse` and `state.selectedTrigger`, and `handleCompile` still depends on reducer actions `COMPILE_START`, `COMPILE_SUCCESS`, and `COMPILE_ERROR`. Moving empty-state UI before moving the draft/compile state model creates a temporary architecture where the parent no longer owns the inputs it still uses for correctness.

3. "`Extract `CanvasGraphCanvas` ... It will receive the `viewModel` and the simplified handlers as props.`" If done after the earlier steps as written, the canvas extraction lands after state has already been fragmented. But the canvas currently owns `centerRef`, `selectedNodeAnchorRect`, drag hover previews, node drag starts, append targeting, and selected-edge dismissal behavior that coordinate with popout and detail dock state. That is a large integration seam, not a late cleanup step.

4. "`Refactor Docks (`CanvasActionDock`, `CanvasNodeDetail`, etc.). ... the parent can pass down the `mutations` object from `useCanvasMutations`, allowing the docks to call `mutations.commitAuthorityAction` directly.`" This order pushes dock refactoring to the end while earlier steps already assume cleaner contracts. In reality, `CanvasNodeDetail` currently needs both authority mutation paths and graph mutation paths, plus graph data, edge labels, selected-edge metadata, and close behavior. Leaving that until last means earlier extractions still have to preserve the old wide prop surface.

## Unstated Assumptions

1. The plan assumes reducer removal is free. "`This decomposition eliminates the `canvasBuildReducer`.`" Unstated assumption: the reducer is only an organizational choice. False. The reducer encodes cross-cutting UI invariants: selecting a node clears the selected edge and conditionally opens the popout; opening a dock closes release and popout; compile success clears prose, queues active-node advancement, and exits empty mode; dispatch success opens run view and closes release. The proposal never states where those invariants move.

2. The plan assumes `CanvasNodeDetail` can import mutation hooks directly and become "completely decoupl[ed] ... from its parent." Unstated assumption: the dock does not need parent-owned graph/selection context. False. The detail dock currently depends on parent-provided `buildGraph`, `selectedEdge`, `edgeFromLabel`, `edgeToLabel`, `onApplyGate`, and close semantics that differ between edge mode and node mode.

3. The plan assumes catalog loading has a single source. "`The `useCanvasCatalog` hook provides a single source of truth for the catalog data.`" Unstated assumption: the page is the only catalog consumer. False. `CanvasActionDock` independently uses `loadCatalogEnvelope`, `refreshCatalogEnvelope`, `getCatalogEnvelope`, and reports changes upward via `onCatalogChange`. The proposal does not say whether that duplication is being consolidated or deliberately left in place.

4. The plan assumes selection can remain in the parent while children become cleanly isolated. "`This plan keeps selection state (`selectedNodeId`, `selectedEdgeId`) in the parent.`" Unstated assumption: that state is only read for rendering. False. Selection drives popout anchoring, active-node advancement guards, edge-menu dismissal, right-dock content, and drag/drop side effects.

## Coupling Surfaces Not Addressed

1. `CanvasActionDock` catalog coupling is missing. The proposal talks about "`three-way catalog hydration`" only in `CanvasBuildPage`, but `CanvasActionDock` runs its own catalog envelope lifecycle and also pushes `onCatalogChange` back to the page. Without addressing both surfaces, `useCanvasCatalog` is not a true consolidation.

2. The draft guard contract is only partially modeled. The plan creates `useDraftManager`, but it never addresses that the dirty signal depends on local draft payload plus reducer state (`selectedTrigger`, `compileProse`) and is explicitly cleared on unmount. That is a lifecycle contract, not just a derived boolean.

3. The popout/selection/edge-menu triangle is omitted. There is no mention of `pinnedSelectionRef`, `selectedNodeAnchorRect`, `dismissSelectedEdgeMenus`, `handleSelectEdge`, or `shouldKeepEdgeMenusOpen`. Those behaviors span graph canvas, document-level events, and dock visibility. Any decomposition that ignores them will regress interaction behavior.

4. The run/release surface coupling is ignored. `releaseOpen`, `runViewOpen`, `activeRunId`, `useLiveRunSnapshot`, and `CanvasReleaseTray`/`CanvasRunPanel` are not part of the decomposition narrative except as state ownership rows. But reducer actions explicitly coordinate release tray, run panel, and dock visibility. That cross-surface orchestration is a real migration concern.

5. The plan does not address root-owned payload mutation pathways outside the new hook story. `handleCompile` writes payload directly via `setPayload`; `CanvasActionDock` writes payload through `onPayloadChange`; graph edits may mutate persisted workflows or local drafts depending on `workflowId`; undo registration depends on `canvasUndoScope`. Those are separate mutation classes with different persistence semantics. The proposal flattens them into "`useCanvasMutations`" without defining the boundaries.

## Verdict

CONTESTED-NEEDS-REVISION

The plan identifies real hotspots, but it is not ready to execute because it misreads the core dependency structure. The biggest failure is the claim that it can "`eliminate[] the `canvasBuildReducer` `" while also moving `emptyMode`, `compileProse`, and `selectedTrigger` into `CanvasEmptyState`. In the current code those values participate in compile lifecycle, draft guarding, active-node advancement, popout behavior, and release/run/dock coordination. The single highest-priority revision is to replace the reducer-elimination narrative with an explicit state-boundary plan: define which reducer invariants stay centralized, which can be extracted behind hooks, and how compile/draft/selection transitions remain atomic across the new components.
