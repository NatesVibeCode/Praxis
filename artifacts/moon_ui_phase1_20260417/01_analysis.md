# MoonBuildPage.tsx — Architectural Analysis

Source: `surfaces/app/src/moon/MoonBuildPage.tsx` (1607 lines)

---

## Layout Regions

The top-level return is a single `div.moon-page` that contains the following distinct visual/functional regions, listed in DOM order.

### 1. Error Toast Banner
**Approx. lines: 870–878**
Conditionally rendered `div.moon-error-toast` driven by `mutationError`. Sits above everything in the stacking context. Contains a dismiss button. This is a secondary error channel alongside the `Toast` primitive.

### 2. Middle Row (`moon-middle`)
**Approx. lines: 880–1090**
A flex container whose class name encodes open-dock state (`moon-middle--action-open`, `--context-open`, `--release-open`). Parent shell for the three horizontal panels.

#### 2a. Left Dock — Action Overlay
**Approx. lines: 882–897**
`div.moon-dock-overlay--left`. Conditionally mounts `<MoonActionDock>` when `actionOpen === true`. Receives `workflowId`, `payload`, `selectedNodeId`, drag-start callback, payload-change callback, and catalog-change callback. Acts as the toolbox / catalog source.

#### 2b. Center Canvas
**Approx. lines: 898–1060**
`div.moon-center`. Two mutually exclusive sub-trees:

- **Empty State** (`moon-start`, lines ~925–1005): Rendered when `!hasNodes`. Contains three sub-panels that are shown/hidden by `state.emptyMode`:
  - Nucleus circle (mode=`choice`) — entry point to mode selection
  - Selection window (mode=`selection`) — choice cards for "Pick a trigger" vs "Describe it"
  - Compose panel (mode=`compose`) — prose textarea, example chips, compile button; shown via `showComposePanel`

- **Graph Canvas** (`moon-graph`, lines ~1005–1060): Rendered when `hasNodes`. Contains:
  - Dock toggle buttons (`moon-center__dock-actions`) — `DockToggleButton` pair for Action/Detail
  - HalfMoon release invitation — `<HalfMoon position="bottom">`, shown when `!releaseOpen && !state.runViewOpen`
  - `<MoonEdges>` — SVG edge layer
  - Edge gate controls loop (`moon-graph-gate`) — one pod per edge with inline gate card
  - Node ring loop (`moon-graph-node`) — one ring div per `viewModel.nodes` entry
  - Append target (`moon-graph-append`) — clickable/drop target to add a new node
  - `<MoonDragGhost>` — drag preview overlay

#### 2c. Right Dock — Detail Overlay
**Approx. lines: 1062–1090**
`div.moon-dock-overlay--right`. Conditionally mounts `<MoonNodeDetail>` when `contextOpen === true`. Receives node, edge, build graph, multiple commit callbacks, and gate item list.

### 3. Bottom Dock
**Approx. lines: 1092–1115**
`div.moon-dock-bottom`. Mutually exclusive between two panels:
- `<MoonRunPanel>` — shown when `state.runViewOpen && state.activeRunId`
- `<MoonReleaseTray>` — shown when `releaseOpen`

### 4. Trigger Picker Menu (Portal)
**Approx. lines: 1117–1128**
`<MenuPanel>` rendered when `state.emptyMode === 'trigger-picker'`. Anchored to `triggerAnchorRef`. Driven entirely by `triggerMenuSections` (a `useMemo` over the catalog).

### 5. Node Popout (Portal)
**Approx. lines: 1130–1141**
`<MoonPopout>` rendered when `state.selectedNodeId && state.popoutOpen && viewModel.selectedNode`. Anchored to `selectedNodeAnchorRect` (itself a `useMemo` off `centerRef`).

### 6. Toast
**Approx. lines: 1143**
Stateless `<Toast />` primitive; its imperative state lives inside `useToast`.

---

## State Ownership

### Primary state stores

| Identifier | Kind | Current home | Layout regions that read it | Layout regions that write it | Disposition |
|---|---|---|---|---|---|
| `payload` / `setPayload` | `useBuildPayload` hook | Top-level | Graph canvas, both docks, release tray, compose panel | Action dock, node/edge mutation handlers, compile handler | Could not be lifted (workflowId is the key). A `useMoonPayload` wrapper hook would sharpen the boundary. |
| `state` / `dispatch` | `useReducer` (moonBuildReducer) | Top-level | Every region reads `state.*` sub-fields | All handler callbacks + a dozen inline `onClick`s | Over-aggregated. Sub-domains (empty-mode, active/selected, dock open, run view, drag) could each have colocated owners once regions are extracted. |
| `catalog` / `setCatalog` | `useState` | Top-level | Action dock, trigger picker, popout, gate controls | Three `useEffect`s for catalog loading | Extract into `useMoonCatalog()`. Currently loaded in three overlapping effects. |
| `moonGlowProfile` / `setMoonGlowProfile` | `useState` | Top-level | Root `div[data-moon-glow-profile]` only | A `storage` event listener effect | Pure cross-cutting concern; extract into `useMoonGlowProfile()`. |
| `mutationError` / `setMutationError` | `useState` | Top-level | Error toast banner | 7+ catch blocks + error propagation effect | Could be merged into the reducer or extracted into a `useMutationError()` hook alongside `runMutation`. |
| `centerWidth` / `setCenterWidth` | `useState` | Top-level | `translateX` useMemo only | ResizeObserver effect on `centerRef` | Extract into `useMoonCanvasOffset(centerRef, activeIdx)`. |

### Derived / memoised values

| Identifier | Depends on | Layout regions that read it | Disposition |
|---|---|---|---|
| `persistedWorkflowId` | `workflowId`, `payload` | `draftGuardState` memo, `moonUndoScope` inline | Fine at top level; trivial derivation. |
| `draftGuardState` | `payload`, `persistedWorkflowId`, `state.compileProse`, `state.selectedTrigger` | Propagated to `onDraftStateChange` prop | Lives in a side-effect; does not render. Move to custom hook or effect group. |
| `viewModel` | `payload`, `state.selectedNodeId`, `state.activeNodeId`, `runJobs` | Graph canvas, both docks, popout | Central presentation model; correct home is top-level, but should be the only dependency surface for rendering children. |
| `runJobs` | `activeRun?.jobs` | `viewModel` memo | Inline fine; trivial. |
| `contractSuggestionExtras` | `payload` | Right dock (`MoonNodeDetail`) only | Could be colocated inside the right dock when it becomes a component. |
| `triggerMenuSections` | `catalog`, `handleTriggerSelect`, `state.selectedTrigger?.id` | Trigger picker MenuPanel | Could move into trigger picker if extracted as a component. |
| `selectedNodeAnchorRect` | `state.selectedNodeId`, `centerRef`, `viewModel.nodes` | Popout anchor | Colocate with popout once extracted. |
| `translateX` | `activeIdx`, `centerWidth`, `hasMeasured` | Graph canvas transform style | Belongs in `useMoonCanvasOffset`. |
| `nodeById` | `viewModel.nodes` | Edge controls loop | Stays near graph canvas. |
| `gateCatalogByFamily` | `catalog` | Edge controls loop | Stays near graph canvas or in `useMoonCatalog`. |
| `edgeControls` | `viewModel.edges`, `viewModel.layout`, `gateCatalogByFamily`, `nodeById` | Gate pod loop in graph canvas | Belongs inside graph canvas component. |
| `appendPosition` | `viewModel.layout.width` | Append target div | Belongs inside graph canvas component. |
| `middleClassName` | `actionOpen`, `contextOpen`, `releaseOpen` | Middle row div | Trivial derived string; fine inline. |

### Callbacks / imperative logic

| Identifier | Closes over | Layout regions that invoke it |
|---|---|---|
| `runMutation` | `mutate`, `setMutationError` | All mutation paths |
| `commitMoonGraphAction` | `payload`, `workflowId`, `runMutation`, `setPayload`, `applyGraphPayload`, `buildMutationUndoDescriptor`, `buildDraftGraphUndoDescriptor`, `moonUndoScope`, `handleUndoAction`, `show` | Every graph edit handler |
| `commitMoonAuthorityAction` | `buildMutationUndoDescriptor`, `handleUndoAction`, `moonUndoScope`, `runMutation`, `show` | Right dock via inline adapter |
| `handleNodeAction` | `catalog`, `commitMoonGraphAction`, `payload`, `state.activeNodeId`, `state.openDock` | Node click, drag-drop, popout select |
| `handleCreateBranch` | `commitMoonGraphAction`, `payload` | Gate pod (inline JSX), `handleApplyGate` |
| `handleApplyGate` | `catalog`, `commitMoonGraphAction`, `handleCreateBranch`, `payload` | Gate pod (inline JSX), right dock |
| `handleCompile` | `onWorkflowCreated`, `setPayload`, `state.compileProse`, `state.selectedTrigger`, `workflowId` | Compose panel |
| `handleTriggerSelect` | `commitMoonGraphAction` | Trigger picker menu |
| `appendNode` | `commitMoonGraphAction`, `payload` | Append target, drag-drop |
| `reorderNode` | `commitMoonGraphAction`, `payload` | Drag-drop |
| `drag` / `startCatalogDrag` / `startNodeDrag` | `drag`, `catalog`, `reorderNode`, `appendNode`, `applyCatalogToNode`, `applyCatalogToEdge`, `dispatch` | Action dock (drag start), graph canvas (node drag) |

---

## Coupling Surfaces

### 1. `commitMoonGraphAction` mega-closure (approx. lines 550–620)
This single callback is the choke point for every graph mutation. It closes over `payload`, `workflowId`, `runMutation`, `setPayload`, `applyGraphPayload`, `buildMutationUndoDescriptor`, `buildDraftGraphUndoDescriptor`, `moonUndoScope`, `handleUndoAction`, and `show`. Any change to undo semantics, toast behavior, draft-vs-persisted branching, or the action ledger API requires touching this function. Every handler (`handleNodeAction`, `handleCreateBranch`, `handleApplyGate`, `appendNode`, `reorderNode`, `handleTriggerSelect`) in turn depends on it, propagating its full closure chain upward.

### 2. Inline gate card JSX coupled to action handlers (approx. lines 1000–1055)
The `edgeControls.map()` block embeds gate selection logic (`handleCreateBranch`, `handleApplyGate`, `handleSelectEdge`) directly into JSX rather than delegating to a component. This makes it impossible to unit-test gate interaction or restyle the card without touching the monolith. The gate card contains conditional rendering for three gate states (`empty`, `conditional`, `failure`) plus a "Edit gate" button that opens the detail dock—spanning both presentation and dock orchestration.

### 3. `state` reducer as a single blob coupled to every region (approx. lines 293–295, used throughout)
The `moonBuildReducer` state is a single object consumed by every region: empty-mode rendering, node/edge selection, dock open/close, release tray, run panel, drag state, compile phase, and advance queuing. Because all of these live in one reducer, any region change requires reasoning about the entire state shape. There is no encapsulation boundary between e.g. "empty state flow" and "active graph interaction."

### 4. Right dock adapter (approx. lines 1062–1090)
`MoonNodeDetail` receives four distinct callback adapters—`onMutate`, `onUpdateBuildGraph`, `onCommitGraphAction`, `onCommitAuthorityAction`—each of which is an inline async arrow function wrapping `commitMoonGraphAction` or `runMutation`. The inline `afterApply`/`afterUndo` closures also call `dispatch` for both `viewModel.selectedNode` and `selectedEdge` branches. This makes the right dock's interaction contract impossible to test or refactor without the parent.

### 5. Three-way catalog hydration effects (approx. lines 378–430)
Catalog loading is spread across three separate `useEffect` calls with overlapping conditions (`catalog.length`, `state.emptyMode`, mount). The three effects share `setCatalog` but have different retry triggers; their interaction is only understood by reading all three together. A bug in any one (e.g. the 2500ms self-heal timer) is invisible unless all three are held in mind simultaneously.

---

## Decomposition Hints

1. **Empty-state is a self-contained sub-flow.** The nucleus circle, selection window, and compose panel only interact with `state.emptyMode`, `state.compileProse`, `state.selectedTrigger`, `catalog`, and two handlers (`handleCompile`, `handleTriggerSelect`). They have no dependency on the graph canvas. This entire branch could be extracted and given its own state slice.

2. **Graph canvas is a distinct rendering domain.** Everything inside `div.moon-graph` (edges, gate pods, node rings, append target, drag ghost) is driven by `viewModel`, `edgeControls`, and a handful of callbacks. It owns its own position calculations (`getMoonNodeCanvasPosition`, `getMoonCanvasDimensions`, `appendPosition`, `translateX`). These are the only concerns that need `centerRef` and the ResizeObserver.

3. **The gate pod card is a render-heavy inline block that warrants its own component boundary.** The `edgeControls.map()` renders a full card with branching UI for three gate states. This inline block makes the graph canvas JSX hard to scan and impossible to test in isolation.

4. **`commitMoonGraphAction` is already a protocol, not a component concern.** Its signature has stabilised into a well-defined interface (`label`, `reason`, `outcome`, `target`, `changeSummary`, `nextPayload`, `afterApply`, `afterUndo`). It belongs in a custom hook (`useMoonMutationCommit`) that returns it alongside `commitMoonAuthorityAction` and `runMutation`, decoupled from the component tree.

5. **Catalog hydration is a self-contained async concern.** The three overlapping catalog `useEffect` calls, the self-heal timer, and the `setCatalog`/`getCatalog` interaction belong in a `useMoonCatalog()` hook that returns `[catalog, reloadCatalog]` with a single, unified retry strategy.

6. **Undo executor registration is framework plumbing, not UI logic.** The two `useEffect` calls that call `registerUiActionUndoExecutor` are side-effects with no rendering consequence. They could be isolated in a `useMoonUndoRegistry(moonUndoScope, workflowId, setPayload, runMutation)` hook.

7. **`moonGlowProfile` is a pure localStorage preference with no graph coupling.** A `useMoonGlowProfile()` hook returning `[moonGlowProfile]` would make the preference portable without touching the main component.

8. **The reducer's responsibilities span at least four distinct state domains**: (a) empty-mode navigation, (b) selection (node/edge/trigger), (c) dock/tray open state, (d) run/compile lifecycle. These domains have little cross-dependency and could be separated, potentially eliminating the need for a monolithic reducer.

9. **The draft guard is a side-effect-only concern.** `draftGuardState` and its `useEffect` that calls `onDraftStateChange` have no rendering consequence inside the component—they communicate only upward. This is a good candidate for isolation so that the dirty-state logic doesn't contribute to the main render derivation chain.

10. **`selectedNodeAnchorRect` ties geometry to selection state in the top-level memo.** Because it reads `centerRef.current.getBoundingClientRect()` at memo time, it creates a subtle timing coupling between DOM layout and React render. Extracting this into the Popout itself (as an effect or callback-ref approach) would remove `centerRef` from the set of top-level memo dependencies and make the geometry concern local to the component that uses it.
