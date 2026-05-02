# Praxis primitive-adoption guide

> Standing-order row · `architecture-policy::design-system-single-react-primitive-library`
> Owner · `@nate` (CODEOWNERS)

## Single source of truth

Praxis has **one** React primitive library: `Code&DBs/Workflow/surfaces/app/src/primitives/`. It renders the `prx-*` CSS classes from `styles/primitives.css`. There is **no** parallel `src/primitives-prx/` library for presentational components — the `primitives-prx/` directory contains only **stateful** adapters (PromptInput, Tabstrip, DispatchButton) for behaviors that don't fit pure CSS.

This decision is binding. New primitives go through one of two paths:

| primitive shape | where it lives | how consumers use it |
|---|---|---|
| presentational (no behavior beyond CSS pseudo-classes) | `styles/primitives.css` | raw `<div className="prx-…">` markup |
| stateful (autocomplete, keyboard accelerators, hash calc, …) | `primitives-prx/` | `import { ... } from '@/primitives-prx'` |
| reusable React shape over a presentational primitive | `primitives/` | `import { DataTable } from '@/primitives'` |

Reviewers should reject PRs that re-introduce a parallel React primitive library or add adapters for purely presentational primitives.

## What lives where, today

### Presentational — raw className, no wrapper

Use these as `<div className="prx-X">…</div>` directly. There is **no** React adapter.

| primitive class | purpose |
|---|---|
| `prx-section-strip` | cap-marker eyebrow above section blocks |
| `prx-chip[data-source]` | data pill with provenance glyph |
| `prx-gate` | approval-gate badge |
| `prx-tree` | manifest / scope tree |
| `prx-runlog` | syslog-style row stream |
| `prx-diff` | `+/-/=/!` line marks |
| `prx-kbd-cluster` | keyboard chord display |
| `prx-gauge` | discrete-tick meter |
| `prx-receipt[data-state]` | sealed-receipt card |
| `prx-spark` | inline sparkline (SVG) |
| `prx-bargraph` | block-bar history (`▁▂▃…█`) |
| `prx-led[data-tone]` | pulsing status dot |
| `prx-cursor` | stepped block-cursor |
| `prx-frame[data-tone]` | ASCII corner-bracket frame |
| `prx-lcd` | block-character progress bar |
| `prx-rule[data-tone]` | marquee-glyph hairline rule |
| `prx-agent` | agent identity pill |
| `prx-icon-tile[data-size,data-tone]` | brand iconography slot |
| `prx-node-band` | RECEIVES · NUCLEUS · PRODUCES inspector header |
| `prx-flow-node[data-state]` | op card on flow canvas (incl. `placeholder`) |
| `prx-flow-edge` | type-labeled edge connector |
| `prx-flow-cursor` | live execution position marker |
| `prx-status-rail` | one-row compressed readout |
| `prx-numeral` | live odometer-style counter (CSS only — `data-value` updates from outside) |
| `prx-radar` | sweep radar SVG |
| `prx-spinner` | braille / quadrant spinner |
| `prx-diag` | BIOS-style diagnostic readout |
| `prx-transport` | tape-deck transport buttons |
| `[data-tip]` | hover tooltip |

### React primitives — `primitives/`

Render the prx-* structure with controlled-component semantics.

| component | renders | public API |
|---|---|---|
| `DataTable` | `prx-table` with fixed-layout containment, sort, click | `columns, data, onRowClick, selectedIndex, emptyState` |
| `StatsRow` | `prx-status-rail` (compressed multi-stat row) | `stats: { label, value, color?, tone? }[]` |
| `MetricCard` | `prx-roi`-style stat tile | `label, value, color` |
| `ActivityFeed` | `prx-runlog` rows with relative-time + status caps | `title?, data: unknown[]` |
| `LoadingSkeleton` | `ws-skeleton` content-shape hint | `lines, height, width, widths` |
| `Toast` / `useToast` | own dedicated stylesheet (`app-toast-*`) | hook + portal |
| `ChartView`, `StatusGrid`, `SlotLayout` | unchanged — domain wrappers, not primitives |

### Stateful adapters — `primitives-prx/`

Only behaviors pure CSS can't express.

| component | state managed | consumer use |
|---|---|---|
| `PromptInput` | autocomplete cursor, classification | `<PromptInput refs={...} onChange={...} />` |
| `Tabstrip` | active tab, kbd accelerators | `<Tabstrip tabs={...} value={...} onChange={...} />` |
| `DispatchButton` | hash calc, replay-cache tracking, dry-run | `<DispatchButton op="…" payload={...} idempotencyPolicy="…" onDispatch={...} />` |

## Canvas UI adoption map

| Canvas file | swap target |
|---|---|
| `canvas/CanvasBuildPage.tsx` | `prx-flow[data-layout="dag"]`, `prx-flow-node` (incl. placeholder), `prx-edge-halo`, `prx-flow-cursor`, `<Tabstrip>` |
| `canvas/CanvasNodeDetail.tsx` | `prx-node-band` header, `prx-icon-tile` capability id, `<Tabstrip>` body, schema-driven form |
| `canvas/CanvasPickers.tsx` | `<Tabstrip>` for verb-grouping, `<PromptInput>` for describe-path, `prx-icon-tile` per row |
| `canvas/CanvasReleaseTray.tsx` | `prx-runlog` checklist + `<DispatchButton opensDrawer={false}>` for fire vs `▸` chevron drawer-opener |
| `canvas/CanvasRunOverlay.tsx` | `prx-flow-cursor`, `<StatsRow>` (renders status-rail), `prx-runlog` |
| `canvas/CanvasEdges.tsx` | `prx-edge-halo` gradient, tighten edge labels |
| `canvas/CanvasOutcomeContract.tsx` | `prx-tree`, `prx-chip[data-source]`, `prx-gate` |
| `canvas/CanvasDataDictionaryPanel.tsx` | `<DataTable>` + drawer |
| `canvas/CanvasDecisionsPanel.tsx` | `<DataTable>` + `prx-frame[data-tone="warn"]` for active orders |
| `canvas/CanvasIntegrationsPanel.tsx` | `<DataTable>` |
| `canvas/CanvasBindingReviewQueue.tsx` | `prx-gate` rows |
| `canvas/CanvasSurfaceReviewPanel.tsx` | `<StatsRow>` (status-rail), `prx-frame` |
| `canvas/CanvasRunPanel.tsx` | `<ActivityFeed>` + `<DataTable>` for runs |
| `dashboard/Dashboard.tsx` | `<StatsRow>` replaces card-shaped readouts |
| `dashboard/RunDetailView.tsx` | `<ActivityFeed>`, `prx-receipt`, `<Tabstrip>` |
| `dashboard/RunEvidencePanel.tsx` | `prx-event-chain` (raw classes), `prx-receipt` |
| `dashboard/CostsPanel.tsx` | `<MetricCard>`, `prx-bargraph`, `prx-roi` |
| `dashboard/StrategyConsole.tsx` | `<PromptInput>`, `prx-receipt` |
| `dashboard/ChatPanel.tsx` | `<PromptInput>` for input, `prx-receipt`/event-chain for response |
| `launcher/LauncherFrontdoor.tsx` | `<PromptInput>` dominant, `<Tabstrip>` surface chooser |

## Rollout policy

1. Per-file PRs. No "land 5 files at once" mega-PRs.
2. Each PR must keep the file's existing `.test.tsx` passing.
3. Tests querying by tag tree must be migrated to `data-testid` / `getByRole` / `getByText` BEFORE the structural refactor lands. (Most existing test files already use role/text queries — verify per file.)
4. Adopt one library only. Do not import `prx-*` into `src/primitives/` files as a hand-rolled replacement; refactor the existing component to render `prx-*` structure. New presentational shapes don't earn a React wrapper unless a stateful behavior demands it.
5. PRs touching `primitives/`, `primitives-prx/`, or `styles/primitives*.css` require approval from the design-system owner per CODEOWNERS.
6. Each refactor PR runs the contract test (`primitives/__tests__/contract.test.tsx`) — fails on PR if a primitive loses its `prx-*` class.

## Phase 2 sequencing — Canvas UI adoption (19 files)

Sequenced by complexity to build pattern confidence on small files before tackling the canvas-grade complexity. Parallel agents OK within a phase, never across phases.

### 2.a — Warmup (weeks 1–2 from kickoff)

Small, high-test-coverage files that pressure-test the adapters under real consumers without canvas complexity.

| file | LOC | adapters used |
|---|---|---|
| `canvas/CanvasEdges.tsx` | 230 | raw `prx-flow-edge` + `prx-edge-halo` gradient |
| `launcher/LauncherFrontdoor.tsx` | 318 | `<PromptInput>`, `<Tabstrip>` |
| `canvas/CanvasSurfaceReviewPanel.tsx` | (small) | `<StatsRow>` + raw `prx-frame` |

**Exit criteria:** all 3 land + tests pass + design-system owner reviews the diffs and signs off on the pattern.

### 2.b — Canvas (weeks 3–5)

The hard ones. Land only after 2.a proves the adapter pattern.

| file | LOC | adapters used |
|---|---|---|
| `canvas/CanvasNodeDetail.tsx` | 2,782 | raw `prx-node-band` + `<Tabstrip>` + schema-driven `<DataTable>` for inputs |
| `canvas/CanvasBuildPage.tsx` | 3,113 | raw `prx-flow[data-layout="dag"]`, `prx-flow-node`, `prx-flow-cursor`, `<Tabstrip>` |
| `canvas/CanvasPickers.tsx` | 783 | `<PromptInput>`, `<Tabstrip>`, raw `prx-icon-tile` |
| `canvas/CanvasReleaseTray.tsx` | 480 | raw `prx-runlog` + `<DispatchButton opensDrawer>` |
| `canvas/CanvasRunOverlay.tsx` | 376 | raw `prx-flow-cursor` + `<StatsRow>` + `<ActivityFeed>` |

**Exit criteria:** all canvas surfaces render through the primitives. No bespoke release/inspector/picker markup remains.

### 2.c — Panels (week 6)

Straight `<DataTable>` consumers. Lower risk; can be parallelized.

- `canvas/CanvasDataDictionaryPanel.tsx`
- `canvas/CanvasDecisionsPanel.tsx`
- `canvas/CanvasIntegrationsPanel.tsx`
- `canvas/CanvasBindingReviewQueue.tsx`
- `canvas/CanvasOutcomeContract.tsx`

### 2.d — Run / Dashboard (weeks 7–8)

Runlog + event-chain consumers. Pairs with the receipt drawer pattern from 2.b.

- `canvas/CanvasRunPanel.tsx`
- `dashboard/RunDetailView.tsx`
- `dashboard/RunEvidencePanel.tsx`
- `dashboard/CostsPanel.tsx`
- `dashboard/Dashboard.tsx`

### 2.e — Chat / Strategy (week 9)

`<PromptInput>` consumers.

- `dashboard/StrategyConsole.tsx`
- `dashboard/ChatPanel.tsx`

### Adoption gates between phases

- 2.a → 2.b: design-system owner sign-off on the warmup diffs
- 2.b → 2.c: canvas surfaces visually inspected against firmware + lite mode
- 2.c → 2.d: contract test still passes on every refactored primitive
- 2.d → 2.e: telemetry shows `frontend.primitive.event` rows landing for at least 5 surfaces
- After 2.e: archive `src/primitives-prx/lazyMode.ts` mode toggle to a single feature surface (Settings panel) — no more free-floating localStorage toggle

## Validation

```bash
# Confirm no parallel presentational adapter library exists
[ "$(find Code\&DBs/Workflow/surfaces/app/src/primitives-prx -name '*.tsx' | wc -l)" -le 4 ] || echo "FAIL: too many adapters"

# Confirm primitives/ files render prx-* classes
grep -l "prx-" Code\&DBs/Workflow/surfaces/app/src/primitives/*.tsx | wc -l

# Confirm CODEOWNERS binds the design-system paths
grep "primitives" .github/CODEOWNERS
```
