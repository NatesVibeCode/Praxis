# Praxis primitives — containment contract

> **The primitive layer is the authority. Pages are data adapters.**
> Containment is semantic, not decorative. A border means "this thing has independent authority." If it does not, no border.

This is a contract, not a moodboard. Future agents (and humans) MUST satisfy the checklist before adding any rim, radius, bg-tint, or shadow to a surface element. Failing the checklist = the element should be bare type on field.

The primitive system in this repo (`primitives.css` + `primitives-ext.css` + `DisplayPrimitives.tsx` + `StructuralPrimitives.tsx`) is the source of truth for visual language. This doc is the rule-book for *when* to reach for which primitive.

**The mandate:** stop page-by-page patching. First the canonical primitive contract and the minimal component set. Then map each visible block to exactly one primitive. Delete local lookalikes. No new frames unless the block is a proof record, discrete actionable object, or contained inspector panel.

---

## Core primitive set (the entire visual vocabulary)

This is the fixed inventory. Every visible element on every Praxis surface MUST resolve to one of these. Anything else is a local lookalike and is a regression.

| # | Role | React primitive | CSS class | Status |
|---|---|---|---|---|
| 1 | Page shell / black field | (none — global) | `.app-shell` + `.dash-page` bg | ✓ built |
| 2 | Section kicker | `<SectionStrip>` | `.prx-section-strip` | ✓ built |
| 3 | Bare metric tile (label/value/detail/action — NO border) | `<MetricTile>` | `.prx-tile` | **to build** |
| 4 | Receipt card (sealed proof record) | `<ReceiptCard>` | `.prx-receipt` | ✓ built |
| 5 | Actionable workflow card / contained inspector | `<PanelCard>` | `.prx-card` | ✓ built (renamed from FrameCard) |
| 6 | Side / list panel (kicker + count + body, NO border) | `<ListPanel>` | `.prx-list-panel` | **to build** |
| 7 | Status row (dot + body + meta in a list) | `<StatusRow>` | `.prx-status-row` | **to build** |
| 8 | Button (primary / ghost / danger × sm / md / lg) | `<Button>` | `.prx-button` | ✓ built |
| 9 | LED dot (status indicator) | `<LedDot>` | `.prx-led` | ✓ built |
| 10 | Source / token chip | `<SourceChip>` / `<TokenChip>` | `.prx-source-chip` / `.prx-chip` | ✓ built |
| 11 | Empty / error state | `<EmptyStateExplainer>` | `.prx-empty-state` | ✓ built (underused) |

**Specialized / parked** (not part of the daily set, only used in specific surfaces): `Gauge`, `Bargraph`, `Sparkline`, `DiffBlock`, `KbdCluster`, `ManifestTree`, `Runlog`, `RadioPillGroup`, `TableFilterInput`, `GateBadge`, `DispatchButton`, plus the 25 structural primitives (`AgentPill`, `ClaimGrid`, `EventChain`, `PrxTable`, `Timeline`, `WizardScaffold`, etc.). Reach for these only when the surface explicitly needs that semantic. Don't build new ones until a surface actually consumes them.

---

## Pages-as-data-adapters mandate

Page components (`Dashboard.tsx`, `MoonBuildPage.tsx`, `WorkspaceComposeSurface.tsx`, `ManifestBundleView.tsx`, `AtlasPage.tsx`) MUST NOT:
- Define their own bordered cards / panels / tiles / chips.
- Define their own per-page color tokens (`--dash-*`, `--moon-*` variants of palette tokens).
- Use inline `style={{...}}` for color / spacing / typography (positioning math only).
- Compose decorative gradients (the page-level `dash-page` glow is the only one).
- Invent local class names that mimic the primitive vocabulary (`.dash-card`, `.moon-tile`, `.workspace-panel`).

Page components SHOULD:
- Fetch data, hold state, and dispatch operations.
- Render exactly one primitive per visible block, populated with that data.
- Pass tone / size / variant props to express semantic intent.

If a primitive is missing a feature, **extend the primitive** — never one-off in the page.

---

## Delete local lookalikes

The following per-page class names are banned. Any remaining matches in the named directories must be replaced with the primitive in the right column.

| Banned class | Replacement primitive |
|---|---|
| `.dash-tile`, `.wf-stat`, `.moon-tile` | `<MetricTile>` |
| `.dash-review-item`, `.dash-run`, `.dash-file` | `<StatusRow>` |
| `.dash-panel`, `.dash-section` (as a card) | `<ListPanel>` |
| `.wf-card`, `.moon-card`, `.workspace-card` | `<PanelCard>` |
| `.dash-empty`, `.moon-empty`, `.workspace-empty` | `<EmptyStateExplainer>` |
| `.dash-receipts`, `.dash-run-instrument` (outer rim wrappers) | bare grid (no border) |
| `.dash-overview-grid`, `.dash-board__rail` (outer rim wrappers) | bare grid (no border) |
| `.workspace-compose__primary`, `.workspace-compose__ghost` | `<Button>` |
| `.workspace-compose__primary--dispatch` | `<DispatchButton>` |
| `.workspace-receipts__filter`, `.workspace-receipts__row` | `<RadioPillGroup>` / `<StatusRow>` |
| `.app-shell__surface-chip` (with inline color hex) | `<SourceChip>` |
| `.moon-compose__btn`, `.moon-compose__secondary-link` | `<Button>` |
| `.moon-center__dock-btn` | `<Button data-active>` |

Removing the class definition AND the consuming JSX — both — is the standard for "deleted lookalike."

---

## Containment audit (run before adding ANY border / radius / bg-tint)

Answer YES to exactly one. If none match, no frame — type and whitespace carry it.

1. **Sealed proof object?** Receipt, verifier result, durable hash, attached evidence.
   → `<ReceiptCard>` (kicker title + key/value rows + seal footer)
2. **Discrete actionable object with its own lifecycle?** A workflow you can open/run/delete, a draggable card, a deployable, a manifest-bound surface tab.
   → `<PanelCard>` (eyebrow + title + body + optional footer button row)
3. **Scrollable inspector or side panel?** Drawer body, detail pane, contained scroll region.
   → `<PanelCard>` — ONCE, no nesting inside another PanelCard
4. **ASCII-decorated callout?** Warning banner, hero standing-order strip, "live now" call-out.
   → `<div className="prx-frame">` (corner-bracket ASCII chrome)
5. **None of the above?** No frame. No border. No radius. No bg-tint.
   → Bare type on the page field. Whitespace separates from neighbors.

---

## Containment decision tree

```
Is this surface element a SEALED PROOF OBJECT?
├─ YES → ReceiptCard
└─ NO  ↓

Is it a DISCRETE ACTIONABLE OBJECT (workflow, draggable, manifest tab)?
├─ YES → PanelCard
└─ NO  ↓

Is it a SCROLLABLE INSPECTOR / SIDE PANEL?
├─ YES → PanelCard (single rim, never nested)
└─ NO  ↓

Is it an ASCII CALLOUT (warning, hero strip)?
├─ YES → prx-frame
└─ NO  ↓

Default: bare typography on field.
   - heading        → <h1> / <h2> with token typography
   - metric tile    → label / value / detail / action — no border
   - section list   → kicker label + rows; rows hover, no per-row border
   - status row     → mono label + tracked-mono value, no chrome
   - section divider → SectionStrip (rare)
```

---

## Primitive ownership table

Every surface element has a contract: a primitive, a source (where the data comes from), an action (what the user can do), an empty/error state, and a proof path (what receipt or decision it links to).

| Surface element | Primitive | Allowed containment | Source | Action | Empty / error | Proof |
|---|---|---|---|---|---|---|
| Page hero (`Continue work`, intent prompt) | bare `<h1>` + `<p>` + `<Button>` row | none | `summary.workflow_counts` | describe / blank-build / chat / file-attach | "Start work" copy | n/a |
| At-a-glance metric tile | bare `dash-tile` (label/value/detail/action) | none | gateway op (e.g. `/api/dashboard`) | drill into source | "—" / "Unavailable" | receipt_id |
| Sealed receipt strip | `ReceiptCard` × N in a grid | per-card only | `authority_operation_receipts` | open receipt | "no receipts yet" | hash + receipt_id |
| Workflow lane (Live/Saved/Drafts) header | bare kicker label + count | none | `summary.sections[*]` | none | "no workflows in this lane" | n/a |
| Workflow card | `PanelCard` (eyebrow + title + footer Button row) | per-card | `workflows[*]` | open / run / delete | "no description" body copy | workflow_id |
| Sidebar panel header (Toolbelt Review, Recent Runs) | bare kicker label + count | **none** | various | header `<Button size="sm">` if any | "No review pressure" copy | n/a |
| Sidebar list row | bare row with `<LedDot>` | none | `failed_runs[*]` / `recent_runs[*]` / `tool_opportunities[*]` | open run / open spend | per-row "no recent runs" | run_id / receipt_id |
| Source pill | `<SourceChip>` | per-pill | `source_options[*]` | toggle source | hidden if empty | option.id |
| Tab toggle | `<RadioPillGroup>` | none | `bundle.tabs` | switch tab | n/a | tab.id |
| Action button | `<Button>` | none | n/a | the action | disabled state | dispatch receipt |
| Dispatch (gateway op) | `<DispatchButton>` from primitives-prx | none | operation catalog | dispatch op | dry-run preview | hash + replay indicator |
| Status indicator | `<LedDot>` | none | `signal_*` flag | n/a | tone="idle" | n/a |
| Section divider (rare) | `<SectionStrip>` | none | n/a | n/a | n/a | n/a |
| ASCII callout (rare) | `prx-frame` | full | n/a | n/a | n/a | n/a |
| Drawer / inspector body | `<PanelCard>` (single, no nesting) | full | the bound record | close / dispatch / drill | "select a record" | record_id |

---

## Hard anti-patterns (banned)

These are not style suggestions — they are forbidden patterns. Each one has a grep / lint recipe.

1. **No nested cards.** A `prx-card` inside a `prx-card` (or `prx-receipt` inside a `prx-card`) is always wrong. The outer container exists for nothing — the inner card already has its own border.
2. **No outer rim around grids.** A grid of metric tiles, receipts, source pills, or tabs MUST NOT have a wrapping rim. The grid is layout, not a container.
3. **No local lookalike cards.** `.dash-card`, `.moon-card`, `.wf-card`, `.workspace-card`, `.surface-card` — every per-surface card class that mimics what `prx-card` / `prx-receipt` already does is banned. Use the primitive or use bare composition.
4. **No inline hex outside `tokens.css`.** Every color must route through a CSS variable. The token vocabulary: `--bg`, `--bg-card`, `--bg-alt`, `--text`, `--text-muted`, `--text-inverse`, `--accent`, `--success`, `--warning`, `--danger`, `--border`. Anything else is a regression.
5. **No fifth semantic color.** White (inert / sealed / static) · Amber (in-process / pending / draft) · Green (success / ok) · Red (error / refused). No blue tints, no purple accents, no sage / ochre / rust.
6. **No `<PanelCard>` around headings, metric rows, or list sections.** `PanelCard` is for actionable units. A section title with a list below it is bare type — kicker label + body, no rim.
7. **No decorative gradients.** The page-level orange glow at one corner of `.dash-page` is the only decorative gradient in the entire app. No cream washes (cream + black = brown), no per-card gradient backgrounds, no animated glows except as state indicators.
8. **No inline `style={{…}}`.** Every visual rule lives in CSS. Inline style is reserved for positioning math (canvas transforms, drag offsets, animated values that depend on runtime state).
9. **No `clamp()` typography.** Type sizes are fixed numbers from the token scale. Responsive layouts use grid-template / breakpoints, not fluid type.
10. **No "just-this-one-time" exceptions.** If a surface needs something the primitive system doesn't have, extend the primitive system. Never one-off.

---

## Lint / grep recipes (must return empty)

Run from `Code&DBs/Workflow/surfaces/app/`. Each command should print no matches in the named directories. CI should run these and fail the build on regression.

```bash
# 1. No inline styles outside Moon's canvas-transform exceptions
grep -rE 'style=\{\{' src/dashboard/ src/praxis/ src/grid/ src/atlas/

# 2. No hardcoded hex outside tokens.css / primitives*.css
grep -rE '#[0-9a-fA-F]{3,6}\b' src/dashboard/ src/moon/ src/praxis/ src/grid/ src/atlas/

# 3. No nested prx-card (containment violation)
grep -rzoE 'prx-card[^"]*"[^<]*<[^>]*prx-card' src/

# 4. No local lookalike card classes (banned naming)
grep -rE '\.(dash|moon|wf|workspace|surface)-[a-z_]+(card|panel|frame|tile|chip)' src/dashboard/*.css src/moon/*.css src/praxis/*.css

# 5. No `clamp(` on font-size
grep -rE 'font-size:\s*clamp' src/dashboard/ src/moon/ src/praxis/ src/atlas/ src/grid/

# 6. No PanelCard / FrameCard wrapping a metric-row grid (rim around grid is banned)
# Manual audit: search for `<PanelCard` and confirm each contains a discrete actionable record, not a grid of tiles.

# 7. No decorative gradients (only the page-level glow is allowed)
grep -rE 'linear-gradient|radial-gradient' src/dashboard/ src/moon/ src/praxis/ src/grid/ src/atlas/ \
  | grep -v 'dash-page' \
  | grep -v 'moon-bg-glow'   # add other allowed glow rules here
```

---

## Anti-pattern showcase (with fixes)

### Anti-pattern 1: outer wrapper around 2 receipt cards
**Wrong:**
```tsx
<section className="dash-receipts">
  <ReceiptCard {...workflow} />
  <ReceiptCard {...health} />
</section>
```
where `.dash-receipts` has `padding: 24px; background: rgba(255,255,255,0.035)` — a wrapping rim/bg around two cards that already have rims.
**Right:**
```tsx
<div className="dash-receipts">  {/* layout grid only — no border, no padding, no bg */}
  <ReceiptCard {...workflow} />
  <ReceiptCard {...health} />
</div>
```
CSS: `.dash-receipts { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }`. No rim, no padding, no bg.

### Anti-pattern 2: PanelCard around a metric grid
**Wrong:** `<PanelCard><div className="overview-grid">{tiles}</div></PanelCard>` — adds a rim around a grid of bare tiles.
**Right:** `<div className="overview-grid">{tiles}</div>` — bare grid; each tile already has its own subtle hover hairline.

### Anti-pattern 3: PanelCard around a sidebar list
**Wrong:** `<PanelCard eyebrow="Materialize" title="Toolbelt Review" count={5}>{list}</PanelCard>`
**Right:**
```tsx
<section className="dash-rail-section">
  <header className="dash-rail-section__head">
    <span className="eyebrow">Materialize</span>
    <span className="title">Toolbelt Review</span>
    <span className="count">5</span>
  </header>
  <div className="dash-rail-section__list">{list}</div>
</section>
```
A bare kicker + title + body. No rim. Rows inside have `:hover` background, no per-row border.

### Anti-pattern 4: cream gradient on dark bg
**Wrong:** `background: linear-gradient(180deg, rgba(243, 238, 228, 0.06), transparent);` — cream + black = brown.
**Right:** Either pure black bg, or `rgba(255, 255, 255, 0.025)` for translucent panels (neutral white tint, not warm).

---

## Naming hygiene

- `PanelCard` is the canonical name for the discrete-record / scrollable-panel primitive. It used to be called `FrameCard` — that name invited the wrong instinct ("everything is a frame"). The CSS class stays `prx-card` (frames in CSS-land are fine; the danger is the React name).
- `prx-frame` is reserved for the ASCII corner-bracket callout. Different primitive entirely.
- `ReceiptCard` is reserved for sealed proof records. Don't reuse it for at-a-glance metrics.
- `dash-tile` is the bare metric pattern. Not a primitive — it's a composition of plain elements with a class.

---

## Update protocol

When the primitive system gains a new component:
1. Add it to `DisplayPrimitives.tsx` or `StructuralPrimitives.tsx`.
2. Add tests in `__tests__/`.
3. **Add a row to the ownership table above** specifying its source / action / empty / proof contract.
4. **Add a containment-audit rule** clarifying when to reach for it.
5. Document any new anti-pattern it makes possible.

When you find a surface using a banned pattern:
1. Refactor to the primitive — don't paper over it.
2. If the primitive is missing the feature, extend the primitive (Phase 0 of any refactor).
3. Update this doc to reflect the new capability.

---

## Validation

Refactor success isn't "looks better." It's:

1. The grep recipes above all return empty for the touched surface.
2. Every visible block on the surface satisfies a row in the ownership table (source / action / empty / proof).
3. No nested rims, no outer-grid rims, no local lookalike cards.
4. Visual screenshot confirms type + whitespace carry the page.

Anything less is a regression dressed up as progress.
