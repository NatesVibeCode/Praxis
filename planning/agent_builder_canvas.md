# First-Class Agent Builder in Canvas

## Context

Agents in Praxis today exist in two disconnected universes:

1. **Canvas / Compiler (implicit):** Agents are compile-time inferences (`infer_agent_route()`). The compiler regex-matches node prose to guess the route (`auto/build`, `auto/review`). The user has no visibility, no configuration, no way to define custom agents.

2. **Agent Registry (explicit):** Durable agent identities exist in `agent_registry` (migration 398, renamed in 407) with strict scope cages — `write_envelope`, `capability_refs`, `allowed_tools`, `network_policy`, `standing_order_keys`, `routing_policy`. Full CQRS operations registered (register, update_status, describe, list, wake, delegate, tool_gap). But these are only used for chat triggers, webhooks, and background delegations — never Canvas.

**The goal:** Bridge these universes. `agent_registry` becomes the single authority for all LLM actor definitions. Canvas exposes it for creation and assignment. The compiler and runtime enforce the agent's full envelope — prompt, standing orders, tools, network, model — whether the agent was triggered by chat or by a Canvas workflow node.

---

## Phase 1: Database — UI metadata on existing authority

**File:** `Code&DBs/Databases/migrations/workflow/419_agent_builder_authority.sql`

Add columns to `agent_registry` for the Canvas builder:

| Column | Type | Purpose |
|--------|------|---------|
| `system_prompt_template` | text | Base instructions injected before standing orders. Supports `{node_prompt}`, `{operating_model}` variables |
| `description` | text | Shown in Canvas agent panel and picker |
| `icon_hint` | text | Visual identifier (`build`, `review`, `research`, `reasoning`, or custom) |
| `visibility` | text | `visible` / `hidden` / `archived` — controls Canvas picker |
| `builder_category` | text | `builtin` / `custom` — builtins protected from deletion |
| `model_preference` | text | Optional model pin (e.g. `claude-sonnet-4-6`) |
| `reasoning_effort` | text | Optional `low` / `medium` / `high` |

Seed four builtin agents:
- `agent.builtin.build` — general-purpose builder
- `agent.builtin.review` — validation/audit
- `agent.builtin.research` — investigation/evidence
- `agent.builtin.reasoning` — synthesis/reconciliation

**CQRS evolution (not new operations):**
- Use `praxis_evolve_operation_field` to update `agent_principal.command.register` (`RegisterAgentPrincipalCommand`) to accept the new fields. This remains the single authoritative upsert path — no parallel UI-only mutation surface.
- Update `agent_principal.query.list` to return the new fields so Canvas can populate the picker efficiently.

---

## Phase 2: Canvas UI — exposing the registry

### 2a. CanvasAgentPanel

**New file:** `surfaces/app/src/canvas/CanvasAgentPanel.tsx`

Pattern: identical to `CanvasIntegrationsPanel.tsx` — collapsible section in the action dock.

- Header: "Agents" with count badge
- Lists `agent_registry` rows with `visibility='visible'`, builtins first
- Each row: icon glyph, title, description (truncated), capability count
- "New agent" button → inline creation form → invokes the evolved `agent_principal.command.register`
- Click row → inline edit form (system prompt, capabilities, tools, network policy, model pref, reasoning effort)
- Draggable rows for drag-to-assign onto canvas nodes

**Mount in CanvasActionDock.tsx** at line ~494, between `<CanvasDecisionsPanel />` and `<AccessControlPanel />`:
```tsx
<CanvasAgentPanel />
```

### 2b. CanvasAgentPicker

**New file:** `surfaces/app/src/canvas/CanvasAgentPicker.tsx`

Replace read-only display in `CanvasNodeDetail.tsx` (lines 1962–1965):
```tsx
// Before: static text
{node.agent && (
  <span className="canvas-run-contract__label">Agent</span>
  <span className="canvas-run-contract__value">{node.agent}</span>
)}

// After: interactive picker
<CanvasAgentPicker
  value={node.agent}
  agents={agentRegistry}
  onChange={(ref) => onMutate(`nodes/${node.id}/agent`, { value: ref })}
/>
```

- Shows resolved agent title or "Inferred" when null
- Dropdown lists all visible agents from registry
- Fires `canvas_mutate_field` on `nodes/{node_id}/agent`
- Supports drag-from-panel onto node

### 2c. Data hook

**New file:** `surfaces/app/src/shared/hooks/useAgentRegistry.ts`

Fetches agent list from `/api/agent_principals` (existing endpoint backed by `agent_principal.query.list`). Caches in component state. Used by both CanvasAgentPanel and CanvasAgentPicker.

### 2d. Type update

**File:** `surfaces/app/src/shared/types.ts`

`BuildNode.agent` accepts `agent_principal_ref` strings (e.g. `"agent.builtin.build"`).

---

## Phase 3: Compiler bridge + standing-order preservation

**Files:** `Code&DBs/Workflow/runtime/compiler_references.py`, `Code&DBs/Workflow/runtime/agent_context.py`

### 3a. Resolve from registry

Update `generate_jobs()` in `compiler_references.py`:
- If `node.agent` matches an `agent_registry` row → retrieve full definition
- If `node.agent` is a legacy route string (`auto/build`) → map to `agent.builtin.*`
- If `node.agent` is null → fall back to `infer_agent_route()` (temporary, removed in Phase 5)

### 3b. Preserve the context envelope (the lobotomy fix)

`compile_agent_context()` in `agent_context.py` currently builds the system prompt for wake-triggered runs by appending standing orders and recent chat/wakes to the base prompt. A Canvas-assigned agent must receive the same context — otherwise it's legally blind to its durable directives.

Refactor the prompt builder so the compiler can use it for Canvas jobs:
1. `system_prompt_template` from the builder becomes the **base instruction**
2. The compiler **appends** the agent's bound `standing_order_keys` (resolved from `operator_decisions`)
3. The compiler appends recent context (wakes, delegation history) from the agent's ledger
4. The compiled prompt = template + standing orders + context — same envelope as wake-triggered runs

### 3c. Job identity

The compiled job spec carries:
- `job["agent_principal_ref"]` — the resolved ref
- `job["system_prompt"]` — full compiled prompt (template + standing orders + context)
- `job["write_envelope"]`, `job["allowed_tools"]`, `job["network_policy"]` — from the agent definition

---

## Phase 4: Runtime enforcement + execution ledger

**Files:** `Code&DBs/Workflow/runtime/agent_context.py`, `Code&DBs/Workflow/runtime/execution_policy.py`

### 4a. Ledger integrity

When the runtime executes a workflow job carrying `agent_principal_ref`, it must write an `agent_delegations` row (or `agent_wakes` with `trigger_kind='workflow_node'`). Without this, Canvas agent executions are invisible to `praxis_agent_describe` — violating observability. The execution becomes queryable in the agent's delegation/wake history.

### 4b. Trust compiler enforcement

At execution time:
1. Intersect node capabilities with agent's `allowed_tools` → `effective_tools`
2. Intersect node's `write_scope` with agent's `write_envelope` → `effective_write_scope`
3. If node requests capabilities the agent lacks → compile error surfaced in Canvas
4. Agent's `network_policy` overrides node default if more restrictive

### 4c. Tool gap filing

If an agent-bound execution hits a capability boundary it cannot cross, file an `agent_tool_gaps` row (roadmap fuel) rather than silently failing.

---

## Phase 5: Delete inference (fast-follow)

Remove the backward compatibility trap entirely.

1. **Backfill script** (`scripts/praxis-migrate-canvas-agents`): Scan all existing `build_graph` JSONB records. For any node where `agent` is null or uses a legacy route string, map to the exact `agent.builtin.*` reference and update the JSONB.
2. **Delete `infer_agent_route()`** from `compiler_references.py`. The compiler now strictly requires an explicit `agent_principal_ref` for all LLM-executed nodes.

---

## Phase 6: Chat tool for conversational agent assignment

**File:** `Code&DBs/Workflow/runtime/chat_tools.py`

Add `canvas_assign_agent` tool:
- Input: `node_id`, `agent_principal_ref` (or null to clear)
- Validates ref against registry
- Delegates to `canvas_mutate_field` on `nodes/{node_id}/agent`

---

## Critical files

| Purpose | Path |
|---------|------|
| Migration | `Code&DBs/Databases/migrations/workflow/419_agent_builder_authority.sql` |
| Agent panel (new) | `surfaces/app/src/canvas/CanvasAgentPanel.tsx` |
| Agent picker (new) | `surfaces/app/src/canvas/CanvasAgentPicker.tsx` |
| Agent hook (new) | `surfaces/app/src/shared/hooks/useAgentRegistry.ts` |
| Action dock mount | `surfaces/app/src/canvas/CanvasActionDock.tsx` (line ~494) |
| Node detail picker | `surfaces/app/src/canvas/CanvasNodeDetail.tsx` (lines 1962–1965) |
| Compiler bridge | `Code&DBs/Workflow/runtime/compiler_references.py` |
| Context envelope | `Code&DBs/Workflow/runtime/agent_context.py` |
| Runtime enforcement | `Code&DBs/Workflow/runtime/execution_policy.py` |
| Chat tool | `Code&DBs/Workflow/runtime/chat_tools.py` |
| Types | `surfaces/app/src/shared/types.ts` |
| Register command | `runtime/operations/commands/agent_principals.py` (evolve, not replace) |
| List query | `runtime/operations/queries/agent_principals.py` (evolve projection) |
| Backfill script | `scripts/praxis-migrate-canvas-agents` |

## Verification

1. **Migration:** Run migration 419 → `agent_registry` has new columns + 4 builtin rows
2. **Canvas panel:** Open Canvas → action dock shows "Agents" section with 4 builtins
3. **Agent creation:** Click "New agent" → fill form → verify row in `agent_registry` via `praxis_agent_describe`
4. **Node assignment:** Agent picker dropdown → select agent → verify `build_graph` JSONB carries `agent_principal_ref`
5. **Drag-to-assign:** Drag agent from panel onto node → verify assignment
6. **Standing orders:** Compose workflow with agent-assigned node → verify compiled job prompt includes template + standing orders + context (not template alone)
7. **Ledger:** Execute workflow with agent-bound node → verify `agent_delegations` row appears → verify `praxis_agent_describe` shows the execution
8. **Enforcement:** Assign agent with restricted `allowed_tools` → node requests broader capabilities → verify compile error in Canvas
9. **Tool gaps:** Agent-bound execution hits missing capability → verify `agent_tool_gaps` row filed
10. **Backfill (Phase 5):** Run `scripts/praxis-migrate-canvas-agents` → all legacy `auto/*` refs replaced → `infer_agent_route()` deleted → existing workflows still compile